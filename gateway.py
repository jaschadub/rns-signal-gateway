#!/usr/bin/env python3
"""Bridge between Reticulum (LXMF) and Signal (signal-cli JSON-RPC/SSE).

Text-only MVP. Maps Signal groups to lists of LXMF destination hashes
("channels"): Signal group messages fan out to the channel's LXMF members,
and LXMF messages from members are posted into the Signal group.
"""

import argparse
import hashlib
import json
import os
import shutil
import tempfile
import threading
import time
import urllib.request

import LXMF
import RNS
import tomllib

DEDUP_TTL = 24 * 3600


# ---------- pure helpers (exercised by test_gateway.py) ----------

def message_id(source, timestamp, content):
    return hashlib.sha256(f"{source}|{timestamp}|{content}".encode()).hexdigest()


class Dedup:
    """Remembers message ids for ttl seconds; drops repeats."""

    def __init__(self, ttl=DEDUP_TTL):
        self.ttl = ttl
        self.seen = {}
        self.lock = threading.Lock()

    def check(self, mid, now=None):
        """True if new (and records it), False if already seen."""
        now = time.time() if now is None else now
        with self.lock:
            # ponytail: O(n) prune per call; fine at gateway message volumes
            for k in [k for k, t in self.seen.items() if now - t > self.ttl]:
                del self.seen[k]
            if mid in self.seen:
                return False
            self.seen[mid] = now
            return True


def parse_signal_event(event, own_account):
    """Extract (source, name, group_id, text, timestamp) from an SSE event.

    Handles dataMessage (posts from others) and syncMessage.sentMessage
    (posts from the gateway account's own other devices, e.g. when the
    gateway is linked to a personal Signal account). Returns None for
    anything that should not be bridged.
    """
    envelope = event.get("envelope") or {}
    data = envelope.get("dataMessage")
    sync = data is None
    if sync:
        data = (envelope.get("syncMessage") or {}).get("sentMessage") or {}
    text = data.get("message") or ""
    attachments = data.get("attachments") or []
    if not text and not attachments:
        return None
    if sync and text.startswith("[RNS "):
        return None  # loop guard: the gateway's own bridged posts
    source = (envelope.get("sourceNumber")
              or envelope.get("sourceUuid")
              or envelope.get("source"))
    if source is None or (not sync and source == own_account):
        return None
    group_id = (data.get("groupInfo") or {}).get("groupId")
    name = envelope.get("sourceName") or source
    return (source, name, group_id, text, envelope.get("timestamp"),
            attachments)


def attachment_fields(loaded):
    """Build LXMF fields from [(filename, content_type, bytes), ...].

    The first image becomes FIELD_IMAGE (rendered inline by Sideband);
    everything else becomes FIELD_FILE_ATTACHMENTS.
    """
    fields = {}
    files = []
    for name, ctype, data in loaded:
        ext = os.path.splitext(name)[1].lstrip(".").lower()
        if ctype.startswith("image/") and LXMF.FIELD_IMAGE not in fields:
            fields[LXMF.FIELD_IMAGE] = [ext or ctype.split("/", 1)[1], data]
        else:
            files.append([os.path.basename(name), data])
    if files:
        fields[LXMF.FIELD_FILE_ATTACHMENTS] = files
    return fields


def lxmf_attachments(message_fields, max_bytes):
    """Extract [(filename, bytes), ...] plus drop notes from LXMF fields."""
    out, notes = [], []
    fields = message_fields or {}
    image = fields.get(LXMF.FIELD_IMAGE)
    if isinstance(image, (list, tuple)) and len(image) >= 2 and image[1]:
        out.append((f"image.{image[0]}", image[1]))
    audio = fields.get(LXMF.FIELD_AUDIO)
    if isinstance(audio, (list, tuple)) and len(audio) >= 2 and audio[1]:
        # opus modes (>= AM_OPUS_OGG) are ogg containers Signal can play;
        # codec2 modes are raw low-bitrate radio audio, forwarded as .c2
        ext = "ogg" if audio[0] >= LXMF.AM_OPUS_OGG else "c2"
        out.append((f"voice.{ext}", audio[1]))
    for att in fields.get(LXMF.FIELD_FILE_ATTACHMENTS) or []:
        if isinstance(att, (list, tuple)) and len(att) >= 2 and att[1]:
            out.append((str(att[0]) or "file", att[1]))
    kept = []
    for name, data in out:
        if len(data) > max_bytes:
            notes.append(f"[dropped {os.path.basename(name)}: "
                         f"{len(data)} B over {max_bytes} B limit]")
        else:
            kept.append((os.path.basename(name), data))
    return kept, notes


def attachment_sig(items):
    """Stable digest input for dedup: names and sizes of attachments.

    Accepts (name, ..., data) tuples — name first, data last.
    """
    return "".join(f"|{i[0]}:{len(i[-1])}" for i in items)


def load_config(path):
    with open(path, "rb") as f:
        cfg = tomllib.load(f)
    cfg.setdefault("channels", [])
    for ch in cfg["channels"]:
        ch["members"] = [m.lower() for m in ch.get("members", [])]
    return cfg


def channel_for_group(cfg, group_id):
    for ch in cfg["channels"]:
        if group_id is not None and ch["signal_group"] == group_id:
            return ch
    return None


def channel_for_member(cfg, lxmf_hash):
    for ch in cfg["channels"]:
        if lxmf_hash in ch["members"]:
            return ch
    return None


# ---------- gateway ----------

class Gateway:
    def __init__(self, cfg):
        self.cfg = cfg
        self.dedup = Dedup()
        self.rpc_url = cfg["signal"]["rpc_url"].rstrip("/")
        self.account = cfg["signal"]["account"]
        self.max_bytes = cfg["gateway"].get("max_message_bytes", 4096)
        self.max_attachment_bytes = cfg["gateway"].get(
            "max_attachment_bytes", 1_000_000)
        self.attachment_dir = os.path.expanduser(cfg["signal"].get(
            "attachment_dir", "~/.local/share/signal-cli/attachments"))

        storage = cfg["gateway"]["storage"]
        os.makedirs(storage, exist_ok=True)
        # optional isolated RNS config dir; None = default ~/.reticulum
        self.reticulum = RNS.Reticulum(cfg["gateway"].get("rns_configdir"))

        identity_path = os.path.join(storage, "identity")
        if os.path.isfile(identity_path):
            identity = RNS.Identity.from_file(identity_path)
        else:
            identity = RNS.Identity()
            identity.to_file(identity_path)

        self.router = LXMF.LXMRouter(storagepath=os.path.join(storage, "lxmf"))
        self.dest = self.router.register_delivery_identity(
            identity,
            display_name=cfg["gateway"].get("display_name", "Signal Gateway"),
        )
        self.router.register_delivery_callback(self.on_lxmf)
        RNS.log(f"Gateway LXMF address: {RNS.prettyhexrep(self.dest.hash)}",
                RNS.LOG_NOTICE)

    # ----- Signal side -----

    def signal_rpc(self, method, params):
        body = json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params,
        }).encode()
        req = urllib.request.Request(
            self.rpc_url + "/api/v1/rpc", data=body,
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            reply = json.load(resp)
        if "error" in reply:
            raise RuntimeError(f"signal-cli error: {reply['error']}")
        return reply.get("result")

    def sse_loop(self):
        url = self.rpc_url + "/api/v1/events"
        while True:
            try:
                req = urllib.request.Request(
                    url, headers={"Accept": "text/event-stream"})
                with urllib.request.urlopen(req) as resp:
                    RNS.log("Connected to signal-cli event stream", RNS.LOG_NOTICE)
                    for raw in resp:
                        line = raw.decode("utf-8", "replace").strip()
                        if not line.startswith("data:"):
                            continue
                        try:
                            event = json.loads(line[len("data:"):])
                        except ValueError:
                            continue
                        try:
                            self.on_signal(event)
                        except Exception as e:  # noqa: BLE001 - daemon must survive bad messages
                            RNS.log(f"Signal handler error: {e}", RNS.LOG_ERROR)
            except Exception as e:  # noqa: BLE001 - daemon must survive bad messages
                RNS.log(f"SSE stream error, reconnecting in 5s: {e}",
                        RNS.LOG_WARNING)
                time.sleep(5)

    def load_signal_attachments(self, attachments):
        """Read attachment files signal-cli stored; returns (loaded, notes)."""
        loaded, notes = [], []
        for att in attachments:
            aid = att.get("id")
            name = att.get("filename") or aid or "file"
            path = os.path.join(self.attachment_dir, aid) if aid else None
            if not path or not os.path.isfile(path):
                notes.append(f"[attachment {name} unavailable]")
                continue
            size = os.path.getsize(path)
            if size > self.max_attachment_bytes:
                notes.append(f"[dropped {name}: {size} B over "
                             f"{self.max_attachment_bytes} B limit]")
                continue
            with open(path, "rb") as f:
                loaded.append((name, att.get("contentType") or "", f.read()))
        return loaded, notes

    def on_signal(self, event):
        parsed = parse_signal_event(event, self.account)
        if parsed is None:
            return
        source, name, group_id, text, timestamp, attachments = parsed

        allowed = self.cfg["signal"].get("allowed_users")
        if allowed and source not in allowed:
            RNS.log(f"Dropping Signal message from unlisted user {source}",
                    RNS.LOG_VERBOSE)
            return
        channel = channel_for_group(self.cfg, group_id)
        if channel is None:  # deny by default: unmapped groups and DMs
            return
        if len(text.encode()) > self.max_bytes:
            RNS.log(f"Dropping oversize Signal message ({len(text)} chars) "
                    f"from {source}", RNS.LOG_WARNING)
            return
        loaded, notes = self.load_signal_attachments(attachments)
        if not self.dedup.check(message_id(
                source, timestamp, text + attachment_sig(loaded))):
            return

        RNS.log(f"Bridging Signal message from {source} "
                f"({len(loaded)} attachment(s)) to "
                f"{len(channel['members'])} LXMF member(s) on "
                f"'{channel['name']}'", RNS.LOG_INFO)
        body = "\n".join(p for p in [f"[Signal {name}] {text}".rstrip(),
                                     *notes] if p)
        fields = attachment_fields(loaded)
        for member in channel["members"]:
            threading.Thread(target=self.send_lxmf,
                             args=(member, body, fields),
                             daemon=True).start()

    # ----- Reticulum side -----

    def on_lxmf(self, message):
        try:
            self.handle_lxmf(message)
        except Exception as e:  # noqa: BLE001 - daemon must survive bad messages
            RNS.log(f"LXMF handler error: {e}", RNS.LOG_ERROR)

    def handle_lxmf(self, message):
        sender = message.source_hash.hex()
        channel = channel_for_member(self.cfg, sender)
        if channel is None:  # deny by default
            RNS.log(f"Dropping LXMF message from non-member {sender}",
                    RNS.LOG_VERBOSE)
            return
        if not message.signature_validated:
            RNS.log(f"Dropping LXMF message from member {sender} without "
                    f"validated signature (no announce seen yet?)",
                    RNS.LOG_WARNING)
            return
        if len(message.content) > self.max_bytes:
            RNS.log(f"Dropping oversize LXMF message ({len(message.content)} "
                    f"bytes) from {sender}", RNS.LOG_WARNING)
            return
        text = message.content.decode("utf-8", "replace").strip()
        attachments, notes = lxmf_attachments(message.fields,
                                              self.max_attachment_bytes)
        if not text and not attachments and not notes:
            return
        if not self.dedup.check(message_id(
                sender, message.timestamp, text + attachment_sig(attachments))):
            return
        params = {
            "account": self.account,
            "groupId": channel["signal_group"],
            "message": "\n".join(p for p in [f"[RNS {sender[:8]}]", text,
                                             *notes] if p),
        }
        tmpdir = None
        try:
            if attachments:
                tmpdir = tempfile.mkdtemp(prefix="rns-signal-att-")
                paths = []
                # ponytail: same-basename attachments overwrite; prefix an
                # index if that ever matters
                for name, data in attachments:
                    path = os.path.join(tmpdir, os.path.basename(name))
                    with open(path, "wb") as f:
                        f.write(data)
                    paths.append(path)
                params["attachments"] = paths
            self.signal_rpc("send", params)
        finally:
            if tmpdir:
                shutil.rmtree(tmpdir, ignore_errors=True)
        RNS.log(f"Bridged LXMF message from {sender} "
                f"({len(attachments)} attachment(s)) to Signal group on "
                f"'{channel['name']}'", RNS.LOG_INFO)

    def send_lxmf(self, dest_hex, text, fields=None):
        try:
            dest_hash = bytes.fromhex(dest_hex)
            if not RNS.Transport.has_path(dest_hash):
                RNS.Transport.request_path(dest_hash)
                deadline = time.time() + 30
                while (not RNS.Transport.has_path(dest_hash)
                       and time.time() < deadline):
                    time.sleep(0.25)
            identity = RNS.Identity.recall(dest_hash)
            if identity is None:
                RNS.log(f"No identity known for {dest_hex}, dropping",
                        RNS.LOG_WARNING)
                return
            destination = RNS.Destination(
                identity, RNS.Destination.OUT, RNS.Destination.SINGLE,
                "lxmf", "delivery")
            lxm = LXMF.LXMessage(destination, self.dest, text,
                                 fields=fields or None,
                                 desired_method=LXMF.LXMessage.DIRECT)
            self.router.handle_outbound(lxm)
        except Exception as e:  # noqa: BLE001 - daemon must survive bad messages
            RNS.log(f"LXMF send to {dest_hex} failed: {e}", RNS.LOG_ERROR)

    def run(self):
        threading.Thread(target=self.sse_loop, daemon=True).start()
        interval = self.cfg["gateway"].get("announce_interval", 3600)
        while True:
            self.router.announce(self.dest.hash)
            time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-c", "--config", default="config.toml",
                        help="path to TOML config (default: config.toml)")
    args = parser.parse_args()
    Gateway(load_config(args.config)).run()


if __name__ == "__main__":
    main()
