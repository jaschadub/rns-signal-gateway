#!/usr/bin/env python3
"""Bridge between Reticulum (LXMF) and Signal (signal-cli JSON-RPC/SSE).

Text-only MVP. Maps Signal groups to lists of LXMF destination hashes
("channels"): Signal group messages fan out to the channel's LXMF members,
and LXMF messages from members are posted into the Signal group.
"""

import argparse
import hashlib
import io
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.request
import wave
from concurrent.futures import ThreadPoolExecutor

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


def shrink_image(data, max_bytes):
    """Recompress (and if needed downscale) an image to WebP under max_bytes.

    Returns WebP bytes, or None if Pillow is missing or the data isn't a
    decodable image. Always terminates: dimensions halve each round.
    """
    try:
        from PIL import Image
    except ImportError:
        return None
    try:
        img = Image.open(io.BytesIO(data)).convert("RGB")
        while True:
            buf = io.BytesIO()
            img.save(buf, "WEBP", quality=75)
            if buf.tell() <= max_bytes or min(img.size) <= 32:
                return buf.getvalue()
            img = img.resize((max(img.width // 2, 16),
                              max(img.height // 2, 16)))
    except Exception as e:  # noqa: BLE001 - undecodable input falls through
        RNS.log(f"Image downscale failed ({e})", RNS.LOG_VERBOSE)
        return None


def attachment_fields(loaded, max_bytes, voice_codec2_bitrate=None,
                      image_max_bytes=None):
    """Build LXMF fields from [(filename, content_type, bytes), ...].

    Returns (fields, notes). The first image becomes FIELD_IMAGE (rendered
    inline by Sideband), downscaled to image_max_bytes (or max_bytes) when
    it doesn't already fit. When voice_codec2_bitrate is set, the first
    audio attachment is transcoded to a codec2 FIELD_AUDIO (tiny, plays in
    Sideband's voice UI, LoRa-friendly). Everything else becomes
    FIELD_FILE_ATTACHMENTS; anything still over max_bytes is dropped with
    a note.
    """
    fields = {}
    files = []
    notes = []
    for name, ctype, data in loaded:
        ext = os.path.splitext(name)[1].lstrip(".").lower()
        if ctype.startswith("image/") and LXMF.FIELD_IMAGE not in fields:
            budget = image_max_bytes or max_bytes
            if len(data) > budget:
                shrunk = shrink_image(data, budget)
                if shrunk is not None:
                    data, ext = shrunk, "webp"
            if len(data) > max_bytes:
                notes.append(f"[dropped {name}: {len(data)} B over "
                             f"{max_bytes} B limit]")
                continue
            fields[LXMF.FIELD_IMAGE] = [ext or ctype.split("/", 1)[1], data]
            continue
        if (ctype.startswith("audio/") and voice_codec2_bitrate
                and LXMF.FIELD_AUDIO not in fields):
            c2 = audio_to_codec2(data, voice_codec2_bitrate)
            if c2 is not None:
                fields[LXMF.FIELD_AUDIO] = [
                    AM_FOR_BITRATE[voice_codec2_bitrate], c2]
                continue
        if len(data) > max_bytes:
            notes.append(f"[dropped {name}: {len(data)} B over "
                         f"{max_bytes} B limit]")
            continue
        files.append([os.path.basename(name), data])
    if files:
        fields[LXMF.FIELD_FILE_ATTACHMENTS] = files
    return fields, notes


def lxmf_attachments(message_fields, max_bytes):
    """Extract [(filename, bytes), ...] plus drop notes from LXMF fields."""
    out, notes = [], []
    fields = message_fields or {}
    image = fields.get(LXMF.FIELD_IMAGE)
    if isinstance(image, (list, tuple)) and len(image) >= 2 and image[1]:
        name, data = f"image.{image[0]}", image[1]
        if len(data) > max_bytes:
            shrunk = shrink_image(data, max_bytes)
            if shrunk is not None:
                name, data = "image.webp", shrunk
        out.append((name, data))
    audio = fields.get(LXMF.FIELD_AUDIO)
    if isinstance(audio, (list, tuple)) and len(audio) >= 2 and audio[1]:
        if audio[0] >= LXMF.AM_OPUS_OGG:
            # opus modes are ogg containers Signal plays natively
            out.append(("voice.ogg", audio[1]))
        else:
            # codec2: raw low-bitrate radio audio; transcode to WAV if
            # pycodec2 is installed, else forward raw
            decoded = codec2_to_wav(audio[0], audio[1])
            if decoded is not None:
                out.append(("voice.wav", decoded))
            else:
                out.append(("voice.c2", audio[1]))
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


CODEC2_BITRATES = {
    LXMF.AM_CODEC2_450PWB: 450, LXMF.AM_CODEC2_450: 450,
    LXMF.AM_CODEC2_700C: 700, LXMF.AM_CODEC2_1200: 1200,
    LXMF.AM_CODEC2_1300: 1300, LXMF.AM_CODEC2_1400: 1400,
    LXMF.AM_CODEC2_1600: 1600, LXMF.AM_CODEC2_2400: 2400,
    LXMF.AM_CODEC2_3200: 3200,
}

AM_FOR_BITRATE = {
    450: LXMF.AM_CODEC2_450, 700: LXMF.AM_CODEC2_700C,
    1200: LXMF.AM_CODEC2_1200, 1300: LXMF.AM_CODEC2_1300,
    1400: LXMF.AM_CODEC2_1400, 1600: LXMF.AM_CODEC2_1600,
    2400: LXMF.AM_CODEC2_2400, 3200: LXMF.AM_CODEC2_3200,
}


def audio_to_codec2(data, bitrate):
    """Transcode any ffmpeg-readable audio to raw codec2 frames.

    Optional feature: requires ffmpeg and pycodec2. Returns None on any
    failure so callers can fall back to passing the original through.
    """
    if bitrate not in AM_FOR_BITRATE:
        return None
    try:
        import numpy as np
        import pycodec2
        pcm = subprocess.run(
            ["ffmpeg", "-v", "quiet", "-i", "pipe:0",
             "-f", "s16le", "-ar", "8000", "-ac", "1", "pipe:1"],
            input=data, capture_output=True, check=True, timeout=60).stdout
        codec = pycodec2.Codec2(bitrate)
        frame_len = codec.samples_per_frame() * 2
        out = bytearray()
        for i in range(0, len(pcm) - frame_len + 1, frame_len):
            samples = np.frombuffer(pcm[i:i + frame_len], dtype=np.int16)
            out += codec.encode(samples)
        return bytes(out) or None
    except Exception as e:  # noqa: BLE001 - fall back to passthrough
        RNS.log(f"codec2 encode unavailable/failed ({e}), passing audio "
                f"through", RNS.LOG_VERBOSE)
        return None


def codec2_to_wav(mode, data):
    """Decode raw codec2 frames to WAV bytes; None if not decodable here.

    Optional feature: requires pycodec2 (which needs libcodec2).
    """
    bitrate = CODEC2_BITRATES.get(mode)
    if bitrate is None:
        return None
    try:
        import pycodec2
        codec = pycodec2.Codec2(bitrate)
        frame_bytes = codec.bytes_per_frame()
        pcm = bytearray()
        for i in range(0, len(data) - frame_bytes + 1, frame_bytes):
            pcm += codec.decode(data[i:i + frame_bytes]).tobytes()
        if not pcm:
            return None
        buf = io.BytesIO()
        with wave.open(buf, "wb") as wav:
            wav.setnchannels(1)
            wav.setsampwidth(2)
            # ponytail: 8 kHz for all modes; 450PWB is nominally 16 kHz and
            # will play slow — special-case it if anyone actually uses it
            wav.setframerate(8000)
            wav.writeframes(bytes(pcm))
        return buf.getvalue()
    except Exception as e:  # noqa: BLE001 - fall back to raw .c2 forwarding
        RNS.log(f"codec2 transcode unavailable/failed ({e}), "
                f"forwarding raw", RNS.LOG_VERBOSE)
        return None


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


def channel_for_member(cfg, lxmf_hash, dynamic=None):
    for ch in cfg["channels"]:
        if (lxmf_hash in ch["members"]
                or lxmf_hash in (dynamic or {}).get(ch["name"], [])):
            return ch
    return None


def command_reply(cfg, dynamic, sender, text):
    """Handle a /command from an LXMF user. Returns (reply, changed).

    Mutates `dynamic` ({channel_name: [hash, ...]}) in place; `changed`
    tells the caller to persist it. Only channels marked `open = true` in
    the config accept /join — deny by default holds for the rest.
    """
    parts = text.split()
    cmd, arg = parts[0].lower(), (parts[1] if len(parts) > 1 else None)
    by_name = {c["name"]: c for c in cfg["channels"]}

    if cmd == "/join" and arg:
        ch = by_name.get(arg)
        if ch is None:
            return f"No such channel: {arg}", False
        if sender in ch["members"] or sender in dynamic.get(arg, []):
            return f"Already a member of {arg}", False
        if not ch.get("open"):
            return f"Channel {arg} is closed; ask the operator", False
        dynamic.setdefault(arg, []).append(sender)
        return f"Joined {arg}", True

    if cmd == "/leave" and arg:
        joined = dynamic.get(arg, [])
        if sender in joined:
            joined.remove(sender)
            return f"Left {arg}", True
        if arg in by_name and sender in by_name[arg]["members"]:
            return ((f"You are in {arg} via the gateway config; "
                     f"ask the operator to remove you"), False)
        return f"Not a member of {arg}", False

    if cmd in ("/channels", "/list"):
        lines = []
        for ch in cfg["channels"]:
            if (sender in ch["members"]
                    or sender in dynamic.get(ch["name"], [])):
                status = "member"
            else:
                status = "open" if ch.get("open") else "closed"
            lines.append(f"{ch['name']} ({status})")
        return "\n".join(lines) or "No channels configured", False

    return ("Commands: /join <channel>, /leave <channel>, /channels",
            False)


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

        delivery_limit_kb = cfg["gateway"].get("lxmf_delivery_limit_kb", 8192)
        # toward Signal, anything the LXMF router accepted is fine — the
        # tight max_attachment_bytes cap protects the Reticulum side only
        self.signal_bound_max_bytes = delivery_limit_kb * 1000
        self.router = LXMF.LXMRouter(
            storagepath=os.path.join(storage, "lxmf"),
            # per-transfer acceptance limit (KB); transfers above this are
            # rejected before the gateway sees them, so keep it above the
            # largest attachment you want to be able to downscale/bridge
            delivery_limit=delivery_limit_kb)
        self.dest = self.router.register_delivery_identity(
            identity,
            display_name=cfg["gateway"].get("display_name", "Signal Gateway"),
        )
        # bounds LXMF send fan-out; sends can block ~30s on path requests
        self.pool = ThreadPoolExecutor(max_workers=8)
        self.members_lock = threading.Lock()
        self.members_path = os.path.join(storage, "members.json")
        self.dynamic_members = {}
        if os.path.isfile(self.members_path):
            with open(self.members_path) as f:
                self.dynamic_members = json.load(f)

        self.router.register_delivery_callback(self.on_lxmf)
        RNS.log(f"Gateway LXMF address: {RNS.prettyhexrep(self.dest.hash)}",
                RNS.LOG_NOTICE)

    def save_members(self):
        tmp = self.members_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.dynamic_members, f, indent=2)
        os.replace(tmp, self.members_path)

    def channel_members(self, channel):
        joined = self.dynamic_members.get(channel["name"], [])
        return channel["members"] + [m for m in joined
                                     if m not in channel["members"]]

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

    # sanity cap on reading attachment files into memory; size policy
    # (drop vs downscale vs transcode) is applied in attachment_fields()
    ATTACHMENT_LOAD_CAP = 32_000_000

    def load_signal_attachments(self, attachments):
        """Read attachment files signal-cli stored; returns (loaded, notes)."""
        loaded, notes = [], []
        for att in attachments:
            aid = att.get("id")
            name = att.get("filename") or aid or "file"
            # basename: never let an upstream-supplied id escape the dir
            path = (os.path.join(self.attachment_dir, os.path.basename(aid))
                    if aid else None)
            if not path or not os.path.isfile(path):
                notes.append(f"[attachment {name} unavailable]")
                continue
            if os.path.getsize(path) > self.ATTACHMENT_LOAD_CAP:
                notes.append(f"[dropped {name}: exceeds "
                             f"{self.ATTACHMENT_LOAD_CAP} B]")
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

        fields, field_notes = attachment_fields(
            loaded, self.max_attachment_bytes,
            self.cfg["gateway"].get("voice_to_codec2"),
            channel.get("image_max_bytes")
            or self.cfg["gateway"].get("image_max_bytes"))
        notes += field_notes
        members = self.channel_members(channel)
        RNS.log(f"Bridging Signal message from {source} "
                f"({len(loaded)} attachment(s)) to "
                f"{len(members)} LXMF member(s) on "
                f"'{channel['name']}'", RNS.LOG_INFO)
        body = "\n".join(p for p in [f"[Signal {name}] {text}".rstrip(),
                                     *notes] if p)
        for member in members:
            self.pool.submit(self.send_lxmf, member, body, fields)

    # ----- Reticulum side -----

    def on_lxmf(self, message):
        try:
            self.handle_lxmf(message)
        except Exception as e:  # noqa: BLE001 - daemon must survive bad messages
            RNS.log(f"LXMF handler error: {e}", RNS.LOG_ERROR)

    def handle_lxmf(self, message):
        sender = message.source_hash.hex()
        text = message.content.decode("utf-8", "replace").strip()
        channel = channel_for_member(self.cfg, sender, self.dynamic_members)
        if channel is None and not text.startswith("/"):  # deny by default
            RNS.log(f"Dropping LXMF message from non-member {sender}",
                    RNS.LOG_VERBOSE)
            return
        if not message.signature_validated:
            RNS.log(f"Dropping LXMF message from {sender} without "
                    f"validated signature (no announce seen yet?); "
                    f"requesting path so the next attempt validates",
                    RNS.LOG_WARNING)
            # path responses carry the sender's announce, teaching us
            # their identity for the next delivery
            RNS.Transport.request_path(message.source_hash)
            return
        if text.startswith("/"):
            with self.members_lock:
                reply, changed = command_reply(self.cfg, self.dynamic_members,
                                               sender, text)
                if changed:
                    self.save_members()
            RNS.log(f"Command from {sender}: {text.split()[0]}", RNS.LOG_INFO)
            self.pool.submit(self.send_lxmf, sender, reply)
            return
        if len(message.content) > self.max_bytes:
            RNS.log(f"Dropping oversize LXMF message ({len(message.content)} "
                    f"bytes) from {sender}", RNS.LOG_WARNING)
            return
        attachments, notes = lxmf_attachments(message.fields,
                                              self.signal_bound_max_bytes)
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
        # distribution-group semantics: other LXMF members get the post
        # too, with original fields passed through natively
        peers = [m for m in self.channel_members(channel) if m != sender]
        for member in peers:
            self.pool.submit(self.send_lxmf, member, params["message"],
                             message.fields or None)
        RNS.log(f"Bridged LXMF message from {sender} "
                f"({len(attachments)} attachment(s)) to Signal group and "
                f"{len(peers)} LXMF member(s) on '{channel['name']}'",
                RNS.LOG_INFO)

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
