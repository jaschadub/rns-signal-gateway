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

    Returns None for anything that should not be bridged: non-text events,
    messages from the gateway's own account, sync messages.
    """
    envelope = event.get("envelope") or {}
    data = envelope.get("dataMessage") or {}
    text = data.get("message")
    if not text:
        return None
    source = (envelope.get("sourceNumber")
              or envelope.get("sourceUuid")
              or envelope.get("source"))
    if source is None or source == own_account:
        return None
    group_id = (data.get("groupInfo") or {}).get("groupId")
    name = envelope.get("sourceName") or source
    return (source, name, group_id, text, envelope.get("timestamp"))


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

        storage = cfg["gateway"]["storage"]
        os.makedirs(storage, exist_ok=True)
        self.reticulum = RNS.Reticulum()

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

    def on_signal(self, event):
        parsed = parse_signal_event(event, self.account)
        if parsed is None:
            return
        source, name, group_id, text, timestamp = parsed

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
        if not self.dedup.check(message_id(source, timestamp, text)):
            return

        body = f"[Signal {name}] {text}"
        for member in channel["members"]:
            threading.Thread(target=self.send_lxmf, args=(member, body),
                             daemon=True).start()

    # ----- Reticulum side -----

    def on_lxmf(self, message):
        try:
            self.handle_lxmf(message)
        except Exception as e:  # noqa: BLE001 - daemon must survive bad messages
            RNS.log(f"LXMF handler error: {e}", RNS.LOG_ERROR)

    def handle_lxmf(self, message):
        if not message.signature_validated:
            return
        sender = message.source_hash.hex()
        channel = channel_for_member(self.cfg, sender)
        if channel is None:  # deny by default
            RNS.log(f"Dropping LXMF message from non-member {sender}",
                    RNS.LOG_VERBOSE)
            return
        if len(message.content) > self.max_bytes:
            RNS.log(f"Dropping oversize LXMF message ({len(message.content)} "
                    f"bytes) from {sender}", RNS.LOG_WARNING)
            return
        text = message.content.decode("utf-8", "replace").strip()
        if not text:
            return
        if not self.dedup.check(message_id(sender, message.timestamp, text)):
            return
        self.signal_rpc("send", {
            "account": self.account,
            "groupId": channel["signal_group"],
            "message": f"[RNS {sender[:8]}]\n{text}",
        })

    def send_lxmf(self, dest_hex, text):
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
