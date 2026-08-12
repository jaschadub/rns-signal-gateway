#!/usr/bin/env python3
"""End-to-end MVP test: mock signal-cli + two real RNS/LXMF instances.

Run: python3 test_integration.py

Spawns the gateway and a fake LXMF user as subprocesses, wired together over
a local TCP interface, plus an in-process mock of signal-cli's JSON-RPC/SSE
HTTP API. Verifies both directions:

  LXMF user -> gateway -> Signal group (JSON-RPC send)
  Signal group (SSE event) -> gateway -> LXMF user
"""
import json
import os
import queue
import re
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

RNS_PORT = 42842
SIGNAL_PORT = 47583
GROUP_ID = "dGVzdGdyb3VwaWQ="
ACCOUNT = "+10005550000"

SERVER_RNS_CONFIG = f"""
[reticulum]
  enable_transport = False
  share_instance = False
  panic_on_interface_error = False

[logging]
  loglevel = 4

[interfaces]
  [[TCP Server]]
    type = TCPServerInterface
    enabled = True
    listen_ip = 127.0.0.1
    listen_port = {RNS_PORT}
"""

CLIENT_RNS_CONFIG = f"""
[reticulum]
  enable_transport = False
  share_instance = False
  panic_on_interface_error = False

[logging]
  loglevel = 4

[interfaces]
  [[TCP Client]]
    type = TCPClientInterface
    enabled = True
    target_host = 127.0.0.1
    target_port = {RNS_PORT}
"""


# ---------- fake LXMF user (subprocess role) ----------

def run_client(workdir):
    import LXMF
    import RNS

    RNS.Reticulum(os.path.join(workdir, "client-rns"))
    identity = RNS.Identity()
    router = LXMF.LXMRouter(storagepath=os.path.join(workdir, "client-lxmf"))
    dest = router.register_delivery_identity(identity, display_name="Test User")

    def on_recv(message):
        print("LXMF-RECV: " + message.content.decode("utf-8", "replace"),
              flush=True)

    router.register_delivery_callback(on_recv)
    print("CLIENT-HASH: " + dest.hash.hex(), flush=True)

    gateway_hash = bytes.fromhex(sys.stdin.readline().strip())

    # retry: the TCP client interface may still be reconnecting
    gw_identity = None
    deadline = time.time() + 60
    while gw_identity is None and time.time() < deadline:
        router.announce(dest.hash)
        if not RNS.Transport.has_path(gateway_hash):
            RNS.Transport.request_path(gateway_hash)
        time.sleep(2)
        gw_identity = RNS.Identity.recall(gateway_hash)
    assert gw_identity is not None, "could not recall gateway identity"

    # connectivity is confirmed now; announce so the gateway can validate
    # our signature and route replies to us
    router.announce(dest.hash)
    time.sleep(2)
    gw_dest = RNS.Destination(gw_identity, RNS.Destination.OUT,
                              RNS.Destination.SINGLE, "lxmf", "delivery")
    lxm = LXMF.LXMessage(gw_dest, dest, "hello from reticulum",
                         desired_method=LXMF.LXMessage.DIRECT)
    router.handle_outbound(lxm)
    print("CLIENT-SENT", flush=True)
    while True:
        router.announce(dest.hash)
        time.sleep(5)


# ---------- mock signal-cli ----------

rpc_calls = []
sse_queue = queue.Queue()


class MockSignalHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        req = json.loads(self.rfile.read(n))
        rpc_calls.append(req)
        body = json.dumps({"jsonrpc": "2.0", "id": req.get("id"),
                           "result": {"timestamp": 1}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        while True:
            try:
                event = sse_queue.get(timeout=1)
                self.wfile.write(f"data: {json.dumps(event)}\n\n".encode())
            except queue.Empty:
                self.wfile.write(b":keepalive\n\n")
            self.wfile.flush()

    def log_message(self, *args):
        pass


# ---------- orchestration ----------

def wait_for(pred, timeout, what):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return
        time.sleep(0.2)
    raise TimeoutError(f"timed out waiting for {what}")


def pump(proc, lines, tag):
    for line in proc.stdout:
        line = line.rstrip()
        lines.append(line)
        print(f"  [{tag}] {line}", flush=True)


def spawn(args, lines, tag, stdin=None):
    proc = subprocess.Popen(
        [sys.executable, "-u", *args], stdin=stdin,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    threading.Thread(target=pump, args=(proc, lines, tag), daemon=True).start()
    return proc


def grep(lines, pattern):
    for line in lines:
        m = re.search(pattern, line)
        if m:
            return m
    return None


def main():
    workdir = tempfile.mkdtemp(prefix="rns-signal-test-")
    here = os.path.dirname(os.path.abspath(__file__))

    for name, content in (("gateway-rns", SERVER_RNS_CONFIG),
                          ("client-rns", CLIENT_RNS_CONFIG)):
        os.makedirs(os.path.join(workdir, name))
        with open(os.path.join(workdir, name, "config"), "w") as f:
            f.write(content)

    server = ThreadingHTTPServer(("127.0.0.1", SIGNAL_PORT), MockSignalHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    client_lines, gateway_lines = [], []
    client = gateway = None
    try:
        print("* starting fake LXMF user")
        client = spawn([os.path.join(here, "test_integration.py"),
                        "--client", workdir],
                       client_lines, "client", stdin=subprocess.PIPE)
        wait_for(lambda: grep(client_lines, r"CLIENT-HASH: ([0-9a-f]+)"),
                 30, "client hash")
        client_hash = grep(client_lines, r"CLIENT-HASH: ([0-9a-f]+)").group(1)

        config_path = os.path.join(workdir, "config.toml")
        with open(config_path, "w") as f:
            f.write(f"""
[gateway]
storage = "{workdir}/gateway-storage"
rns_configdir = "{workdir}/gateway-rns"
announce_interval = 5

[signal]
rpc_url = "http://127.0.0.1:{SIGNAL_PORT}"
account = "{ACCOUNT}"

[[channels]]
name = "test"
signal_group = "{GROUP_ID}"
members = ["{client_hash}"]
""")

        print("* starting gateway")
        gateway = spawn([os.path.join(here, "gateway.py"), "-c", config_path],
                        gateway_lines, "gateway")
        wait_for(lambda: grep(gateway_lines,
                              r"Gateway LXMF address: <([0-9a-f]+)>"),
                 30, "gateway address")
        gateway_hash = grep(gateway_lines,
                            r"Gateway LXMF address: <([0-9a-f]+)>").group(1)
        wait_for(lambda: grep(gateway_lines, r"event stream"),
                 15, "gateway SSE connect")

        print("* LXMF -> Signal")
        client.stdin.write(gateway_hash + "\n")
        client.stdin.flush()
        wait_for(lambda: any(c.get("method") == "send" for c in rpc_calls),
                 60, "gateway RPC send to Signal")
        send = next(c for c in rpc_calls if c["method"] == "send")
        assert send["params"]["groupId"] == GROUP_ID, send
        assert send["params"]["account"] == ACCOUNT, send
        assert "hello from reticulum" in send["params"]["message"], send
        assert send["params"]["message"].startswith("[RNS "), send
        print("  ok: Signal group received", json.dumps(send["params"]["message"]))

        print("* Signal -> LXMF")
        sse_queue.put({"envelope": {
            "sourceNumber": "+15551234567", "sourceName": "Alice",
            "timestamp": 1723500000000,
            "dataMessage": {"message": "hello from signal",
                            "groupInfo": {"groupId": GROUP_ID}}}})
        wait_for(lambda: grep(client_lines, r"LXMF-RECV: .*hello from signal"),
                 60, "LXMF delivery to client")
        received = grep(client_lines, r"LXMF-RECV: (.*)").group(1)
        assert received == "[Signal Alice] hello from signal", received
        print("  ok: LXMF user received", json.dumps(received))

        print("\nINTEGRATION TEST PASSED")
    finally:
        for proc in (client, gateway):
            if proc:
                proc.terminate()
        server.shutdown()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--client":
        run_client(sys.argv[2])
    else:
        main()
