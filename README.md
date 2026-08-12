# rns-signal-gateway

Bidirectional text bridge between [Reticulum](https://reticulum.network)
(LXMF) and [Signal](https://signal.org) via
[signal-cli](https://github.com/AsamK/signal-cli)'s JSON-RPC/SSE HTTP API.

Maps Signal groups to lists of LXMF destinations ("channels"):

```text
Signal group  <->  signal-cli  <->  gateway  <->  LXMF  <->  Reticulum users
```

- Signal group messages fan out to the channel's LXMF members as
  `[Signal Alice] text`.
- LXMF messages from members post into the Signal group as
  `[RNS a93d12fc]` + text.
- Deny by default: unmapped groups, DMs, and non-member LXMF sources are
  dropped. LXMF signatures are validated before forwarding.
- Dedup (24 h) and a prefix loop guard prevent bridge loops; oversize
  messages are dropped to protect low-bandwidth (LoRa) routes.
- Works linked to a personal Signal account: posts from your own phone
  arrive as sync messages and are bridged too.
- Images and file attachments bridge both ways (up to
  `max_attachment_bytes`, default 1 MB). Signal images arrive in Sideband
  as inline images (LXMF `FIELD_IMAGE`); other files use LXMF file
  attachments. Oversize attachments are dropped with a note in the bridged
  text, so radio-bound channels can set a small cap without losing the
  conversation.

## Requirements

- Python 3.11+ (uses stdlib `tomllib`)
- `pip install -r requirements.txt` (installs `lxmf`, which pulls in `rns`)
- signal-cli 0.14+ — use the **JVM build**. The GraalVM native build works
  for text but **cannot send attachments** (it fails with
  `Can't load library: awt` because AWT isn't bundled). The JVM build
  needs the Java version it was compiled for (0.14.7 wants Java 25); a
  Temurin JRE tarball avoids touching system Java:

  ```sh
  mkdir -p ~/.local/opt ~/.local/bin && cd ~/.local/opt
  curl -sL -o jre.tar.gz "https://api.adoptium.net/v3/binary/latest/25/ga/linux/x64/jre/hotspot/normal/eclipse"
  tar xzf jre.tar.gz && rm jre.tar.gz            # extracts jdk-25.x.y+z-jre
  curl -sLO https://github.com/AsamK/signal-cli/releases/download/v0.14.7/signal-cli-0.14.7.tar.gz
  tar xzf signal-cli-0.14.7.tar.gz && rm signal-cli-0.14.7.tar.gz
  cat > ~/.local/bin/signal-cli <<'EOF'
  #!/bin/sh
  export JAVA_HOME="$HOME/.local/opt/jdk-25.0.4+7-jre"   # adjust to extracted dir
  exec "$HOME/.local/opt/signal-cli-0.14.7/bin/signal-cli" "$@"
  EOF
  chmod +x ~/.local/bin/signal-cli
  signal-cli --version
  ```

## Setup

### 1. Link (or register) the Signal account

The gateway can run as a **linked device on an existing account** (easiest;
your phone keeps working normally) or on a **dedicated account** (cleaner
for shared/production bridges — see [Account modes](#account-modes)).

To link:

```sh
signal-cli link -n "rns-gateway"
```

This prints a `sgnl://linkdevice?...` URI and waits. Turn it into a QR code
(e.g. `pip install qrcode` then `python -c "import qrcode; q=qrcode.QRCode(border=2); q.add_data('URI_HERE'); q.print_ascii(invert=True)"`,
or `qrencode -t ansiutf8 'URI_HERE'`) and scan it from your phone:
**Signal → Settings → Linked Devices → ➕**. The command exits with
`Associated with: +1...` on success.

The first sync of a busy account can take several minutes; the daemon in
the next step handles it — you don't need to run `receive` manually.

### 2. Start the signal-cli daemon

```sh
signal-cli -a +1XXXXXXXXXX daemon --http 127.0.0.1:7583
```

Keep it bound to localhost — never expose this port. The gateway talks to
`POST /api/v1/rpc` and streams events from `GET /api/v1/events`.

### 3. Create the Signal group and get its id

Create a group in the Signal app (it can contain just you), then:

```sh
curl -s -X POST http://127.0.0.1:7583/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"listGroups","params":{"account":"+1XXXXXXXXXX"}}'
```

Copy the base64 `id` of your group from the result.

### 4. Reticulum connectivity

The gateway uses your existing `~/.reticulum` config by default. To keep it
isolated (recommended for a dedicated gateway host), point `rns_configdir`
at its own config dir, e.g.:

```ini
# live/rns/config
[reticulum]
  enable_transport = False
  share_instance = False
  panic_on_interface_error = False

[logging]
  loglevel = 4

[interfaces]
  [[Backbone]]
    type = TCPClientInterface
    enabled = True
    target_host = your.rns.node
    target_port = 4242
```

Point it at whatever gets you onto the same Reticulum network as your
users: your own transport node, a public testnet node, an RNode, etc.

### 5. Configure the gateway

```sh
cp config.example.toml config.toml
```

```toml
[gateway]
display_name = "Signal Gateway"
storage = "live/storage"          # identity + LXMF state live here
rns_configdir = "live/rns"        # omit to use ~/.reticulum
max_message_bytes = 4096          # drop anything larger
announce_interval = 360           # seconds between LXMF announces

[signal]
rpc_url = "http://127.0.0.1:7583"
account = "+1XXXXXXXXXX"
# allowed_users = ["+15551234567"]  # optional extra ACL for Signal senders

[[channels]]
name = "bridge-test"
signal_group = "BASE64_GROUP_ID_FROM_STEP_3"
members = [
    # LXMF destination hashes (shown in Sideband: Settings -> your address).
    # These receive the fan-out AND are the only ones allowed to post.
    "a91d915ce2a63cd6bc4b0f97b90e8a9c",
]
```

Repeat `[[channels]]` for each Signal group you want bridged.

### 6. Run

```sh
python3 gateway.py -c config.toml
```

On startup it logs:

```text
[Notice]  Gateway LXMF address: <02b92429dc2cf8867fc4bd3e224bdc95>
[Notice]  Connected to signal-cli event stream
```

That address is stable across restarts (the identity is persisted under
`storage`). Every bridged message is logged at Info level; drops (ACL,
oversize, bad signature) at Verbose/Warning.

## Usage

- **Reticulum → Signal**: from Sideband/MeshChat/NomadNet, start a
  conversation with the gateway's LXMF address and send a message. If your
  hash is a member of a channel, it appears in that channel's Signal group
  as `[RNS <your-hash-prefix>]`. Non-members are silently dropped.
- **Signal → Reticulum**: post in a mapped group. Every LXMF member of the
  channel receives `[Signal <name>] text`. On a linked personal account
  your own posts bridge too (via sync messages).

First-message notes:

- The gateway must have seen a member's LXMF **announce** to validate
  signatures and deliver to them. Sideband announces periodically; if a
  member has never announced on the network, delivery waits and can drop
  (logged as `No identity known for ...`).
- Sideband may need a moment to resolve the gateway's path after the first
  announce (the gateway announces on startup and every
  `announce_interval` seconds).

## Account modes

**Linked to your personal account**: your Signal identity posts the
bridged messages, so group members see them as coming from you (with the
`[RNS ...]` prefix). Your own posts bridge to LXMF; the prefix loop guard
stops the gateway's own posts from echoing back.

**Dedicated account** (own number, registered with
`signal-cli -a +1... register`): the bridge posts under its own identity.
Cleaner attribution for multi-user groups; needs a number that can receive
an SMS/voice verification once.

## Running under systemd

Adjust paths/numbers in `systemd/*.service` (signal-cli binary location,
repo path, config path, `User=`), then:

```sh
sudo cp systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now signal-cli rns-signal
```

`rns-signal.service` depends on `signal-cli.service`, so starting the
gateway brings up both.

## Security notes

The gateway is a **trusted endpoint**, not a transparent E2E bridge: LXMF
messages are decrypted at the gateway and re-encrypted by Signal, and vice
versa. Practical consequences:

- Run it on a dedicated host/VM you control.
- Keep the signal-cli HTTP port on localhost.
- List only trusted LXMF hashes as channel members (membership is also the
  send ACL).
- `storage/identity` is the gateway's private key; `~/.local/share/signal-cli`
  holds the Signal account keys. Protect both.

## Tests

```sh
python3 test_gateway.py        # unit checks: parsing, dedup, routing
python3 test_integration.py    # full bridge against a mock signal-cli
                               # + two real RNS instances over local TCP
```

## Troubleshooting

- **JVM signal-cli fails with `UnsupportedClassVersionError`** — your Java
  is too old for that build; install the matching JRE (see Requirements).
- **Sending attachments fails with `Can't load library: awt`** — you're
  running the GraalVM native build; switch to the JVM build (see
  Requirements). Receiving attachments works on both.
- **Nothing bridges, no logs** — check the daemon is on `rpc_url`
  (`curl http://127.0.0.1:7583/api/v1/check`) and the group id matches
  `listGroups` exactly (base64, including trailing `=`).
- **`Dropping LXMF message from non-member`** — the sender's hash isn't in
  any channel's `members` list (hashes are lowercase hex, no `<>`).
- **`... without validated signature`** — the gateway hasn't seen that
  member's announce yet; have the client announce (Sideband does this
  automatically) and resend.
- **Your own Signal posts don't bridge** — only on linked accounts, and
  only group posts; DMs are never bridged.

## Later

Image downscaling/recompression for radio-bound channels (attachments are
currently passed through as-is under the size cap), reactions, delivery
receipts, `/join`-style self-service membership over LXMF, per-user 1:1
mapping, and a Signal command console (`/rns-status`, `/rns-send ...`).
