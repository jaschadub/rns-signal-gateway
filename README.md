# rns-signal-gateway

Bidirectional bridge between [Reticulum](https://reticulum.network)
(LXMF) and [Signal](https://signal.org) via
[signal-cli](https://github.com/AsamK/signal-cli)'s JSON-RPC/SSE HTTP API —
text, images, files, and voice memos (including codec2 transcoding for
low-bandwidth radio links).

**Status**: text and image/file bridging verified end-to-end in both
directions (2026-08-12) on a live deployment — Sideband over a Reticulum
TCP backbone on one side, a linked personal Signal account on the other.

Maps Signal groups to lists of LXMF destinations ("channels"):

```text
Signal group  <->  signal-cli  <->  gateway  <->  LXMF  <->  Reticulum users
```

- Signal group messages fan out to the channel's LXMF members as
  `[Signal Alice] text`.
- LXMF messages from members post into the Signal group as
  `[RNS a93d12fc]` + text, and are relayed to the channel's other LXMF
  members (a channel is a full distribution group, not just a Signal
  bridge).
- Deny by default: unmapped groups, DMs, and non-member LXMF sources are
  dropped. LXMF signatures are validated before forwarding.
- Dedup (24 h) and a prefix loop guard prevent bridge loops; oversize
  messages are dropped to protect low-bandwidth (LoRa) routes.
- Works linked to a personal Signal account: posts from your own phone
  arrive as sync messages and are bridged too.
- Images and file attachments bridge both ways (up to
  `max_attachment_bytes`, default 1 MB). Signal images arrive in Sideband
  as inline images (LXMF `FIELD_IMAGE`); other files use LXMF file
  attachments. Oversize images are downscaled to fit (see Image budgets);
  other oversize attachments are dropped with a note in the bridged text,
  so radio-bound channels can set a small cap without losing the
  conversation.
- Store-and-forward: with `propagation_node` set ("auto" or a node hash),
  deliveries to offline members are handed to an LXMF propagation node
  and picked up when they reconnect, and the gateway syncs its own
  mailbox from the node after downtime.
- Voice memos bridge both ways. Reticulum → Signal: Opus voice arrives as
  a playable `.ogg`; codec2 voice is decoded to a playable `.wav` when
  `pycodec2` is installed (raw `.c2` otherwise). Signal → Reticulum: set
  `voice_to_codec2 = 2400` (or another codec2 bitrate) to turn Signal
  voice notes into tiny codec2 `FIELD_AUDIO` messages that play in
  Sideband's voice UI and fit LoRa links; without it, audio passes through
  as files. Voice transcoding needs `ffmpeg` on the PATH and
  `pip install pycodec2` (which needs libcodec2, e.g.
  `apt install libcodec2-dev`).

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
  # exit on unrecoverable interface errors so systemd/Docker restarts the
  # gateway with a fresh connection instead of running disconnected
  panic_on_interface_error = True

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

### LXMF commands

Any LXMF user can message the gateway directly with:

```text
/channels          list channels and your membership
/join <channel>    join a channel marked `open = true` in the config
/leave <channel>   leave a channel you joined
```

Self-service joins only work on channels the operator marked `open`;
everything else stays deny-by-default. Joined members persist in
`storage/members.json` and behave exactly like configured members.

### Image budgets

With Pillow installed, images that don't fit are downscaled/recompressed
to WebP instead of dropped. Set `image_max_bytes` globally or per channel
to force all bridged images under a budget — e.g. 32 kB for a channel
whose members sit behind LoRa. Without Pillow, oversize images are
dropped with a note, as before.

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

**User units (no root)** — recommended when everything lives under one
user account. Adjust the account number and repo path in
`systemd/user/*.service`, then:

```sh
cp systemd/user/*.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now signal-cli rns-signal
loginctl enable-linger        # start at boot without a login session
```

**System units** — for a dedicated service user. Adjust paths/numbers in
`systemd/*.service` (signal-cli binary location, repo path, config path,
`User=`), then:

```sh
sudo cp systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now signal-cli rns-signal
```

In both cases `rns-signal.service` depends on `signal-cli.service`, so
starting the gateway brings up both. Gateway logs go to the journal:
`journalctl --user -u rns-signal -f` (drop `--user` for system units).

## Running with Docker

`docker-compose.yml` runs both daemons as containers: signal-cli (JVM,
Java bundled in the image) and the gateway, joined by an internal network.
The signal-cli port is not published to the host.

Prebuilt multi-arch images (amd64/arm64 — Raspberry Pi works) are
published to GHCR on every push to main: `docker compose pull` fetches
them; plain `docker compose up` builds locally from the repo instead.

```sh
# 1. Config: bind-mounted from ./docker-data
mkdir -p docker-data/gateway/rns docker-data/signal
cp config.example.toml docker-data/gateway/config.toml
# edit docker-data/gateway/config.toml:
#   storage        = "/data/storage"
#   rns_configdir  = "/data/rns"
#   rpc_url        = "http://signal-cli:7583"
#   attachment_dir = "/signal/attachments"
# and put your Reticulum interface config in docker-data/gateway/rns/config

# 2. Set the Signal account
echo 'SIGNAL_ACCOUNT=+1XXXXXXXXXX' > .env

# 3. Link the Signal account (prints the sgnl:// URI to scan)
docker compose run --rm signal-cli link -n rns-gateway

# 4. Up
docker compose up -d
docker compose logs -f gateway
```

Account keys persist in `docker-data/signal`, gateway identity and LXMF
state in `docker-data/gateway/storage` — back both up, and don't commit
`docker-data` anywhere.

## Security notes

### The gateway is a trusted man-in-the-middle

Signal's end-to-end encryption ends *at the gateway*, and Reticulum's
ends there too. Every bridged message — text, image, file, voice — exists
as **plaintext in the gateway's memory** while it crosses, and parts
touch disk (signal-cli's attachment store, temporary files during
attachment sends). Nobody in a bridged conversation has end-to-end
encryption with the person on the other network; they have E2E with the
gateway, which re-encrypts on their behalf.

Concretely, whoever operates the gateway host can read, alter, inject,
or drop any bridged message, and a compromise of the host exposes all
bridged traffic from that point on — plus whatever history lives in the
Signal account data and logs. Signal participants also see bridged
messages as coming from the gateway's Signal account (with an
`[RNS ...]` prefix), not from a distinct Signal identity per Reticulum
user, and vice versa — attribution inside the message text is a
convention, not cryptography. Metadata (who talks to whom, when, how
much) is fully visible to the gateway even beyond content.

**Everyone in a bridged channel should know the bridge exists and trust
its operator** to roughly the same degree they'd trust them running a
group chat server that can read everything. If a conversation needs true
end-to-end privacy, keep it native to one network — don't bridge it.

Practical consequences for deployment:

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

## License

[MIT](LICENSE)

## Later

Reactions, delivery receipts, per-user 1:1 mapping, and a Signal command
console (`/rns-status`, `/rns-send ...`). Codec2 450PWB voice currently
decodes at 8 kHz (plays slow); special-case it if anyone uses it.
