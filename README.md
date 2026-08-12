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
  `[RNS a93d12fc]\ntext`.
- Deny by default: unmapped groups, DMs, and non-member LXMF sources are
  dropped. LXMF signatures are validated before forwarding.
- Dedup (24 h) prevents bridge loops; oversize messages are dropped to
  protect low-bandwidth (LoRa) routes.

## Setup

1. Install signal-cli and register or link a dedicated account, then run its
   daemon (see `systemd/signal-cli.service`):

   ```sh
   signal-cli -a +1XXXXXXXXXX daemon --http 127.0.0.1:7583
   ```

2. Install the gateway:

   ```sh
   pip install -r requirements.txt
   cp config.example.toml config.toml   # then edit
   ```

   Get the Signal group id with `signal-cli -a +1XXXXXXXXXX listGroups`.
   LXMF member hashes are the destination hashes shown in Sideband/MeshChat.

3. Run it:

   ```sh
   python3 gateway.py -c config.toml
   ```

   The gateway logs its LXMF address on startup and announces it
   periodically. Reticulum uses your existing `~/.reticulum` config.

Systemd units for both daemons are in `systemd/`.

## Security notes

The gateway is a trusted endpoint, not a transparent E2E bridge: LXMF
messages are decrypted at the gateway and re-encrypted by Signal (and vice
versa). Run it on a dedicated host/VM, keep the signal-cli HTTP port bound
to localhost, and list only trusted LXMF hashes as channel members.

## Tests

```sh
python3 test_gateway.py
```

## Later

Signal attachments, LXMF file transfer, reactions, delivery receipts,
`/join`-style self-service membership over LXMF, per-user 1:1 mapping, and a
Signal command console (`/rns-status`, `/rns-send ...`).
