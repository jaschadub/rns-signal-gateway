# Changelog

## v0.3.0 — 2026-08-14

- Channels are full distribution groups: LXMF member posts relay to the
  channel's other LXMF members as well as the Signal group, with
  attachments and voice passed through natively between LXMF clients
- Optional `stamp_cost` announces an inbound proof-of-work requirement;
  outbound messages then carry tickets so members reply without the work
  (outbound stamps for stamp-requiring members were already automatic)
- Per-recipient LXMF delivery outcomes (delivered/failed) are logged

## v0.2.0 — 2026-08-14

- Configurable LXMF per-transfer acceptance limit
  (`lxmf_delivery_limit_kb`, default 8 MB); the router's 1 MB default
  silently rejected larger attachments
- Direction-aware size caps: `max_attachment_bytes` protects the
  Reticulum side only; LXMF-to-Signal passes anything under the delivery
  limit, and oversize LXMF images headed to Signal are downscaled
- Sender path requested on unvalidated LXMF signatures so first contact
  from an unseen member recovers on the next attempt
- Hardening from external review: attachment ids sanitized with
  `basename`, LXMF send fan-out bounded by a thread pool, dynamic
  membership serialized behind a lock
- `panic_on_interface_error = True` recommended for supervised
  deployments; CI actions updated to Node 24 majors

## v0.1.0 — 2026-08-13

Initial release.

- Bidirectional Signal ↔ Reticulum (LXMF) bridge via signal-cli's
  JSON-RPC/SSE API: text, images, files, and voice memos
- Channel model: one Signal group per LXMF member list
- Voice transcoding: codec2 → playable WAV into Signal; Signal voice
  notes → codec2 `FIELD_AUDIO` (`voice_to_codec2`)
- Image downscaling to WebP under global or per-channel budgets
- Linked-device support for personal Signal accounts (sync messages),
  with loop guards and 24 h dedup
- Deny-by-default ACLs, LXMF signature validation, size caps,
  `/join`/`/leave`/`/channels` self-service for open channels
- Deployment: bare Python, systemd (user/system), Docker Compose;
  multi-arch (amd64/arm64) images on GHCR
