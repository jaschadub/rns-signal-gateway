#!/usr/bin/env python3
"""Self-check for gateway routing/dedup/parsing logic. Run: python3 test_gateway.py"""

from gateway import (
                     Dedup,
                     channel_for_group,
                     channel_for_member,
                     message_id,
                     parse_signal_event,
)

# dedup: new, duplicate, expiry
d = Dedup(ttl=10)
assert d.check("a", now=0) is True
assert d.check("a", now=5) is False
assert d.check("a", now=20) is True  # expired, seen again

# message_id: stable and distinct
assert message_id("s", 1, "x") == message_id("s", 1, "x")
assert message_id("s", 1, "x") != message_id("s", 2, "x")

# parse_signal_event
ev = {"envelope": {"sourceNumber": "+1555", "sourceName": "Alice",
                   "timestamp": 42,
                   "dataMessage": {"message": "hi",
                                   "groupInfo": {"groupId": "G1"}}}}
assert parse_signal_event(ev, "+1000") == ("+1555", "Alice", "G1", "hi", 42)
assert parse_signal_event(ev, "+1555") is None          # own account ignored
assert parse_signal_event({"envelope": {}}, "+1000") is None  # no text
receipt = {"envelope": {"sourceNumber": "+1555", "receiptMessage": {}}}
assert parse_signal_event(receipt, "+1000") is None     # non-text event
dm = {"envelope": {"sourceNumber": "+1555",
                   "dataMessage": {"message": "psst"}}}
assert parse_signal_event(dm, "+1000") == ("+1555", "+1555", None, "psst", None)

# syncMessage: own-device posts bridge (linked personal account)...
sync = {"envelope": {"sourceNumber": "+1000", "sourceName": "Me",
                     "timestamp": 43,
                     "syncMessage": {"sentMessage": {
                         "message": "from my phone",
                         "groupInfo": {"groupId": "G1"}}}}}
assert parse_signal_event(sync, "+1000") == ("+1000", "Me", "G1",
                                             "from my phone", 43)
# ...but the gateway's own bridged posts never loop back
echo = {"envelope": {"sourceNumber": "+1000",
                     "syncMessage": {"sentMessage": {
                         "message": "[RNS aabbccdd]\nhi",
                         "groupInfo": {"groupId": "G1"}}}}}
assert parse_signal_event(echo, "+1000") is None

# channel routing (deny by default)
cfg = {"channels": [{"name": "c1", "signal_group": "G1",
                     "members": ["aa11", "bb22"]}]}
assert channel_for_group(cfg, "G1")["name"] == "c1"
assert channel_for_group(cfg, "G9") is None
assert channel_for_group(cfg, None) is None             # DMs never match
assert channel_for_member(cfg, "aa11")["name"] == "c1"
assert channel_for_member(cfg, "cc33") is None

print("all checks passed")
