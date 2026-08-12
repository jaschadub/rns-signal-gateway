#!/usr/bin/env python3
"""Self-check for gateway routing/dedup/parsing logic. Run: python3 test_gateway.py"""

import LXMF

from gateway import (
                     Dedup,
                     attachment_fields,
                     attachment_sig,
                     channel_for_group,
                     channel_for_member,
                     lxmf_attachments,
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
assert parse_signal_event(ev, "+1000") == ("+1555", "Alice", "G1", "hi", 42,
                                           [])
assert parse_signal_event(ev, "+1555") is None          # own account ignored
assert parse_signal_event({"envelope": {}}, "+1000") is None  # no content
receipt = {"envelope": {"sourceNumber": "+1555", "receiptMessage": {}}}
assert parse_signal_event(receipt, "+1000") is None     # non-text event
dm = {"envelope": {"sourceNumber": "+1555",
                   "dataMessage": {"message": "psst"}}}
assert parse_signal_event(dm, "+1000") == ("+1555", "+1555", None, "psst",
                                           None, [])

# attachment-only message (no text) still bridges
att = {"contentType": "image/png", "filename": "pic.png", "id": "abc.png"}
pic = {"envelope": {"sourceNumber": "+1555", "timestamp": 44,
                    "dataMessage": {"attachments": [att],
                                    "groupInfo": {"groupId": "G1"}}}}
assert parse_signal_event(pic, "+1000") == ("+1555", "+1555", "G1", "", 44,
                                            [att])

# syncMessage: own-device posts bridge (linked personal account)...
sync = {"envelope": {"sourceNumber": "+1000", "sourceName": "Me",
                     "timestamp": 43,
                     "syncMessage": {"sentMessage": {
                         "message": "from my phone",
                         "groupInfo": {"groupId": "G1"}}}}}
assert parse_signal_event(sync, "+1000") == ("+1000", "Me", "G1",
                                             "from my phone", 43, [])
# ...but the gateway's own bridged posts never loop back
echo = {"envelope": {"sourceNumber": "+1000",
                     "syncMessage": {"sentMessage": {
                         "message": "[RNS aabbccdd]\nhi",
                         "groupInfo": {"groupId": "G1"}}}}}
assert parse_signal_event(echo, "+1000") is None

# attachment_fields: first image inline, rest as files
fields = attachment_fields([("a.png", "image/png", b"img1"),
                            ("b.jpg", "image/jpeg", b"img2"),
                            ("notes.txt", "text/plain", b"doc")])
assert fields[LXMF.FIELD_IMAGE] == ["png", b"img1"]
assert fields[LXMF.FIELD_FILE_ATTACHMENTS] == [["b.jpg", b"img2"],
                                               ["notes.txt", b"doc"]]
assert attachment_fields([]) == {}

# lxmf_attachments: extraction and size cap
kept, notes = lxmf_attachments({LXMF.FIELD_IMAGE: ["webp", b"12345"],
                                LXMF.FIELD_FILE_ATTACHMENTS:
                                    [["big.bin", b"123456789"]]}, 5)
assert kept == [("image.webp", b"12345")]
assert notes == ["[dropped big.bin: 9 B over 5 B limit]"]
assert lxmf_attachments(None, 5) == ([], [])
assert lxmf_attachments({LXMF.FIELD_FILE_ATTACHMENTS:
                         [["../evil", b"x"]]}, 5)[0] == [("evil", b"x")]

# attachment_sig: stable, order-sensitive, works on 2- and 3-tuples
assert attachment_sig([("a", b"xx")]) == "|a:2"
assert attachment_sig([("a", "ct", b"xx")]) == "|a:2"
assert attachment_sig([]) == ""

# channel routing (deny by default)
cfg = {"channels": [{"name": "c1", "signal_group": "G1",
                     "members": ["aa11", "bb22"]}]}
assert channel_for_group(cfg, "G1")["name"] == "c1"
assert channel_for_group(cfg, "G9") is None
assert channel_for_group(cfg, None) is None             # DMs never match
assert channel_for_member(cfg, "aa11")["name"] == "c1"
assert channel_for_member(cfg, "cc33") is None

print("all checks passed")
