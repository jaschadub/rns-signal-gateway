#!/usr/bin/env python3
"""Self-check for gateway routing/dedup/parsing logic. Run: python3 test_gateway.py"""

import shutil

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

# attachment_fields: first image inline, rest as files, oversize dropped
fields, notes = attachment_fields([("a.png", "image/png", b"img1"),
                                   ("b.jpg", "image/jpeg", b"img2"),
                                   ("notes.txt", "text/plain", b"doc"),
                                   ("big.bin", "application/x", b"x" * 20)],
                                  10)
assert fields[LXMF.FIELD_IMAGE] == ["png", b"img1"]
assert fields[LXMF.FIELD_FILE_ATTACHMENTS] == [["b.jpg", b"img2"],
                                               ["notes.txt", b"doc"]]
assert notes == ["[dropped big.bin: 20 B over 10 B limit]"]
assert attachment_fields([], 10) == ({}, [])

# image downscaling: a big image shrinks to fit instead of dropping
try:
    from PIL import Image
except ImportError:
    Image = None
if Image:
    import io as _io
    import os as _os
    buf = _io.BytesIO()
    Image.frombytes("RGB", (600, 600), _os.urandom(600 * 600 * 3)).save(
        buf, "PNG")
    big_png = buf.getvalue()
    assert len(big_png) > 10000
    fields, notes = attachment_fields(
        [("photo.png", "image/png", big_png)], 1000000,
        image_max_bytes=10000)
    fmt, data = fields[LXMF.FIELD_IMAGE]
    assert fmt == "webp" and len(data) <= 10000, len(data)
    assert notes == []
    # undecodable "image" over the cap still drops with a note
    _, notes = attachment_fields([("x.png", "image/png", b"z" * 30)], 10)
    assert "dropped x.png" in notes[0]
    # oversize LXMF image toward Signal shrinks instead of dropping
    kept, notes = lxmf_attachments({LXMF.FIELD_IMAGE: ["png", big_png]},
                                   10000)
    assert notes == [] and kept[0][0] == "image.webp"
    assert len(kept[0][1]) <= 10000
else:
    print("skipping image tests (no Pillow)")

# lxmf_attachments: extraction and size cap
kept, notes = lxmf_attachments({LXMF.FIELD_IMAGE: ["webp", b"12345"],
                                LXMF.FIELD_FILE_ATTACHMENTS:
                                    [["big.bin", b"123456789"]]}, 5)
assert kept == [("image.webp", b"12345")]
assert notes == ["[dropped big.bin: 9 B over 5 B limit]"]

# voice memos: opus -> playable ogg, undecodable codec2 -> raw .c2
kept, _ = lxmf_attachments({LXMF.FIELD_AUDIO: [LXMF.AM_OPUS_OGG, b"OGGDATA"]},
                           100)
assert kept == [("voice.ogg", b"OGGDATA")]
kept, _ = lxmf_attachments({LXMF.FIELD_AUDIO: [LXMF.AM_CODEC2_1200, b"C2"]},
                           100)
assert kept == [("voice.c2", b"C2")]  # under one frame: passthrough

# codec2 transcoding, both directions (optional: pycodec2 + ffmpeg)
try:
    import pycodec2
except ImportError:
    pycodec2 = None
if pycodec2:
    from gateway import audio_to_codec2, codec2_to_wav
    frame = bytes(pycodec2.Codec2(1200).bytes_per_frame())
    wav = codec2_to_wav(LXMF.AM_CODEC2_1200, frame)
    assert wav is not None and wav[:4] == b"RIFF"
    kept, _ = lxmf_attachments(
        {LXMF.FIELD_AUDIO: [LXMF.AM_CODEC2_1200, frame]}, 100000)
    assert kept[0][0] == "voice.wav"
    if shutil.which("ffmpeg"):
        assert audio_to_codec2(wav, 2400)
        fields, _ = attachment_fields([("v.m4a", "audio/mp4", wav)], 100000,
                                      voice_codec2_bitrate=2400)
        assert fields[LXMF.FIELD_AUDIO][0] == LXMF.AM_CODEC2_2400
    else:
        print("skipping encode test (no ffmpeg)")
else:
    print("skipping codec2 tests (no pycodec2)")

# dynamic membership + /commands
cfg2 = {"channels": [{"name": "camp", "signal_group": "G2",
                      "members": ["aa11"], "open": True},
                     {"name": "ops", "signal_group": "G3",
                      "members": ["aa11"]}]}
dyn = {}
from gateway import command_reply

assert command_reply(cfg2, dyn, "bb22", "/join camp") == ("Joined camp", True)
assert dyn == {"camp": ["bb22"]}
assert channel_for_member(cfg2, "bb22", dyn)["name"] == "camp"
assert channel_for_member(cfg2, "bb22") is None          # config-only view
assert command_reply(cfg2, dyn, "bb22", "/join camp") == \
    ("Already a member of camp", False)
assert command_reply(cfg2, dyn, "bb22", "/join ops") == \
    ("Channel ops is closed; ask the operator", False)
assert command_reply(cfg2, dyn, "bb22", "/join nope") == \
    ("No such channel: nope", False)
assert command_reply(cfg2, dyn, "bb22", "/channels") == \
    ("camp (member)\nops (closed)", False)
assert command_reply(cfg2, dyn, "bb22", "/leave camp") == ("Left camp", True)
assert dyn == {"camp": []}
assert command_reply(cfg2, dyn, "aa11", "/leave ops")[1] is False  # static
assert command_reply(cfg2, dyn, "bb22", "/help")[0].startswith("Commands:")

# propagation node picker: nearest active node wins, inactive ignored
import RNS.vendor.umsgpack as msgpack

from gateway import PropagationNodePicker

picks = []
hops = {b"far": 5, b"near": 2, b"nearer": 1}
picker = PropagationNodePicker(picks.append, hops=lambda h: hops[h])
active = msgpack.packb([True, 0])
picker.received_announce(b"far", None, active)
picker.received_announce(b"near", None, active)
picker.received_announce(b"far", None, active)          # not better
picker.received_announce(b"nearer", None, msgpack.packb([False, 0]))
picker.received_announce(b"bogus", None, b"\xff")       # unparseable
assert picks == [b"far", b"near"]
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
