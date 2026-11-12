import random

import pytest

from probelpangolin.protocol import (
    Ack,
    Command,
    Crosspoint,
    Frame,
    FrameDecoder,
    FrameError,
    Nak,
    NameLength,
    Names,
    ProtocolError,
    ProtocolImplementation,
    TallyDump,
    UnknownMessage,
    build_connect,
    build_dest_names_request,
    build_interrogate,
    build_source_names_request,
    build_tally_dump_request,
    encode_frame,
    needs_extended,
    parse_message,
)


def decode(data: bytes):
    return FrameDecoder().feed(data)


# -- framing ----------------------------------------------------------------


def test_encode_frame_known_vector():
    # connect source 5 -> dest 3, level 2: sum(data)=9, BTC=5, CHK=-(14)=0xF2
    assert encode_frame([0x02, 0x01, 0x00, 0x02, 0x04]).hex() == "1002020100020405f21003"


def test_encode_frame_stuffs_dle_in_data():
    frame = encode_frame([0x02, 0x00, 0x00, 0x10, 0x03])
    assert frame.startswith(bytes([0x10, 0x02, 0x02, 0x00, 0x00, 0x10, 0x10, 0x03]))
    assert decode(frame) == [Frame(bytes([0x02, 0x00, 0x00, 0x10, 0x03]))]


def test_encode_frame_stuffs_dle_in_byte_count_and_checksum():
    sixteen = bytes([0x01] * 16)  # BTC == 0x10
    frame = encode_frame(sixteen)
    assert frame[2:18] == sixteen and frame[18:20] == b"\x10\x10"
    assert decode(frame) == [Frame(sixteen)]

    # data chosen so that the checksum itself is 0x10: sum + btc == 0xF0
    payload = bytes([0xEE, 0x00])
    frame = encode_frame(payload)
    assert frame == bytes([0x10, 0x02, 0xEE, 0x00, 0x02, 0x10, 0x10, 0x10, 0x03])
    assert decode(frame) == [Frame(payload)]


def test_encode_frame_rejects_empty_and_oversized():
    with pytest.raises(ProtocolError):
        encode_frame(b"")
    with pytest.raises(ProtocolError):
        encode_frame(bytes(256))


def test_decoder_handles_stuffed_dle_followed_by_etx_value():
    # tally "dest 17 <- source 4" contains the data bytes 10 03, which appear on
    # the wire as 10 10 03 and must not be taken for the end of the message
    payload = bytes([Command.CROSSPOINT_CONNECTED, 0x00, 0x00, 0x10, 0x03])
    assert bytes([0x10, 0x10, 0x03]) in encode_frame(payload)
    (event,) = decode(encode_frame(payload))
    assert event == Frame(payload)
    assert parse_message(event.payload) == Crosspoint(Command.CROSSPOINT_CONNECTED, 1, 1, 17, 4)


def test_decoder_ack_nak_and_resync():
    stream = b"\x10\x06" + b"\x99\x98" + b"\x10\x15" + encode_frame([0x61]) + b"\x10\x06"
    decoder = FrameDecoder()
    assert decoder.feed(stream) == [Ack(), Nak(), Frame(b"\x61"), Ack()]
    assert decoder.discarded == 2


def test_decoder_byte_by_byte():
    stream = encode_frame([0x04, 0x01, 0x00, 0x02, 0x04]) + b"\x10\x06" + encode_frame([0x10, 0x10])
    decoder = FrameDecoder()
    events = []
    for byte in stream:
        events += decoder.feed(bytes([byte]))
    assert events == [Frame(bytes([0x04, 0x01, 0x00, 0x02, 0x04])), Ack(), Frame(b"\x10\x10")]


def test_decoder_reports_bad_checksum_and_recovers():
    frame = bytearray(encode_frame([0x04, 0x01, 0x02, 0x02, 0x04]))
    frame[3] ^= 0xFF
    events = decode(bytes(frame) + encode_frame([0x61]))
    assert isinstance(events[0], FrameError) and "checksum" in events[0].reason
    assert events[1] == Frame(b"\x61")


def test_decoder_reports_bad_byte_count():
    body = bytes([0x04, 0x01, 0x00, 0x02, 0x04, 0x04])  # BTC says 4, there are 5
    chk = (-sum(body)) & 0xFF
    (event,) = decode(b"\x10\x02" + body + bytes([chk]) + b"\x10\x03")
    assert isinstance(event, FrameError) and "byte count" in event.reason


def test_decoder_restart_and_overlong_frame():
    events = decode(b"\x10\x02\x01\x02" + encode_frame([0x61]))
    assert isinstance(events[0], FrameError) and events[1] == Frame(b"\x61")

    decoder = FrameDecoder()
    events = decoder.feed(b"\x10\x02" + bytes([0x01]) * 400 + encode_frame([0x61]))
    assert isinstance(events[0], FrameError) and "too long" in events[0].reason
    assert events[-1] == Frame(b"\x61")


def test_frame_roundtrip_random_payloads_and_chunking():
    rng = random.Random(8910)
    for _ in range(500):
        payloads = [
            bytes(rng.choice([0x10, 0x02, 0x03, 0x06, 0x15, rng.randrange(256)])
                  for _ in range(rng.randint(1, 60)))
            for _ in range(rng.randint(1, 4))
        ]
        stream = b"".join(encode_frame(p) for p in payloads)
        decoder = FrameDecoder()
        events = []
        pos = 0
        while pos < len(stream):
            step = rng.randint(1, 7)
            events += decoder.feed(stream[pos : pos + step])
            pos += step
        assert events == [Frame(p) for p in payloads]


# -- builders (vectors taken from the Companion module's tests) --------------


def test_build_connect_standard():
    assert list(build_connect(5, 3, 2)) == [0x02, 0x01, 0x00, 0x02, 0x04]


def test_build_connect_standard_high_numbers():
    # dest 900 -> 899 = 7*128 + 3, source 1000 -> 999 = 7*128 + 103
    assert list(build_connect(1000, 900, 16, 16)) == [0x02, 0xFF, 0x77, 0x03, 0x67]


def test_build_connect_extended():
    assert list(build_connect(1, 1, 20, extended=True)) == [0x82, 0, 19, 0, 0, 0, 0]
    assert list(build_connect(65536, 300, 4, 2, extended=True)) == [0x82, 1, 3, 0x01, 0x2B, 0xFF, 0xFF]


def test_build_interrogate():
    assert list(build_interrogate(3, 2)) == [0x01, 0x01, 0x00, 0x02]
    assert list(build_interrogate(900, 1)) == [0x01, 0x00, 0x70, 0x03]
    assert list(build_interrogate(300, 4, extended=True)) == [0x81, 0, 3, 0x01, 0x2B]


def test_build_tally_dump_and_names_requests():
    assert list(build_tally_dump_request(3, 2)) == [0x15, 0x12]
    assert list(build_tally_dump_request(3, 2, extended=True)) == [0x95, 1, 2]
    assert list(build_source_names_request(2, 1, NameLength.EIGHT)) == [0x64, 0x10, 1]
    assert list(build_dest_names_request(2, NameLength.TWELVE)) == [0x66, 0x10, 2]
    assert list(build_source_names_request(1, 1, NameLength.FOUR, extended=True)) == [0xE4, 0, 0, 0]
    assert list(build_dest_names_request(1, NameLength.EIGHT, extended=True)) == [0xE6, 0, 1]


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(source=0, dest=1),
        dict(source=1, dest=0),
        dict(source=1, dest=1, level=0),
        dict(source=1025, dest=1),
        dict(source=1, dest=1025),
        dict(source=1, dest=1, level=17),
        dict(source=1, dest=1, matrix=17),
        dict(source=65537, dest=1, extended=True),
        dict(source=1, dest=1, level=257, extended=True),
        dict(source=1.5, dest=1),
        dict(source=True, dest=1),
    ],
)
def test_build_connect_rejects_out_of_range(kwargs):
    with pytest.raises(ProtocolError):
        build_connect(**kwargs)


def test_needs_extended():
    assert not needs_extended(16, 16, 1024, 1024)
    assert needs_extended(17) and needs_extended(level=17)
    assert needs_extended(dest=1025) and needs_extended(source=1025)


def test_name_length():
    assert NameLength.from_chars(12) is NameLength.TWELVE
    assert NameLength.EIGHT.chars == 8
    with pytest.raises(ProtocolError):
        NameLength.from_chars(16)


# -- parsers ------------------------------------------------------------------


def test_parse_crosspoint_standard():
    msg = parse_message(bytes([0x04, 0x01, 0x00, 0x02, 0x04]))
    assert msg == Crosspoint(0x04, matrix=1, level=2, dest=3, source=5)
    assert msg.is_connected and not msg.extended
    assert not parse_message(bytes([0x03, 0x11, 0x00, 0x02, 0x04])).is_connected
    assert parse_message(bytes([0x03, 0x11, 0x00, 0x02, 0x04])).matrix == 2


def test_parse_crosspoint_roundtrip_all_standard_ports():
    for dest in (1, 127, 128, 129, 512, 1024):
        for source in (1, 128, 129, 1000, 1024):
            wire = bytearray(build_connect(source, dest, 7, 3))
            wire[0] = Command.CROSSPOINT_CONNECTED
            assert parse_message(wire) == Crosspoint(0x04, 3, 7, dest, source)


def test_parse_crosspoint_extended():
    msg = parse_message(bytes([0x84, 0, 3, 299 >> 8, 299 & 0xFF, 499 >> 8, 499 & 0xFF]))
    assert msg == Crosspoint(0x84, matrix=1, level=4, dest=300, source=500)
    assert msg.is_connected and msg.extended


def test_parse_tally_dump_byte():
    msg = parse_message(bytes([0x16, 0x00, 3, 9, 19, 20, 21]))
    assert isinstance(msg, TallyDump)
    assert (msg.matrix, msg.level) == (1, 1)
    assert msg.routes() == {10: 20, 11: 21, 12: 22}


def test_parse_tally_dump_word_and_extended():
    msg = parse_message(bytes([0x17, 0x12, 2, 0x01, 0x00, 0x00, 0x05, 0x03, 0xE7]))
    assert (msg.matrix, msg.level) == (2, 3)
    assert msg.routes() == {257: 6, 258: 1000}
    msg = parse_message(bytes([0x97, 1, 2, 2, 0x01, 0x00, 0x00, 0x05, 0x03, 0xE7]))
    assert (msg.matrix, msg.level) == (2, 3)
    assert msg.routes() == {257: 6, 258: 1000}


def test_parse_names():
    body = b"CAM 1   CAM 2\x00\x00\x00"
    msg = parse_message(bytes([0x6A, 0x12, 1, 0x00, 0x10, 2]) + body)
    assert msg == Names(0x6A, "source", 2, 3, 17, ("CAM 1", "CAM 2"))
    assert msg.as_dict() == {17: "CAM 1", 18: "CAM 2"}

    msg = parse_message(bytes([0x6B, 0x00, 0, 0x00, 0x00, 2]) + b"MON1MON2")
    assert msg == Names(0x6B, "dest", 1, None, 1, ("MON1", "MON2"))

    msg = parse_message(bytes([0xEA, 0, 0, 2, 0x04, 0x00, 1]) + b"SRC 1025    ")
    assert msg == Names(0xEA, "source", 1, 1, 1025, ("SRC 1025",))

    msg = parse_message(bytes([0xEB, 1, 1, 0x00, 0x02, 1]) + b"\xdcW 1    ")
    assert msg == Names(0xEB, "dest", 2, None, 3, ("ÜW 1",))


def test_parse_names_truncated_part_keeps_complete_names():
    msg = parse_message(bytes([0x6B, 0x00, 0, 0x00, 0x00, 3]) + b"MON1MON2")
    assert msg.names == ("MON1", "MON2")


def test_parse_protocol_implementation_and_unknown():
    msg = parse_message(bytes([0x62, 1, 3, 0x01, 0x02, 0x81]))
    assert isinstance(msg, ProtocolImplementation)
    assert msg.commands == (0x01, 0x02, 0x81)
    assert msg.supports(Command.EXT_CROSSPOINT_INTERROGATE) and not msg.supports(0x15)
    assert parse_message(b"\xd0\x01") == UnknownMessage(0xD0, b"\xd0\x01")


def test_parse_truncated_message_raises():
    with pytest.raises(ProtocolError):
        parse_message(bytes([0x04, 0x01, 0x00]))
    with pytest.raises(ProtocolError):
        parse_message(b"")
