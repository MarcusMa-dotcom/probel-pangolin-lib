"""What the specification SW-P-88 (Router Control Protocols, issue 4a) says, checked against the library.

The byte strings are the examples of its appendix 1 and the layouts of its
chapters 7 and 8.
"""

import asyncio

import pytest

from probelpangolin import (AsyncRouter, Command, Crosspoint, CrosspointRequest, DualControllerStatus,
                           FrameDecoder, SalvoCrosspoint, SalvoGo, SalvoInterrogate, describe,
                           encode_frame, parse_message)
from probelpangolin.protocol import (GUARANTEED_DATA, Ack, Frame, FrameError, Nak, ProtocolError,
                                    build_connect, build_dual_controller_status_request,
                                    build_interrogate, build_protocol_implementation_request,
                                    build_salvo_connect, build_salvo_go, build_salvo_interrogate)
from probelpangolin.simulator import SimulatedRouter


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, 10))


def frames(text: str):
    return FrameDecoder().feed(bytes.fromhex(text))


# -- appendix 1: the examples -------------------------------------------------

def test_example_10_1_protocol_implementation():
    assert encode_frame(build_protocol_implementation_request()) == bytes.fromhex("10 02 61 01 9E 10 03")
    reply = "10 02 62 08 09 03 04 16 20 22 53 62 63 01 02 15 1F 20 21 43 4A 61 14 9C 10 03"
    (frame,) = frames(reply)
    message = parse_message(frame.payload)
    # Two counts, then the commands the controller sends and the ones it accepts.
    assert message.transmitted == (0x03, 0x04, 0x16, 0x20, 0x22, 0x53, 0x62, 0x63)
    assert message.received == (0x01, 0x02, 0x15, 0x1F, 0x20, 0x21, 0x43, 0x4A, 0x61)
    assert message.supports(Command.CROSSPOINT_CONNECT) and not message.supports(Command.EXT_CROSSPOINT_CONNECT)
    assert describe(message) == "Protocol implementation response: the router sends 8 commands and accepts 9"


def test_example_10_2_interrogate_and_tally():
    # "Interrogate router connection at destination 1": matrix 1 and level 1 on the wire.
    (frame,) = frames("10 02 01 11 00 01 04 E9 10 03")
    assert parse_message(frame.payload) == CrosspointRequest(0x01, matrix=2, level=2, dest=2, source=None)
    assert encode_frame(build_interrogate(2, 2, 2)) == bytes.fromhex("10 02 01 11 00 01 04 E9 10 03")
    # "The Tally reply indicates source 153 routed at destination 1", counted from 0.
    (frame,) = frames("10 02 03 11 01 01 19 05 CC 10 03")
    assert parse_message(frame.payload) == Crosspoint(0x03, matrix=2, level=2, dest=2, source=154)


def test_example_10_3_connect_and_connected():
    # "Connect Matrix 2, Level 2, Destination 3 to Source 1", counted from 1.
    assert encode_frame(build_connect(1, 3, 2, 2)) == bytes.fromhex("10 02 02 11 00 02 00 05 E6 10 03")
    (frame,) = frames("10 02 04 11 00 02 00 05 E4 10 03")
    message = parse_message(frame.payload)
    assert message == Crosspoint(0x04, matrix=2, level=2, dest=3, source=1) and message.is_connected


# -- chapter 7: the data link layer -------------------------------------------

def test_ack_embedded_in_a_frame_does_not_break_it():
    # 7.2.2: "Embedded ACK/NAKs within transmitted messages are permitted in the protocol."
    events = frames("10 02 04 11 00  10 06  02 00 05 E4 10 03")
    assert [type(event) for event in events] == [Ack, Frame]
    assert events[1].payload == bytes.fromhex("04 11 00 02 00")
    events = frames("10 02 04 11 00 02  10 15  00 05 E4 10 03 10 06")
    assert [type(event) for event in events] == [Nak, Frame, Ack]


def test_decoder_state_and_guaranteed_length():
    decoder = FrameDecoder()
    assert not decoder.in_frame
    decoder.feed(bytes.fromhex("10 02 01"))
    assert decoder.in_frame
    decoder.feed(bytes.fromhex("11 00 01 04 E9 10 03"))
    assert not decoder.in_frame
    assert GUARANTEED_DATA == 128
    assert isinstance(frames("10 02 01 11 00 01 04 E8 10 03")[0], FrameError)


def test_five_attempts_by_default():
    # 7.2.2 recommends a "5 times retry" procedure for the remote device.
    async def scenario():
        async with SimulatedRouter(4, 4) as sim:
            async with AsyncRouter("127.0.0.1", sim.port, query_commands=False, ack_timeout=0.05) as router:
                sim.drop_frames = 4
                await router.route(2, 1)            # the fifth attempt gets through
                return sim.crosspoint(1)

    assert run(scenario()) == 2


# -- chapter 8: messages ------------------------------------------------------

def test_bad_source_bit_of_the_multiplier():
    # 8.1.1: bit 3 of the multiplier is the "bad" status of a TDM or digital video source.
    message = parse_message(bytes.fromhex("03 00 08 02 04"))
    assert (message.dest, message.source, message.source_bad) == (3, 5, True)
    assert describe(message).endswith("the source is marked as bad")
    assert not parse_message(bytes.fromhex("03 00 00 02 04")).source_bad
    high = parse_message(bytes.fromhex("04 00 79 7F 7F"))      # bit 3 set, destination 1023, source 255
    assert (high.dest, high.source, high.source_bad) == (1024, 256, True)


def test_salvo_messages_build_and_parse():
    connect = build_salvo_connect(source=200, dest=300, salvo=5, level=2, matrix=1)
    assert connect == bytes((120, 0x01, 0x21, 300 - 1 - 256, 200 - 1 - 128, 5))
    assert parse_message(connect) == SalvoCrosspoint(120, 1, 2, 300, 200, 5)
    acknowledge = parse_message(bytes((122, 0x01, 0x21, 43, 71, 5)))
    assert (acknowledge.dest, acknowledge.source, acknowledge.salvo) == (300, 200, 5)
    assert build_salvo_go(5) == bytes((121, 0, 5)) and build_salvo_go(5, clear=True) == bytes((121, 1, 5))
    assert parse_message(build_salvo_go(5, clear=True)) == SalvoGo(121, salvo=5, action=1)
    done = parse_message(bytes((123, 2, 5)))
    assert done.is_done and describe(done).endswith("salvo 5, nothing to set or clear")
    assert parse_message(build_salvo_interrogate(5, 3)) == SalvoInterrogate(124, salvo=5, index=3)
    tally = parse_message(bytes((125, 0x01, 0x21, 43, 71, 5, 3, 1)))
    assert (tally.index, tally.validity, tally.is_last, tally.has_data) == (3, 1, True, True)
    assert describe(tally).endswith("index 3, the last one")
    assert "no data in this salvo" in describe(parse_message(bytes((125, 0, 0, 0, 0, 5, 0, 2))))
    for bad in (lambda: build_salvo_go(128), lambda: build_salvo_connect(1, 1, -1),
                lambda: build_salvo_connect(1025, 1, 0), lambda: parse_message(bytes((120, 0, 0, 1)))):
        with pytest.raises(ProtocolError):
            bad()


def test_dual_controller_status():
    assert build_dual_controller_status_request() == b"\x08"
    assert describe(parse_message(b"\x08")) == "Dual controller status request"
    status = parse_message(b"\x09\x02")
    assert status == DualControllerStatus(0x09, b"\x02") and status.bit1 is True
    assert "bit 1) is set" in describe(status)
    assert parse_message(b"\x09\x01").bit1 is False and parse_message(b"\x09").bit1 is None


def test_salvo_with_the_simulated_router():
    async def scenario():
        events = []
        sim = SimulatedRouter(8, 8, 2)
        sim.on_salvo = lambda action, salvo, client, count: events.append((action, salvo, count))
        async with sim:
            watcher = AsyncRouter("127.0.0.1", sim.port, levels=2, fetch_tally=True)
            async with AsyncRouter("127.0.0.1", sim.port, levels=2) as router, watcher:
                assert Command.CROSSPOINT_GO_GROUP_SALVO in router.implemented_commands
                assert Command.CROSSPOINT_GO_GROUP_SALVO in router.implementation.received
                assert Command.CROSSPOINT_GO_DONE_GROUP_SALVO in router.implementation.transmitted
                sim.protected.add(4)
                await router.salvo_add(5, 1, salvo=7)
                await router.salvo_add(6, 2, salvo=7, level=[1, 2])
                await router.salvo_add(3, 4, salvo=7)
                stored = await router.salvo_read(7)
                nothing_yet = sim.crosspoint(1)
                outcome = await router.salvo_go(7)
                await asyncio.sleep(0.05)
                again = await router.salvo_go(7)
                await router.salvo_add(8, 8, salvo=9)
                cleared = await router.salvo_go(9, clear=True)
                return (stored, nothing_yet, outcome, again, cleared, sim.crosspoints(1), sim.crosspoints(2),
                        router.routes, watcher.routes, events, await router.salvo_read(7), sim.salvos)

    stored, nothing_yet, outcome, again, cleared, level1, level2, routes, watched, events, empty, left = run(scenario())
    assert stored == [(1, 1, 5), (1, 2, 6), (2, 2, 6), (1, 4, 3)]
    assert nothing_yet == 1                       # stored, not switched
    assert (outcome, again, cleared) == (0, 2, 1)
    assert (level1[1], level1[2], level2[2]) == (5, 6, 6)
    assert level1[4] == 1                         # the protected destination kept its source
    assert level1[8] == 1                         # the cleared salvo switched nothing
    # No Connected messages are sent for a salvo. Both clients follow the acknowledges.
    assert routes[1][1] == 5 and routes[2][2] == 6
    assert watched[1][1] == 5 and watched[1][2] == 6 and watched[2][2] == 6
    assert events == [("stored", 7, 1)] * 4 + [("set", 7, 4), ("empty", 7, 0), ("stored", 9, 1), ("cleared", 9, 1)]
    assert empty == [] and left == {}


def test_simulated_router_reports_its_commands_as_the_specification_lays_them_out():
    async def scenario():
        async with SimulatedRouter(4, 4, extended=False) as sim:
            async with AsyncRouter("127.0.0.1", sim.port) as router:
                return router.implementation

    answer = run(scenario())
    assert answer.header == bytes((len(answer.transmitted), len(answer.received)))
    assert Command.CROSSPOINT_CONNECTED in answer.transmitted and Command.CROSSPOINT_CONNECT in answer.received
    assert not any(code >= 0x80 for code in answer.commands)
