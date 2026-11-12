"""SW-P-02, the General Switcher Communication Protocol: wire format, client, simulated router."""

import asyncio

import pytest

from probelpangolin import RouteRejectedError, UnsupportedCommandError
from probelpangolin.errors import ResponseTimeoutError
from probelpangolin.protocol import Frame, FrameError, ProtocolError
from probelpangolin.swp02 import (AsyncRouter, Command, Go, MessageDecoder, Router, SimulatedRouter, Switch,
                                 Unknown, build_connect, build_connect_on_go, build_go, build_interrogate,
                                 describe, encode_message, parse_message)

FAST = {"response_timeout": 0.3, "reconnect_delay": 0.05, "keepalive": None}


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, 10))


async def until(predicate, timeout=3.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not predicate():
        assert loop.time() < end, "condition not reached"
        await asyncio.sleep(0.01)


# -- wire format ---------------------------------------------------------------

def test_message_is_start_command_message_and_a_seven_bit_checksum():
    # CONNECT source 5 to destination 3: 02 00 02 04, sum 8, two's complement in 7 bits 0x78.
    assert build_connect(5, 3) == bytes((2, 0x00, 2, 4))
    assert encode_message(build_connect(5, 3)) == bytes.fromhex("FF 02 00 02 04 78")
    assert encode_message(build_interrogate(3)) == bytes.fromhex("FF 01 00 02 7D")
    assert encode_message(build_go()) == bytes.fromhex("FF 06 00 7A")
    assert encode_message(build_go(clear=True)) == bytes.fromhex("FF 06 01 79")
    for frame in (encode_message(build_connect(1023, 1024)), encode_message(build_connect_on_go(200, 300))):
        assert frame[0] == 0xFF and all(byte < 0x80 for byte in frame[1:])    # top bit is 0 everywhere
        assert sum(frame[1:]) & 0x7F == 0


def test_multiplier_carries_the_high_bits():
    payload = build_connect(source=200, dest=300)
    assert payload == bytes((2, (2 << 4) | 1, 299 - 256, 199 - 128))
    assert parse_message(payload) == Switch(Command.CONNECT, dest=300, source=200)
    assert parse_message(build_interrogate(1024)) == Switch(Command.INTERROGATE, 1024, None)
    flagged = parse_message(bytes((3, 0x08, 2, 4)))
    assert (flagged.dest, flagged.source, flagged.flag) == (3, 5, True)
    for bad in (lambda: build_connect(1024, 1), lambda: build_connect(1, 1025), lambda: build_interrogate(0),
                lambda: parse_message(bytes((2, 0, 1))), lambda: encode_message(b"")):
        with pytest.raises(ProtocolError):
            bad()


def test_reserved_source_means_destination_out_of_range():
    tally = parse_message(bytes((3, 0x07, 9, 0x7F)))            # source 1023 on the wire
    assert tally.out_of_range
    assert describe(tally) == "Tally: destination 10 is out of range at the router"


def test_describe():
    assert describe(parse_message(build_connect(5, 3))) == "Connect: destination 3 <- source 5"
    assert describe(parse_message(build_interrogate(3))) == "Interrogate: destination 3"
    assert describe(parse_message(bytes((4, 0, 2, 4)))) == "Connected: destination 3 <- source 5"
    assert describe(parse_message(build_connect_on_go(5, 3))) == "Connect on go: destination 3 <- source 5"
    assert describe(parse_message(build_go(clear=True))) == "Go: clear the stored crosspoints"
    assert describe(Go(Command.GO_DONE_ACKNOWLEDGE, 2)) == "Go done acknowledge: nothing to set or clear"
    assert "bit 3 is set" in describe(parse_message(bytes((3, 0x08, 2, 4))))
    assert describe(parse_message(bytes((7,)))) == "Status request: 1 bytes, not decoded"
    assert parse_message(bytes((66, 1, 2, 3, 4))) == Unknown(66, bytes((66, 1, 2, 3, 4)))


def test_decoder_takes_the_length_from_the_command():
    stream = encode_message(build_interrogate(3)) + encode_message(bytes((3, 0, 2, 4))) + encode_message(build_go())
    decoder = MessageDecoder()
    events = []
    for byte in stream:                                  # byte by byte, as from a slow line
        events += decoder.feed(bytes((byte,)))
    assert [event.payload for event in events] == [build_interrogate(3), bytes((3, 0, 2, 4)), build_go()]
    assert not decoder.in_frame and decoder.discarded == 0


def test_decoder_reports_what_it_cannot_read():
    decoder = MessageDecoder()
    events = decoder.feed(bytes.fromhex("11 22 FF 02 00 02 04 77"))        # junk, then a wrong checksum
    assert decoder.discarded == 2
    assert isinstance(events[0], FrameError) and events[0].reason == "checksum mismatch"
    events = decoder.feed(bytes.fromhex("FF 02 00 FF 01 00 02 7D"))         # cut short by the next start
    assert events[0] == FrameError("message cut short", bytes.fromhex("02 00"))
    assert events[1] == Frame(build_interrogate(3))
    # A command without a known length is collected up to the next start of message.
    events = decoder.feed(bytes.fromhex("FF 07 79 FF 01 00 02 7D"))
    assert isinstance(events[0], FrameError) and "STATUS_REQUEST" in events[0].reason
    assert events[0].raw == bytes.fromhex("07 79") and events[1] == Frame(build_interrogate(3))
    assert decoder.feed(bytes.fromhex("FF 13 00")) == []
    assert decoder.flush()[0].raw == bytes.fromhex("13 00") and not decoder.in_frame


# -- client and simulated router ---------------------------------------------------

def test_route_interrogate_and_poll():
    async def scenario():
        async with SimulatedRouter(8, 8) as sim:
            trace = []
            router = AsyncRouter("127.0.0.1", sim.port, dests=8, fetch_tally=True, **FAST)
            router.on_trace(trace.append)
            async with router:
                polled = dict(router.routes[1])
                await router.route(5, 3)
                await until(lambda: sim.crosspoint(3) == 5)
                await router.route(7, 3, confirm=True)
                asked = await router.get_route(3)
                sim.set_crosspoint(2, 6)                           # switched at the router
                await until(lambda: router.source_of(6) == 2)
                with pytest.raises(ProtocolError, match="out of range"):
                    await router.get_route(9)
                return polled, asked, router.routes[1], sim.received, trace

    polled, asked, routes, received, trace = run(scenario())
    assert polled == {dest: 1 for dest in range(1, 9)}
    assert asked == 7 and routes[3] == 7 and routes[6] == 2
    assert received[8] == build_connect(5, 3)
    assert all(event.protocol == "sw-p-02" for event in trace)
    assert trace[0].wire == encode_message(build_interrogate(1))
    assert {(event.direction, event.kind) for event in trace} == {("tx", "message"), ("rx", "message")}


def test_confirm_detects_protected_destination_and_silence():
    async def scenario():
        async with SimulatedRouter(8, 8) as sim:
            async with AsyncRouter("127.0.0.1", sim.port, **FAST) as router:
                sim.protected.add(4)
                with pytest.raises(RouteRejectedError) as rejected:
                    await router.route(6, 4, confirm=True)
                sim.drop_frames = 1
                with pytest.raises(ResponseTimeoutError):
                    await router.route(6, 5, confirm=True)
                await router.route(6, 5, confirm=True)             # the link is fine afterwards
                return rejected.value, sim.crosspoint(4), sim.crosspoint(5)

    error, kept, taken = run(scenario())
    assert (error.dest, error.requested, error.actual) == (4, 6, 1)
    assert (kept, taken) == (1, 6)


def test_connect_on_go():
    async def scenario():
        events = []
        sim = SimulatedRouter(8, 8)
        sim.on_salvo = lambda action, salvo, client, count: events.append((action, count))
        async with sim:
            watcher = AsyncRouter("127.0.0.1", sim.port, dests=8, fetch_tally=True, **FAST)
            async with AsyncRouter("127.0.0.1", sim.port, **FAST) as router, watcher:
                await router.salvo_add(5, 1)
                await router.salvo_add(6, 2)
                stored, before = sim.salvos, sim.crosspoint(1)
                outcome = await router.salvo_go()
                # No CONNECTED after a GO: both clients interrogate.
                await until(lambda: router.source_of(2) == 6 and watcher.source_of(2) == 6)
                again = await router.salvo_go()
                await router.salvo_add(8, 8)
                cleared = await router.salvo_go(clear=True)
                return stored, before, outcome, again, cleared, sim.crosspoints(), router.routes[1], events

    stored, before, outcome, again, cleared, state, routes, events = run(scenario())
    assert stored == {0: [(1, 1, 5), (1, 2, 6)]} and before == 1
    assert (outcome, again, cleared) == (0, 2, 1)
    assert (state[1], state[2], state[8]) == (5, 6, 1)
    assert routes[1] == 5 and routes[2] == 6
    assert events == [("stored", 1), ("stored", 1), ("set", 2), ("empty", 0), ("stored", 1), ("cleared", 1)]


def test_what_the_protocol_does_not_have():
    async def scenario():
        async with SimulatedRouter(4, 4) as sim:
            async with AsyncRouter("127.0.0.1", sim.port, **FAST) as router:
                for call in (router.read_names, router.query_commands, router.tally_dump,
                             router.salvo_read, router.dual_controller_status):
                    with pytest.raises(UnsupportedCommandError):
                        await call()
                assert router.implemented_commands is None
        with pytest.raises(ValueError):
            SimulatedRouter(1024, 4)

    run(scenario())


def test_keepalive_notices_a_silent_router_and_reconnects():
    async def scenario():
        async with SimulatedRouter(4, 4) as sim:
            options = {**FAST, "keepalive": 0.1, "response_timeout": 0.2}
            async with AsyncRouter("127.0.0.1", sim.port, **options) as router:
                states = []
                router.on_connection(states.append)
                await asyncio.sleep(0.25)
                sim.drop_frames = 1
                await until(lambda: states[:2] == [False, True], timeout=5)
                await router.wait_connected(5)
                return await router.get_route(1), len(sim.received)

    source, probes = run(scenario())
    assert source == 1 and probes >= 2


def test_blocking_router():
    async def serve(ready, stop):
        async with SimulatedRouter(8, 8) as sim:
            ready.append(sim.port)
            while not stop:
                await asyncio.sleep(0.02)

    import threading
    ready, stop = [], []
    thread = threading.Thread(target=lambda: asyncio.run(serve(ready, stop)), daemon=True)
    thread.start()
    while not ready:
        pass
    try:
        with Router("127.0.0.1", ready[0], dests=8, **FAST) as router:
            router.route(4, 2, confirm=True)
            assert router.get_route(2) == 4
            assert router.tally_dump()[2] == 4
            router.salvo_add(3, 1)
            assert router.salvo_go() == 0
    finally:
        stop.append(True)
        thread.join(3)
