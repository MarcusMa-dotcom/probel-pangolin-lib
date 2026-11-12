"""Requests as typed messages, plain text for every message, and the trace hooks."""

import asyncio

import pytest

from probelpangolin import (AsyncRouter, Command, CrosspointRequest, NamesRequest,
                           ProtocolImplementationRequest, TallyDumpRequest, TraceEvent, describe,
                           encode_frame, parse_message)
from probelpangolin.protocol import (ACK_FRAME, NAK_FRAME, ProtocolError, build_connect,
                                    build_dest_names_request, build_interrogate,
                                    build_protocol_implementation_request, build_source_names_request,
                                    build_tally_dump_request)
from probelpangolin.simulator import SimulatedRouter


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, 10))


async def until(predicate, timeout=3.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not predicate():
        assert loop.time() < end, "condition not reached"
        await asyncio.sleep(0.01)


@pytest.mark.parametrize("extended", [False, True])
def test_requests_parse_back_to_what_was_built(extended):
    connect = parse_message(build_connect(700, 900, 3, 2, extended=extended))
    assert connect == CrosspointRequest(connect.command, 2, 3, 900, 700)
    assert connect.is_connect and connect.extended is extended

    ask = parse_message(build_interrogate(900, 3, 2, extended=extended))
    assert (ask.matrix, ask.level, ask.dest, ask.source) == (2, 3, 900, None)
    assert not ask.is_connect

    dump = parse_message(build_tally_dump_request(3, 2, extended=extended))
    assert dump == TallyDumpRequest(dump.command, 2, 3)

    sources = parse_message(build_source_names_request(2, 3, 2, extended=extended))
    assert sources == NamesRequest(sources.command, "source", 2, 3, 12)
    dests = parse_message(build_dest_names_request(2, 0, extended=extended))
    assert dests == NamesRequest(dests.command, "dest", 2, None, 4)


def test_implementation_request_and_truncated_requests():
    assert parse_message(build_protocol_implementation_request()) == ProtocolImplementationRequest()
    with pytest.raises(ProtocolError):
        parse_message(bytes((Command.CROSSPOINT_CONNECT, 0, 0, 1)))
    with pytest.raises(ProtocolError):
        parse_message(bytes((Command.SOURCE_NAMES_REQUEST, 0, 7)))  # no such name length


def test_describe_uses_panel_numbers():
    assert describe(parse_message(bytes.fromhex("02 01 00 02 04"))) == (
        "Crosspoint connect: matrix 1, level 2, destination 3 <- source 5")
    assert describe(parse_message(bytes.fromhex("01 00 00 02"))) == (
        "Crosspoint interrogate: matrix 1, level 1, destination 3")
    assert describe(parse_message(bytes.fromhex("84 00 00 00 02 00 04"))).startswith(
        "Extended crosspoint connected: matrix 1, level 1, destination 3 <- source 5")
    assert "destinations 1 to 3 <- sources 1, 2, 3" in describe(
        parse_message(bytes((Command.CROSSPOINT_TALLY_DUMP_BYTE, 0, 3, 0, 0, 1, 2))))
    assert describe(parse_message(b"\xd0\x01")) == "Unknown 0xd0: 2 bytes, not decoded"
    assert "8 characters" in describe(parse_message(build_source_names_request()))


def test_trace_event_wire_bytes():
    payload = build_connect(5, 3)
    assert TraceEvent("tx", "message", payload).wire == encode_frame(payload)
    assert TraceEvent("rx", "ack").wire == ACK_FRAME
    assert TraceEvent("rx", "nak").wire == NAK_FRAME
    assert TraceEvent("tx", "raw", b"\x10\x02").wire == b"\x10\x02"


def test_client_trace_sees_every_frame_and_ack():
    async def scenario():
        async with SimulatedRouter(8, 8) as sim:
            events = []
            router = AsyncRouter("127.0.0.1", sim.port, query_commands=False)
            router.on_trace(events.append)
            async with router:
                await router.route(5, 3, confirm=True)
            return events

    events = run(scenario())
    assert [(e.direction, e.kind) for e in events] == [
        ("tx", "message"), ("rx", "ack"), ("rx", "message"), ("tx", "ack")]
    assert parse_message(events[0].payload).source == 5
    assert parse_message(events[2].payload).is_connected


def test_client_trace_marks_repeats_and_broken_frames():
    async def scenario():
        async with SimulatedRouter(8, 8) as sim:
            events = []
            router = AsyncRouter("127.0.0.1", sim.port, query_commands=False, ack_timeout=0.2)
            router.on_trace(events.append)
            async with router:
                sim.drop_frames = 1
                await router.route(2, 1)
                # a frame with a wrong checksum, written as it is
                await router.write_raw(bytes.fromhex("10 02 01 00 00 02 04 99 10 03"))
                await until(lambda: any(e.kind == "nak" for e in events))
            return events

    events = run(scenario())
    sent = [e for e in events if e.direction == "tx" and e.kind == "message"]
    assert [e.note for e in sent] == ["", "attempt 2 of 5"]
    assert ("tx", "raw") in [(e.direction, e.kind) for e in events]
    assert ("rx", "nak") in [(e.direction, e.kind) for e in events]


def test_send_without_check_reaches_a_router_that_did_not_list_the_command():
    async def scenario():
        async with SimulatedRouter(8, 8, extended=False) as sim:
            async with AsyncRouter("127.0.0.1", sim.port) as router:
                await router.send(bytes((Command.EXT_CROSSPOINT_INTERROGATE, 0, 0, 0, 0)), check=False)
            return sim.received[-1][0]

    assert run(scenario()) == Command.EXT_CROSSPOINT_INTERROGATE


def test_simulator_hooks_report_clients_takes_and_trace():
    async def scenario():
        sim = SimulatedRouter(8, 8, 2)
        trace, takes, clients = [], [], []
        sim.on_trace = lambda client, event: trace.append((client, event))
        sim.on_crosspoint = lambda *args: takes.append(args)
        sim.on_client = lambda client, connected: clients.append(connected)
        async with sim:
            async with AsyncRouter("127.0.0.1", sim.port, query_commands=False, levels=2) as router:
                await until(lambda: len(sim.clients) == 1)
                sim.protected.add(4)
                await router.route(5, 3, 2, confirm=True)
                await router.route(5, 4)
                await asyncio.sleep(0.05)
                address = sim.clients[0]["address"]
                messages = sim.clients[0]["messages"]
            await until(lambda: clients == [True, False])
        return sim, trace, takes, address, messages

    sim, trace, takes, address, messages = run(scenario())
    assert takes == [(2, 3, 5, address, True), (1, 4, 5, address, False)]
    assert messages == 2
    assert sim.crosspoints(2)[3] == 5 and sim.crosspoints(1)[4] == 1
    kinds = [(event.direction, event.kind) for _client, event in trace]
    assert kinds[:3] == [("rx", "message"), ("tx", "ack"), ("tx", "message")]
    assert all(client == address for client, _event in trace)


def test_simulator_set_crosspoint_checks_range_and_can_stay_silent():
    async def scenario():
        async with SimulatedRouter(8, 8) as sim:
            seen = []
            router = AsyncRouter("127.0.0.1", sim.port, query_commands=False)
            router.on_route(seen.append)
            async with router:
                sim.set_crosspoint(7, 2, announce=False)
                await asyncio.sleep(0.1)
                silent = list(seen)
                sim.set_crosspoint(6, 2)
                await until(lambda: seen)
                with pytest.raises(ValueError):
                    sim.set_crosspoint(9, 2)
            return silent, seen[0]

    silent, change = run(scenario())
    assert silent == []
    assert (change.dest, change.source) == (2, 6)
