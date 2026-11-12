"""Client tests against the simulated router (real TCP on localhost)."""

import asyncio
import contextlib
import threading

import pytest

from probelpangolin import (
    AckTimeoutError,
    AsyncRouter,
    Command,
    NakError,
    NotConnectedError,
    ResponseTimeoutError,
    RouteChange,
    Router,
    RouteRejectedError,
    UnknownNameError,
    UnsupportedCommandError,
)
from probelpangolin.protocol import ProtocolError
from probelpangolin.simulator import SimulatedRouter

FAST = dict(ack_timeout=0.2, response_timeout=0.5, keepalive=None, reconnect_delay=0.05)


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, 20))


def client(sim, **options):
    return AsyncRouter("127.0.0.1", sim.port, **{**FAST, **options})


async def until(predicate, timeout=3.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.01)


def test_route_and_interrogate_standard():
    async def main():
        async with SimulatedRouter(16, 16, levels=2, extended=False) as sim, client(sim, levels=2) as r:
            assert Command.EXT_CROSSPOINT_CONNECT not in r.implemented_commands
            await r.route(5, 3)
            assert sim.crosspoint(3) == 5
            assert sim.received[-1] == bytes([0x02, 0x00, 0x00, 0x02, 0x04])
            await r.route(7, 3, level=r.all_levels, confirm=True)
            assert (sim.crosspoint(3, 1), sim.crosspoint(3, 2)) == (7, 7)
            assert await r.get_route(3) == 7
            assert await r.get_routes(3) == {1: 7, 2: 7}
            assert r.source_of(3, 2) == 7 and r.destinations_of(7) == [3]
            # every message from the router was acknowledged
            await until(lambda: sim.acks_received >= 6)
            assert sim.naks_received == 0

    run(main())


def test_auto_extended_when_router_reports_it():
    async def main():
        async with SimulatedRouter(16, 16) as sim, client(sim) as r:
            assert Command.EXT_CROSSPOINT_CONNECT in r.implemented_commands
            await r.route(2, 4, confirm=True)
            assert sim.received[-1] == bytes([0x82, 0, 0, 0, 3, 0, 1])
            assert sim.crosspoint(4) == 2

    run(main())


def test_large_router_uses_extended_commands():
    async def main():
        sim = SimulatedRouter(2000, 1500, levels=20)
        async with sim, client(sim, levels=20, query_commands=False) as r:
            assert r.implemented_commands is None
            await r.route(1999, 1400, level=18, confirm=True)
            assert sim.crosspoint(1400, 18) == 1999
            assert await r.get_route(1400, 18) == 1999
            # small numbers still go out as standard commands when nothing is known
            await r.route(3, 2, confirm=True)
            assert sim.received[-1][0] == Command.CROSSPOINT_CONNECT
            table = await r.tally_dump(18, settle=0.1)
            assert len(table) == 1500 and table[1400] == 1999 and table[1] == 1

    run(main())


def test_extended_false_refuses_large_numbers():
    async def main():
        async with SimulatedRouter(16, 16) as sim, client(sim, extended=False) as r:
            with pytest.raises(ProtocolError):
                await r.route(2000, 1)
            await r.route(2, 1, confirm=True)
            assert sim.received[-1][0] == Command.CROSSPOINT_CONNECT

    run(main())
    with pytest.raises(ValueError):
        AsyncRouter("127.0.0.1", levels=20, extended=False)


def test_tally_dump_byte_and_word_formats():
    async def main():
        for sources, dests, command in ((16, 150, 0x15), (300, 300, 0x15)):
            sim = SimulatedRouter(sources, dests, levels=2, extended=False)
            async with sim, client(sim, levels=2) as r:
                sim.set_crosspoint(9, dests, 2)
                dump = await r.tally_dump_all(settle=0.1)
                assert sim.received[-1][0] == command
                assert set(dump) == {1, 2}
                assert len(dump[1]) == dests and set(dump[1].values()) == {1}
                assert dump[2][dests] == 9
                assert r.routes == dump

    run(main())


def test_fetch_on_connect_names_and_name_routing():
    async def main():
        sim = SimulatedRouter(40, 20)
        sim.source_names[4] = "CAM 5"
        sim.dest_names[2] = "MON 3"
        sim.dest_names[5] = "mon 3"
        sim.dest_names[6] = "PGM"
        sim.dest_names[7] = "PGM"
        async with sim, client(sim, fetch_names=True, fetch_tally=True) as r:
            assert len(r.source_names) == 40 and len(r.dest_names) == 20
            assert r.source_name(5) == "CAM 5" and r.dest_name(20) == "DST 20"
            assert len(r.routes[1]) == 20
            await r.route("CAM 5", "MON 3", confirm=True)
            assert sim.crosspoint(3) == 5
            assert r.find_dest("dst 20") == 20  # case-insensitive fallback
            assert await r.get_route("MON 3") == 5
            with pytest.raises(UnknownNameError):
                r.find_source("nope")
            with pytest.raises(UnknownNameError, match="ambiguous"):
                await r.route(1, "PGM")

    run(main())


@pytest.mark.parametrize("chars", [4, 8, 12])
def test_name_lengths_standard_commands(chars):
    async def main():
        sim = SimulatedRouter(70, 70, extended=False)
        sim.source_names[69] = "ABCDEFGHIJKLMNOP"
        async with sim, client(sim, name_length=chars) as r:
            sources, dests = await r.read_names(settle=0.1)
            assert len(sources) == 70 and len(dests) == 70
            assert sources[70] == "ABCDEFGHIJKLMNOP"[:chars]
            assert dests[1] == "DST 1"[:chars].strip()

    run(main())


def test_names_without_loading_raise_helpful_error():
    async def main():
        async with SimulatedRouter() as sim, client(sim) as r:
            with pytest.raises(UnknownNameError, match="read_names"):
                await r.route("CAM 1", 1)

    run(main())


def test_confirm_detects_protected_destination():
    async def main():
        async with SimulatedRouter() as sim, client(sim) as r:
            await r.route(4, 2, confirm=True)
            sim.protected.add(2)
            with pytest.raises(RouteRejectedError) as info:
                await r.route(9, 2, confirm=True)
            assert (info.value.requested, info.value.actual) == (9, 4)
            assert sim.crosspoint(2) == 4

    run(main())


def test_confirm_times_out_when_router_stays_silent():
    async def main():
        async with SimulatedRouter() as sim, client(sim) as r:
            with pytest.raises(ResponseTimeoutError):
                await r.route(99, 2, confirm=True)  # source does not exist: sim ACKs but ignores
            with pytest.raises(ResponseTimeoutError):
                await r.get_route(500)

    run(main())


def test_route_changes_from_other_controllers_are_tracked():
    async def main():
        async with SimulatedRouter(16, 16, levels=2) as sim, client(sim, levels=2) as a, client(sim) as b:
            seen = []
            a.on_route(seen.append)
            async_seen = []

            async def async_callback(change):
                async_seen.append(change)

            unsubscribe = a.on_route(async_callback)
            await b.route(6, 8, level=2, confirm=True)
            await until(lambda: a.source_of(8, 2) == 6)
            sim.set_crosspoint(3, 1)
            await until(lambda: a.source_of(1) == 3)
            assert seen == [RouteChange(2, 8, 6, None), RouteChange(1, 1, 3, None)]
            await until(lambda: len(async_seen) == 2)
            unsubscribe()
            # same source again is not a change; a new one reports the previous source
            sim.set_crosspoint(3, 1)
            sim.set_crosspoint(4, 1)
            await until(lambda: a.source_of(1) == 4)
            assert seen[2:] == [RouteChange(1, 1, 4, 3)] and len(async_seen) == 2

    run(main())


def test_changes_iterator_and_broken_callback():
    async def main():
        async with SimulatedRouter() as sim, client(sim) as r:
            r.on_route(lambda change: 1 / 0)  # must not break anything

            async def first_two():
                result = []
                async with contextlib.aclosing(r.changes()) as changes:
                    async for change in changes:
                        result.append((change.dest, change.source))
                        if len(result) == 2:
                            return result

            task = asyncio.ensure_future(first_two())
            await asyncio.sleep(0.05)
            await r.route(2, 1, confirm=True)
            await r.route(3, 5, confirm=True)
            assert await task == [(1, 2), (5, 3)]
            assert not r._route_cbs[1:]  # iterator unsubscribed itself

    run(main())


def test_other_matrix_is_ignored():
    async def main():
        async with SimulatedRouter(matrix=2) as sim, client(sim, matrix=2) as good, client(sim) as other:
            await good.route(5, 5, confirm=True)
            await asyncio.sleep(0.05)
            assert good.source_of(5) == 5
            assert other.routes == {}
            with pytest.raises(ResponseTimeoutError):
                await other.get_route(5)

    run(main())


def test_retry_after_missing_ack():
    async def main():
        async with SimulatedRouter() as sim, client(sim, query_commands=False, attempts=2) as r:
            sim.drop_frames = 1
            await r.route(3, 3)
            assert sim.crosspoint(3) == 3
            sim.drop_frames = 2
            with pytest.raises(AckTimeoutError):
                await r.route(4, 3)
            assert sim.crosspoint(3) == 3
            await r.route(5, 3)  # link is usable again afterwards
            assert sim.crosspoint(3) == 5

    run(main())


def test_nak_is_retried_then_reported():
    async def main():
        async with SimulatedRouter() as sim, client(sim, query_commands=False, attempts=3) as r:
            sim.nak_frames = 2
            await r.route(3, 3)
            assert sim.crosspoint(3) == 3
            sim.nak_frames = 3
            with pytest.raises(NakError):
                await r.route(4, 3)

    run(main())


def test_wait_ack_false_for_devices_that_never_acknowledge():
    async def main():
        async with SimulatedRouter() as sim, client(sim, query_commands=False, wait_ack=False) as r:
            sim.drop_frames = 1
            await r.route(3, 3)  # lost, but no error
            await r.route(4, 3, confirm=True)
            assert sim.crosspoint(3) == 4

    run(main())


def test_unsupported_command_is_refused_locally():
    async def main():
        async with SimulatedRouter(extended=False) as sim, client(sim) as r:
            count = len(sim.received)
            with pytest.raises(UnsupportedCommandError):
                await r.send([Command.PROTECT_CONNECT, 0, 0, 0, 0])
            with pytest.raises(UnsupportedCommandError):
                await r.route(2000, 1)  # would need the extended command
            assert len(sim.received) == count

    run(main())


def test_request_returns_raw_reply_for_unparsed_commands():
    async def main():
        async with SimulatedRouter() as sim, client(sim) as r:
            reply = await r.request([Command.PROTOCOL_IMPLEMENTATION_REQUEST], 0x62)
            assert reply.command == 0x62 and 0x02 in reply.commands
        async with SimulatedRouter() as sim, client(sim, query_commands=False) as r:
            with pytest.raises(ResponseTimeoutError):
                await r.request([Command.DUAL_CONTROLLER_STATUS_REQUEST], 0x09, timeout=0.1)
            assert sim.received[-1] == b"\x08"

    run(main())


def test_bad_frame_from_router_is_answered_with_nak():
    async def main():
        async with SimulatedRouter() as sim, client(sim) as r:
            (writer,) = sim._clients
            writer.write(b"\x10\x02\x04\x00\x00\x01\x01\x05\x00\x10\x03")  # wrong checksum
            await until(lambda: sim.naks_received == 1)
            assert r.routes == {}

    run(main())


def test_reconnect_restores_state():
    async def main():
        sim = SimulatedRouter()
        async with sim, client(sim, fetch_tally=True, fetch_names=True) as r:
            states = []
            r.on_connection(states.append)
            await r.route(7, 2, confirm=True)
            sim.disconnect_clients()
            await until(lambda: states == [False])
            assert not r.connected
            with pytest.raises(NotConnectedError):
                await r.route(1, 1)
            await r.wait_connected(5)
            assert states == [False, True] and r.connected
            assert r.source_of(2) == 7 and len(r.source_names) == 16
            await r.route(8, 2, confirm=True)
            assert sim.crosspoint(2) == 8

    run(main())


def test_pending_request_fails_when_connection_drops():
    async def main():
        async with SimulatedRouter() as sim, client(sim, ack_timeout=5, reconnect=False) as r:
            sim.drop_frames = 1
            task = asyncio.ensure_future(r.route(1, 1))
            await until(lambda: sim.drop_frames == 0)
            sim.disconnect_clients()
            with pytest.raises(NotConnectedError):
                await task
            assert not r.connected

    run(main())


def test_keepalive_detects_dead_link_and_reconnects():
    async def main():
        async with SimulatedRouter() as sim, client(sim, keepalive=0.1, query_commands=False, attempts=2) as r:
            await asyncio.sleep(0.35)
            probes = [p for p in sim.received if p[0] == Command.CROSSPOINT_INTERROGATE]
            assert 2 <= len(probes) <= 4
            states = []
            r.on_connection(states.append)
            sim.drop_frames = 2  # one probe = two attempts without ACK
            await until(lambda: states[:2] == [False, True], timeout=5)
            await r.wait_connected(5)
            assert await r.get_route(1) == 1

    run(main())


def test_open_fails_fast_when_nothing_listens():
    async def main():
        sim = await SimulatedRouter().start()
        port = sim.port
        await sim.stop()
        with pytest.raises(NotConnectedError):
            await AsyncRouter("127.0.0.1", port, **FAST).open()

    run(main())


def test_sends_are_serialised():
    async def main():
        async with SimulatedRouter(64, 64) as sim, client(sim, query_commands=False) as r:
            await asyncio.gather(*(r.route(d, d, confirm=True) for d in range(1, 41)))
            assert all(sim.crosspoint(d) == d for d in range(1, 41))
            assert r.routes[1] == {d: d for d in range(1, 41)}

    run(main())


def test_custom_connection_factory():
    async def main():
        async with SimulatedRouter() as sim:
            factory = lambda: asyncio.open_connection("127.0.0.1", sim.port)  # noqa: E731
            async with AsyncRouter(connection_factory=factory, **FAST) as r:
                await r.route(2, 2, confirm=True)
                assert sim.crosspoint(2) == 2
    run(main())
    with pytest.raises(ValueError):
        AsyncRouter()


# -- blocking front end -------------------------------------------------------


class SimThread:
    """Runs a SimulatedRouter in its own thread for the blocking client."""

    def __init__(self, **kwargs):
        self.sim = SimulatedRouter(**kwargs)
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        asyncio.run_coroutine_threadsafe(self.sim.start(), self.loop).result(5)
        return self.sim

    def __exit__(self, *exc_info):
        asyncio.run_coroutine_threadsafe(self.sim.stop(), self.loop).result(5)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)
        self.loop.close()


def test_blocking_router():
    with SimThread(sources=20, dests=20, levels=2) as sim:
        changes = []
        done = threading.Event()
        with Router("127.0.0.1", sim.port, levels=2, fetch_names=True, **FAST) as router:
            assert router.connected and "connected" in repr(router)
            unsubscribe = router.on_route(lambda change: (changes.append(change), done.set()))
            router.route("SRC 5", "DST 3", confirm=True)
            assert done.wait(2)
            assert changes == [RouteChange(1, 3, 5, None)]
            unsubscribe()
            assert router.get_route(3) == 5
            assert router.get_routes(3) == {1: 5, 2: 1}
            router.route(9, 4, level=router.all_levels, confirm=True)
            assert router.tally_dump(2, settle=0.1)[4] == 9
            assert router.tally_dump_all(settle=0.1)[1][3] == 5
            assert router.source_of(4) == 9 and router.destinations_of(9, 2) == [4]
            assert router.routes[2][4] == 9
            assert router.source_names[20] == "SRC 20" and router.find_dest("DST 7") == 7
            assert router.source_name(1) == "SRC 1" and router.dest_name(2) == "DST 2"
            assert Command.CROSSPOINT_CONNECT in router.implemented_commands
            assert Command.CROSSPOINT_CONNECT in router.query_commands()
            router.send([Command.CROSSPOINT_CONNECT, 0, 0, 0, 1])
            assert router.request([0x61], 0x62).command == 0x62
            with pytest.raises(RouteRejectedError):
                sim.protected.add(1)
                router.route(3, 1, confirm=True)
            assert len(changes) == 1
        assert not router.connected
        with pytest.raises(RuntimeError):
            router.route(1, 1)
        router.close()  # closing twice is harmless


def test_blocking_router_open_failure_cleans_up():
    router = Router("127.0.0.1", 1, connect_timeout=1)
    before = threading.active_count()
    with pytest.raises(NotConnectedError):
        router.open()
    assert threading.active_count() == before
    assert not router.connected
