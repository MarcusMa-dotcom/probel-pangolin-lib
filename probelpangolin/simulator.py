"""A small simulated SW-P-08 router.

Useful for trying out code without hardware and for the test suite::

    python -m probelpangolin.simulator --port 8910 --sources 32 --dests 32 --levels 2

It answers crosspoint connect / interrogate, tally dumps, name requests and
the protocol implementation request, in both the standard and the extended
command set, and forwards *Crosspoint Connected* to every connected client.
It also keeps salvo groups: crosspoints that are stored first and set all at
once.

Unlike a real controller it does not send a message again when a client
fails to acknowledge it.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .protocol import (
    ACK_FRAME,
    NAK_FRAME,
    Ack,
    Command,
    Frame,
    FrameDecoder,
    FrameError,
    Nak,
    TraceEvent,
    encode_frame,
)

__all__ = ["SimulatedRouter"]

log = logging.getLogger("probelpangolin.simulator")

# What the simulated router accepts and what it sends, as it reports them
# in the Protocol Implementation Response.
_STANDARD_RX = (
    Command.CROSSPOINT_INTERROGATE,
    Command.CROSSPOINT_CONNECT,
    Command.CROSSPOINT_TALLY_DUMP_REQUEST,
    Command.PROTOCOL_IMPLEMENTATION_REQUEST,
    Command.SOURCE_NAMES_REQUEST,
    Command.DEST_NAMES_REQUEST,
    Command.CROSSPOINT_CONNECT_ON_GO_SALVO,
    Command.CROSSPOINT_GO_GROUP_SALVO,
    Command.CROSSPOINT_SALVO_GROUP_INTERROGATE,
)
_STANDARD_TX = (
    Command.CROSSPOINT_TALLY,
    Command.CROSSPOINT_CONNECTED,
    Command.CROSSPOINT_TALLY_DUMP_BYTE,
    Command.CROSSPOINT_TALLY_DUMP_WORD,
    Command.PROTOCOL_IMPLEMENTATION_RESPONSE,
    Command.SOURCE_NAMES_RESPONSE,
    Command.DEST_NAMES_RESPONSE,
    Command.CROSSPOINT_CONNECT_ON_GO_GROUP_SALVO_ACK,
    Command.CROSSPOINT_GO_DONE_GROUP_SALVO,
    Command.CROSSPOINT_GROUP_SALVO_TALLY,
)
_EXTENDED_RX = (
    Command.EXT_CROSSPOINT_INTERROGATE,
    Command.EXT_CROSSPOINT_CONNECT,
    Command.EXT_CROSSPOINT_TALLY_DUMP_REQUEST,
    Command.EXT_SOURCE_NAMES_REQUEST,
    Command.EXT_DEST_NAMES_REQUEST,
)
_EXTENDED_TX = (
    Command.EXT_CROSSPOINT_TALLY,
    Command.EXT_CROSSPOINT_CONNECTED,
    Command.EXT_CROSSPOINT_TALLY_DUMP_WORD,
    Command.EXT_SOURCE_NAMES_RESPONSE,
    Command.EXT_DEST_NAMES_RESPONSE,
)
_NAME_CHARS = {0: 4, 1: 8, 2: 12}
_NAMES_PER_PART = {4: 32, 8: 16, 12: 10}
_TALLIES_PER_PART = 64


class SimulatedRouter:
    """In-process SW-P-08 router.

    Numbers passed to the Python API (``crosspoint``, ``set_crosspoint``,
    ``protected``) are 1-based like everywhere else in this package.
    """

    def __init__(
        self,
        sources: int = 16,
        dests: int = 16,
        levels: int = 1,
        *,
        matrix: int = 1,
        extended: bool = True,
        host: str = "127.0.0.1",
        port: int = 0,
    ) -> None:
        self.sources = sources
        self.dests = dests
        self.levels = levels
        self.matrix = matrix
        self.extended = extended
        self.host = host
        self.port = port
        # (level0, dest0) -> source0; everything starts on source 1
        self._xpt: Dict[Tuple[int, int], int] = {
            (lv, d): 0 for lv in range(levels) for d in range(dests)
        }
        self.source_names: List[str] = [f"SRC {i + 1}" for i in range(sources)]
        self.dest_names: List[str] = [f"DST {i + 1}" for i in range(dests)]
        #: destinations (1-based) that refuse a connect and report the old source
        self.protected: Set[int] = set()
        #: every valid message received, as raw DATA bytes
        self.received: List[bytes] = []
        self.acks_received = 0
        self.naks_received = 0
        # fault injection: the next n frames are silently dropped / answered with NAK
        self.drop_frames = 0
        self.nak_frames = 0
        #: called as ``on_trace(client, TraceEvent)`` for everything sent and received
        self.on_trace: Optional[Callable[[str, TraceEvent], Any]] = None
        #: called as ``on_crosspoint(level, dest, source, client, accepted)`` for every
        #: connect a client asks for; ``accepted`` is False for a protected destination
        self.on_crosspoint: Optional[Callable[[int, int, int, str, bool], Any]] = None
        #: called as ``on_client(client, connected)`` when a client comes or goes
        self.on_client: Optional[Callable[[str, bool], Any]] = None
        #: called as ``on_salvo(action, salvo, client, count)``; ``action`` is ``"stored"``,
        #: ``"set"``, ``"cleared"`` or ``"empty"``, ``count`` the crosspoints concerned
        self.on_salvo: Optional[Callable[[str, int, str, int], Any]] = None
        # salvo group -> {(level0, dest0): source0}, in the order the crosspoints were stored
        self._salvos: Dict[int, Dict[Tuple[int, int], int]] = {}
        self._current: Optional[asyncio.StreamWriter] = None
        self._server: Optional[asyncio.AbstractServer] = None
        self._clients: Set[asyncio.StreamWriter] = set()
        self._info: Dict[asyncio.StreamWriter, Dict[str, Any]] = {}

    # -- public helpers ----------------------------------------------------

    async def start(self) -> "SimulatedRouter":
        self._server = await asyncio.start_server(self._serve, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]
        log.info("simulated router listening on %s:%d", self.host, self.port)
        return self

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
        self.disconnect_clients()
        if self._server is not None:
            await self._server.wait_closed()
            self._server = None

    async def __aenter__(self) -> "SimulatedRouter":
        return await self.start()

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    def disconnect_clients(self) -> None:
        """Drop all client connections (to test reconnect behaviour)."""
        for writer in list(self._clients):
            writer.close()

    def crosspoint(self, dest: int, level: int = 1) -> int:
        return self._xpt[(level - 1, dest - 1)] + 1

    def crosspoints(self, level: int = 1) -> Dict[int, int]:
        """All crosspoints of one level: ``{destination: source}``."""
        return {d + 1: self._xpt[(level - 1, d)] + 1 for d in range(self.dests)}

    def set_crosspoint(self, source: int, dest: int, level: int = 1, *, announce: bool = True) -> None:
        """Change a crosspoint "at the router" and tell all clients.

        With ``announce=False`` the clients are not told, as if the router
        had lost the tally on its way.
        """
        if not (1 <= source <= self.sources and 1 <= dest <= self.dests and 1 <= level <= self.levels):
            raise ValueError(
                f"source {source}, destination {dest}, level {level} outside "
                f"{self.sources} x {self.dests} on {self.levels} level(s)"
            )
        self._xpt[(level - 1, dest - 1)] = source - 1
        if announce:
            self._broadcast(self._connected_message(level - 1, dest - 1, self.extended and self._big))

    @property
    def salvos(self) -> Dict[int, List[Tuple[int, int, int]]]:
        """What the salvo groups hold: ``{salvo: [(level, destination, source), ...]}``."""
        return {
            salvo: [(lv + 1, d + 1, src + 1) for (lv, d), src in stored.items()]
            for salvo, stored in self._salvos.items() if stored
        }

    @property
    def clients(self) -> List[Dict[str, Any]]:
        """Who is connected: address, time of connecting, number of messages received."""
        return [dict(info) for info in self._info.values()]

    def _name_of(self, writer: asyncio.StreamWriter) -> str:
        info = self._info.get(writer)
        return info["address"] if info else "?"

    def _trace(self, writer: asyncio.StreamWriter, event: TraceEvent) -> None:
        if self.on_trace is not None:
            try:
                self.on_trace(self._name_of(writer), event)
            except Exception:
                log.exception("trace callback failed")

    # -- connection handling ----------------------------------------------

    @property
    def _big(self) -> bool:
        return self.sources > 1024 or self.dests > 1024 or self.levels > 16 or self.matrix > 16

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        address = f"{peer[0]}:{peer[1]}" if isinstance(peer, tuple) and len(peer) >= 2 else "client"
        self._clients.add(writer)
        self._info[writer] = {"address": address, "since": time.time(), "messages": 0}
        self._notify_client(address, True)
        decoder = FrameDecoder()
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                for event in decoder.feed(data):
                    if isinstance(event, Ack):
                        self.acks_received += 1
                        self._trace(writer, TraceEvent("rx", "ack"))
                    elif isinstance(event, Nak):
                        self.naks_received += 1
                        self._trace(writer, TraceEvent("rx", "nak"))
                    elif isinstance(event, FrameError):
                        self._trace(writer, TraceEvent("rx", "error", event.raw, event.reason))
                        writer.write(NAK_FRAME)
                        self._trace(writer, TraceEvent("tx", "nak"))
                    elif isinstance(event, Frame):
                        self._on_frame(event.payload, writer)
                await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            self._clients.discard(writer)
            self._info.pop(writer, None)
            writer.close()
            self._notify_client(address, False)

    def _notify_client(self, address: str, connected: bool) -> None:
        if self.on_client is not None:
            try:
                self.on_client(address, connected)
            except Exception:
                log.exception("client callback failed")

    def _on_frame(self, payload: bytes, writer: asyncio.StreamWriter) -> None:
        info = self._info.get(writer)
        if info is not None:
            info["messages"] += 1
        if self.drop_frames > 0:
            self.drop_frames -= 1
            self._trace(writer, TraceEvent("rx", "message", payload, "dropped on purpose, no answer"))
            return
        if self.nak_frames > 0:
            self.nak_frames -= 1
            self._trace(writer, TraceEvent("rx", "message", payload, "refused on purpose"))
            writer.write(NAK_FRAME)
            self._trace(writer, TraceEvent("tx", "nak"))
            return
        self._trace(writer, TraceEvent("rx", "message", payload))
        # ACK and answers leave in one piece, as a controller sends them back to back.
        out = bytearray(ACK_FRAME)
        self._trace(writer, TraceEvent("tx", "ack"))
        self.received.append(payload)
        self._current = writer
        try:
            for reply, to_all in self._process(payload):
                out += encode_frame(reply)
                self._trace(writer, TraceEvent("tx", "message", reply))
                if to_all:
                    self._broadcast(reply, skip=writer)
        except (IndexError, KeyError):
            log.warning("ignoring malformed or out-of-range message %s", payload.hex())
            self._trace(writer, TraceEvent("rx", "error", payload, "malformed or out of range, ignored"))
        finally:
            self._current = None
            writer.write(bytes(out))

    def _broadcast(self, payload: bytes, skip: Optional[asyncio.StreamWriter] = None) -> None:
        frame = encode_frame(payload)
        for writer in list(self._clients):
            if writer is not skip and not writer.is_closing():
                writer.write(frame)
                self._trace(writer, TraceEvent("tx", "message", payload))

    # -- message handling --------------------------------------------------

    def _tally_message(self, command: int, level: int, dest: int, extended: bool) -> bytes:
        source = self._xpt[(level, dest)]
        m = self.matrix - 1
        if extended:
            return bytes((command, m, level, dest >> 8, dest & 0xFF, source >> 8, source & 0xFF))
        multiplier = ((dest >> 7) & 0x07) << 4 | ((source >> 7) & 0x07)
        return bytes((command, (m << 4) | level, multiplier, dest & 0x7F, source & 0x7F))

    def _connected_message(self, level: int, dest: int, extended: bool) -> bytes:
        command = Command.EXT_CROSSPOINT_CONNECTED if extended else Command.CROSSPOINT_CONNECTED
        return self._tally_message(command, level, dest, extended)

    def _process(self, p: bytes) -> List[Tuple[bytes, bool]]:
        cmd = p[0]
        m = self.matrix - 1
        if cmd >= 0x80 and not self.extended:
            return []

        if cmd == Command.PROTOCOL_IMPLEMENTATION_REQUEST:
            # SW-P-88, 10.1: how many commands the controller sends, how many it
            # accepts, then the two lists.
            sent = _STANDARD_TX + (_EXTENDED_TX if self.extended else ())
            accepted = _STANDARD_RX + (_EXTENDED_RX if self.extended else ())
            header = bytes((Command.PROTOCOL_IMPLEMENTATION_RESPONSE, len(sent), len(accepted)))
            return [(header + bytes(sent) + bytes(accepted), False)]

        if cmd == Command.CROSSPOINT_CONNECT_ON_GO_SALVO:
            if p[1] >> 4 != m:
                return []
            level = p[1] & 0x0F
            dest = ((p[2] & 0x70) << 3) | (p[3] & 0x7F)
            source = ((p[2] & 0x07) << 7) | (p[4] & 0x7F)
            salvo = p[5] & 0x7F
            if (level, dest) not in self._xpt or source >= self.sources:
                raise KeyError((level, dest, source))
            self._salvos.setdefault(salvo, {})[(level, dest)] = source
            self._salvo_event("stored", salvo, 1)
            acknowledge = bytes((Command.CROSSPOINT_CONNECT_ON_GO_GROUP_SALVO_ACK,
                                 p[1], p[2] & 0x77, p[3] & 0x7F, p[4] & 0x7F, salvo))
            return [(acknowledge, True)]

        if cmd == Command.CROSSPOINT_GO_GROUP_SALVO:
            salvo = p[2] & 0x7F
            stored = self._salvos.pop(salvo, {})
            if not stored:
                outcome = 2
                self._salvo_event("empty", salvo, 0)
            elif p[1] == 1:
                outcome = 1
                self._salvo_event("cleared", salvo, len(stored))
            else:
                outcome = 0
                # No Connected messages for a salvo: controllers follow the acknowledges.
                for (level, dest), source in stored.items():
                    self._take(level, dest, source)
                self._salvo_event("set", salvo, len(stored))
            return [(bytes((Command.CROSSPOINT_GO_DONE_GROUP_SALVO, outcome, salvo)), True)]

        if cmd == Command.CROSSPOINT_SALVO_GROUP_INTERROGATE:
            salvo, index = p[1] & 0x7F, p[2]
            entries = list(self._salvos.get(salvo, {}).items())
            if index >= len(entries):
                tally = bytes((Command.CROSSPOINT_GROUP_SALVO_TALLY, m << 4, 0, 0, 0, salvo, index & 0x7F, 2))
                return [(tally, False)]
            (level, dest), source = entries[index]
            multiplier = ((dest >> 7) & 0x07) << 4 | ((source >> 7) & 0x07)
            validity = 1 if index == len(entries) - 1 else 0
            tally = bytes((Command.CROSSPOINT_GROUP_SALVO_TALLY, (m << 4) | level, multiplier,
                           dest & 0x7F, source & 0x7F, salvo, index, validity))
            return [(tally, False)]

        if cmd in (Command.CROSSPOINT_INTERROGATE, Command.CROSSPOINT_CONNECT):
            if p[1] >> 4 != m:
                return []
            level = p[1] & 0x0F
            dest = ((p[2] & 0x70) << 3) | p[3]
            if cmd == Command.CROSSPOINT_INTERROGATE:
                return [(self._tally_message(Command.CROSSPOINT_TALLY, level, dest, False), False)]
            source = ((p[2] & 0x07) << 7) | p[4]
            return self._connect(level, dest, source, False)

        if cmd in (Command.EXT_CROSSPOINT_INTERROGATE, Command.EXT_CROSSPOINT_CONNECT):
            if p[1] != m:
                return []
            level = p[2]
            dest = (p[3] << 8) | p[4]
            if cmd == Command.EXT_CROSSPOINT_INTERROGATE:
                return [(self._tally_message(Command.EXT_CROSSPOINT_TALLY, level, dest, True), False)]
            return self._connect(level, dest, (p[5] << 8) | p[6], True)

        if cmd == Command.CROSSPOINT_TALLY_DUMP_REQUEST:
            if p[1] >> 4 != m:
                return []
            return [(part, False) for part in self._dump(p[1] & 0x0F, False)]

        if cmd == Command.EXT_CROSSPOINT_TALLY_DUMP_REQUEST:
            if p[1] != m:
                return []
            return [(part, False) for part in self._dump(p[2], True)]

        if cmd in (Command.SOURCE_NAMES_REQUEST, Command.DEST_NAMES_REQUEST):
            if p[1] >> 4 != m:
                return []
            is_source = cmd == Command.SOURCE_NAMES_REQUEST
            head = bytes((
                Command.SOURCE_NAMES_RESPONSE if is_source else Command.DEST_NAMES_RESPONSE,
                p[1],
            ))
            names = self.source_names if is_source else self.dest_names
            return [(part, False) for part in self._names(head, names, p[2])]

        if cmd == Command.EXT_SOURCE_NAMES_REQUEST:
            if p[1] != m:
                return []
            head = bytes((Command.EXT_SOURCE_NAMES_RESPONSE, p[1], p[2]))
            return [(part, False) for part in self._names(head, self.source_names, p[3])]

        if cmd == Command.EXT_DEST_NAMES_REQUEST:
            if p[1] != m:
                return []
            head = bytes((Command.EXT_DEST_NAMES_RESPONSE, p[1]))
            return [(part, False) for part in self._names(head, self.dest_names, p[2])]

        return []

    def _connect(self, level: int, dest: int, source: int, extended: bool) -> List[Tuple[bytes, bool]]:
        if (level, dest) not in self._xpt or source >= self.sources:
            raise KeyError((level, dest, source))
        self._take(level, dest, source)
        return [(self._connected_message(level, dest, extended), True)]

    def _take(self, level: int, dest: int, source: int) -> bool:
        """Set a crosspoint a client asked for, unless the destination is protected."""
        accepted = dest + 1 not in self.protected
        if accepted:
            self._xpt[(level, dest)] = source
        if self.on_crosspoint is not None:
            try:
                self.on_crosspoint(level + 1, dest + 1, source + 1, self._current_name(), accepted)
            except Exception:
                log.exception("crosspoint callback failed")
        return accepted

    def _current_name(self) -> str:
        """Address of the client whose message is being processed."""
        return self._name_of(self._current) if self._current is not None else ""

    def _salvo_event(self, action: str, salvo: int, count: int) -> None:
        if self.on_salvo is not None:
            try:
                self.on_salvo(action, salvo, self._current_name(), count)
            except Exception:
                log.exception("salvo callback failed")

    def _dump(self, level: int, extended: bool) -> List[bytes]:
        if level >= self.levels:
            return []
        m = self.matrix - 1
        byte_format = not extended and self.sources <= 191 and self.dests <= 191
        parts = []
        for first in range(0, self.dests, _TALLIES_PER_PART):
            sources = [
                self._xpt[(level, d)] for d in range(first, min(first + _TALLIES_PER_PART, self.dests))
            ]
            if byte_format:
                head = bytes((Command.CROSSPOINT_TALLY_DUMP_BYTE, (m << 4) | level, len(sources), first))
                parts.append(head + bytes(sources))
                continue
            if extended:
                head = bytes((Command.EXT_CROSSPOINT_TALLY_DUMP_WORD, m, level))
            else:
                head = bytes((Command.CROSSPOINT_TALLY_DUMP_WORD, (m << 4) | level))
            body = bytes((len(sources), first >> 8, first & 0xFF))
            for source in sources:
                body += bytes((source >> 8, source & 0xFF))
            parts.append(head + body)
        return parts

    @staticmethod
    def _names(head: bytes, names: List[str], length_code: int) -> List[bytes]:
        chars = _NAME_CHARS[length_code]
        per_part = _NAMES_PER_PART[chars]
        parts = []
        for first in range(0, len(names), per_part):
            chunk = names[first : first + per_part]
            body = bytes((length_code, first >> 8, first & 0xFF, len(chunk)))
            for name in chunk:
                body += name.encode("latin-1", errors="replace")[:chars].ljust(chars, b" ")
            parts.append(head + body)
        return parts


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m probelpangolin.simulator", description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8910)
    parser.add_argument("--sources", type=int, default=16)
    parser.add_argument("--dests", type=int, default=16)
    parser.add_argument("--levels", type=int, default=1)
    parser.add_argument("--matrix", type=int, default=1)
    parser.add_argument("--no-extended", action="store_true", help="do not implement extended commands")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    async def run() -> None:
        router = SimulatedRouter(
            args.sources, args.dests, args.levels,
            matrix=args.matrix, extended=not args.no_extended, host=args.host, port=args.port,
        )
        async with router:
            await asyncio.Event().wait()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
