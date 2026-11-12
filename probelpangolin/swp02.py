"""SW-P-02, the General Switcher Communication Protocol of Pro-Bel.

The older and simpler sibling of SW-P-08: one level, no matrix number, no
names, no acknowledge on the data link.  A router answers a CONNECT with a
CONNECTED and an INTERROGATE with a TALLY, and that answer is the only
confirmation there is.

Message layout (SW-P-88, chapter 3)::

    +------+---------+------~~------+----------+
    | 0xFF | COMMAND |   MESSAGE    | CHECKSUM |
    +------+---------+------~~------+----------+

* The command byte also defines the length of the message.
* The checksum is the 7-bit two's complement of the sum of command and
  message, its top bit is 0.

This module holds the wire format, a client with the same methods as
:class:`probelpangolin.AsyncRouter` and a simulated router::

    from probelpangolin.swp02 import Router

    with Router("10.0.0.30", dests=16) as router:      # TCP port 2000
        router.route(source=5, dest=3, confirm=True)
        print(router.get_route(3))

Sources and destinations count from 1 here, as everywhere in this package.
Only the commands of the public specification are implemented: interrogate,
connect, tally, connected, connect on go, go and their acknowledges.  The
extended commands (65 and up) and the status commands are named in the
specification but not laid out, so they are reported and not decoded.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

from . import client as _client
from . import simulator as _simulator
from . import sync as _sync
from .errors import ResponseTimeoutError, RouteRejectedError, UnsupportedCommandError
from .protocol import Crosspoint, Frame, FrameError, ProtocolError, SWP08Error, TraceEvent

__all__ = [
    "SOM",
    "DEFAULT_PORT",
    "MAX_DEST",
    "MAX_SOURCE",
    "PROTOCOL",
    "Command",
    "command_name",
    "encode_message",
    "MessageDecoder",
    "build_interrogate",
    "build_connect",
    "build_connect_on_go",
    "build_go",
    "Switch",
    "Go",
    "Unknown",
    "parse_message",
    "describe",
    "AsyncRouter",
    "Router",
    "SimulatedRouter",
]

log = logging.getLogger("probelpangolin.swp02")

PROTOCOL = "sw-p-02"
SOM = 0xFF
#: Default TCP port of SW-P-02 over IP (SW-P-88, 4.4).
DEFAULT_PORT = 2000
#: Destinations 1 to 1024 fit the multiplier byte.
MAX_DEST = 1024
#: Sources 1 to 1023.  The last number is reserved: a TALLY with it says
#: that the destination asked for is out of range.
MAX_SOURCE = 1023
_OUT_OF_RANGE = 1024

#: Bytes an unknown command may carry before the decoder gives up on it.
_MAX_UNKNOWN = 64


class Command(enum.IntEnum):
    """SW-P-02 command bytes.  The specification writes them in decimal."""

    INTERROGATE = 1
    CONNECT = 2
    TALLY = 3
    CONNECTED = 4
    CONNECT_ON_GO = 5
    GO = 6
    STATUS_REQUEST = 7
    CONNECT_ON_GO_ACKNOWLEDGE = 12
    GO_DONE_ACKNOWLEDGE = 13
    STATUS_REQUEST_2 = 18
    STATUS_RESPONSE_6 = 19
    EXTENDED_CONNECT = 66
    EXTENDED_CONNECTED = 68


#: Number of message bytes after the command byte, for the commands the
#: specification lays out.
LENGTHS: Dict[int, int] = {
    Command.INTERROGATE: 2,
    Command.CONNECT: 3,
    Command.TALLY: 3,
    Command.CONNECTED: 3,
    Command.CONNECT_ON_GO: 3,
    Command.GO: 1,
    Command.CONNECT_ON_GO_ACKNOWLEDGE: 3,
    Command.GO_DONE_ACKNOWLEDGE: 1,
}


def command_name(code: int) -> str:
    """Readable name for a command byte, e.g. ``CONNECT``."""
    try:
        return Command(code).name
    except ValueError:
        return f"UNKNOWN_{code}"


# ---------------------------------------------------------------------------
# Framing
# ---------------------------------------------------------------------------


def checksum(payload: Union[bytes, bytearray]) -> int:
    """7-bit two's complement of the sum of command and message."""
    return (-sum(payload)) & 0x7F


def encode_message(payload: Union[bytes, bytearray, List[int]]) -> bytes:
    """Put start of message and checksum around command and message bytes."""
    data = bytes(payload)
    if not data:
        raise ProtocolError("cannot send an empty message")
    return bytes((SOM,)) + data + bytes((checksum(data),))


class MessageDecoder:
    """Incremental decoder for the SW-P-02 byte stream.

    Returns :class:`~probelpangolin.protocol.Frame` for a complete message
    with a correct checksum (``payload`` is command and message) and
    :class:`~probelpangolin.protocol.FrameError` for everything else.  The
    command byte tells how long a message is.  A command without a known
    length is collected until the next start of message and reported
    undecoded.
    """

    def __init__(self) -> None:
        self._body = bytearray()
        self._active = False
        self.discarded = 0  #: bytes dropped while hunting for a start of message

    @property
    def in_frame(self) -> bool:
        """True while a message has started and is not complete yet."""
        return self._active

    def reset(self) -> None:
        self._body.clear()
        self._active = False

    def feed(self, data: Union[bytes, bytearray]) -> List[Union[Frame, FrameError]]:
        events: List[Union[Frame, FrameError]] = []
        for byte in data:
            if byte == SOM:
                if self._active and self._body:
                    events.append(self._abandon())
                elif self._active:
                    self.discarded += 1  # a start of message right after another one
                self._active = True
                self._body.clear()
                continue
            if not self._active:
                self.discarded += 1
                continue
            self._body.append(byte)
            length = LENGTHS.get(self._body[0])
            if length is None:
                if len(self._body) > _MAX_UNKNOWN:
                    events.append(self._abandon())
                    self._active = False
                continue
            if len(self._body) == length + 2:  # command, message, checksum
                body = bytes(self._body)
                self._body.clear()
                self._active = False
                if sum(body) & 0x7F:
                    events.append(FrameError("checksum mismatch", body))
                else:
                    events.append(Frame(body[:-1]))
        return events

    def flush(self) -> List[Union[Frame, FrameError]]:
        """Report what is still waiting, at the end of a capture."""
        if self._active and self._body:
            event = self._abandon()
            self._active = False
            return [event]
        return []

    def _abandon(self) -> FrameError:
        body = bytes(self._body)
        self._body.clear()
        if body[0] in LENGTHS:
            return FrameError("message cut short", body)
        return FrameError(f"command {body[0]} ({command_name(body[0])}) has no known length, not decoded", body)


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


def _number(name: str, value: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ProtocolError(f"{name} must be an integer, got {value!r}")
    if not 1 <= value <= maximum:
        raise ProtocolError(f"{name} {value} out of range 1..{maximum}")
    return value - 1


def _switch(command: int, dest: int, source: Optional[int]) -> bytes:
    d = _number("destination", dest, MAX_DEST)
    if source is None:
        return bytes((command, ((d >> 7) & 0x07) << 4, d & 0x7F))
    s = _number("source", source, MAX_SOURCE)
    return bytes((command, ((d >> 7) & 0x07) << 4 | ((s >> 7) & 0x07), d & 0x7F, s & 0x7F))


def build_interrogate(dest: int) -> bytes:
    """INTERROGATE (01): ask for the source on a destination."""
    return _switch(Command.INTERROGATE, dest, None)


def build_connect(source: int, dest: int) -> bytes:
    """CONNECT (02): route a source to a destination."""
    return _switch(Command.CONNECT, dest, source)


def build_connect_on_go(source: int, dest: int) -> bytes:
    """CONNECT ON GO (05): store a crosspoint until the next GO."""
    return _switch(Command.CONNECT_ON_GO, dest, source)


def build_go(*, clear: bool = False) -> bytes:
    """GO (06): set the crosspoints stored with CONNECT ON GO, or clear them."""
    return bytes((Command.GO, 1 if clear else 0))


@dataclass(frozen=True)
class Switch:
    """INTERROGATE, CONNECT, TALLY, CONNECTED, CONNECT ON GO and its acknowledge."""

    command: int
    dest: int
    source: Optional[int]  #: ``None`` for an interrogate
    #: bit 3 of the multiplier: the source is bad, or updating the crosspoint
    #: is disabled, depending on the router
    flag: bool = False

    @property
    def out_of_range(self) -> bool:
        """True for a TALLY that says the destination asked for does not exist."""
        return self.command == Command.TALLY and self.source == _OUT_OF_RANGE


@dataclass(frozen=True)
class Go:
    """GO (06) and GO DONE ACKNOWLEDGE (13).

    ``action`` in a GO: 0 = set the stored crosspoints, 1 = clear them.  In
    the acknowledge: 0 = crosspoints set, 1 = stored crosspoints cleared,
    2 = there was nothing to set or clear.
    """

    command: int
    action: int

    @property
    def is_done(self) -> bool:
        return self.command == Command.GO_DONE_ACKNOWLEDGE


@dataclass(frozen=True)
class Unknown:
    """A message this module has no layout for."""

    command: int
    payload: bytes


Message = Union[Switch, Go, Unknown]


def parse_message(payload: Union[bytes, bytearray]) -> Message:
    """Turn command and message bytes into a typed message."""
    payload = bytes(payload)
    if not payload:
        raise ProtocolError("empty message")
    command = payload[0]
    length = LENGTHS.get(command)
    if length is None:
        return Unknown(command, payload)
    if len(payload) != length + 1:
        raise ProtocolError(
            f"{command_name(command)} has {length} message bytes, got {len(payload) - 1}"
        )
    if command in (Command.GO, Command.GO_DONE_ACKNOWLEDGE):
        return Go(command, payload[1])
    multiplier = payload[1]
    dest = (((multiplier & 0x70) << 3) | (payload[2] & 0x7F)) + 1
    if command == Command.INTERROGATE:
        return Switch(command, dest, None, bool(multiplier & 0x08))
    source = (((multiplier & 0x07) << 7) | (payload[3] & 0x7F)) + 1
    return Switch(command, dest, source, bool(multiplier & 0x08))


def describe(message: Message) -> str:
    """One line of plain text for a parsed message, with numbers counted from 1."""
    title = command_name(message.command).replace("_", " ").capitalize()
    if isinstance(message, Switch):
        if message.source is None:
            return f"{title}: destination {message.dest}"
        if message.out_of_range:
            return f"{title}: destination {message.dest} is out of range at the router"
        flag = ", bit 3 is set: bad source, or updating disabled" if message.flag else ""
        return f"{title}: destination {message.dest} <- source {message.source}{flag}"
    if isinstance(message, Go):
        if message.is_done:
            outcome = {0: "crosspoints set", 1: "stored crosspoints cleared",
                       2: "nothing to set or clear"}.get(message.action, f"code {message.action}")
        else:
            outcome = {0: "set the stored crosspoints",
                       1: "clear the stored crosspoints"}.get(message.action, f"code {message.action}")
        return f"{title}: {outcome}"
    return f"{title}: {len(message.payload)} bytes, not decoded"


def _event(direction: str, kind: str, payload: bytes = b"", note: str = "") -> TraceEvent:
    raw = encode_message(payload) if kind == "message" else b""
    return TraceEvent(direction, kind, payload, note, raw=raw, protocol=PROTOCOL)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class AsyncRouter(_client.AsyncRouter):
    """Control a router via SW-P-02, with the methods of :class:`probelpangolin.AsyncRouter`.

    What is different, because the protocol is:

    * One level and no matrix number: ``level`` arguments are accepted and ignored.
    * No acknowledge on the data link.  A take is confirmed by the CONNECTED
      of the router (``confirm=True``), an interrogate by its TALLY.
    * No tally dump.  :meth:`tally_dump` interrogates the destinations 1 to
      ``dests`` one by one.
    * No names and no list of implemented commands.
    * Connect on go has one store, not numbered groups: ``salvo`` arguments
      are accepted and ignored.  After a GO the router sends no CONNECTED,
      so the client interrogates the destinations concerned.

    Parameters
    ----------
    host, port:
        Address of the SW-P-02 TCP port, 2000 unless configured otherwise.
    dests:
        Number of destinations of the router, for :meth:`tally_dump`.
    fetch_tally:
        Interrogate all ``dests`` on every (re)connect.
    response_timeout:
        Time to wait for a TALLY or CONNECTED.  The specification recommends
        at least 100 ms for a client over IP.
    keepalive:
        Seconds of silence after which destination 1 is interrogated.  No
        TALLY in time counts as a lost link, as the specification describes
        it.  ``None`` disables it.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: int = DEFAULT_PORT,
        *,
        dests: int = 0,
        fetch_tally: bool = False,
        response_timeout: float = 1.0,
        keepalive: Optional[float] = 30.0,
        reconnect: bool = True,
        reconnect_delay: float = 2.0,
        connect_timeout: float = 5.0,
        connection_factory: Optional[_client.ConnectionFactory] = None,
    ) -> None:
        if not 0 <= dests <= MAX_DEST:
            raise ValueError(f"dests must be 0..{MAX_DEST}")
        super().__init__(
            host, port,
            matrix=1, levels=1, extended=False, query_commands=False, fetch_names=False,
            fetch_tally=fetch_tally, wait_ack=False, response_timeout=response_timeout,
            keepalive=keepalive, reconnect=reconnect, reconnect_delay=reconnect_delay,
            connect_timeout=connect_timeout, connection_factory=connection_factory,
        )
        self.dests = dests
        self._decoder = MessageDecoder()  # type: ignore[assignment]
        self._stored: Dict[int, int] = {}  # connect on go, as the router acknowledged it

    def __repr__(self) -> str:
        state = "connected" if self.connected else "disconnected"
        return f"<swp02.AsyncRouter {self.host}:{self.port} {state}>"

    # -- sending -----------------------------------------------------------

    async def send(self, payload: Union[bytes, bytearray, List[int]], *, check: bool = True) -> None:
        """Send command and message bytes.  There is no ACK to wait for."""
        payload = bytes(payload)
        frame = encode_message(payload)
        async with self._send_lock:
            writer = self._writer
            if writer is None or not self._connected.is_set():
                raise _client.NotConnectedError(f"not connected to {self._target}")
            log.debug("tx %s", frame.hex())
            writer.write(frame)
            self._emit(self._trace_cbs, _event("tx", "message", payload))
            try:
                await writer.drain()
            except OSError as exc:
                raise _client.NotConnectedError(f"connection to {self._target} lost: {exc}") from exc
            self._last_tx = asyncio.get_running_loop().time()

    # -- receiving ---------------------------------------------------------

    def _handle_event(self, event: Any) -> None:
        if isinstance(event, FrameError):
            log.warning("bad message from router (%s): %s", event.reason, event.raw.hex())
            self._emit(self._trace_cbs, _event("rx", "error", event.raw, event.reason))
            return
        assert isinstance(event, Frame)
        self._emit(self._trace_cbs, _event("rx", "message", event.payload))
        try:
            message = parse_message(event.payload)
        except ProtocolError as exc:
            log.warning("cannot parse message %s: %s", event.payload.hex(), exc)
            return
        if isinstance(message, Switch) and message.command in (Command.TALLY, Command.CONNECTED):
            if message.out_of_range:
                error = ProtocolError(f"the router reports destination {message.dest} as out of range")
                for fut, _connected_only in self._xpt_waiters.get((1, message.dest), ()):
                    if not fut.done():
                        fut.set_exception(error)
                return
            assert message.source is not None
            # Same command numbers as in SW-P-08: the cache and the waiters of the
            # parent class take it from here.
            self._dispatch(Crosspoint(message.command, 1, 1, message.dest, message.source, message.flag))
            return
        if isinstance(message, Switch) and message.command == Command.CONNECT_ON_GO_ACKNOWLEDGE:
            assert message.source is not None
            self._stored[message.dest] = message.source
        elif isinstance(message, Go) and message.is_done:
            stored, self._stored = self._stored, {}
            if message.action == 0:
                # No CONNECTED messages follow a GO.  The specification leaves it to
                # the controller to interrogate.
                dests = sorted(stored) or list(range(1, self.dests + 1))
                task = asyncio.ensure_future(self._refresh(dests))
                self._background.add(task)
                task.add_done_callback(self._background.discard)
        for fut in self._cmd_waiters.get(message.command, ()):
            if not fut.done():
                fut.set_result(message)
        self._emit(self._message_cbs, message)

    async def _refresh(self, dests: List[int]) -> None:
        for dest in dests:
            try:
                await self.get_route(dest)
            except SWP08Error:
                return

    def _attach(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._stored = {}
        super()._attach(reader, writer)

    async def _keepalive_loop(self) -> None:
        interval = float(self._keepalive or 0)
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(interval)
            if loop.time() - self._last_tx < interval:
                continue
            try:
                await self.get_route(1)
            except ResponseTimeoutError as exc:
                log.warning("keepalive failed (%s), dropping connection", exc)
                if self._writer is not None:
                    self._writer.close()  # read loop sees EOF and reconnects
                return
            except SWP08Error as exc:
                log.debug("keepalive: %s", exc)

    # -- crosspoints -------------------------------------------------------

    async def route(
        self,
        source: _client.Port,
        dest: _client.Port,
        level: _client.Levels = 1,
        *,
        confirm: bool = False,
        timeout: Optional[float] = None,
    ) -> None:
        """Route ``source`` to ``dest``.  With ``confirm`` the CONNECTED of the router is awaited."""
        src = self._source(source)
        dst = self._dest(dest)
        payload = build_connect(src, dst)
        if not confirm:
            await self.send(payload)
            return
        reply = await self._crosspoint_request(payload, 1, dst, True, timeout)
        if reply.source != src:
            raise RouteRejectedError(dst, 1, src, reply.source)

    async def get_route(self, dest: _client.Port, level: int = 1, *, timeout: Optional[float] = None) -> int:
        """Interrogate the router: which source is on ``dest``."""
        dst = self._dest(dest)
        reply = await self._crosspoint_request(build_interrogate(dst), 1, dst, False, timeout)
        return reply.source

    async def tally_dump(
        self, level: int = 1, *, settle: float = 0.0, timeout: Optional[float] = None
    ) -> Dict[int, int]:
        """Interrogate the destinations 1 to ``dests`` one by one: ``{dest: source}``."""
        if not self.dests:
            raise UnsupportedCommandError(
                "SW-P-02 has no tally dump: set dests to the number of destinations to interrogate"
            )
        for dest in range(1, self.dests + 1):
            try:
                await self.get_route(dest, timeout=timeout)
            except ProtocolError:
                break  # the router says this destination is out of range
        return dict(self._routes.get(1, {}))

    async def read_names(self, **_options: Any) -> Tuple[Dict[int, str], Dict[int, str]]:
        raise UnsupportedCommandError("SW-P-02 carries no names")

    async def query_commands(self, **_options: Any) -> Any:
        raise UnsupportedCommandError("SW-P-02 has no list of implemented commands")

    # -- connect on go -----------------------------------------------------

    async def salvo_add(
        self, source: _client.Port, dest: _client.Port, salvo: int = 0, level: _client.Levels = 1,
        *, timeout: Optional[float] = None,
    ) -> None:
        """CONNECT ON GO: store a crosspoint at the router until :meth:`salvo_go`."""
        await self.request(
            build_connect_on_go(self._source(source), self._dest(dest)),
            Command.CONNECT_ON_GO_ACKNOWLEDGE, timeout=timeout,
        )

    async def salvo_go(self, salvo: int = 0, *, clear: bool = False, timeout: Optional[float] = None) -> int:
        """GO: set the stored crosspoints, or clear them.  Returns 0 (set), 1 (cleared) or 2 (nothing)."""
        reply = await self.request(build_go(clear=clear), Command.GO_DONE_ACKNOWLEDGE, timeout=timeout)
        assert isinstance(reply, Go)
        return reply.action

    async def salvo_read(self, salvo: int = 0, **_options: Any) -> List[Tuple[int, int, int]]:
        raise UnsupportedCommandError("SW-P-02 cannot read back what is stored for the next GO")

    async def dual_controller_status(self, **_options: Any) -> Any:
        raise UnsupportedCommandError("SW-P-02 has no dual controller status")


class Router(_sync.Router):
    """Blocking front end for :class:`AsyncRouter` of this module."""

    _client_class = AsyncRouter

    def __init__(self, host: Optional[str] = None, port: int = DEFAULT_PORT, **options: Any) -> None:
        super().__init__(host, port, **options)

    def salvo_add(
        self, source: _client.Port, dest: _client.Port, salvo: int = 0, level: _client.Levels = 1,
        *, timeout: Optional[float] = None,
    ) -> None:
        """CONNECT ON GO: store a crosspoint at the router until :meth:`salvo_go`."""
        self._await(lambda r: r.salvo_add(source, dest, salvo, level, timeout=timeout))

    def salvo_go(self, salvo: int = 0, *, clear: bool = False, timeout: Optional[float] = None) -> int:
        """GO: set the stored crosspoints, or clear them.  Returns 0, 1 or 2."""
        return self._await(lambda r: r.salvo_go(salvo, clear=clear, timeout=timeout))


# ---------------------------------------------------------------------------
# Simulated router
# ---------------------------------------------------------------------------


class SimulatedRouter(_simulator.SimulatedRouter):
    """In-process SW-P-02 router with one level.

    It answers INTERROGATE with TALLY, makes a CONNECT and tells every
    client with CONNECTED, stores CONNECT ON GO until a GO, and reports a
    destination that does not exist with the reserved source number.
    ``drop_frames`` leaves the next messages unanswered.  There is no NAK
    in this protocol, so ``nak_frames`` has no effect.
    """

    def __init__(self, sources: int = 16, dests: int = 16, *, host: str = "127.0.0.1", port: int = 0) -> None:
        if not 1 <= sources <= MAX_SOURCE or not 1 <= dests <= MAX_DEST:
            raise ValueError(f"SW-P-02 reaches {MAX_SOURCE} sources and {MAX_DEST} destinations")
        super().__init__(sources, dests, 1, matrix=1, extended=False, host=host, port=port)
        self._stored: Dict[int, int] = {}  # connect on go: dest0 -> source0

    @property
    def salvos(self) -> Dict[int, List[Tuple[int, int, int]]]:
        """What waits for the next GO, in the form of the SW-P-08 simulator: ``{0: [(1, dest, source)]}``."""
        if not self._stored:
            return {}
        return {0: [(1, dest + 1, source + 1) for dest, source in self._stored.items()]}

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        address = f"{peer[0]}:{peer[1]}" if isinstance(peer, tuple) and len(peer) >= 2 else "client"
        self._clients.add(writer)
        self._info[writer] = {"address": address, "since": time.time(), "messages": 0}
        self._notify_client(address, True)
        decoder = MessageDecoder()
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                for event in decoder.feed(data):
                    if isinstance(event, FrameError):
                        self._trace(writer, _event("rx", "error", event.raw, event.reason))
                    else:
                        self._on_message(event.payload, writer)
                await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            self._clients.discard(writer)
            self._info.pop(writer, None)
            writer.close()
            self._notify_client(address, False)

    def _on_message(self, payload: bytes, writer: asyncio.StreamWriter) -> None:
        info = self._info.get(writer)
        if info is not None:
            info["messages"] += 1
        if self.drop_frames > 0:
            self.drop_frames -= 1
            self._trace(writer, _event("rx", "message", payload, "dropped on purpose, no answer"))
            return
        self._trace(writer, _event("rx", "message", payload))
        self.received.append(payload)
        self._current = writer
        out = bytearray()
        try:
            for reply, to_all in self._answers(parse_message(payload)):
                out += encode_message(reply)
                self._trace(writer, _event("tx", "message", reply))
                if to_all:
                    self._broadcast(reply, skip=writer)
        except (IndexError, KeyError, ProtocolError):
            log.warning("ignoring malformed or out-of-range message %s", payload.hex())
            self._trace(writer, _event("rx", "error", payload, "malformed or out of range, ignored"))
        finally:
            self._current = None
            if out:
                writer.write(bytes(out))

    def _broadcast(self, payload: bytes, skip: Optional[asyncio.StreamWriter] = None) -> None:
        frame = encode_message(payload)
        for writer in list(self._clients):
            if writer is not skip and not writer.is_closing():
                writer.write(frame)
                self._trace(writer, _event("tx", "message", payload))

    def _connected_message(self, level: int, dest: int, extended: bool) -> bytes:
        return self._switch(Command.CONNECTED, dest, self._xpt[(0, dest)])

    @staticmethod
    def _switch(command: int, dest: int, source: int) -> bytes:
        return bytes((command, ((dest >> 7) & 0x07) << 4 | ((source >> 7) & 0x07), dest & 0x7F, source & 0x7F))

    def _answers(self, message: Message) -> List[Tuple[bytes, bool]]:
        if isinstance(message, Go):
            if message.is_done:
                return []
            stored, self._stored = self._stored, {}
            if not stored:
                self._salvo_event("empty", 0, 0)
                return [(bytes((Command.GO_DONE_ACKNOWLEDGE, 2)), False)]
            if message.action == 1:
                self._salvo_event("cleared", 0, len(stored))
                return [(bytes((Command.GO_DONE_ACKNOWLEDGE, 1)), False)]
            for dest, source in stored.items():
                self._take(0, dest, source)
            self._salvo_event("set", 0, len(stored))
            return [(bytes((Command.GO_DONE_ACKNOWLEDGE, 0)), True)]  # on all ports
        if not isinstance(message, Switch):
            return []
        dest = message.dest - 1
        if message.command == Command.INTERROGATE:
            if dest >= self.dests:
                # Source 1023 is reserved to say: destination out of range.
                return [(self._switch(Command.TALLY, dest, _OUT_OF_RANGE - 1), False)]
            return [(self._switch(Command.TALLY, dest, self._xpt[(0, dest)]), False)]
        if message.command not in (Command.CONNECT, Command.CONNECT_ON_GO):
            return []
        assert message.source is not None
        source = message.source - 1
        if dest >= self.dests or source >= self.sources:
            raise KeyError((dest, source))
        if message.command == Command.CONNECT:
            self._take(0, dest, source)
            return [(self._connected_message(0, dest, False), True)]
        self._stored[dest] = source
        self._salvo_event("stored", 0, 1)
        return [(self._switch(Command.CONNECT_ON_GO_ACKNOWLEDGE, dest, source), False)]
