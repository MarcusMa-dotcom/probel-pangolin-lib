"""asyncio client for routers that speak SW-P-08 over TCP."""

from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from .errors import (
    AckError,
    AckTimeoutError,
    NakError,
    NotConnectedError,
    ResponseTimeoutError,
    RouteRejectedError,
    SWP08Error,
    UnknownNameError,
    UnsupportedCommandError,
)
from .protocol import (
    ACK_FRAME,
    NAK_FRAME,
    Ack,
    Command,
    Crosspoint,
    DualControllerStatus,
    Frame,
    FrameDecoder,
    FrameError,
    Message,
    Nak,
    NameLength,
    Names,
    ProtocolError,
    ProtocolImplementation,
    SalvoCrosspoint,
    SalvoGo,
    TallyDump,
    TraceEvent,
    build_connect,
    build_dest_names_request,
    build_dual_controller_status_request,
    build_interrogate,
    build_protocol_implementation_request,
    build_salvo_connect,
    build_salvo_go,
    build_salvo_interrogate,
    build_source_names_request,
    build_tally_dump_request,
    command_name,
    encode_frame,
    needs_extended,
    parse_message,
)

__all__ = ["AsyncRouter", "RouteChange", "DEFAULT_PORT"]

log = logging.getLogger("probelpangolin")

DEFAULT_PORT = 8910

#: A source or destination: its 1-based number, or its name once names are loaded.
Port = Union[int, str]
Levels = Union[int, Iterable[int]]
ConnectionFactory = Callable[[], Awaitable[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]]

# Commands that are always sent, even if the router did not list them.
_ALWAYS_ALLOWED = frozenset(
    (Command.PROTOCOL_IMPLEMENTATION_REQUEST, Command.COMMAND_ENABLE)
)


@dataclass(frozen=True)
class RouteChange:
    """A crosspoint changed (or was learned for the first time)."""

    level: int
    dest: int
    source: int
    previous: Optional[int]  #: source that was routed before, ``None`` if unknown


class AsyncRouter:
    """Control a router / router controller via SW-P-08.

    All numbers are 1-based.  Typical use::

        async with AsyncRouter("10.0.0.20", fetch_names=True) as router:
            await router.route(source=5, dest=3)
            print(await router.get_route(3))

    Parameters
    ----------
    host, port:
        Address of the router's SW-P-08 TCP port (default port 8910).
    matrix:
        Matrix number, 1 in most systems.
    levels:
        Number of levels in the router.  Used for "all levels" operations
        such as :meth:`tally_dump_all`.
    extended:
        ``None`` (default) picks the extended command set automatically: when
        a number does not fit the standard commands (more than 1024 sources or
        destinations, 16 levels, 16 matrices) or when the router reports that
        it implements the extended command.  ``True`` forces extended commands,
        ``False`` forbids them.
    query_commands:
        Ask the router on connect which commands it implements.
    fetch_names, fetch_tally:
        Load source/destination names and the complete crosspoint state on
        every (re)connect.  :meth:`open` returns once this is done.
    name_length:
        Requested label length: 4, 8 or 12 characters.
    name_encoding:
        Character set of the labels.
    ack_timeout, attempts:
        Time to wait for ``DLE ACK`` and number of send attempts per message.
        One second and five attempts are what the specification recommends.
    wait_ack:
        Set to ``False`` for devices that never acknowledge.
    response_timeout:
        Default time to wait for the reply to a request.
    keepalive:
        Seconds of silence after which a crosspoint interrogate is sent as a
        connection check; ``None`` disables it.
    reconnect, reconnect_delay:
        Re-establish a lost connection automatically.
    connection_factory:
        Optional coroutine function returning ``(reader, writer)``.  Lets you
        run the protocol over something other than TCP (e.g. a serial port
        via ``serial_asyncio.open_serial_connection``).
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: int = DEFAULT_PORT,
        *,
        matrix: int = 1,
        levels: int = 1,
        extended: Optional[bool] = None,
        query_commands: bool = True,
        fetch_names: bool = False,
        fetch_tally: bool = False,
        name_length: Union[NameLength, int] = NameLength.EIGHT,
        name_encoding: str = "latin-1",
        ack_timeout: float = 1.0,
        attempts: int = 5,
        wait_ack: bool = True,
        response_timeout: float = 2.0,
        keepalive: Optional[float] = 30.0,
        reconnect: bool = True,
        reconnect_delay: float = 2.0,
        connect_timeout: float = 5.0,
        connection_factory: Optional[ConnectionFactory] = None,
    ) -> None:
        if host is None and connection_factory is None:
            raise ValueError("either host or connection_factory is required")
        if not 1 <= matrix <= 256:
            raise ValueError("matrix must be 1..256")
        if not 1 <= levels <= 256:
            raise ValueError("levels must be 1..256")
        if attempts < 1:
            raise ValueError("attempts must be at least 1")
        if extended is False and needs_extended(matrix, levels):
            raise ValueError("more than 16 matrices or levels need extended=True (or None)")
        self.host = host
        self.port = port
        self.matrix = matrix
        self.levels = levels
        self._extended = extended
        self._opt_query_commands = query_commands
        self._opt_fetch_names = fetch_names
        self._opt_fetch_tally = fetch_tally
        self._name_length = (
            NameLength.from_chars(name_length) if name_length in (4, 8, 12) else NameLength(name_length)
        )
        self._name_encoding = name_encoding
        self._ack_timeout = ack_timeout
        self._attempts = attempts
        self._wait_ack = wait_ack
        self._response_timeout = response_timeout
        self._keepalive = keepalive
        self._reconnect = reconnect
        self._reconnect_delay = reconnect_delay
        self._connect_timeout = connect_timeout
        self._connection_factory = connection_factory

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._decoder = FrameDecoder()
        self._runner: Optional[asyncio.Task] = None
        self._init_task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._background: Set[asyncio.Task] = set()
        self._closing = False
        self._connected = asyncio.Event()
        self._ready = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._ack_future: Optional[asyncio.Future] = None
        self._last_tx = 0.0

        self._commands: Optional[FrozenSet[int]] = None
        self._implementation: Optional[ProtocolImplementation] = None
        self._routes: Dict[int, Dict[int, int]] = {}
        # crosspoints stored in salvo groups, as the router acknowledged them
        self._salvos: Dict[int, Dict[Tuple[int, int], int]] = {}
        self._source_names: Dict[int, str] = {}
        self._dest_names: Dict[int, str] = {}

        self._xpt_waiters: Dict[Tuple[int, int], List[Tuple[asyncio.Future, bool]]] = {}
        self._cmd_waiters: Dict[int, List[asyncio.Future]] = {}
        self._activity: Dict[Tuple[str, Any], Set[asyncio.Event]] = {}

        self._route_cbs: List[Callable[[RouteChange], Any]] = []
        self._names_cbs: List[Callable[[Names], Any]] = []
        self._connection_cbs: List[Callable[[bool], Any]] = []
        self._message_cbs: List[Callable[[Message], Any]] = []
        self._trace_cbs: List[Callable[[TraceEvent], Any]] = []

    def __repr__(self) -> str:
        state = "connected" if self.connected else "disconnected"
        return f"<AsyncRouter {self.host}:{self.port} matrix={self.matrix} {state}>"

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def open(self) -> "AsyncRouter":
        """Connect and run the configured start-up queries.

        Raises :class:`NotConnectedError` if the first connection attempt
        fails.  Later connection losses are handled by the reconnect logic.
        """
        if self._runner is not None:
            return self
        self._closing = False
        try:
            reader, writer = await self._open_transport()
        except (OSError, asyncio.TimeoutError) as exc:
            raise NotConnectedError(f"cannot connect to {self._target}: {exc or type(exc).__name__}") from exc
        self._attach(reader, writer)
        self._runner = asyncio.ensure_future(self._run())
        await self._ready.wait()
        if not self.connected and not self._reconnect:
            await self.close()
            raise NotConnectedError(f"connection to {self._target} closed during start-up")
        return self

    async def close(self) -> None:
        """Disconnect and stop reconnecting."""
        self._closing = True
        runner, self._runner = self._runner, None
        if runner is not None:
            runner.cancel()
            await asyncio.gather(runner, return_exceptions=True)
        writer = self._writer
        self._detach()
        if writer is not None:
            try:
                await writer.wait_closed()
            except Exception:  # pragma: no cover - transport specific
                pass

    async def __aenter__(self) -> "AsyncRouter":
        return await self.open()

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    async def wait_connected(self, timeout: Optional[float] = None) -> None:
        """Block until the connection is up and the start-up queries are done."""
        try:
            await asyncio.wait_for(self._wait_ready(), timeout)
        except asyncio.TimeoutError:
            raise NotConnectedError(f"not connected to {self._target}") from None

    async def _wait_ready(self) -> None:
        while not (self._connected.is_set() and self._ready.is_set()):
            await self._connected.wait()
            await self._ready.wait()

    @property
    def _target(self) -> str:
        return f"{self.host}:{self.port}" if self.host else "router"

    async def _open_transport(self) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if self._connection_factory is not None:
            coro = self._connection_factory()
        else:
            coro = asyncio.open_connection(self.host, self.port)
        return await asyncio.wait_for(coro, self._connect_timeout)

    def _attach(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader, self._writer = reader, writer
        self._decoder.reset()
        self._commands = None
        self._implementation = None
        self._routes = {}
        self._salvos = {}
        self._ready.clear()
        self._connected.set()
        self._last_tx = asyncio.get_running_loop().time()
        log.info("connected to %s", self._target)
        self._init_task = asyncio.ensure_future(self._initialise())
        if self._keepalive:
            self._keepalive_task = asyncio.ensure_future(self._keepalive_loop())
        self._emit(self._connection_cbs, True)

    def _detach(self) -> None:
        was_connected = self._connected.is_set()
        self._connected.clear()
        current = asyncio.current_task()
        for task in (self._init_task, self._keepalive_task):
            if task is not None and task is not current and not task.done():
                task.cancel()
        self._init_task = self._keepalive_task = None
        if self._writer is not None:
            try:
                self._writer.close()
            except Exception:  # pragma: no cover - transport specific
                pass
            self._writer = None
        self._reader = None
        error = NotConnectedError(f"connection to {self._target} lost")
        if self._ack_future is not None and not self._ack_future.done():
            self._ack_future.set_exception(error)
        for waiters in self._xpt_waiters.values():
            for fut, _ in waiters:
                if not fut.done():
                    fut.set_exception(error)
        for futures in self._cmd_waiters.values():
            for fut in futures:
                if not fut.done():
                    fut.set_exception(error)
        self._ready.set()
        if was_connected:
            self._emit(self._connection_cbs, False)

    async def _run(self) -> None:
        try:
            while True:
                try:
                    await self._read_loop()
                except (OSError, asyncio.IncompleteReadError) as exc:
                    log.warning("connection error: %s", exc)
                self._detach()
                if self._closing or not self._reconnect:
                    return
                log.warning("connection to %s lost, reconnecting", self._target)
                while True:
                    await asyncio.sleep(self._reconnect_delay)
                    try:
                        reader, writer = await self._open_transport()
                    except (OSError, asyncio.TimeoutError) as exc:
                        log.debug("reconnect failed: %s", exc)
                        continue
                    self._attach(reader, writer)
                    break
        finally:
            self._detach()

    async def _read_loop(self) -> None:
        reader = self._reader
        assert reader is not None
        while True:
            data = await reader.read(4096)
            if not data:
                return
            for event in self._decoder.feed(data):
                try:
                    self._handle_event(event)
                except Exception:  # a broken callback must not kill the connection
                    log.exception("error while handling %r", event)

    async def _initialise(self) -> None:
        try:
            if self._opt_query_commands:
                try:
                    await self.query_commands()
                except SWP08Error as exc:
                    log.warning("router did not report its implemented commands: %s", exc)
            if self._opt_fetch_names:
                try:
                    await self.read_names()
                except SWP08Error as exc:
                    log.warning("could not read names: %s", exc)
            if self._opt_fetch_tally:
                try:
                    await self.tally_dump_all()
                except SWP08Error as exc:
                    log.warning("could not read crosspoint tally dump: %s", exc)
        finally:
            self._ready.set()

    async def _keepalive_loop(self) -> None:
        interval = float(self._keepalive or 0)
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(interval)
            if loop.time() - self._last_tx < interval or self._send_lock.locked():
                continue
            try:
                extended = self._extended_for(Command.EXT_CROSSPOINT_INTERROGATE)
                await self.send(build_interrogate(1, 1, self.matrix, extended=extended))
            except AckError as exc:
                log.warning("keepalive failed (%s), dropping connection", exc)
                if self._writer is not None:
                    self._writer.close()  # read loop sees EOF and reconnects
                return
            except SWP08Error as exc:
                log.debug("keepalive skipped: %s", exc)

    # ------------------------------------------------------------------
    # Sending
    # ------------------------------------------------------------------

    def _write(self, data: bytes) -> None:
        if self._writer is not None:
            self._writer.write(data)
            if self._trace_cbs:
                self._emit(self._trace_cbs, TraceEvent("tx", "ack" if data == ACK_FRAME else "nak"))

    async def write_raw(self, data: Union[bytes, bytearray, List[int]]) -> None:
        """Write bytes exactly as given: no framing, no checksum, no waiting for an ACK.

        Meant for testing how a router reacts to broken or unusual frames.
        """
        data = bytes(data)
        writer = self._writer
        if writer is None or not self._connected.is_set():
            raise NotConnectedError(f"not connected to {self._target}")
        async with self._send_lock:
            writer.write(data)
            self._emit(self._trace_cbs, TraceEvent("tx", "raw", data))
            try:
                await writer.drain()
            except OSError as exc:
                raise NotConnectedError(f"connection to {self._target} lost: {exc}") from exc
            self._last_tx = asyncio.get_running_loop().time()

    async def send(self, payload: Union[bytes, bytearray, List[int]], *, check: bool = True) -> None:
        """Send one raw message (DATA bytes) and wait for the router's ACK.

        Messages are sent strictly one after another.  Raises
        :class:`NakError` / :class:`AckTimeoutError` when all attempts failed.
        With ``check=False`` the message goes out even if the router did not
        list its command as implemented.
        """
        payload = bytes(payload)
        frame = encode_frame(payload)
        command = payload[0]
        if (check and self._commands and command not in _ALWAYS_ALLOWED
                and command not in self._commands):
            raise UnsupportedCommandError(
                f"router does not implement {command_name(command)} (0x{command:02X})"
            )
        loop = asyncio.get_running_loop()
        async with self._send_lock:
            got_nak = False
            for attempt in range(1, self._attempts + 1):
                writer = self._writer
                if writer is None or not self._connected.is_set():
                    raise NotConnectedError(f"not connected to {self._target}")
                ack: asyncio.Future = loop.create_future()
                self._ack_future = ack
                try:
                    log.debug("tx %s", frame.hex())
                    writer.write(frame)
                    if self._trace_cbs:
                        note = f"attempt {attempt} of {self._attempts}" if attempt > 1 else ""
                        self._emit(self._trace_cbs, TraceEvent("tx", "message", payload, note))
                    try:
                        await writer.drain()
                    except OSError as exc:
                        raise NotConnectedError(f"connection to {self._target} lost: {exc}") from exc
                    self._last_tx = loop.time()
                    if not self._wait_ack:
                        return
                    try:
                        accepted = await asyncio.wait_for(ack, self._ack_timeout)
                    except NotConnectedError:
                        raise
                    except asyncio.TimeoutError:
                        got_nak = False
                        log.warning(
                            "no ACK for %s (attempt %d/%d)",
                            command_name(command), attempt, self._attempts,
                        )
                        continue
                finally:
                    self._retire(ack)
                    if self._ack_future is ack:
                        self._ack_future = None
                if accepted:
                    return
                got_nak = True
                log.warning(
                    "NAK for %s (attempt %d/%d)", command_name(command), attempt, self._attempts
                )
        if got_nak:
            raise NakError(f"router rejected {command_name(command)} with NAK")
        raise AckTimeoutError(f"router did not acknowledge {command_name(command)}")

    async def request(
        self,
        payload: Union[bytes, bytearray, List[int]],
        reply: Union[int, Iterable[int]],
        *,
        timeout: Optional[float] = None,
        check: bool = True,
    ) -> Message:
        """Send a raw message and return the first reply with one of the given command bytes.

        This is the escape hatch for commands without a dedicated method
        (protects, salvos, ...): the reply comes back as
        :class:`~probelpangolin.protocol.UnknownMessage` with the raw payload.
        """
        codes = [int(reply)] if isinstance(reply, int) else [int(c) for c in reply]
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        for code in codes:
            self._cmd_waiters.setdefault(code, []).append(fut)
        try:
            await self.send(payload, check=check)
            return await self._await_reply(fut, timeout, command_name(codes[0]))
        finally:
            self._retire(fut)
            for code in codes:
                waiters = self._cmd_waiters.get(code, [])
                if fut in waiters:
                    waiters.remove(fut)
                if not waiters:
                    self._cmd_waiters.pop(code, None)

    @staticmethod
    def _retire(fut: asyncio.Future) -> None:
        """Drop a waiter without leaving an unretrieved exception behind."""
        if fut.done() and not fut.cancelled():
            fut.exception()
        else:
            fut.cancel()

    async def _await_reply(self, fut: asyncio.Future, timeout: Optional[float], what: str) -> Any:
        try:
            return await asyncio.wait_for(fut, timeout or self._response_timeout)
        except asyncio.TimeoutError:
            raise ResponseTimeoutError(f"no {what} reply from router") from None

    def _extended_for(
        self, ext_command: int, *, level: int = 1, dest: int = 1, source: int = 1
    ) -> bool:
        out_of_range = needs_extended(self.matrix, level, dest, source)
        if self._extended is True:
            return True
        if self._extended is False:
            if out_of_range:
                raise ProtocolError(
                    "these numbers need the extended command set, but extended=False"
                )
            return False
        if out_of_range:
            return True
        return bool(self._commands) and int(ext_command) in self._commands  # type: ignore[operator]

    # ------------------------------------------------------------------
    # Receiving
    # ------------------------------------------------------------------

    def _handle_event(self, event: Any) -> None:
        if isinstance(event, (Ack, Nak)):
            if self._trace_cbs:
                self._emit(self._trace_cbs, TraceEvent("rx", "ack" if isinstance(event, Ack) else "nak"))
            fut = self._ack_future
            if fut is not None and not fut.done():
                fut.set_result(isinstance(event, Ack))
            else:
                log.debug("unexpected %s", type(event).__name__.upper())
            return
        if isinstance(event, FrameError):
            log.warning("bad frame from router (%s): %s", event.reason, event.raw.hex())
            self._emit(self._trace_cbs, TraceEvent("rx", "error", event.raw, event.reason))
            self._write(NAK_FRAME)
            return
        assert isinstance(event, Frame)
        if self._trace_cbs:
            self._emit(self._trace_cbs, TraceEvent("rx", "message", event.payload))
        self._write(ACK_FRAME)
        log.debug("rx %s", event.payload.hex())
        try:
            message = parse_message(event.payload, name_encoding=self._name_encoding)
        except ProtocolError as exc:
            log.warning("cannot parse message %s: %s", event.payload.hex(), exc)
            return
        self._dispatch(message)

    def _dispatch(self, message: Message) -> None:
        if isinstance(message, Crosspoint):
            if message.matrix == self.matrix:
                self._set_route(message.level, message.dest, message.source)
                for fut, connected_only in self._xpt_waiters.get((message.level, message.dest), ()):
                    if not fut.done() and (message.is_connected or not connected_only):
                        fut.set_result(message)
        elif isinstance(message, TallyDump):
            if message.matrix == self.matrix:
                for dest, source in message.routes().items():
                    self._set_route(message.level, dest, source)
                self._mark_activity(("dump", message.level))
        elif isinstance(message, Names):
            if message.matrix == self.matrix:
                target = self._source_names if message.kind == "source" else self._dest_names
                target.update(message.as_dict())
                self._mark_activity(("names", message.kind))
                self._emit(self._names_cbs, message)
        elif isinstance(message, ProtocolImplementation):
            self._commands = frozenset(message.commands)
            self._implementation = message
        elif isinstance(message, SalvoCrosspoint):
            # The router sends no Connected messages for a salvo.  Its acknowledges are
            # what a controller has to keep its tallies up to date with.
            if (message.command == Command.CROSSPOINT_CONNECT_ON_GO_GROUP_SALVO_ACK
                    and message.matrix == self.matrix):
                self._salvos.setdefault(message.salvo, {})[(message.level, message.dest)] = message.source
        elif isinstance(message, SalvoGo):
            if message.is_done and message.action in (0, 1):
                stored = self._salvos.pop(message.salvo, {})
                if message.action == 0:
                    for (level, dest), source in stored.items():
                        self._set_route(level, dest, source)

        for fut in self._cmd_waiters.get(message.command, ()):
            if not fut.done():
                fut.set_result(message)
        self._emit(self._message_cbs, message)

    def _set_route(self, level: int, dest: int, source: int) -> None:
        table = self._routes.setdefault(level, {})
        previous = table.get(dest)
        if previous == source:
            return
        table[dest] = source
        self._emit(self._route_cbs, RouteChange(level, dest, source, previous))

    def _mark_activity(self, key: Tuple[str, Any]) -> None:
        for event in self._activity.get(key, ()):
            event.set()

    def _emit(self, callbacks: List[Callable[[Any], Any]], argument: Any) -> None:
        for callback in list(callbacks):
            try:
                result = callback(argument)
                if inspect.isawaitable(result):
                    task = asyncio.ensure_future(result)
                    self._background.add(task)
                    task.add_done_callback(self._background.discard)
            except Exception:
                log.exception("callback %r failed", callback)

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------

    @staticmethod
    def _subscribe(callbacks: List[Any], callback: Any) -> Callable[[], None]:
        callbacks.append(callback)

        def unsubscribe() -> None:
            if callback in callbacks:
                callbacks.remove(callback)

        return unsubscribe

    def on_route(self, callback: Callable[[RouteChange], Any]) -> Callable[[], None]:
        """Call ``callback(RouteChange)`` whenever a crosspoint changes.

        The callback may be a plain function or a coroutine function.
        Returns a function that removes the subscription again.
        """
        return self._subscribe(self._route_cbs, callback)

    def on_names(self, callback: Callable[[Names], Any]) -> Callable[[], None]:
        """Call ``callback(Names)`` for every part of a name list that arrives."""
        return self._subscribe(self._names_cbs, callback)

    def on_connection(self, callback: Callable[[bool], Any]) -> Callable[[], None]:
        """Call ``callback(True/False)`` when the connection comes up or goes down."""
        return self._subscribe(self._connection_cbs, callback)

    def on_message(self, callback: Callable[[Message], Any]) -> Callable[[], None]:
        """Call ``callback(message)`` for every valid message from the router."""
        return self._subscribe(self._message_cbs, callback)

    def on_trace(self, callback: Callable[[TraceEvent], Any]) -> Callable[[], None]:
        """Call ``callback(TraceEvent)`` for everything sent and received.

        That includes every ACK and NAK, repeated attempts and broken frames,
        which makes it the hook for a protocol trace.
        """
        return self._subscribe(self._trace_cbs, callback)

    async def changes(self) -> AsyncIterator[RouteChange]:
        """Async iterator over crosspoint changes::

            async for change in router.changes():
                print(change.dest, "<-", change.source)

        If you leave the loop early, wrap the iterator in
        ``contextlib.aclosing()`` so the subscription is removed right away.
        """
        queue: asyncio.Queue = asyncio.Queue()
        unsubscribe = self.on_route(queue.put_nowait)
        try:
            while True:
                yield await queue.get()
        finally:
            unsubscribe()

    # ------------------------------------------------------------------
    # Names
    # ------------------------------------------------------------------

    @property
    def source_names(self) -> Dict[int, str]:
        """``{source number: name}`` as last read from the router."""
        return dict(self._source_names)

    @property
    def dest_names(self) -> Dict[int, str]:
        """``{destination number: name}`` as last read from the router."""
        return dict(self._dest_names)

    def source_name(self, source: int) -> Optional[str]:
        return self._source_names.get(source)

    def dest_name(self, dest: int) -> Optional[str]:
        return self._dest_names.get(dest)

    def find_source(self, name: str) -> int:
        """Number of the source with this name (exact match, then case-insensitive)."""
        return self._find(self._source_names, name, "source")

    def find_dest(self, name: str) -> int:
        """Number of the destination with this name."""
        return self._find(self._dest_names, name, "destination")

    @staticmethod
    def _find(names: Dict[int, str], name: str, what: str) -> int:
        if not names:
            raise UnknownNameError(
                f"no {what} names loaded - call read_names() or use fetch_names=True"
            )
        matches = [number for number, label in names.items() if label == name]
        if not matches:
            folded = name.strip().casefold()
            matches = [n for n, label in names.items() if label.strip().casefold() == folded]
        if not matches:
            raise UnknownNameError(f"unknown {what} name {name!r}")
        if len(matches) > 1:
            raise UnknownNameError(f"{what} name {name!r} is ambiguous: numbers {sorted(matches)}")
        return matches[0]

    def _source(self, source: Port) -> int:
        return self.find_source(source) if isinstance(source, str) else source

    def _dest(self, dest: Port) -> int:
        return self.find_dest(dest) if isinstance(dest, str) else dest

    async def read_names(
        self, *, settle: float = 0.3, timeout: Optional[float] = None
    ) -> Tuple[Dict[int, str], Dict[int, str]]:
        """Read all source and destination names; returns ``(sources, dests)``.

        The router sends the names in several parts without an end marker, so
        the list counts as complete after ``settle`` seconds without a new part.
        """
        extended = self._extended_for(Command.EXT_SOURCE_NAMES_REQUEST)
        self._source_names = {}
        self._dest_names = {}
        await self._request_parts(
            build_source_names_request(self.matrix, 1, self._name_length, extended=extended),
            ("names", "source"), settle, timeout, "source names",
        )
        extended = self._extended_for(Command.EXT_DEST_NAMES_REQUEST)
        await self._request_parts(
            build_dest_names_request(self.matrix, self._name_length, extended=extended),
            ("names", "dest"), settle, timeout, "destination names",
        )
        return self.source_names, self.dest_names

    async def _request_parts(
        self,
        payload: bytes,
        key: Tuple[str, Any],
        settle: float,
        timeout: Optional[float],
        what: str,
    ) -> None:
        """Send a request whose reply arrives in an unknown number of parts."""
        event = asyncio.Event()
        self._activity.setdefault(key, set()).add(event)
        try:
            await self.send(payload)
            try:
                await asyncio.wait_for(event.wait(), timeout or self._response_timeout)
            except asyncio.TimeoutError:
                raise ResponseTimeoutError(f"no {what} reply from router") from None
            while self.connected:
                event.clear()
                try:
                    await asyncio.wait_for(event.wait(), settle)
                except asyncio.TimeoutError:
                    break
        finally:
            listeners = self._activity.get(key)
            if listeners is not None:
                listeners.discard(event)
                if not listeners:
                    self._activity.pop(key, None)

    # ------------------------------------------------------------------
    # Crosspoints
    # ------------------------------------------------------------------

    @property
    def all_levels(self) -> range:
        """All configured levels, handy as ``level=router.all_levels``."""
        return range(1, self.levels + 1)

    @staticmethod
    def _level_list(level: Levels) -> List[int]:
        return [level] if isinstance(level, int) else list(level)

    async def route(
        self,
        source: Port,
        dest: Port,
        level: Levels = 1,
        *,
        confirm: bool = False,
        timeout: Optional[float] = None,
    ) -> None:
        """Route ``source`` to ``dest`` on one or several levels.

        ``source`` and ``dest`` are 1-based numbers, or names if names have
        been loaded.  Without ``confirm`` the call returns as soon as the
        router acknowledged the message.  With ``confirm=True`` it waits for
        the router's *Crosspoint Connected* message and raises
        :class:`RouteRejectedError` if a different source ended up on the
        destination (e.g. because it is protected) or
        :class:`ResponseTimeoutError` if no confirmation arrives.
        """
        src = self._source(source)
        dst = self._dest(dest)
        for lv in self._level_list(level):
            extended = self._extended_for(
                Command.EXT_CROSSPOINT_CONNECT, level=lv, dest=dst, source=src
            )
            payload = build_connect(src, dst, lv, self.matrix, extended=extended)
            if not confirm:
                await self.send(payload)
                continue
            reply = await self._crosspoint_request(payload, lv, dst, True, timeout)
            if reply.source != src:
                raise RouteRejectedError(dst, lv, src, reply.source)

    async def get_route(self, dest: Port, level: int = 1, *, timeout: Optional[float] = None) -> int:
        """Ask the router which source is routed to ``dest`` right now."""
        dst = self._dest(dest)
        extended = self._extended_for(Command.EXT_CROSSPOINT_INTERROGATE, level=level, dest=dst)
        payload = build_interrogate(dst, level, self.matrix, extended=extended)
        reply = await self._crosspoint_request(payload, level, dst, False, timeout)
        return reply.source

    async def get_routes(
        self, dest: Port, levels: Optional[Iterable[int]] = None, *, timeout: Optional[float] = None
    ) -> Dict[int, int]:
        """``{level: source}`` for one destination, by default on all levels."""
        result = {}
        for lv in self.all_levels if levels is None else levels:
            result[lv] = await self.get_route(dest, lv, timeout=timeout)
        return result

    async def _crosspoint_request(
        self, payload: bytes, level: int, dest: int, connected_only: bool, timeout: Optional[float]
    ) -> Crosspoint:
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        entry = (fut, connected_only)
        key = (level, dest)
        self._xpt_waiters.setdefault(key, []).append(entry)
        try:
            await self.send(payload)
            what = "crosspoint connected" if connected_only else "crosspoint tally"
            return await self._await_reply(fut, timeout, f"{what} (dest {dest}, level {level})")
        finally:
            self._retire(fut)
            waiters = self._xpt_waiters.get(key, [])
            if entry in waiters:
                waiters.remove(entry)
            if not waiters:
                self._xpt_waiters.pop(key, None)

    async def tally_dump(
        self, level: int = 1, *, settle: float = 0.3, timeout: Optional[float] = None
    ) -> Dict[int, int]:
        """Read the complete crosspoint state of one level: ``{dest: source}``."""
        extended = self._extended_for(Command.EXT_CROSSPOINT_TALLY_DUMP_REQUEST, level=level)
        await self._request_parts(
            build_tally_dump_request(level, self.matrix, extended=extended),
            ("dump", level), settle, timeout, f"tally dump (level {level})",
        )
        return dict(self._routes.get(level, {}))

    async def tally_dump_all(
        self, *, settle: float = 0.3, timeout: Optional[float] = None
    ) -> Dict[int, Dict[int, int]]:
        """Tally dump of all configured levels: ``{level: {dest: source}}``."""
        return {
            lv: await self.tally_dump(lv, settle=settle, timeout=timeout) for lv in self.all_levels
        }

    # Cached state -------------------------------------------------------

    @property
    def routes(self) -> Dict[int, Dict[int, int]]:
        """Crosspoints known so far: ``{level: {dest: source}}``.

        Filled by tally dumps, interrogates and every tally the router sends
        on its own.  Cleared on reconnect.
        """
        return {level: dict(table) for level, table in self._routes.items()}

    def source_of(self, dest: Port, level: int = 1) -> Optional[int]:
        """Cached source of a destination, ``None`` if not known yet (no I/O)."""
        return self._routes.get(level, {}).get(self._dest(dest))

    def destinations_of(self, source: Port, level: int = 1) -> List[int]:
        """Cached list of destinations a source is routed to (no I/O)."""
        src = self._source(source)
        return sorted(d for d, s in self._routes.get(level, {}).items() if s == src)

    # ------------------------------------------------------------------
    # Protocol implementation
    # ------------------------------------------------------------------

    @property
    def implemented_commands(self) -> Optional[FrozenSet[int]]:
        """Command bytes the router reported, ``None`` if it was not asked."""
        return self._commands

    @property
    def implementation(self) -> Optional[ProtocolImplementation]:
        """The router's answer as it came, with ``transmitted`` and ``received`` kept apart."""
        return self._implementation

    # ------------------------------------------------------------------
    # Salvo groups and dual controllers
    # ------------------------------------------------------------------

    async def salvo_add(
        self, source: Port, dest: Port, salvo: int, level: Levels = 1, *, timeout: Optional[float] = None
    ) -> None:
        """Store a crosspoint in a salvo group (0 to 127) and wait for the router's acknowledge.

        Nothing is switched until :meth:`salvo_go`.  Salvo groups exist in the
        standard command set only: sources and destinations up to 1024.
        """
        src = self._source(source)
        dst = self._dest(dest)
        for lv in self._level_list(level):
            await self.request(
                build_salvo_connect(src, dst, salvo, lv, self.matrix),
                Command.CROSSPOINT_CONNECT_ON_GO_GROUP_SALVO_ACK,
                timeout=timeout,
            )

    async def salvo_go(self, salvo: int, *, clear: bool = False, timeout: Optional[float] = None) -> int:
        """Set all crosspoints stored in a salvo group at once, or clear the group.

        Returns the router's answer: 0 = crosspoints set, 1 = stored
        crosspoints cleared, 2 = the group held nothing.
        """
        reply = await self.request(
            build_salvo_go(salvo, clear=clear), Command.CROSSPOINT_GO_DONE_GROUP_SALVO, timeout=timeout
        )
        assert isinstance(reply, SalvoGo)
        return reply.action

    async def salvo_read(self, salvo: int, *, timeout: Optional[float] = None) -> List[Tuple[int, int, int]]:
        """What a salvo group holds: a list of ``(level, destination, source)``."""
        entries: List[Tuple[int, int, int]] = []
        for index in range(128):
            reply = await self.request(
                build_salvo_interrogate(salvo, index), Command.CROSSPOINT_GROUP_SALVO_TALLY, timeout=timeout
            )
            assert isinstance(reply, SalvoCrosspoint)
            if reply.has_data:
                entries.append((reply.level, reply.dest, reply.source))
            if reply.is_last:
                break
        return entries

    async def dual_controller_status(self, *, timeout: Optional[float] = None) -> DualControllerStatus:
        """Ask a router with two controllers whether this one is the active one."""
        reply = await self.request(
            build_dual_controller_status_request(), Command.DUAL_CONTROLLER_STATUS_RESPONSE, timeout=timeout
        )
        assert isinstance(reply, DualControllerStatus)
        return reply

    async def query_commands(self, *, timeout: Optional[float] = None) -> FrozenSet[int]:
        """Ask the router which commands it implements."""
        await self.request(
            build_protocol_implementation_request(),
            Command.PROTOCOL_IMPLEMENTATION_RESPONSE,
            timeout=timeout,
        )
        return self._commands or frozenset()
