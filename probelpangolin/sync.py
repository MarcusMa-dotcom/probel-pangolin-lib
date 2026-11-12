"""Blocking front end for :class:`probelpangolin.AsyncRouter`.

The asyncio client runs in a background thread; every method here simply
waits for the result.  Use this in scripts, Jupyter or anywhere without an
event loop::

    from probelpangolin import Router

    with Router("10.0.0.20", fetch_names=True) as router:
        router.route(source=5, dest=3)
        print(router.get_route(3))
"""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future
from typing import Any, Callable, Coroutine, Dict, FrozenSet, Iterable, List, Optional, Tuple, TypeVar, Union

from .client import AsyncRouter, Levels, Port, RouteChange
from .protocol import DualControllerStatus, Message, Names, ProtocolImplementation, TraceEvent

__all__ = ["Router"]

T = TypeVar("T")


class Router:
    """Synchronous SW-P-08 client.  Takes the same arguments as :class:`AsyncRouter`.

    Callbacks registered with :meth:`on_route` etc. are called from the
    background thread; keep them short and thread-safe.
    """

    #: the asyncio client this front end runs; :mod:`probelpangolin.swp02` puts its own here
    _client_class = AsyncRouter

    def __init__(self, host: Optional[str] = None, port: int = 8910, **options: Any) -> None:
        self._args = (host, port)
        self._options = options
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._router: Optional[AsyncRouter] = None

    def __repr__(self) -> str:
        state = "connected" if self.connected else "disconnected"
        return f"<Router {self._args[0]}:{self._args[1]} {state}>"

    # -- plumbing ----------------------------------------------------------

    def _start_loop(self) -> None:
        loop = asyncio.new_event_loop()
        started = threading.Event()

        def run() -> None:
            asyncio.set_event_loop(loop)
            loop.call_soon(started.set)
            try:
                loop.run_forever()
            finally:
                try:
                    pending = asyncio.all_tasks(loop)
                    for task in pending:
                        task.cancel()
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                finally:
                    loop.close()

        thread = threading.Thread(target=run, name="probelpangolin", daemon=True)
        thread.start()
        started.wait()
        self._loop, self._thread = loop, thread

    def _stop_loop(self) -> None:
        loop, thread = self._loop, self._thread
        self._loop = self._thread = self._router = None
        if loop is not None and thread is not None:
            loop.call_soon_threadsafe(loop.stop)
            thread.join()

    def _need(self) -> Tuple[asyncio.AbstractEventLoop, AsyncRouter]:
        if self._loop is None or self._router is None:
            raise RuntimeError("router is not open - call open() or use a with block")
        return self._loop, self._router

    def _await(self, make: Callable[[AsyncRouter], Coroutine[Any, Any, T]]) -> T:
        loop, router = self._need()
        return asyncio.run_coroutine_threadsafe(make(router), loop).result()

    def _call(self, func: Callable[[AsyncRouter], T]) -> T:
        """Run a plain function inside the loop thread and return its result."""
        loop, router = self._need()
        future: "Future[T]" = Future()

        def run() -> None:
            try:
                future.set_result(func(router))
            except BaseException as exc:  # noqa: BLE001 - forwarded to the caller
                future.set_exception(exc)

        loop.call_soon_threadsafe(run)
        return future.result()

    # -- lifecycle ---------------------------------------------------------

    def open(self) -> "Router":
        """Connect; raises :class:`probelpangolin.NotConnectedError` if that fails."""
        if self._router is not None:
            return self
        self._start_loop()
        assert self._loop is not None
        host, port = self._args

        async def create() -> AsyncRouter:
            router = self._client_class(host, port, **self._options)
            await router.open()
            return router

        try:
            self._router = asyncio.run_coroutine_threadsafe(create(), self._loop).result()
        except BaseException:
            self._stop_loop()
            raise
        return self

    def close(self) -> None:
        """Disconnect and stop the background thread."""
        if self._router is None:
            return
        try:
            self._await(lambda r: r.close())
        finally:
            self._stop_loop()

    def __enter__(self) -> "Router":
        return self.open()

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    @property
    def connected(self) -> bool:
        router = self._router
        return bool(router and router.connected)

    def wait_connected(self, timeout: Optional[float] = None) -> None:
        self._await(lambda r: r.wait_connected(timeout))

    # -- crosspoints -------------------------------------------------------

    def route(
        self,
        source: Port,
        dest: Port,
        level: Levels = 1,
        *,
        confirm: bool = False,
        timeout: Optional[float] = None,
    ) -> None:
        """Route ``source`` to ``dest``; see :meth:`AsyncRouter.route`."""
        self._await(lambda r: r.route(source, dest, level, confirm=confirm, timeout=timeout))

    def get_route(self, dest: Port, level: int = 1, *, timeout: Optional[float] = None) -> int:
        """Ask the router which source is on ``dest``."""
        return self._await(lambda r: r.get_route(dest, level, timeout=timeout))

    def get_routes(
        self, dest: Port, levels: Optional[Iterable[int]] = None, *, timeout: Optional[float] = None
    ) -> Dict[int, int]:
        """``{level: source}`` for one destination."""
        return self._await(lambda r: r.get_routes(dest, levels, timeout=timeout))

    def tally_dump(
        self, level: int = 1, *, settle: float = 0.3, timeout: Optional[float] = None
    ) -> Dict[int, int]:
        """Complete crosspoint state of one level: ``{dest: source}``."""
        return self._await(lambda r: r.tally_dump(level, settle=settle, timeout=timeout))

    def tally_dump_all(
        self, *, settle: float = 0.3, timeout: Optional[float] = None
    ) -> Dict[int, Dict[int, int]]:
        """Complete crosspoint state: ``{level: {dest: source}}``."""
        return self._await(lambda r: r.tally_dump_all(settle=settle, timeout=timeout))

    @property
    def routes(self) -> Dict[int, Dict[int, int]]:
        """Cached crosspoints ``{level: {dest: source}}``."""
        return self._call(lambda r: r.routes)

    def source_of(self, dest: Port, level: int = 1) -> Optional[int]:
        """Cached source of a destination (no I/O)."""
        return self._call(lambda r: r.source_of(dest, level))

    def destinations_of(self, source: Port, level: int = 1) -> List[int]:
        """Cached destinations a source is routed to (no I/O)."""
        return self._call(lambda r: r.destinations_of(source, level))

    @property
    def all_levels(self) -> range:
        return self._need()[1].all_levels

    # -- names -------------------------------------------------------------

    def read_names(
        self, *, settle: float = 0.3, timeout: Optional[float] = None
    ) -> Tuple[Dict[int, str], Dict[int, str]]:
        """Read all names; returns ``(sources, dests)``."""
        return self._await(lambda r: r.read_names(settle=settle, timeout=timeout))

    @property
    def source_names(self) -> Dict[int, str]:
        return self._call(lambda r: r.source_names)

    @property
    def dest_names(self) -> Dict[int, str]:
        return self._call(lambda r: r.dest_names)

    def source_name(self, source: int) -> Optional[str]:
        return self._call(lambda r: r.source_name(source))

    def dest_name(self, dest: int) -> Optional[str]:
        return self._call(lambda r: r.dest_name(dest))

    def find_source(self, name: str) -> int:
        return self._call(lambda r: r.find_source(name))

    def find_dest(self, name: str) -> int:
        return self._call(lambda r: r.find_dest(name))

    # -- raw access --------------------------------------------------------

    @property
    def implemented_commands(self) -> Optional[FrozenSet[int]]:
        return self._need()[1].implemented_commands

    def query_commands(self, *, timeout: Optional[float] = None) -> FrozenSet[int]:
        return self._await(lambda r: r.query_commands(timeout=timeout))

    @property
    def implementation(self) -> Optional[ProtocolImplementation]:
        return self._need()[1].implementation

    # -- salvo groups and dual controllers ----------------------------------

    def salvo_add(
        self, source: Port, dest: Port, salvo: int, level: Levels = 1, *, timeout: Optional[float] = None
    ) -> None:
        """Store a crosspoint in a salvo group (0 to 127); nothing switches until :meth:`salvo_go`."""
        self._await(lambda r: r.salvo_add(source, dest, salvo, level, timeout=timeout))

    def salvo_go(self, salvo: int, *, clear: bool = False, timeout: Optional[float] = None) -> int:
        """Set the stored crosspoints of a salvo group, or clear them.  Returns 0, 1 or 2."""
        return self._await(lambda r: r.salvo_go(salvo, clear=clear, timeout=timeout))

    def salvo_read(self, salvo: int, *, timeout: Optional[float] = None) -> List[Tuple[int, int, int]]:
        """What a salvo group holds: ``(level, destination, source)``."""
        return self._await(lambda r: r.salvo_read(salvo, timeout=timeout))

    def dual_controller_status(self, *, timeout: Optional[float] = None) -> DualControllerStatus:
        """Ask a router with two controllers whether this one is the active one."""
        return self._await(lambda r: r.dual_controller_status(timeout=timeout))

    def send(self, payload: Union[bytes, bytearray, List[int]], *, check: bool = True) -> None:
        """Send one raw message and wait for the ACK."""
        self._await(lambda r: r.send(payload, check=check))

    def write_raw(self, data: Union[bytes, bytearray, List[int]]) -> None:
        """Write bytes exactly as given, without framing and without waiting for an ACK."""
        self._await(lambda r: r.write_raw(data))

    def request(
        self,
        payload: Union[bytes, bytearray, List[int]],
        reply: Union[int, Iterable[int]],
        *,
        timeout: Optional[float] = None,
    ) -> Message:
        """Send a raw message and return the first reply with the given command byte(s)."""
        return self._await(lambda r: r.request(payload, reply, timeout=timeout))

    # -- callbacks ---------------------------------------------------------

    def _subscribe(self, register: Callable[[AsyncRouter], Callable[[], None]]) -> Callable[[], None]:
        unsubscribe = self._call(register)
        loop = self._need()[0]

        def remove() -> None:
            if not loop.is_closed():
                loop.call_soon_threadsafe(unsubscribe)

        return remove

    def on_route(self, callback: Callable[[RouteChange], Any]) -> Callable[[], None]:
        """Call ``callback(RouteChange)`` on every crosspoint change."""
        return self._subscribe(lambda r: r.on_route(callback))

    def on_names(self, callback: Callable[[Names], Any]) -> Callable[[], None]:
        return self._subscribe(lambda r: r.on_names(callback))

    def on_connection(self, callback: Callable[[bool], Any]) -> Callable[[], None]:
        return self._subscribe(lambda r: r.on_connection(callback))

    def on_message(self, callback: Callable[[Message], Any]) -> Callable[[], None]:
        return self._subscribe(lambda r: r.on_message(callback))

    def on_trace(self, callback: Callable[[TraceEvent], Any]) -> Callable[[], None]:
        """Call ``callback(TraceEvent)`` for everything sent and received."""
        return self._subscribe(lambda r: r.on_trace(callback))
