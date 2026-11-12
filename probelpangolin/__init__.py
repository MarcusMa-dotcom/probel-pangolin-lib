"""probelpangolin - control broadcast routers (Kreuzschienen) via the SW-P-08 protocol.

Quick start::

    from probelpangolin import Router

    with Router("10.0.0.20") as router:
        router.route(source=5, dest=3)
        print(router.get_route(3))

:class:`Router` is the blocking front end, :class:`AsyncRouter` the asyncio
client underneath.  :mod:`probelpangolin.swp02` has the same for the older
protocol SW-P-02.  :mod:`probelpangolin.protocol` contains the pure wire format.
"""

from .client import DEFAULT_PORT, AsyncRouter, RouteChange
from .errors import (
    AckError,
    AckTimeoutError,
    NakError,
    NotConnectedError,
    ProtocolError,
    ResponseTimeoutError,
    RouteRejectedError,
    SWP08Error,
    UnknownNameError,
    UnsupportedCommandError,
)
from .protocol import (
    Command,
    Crosspoint,
    CrosspointRequest,
    DualControllerStatus,
    FrameDecoder,
    Message,
    NameLength,
    Names,
    NamesRequest,
    ProtocolImplementation,
    ProtocolImplementationRequest,
    SalvoCrosspoint,
    SalvoGo,
    SalvoInterrogate,
    TallyDump,
    TallyDumpRequest,
    TraceEvent,
    UnknownMessage,
    command_name,
    describe,
    encode_frame,
    parse_message,
)
from .sync import Router

__version__ = "0.1.0"

__all__ = [
    "Router",
    "AsyncRouter",
    "RouteChange",
    "DEFAULT_PORT",
    "Command",
    "NameLength",
    "Crosspoint",
    "TallyDump",
    "Names",
    "ProtocolImplementation",
    "UnknownMessage",
    "CrosspointRequest",
    "TallyDumpRequest",
    "NamesRequest",
    "ProtocolImplementationRequest",
    "TraceEvent",
    "SalvoCrosspoint",
    "SalvoGo",
    "SalvoInterrogate",
    "DualControllerStatus",
    "describe",
    "Message",
    "FrameDecoder",
    "encode_frame",
    "parse_message",
    "command_name",
    "SWP08Error",
    "ProtocolError",
    "NotConnectedError",
    "AckError",
    "NakError",
    "AckTimeoutError",
    "ResponseTimeoutError",
    "UnsupportedCommandError",
    "RouteRejectedError",
    "UnknownNameError",
    "__version__",
]
