"""Exceptions raised by the probelpangolin client."""

from __future__ import annotations

from .protocol import ProtocolError, SWP08Error

__all__ = [
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
]


class NotConnectedError(SWP08Error, ConnectionError):
    """There is no connection to the router (or it dropped mid-request)."""


class AckError(SWP08Error):
    """The router did not accept a message on the data-link level."""


class NakError(AckError):
    """The router answered every attempt with ``DLE NAK``."""


class AckTimeoutError(AckError, TimeoutError):
    """The router never answered with ``DLE ACK``."""


class ResponseTimeoutError(SWP08Error, TimeoutError):
    """The router acknowledged a request but the expected reply never came."""


class UnsupportedCommandError(SWP08Error):
    """The router reported that it does not implement this command."""


class RouteRejectedError(SWP08Error):
    """The router confirmed a different source than the one requested.

    Typically the destination is protected / locked.
    """

    def __init__(self, dest: int, level: int, requested: int, actual: int) -> None:
        super().__init__(
            f"destination {dest} level {level}: requested source {requested}, "
            f"router reports source {actual}"
        )
        self.dest = dest
        self.level = level
        self.requested = requested
        self.actual = actual


class UnknownNameError(SWP08Error, LookupError):
    """A source or destination name is not (or not uniquely) known."""
