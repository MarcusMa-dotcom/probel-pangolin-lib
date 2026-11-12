"""SW-P-08 wire format: framing, command codes, message builders and parsers.

This module is pure Python without any I/O, so it can be used on its own
(e.g. to build frames for a serial port or to decode a capture).

All public builders and parsed messages use **1-based** numbers for matrix,
level, source and destination, exactly as they are shown on router panels
and in the Bitfocus Companion module.  The conversion to the 0-based values
that travel on the wire happens here.

Frame layout::

    +---------+------~~------+-----+-----+---------+
    | DLE STX |     DATA     | BTC | CHK | DLE ETX |
    +---------+------~~------+-----+-----+---------+

* ``BTC`` is the number of DATA bytes (before DLE stuffing).
* ``CHK`` is the two's complement of the 8-bit sum of DATA and BTC.
* Every ``0x10`` (DLE) inside DATA, BTC and CHK is sent twice.
* Every valid frame is answered with ``DLE ACK``, every broken one with
  ``DLE NAK``.  An ACK or NAK may arrive in the middle of a frame that is
  still being received; the frame goes on after it.

Framing, the crosspoint, tally dump, salvo and dual controller messages follow
the specification *SW-P-88 Router Control Protocols* (Pro-Bel / Snell).  The
extended commands and the name messages are not part of that public document
and follow the Bitfocus Companion module ``generic-swp08``.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

__all__ = [
    "DLE",
    "STX",
    "ETX",
    "ACK",
    "NAK",
    "ACK_FRAME",
    "NAK_FRAME",
    "Command",
    "NameLength",
    "SWP08Error",
    "ProtocolError",
    "command_name",
    "encode_frame",
    "FrameDecoder",
    "Ack",
    "Nak",
    "Frame",
    "FrameError",
    "needs_extended",
    "build_connect",
    "build_interrogate",
    "build_tally_dump_request",
    "build_source_names_request",
    "build_dest_names_request",
    "build_protocol_implementation_request",
    "build_salvo_connect",
    "build_salvo_go",
    "build_salvo_interrogate",
    "build_dual_controller_status_request",
    "GUARANTEED_DATA",
    "SalvoCrosspoint",
    "SalvoGo",
    "SalvoInterrogate",
    "DualControllerStatus",
    "Crosspoint",
    "TallyDump",
    "Names",
    "ProtocolImplementation",
    "UnknownMessage",
    "CrosspointRequest",
    "TallyDumpRequest",
    "NamesRequest",
    "ProtocolImplementationRequest",
    "Message",
    "parse_message",
    "describe",
    "TraceEvent",
]

DLE = 0x10
STX = 0x02
ETX = 0x03
ACK = 0x06
NAK = 0x15

ACK_FRAME = bytes((DLE, ACK))
NAK_FRAME = bytes((DLE, NAK))

_SOM = bytes((DLE, STX))
_EOM = bytes((DLE, ETX))
_DLE_BYTE = bytes((DLE,))
_DLE_DLE = bytes((DLE, DLE))

#: Largest frame body (DATA + BTC + CHK) the decoder accepts before it
#: assumes it has lost sync.  BTC is a single byte, so 255 + 2 is the limit.
MAX_BODY = 257
#: Largest DATA field that is guaranteed to work with all systems (SW-P-88, 7.2.1).
#: Custom applications may use up to 255 bytes.
GUARANTEED_DATA = 128
#: Salvo group numbers and connect indexes are 0 to 127 and travel as they are.
MAX_SALVO = 127

# Limits of the standard (non-extended) command set.
STD_MAX_MATRIX = 16
STD_MAX_LEVEL = 16
STD_MAX_PORT = 1024
# Limits of the extended command set.
EXT_MAX_MATRIX = 256
EXT_MAX_LEVEL = 256
EXT_MAX_PORT = 65536


class SWP08Error(Exception):
    """Base class for everything this library raises on its own."""


class ProtocolError(SWP08Error, ValueError):
    """A message could not be built or parsed."""


class Command(enum.IntEnum):
    """SW-P-08 command bytes (first DATA byte of every message)."""

    COMMAND_ENABLE = 0x00
    CROSSPOINT_INTERROGATE = 0x01
    CROSSPOINT_CONNECT = 0x02
    CROSSPOINT_TALLY = 0x03
    CROSSPOINT_CONNECTED = 0x04
    MAINTENANCE = 0x07
    DUAL_CONTROLLER_STATUS_REQUEST = 0x08
    DUAL_CONTROLLER_STATUS_RESPONSE = 0x09
    PROTECT_INTERROGATE = 0x0A
    PROTECT_TALLY = 0x0B
    PROTECT_CONNECT = 0x0C
    PROTECT_CONNECTED = 0x0D
    PROTECT_DISCONNECT = 0x0E
    PROTECT_DISCONNECTED = 0x0F
    PROTECT_DEVICE_NAME_REQUEST = 0x11
    PROTECT_DEVICE_NAME_RESPONSE = 0x12
    PROTECT_TALLY_DUMP_REQUEST = 0x13
    PROTECT_TALLY_DUMP_RESPONSE = 0x14
    CROSSPOINT_TALLY_DUMP_REQUEST = 0x15
    CROSSPOINT_TALLY_DUMP_BYTE = 0x16
    CROSSPOINT_TALLY_DUMP_WORD = 0x17
    ROUTER_IO_PARAMETER_INTERROGATE = 0x19
    ROUTER_IO_PARAMETER_TALLY = 0x1A
    ROUTER_IO_PARAMETER_CONNECT = 0x1B
    ROUTER_IO_PARAMETER_CONNECTED = 0x1C
    MASTER_PROTECT_CONNECT = 0x1D
    NAMES_UPDATED = 0x1E
    DIAGNOSTIC_REQUEST = 0x29
    DIAGNOSTIC_RESPONSE = 0x2B
    LOG_MESSAGE = 0x2C
    STATUS_CONFIGURATION_REQUEST = 0x4C
    STATUS_CONFIGURATION_TALLY = 0x4D
    LOGGING_STRINGS = 0x4E
    ERROR_AND_STATUS_REQUEST = 0x4F
    SOFT_KEY_TALLY_REQUEST = 0x57
    SOFT_KEY_TALLY_RESPONSE = 0x58
    SOFT_KEY_ASSIGNMENT_SET_REQUEST = 0x59
    SOFT_KEY_ASSIGNMENT_SET_RESPONSE = 0x5A
    PROTOCOL_IMPLEMENTATION_REQUEST = 0x61
    PROTOCOL_IMPLEMENTATION_RESPONSE = 0x62
    SOURCE_NAMES_REQUEST = 0x64
    SINGLE_SOURCE_NAME_REQUEST = 0x65
    DEST_NAMES_REQUEST = 0x66
    SINGLE_DEST_NAME_REQUEST = 0x67
    UMD_LABELS_REQUEST = 0x68
    SINGLE_UMD_LABEL_REQUEST = 0x69
    SOURCE_NAMES_RESPONSE = 0x6A
    DEST_NAMES_RESPONSE = 0x6B
    UMD_LABELS_RESPONSE = 0x6C
    CROSSPOINT_TIE_LINE_CONNECT = 0x6F
    CROSSPOINT_TIE_LINE_INTERROGATE = 0x70
    CROSSPOINT_TIE_LINE_TALLY = 0x71
    SOURCE_ASSOCIATION_NAMES_REQUEST = 0x72
    SINGLE_SOURCE_ASSOCIATION_NAME_REQUEST = 0x73
    SOURCE_ASSOCIATION_NAMES_RESPONSE = 0x74
    UPDATE_NAME = 0x75
    CROSSPOINT_CONNECT_ON_GO_SALVO = 0x78
    CROSSPOINT_GO_GROUP_SALVO = 0x79
    CROSSPOINT_CONNECT_ON_GO_GROUP_SALVO_ACK = 0x7A
    CROSSPOINT_GO_DONE_GROUP_SALVO = 0x7B
    CROSSPOINT_SALVO_GROUP_INTERROGATE = 0x7C
    CROSSPOINT_GROUP_SALVO_TALLY = 0x7D
    EXT_CROSSPOINT_INTERROGATE = 0x81
    EXT_CROSSPOINT_CONNECT = 0x82
    EXT_CROSSPOINT_TALLY = 0x83
    EXT_CROSSPOINT_CONNECTED = 0x84
    EXT_PROTECT_INTERROGATE = 0x8A
    EXT_PROTECT_TALLY = 0x8B
    EXT_PROTECT_CONNECT = 0x8C
    EXT_PROTECT_CONNECTED = 0x8D
    EXT_PROTECT_DISCONNECT = 0x8E
    EXT_PROTECT_DISCONNECTED = 0x8F
    EXT_PROTECT_TALLY_DUMP_REQUEST = 0x93
    EXT_CROSSPOINT_TALLY_DUMP_REQUEST = 0x95
    EXT_CROSSPOINT_TALLY_DUMP_WORD = 0x97
    EXT_ROUTER_IO_PARAMETER_INTERROGATE = 0x99
    EXT_ROUTER_IO_PARAMETER_CONNECT = 0x9B
    EXT_SOURCE_NAMES_REQUEST = 0xE4
    EXT_SINGLE_SOURCE_NAME_REQUEST = 0xE5
    EXT_DEST_NAMES_REQUEST = 0xE6
    EXT_SINGLE_DEST_NAME_REQUEST = 0xE7
    EXT_UMD_LABELS_REQUEST = 0xE8
    EXT_SINGLE_UMD_LABEL_REQUEST = 0xE9
    EXT_SOURCE_NAMES_RESPONSE = 0xEA
    EXT_DEST_NAMES_RESPONSE = 0xEB
    EXT_CROSSPOINT_CONNECT_ON_GO_GROUP_SALVO = 0xF8
    EXT_CROSSPOINT_SALVO_GROUP_INTERROGATE = 0xFC


def command_name(code: int) -> str:
    """Readable name for a command byte, e.g. ``CROSSPOINT_CONNECT``."""
    try:
        return Command(code).name
    except ValueError:
        return f"UNKNOWN_0x{code:02X}"


class NameLength(enum.IntEnum):
    """Label length code used in the name requests."""

    FOUR = 0
    EIGHT = 1
    TWELVE = 2

    @property
    def chars(self) -> int:
        return _NAME_CHARS[int(self)]

    @classmethod
    def from_chars(cls, chars: int) -> "NameLength":
        for code, n in _NAME_CHARS.items():
            if n == chars:
                return cls(code)
        raise ProtocolError(f"unsupported name length {chars}, use 4, 8 or 12")


_NAME_CHARS = {0: 4, 1: 8, 2: 12}


# ---------------------------------------------------------------------------
# Framing
# ---------------------------------------------------------------------------


def encode_frame(payload: Union[bytes, bytearray, List[int]]) -> bytes:
    """Wrap message DATA into a complete frame ready to be written."""
    data = bytes(payload)
    if not data:
        raise ProtocolError("cannot frame an empty message")
    if len(data) > 0xFF:
        raise ProtocolError(f"message too long ({len(data)} bytes, max 255)")
    btc = len(data)
    chk = (-(sum(data) + btc)) & 0xFF
    body = data + bytes((btc, chk))
    return _SOM + body.replace(_DLE_BYTE, _DLE_DLE) + _EOM


@dataclass(frozen=True)
class Ack:
    """``DLE ACK`` – the other side accepted our last frame."""


@dataclass(frozen=True)
class Nak:
    """``DLE NAK`` – the other side rejected our last frame."""


@dataclass(frozen=True)
class Frame:
    """A complete, checksum-verified message (DATA bytes only)."""

    payload: bytes


@dataclass(frozen=True)
class FrameError:
    """A frame arrived but was broken; it should be answered with NAK."""

    reason: str
    raw: bytes = b""


DecoderEvent = Union[Ack, Nak, Frame, FrameError]


class FrameDecoder:
    """Incremental decoder for the SW-P-08 byte stream.

    Feed it whatever arrives from the socket; it returns the events that
    became complete.  It is a real state machine, so a stuffed DLE followed
    by a data byte ``0x03`` (``10 10 03``) is *not* mistaken for the end of
    the message, and an ACK that arrives in the middle of a frame does not
    break the frame.
    """

    def __init__(self) -> None:
        self._body = bytearray()
        self._in_frame = False
        self._dle = False
        self.discarded = 0  #: bytes dropped while hunting for a frame start

    @property
    def in_frame(self) -> bool:
        """True while a frame has started and its end has not arrived yet."""
        return self._in_frame

    def reset(self) -> None:
        self._body.clear()
        self._in_frame = False
        self._dle = False

    def feed(self, data: Union[bytes, bytearray]) -> List[DecoderEvent]:
        events: List[DecoderEvent] = []
        for byte in data:
            if self._dle:
                self._dle = False
                if byte == DLE and self._in_frame:
                    self._body.append(DLE)
                elif byte == DLE:
                    # DLE DLE outside a frame: stay armed, the second DLE may
                    # be the start of a real control sequence.
                    self.discarded += 1
                    self._dle = True
                elif byte == STX:
                    if self._in_frame:
                        events.append(FrameError("frame restarted before ETX", bytes(self._body)))
                    self._body.clear()
                    self._in_frame = True
                elif byte == ETX and self._in_frame:
                    events.append(self._finish())
                elif byte == ACK or byte == NAK:
                    # SW-P-88, 7.2.2: an ACK or NAK embedded in a message that is still
                    # arriving is permitted.  Report it and carry on with the frame.
                    events.append(Ack() if byte == ACK else Nak())
                else:
                    if self._in_frame:
                        events.append(
                            FrameError(f"invalid DLE sequence 0x{byte:02X}", bytes(self._body))
                        )
                        self._body.clear()
                        self._in_frame = False
                    else:
                        self.discarded += 2
            elif byte == DLE:
                self._dle = True
            elif self._in_frame:
                self._body.append(byte)
                if len(self._body) > MAX_BODY:
                    events.append(FrameError("frame too long", bytes(self._body)))
                    self._body.clear()
                    self._in_frame = False
            else:
                self.discarded += 1
        return events

    def _finish(self) -> DecoderEvent:
        body = bytes(self._body)
        self._body.clear()
        self._in_frame = False
        if len(body) < 3:
            return FrameError("frame too short", body)
        btc = body[-2]
        if btc != len(body) - 2:
            return FrameError(f"byte count {btc} does not match {len(body) - 2} data bytes", body)
        if sum(body) & 0xFF:
            return FrameError("checksum mismatch", body)
        return Frame(body[:-2])


# ---------------------------------------------------------------------------
# Message builders (controller -> router)
# ---------------------------------------------------------------------------


def _check(name: str, value: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ProtocolError(f"{name} must be an integer, got {value!r}")
    if not 1 <= value <= maximum:
        raise ProtocolError(f"{name} {value} out of range 1..{maximum}")
    return value - 1


def needs_extended(matrix: int = 1, level: int = 1, dest: int = 1, source: int = 1) -> bool:
    """True if these (1-based) numbers do not fit the standard commands."""
    return (
        matrix > STD_MAX_MATRIX
        or level > STD_MAX_LEVEL
        or dest > STD_MAX_PORT
        or source > STD_MAX_PORT
    )


def _matrix_level(matrix: int, level: int) -> int:
    return (_check("matrix", matrix, STD_MAX_MATRIX) << 4) | _check("level", level, STD_MAX_LEVEL)


def build_connect(
    source: int, dest: int, level: int = 1, matrix: int = 1, *, extended: bool = False
) -> bytes:
    """Crosspoint Connect (0x02) / Extended Crosspoint Connect (0x82)."""
    if extended:
        m = _check("matrix", matrix, EXT_MAX_MATRIX)
        lv = _check("level", level, EXT_MAX_LEVEL)
        d = _check("destination", dest, EXT_MAX_PORT)
        s = _check("source", source, EXT_MAX_PORT)
        return bytes((Command.EXT_CROSSPOINT_CONNECT, m, lv, d >> 8, d & 0xFF, s >> 8, s & 0xFF))
    ml = _matrix_level(matrix, level)
    d = _check("destination", dest, STD_MAX_PORT)
    s = _check("source", source, STD_MAX_PORT)
    multiplier = ((d >> 7) & 0x07) << 4 | ((s >> 7) & 0x07)
    return bytes((Command.CROSSPOINT_CONNECT, ml, multiplier, d & 0x7F, s & 0x7F))


def build_interrogate(dest: int, level: int = 1, matrix: int = 1, *, extended: bool = False) -> bytes:
    """Crosspoint Interrogate (0x01) / Extended Interrogate (0x81)."""
    if extended:
        m = _check("matrix", matrix, EXT_MAX_MATRIX)
        lv = _check("level", level, EXT_MAX_LEVEL)
        d = _check("destination", dest, EXT_MAX_PORT)
        return bytes((Command.EXT_CROSSPOINT_INTERROGATE, m, lv, d >> 8, d & 0xFF))
    ml = _matrix_level(matrix, level)
    d = _check("destination", dest, STD_MAX_PORT)
    return bytes((Command.CROSSPOINT_INTERROGATE, ml, ((d >> 7) & 0x07) << 4, d & 0x7F))


def build_tally_dump_request(level: int = 1, matrix: int = 1, *, extended: bool = False) -> bytes:
    """Crosspoint Tally Dump Request (0x15) / extended (0x95)."""
    if extended:
        m = _check("matrix", matrix, EXT_MAX_MATRIX)
        lv = _check("level", level, EXT_MAX_LEVEL)
        return bytes((Command.EXT_CROSSPOINT_TALLY_DUMP_REQUEST, m, lv))
    return bytes((Command.CROSSPOINT_TALLY_DUMP_REQUEST, _matrix_level(matrix, level)))


def build_source_names_request(
    matrix: int = 1,
    level: int = 1,
    name_length: NameLength = NameLength.EIGHT,
    *,
    extended: bool = False,
) -> bytes:
    """All Source Names Request (0x64) / extended (0xE4)."""
    length = int(NameLength(name_length))
    if extended:
        m = _check("matrix", matrix, EXT_MAX_MATRIX)
        lv = _check("level", level, EXT_MAX_LEVEL)
        return bytes((Command.EXT_SOURCE_NAMES_REQUEST, m, lv, length))
    return bytes((Command.SOURCE_NAMES_REQUEST, _matrix_level(matrix, level), length))


def build_dest_names_request(
    matrix: int = 1, name_length: NameLength = NameLength.EIGHT, *, extended: bool = False
) -> bytes:
    """All Destination Association Names Request (0x66) / extended (0xE6)."""
    length = int(NameLength(name_length))
    if extended:
        return bytes((Command.EXT_DEST_NAMES_REQUEST, _check("matrix", matrix, EXT_MAX_MATRIX), length))
    return bytes((Command.DEST_NAMES_REQUEST, _check("matrix", matrix, STD_MAX_MATRIX) << 4, length))


def build_protocol_implementation_request() -> bytes:
    """Protocol Implementation Request (0x61)."""
    return bytes((Command.PROTOCOL_IMPLEMENTATION_REQUEST,))


def _salvo(value: int, name: str = "salvo") -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ProtocolError(f"{name} must be an integer, got {value!r}")
    if not 0 <= value <= MAX_SALVO:
        raise ProtocolError(f"{name} {value} out of range 0..{MAX_SALVO}")
    return value


def build_salvo_connect(source: int, dest: int, salvo: int, level: int = 1, matrix: int = 1) -> bytes:
    """Crosspoint Connect On Go Group Salvo (120): store a crosspoint in a salvo group.

    The router keeps it until a Go Group Salvo sets or clears the group.
    ``salvo`` is 0 to 127.
    """
    ml = _matrix_level(matrix, level)
    d = _check("destination", dest, STD_MAX_PORT)
    s = _check("source", source, STD_MAX_PORT)
    multiplier = ((d >> 7) & 0x07) << 4 | ((s >> 7) & 0x07)
    return bytes((Command.CROSSPOINT_CONNECT_ON_GO_SALVO, ml, multiplier, d & 0x7F, s & 0x7F,
                  _salvo(salvo)))


def build_salvo_go(salvo: int, *, clear: bool = False) -> bytes:
    """Crosspoint Go Group Salvo (121): set the stored crosspoints of a group, or clear them."""
    return bytes((Command.CROSSPOINT_GO_GROUP_SALVO, 1 if clear else 0, _salvo(salvo)))


def build_salvo_interrogate(salvo: int, index: int = 0) -> bytes:
    """Crosspoint Salvo Group Interrogate (124): ask for one stored crosspoint of a group.

    Ask from ``index`` 0 upwards until the tally says it was the last one.
    """
    return bytes((Command.CROSSPOINT_SALVO_GROUP_INTERROGATE, _salvo(salvo), _salvo(index, "index")))


def build_dual_controller_status_request() -> bytes:
    """Dual Controller Status Request (08): which of two controllers is the active one."""
    return bytes((Command.DUAL_CONTROLLER_STATUS_REQUEST,))


# ---------------------------------------------------------------------------
# Parsed messages (router -> controller)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Crosspoint:
    """Crosspoint Tally / Connected, standard or extended."""

    command: int
    matrix: int
    level: int
    dest: int
    source: int
    #: the router marks the source as bad (TDM and digital video routers, standard commands only)
    source_bad: bool = False

    @property
    def is_connected(self) -> bool:
        """True for a *Connected* message (a take happened), False for a tally reply."""
        return self.command in (Command.CROSSPOINT_CONNECTED, Command.EXT_CROSSPOINT_CONNECTED)

    @property
    def extended(self) -> bool:
        return self.command >= 0x80


@dataclass(frozen=True)
class TallyDump:
    """One part of a crosspoint tally dump: consecutive destinations."""

    command: int
    matrix: int
    level: int
    first_dest: int
    sources: Tuple[int, ...]

    def routes(self) -> Dict[int, int]:
        """``{destination: source}`` for this part."""
        return {self.first_dest + i: src for i, src in enumerate(self.sources)}


@dataclass(frozen=True)
class Names:
    """One part of a source or destination name list."""

    command: int
    kind: str  #: ``"source"`` or ``"dest"``
    matrix: int
    level: Optional[int]  #: only present for source names
    first: int
    names: Tuple[str, ...]

    def as_dict(self) -> Dict[int, str]:
        """``{number: name}`` for this part."""
        return {self.first + i: name for i, name in enumerate(self.names)}


@dataclass(frozen=True)
class ProtocolImplementation:
    """Protocol Implementation Response (0x62)."""

    command: int
    #: the two bytes between the command byte and the command list: how many of
    #: the listed commands the router sends, and how many it accepts
    header: bytes
    commands: Tuple[int, ...]

    def supports(self, command: int) -> bool:
        return int(command) in self.commands

    @property
    def _split(self) -> Optional[int]:
        if len(self.header) == 2 and self.header[0] + self.header[1] == len(self.commands):
            return self.header[0]
        return None

    @property
    def transmitted(self) -> Tuple[int, ...]:
        """Commands the router sends.  Empty if the two count bytes do not fit the list."""
        split = self._split
        return self.commands[:split] if split is not None else ()

    @property
    def received(self) -> Tuple[int, ...]:
        """Commands the router accepts.  Empty if the two count bytes do not fit the list."""
        split = self._split
        return self.commands[split:] if split is not None else ()


@dataclass(frozen=True)
class UnknownMessage:
    """A valid frame this library has no parser for."""

    command: int
    payload: bytes


# -- messages a controller sends (parsed for traces, captures and the simulator) --


@dataclass(frozen=True)
class CrosspointRequest:
    """Crosspoint Interrogate or Connect as a controller sends it, standard or extended."""

    command: int
    matrix: int
    level: int
    dest: int
    source: Optional[int]  #: ``None`` for an interrogate

    @property
    def is_connect(self) -> bool:
        return self.source is not None

    @property
    def extended(self) -> bool:
        return self.command >= 0x80


@dataclass(frozen=True)
class TallyDumpRequest:
    """Crosspoint Tally Dump Request, standard or extended."""

    command: int
    matrix: int
    level: int


@dataclass(frozen=True)
class NamesRequest:
    """All Source Names / All Destination Association Names Request."""

    command: int
    kind: str  #: ``"source"`` or ``"dest"``
    matrix: int
    level: Optional[int]  #: only present for source names
    chars: int  #: requested label length: 4, 8 or 12


@dataclass(frozen=True)
class ProtocolImplementationRequest:
    """Protocol Implementation Request (0x61)."""

    command: int = Command.PROTOCOL_IMPLEMENTATION_REQUEST


# -- salvo groups and dual controllers (SW-P-88, 8.1.4 to 8.2.7 and 11.5) --


@dataclass(frozen=True)
class SalvoCrosspoint:
    """A crosspoint of a salvo group.

    Connect On Go Group Salvo (120, controller to router), its acknowledge
    (122) and the Group Salvo Tally (125).  ``index`` and ``validity`` are
    only present in a tally.
    """

    command: int
    matrix: int
    level: int
    dest: int
    source: int
    salvo: int  #: salvo group number, 0 to 127
    index: Optional[int] = None  #: place of this crosspoint in the group, from 0
    validity: Optional[int] = None  #: 0 = more follow, 1 = last one, 2 = no data in the group

    @property
    def is_last(self) -> bool:
        """True if a tally says nothing follows this entry."""
        return self.validity in (1, 2)

    @property
    def has_data(self) -> bool:
        """False for a tally of a group that holds nothing at this index."""
        return self.validity != 2


@dataclass(frozen=True)
class SalvoGo:
    """Go Group Salvo (121) and Go Done Group Salvo Acknowledge (123).

    ``action`` in a request: 0 = set the stored crosspoints, 1 = clear them.
    In the acknowledge: 0 = crosspoints set, 1 = stored crosspoints cleared,
    2 = there was nothing to set or clear.
    """

    command: int
    salvo: int
    action: int

    @property
    def is_done(self) -> bool:
        """True for the acknowledge of the router."""
        return self.command == Command.CROSSPOINT_GO_DONE_GROUP_SALVO


@dataclass(frozen=True)
class SalvoInterrogate:
    """Salvo Group Interrogate (124): asks for the crosspoint at ``index`` of a group."""

    command: int
    salvo: int
    index: int


@dataclass(frozen=True)
class DualControllerStatus:
    """Dual Controller Status Response (09).

    The public specification names one fact: bit 1 of the first byte tells
    whether this controller is the active or the idle one.  It does not say
    which value means active, so the bytes are kept as they are.
    """

    command: int
    status: bytes

    @property
    def bit1(self) -> Optional[bool]:
        """Bit 1 of the first status byte, ``None`` if the message has none."""
        return bool(self.status[0] & 0x02) if self.status else None


Message = Union[
    Crosspoint,
    TallyDump,
    Names,
    ProtocolImplementation,
    SalvoCrosspoint,
    SalvoGo,
    SalvoInterrogate,
    DualControllerStatus,
    CrosspointRequest,
    TallyDumpRequest,
    NamesRequest,
    ProtocolImplementationRequest,
    UnknownMessage,
]


def _need(payload: bytes, n: int) -> None:
    if len(payload) < n:
        raise ProtocolError(
            f"{command_name(payload[0])} message too short: {len(payload)} bytes, need {n}"
        )


def _parse_names(
    payload: bytes, kind: str, extended: bool, has_level: bool, encoding: str
) -> Names:
    cmd = payload[0]
    level: Optional[int] = None
    if extended:
        _need(payload, 7 if has_level else 6)
        matrix = payload[1] + 1
        idx = 2
        if has_level:
            level = payload[idx] + 1
            idx += 1
    else:
        _need(payload, 6)
        matrix = (payload[1] >> 4) + 1
        if has_level:
            level = (payload[1] & 0x0F) + 1
        idx = 2
    try:
        chars = _NAME_CHARS[payload[idx]]
    except KeyError:
        raise ProtocolError(f"unknown name length code {payload[idx]}") from None
    first = ((payload[idx + 1] << 8) | payload[idx + 2]) + 1
    count = payload[idx + 3]
    start = idx + 4
    names = []
    for i in range(count):
        chunk = payload[start + i * chars : start + (i + 1) * chars]
        if not chunk:
            break  # truncated part: keep what is there
        names.append(chunk.decode(encoding, errors="replace").replace("\x00", "").strip())
    return Names(cmd, kind, matrix, level, first, tuple(names))


def parse_message(payload: Union[bytes, bytearray], *, name_encoding: str = "latin-1") -> Message:
    """Turn the DATA bytes of a frame into a typed message.

    Messages without a dedicated parser come back as :class:`UnknownMessage`.
    Raises :class:`ProtocolError` if a known message is truncated.
    """
    payload = bytes(payload)
    if not payload:
        raise ProtocolError("empty message")
    cmd = payload[0]

    if cmd in (Command.CROSSPOINT_TALLY, Command.CROSSPOINT_CONNECTED):
        _need(payload, 5)
        return Crosspoint(
            command=cmd,
            matrix=(payload[1] >> 4) + 1,
            level=(payload[1] & 0x0F) + 1,
            dest=(((payload[2] & 0x70) << 3) | (payload[3] & 0x7F)) + 1,
            source=(((payload[2] & 0x07) << 7) | (payload[4] & 0x7F)) + 1,
            source_bad=bool(payload[2] & 0x08),
        )

    if cmd in (Command.EXT_CROSSPOINT_TALLY, Command.EXT_CROSSPOINT_CONNECTED):
        _need(payload, 7)
        return Crosspoint(
            command=cmd,
            matrix=payload[1] + 1,
            level=payload[2] + 1,
            dest=((payload[3] << 8) | payload[4]) + 1,
            source=((payload[5] << 8) | payload[6]) + 1,
        )

    if cmd == Command.CROSSPOINT_TALLY_DUMP_BYTE:
        _need(payload, 4)
        count = payload[2]
        sources = tuple(b + 1 for b in payload[4 : 4 + count])
        return TallyDump(cmd, (payload[1] >> 4) + 1, (payload[1] & 0x0F) + 1, payload[3] + 1, sources)

    if cmd in (Command.CROSSPOINT_TALLY_DUMP_WORD, Command.EXT_CROSSPOINT_TALLY_DUMP_WORD):
        if cmd == Command.CROSSPOINT_TALLY_DUMP_WORD:
            _need(payload, 5)
            matrix, level, idx = (payload[1] >> 4) + 1, (payload[1] & 0x0F) + 1, 2
        else:
            _need(payload, 6)
            matrix, level, idx = payload[1] + 1, payload[2] + 1, 3
        count = payload[idx]
        first = ((payload[idx + 1] << 8) | payload[idx + 2]) + 1
        words = payload[idx + 3 : idx + 3 + 2 * count]
        sources = tuple(
            ((words[i] << 8) | words[i + 1]) + 1 for i in range(0, len(words) - 1, 2)
        )
        return TallyDump(cmd, matrix, level, first, sources)

    if cmd == Command.SOURCE_NAMES_RESPONSE:
        return _parse_names(payload, "source", False, True, name_encoding)
    if cmd == Command.DEST_NAMES_RESPONSE:
        return _parse_names(payload, "dest", False, False, name_encoding)
    if cmd == Command.EXT_SOURCE_NAMES_RESPONSE:
        return _parse_names(payload, "source", True, True, name_encoding)
    if cmd == Command.EXT_DEST_NAMES_RESPONSE:
        return _parse_names(payload, "dest", True, False, name_encoding)

    if cmd == Command.PROTOCOL_IMPLEMENTATION_RESPONSE:
        return ProtocolImplementation(cmd, header=payload[1:3], commands=tuple(payload[3:]))

    # -- salvo groups and dual controllers -------------------------------------

    if cmd in (Command.CROSSPOINT_CONNECT_ON_GO_SALVO, Command.CROSSPOINT_CONNECT_ON_GO_GROUP_SALVO_ACK,
               Command.CROSSPOINT_GROUP_SALVO_TALLY):
        tally = cmd == Command.CROSSPOINT_GROUP_SALVO_TALLY
        _need(payload, 8 if tally else 6)
        return SalvoCrosspoint(
            command=cmd,
            matrix=(payload[1] >> 4) + 1,
            level=(payload[1] & 0x0F) + 1,
            dest=(((payload[2] & 0x70) << 3) | (payload[3] & 0x7F)) + 1,
            source=(((payload[2] & 0x07) << 7) | (payload[4] & 0x7F)) + 1,
            salvo=payload[5] & 0x7F,
            index=payload[6] if tally else None,
            validity=payload[7] if tally else None,
        )

    if cmd in (Command.CROSSPOINT_GO_GROUP_SALVO, Command.CROSSPOINT_GO_DONE_GROUP_SALVO):
        _need(payload, 3)
        return SalvoGo(cmd, salvo=payload[2] & 0x7F, action=payload[1])

    if cmd == Command.CROSSPOINT_SALVO_GROUP_INTERROGATE:
        _need(payload, 3)
        return SalvoInterrogate(cmd, salvo=payload[1] & 0x7F, index=payload[2])

    if cmd == Command.DUAL_CONTROLLER_STATUS_RESPONSE:
        return DualControllerStatus(cmd, payload[1:])

    # -- controller -> router ------------------------------------------------

    if cmd in (Command.CROSSPOINT_INTERROGATE, Command.CROSSPOINT_CONNECT):
        connect = cmd == Command.CROSSPOINT_CONNECT
        _need(payload, 5 if connect else 4)
        return CrosspointRequest(
            command=cmd,
            matrix=(payload[1] >> 4) + 1,
            level=(payload[1] & 0x0F) + 1,
            dest=(((payload[2] & 0x70) << 3) | (payload[3] & 0x7F)) + 1,
            source=(((payload[2] & 0x07) << 7) | (payload[4] & 0x7F)) + 1 if connect else None,
        )

    if cmd in (Command.EXT_CROSSPOINT_INTERROGATE, Command.EXT_CROSSPOINT_CONNECT):
        connect = cmd == Command.EXT_CROSSPOINT_CONNECT
        _need(payload, 7 if connect else 5)
        return CrosspointRequest(
            command=cmd,
            matrix=payload[1] + 1,
            level=payload[2] + 1,
            dest=((payload[3] << 8) | payload[4]) + 1,
            source=((payload[5] << 8) | payload[6]) + 1 if connect else None,
        )

    if cmd == Command.CROSSPOINT_TALLY_DUMP_REQUEST:
        _need(payload, 2)
        return TallyDumpRequest(cmd, (payload[1] >> 4) + 1, (payload[1] & 0x0F) + 1)

    if cmd == Command.EXT_CROSSPOINT_TALLY_DUMP_REQUEST:
        _need(payload, 3)
        return TallyDumpRequest(cmd, payload[1] + 1, payload[2] + 1)

    if cmd in (Command.SOURCE_NAMES_REQUEST, Command.DEST_NAMES_REQUEST):
        _need(payload, 3)
        is_source = cmd == Command.SOURCE_NAMES_REQUEST
        return NamesRequest(
            cmd,
            "source" if is_source else "dest",
            (payload[1] >> 4) + 1,
            (payload[1] & 0x0F) + 1 if is_source else None,
            _name_chars(payload[2]),
        )

    if cmd == Command.EXT_SOURCE_NAMES_REQUEST:
        _need(payload, 4)
        return NamesRequest(cmd, "source", payload[1] + 1, payload[2] + 1, _name_chars(payload[3]))

    if cmd == Command.EXT_DEST_NAMES_REQUEST:
        _need(payload, 3)
        return NamesRequest(cmd, "dest", payload[1] + 1, None, _name_chars(payload[2]))

    if cmd == Command.PROTOCOL_IMPLEMENTATION_REQUEST:
        return ProtocolImplementationRequest(cmd)

    return UnknownMessage(cmd, payload)


def _name_chars(code: int) -> int:
    try:
        return _NAME_CHARS[code]
    except KeyError:
        raise ProtocolError(f"unknown name length code {code}") from None


# ---------------------------------------------------------------------------
# Reading aids: plain text for a message, and the record of a trace
# ---------------------------------------------------------------------------


def _title(code: int) -> str:
    """``CROSSPOINT_CONNECT`` as ``Crosspoint connect``."""
    return command_name(code).replace("_", " ").capitalize().replace("Ext ", "Extended ")


def describe(message: Message) -> str:
    """One line of plain text for a parsed message, with 1-based numbers.

    >>> describe(parse_message(bytes.fromhex("02 01 00 02 04")))
    'Crosspoint connect: matrix 1, level 2, destination 3 <- source 5'
    """
    title = _title(message.command)
    if isinstance(message, Crosspoint):
        return (f"{title}: matrix {message.matrix}, level {message.level}, "
                f"destination {message.dest} <- source {message.source}"
                + (", the source is marked as bad" if message.source_bad else ""))
    if isinstance(message, SalvoCrosspoint):
        text = (f"{title}: salvo {message.salvo}, matrix {message.matrix}, level {message.level}, "
                f"destination {message.dest} <- source {message.source}")
        if message.validity is None:
            return text
        if message.validity == 2:
            return f"{title}: salvo {message.salvo}, index {message.index}: no data in this salvo"
        ending = "more follow" if message.validity == 0 else "the last one"
        return f"{text}, index {message.index}, {ending}"
    if isinstance(message, SalvoGo):
        if message.is_done:
            outcome = {0: "crosspoints set", 1: "stored crosspoints cleared",
                       2: "nothing to set or clear"}.get(message.action, f"code {message.action}")
        else:
            outcome = {0: "set the stored crosspoints",
                       1: "clear the stored crosspoints"}.get(message.action, f"code {message.action}")
        return f"{title}: salvo {message.salvo}, {outcome}"
    if isinstance(message, SalvoInterrogate):
        return f"{title}: salvo {message.salvo}, index {message.index}"
    if isinstance(message, DualControllerStatus):
        if message.bit1 is None:
            return f"{title}: no status byte"
        return (f"{title}: status {message.status.hex(' ').upper()}, "
                f"active/idle bit (bit 1) is {'set' if message.bit1 else 'clear'}")
    if isinstance(message, CrosspointRequest):
        where = f"matrix {message.matrix}, level {message.level}, destination {message.dest}"
        return f"{title}: {where} <- source {message.source}" if message.is_connect else f"{title}: {where}"
    if isinstance(message, TallyDump):
        last = message.first_dest + len(message.sources) - 1
        shown = ", ".join(str(source) for source in message.sources[:16])
        more = " ..." if len(message.sources) > 16 else ""
        return (f"{title}: matrix {message.matrix}, level {message.level}, "
                f"destinations {message.first_dest} to {last} <- sources {shown}{more}")
    if isinstance(message, TallyDumpRequest):
        return f"{title}: matrix {message.matrix}, level {message.level}"
    if isinstance(message, Names):
        level = f", level {message.level}" if message.level is not None else ""
        last = message.first + len(message.names) - 1
        shown = ", ".join(repr(name) for name in message.names[:8])
        more = " ..." if len(message.names) > 8 else ""
        return f"{title}: matrix {message.matrix}{level}, {message.first} to {last}: {shown}{more}"
    if isinstance(message, NamesRequest):
        level = f", level {message.level}" if message.level is not None else ""
        return f"{title}: matrix {message.matrix}{level}, {message.chars} characters"
    if isinstance(message, ProtocolImplementation):
        if message.transmitted or message.received:
            return (f"{title}: the router sends {len(message.transmitted)} commands "
                    f"and accepts {len(message.received)}")
        return f"{title}: {len(message.commands)} commands"
    if isinstance(message, ProtocolImplementationRequest):
        return title
    if len(message.payload) == 1:
        return title  # a request without message bytes, such as the dual controller status request
    return f"{title}: {len(message.payload)} bytes, not decoded"


@dataclass(frozen=True)
class TraceEvent:
    """One thing that crossed the wire, as the trace hooks report it.

    ``kind`` is ``"message"`` (a valid frame, ``payload`` holds its DATA
    bytes), ``"ack"``, ``"nak"``, ``"error"`` (a broken frame, ``payload``
    holds what arrived between STX and ETX) or ``"raw"`` (bytes written
    without framing).
    """

    direction: str  #: ``"tx"`` (sent by this side) or ``"rx"`` (received)
    kind: str
    payload: bytes = b""
    note: str = ""
    raw: bytes = b""  #: the bytes on the wire, where they cannot be derived from ``payload``
    protocol: str = "sw-p-08"  #: ``"sw-p-08"`` or ``"sw-p-02"``

    @property
    def wire(self) -> bytes:
        """The bytes as they travel: the complete frame for a message."""
        if self.raw:
            return self.raw
        if self.kind == "message":
            return encode_frame(self.payload)
        if self.kind == "ack":
            return ACK_FRAME
        if self.kind == "nak":
            return NAK_FRAME
        return self.payload
