<p align="center">
  <img src="docs/images/logo.png" alt="Probel Pangolin logo" width="160">
</p>

# probelpangolin

A Python library for SW-P-08, the General Remote Control Protocol of Pro-Bel
(later Snell, now Grass Valley) that most broadcast routers and router
controllers speak over TCP. It does three things:

- as a **controller** it switches crosspoints, reads names and the complete
  state of a router, and follows what other panels switch,
- as a **simulated router** it answers any SW-P-08 controller, for trying code
  and controllers without hardware,
- as a **decoder** it turns bytes into frames and frames into typed messages
  and plain text, with a hook that reports everything that crosses the wire.

It needs only the Python standard library (Python 3.10 or newer).

`probelpangolin` is the library behind [Probel Pangolin](https://github.com/MarcusMa-dotcom/probel-pangolin),
the test program with a desktop window, a web interface and a REST API. Use the
library when you want SW-P-08 or SW-P-02 inside your own software. The
messages of both protocols are laid out byte by byte in
[the protocols as Probel Pangolin speaks them](https://github.com/MarcusMa-dotcom/probel-pangolin/blob/main/docs/protocols.md).

```python
from probelpangolin import Router

with Router("10.0.0.20", levels=2, fetch_names=True) as router:
    router.route(source=5, dest=3)                    # level 1
    router.route(5, 3, level=[1, 2], confirm=True)    # several levels, with confirmation
    router.route("CAM 5", "MON 3")                    # by the names of the router

    print(router.get_route(3))          # ask for the source on destination 3 -> 5
    print(router.tally_dump(level=1))   # the complete state: {destination: source}
    print(router.source_names)          # {1: "CAM 1", ...}
```

The protocol follows two sources. Framing, ACK and NAK, the crosspoint, tally
dump, salvo and dual controller messages follow the public specification
*SW-P-88 Router Control Protocols* (Pro-Bel / Snell, issue 4a), and its
examples are part of the tests. The extended commands and the name messages
follow the Bitfocus Companion module
[`companion-module-generic-swp08`](https://github.com/bitfocus/companion-module-generic-swp08).
Matrix, level, source and destination count **from 1**, as on a panel. The
default port is 8910.

## Install

```bash
pip install "git+https://github.com/MarcusMa-dotcom/probel-pangolin-lib@v0.1.0"
```

Or from a copy of this folder:

```bash
pip install .
```

## SW-P-08 in five sentences

A controller and a router exchange messages over TCP, on a port set at the
router, or over RS-232 and RS-422. Every message is wrapped into a frame:
`DLE STX`, the data bytes, a byte count, a checksum, `DLE ETX`, and every data
byte that equals DLE (0x10) is sent twice. The receiver answers each frame
with `DLE ACK` or, if count or checksum are wrong, with `DLE NAK`, within one
second and best within 10 milliseconds. A message without ACK is sent again,
up to five times. A crosspoint is named by matrix, level, destination and
source. The standard commands reach 16 matrices, 16 levels and 1024 sources
and destinations, the extended commands 256 matrices, 256 levels and 65536
sources and destinations.

## Control a router

`Router` blocks until each call is done and runs the network in a background
thread. `AsyncRouter` has the same methods for asyncio:

```python
import asyncio
from probelpangolin import AsyncRouter

async def main():
    async with AsyncRouter("10.0.0.20", fetch_tally=True) as router:
        await router.route(5, 3, confirm=True)
        async for change in router.changes():          # follow every tally
            print(f"level {change.level}: destination {change.dest} <- source {change.source}")

asyncio.run(main())
```

| Task | Method |
|---|---|
| Switch a crosspoint | `route(source, dest, level=1, confirm=False)` |
| Ask for a crosspoint | `get_route(dest, level=1)`, `get_routes(dest)` |
| Read the complete state | `tally_dump(level)`, `tally_dump_all()` |
| State without asking | `routes`, `source_of(dest)`, `destinations_of(source)` |
| Names | `read_names()`, `source_names`, `dest_names`, `find_source(name)`, `find_dest(name)` |
| Follow changes | `on_route(callback)`, `changes()` (async), `on_connection(callback)` |
| Implemented commands | `query_commands()`, `implemented_commands`, `implementation` (sent and accepted commands apart) |
| Salvo groups | `salvo_add(source, dest, salvo, level=1)`, `salvo_go(salvo, clear=False)`, `salvo_read(salvo)` |
| Two controllers | `dual_controller_status()` |
| Any command | `send(payload)`, `request(payload, reply)`, `on_message(callback)` |
| Trace | `on_trace(callback)` |
| Bytes as they are | `write_raw(data)` |

- **`confirm=True`** waits for the *Crosspoint Connected* of the router. If it
  reports another source, for example because the destination is protected,
  `RouteRejectedError` is raised. If it stays silent, `ResponseTimeoutError`.
  Without `confirm` the call is done once the message was acknowledged (DLE ACK).
- **Tallies of other panels** go into the cache (`routes`) and call the
  `on_route` callbacks, but only when something really changed.
- **Extended commands** are used by themselves when the numbers need them or
  the router reports them. Force them with `extended=True`, forbid them with
  `extended=False`.
- **Salvo groups** (0 to 127) collect crosspoints at the router and set them
  all at the same moment: `salvo_add` stores one, `salvo_go` sets the group
  and returns 0 (set), 1 (cleared) or 2 (the group held nothing). The router
  sends no *Crosspoint Connected* for a salvo. The client follows the
  acknowledges of the router instead, so `routes` and `on_route` stay right,
  also for a salvo another panel fired. Standard commands only: sources and
  destinations up to 1024.
- **The connection**: every message is sent on its own and its ACK awaited
  (1 second, 5 attempts, as the specification recommends). After 30 seconds of silence an interrogate goes out
  as keep-alive. A lost connection is opened again, and names and state are
  read again if `fetch_names` or `fetch_tally` are set.

```python
Router(
    "10.0.0.20", 8910,
    matrix=1,              # number of the matrix
    levels=1,              # how many levels the router has, for "all levels"
    extended=None,         # None: decide by itself
    query_commands=True,   # ask on connect which commands are implemented
    fetch_names=False,     # read the names on connect
    fetch_tally=False,     # read the complete state on connect
    name_length=8,         # 4, 8 or 12 characters
    name_encoding="latin-1",
    ack_timeout=1.0, attempts=5,
    wait_ack=True,         # False for devices that never acknowledge
    response_timeout=2.0,
    keepalive=30.0,        # None: off
    reconnect=True, reconnect_delay=2.0,
)
```

All errors of the library derive from `SWP08Error`: `NotConnectedError`,
`AckTimeoutError`, `NakError`, `ResponseTimeoutError`, `RouteRejectedError`,
`UnsupportedCommandError`, `UnknownNameError`, `ProtocolError`.

## Trace and decode

`on_trace` reports everything a client sends and receives as a `TraceEvent`:
`direction` (`tx` or `rx`), `kind` (`message`, `ack`, `nak`, `error`, `raw`),
the `payload`, a `note` (such as `attempt 2 of 2`) and `wire`, the bytes as
they travelled. Register it before `open()` to see the start-up queries too.

```python
from probelpangolin import AsyncRouter, describe, parse_message

def show(event):
    meaning = describe(parse_message(event.payload)) if event.kind == "message" else event.kind
    print(event.direction, event.wire.hex(" "), meaning)

router = AsyncRouter("10.0.0.20")
router.on_trace(show)
```

`probelpangolin.protocol` holds framing and messages without any network, for
other transports or for decoding a capture:

```python
from probelpangolin.protocol import build_connect, encode_frame, FrameDecoder, parse_message, describe

encode_frame(build_connect(source=5, dest=3, level=2)).hex(" ")
# '10 02 02 01 00 02 04 05 f2 10 03'

received = bytes.fromhex("10 02 04 01 00 02 04 05 f0 10 03")
for event in FrameDecoder().feed(received):
    message = parse_message(event.payload)
    print(message)             # Crosspoint(command=4, matrix=1, level=2, dest=3, source=5)
    print(describe(message))   # Crosspoint connected: matrix 1, level 2, destination 3 <- source 5
```

`parse_message` knows both directions: what a router sends (`Crosspoint`,
`TallyDump`, `Names`, `ProtocolImplementation`, `DualControllerStatus`), what
a controller sends (`CrosspointRequest`, `TallyDumpRequest`, `NamesRequest`,
`ProtocolImplementationRequest`) and the salvo messages of both
(`SalvoCrosspoint`, `SalvoGo`, `SalvoInterrogate`). Any other valid message
comes back as `UnknownMessage` with its bytes.

The decoder follows the specification where it is easy to get wrong: an ACK
or NAK in the middle of a frame is reported and the frame goes on, and
`Crosspoint.source_bad` carries the bit with which TDM and digital video
routers mark a bad source.

`write_raw(data)` writes bytes exactly as given, without frame and without
waiting for an ACK, to see how a router reacts to a wrong checksum.
`send(payload, check=False)` sends a message even if the router did not list
its command.

## SW-P-02

`probelpangolin.swp02` has the same three things for SW-P-02, the older
General Switcher protocol: one level, no matrix number, no names and no
acknowledge on the data link. Client and simulated router have the methods
of their SW-P-08 siblings, so code written for one runs with the other:

```python
from probelpangolin.swp02 import Router

with Router("10.0.0.30", dests=16) as router:       # TCP port 2000 unless given
    router.route(source=5, dest=3, confirm=True)     # waits for the CONNECTED of the router
    print(router.get_route(3))                       # INTERROGATE, answered with a TALLY
    print(router.tally_dump())                       # asks destinations 1 to 16 one by one
    router.salvo_add(7, 2)                           # CONNECT ON GO
    router.salvo_go()                                # GO: 0 set, 1 cleared, 2 nothing stored
```

| | |
|---|---|
| Wire format | `encode_message`, `MessageDecoder`, `parse_message`, `describe`, `build_connect`, `build_interrogate`, `build_connect_on_go`, `build_go` |
| Client | `AsyncRouter`, `Router`: `route`, `get_route`, `tally_dump` (needs `dests=`), `salvo_add`, `salvo_go`, `on_route`, `on_trace`, `send`, `write_raw` |
| Simulated router | `SimulatedRouter(sources, dests)` with `protected`, `set_crosspoint`, `drop_frames`, the hooks, and `salvos` for what waits for the next GO |

What the protocol does not have raises `UnsupportedCommandError`: names, the
list of implemented commands, reading back the store, the dual controller
status. A destination the router does not have comes back with a reserved
source number and raises `ProtocolError`. After a GO the router sends no
CONNECTED, so the client interrogates the destinations concerned. A message
carries no length: the command byte tells it, and a command without a known
length is reported undecoded up to the next start of message. Only the
commands of the public specification are laid out: 01 to 06, 12 and 13.

A simulated SW-P-02 router on the usual port:

```python
import asyncio
from probelpangolin.swp02 import SimulatedRouter

async def main():
    async with SimulatedRouter(16, 16, port=2000):
        await asyncio.Event().wait()

asyncio.run(main())
```

## Simulate a router

```bash
python -m probelpangolin.simulator --port 8910 --sources 32 --dests 32 --levels 2
```

Or inside a program:

```python
from probelpangolin.simulator import SimulatedRouter

async with SimulatedRouter(16, 16, levels=2, port=8910) as router:
    router.source_names[4] = "CAM 5"
    router.protected.add(8)                  # destination 8 keeps its source
    router.set_crosspoint(5, 3)              # as if switched at the router: all clients are told
    router.set_crosspoint(6, 3, announce=False)   # ... or not
    router.drop_frames = 1                   # the next message gets no answer
    router.nak_frames = 1                    # the one after that a NAK
    router.on_crosspoint = lambda level, dest, source, client, accepted: print(client, dest, source)
    router.on_trace = lambda client, event: print(client, event.direction, event.wire.hex(" "))
```

It answers crosspoint connect and interrogate, tally dump requests, name
requests and the protocol implementation request, standard and extended, and
keeps salvo groups (`router.salvos`, `router.on_salvo`). Unlike a real
controller it does not send a message again when a client fails to
acknowledge it.

## Command line

```bash
python -m probelpangolin --host 10.0.0.20 route 5 3
python -m probelpangolin --host 10.0.0.20 --levels 2 route 5 3 --all-levels
python -m probelpangolin --host 10.0.0.20 route "CAM 5" "MON 3"
python -m probelpangolin --host 10.0.0.20 get 3
python -m probelpangolin --host 10.0.0.20 dump
python -m probelpangolin --host 10.0.0.20 names
python -m probelpangolin --host 10.0.0.20 info
python -m probelpangolin --host 10.0.0.20 monitor
python -m probelpangolin --host 10.0.0.20 -vv route 5 3
```

`info` lists the implemented commands, `monitor` prints tallies until Ctrl+C,
`-vv` shows every frame as hex. Once installed the command is also there as
`probelpangolin`.

## Examples

| File | Shows |
|---|---|
| `examples/take.py` | Route by number or name, with confirmation, and ask what is routed |
| `examples/tally_monitor.py` | Follow every crosspoint change of a router |
| `examples/trace.py` | Every frame of a session with bytes and meaning |
| `examples/simulated_router.py` | A simulated router with own names and a protected destination |
| `examples/decode_capture.py` | Decode captured bytes without any network |
| `examples/swp02_take.py` | The same take with SW-P-02 |

## Notes

- Tested against the simulated router of this library, with the examples of
  the specification and the test vectors of the Companion module, **not
  against a hardware router**. Test runs under Python 3.13.
- The public specification covers the main commands only. The extended
  commands and the names follow the Companion module. The *Protocol
  Implementation Response* (0x62) is read as the example of the specification
  shows it: two counts, then the commands the router sends and the ones it
  accepts.
- Ready-made methods exist for connect, interrogate, tally dump, names,
  protocol implementation, salvo groups and the dual controller status.
  Protects, UMD labels, tie lines and the rest are there as command codes
  (`probelpangolin.Command`) and have to be put together with `send()` and
  `request()`.
- Dual controller status: the specification names the bit that tells the
  active from the idle controller (`DualControllerStatus.bit1`), not which
  value means active.
- SYSTEM2 controllers count matrix and level from 1 on the wire, all others
  from 0. For such a controller use `matrix=2` and add one to the level.
- Salvo numbers and their indexes are 0 to 127 as they travel, unlike matrix,
  level, source and destination.
- `connection_factory=` puts `AsyncRouter` on another transport, for example
  `serial_asyncio.open_serial_connection` for RS-422. That is untested.
- Callbacks of the blocking `Router` are called from its background thread.
  Keep them short.

## Tests

```bash
pip install pytest
```

```bash
python -m pytest
```

## Siblings

The same pair of test program and library exists for two more broadcast
control protocols:

| Library | Protocol | Program |
|---|---|---|
| [tally-tapir-lib](https://github.com/MarcusMa-dotcom/tally-tapir-lib) | TSL UMD tally, V3.1, V4.0 and V5.0 | [Tally Tapir](https://github.com/MarcusMa-dotcom/tally-tapir) |
| [ember-emu-lib](https://github.com/MarcusMa-dotcom/ember-emu-lib) | Ember+: S101, BER and Glow | [Ember Emu](https://github.com/MarcusMa-dotcom/ember-emu) |
| probel-pangolin-lib | SW-P-08 and SW-P-02 router control | [Probel Pangolin](https://github.com/MarcusMa-dotcom/probel-pangolin) |

## License

MIT License. Copyright (c) 2026 Marcus Mathy. Use at your own risk: the
software comes as is, without warranty of any kind. The full text is in
[LICENSE](LICENSE).

Command table and message layouts follow the MIT-licensed Bitfocus Companion
module generic-swp08 (© Bitfocus AS). SW-P-08 is the General Remote Control
Protocol of Pro-Bel, later Snell and Grass Valley. This library is an
independent implementation and is not affiliated with Grass Valley.
