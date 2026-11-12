"""Show every frame of a session with its bytes and its meaning.

    python trace.py 10.0.0.20

Connects, asks for the implemented commands, routes source 2 to destination 1
and prints what crossed the wire, the ACKs included.
"""

import asyncio
import sys

from probelpangolin import AsyncRouter, TraceEvent, describe, parse_message


def show(event: TraceEvent) -> None:
    """One line per frame: direction, bytes on the wire, meaning."""
    if event.kind == "message":
        meaning = describe(parse_message(event.payload))
    else:
        meaning = f"{event.kind.upper()} {event.note}".strip()
    print(f"{event.direction.upper()}  {event.wire.hex(' ').upper():<44}  {meaning}")


async def main(host: str) -> None:
    router = AsyncRouter(host)
    router.on_trace(show)          # before open(), so the start-up queries are traced too
    async with router:
        await router.route(2, 1, confirm=True)


asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"))
