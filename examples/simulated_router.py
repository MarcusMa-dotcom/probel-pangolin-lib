"""Run a simulated router with names of your own and print what controllers do.

    python simulated_router.py

Listens on 127.0.0.1:8910 with 8 sources, 8 destinations and 2 levels.
Destination 8 is protected: it keeps its source whatever a controller asks for.
"""

import asyncio

from probelpangolin.simulator import SimulatedRouter


async def main() -> None:
    router = SimulatedRouter(8, 8, 2, port=8910)
    router.source_names = [f"CAM {n}" for n in range(1, 7)] + ["VTR", "BARS"]
    router.dest_names = [f"MON {n}" for n in range(1, 7)] + ["PVW", "PGM"]
    router.protected.add(8)
    router.on_client = lambda client, connected: print(client, "connected" if connected else "gone")
    router.on_crosspoint = lambda level, dest, source, client, accepted: print(
        f"{client}: level {level}, {router.dest_names[dest - 1]} <- {router.source_names[source - 1]}"
        + ("" if accepted else "  refused, the destination is protected"))
    async with router:
        print(f"Simulated router on 127.0.0.1:{router.port}. Ctrl+C ends.")
        await asyncio.Event().wait()


try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
except OSError as error:
    raise SystemExit(f"Cannot listen on port 8910: {error.strerror or error}")
