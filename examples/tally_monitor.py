"""Print every crosspoint change of a router, whoever made it (asyncio).

    python tally_monitor.py 10.0.0.20
"""

import asyncio
import sys

from probelpangolin import AsyncRouter


async def main(host: str) -> None:
    async with AsyncRouter(host, fetch_names=True, fetch_tally=True) as router:
        print(f"{len(router.routes.get(1, {}))} destinations read, waiting for changes. Ctrl+C ends.")
        async for change in router.changes():
            print(f"level {change.level}: {router.dest_name(change.dest) or change.dest}"
                  f" <- {router.source_name(change.source) or change.source}")


try:
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"))
except KeyboardInterrupt:
    pass
