"""Command line front end: ``python -m probelpangolin --host 10.0.0.20 route 5 3``."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import List, Optional, Union

from .client import DEFAULT_PORT, AsyncRouter
from .errors import SWP08Error
from .protocol import command_name


def _port(value: str) -> Union[int, str]:
    """Sources/destinations may be given as number or as name."""
    return int(value) if value.isdigit() else value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="probelpangolin", description="Control a router (Kreuzschiene) via SW-P-08."
    )
    parser.add_argument("--host", required=True, help="IP address of the router / controller")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="TCP port (default 8910)")
    parser.add_argument("--matrix", type=int, default=1)
    parser.add_argument("--levels", type=int, default=1, help="number of levels in the router")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--extended", action="store_true", help="always use extended commands")
    group.add_argument("--standard", action="store_true", help="never use extended commands")
    parser.add_argument("--no-query", action="store_true",
                        help="do not ask the router for its implemented commands")
    parser.add_argument("--name-length", type=int, choices=(4, 8, 12), default=8)
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="-v: info, -vv: show every frame")
    sub = parser.add_subparsers(dest="action", required=True)

    route = sub.add_parser("route", help="route a source to a destination")
    route.add_argument("source", type=_port, help="source number or name")
    route.add_argument("dest", type=_port, help="destination number or name")
    route.add_argument("-l", "--level", type=int, action="append",
                       help="level, repeatable (default 1)")
    route.add_argument("--all-levels", action="store_true")
    route.add_argument("--no-confirm", action="store_true",
                       help="do not wait for the router's confirmation")

    get = sub.add_parser("get", help="show the source routed to a destination")
    get.add_argument("dest", type=_port)
    get.add_argument("-l", "--level", type=int, help="level (default: all)")

    dump = sub.add_parser("dump", help="show all crosspoints (tally dump)")
    dump.add_argument("-l", "--level", type=int, help="level (default: all)")

    sub.add_parser("names", help="list source and destination names")
    sub.add_parser("info", help="show the commands the router implements")
    sub.add_parser("monitor", help="print crosspoint changes until Ctrl-C")
    return parser


def _label(number: int, names: dict) -> str:
    name = names.get(number)
    return f"{number} ({name})" if name else str(number)


async def _run(args: argparse.Namespace) -> int:
    extended: Optional[bool] = True if args.extended else False if args.standard else None
    uses_names = args.action in ("names", "monitor") or any(
        isinstance(getattr(args, key, None), str) for key in ("source", "dest")
    )
    router = AsyncRouter(
        args.host, args.port,
        matrix=args.matrix, levels=args.levels, extended=extended,
        query_commands=not args.no_query, fetch_names=uses_names,
        name_length=args.name_length, reconnect=args.action == "monitor",
    )
    async with router:
        sources, dests = router.source_names, router.dest_names
        if args.action == "route":
            levels = list(router.all_levels) if args.all_levels else (args.level or [1])
            await router.route(args.source, args.dest, levels, confirm=not args.no_confirm)
            print(f"routed {args.source} -> {args.dest} on level {', '.join(map(str, levels))}")
        elif args.action == "get":
            levels = [args.level] if args.level else list(router.all_levels)
            for level, source in (await router.get_routes(args.dest, levels)).items():
                print(f"level {level}: {args.dest} <- {_label(source, sources)}")
        elif args.action == "dump":
            for level in [args.level] if args.level else router.all_levels:
                table = await router.tally_dump(level)
                print(f"level {level}: {len(table)} destinations")
                for dest in sorted(table):
                    print(f"  {dest:>5} <- {table[dest]}")
        elif args.action == "names":
            print(f"{len(sources)} sources")
            for number in sorted(sources):
                print(f"  {number:>5}  {sources[number]}")
            print(f"{len(dests)} destinations")
            for number in sorted(dests):
                print(f"  {number:>5}  {dests[number]}")
        elif args.action == "info":
            commands = router.implemented_commands
            if not commands:
                print("router did not report its implemented commands")
            for code in sorted(commands or ()):
                print(f"  0x{code:02X}  {command_name(code)}")
        elif args.action == "monitor":
            print("waiting for crosspoint changes, Ctrl-C to stop", file=sys.stderr)
            async for change in router.changes():
                print(
                    f"level {change.level}: {_label(change.dest, router.dest_names)}"
                    f" <- {_label(change.source, router.source_names)}",
                    flush=True,
                )
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    args = _parser().parse_args(argv)
    level = (logging.WARNING, logging.INFO, logging.DEBUG)[min(args.verbose, 2)]
    logging.basicConfig(level=level, format="%(levelname)s %(message)s")
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        return 130
    except (SWP08Error, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
