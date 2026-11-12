"""Route a source to a destination and ask the router what it did (blocking).

    python take.py 10.0.0.20 5 3
    python take.py 10.0.0.20 "CAM 5" "MON 3" --levels 2
"""

import argparse

from probelpangolin import RouteRejectedError, Router, SWP08Error

parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
parser.add_argument("host")
parser.add_argument("source", help="number or name")
parser.add_argument("dest", help="number or name")
parser.add_argument("--port", type=int, default=8910)
parser.add_argument("--levels", type=int, default=1, help="number of levels of the router")
args = parser.parse_args()

source = int(args.source) if args.source.isdigit() else args.source
dest = int(args.dest) if args.dest.isdigit() else args.dest

try:
    with Router(args.host, args.port, levels=args.levels, fetch_names=True) as router:
        try:
            router.route(source, dest, level=router.all_levels, confirm=True)
        except RouteRejectedError as error:
            print("The router did not switch:", error)
        for level, current in router.get_routes(dest).items():
            print(f"level {level}: {args.dest} <- {current} {router.source_name(current) or ''}")
except SWP08Error as error:
    raise SystemExit(f"error: {error}")
