"""Route a source to a destination with SW-P-02 and print every message (blocking).

    python swp02_take.py 10.0.0.30 5 3
"""

import sys

from probelpangolin import RouteRejectedError, SWP08Error
from probelpangolin.swp02 import Router, describe, parse_message

host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
source = int(sys.argv[2]) if len(sys.argv) > 2 else 5
dest = int(sys.argv[3]) if len(sys.argv) > 3 else 3


def show(event):
    """Direction, bytes on the wire and meaning of one message."""
    meaning = describe(parse_message(event.payload)) if event.kind == "message" else event.note
    print(f"{event.direction.upper()}  {event.wire.hex(' ').upper():<22}  {meaning}")


try:
    with Router(host) as router:
        router.on_trace(show)
        try:
            router.route(source, dest, confirm=True)
        except RouteRejectedError as error:
            print("The router did not switch:", error)
        print(f"destination {dest} has source {router.get_route(dest)}")
except SWP08Error as error:
    raise SystemExit(f"error: {error}")
