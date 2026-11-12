"""Decode captured SW-P-08 bytes without any network.

    python decode_capture.py "10 02 02 01 00 02 04 05 F2 10 03 10 06"
"""

import sys

from probelpangolin import FrameDecoder, describe, parse_message
from probelpangolin.protocol import Ack, Frame, FrameError, Nak

text = sys.argv[1] if len(sys.argv) > 1 else "10 02 02 01 00 02 04 05 F2 10 03 10 06"
for event in FrameDecoder().feed(bytes.fromhex(text)):
    if isinstance(event, Frame):
        print(event.payload.hex(" ").upper(), "->", describe(parse_message(event.payload)))
    elif isinstance(event, FrameError):
        print("broken frame:", event.reason, event.raw.hex(" ").upper())
    elif isinstance(event, (Ack, Nak)):
        print(type(event).__name__.upper())
