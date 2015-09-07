
from datetime import datetime

def hex_dump_noparser(source):
    while True:
        packet_stream = yield
        ts = datetime.now()
        print '[{s}] {t} DUMP: {d}'.format(s=source, t=str(ts), d=':'.join('{:02x}'.format(c) for c in bytearray(packet_stream)))

