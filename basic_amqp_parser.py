import struct
from datetime import datetime

AMQP_CLASS_METHODS = {
    10 : { 'name' : 'connection',
           'methods' : {
               10 : 'start',
               11 : 'start-ok',
               20 : 'secure',
               21 : 'secure-ok',
               30 : 'tune',
               31 : 'tune-ok',
               40 : 'open',
               41 : 'open-ok',
               50 : 'close',
               51 : 'close-ok',
            }, 
        },
    20 : { 'name' : 'channel',
           'methods' : { 
               10 : 'open',
               11 : 'open-ok',
               20 : 'flow',
               21 : 'flow-ok',
               40 : 'close',
               41 : 'close-ok',
            },
        },
    40 : { 'name' : 'exchange',
           'methods' : {
               10 : 'declare',
               11 : 'declare-ok',
               20 : 'delete',
               21 : 'delete-ok',
            },
        },
    50 : { 'name' : 'queue',
           'methods' : {
               10 : 'declare',
               11 : 'declare-ok',
               20 : 'bind',
               21 : 'bind-ok',
               50 : 'unbind',
               51 : 'unbind-ok',
               30 : 'purge',
               31 : 'purge-ok',
               40 : 'delete',
               41 : 'delete-ok',
            },
        },
    60 : { 'name' : 'basic',
           'methods' : {
               10 : 'qos',
               11 : 'qos-ok',
               20 : 'consume',
               21 : 'consume-ok',
               30 : 'cancel',
               31 : 'cancel-ok',
               40 : 'publish',
               50 : 'return',
               60 : 'deliver',
               70 : 'get',
               71 : 'get-ok',
               72 : 'get-empty',
               80 : 'ack',
               90 : 'reject',
               100 : 'recover-async',
               110 : 'recover',
               111 : 'recover-ok',
            },
        },
    90 : { 'name' : 'tx',
           'methods' : {
               10 : 'select',
               11 : 'select-ok',
               20 : 'commit',
               21 : 'commit-ok',
               30 : 'rollback',
               31 : 'rollback-ok',
            },
        },
}

def get_class_name(class_id): 
    try: 
        return '{id}/{name}'.format(id=class_id, name=AMQP_CLASS_METHODS[class_id]['name'])
    except KeyError:
        return '{id}/{name}'.format(id=class_id, name='UNKNOWN')

def get_method_name(class_id, method_id): 
    try: 
        return '{id}/{name}'.format(id=method_id, name=AMQP_CLASS_METHODS[class_id]['methods'][method_id])
    except KeyError:
        return '{id}/{name}'.format(id=method_id, name='UNKNOWN')


def basic_amqp_parser(source):
    FRAME_END = chr(0xce)
    AMQP_PROTOCOL_HEADER = bytearray( ['A','M','Q','P',0,0,9,1] )
    AMQP_FRAME_HEADER_FMT = '!BHL'
    AMQP_METHOD_HEADER_FMT = '!HH'
    AMQP_HEADER_HEADER_FMT = '!HHQH'
    AMQP_FRAME_HEADER_SIZE = struct.calcsize(AMQP_FRAME_HEADER_FMT)
    AMQP_METHOD_HEADER_SIZE = struct.calcsize(AMQP_METHOD_HEADER_FMT)
    AMQP_HEADER_HEADER_SIZE = struct.calcsize(AMQP_HEADER_HEADER_FMT)
    FRAME_TYPE_METHOD = 1
    FRAME_TYPE_HEADER = 2
    FRAME_TYPE_BODY = 3
    FRAME_TYPE_HEARTBEAT = 4

    ts = None
    packet_stream = bytearray()
    while True:
        print ':'.join('{:02x}'.format(c) for c in packet_stream)

        try: 
            while len(packet_stream) < max(AMQP_FRAME_HEADER_SIZE,len(AMQP_PROTOCOL_HEADER)):
                packet_stream += yield

            ts = datetime.now()

            if packet_stream[:len(AMQP_PROTOCOL_HEADER)] == AMQP_PROTOCOL_HEADER:
                del packet_stream[:len(AMQP_PROTOCOL_HEADER)]
                print '[{s}] {t} PROTOCOL HEADER.'.format(s=source,t=ts)
                continue

            frame_type, channel, size = struct.unpack_from(AMQP_FRAME_HEADER_FMT,packet_stream[:AMQP_FRAME_HEADER_SIZE]) 
            del packet_stream[:AMQP_FRAME_HEADER_SIZE]

            while len(packet_stream) < size:
                packet_stream += yield

            if frame_type == FRAME_TYPE_METHOD:
                # METHOD
                class_id, method_id = struct.unpack_from(AMQP_METHOD_HEADER_FMT, packet_stream[:AMQP_METHOD_HEADER_SIZE])
                del packet_stream[:AMQP_METHOD_HEADER_SIZE]
                arguments = packet_stream[:size-AMQP_METHOD_HEADER_SIZE]
                del packet_stream[:size-AMQP_METHOD_HEADER_SIZE]
                print '[{s}] {t} METHOD: ({c},{m}) {a}'.format(s=source, t=str(ts), c=get_class_name(class_id), m=get_method_name(class_id, method_id), a=':'.join('{:02x}'.format(c) for c in arguments))

            elif frame_type == FRAME_TYPE_HEADER:
                # HEADER
                property_flags_array = [None]
                class_id, weight, body_size, property_flags_array[0] = struct.unpack_from(AMQP_HEADER_HEADER_FMT,packet_stream[:AMQP_HEADER_HEADER_SIZE])
                del packet_stream[:AMQP_HEADER_HEADER_SIZE]

                # property flags that have bit0 set indicate more property flags
                while property_flags_array[-1] % 2 == 1:
                    property_flags_array.append(struct('!H',packet_stream))
                    del packet_stream[:struct.calcsize('!H')]

                property_list = packet_stream[:size-AMQP_HEADER_HEADER_SIZE-len(property_flags_array)+1]
                del packet_stream[:size-AMQP_HEADER_HEADER_SIZE-len(property_flags_array)+1]
                print '[{s}] {t} HEADER: ({c}) body_size: {size}, property_flags: {f} {l}'.format(s=source, t=str(ts), c=get_class_name(class_id), size=body_size, f=':'.join( '{0:b}'.format(x) for x in property_flags_array ), l=':'.join('{:02x}'.format(c) for c in property_list))

            elif frame_type == FRAME_TYPE_BODY:
                # BODY
                del packet_stream[:size]
                print '[{s}] {t} BODY: {size} bytes.'.format(s=source, t=str(ts), size=size)

            elif frame_type == FRAME_TYPE_HEARTBEAT:
                # HEARTBEAT
                del packet_stream[:size]
                print '[{s}] {t} HEARTBEAT.'.format(s=source, t=str(ts))

            else:
                # UNDEFINED
                del packet_stream[:size]
                print '[{s}] {t} UNDEFINED.'.format(s=source, t=str(ts))

            while len(packet_stream) < len(FRAME_END):
                packet_stream += yield

            # FRAME END
            if packet_stream[:len(FRAME_END)] != FRAME_END:
                print '[{s}] Invalid FRAME END: {e}'.format(s=source, e=':'.join('{:02x}'.format(c) for c in packet_stream[:len(FRAME_END)]))
            del packet_stream[:len(FRAME_END)]
        except Exception as e: 
            import traceback
            print traceback.print_exc()
            print '[{s}] Exception encountered: {e}'.format(s=source, e=e)
            print '[{s}] Falling back to hex dump'
            while True:
                end_mark = packet_stream.find(FRAME_END)
                if end_mark > 0:
                    print '[{s}] {t} DUMP: {d}'.format(s=source, t=str(ts), d=':'.join('{:02x}'.format(c) for c in packet_stream[:end_mark+1]))
                    del packet_stream[:end_mark+1]
                    break
                else:
                    print '[{s}] {t} DUMP: {d}'.format(s=source, t=str(ts), d=':'.join('{:02x}'.format(c) for c in packet_stream))
                    del packet_stream[:]
                    packet_stream += yield
                    ts = datetime.now()
