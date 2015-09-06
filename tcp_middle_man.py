import socket
import sys
import select
from basic_amqp_parser import basic_amqp_parser

listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(('127.0.0.1', 25672))
listener.listen(1)

client, address = listener.accept()

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.connect(('127.0.0.1',5672))

server_parser = basic_amqp_parser('SERVER')
next(server_parser)

client_parser = basic_amqp_parser('CLIENT')
next(client_parser)

while True:

    ready_sockets = select.select([server, client], [], [])[0]

    if server in ready_sockets:
        data = server.recv(16384)
        if len(data) > 0:
            client.send(data)
            server_parser.send(data)
        else:
            break

    if client in ready_sockets:
        data = client.recv(16384)
        if len(data) > 0:
            server.send(data)
            client_parser.send(data)
        else:
            break
