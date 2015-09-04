import socket
import sys
import select

listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(('127.0.0.1', 25672))
listener.listen(1)

client, address = listener.accept()

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.connect(('127.0.0.1',5672))

while True:

    ready_sockets = select.select([server, client], [], [])[0]

    if server in ready_sockets:
        data = server.recv(16384)
        if len(data) > 0:
            client.send(data)
        else:
            break
        print 'SERVER BYTES', ":".join("{:02x}".format(ord(c)) for c in data)
    if client in ready_sockets:
        data = client.recv(16384)
        if len(data):
            server.send(data)
        else:
            break
        print 'CLIENT BYTES', ":".join("{:02x}".format(ord(c)) for c in data)
