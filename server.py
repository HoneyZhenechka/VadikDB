import socket
import logic
import sys

sock = socket.socket()
server_address = ('localhost', 9080)
print('starting up on{} port {}'.format(*server_address))
sock.bind(server_address)
_logic = logic.Logic()

sock.listen(1)

while True:
    print('waiting for connection')
    connection, client_address = sock.accept()
    try:
        print('connection from', client_address)

        while True:
            data = connection.recv(1024)
            print('received {!r}'.format(data))
            if data:
                print('sending data back to the client')
                #connection.sendall(str.encode(_logic.query(data[1:])))
            else:
                print('no data from', client_address)
                break
    finally:
        connection.close()


