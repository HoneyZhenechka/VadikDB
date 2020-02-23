import socket
import logic
import sys

sock = socket.socket()
server_address = ('localhost', 9080)
print('starting up on{} port {}'.format(*server_address))
sock.bind(server_address)
_logic = logic.Logic('vadik')

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
                encoding = 'utf-8'
                data = data.decode(encoding)
                print(data)
                data = _logic.query(data)
                connection.sendall(str.encode(str(data.is_exception)))
                connection.sendall(str.encode(data.str_for_print))
                connection.sendall(str.encode(str(data.exception_func)))
                connection.sendall(str.encode(data.fields_for_func))

            else:
                print('no data from', client_address)
                break
    finally:
        connection.close()


