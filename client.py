import socket

sock = socket.socket()

server_address = ('localhost', 9080)
print('connecting to {} port {}'.format(*server_address))
sock.connect(server_address)

message = (b"123")
try:
    print('sending {!r}'.format(message))
    sock.sendall(message)

    amount_received = 0
    amount_expected = len(message)

    while amount_received < amount_expected:
        data = sock.recv(1024)
        amount_received += len(data)
        print('received {!r}'.format(data))

finally:
    print('closing socket')
    sock.close()