import socket


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, address = server_socket.accept() # wait for client
    data = connection.recv(1024)
    print(data.decode())
    response = "+PONG\r\n"
    connection.sendall(response.encode())


if __name__ == "__main__":
    main()
