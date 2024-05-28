import socket


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    response = "+PONG\r\n"
    
    while True:
        connection, address = server_socket.accept() # wait for client
        data = connection.recv(1024)
        print(data.decode())
        
        connection.sendall(response.encode())
        connection.close()


if __name__ == "__main__":
    main()
