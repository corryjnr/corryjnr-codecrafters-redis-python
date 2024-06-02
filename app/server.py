import socket

def main():
    server_socket = socket.create_server(("localhost", 4221), reuse_port=True)
    while True:
        print("Server started")
        connection, address = server_socket.accept() # accept client
        
        request = connection.recv(1024)
        print(f"Request: {request.decode()}")
        response = """HTTP/1.1 200 OK 
        
                Response received\r\n\r\n"""
        connection.sendall(response.encode())
if __name__ == "__main__":
    main()