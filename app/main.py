import socket
import os
import time

def handle_request(connection):
    try:
        while True:
            request = connection.recv(1024)
            if not request:
                # Break the loop if the client disconnects
                break
            requests = [request]
            print(request.decode())
            print(requests)
            response = "+PONG\r\n"
            connection.sendall(response.encode())
    except Exception as e:
        print(f"Error handling: {e}")
    finally:
        connection.close()
    
def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    
    while True:
        connection, address = server_socket.accept() # wait for client
        pid = os.fork()
        if pid == 0: #child
            server_socket.close() #close child copy
            handle_request(connection)
            print(f"Connection from {address} closed")
            os._exit(0)# child exits here
        else:
            connection.close() # parent closes its copy of the connection

if __name__ == "__main__":
    main()
