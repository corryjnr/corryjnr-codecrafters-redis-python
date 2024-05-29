import socket
import os
import time

set_string = dict()

def parse_request(request):
    requests = list(request.decode().split())
    print(request.decode())
    command = requests[2] #The command used
    try:
        arg1 = requests[4] #First argument passed to command
    except:
        arg1 = None

    print(f"Received command: {command}")
    
    # Handle PING
    if command.lower() == 'ping':
        return '+PONG\r\n'
    
    # Handle ECHO
    elif command.lower() == 'echo':
        return '+' + arg1 + '\r\n'
    
    # Handle SET
    elif command.lower() == 'set':
        if 'px' in requests:
            print(f"Set expiry: {requests[10]}")
        set_command(arg1)
    
    #Handle GET
    elif command.lower() == 'get':
        return get_command(arg1)
    
def set_command(arg1, expiry=0):
    set_string[arg1] = requests[6]
    print(set_string)
    return '+OK\r\n'

def get_command(arg1):
    if set_string.get(arg1) != None:
        return '+' + set_string[arg1] + '\r\n'
    else: return '-1\r\n'

def handle_request(connection):
    try:
        while True:
            request = connection.recv(1024)
            if not request:
                # Break the loop if the client disconnects
                break
            response = parse_request(request)
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
