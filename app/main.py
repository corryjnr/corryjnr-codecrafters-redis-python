import socket
import os
import time

set_string = dict()

def parse_request(request):
    requests = list(request.decode().split())
    print(request.decode())
    command = requests[2].lower() #The command used
    try:
        arg1 = requests[4] #First argument passed to command
    except:
        arg1 = None

    print(f"Received command: {command}")
    
    # Handle PING
    if command == 'ping':
        return b'+PONG\r\n'
    
    # Handle ECHO
    elif command == 'echo':
        return b'+' + arg1.encode() + b'\r\n'
    
    # Handle SET
    elif command == 'set':
        key = arg1
        value = requests[6]
        if 'px' in requests or 'PX' in requests:
            expiry_time = int(requests[10])
            print(f"Set expiry: {expiry_time}")
            return set_command(key, (value, time.time() + expiry_time / 1000))
        else:
            return set_command(key, (value, None))
    
    #Handle GET
    elif command == 'get':
        return get_command(arg1)
    
def set_command(key, value):
    set_string[key] = value
    print(set_string)
    return b'+OK\r\n'

def get_command(arg1):
    value, expiry = set_string.get(arg1, (None, None))
    if value == None or (expiry and time.time() > expiry):
        return b"$-1\r\n"
    return b'+' + value.encode() + b'\r\n'
    

def handle_request(connection):
    try:
        while True:
            request = connection.recv(1024)
            if not request:
                # Break the loop if the client disconnects
                break
            response = parse_request(request)
            connection.sendall(response)
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
