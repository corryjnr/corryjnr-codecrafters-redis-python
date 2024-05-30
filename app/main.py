import socket
import os
import time
import sys
import argparse

set_string = dict()

def parse_request(request):
    requests = list(request.decode().split())
    print(request.decode())
    command = requests[2].upper() #The command used
    try:
        arg1 = requests[4] #First argument passed to command
    except:
        arg1 = None

    print(f"Received command: {command}")
    
    # Handle PING
    if command == 'PING':
        return b'+PONG\r\n'
    # Handle ECHO
    elif command == 'ECHO':
        return b'+' + arg1.encode() + b'\r\n'
    # Handle SET
    elif command == 'SET':
        key = arg1
        value = requests[6]
        if 'px' in requests or 'PX' in requests:
            expiry_time = int(requests[10])
            print(f"Set expiry: {expiry_time}")
            return set_command(key, (value, time.time() + expiry_time / 1000))
        else:
            return set_command(key, (value, None))
    # Handle GET
    elif command == 'GET':
        return get_command(arg1)
    # Handle INFO
    elif command == 'INFO':
        argument = requests[4]
        return info_command(argument)

def info_command(argument):
    if argument == "replication":
        role = server_role
        print(role)
        if role == 'master':
            return b'+role:master\r\n'
        elif role == 'slave':
            return b'+role:slave\r\n'
    
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
    
def main(host, port, role="master"):
    global server_role
    role_ = role
    server_port = port
    server_role = role_
    
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    
    server_socket = socket.create_server((host, port), reuse_port=True)
    
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", help="specify port to connect", type=int, nargs=1)
    parser.add_argument("--replicaof", help="specify master", type=str)
    args = parser.parse_args()
    if args.port != None:
        port = args.port
        if args.replicaof != None:
            master_host, master_port = args.replicaof.split(" ")
            main(host="localhost", port=args.port, role="slave")
            print("Connecting to port {port} as 'slave")
        else:
            print("Connecting to port {port} ...")
            main(port=args.port, host="localhost")
        
    else:
        print("Connecting to port 6379 ...")
        main(host="localhost", port=6379)