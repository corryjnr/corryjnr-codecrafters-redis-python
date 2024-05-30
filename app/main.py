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
    
    if command == 'REPLCONF':
        return b'+OK\r\n'
    
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
            rep_id = master_replid
            rep_offset = master_repl_offset
            response = b'+role:master\n' + b"master_replid:" + rep_id.encode() + b'\n' + b"master_repl_offset:" + str(rep_offset).encode() + b'\r\n'
            print(response)
            return response
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
    
# Handle client requests
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

# Initiate connection with master
def handshake(s_port, host="localhost", port=6379):
    print(f"Sending handshake to {host}:{port}")
    response1 = "*1\r\n$4\r\nping\r\n"
    response2 = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + str(s_port) + "\r\n"
    response3 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
    response4 = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
    s = socket.create_connection((host, port))
    s.send(response1.encode())
    request1 = s.recv(1024).decode()
    print(request1)
    if "PONG" in request1:
        print("Handshake successful")
        print(response2)
        s.send(response2.encode())
    request2 = s.recv(1024).decode()
    print(request2)
    if "OK" in request2:
        print(response3)
        s.send(response3.encode())
    request3 = s.recv(1024).decode()
    print(response2)
    if "OK" in request3:
        s.send(response4.encode())
    
def main(host, port, role="master", m_host=None, m_port=None):
    global server_role
    role_ = role
    server_port = port
    server_role = role_
    
    if server_role == "master":
        global master_replid
        global master_repl_offset
        master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        master_repl_offset = 0
        
    if server_role == "slave":
        handshake(s_port=server_port, host=m_host, port=m_port)
        
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    
    # Create server socket
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
    parser.add_argument("--port", help="specify port to connect", type=int)
    parser.add_argument("--replicaof", help="specify master", type=str)
    args = parser.parse_args()
    if args.port != None:
        port = args.port
        if args.replicaof != None:
            port = args.port
            arg_values = args.replicaof.split()
            master_host = arg_values[0]
            master_port = arg_values[1]
            print(f"Connecting to port {port} as 'slave'")
            main(host="localhost", port=port, role="slave", m_host=master_host, m_port=int(master_port))
        else:
            print("Connecting to port {port} ...")
            main(port=args.port, host="localhost")
        
    else:
        print("Connecting to port 6379 ...")
        main(host="localhost", port=6379)