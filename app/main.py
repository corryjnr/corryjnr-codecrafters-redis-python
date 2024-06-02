import socket
import os
import time
import sys
import argparse
from threading import Thread
strings_store = dict()
to_propagate = dict()

fullresync_count = 0
    
def parse_request(request, connection):
    requests = list(request.decode().split())
    print(request.decode())
    command = requests[2].upper() #The command used
    arguments = requests[1:]
    try:
        arg1 = requests[4] #First argument passed to command
    except:
        arg1 = None
        
    global replicas
    # Handle received Commands
    print(f"Received command: {command}")
    if command == 'PING':
        return b'+PONG\r\n'
    elif command == 'REPLCONF':
        return b'+OK\r\n'
    elif command == 'PSYNC':
        # Response by master
        replicas.append(connection)
        print(replicas)
        rep_id = master_replid
        rep_offset = master_repl_offset
        response = b'+FULLRESYNC' + b" " + rep_id.encode() + b" " + str(rep_offset).encode() + b'\r\n'
        connection.send(response)
        fullresync_command(connection=connection)
        return b''
    elif command == 'ECHO':
        return b'+' + arg1.encode() + b'\r\n'
    elif command == 'SET':
        key = arg1
        value = requests[6]
        r = f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
        if 'px' in requests or 'PX' in requests:
            expiry_time = int(requests[10])
            print(f"Set expiry: {expiry_time}")
            return set_command(key, (value, time.time() + expiry_time / 1000))
        else:
            for replica in replicas:
                try:
                    replica.send(r.encode())
                    print("sent")
                except:
                    print("send failed")
                else:
                    to_propagate[key] = value
            return set_command(key, (value, None))
    elif command == 'GET':
        return get_command(arg1)
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
    strings_store[key] = value
    print(strings_store)
    return b'+OK\r\n'

def get_command(arg1):
    value, expiry = strings_store.get(arg1, (None, None))
    if value == None or (expiry and time.time() > expiry):
        return b"$-1\r\n"
    return b'+' + value.encode() + b'\r\n'

def fullresync_command(connection):
    empty_rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
    rdb = bytes.fromhex(empty_rdb_hex)
    #empty_rdb = empty_rdb.encode()
    rdb_file_response = f"${len(rdb)}\r\n"
    connection.send(rdb_file_response.encode())
    try:
        connection.send(rdb)
    except:
        print("Error sending the empty rdb file to the client")
    else:
        print("Sent the empty rdb file to the client")

    
def replication(connection):
    print("Replication started")
    print(to_propagate)
    for key, value in to_propagate.items():
        print(f"Key:{key} Value:{value}")
        r = f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
        try:
            print(r)
            connection.send(r.encode())
            print("sent")
        except:
            print("send failed")

# Handle client requests
def handle_request(connection):
    try:
        while True:
            request = connection.recv(1024)
            if not request:
                # Break the loop if the client disconnects
                break
            response = parse_request(request, connection)
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
        global replicas
        master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        master_repl_offset = 0
        replicas = list()
        
    if server_role == "slave":
        handshake(s_port=server_port, host=m_host, port=m_port)
        
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    
    # Create server socket
    server_socket = socket.create_server((host, port), reuse_port=True)
    
    #Infinite loop, server waiting for connections and accepting
    while True:
        connection, address = server_socket.accept() # wait for client
        client_thread = Thread(target=handle_request, args=(connection,))
        client_thread.start()

if __name__ == "__main__":
    # Handle cli arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", help="specify port to connect", type=int)
    parser.add_argument("--replicaof", help="specify master", type=str)
    args = parser.parse_args()
    if args.port != None:
        port = args.port
        if args.replicaof != None:
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