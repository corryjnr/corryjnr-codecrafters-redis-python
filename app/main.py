import socket
import os
import time
import sys
import argparse
from threading import Thread
from . import parser as ps
from .pipeline import buffer_requests
import logging
logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s:%(lineno)d: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("main")
logging.getLogger("chardet.charsetprober").disabled = True

strings_store = dict()
to_propagate = dict()
replconf_ack_offset = 0
ack_offset_count = False
fullresync_count = 0


def parse_request(request, connection):
    global ack_offset_count
    global replconf_ack_offset
    logger.info("Request: %s", request)
    # logger.info("Request decoded: %s", request.decode())
    parsed_requests = []
    if request:
        parser = ps.Parser(request)
        if "*" in parser.request_type:
            parsed_requests = parser.bulk_array()
            logger.info("Parsed requests: %s", parsed_requests)
        elif "$" in parser.request_type:
            parsed_requests = parser.bulk_string()
    global replicas

    for parsed_request in parsed_requests:
        if len(parsed_request) <= 0:
            continue
        command = parsed_request[0]
        info = ' '.join(parsed_request)
        # print(f"Request: {' '.join(parsed_request)}")
        logger.info("Received request: %s", info)
        arg1 = parsed_request[1] if len(parsed_request) >= 2 else None
        arg2 = parsed_request[2] if len(parsed_request) >= 3 else None
        arg3 = parsed_request[3] if len(parsed_request) >= 4 else None
        arg4 = parsed_request[4] if len(parsed_request) >= 5 else None
        # print(f"{command}: {arg1}, {arg2}")

        # Handle received Commands
        if command == 'PING':
            if server_role == "master":
                response = b'+PONG\r\n'
                connection.sendall(response)
        if command == 'WAIT':
            if server_role == "master":
                response = f':{len(replicas)}\r\n'
                connection.sendall(response.encode())
        if command == 'REPLCONF':
            if arg1 == 'GETACK':
                response = f'*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(replconf_ack_offset))}\r\n{replconf_ack_offset}\r\n'
                connection.sendall(response.encode())
                if ack_offset_count == False:
                    ack_offset_count = True
                replconf_ack_offset += len(request)
            elif arg1 == 'listening-port':
                response = b'+OK\r\n'
                connection.sendall(response)
            elif arg1 == 'capa':
                response = b'+OK\r\n'
                connection.sendall(response)

        if server_role == "slave" and ack_offset_count == True and arg1 != 'GETACK':
            replconf_ack_offset += len(request)

        if command == 'PSYNC':
            # Response by master
            replicas.append(connection)
            rep_id = master_replid
            rep_offset = master_repl_offset
            response = b'+FULLRESYNC' + b" " + rep_id.encode() + b" " + \
                str(rep_offset).encode() + b'\r\n'
            connection.send(response)
            fullresync_command(connection=connection)
            logger.info("Handshake successful")
            # return b''
        if command == 'ECHO':
            response = b'+' + arg1.encode() + b'\r\n'
            connection.sendall(response)
        if command == 'SET':
            key = arg1
            value = arg2
            r = f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
            # print(r.encode())
            if 'px' in parsed_request or 'PX' in parsed_request:
                expiry_time = int(arg4)
                print(f"Set expiry: {expiry_time}")
                s = set_command(key, (value, time.time() + expiry_time / 1000))
                if s != None:
                    connection.sendall(s)
            else:
                for replica in replicas:
                    try:
                        replica.send(r.encode())
                    except:
                        print("send failed")
                        to_propagate[key] = value
                s = set_command(key, (value, None))
                if s != None:
                    connection.sendall(s)
        if command == 'GET':
            g = get_command(arg1)
            connection.sendall(g)
        if command == 'INFO':
            argument = arg1
            response = info_command(argument)
            connection.sendall(response)


def info_command(argument):
    if argument == "replication":
        global server_role
        role = server_role
        print(role)
        if role == 'master':
            rep_id = master_replid
            rep_offset = master_repl_offset
            response = b'+role:master\n' + b"master_replid:" + rep_id.encode() + b'\n' + \
                b"master_repl_offset:" + str(rep_offset).encode() + b'\r\n'
            logger.info(response)
            return response
        elif role == 'slave':
            return b'+role:slave\r\n'


def set_command(key, value):
    strings_store[key] = value
    logger.info(strings_store)
    if server_role == "slave":
        pass
        # return b''
    else:
        return b'+OK\r\n'


def get_command(key):
    value, expiry = strings_store.get(key, (None, None))
    if value == None or (expiry and time.time() > expiry):
        return b"$-1\r\n"
    else:
        return b'+' + value.encode() + b'\r\n'


def fullresync_command(connection):
    empty_rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
    rdb = bytes.fromhex(empty_rdb_hex)
    # empty_rdb = empty_rdb.encode()
    rdb_file_response = f"${len(rdb)}\r\n"
    connection.send(rdb_file_response.encode())
    try:
        connection.send(rdb)
    except:
        logger.info("Error sending the empty rdb file to the client")
    else:
        logger.info("Sent the empty rdb file to the client")


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
    # buff = []
    connection.settimeout(5)

    online = True
    while online:
        try:
            request = connection.recv(1024)
            # buff.append(request)
            # if buff:
            if request:
                r = buffer_requests(request)
                for i in r:
                    response = parse_request(i, connection)
                # for i in buff:
                # print(buff)
                # req = buff.pop(buff.index(i))
                # if req != None:
                # response = parse_request(req, connection)
        except socket.timeout:
            online = False
        except Exception as e:
            logger.exception("Error handling: %s", e)
            # print(f"Error handling: {e}")

    connection.close()

# Initiate connection with master


def handshake(s_port, host="localhost", port=6379):
    logger.info("Sending handshake to %s:%d", host, port)
    response1 = "*1\r\n$4\r\nping\r\n"
    response2 = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + \
        str(s_port) + "\r\n"
    response3 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
    response4 = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
    s = socket.create_connection((host, port))
    s.send(response1.encode())
    request1 = s.recv(1024).decode()
    if "PONG" in request1:
        logger.info("Handshake started")
        s.send(response2.encode())
    request2 = s.recv(1024).decode()
    if "OK" in request2:
        s.send(response3.encode())
    request3 = s.recv(1024).decode()
    if "OK" in request3:
        s.send(response4.encode())

    r1 = s.recv(1024)
    r2 = s.recv(1024)
    new = r1 + r2
    req = new[149:]
    if len(new) > 149:
        logger.info(new[149:])
        try:
            parse_request(req, s)
        except Exception as e:
            logger.exception("Error parsing: %s", e)
    # logger.info(r1)
    # logger.info(r2)
    # new_reqs = b'\r\n'.join([r1, r2])
    # reqs = new_reqs.split(b'\r\n')
    # logger.info(reqs)
    # if len(reqs) > 4:
    #    r = b'\r\n'.join(r)
    #    r = reqs[4:]
    #    logger.info(r)
    #    try:
    #        parse_request(bytes(r), s)
    #    except Exception as e:
    #        logger.exception("Error parsing: %s", e)
    handle_request(s)


def main(host, port, role="master", m_host=None, m_port=None):
    global server_role
    server_role = role
    server_port = port

    if server_role == "master":
        global master_replid
        global master_repl_offset
        master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        master_repl_offset = 0
    global replicas
    replicas = list()

    logger.info("Logs from your program will appear here!")

    # Create server socket
    server_socket = socket.create_server((host, port), reuse_port=True)

    if server_role == "slave":
        handshake_thread = Thread(
            target=handshake, args=(server_port, m_host, m_port))
        handshake_thread.start()
        # master_connection.send(response)

    # You can use print statements as follows for debugging, they'll be visible when running tests.
    # Infinite loop, server waiting for connections and accepting
    while True:
        connection, address = server_socket.accept()  # wait for client
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
            logger.info("Connecting to port %d as 'slave'", port)
            main(host="localhost", port=port, role="slave",
                 m_host=master_host, m_port=int(master_port))
        else:
            logger.info("Connecting to port %d ...", port)
            main(port=args.port, host="localhost")

    else:
        logger.info("Connecting to port 6379 ...")
        main(host="localhost", port=6379)
