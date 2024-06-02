import socket
import os
import time
import sys
import argparse
import logging
from threading import Thread

logging.basicConfig(level=logging.INFO)

class RedisServer:
    def __init__(self, host='localhost', port=6379, role='master', master_host=None, master_port=None):
        self.host = host
        self.port = port
        self.role = role
        self.master_host = master_host
        self.master_port = master_port
        self.strings_store = {}
        self.to_propagate = {}
        self.replicas = []
        self.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.master_repl_offset = 0
        
        if self.role == 'slave':
            self.handshake()

    def start_server(self):
        logging.info(f"Starting Redis server on {self.host}:{self.port} as {self.role}")
        server_socket = socket.create_server((self.host, self.port), reuse_port=True)
        while True:
            connection, address = server_socket.accept()
            client_thread = Thread(target=self.handle_request, args=(connection,))
            client_thread.start()

    def handle_request(self, connection):
        try:
            while True:
                request = connection.recv(1024)
                if not request:
                    break
                response = self.parse_request(request, connection)
                if response:
                    connection.sendall(response)
        except Exception as e:
            logging.error(f"Error handling request: {e}")
        finally:
            connection.close()

    def parse_request(self, request, connection):
        try:
            requests = list(request.decode().split())
            logging.info(f"Request received: {requests}")
            command = requests[0].upper()
            if command == 'PING':
                return self.handle_ping()
            elif command == 'REPLCONF':
                return self.handle_replconf()
            elif command == 'PSYNC':
                return self.handle_psync(connection)
            elif command == 'ECHO':
                return self.handle_echo(requests)
            elif command == 'SET':
                return self.handle_set(requests)
            elif command == 'GET':
                return self.handle_get(requests)
            elif command == 'INFO':
                return self.handle_info(requests)
            else:
                return self.handle_unknown()
        except Exception as e:
            logging.error(f"Error parsing request: {e}")
            return b'-ERR unknown error\r\n'

    def handle_ping(self):
        return b'+PONG\r\n'

    def handle_replconf(self):
        return b'+OK\r\n'

    def handle_psync(self, connection):
        self.replicas.append(connection)
        response = f"+FULLRESYNC {self.master_replid} {self.master_repl_offset}\r\n"
        connection.send(response.encode())
        self.fullresync_command(connection)
        return b''

    def handle_echo(self, requests):
        return f"+{requests[1]}\r\n".encode()

    def handle_set(self, requests):
        key = requests[1]
        value = requests[2]
        expiry_time = int(requests[4]) if 'px' in requests or 'PX' in requests else None
        return self.set_command(key, value, expiry_time)

    def handle_get(self, requests):
        key = requests[1]
        return self.get_command(key)

    def handle_info(self, requests):
        if len(requests) > 1:
            argument = requests[1]
            return self.info_command(argument)
        return b'-ERR wrong number of arguments for INFO\r\n'

    def handle_unknown(self):
        return b'-ERR unknown command\r\n'

    def set_command(self, key, value, expiry_time):
        self.strings_store[key] = (value, time.time() + expiry_time / 1000 if expiry_time else None)
        logging.info(f"Set key: {key} with value: {value}")
        return b'+OK\r\n'

    def get_command(self, key):
        value, expiry = self.strings_store.get(key, (None, None))
        if value is None or (expiry and time.time() > expiry):
            return b"$-1\r\n"
        return f"+{value}\r\n".encode()

    def info_command(self, argument):
        if argument == "replication":
            if self.role == 'master':
                response = f"+role:master\r\nmaster_replid:{self.master_replid}\r\nmaster_repl_offset:{self.master_repl_offset}\r\n"
                return response.encode()
            elif self.role == 'slave':
                return b'+role:slave\r\n'
        return b'-ERR unknown INFO argument\r\n'

    def fullresync_command(self, connection):
        empty_rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        rdb = bytes.fromhex(empty_rdb_hex)
        rdb_file_response = f"${len(rdb)}\r\n"
        connection.send(rdb_file_response.encode())
        try:
            connection.send(rdb)
        except Exception as e:
            logging.error(f"Error sending RDB file: {e}")
        else:
            logging.info("Sent the empty RDB file to the client")

    def handshake(self):
        logging.info(f"Sending handshake to {self.master_host}:{self.master_port}")
        responses = [
            "*1\r\n$4\r\nPING\r\n",
            f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{self.port}\r\n",
            "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",
            "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
        ]
        s = socket.create_connection((self.master_host, self.master_port))
        for response in responses:
            s.send(response.encode())
            reply = s.recv(1024).decode()
            logging.info(f"Handshake response: {reply}")
            if "PONG" in reply or "OK" in reply:
                continue
            else:
                logging.error("Handshake failed")
                break

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", help="Specify port to connect", type=int, default=6379)
    parser.add_argument("--role", help="Specify server role: master or slave", type=str, default="master")
    parser.add_argument("--master_host", help="Specify master host for replication", type=str)
    parser.add_argument("--master_port", help="Specify master port for replication", type=int)
    args = parser.parse_args()

    server = RedisServer(
        host="localhost", 
        port=args.port, 
        role=args.role, 
        master_host=args.master_host, 
        master_port=args.master_port
    )
    server.start_server()
