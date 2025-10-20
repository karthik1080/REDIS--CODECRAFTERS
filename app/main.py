import socket
import threading
from .redis_list import rpush, lpush, lpop, llen, lrange, blpop
from .redis_streams import get_type, xadd, xrange_cmd

def handle_command(client: socket.socket, store: dict):
    while True:
        request = client.recv(4096)
        if not request:
            break

        data = request.decode().strip()
        if not data:
            continue
        if data.startswith("*"):
            lines = data.split("\r\n")
            command = lines[2].upper()
            print(lines)
            # ---------------------- STRING COMMANDS ----------------------
            if command == "PING":
                response = b"+PONG\r\n"
            elif command == "ECHO":
                message = lines[4]
                response = f"${len(message)}\r\n{message}\r\n".encode()
            elif command == "SET":
                key = lines[4]
                value = lines[6]
                if len(lines) > 8 and lines[8].lower() == "px":
                    ttl = int(lines[10])
                    threading.Timer(ttl / 1000, store.pop, args=[key]).start()
                store[key] = value
                response = b"+OK\r\n"
            elif command == "GET":
                key = lines[4]
                value = store.get(key, None)
                if value is not None:
                    response = f"${len(value)}\r\n{value}\r\n".encode()
                else:
                    response = b"$-1\r\n"

            # ---------------------- LIST COMMANDS ----------------------
            elif command == "RPUSH":
                key = lines[4]
                num_args = int(lines[0][1:])
                values = [lines[i] for i in range(6, 6 + (num_args - 2) * 2, 2)]
                response = rpush(store, key, values).encode()

            elif command == "LPUSH":
                key = lines[4]
                num_args = int(lines[0][1:])
                values = [lines[i] for i in range(6, 6 + (num_args - 2) * 2, 2)]
                response = lpush(store, key, values).encode()

            elif command == "LPOP":
                key = lines[4]
                count = int(lines[6]) if len(lines) > 6 else 1
                response = lpop(store, key, count).encode()

            elif command == "LLEN":
                key = lines[4]
                response = llen(store, key).encode()

            elif command == "LRANGE":
                key = lines[4]
                start = int(lines[6])
                stop = int(lines[8])
                response = lrange(store, key, start, stop).encode()

            elif command == "BLPOP":
                key = lines[4]
                timeout = float(lines[6])
                response = blpop(store, key, timeout).encode()
            # ---------------------- STREAM COMMANDS ----------------------
            elif command == "TYPE":
                key = lines[4]
                response = get_type(store, key).encode()
            elif command == "XADD":
                key = lines[4]
                entry_id = lines[6]
                fields = [lines[i] for i in range(8, len(lines),2)]
                response = xadd(store, key, entry_id, fields).encode()
            elif command == "XRANGE":
                stream_key = lines[4]
                start_id = lines[6]
                end_id = lines[8]
                response = xrange_cmd(store, stream_key, start_id, end_id).encode()

            else:
                response = b"-ERR unknown command\r\n"

            client.send(response)
        else:
            client.send(b"-ERR invalid request\r\n")


def main():
    print("Redis server starting...")

    server_address = ("localhost", 6379)
    server_socket = socket.create_server(server_address, reuse_port=True)

    store = {}

    while True:
        client_conn, _ = server_socket.accept()
        threading.Thread(target=handle_command, args=(client_conn, store), daemon=True).start()


if __name__ == "__main__":
    main()
