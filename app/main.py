import socket
import threading
from .redis_list import rpush, lpush, lpop, llen, lrange, blpop
from .redis_streams import get_type,xadd

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
                fields = [lines[i] for i in range(8, len(lines),2)]  # all remaining are fields
                print(fields, 'hi', lines)
                response = xadd(store, key, entry_id, fields).encode()

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



'''import socket  # noqa: F401
import threading
from .redis_list import rpush, lrange
album cover = ''
def handle_command(client: socket.socket, store: dict,li:list):
    while True:
        request = client.recv(1024)
        if not request:
            break

        data = request.decode().strip()
        if not data:
            continue
        if data.startswith("*"):
            lines = data.split("\r\n")
            print (lines)
            command = lines[2].upper()
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
            elif command == "RPUSH":
                val = int(lines[0][1]) # it gives *4 where 4 is the number of arguments the client has and im converting it into integer
                for i in range(val-2):
                    value = lines[6+i*2]     # extract single element value
                    response = rpush(li,value).encode()
                album_cover = lines[4]
            elif command == "LRANGE":
                stop = lines[8]
                start = lines[6]
                if album_cover == lines[4]:
                    response = lrange(li,start,stop).encode()
                else :
                    response = b"$0\r\n"

            else:
                response = b"-ERR unknown command\r\n"
            client.send(response)
        else:
            client.send(b"-ERR invalid request\r\n")




def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.

    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_address = ("localhost", 6379)
    server_socket = socket.create_server(server_address, reuse_port=True)
    store = {}
    li = []
    while True:
        client_conn, client_addr= server_socket.accept() # wait for client
        threading.Thread(target=handle_command,args = (client_conn,store,li)).start()

if __name__ == "__main__":
    main()
'''
