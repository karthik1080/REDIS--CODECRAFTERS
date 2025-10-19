import socket  # noqa: F401
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
