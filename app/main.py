import socket  # noqa: F401
import threading
def handle_command(client: socket.socket, store: dict):
    while True:
        request = client_socket.recv(1024)
        if not request:
            break

        data = request.decode().strip()
        if not data:
            continue

        if data.startswith("*"):
            lines = data.split("\r\n")
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
            else:
                response = b"-ERR unknown command\r\n"
            client_socket.send(response)
        else:
            client_socket.send(b"-ERR invalid request\r\n")



def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.

    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_address = ("localhost", 6379)
    server_socket = socket.create_server(server_address, reuse_port=True)
    store = {}

    while True:
        client_conn, client_addr= server_socket.accept() # wait for client
        threading.Thread(target=handle_command,args = (client_conn,data)).start()

if __name__ == "__main__":
    main()
