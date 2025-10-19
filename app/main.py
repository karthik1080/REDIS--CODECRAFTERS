import socket  # noqa: F401
import threading
def handle_command(client: socket.socket, data: dict):

    while chunk := client.recv(4096):
        if chunk == b"":
            break

        print(f"[CHUNK] ```\n{chunk.decode()}\n```")
        print(f"Received data: {chunk}")
        if chunk.startswith(b"*1\r\n$4\r\nPING\r\n"):
            client.sendall(b"+PONG\r\n")
        if chunk.startswith(b"*2\r\n$4\r\nECHO\r\n"):
            msg = chunk.split(b"\r\n")[-2]
            client.sendall(b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n")
        if chunk.startswith(b'*3\r\n$3\r\nSET'):
            print(chunk.split(b"\r\n"))
            key = chunk.split(b"\r\n")[-4].decode()
            val = chunk.split(b"\r\n")[-2].decode()
            data[key] = val
            client.sendall(b"+OK\r\n")
        if chunk.startswith(b'*2\r\n$3\r\nGET'):
            print(data)
            key = chunk.split(b"\r\n")[-2].decode()
            val = data[key]
            if val is None:
                client.sendall(b"$-1\r\n")  # Redis protocol for null
            else:
                client.sendall(f"${len(val)}\r\n{val}\r\n".encode())


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.

    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_address = ("localhost", 6379)
    server_socket = socket.create_server(server_address, reuse_port=True)
    data = {}

    while True:
        client_conn, client_addr= server_socket.accept() # wait for client
        threading.Thread(target=handle_command,args = (client_conn,data)).start()

if __name__ == "__main__":
    main()
