import socket  # noqa: F401
import threading

def handle_command(client: socket.socket):
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

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.

    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_address = ("localhost", 6379)
    server_socket = socket.create_server(server_address, reuse_port=True)


    while True:
        client_conn, client_addr= server_socket.accept() # wait for client
        threading.Thread(target=handle_command,args = (client_conn,)).start()

if __name__ == "__main__":
    main()
