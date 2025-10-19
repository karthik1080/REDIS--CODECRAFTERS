import socket  # noqa: F401
import threading

def handle_command(client: socket.socket):
    while chunk := client.recv(4096):
        if chunk == b"":
            break
        print(f"[CHUNK] ```\n{chunk.decode()}\n```")
        client.sendall(b"+PONG\r\n")

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
