import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.

    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_address = ("localhost", 6379)
    server_socket = socket.create_server(server_address, reuse_port=True)

    client_conn, client_addr= server_socket.accept() # wait for client
    print(f"connected to {client_addr}")

    while client_conn.recv(4096):
        client_conn.send(b"""+PONG\r\n""")

if __name__ == "__main__":
    main()
