import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.

    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, x= server_socket.accept() # wait for client
    data = connection.recv(1024).decode()
    for i in range(0,len(data)):
        if i+4 < len(data) and data[i:i+4] == 'PING':
            connection.sendall(b"+PONG\r\n")

if __name__ == "__main__":
    main()
