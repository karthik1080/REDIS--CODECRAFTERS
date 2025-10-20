import socket  # noqa: F401
import threading
from .redis_list import (
    getter,
    setter,
    rpush,
    lrange,
    lpush,
    llen,
    lpop,
    blpop,
    type_getter_lists,
)
from .redis_streams import xadd, type_getter_streams, xrange, xread, blocks_xread

def resp_parser(data):
    if data.startswith(b"*"):
        parts = data.split(b"\r\n")
        num_elements = int(parts[0][1:])
        elements = []
        index = 1
        for _ in range(num_elements):
            length = int(parts[index][1:])
            element = parts[index + 1][:length].decode()
            print(f"Parsed element: {element}")
            elements.append(element)
            index += 2
        return elements
    return []

def resp_encoder(data):
    if data is None:
        return b"$-1\r\n"
    elif isinstance(data, list):
        out = f"*{len(data)}\r\n".encode()
        for item in data:
            out += resp_encoder(item)
        return out
    elif isinstance(data, str):
        return_string = "$"
        return_string += str(len(data)) + "\r\n"
        return_string += data + "\r\n"
        return return_string.encode()
    elif isinstance(data, int):
        return_string = ":" + str(data) + "\r\n"
        return return_string.encode()
    return b"$-1\r\n"

def simple_string_encoder(message):
    return f"+{message}\r\n".encode()

def error_encoder(message):
    return f"-{message}\r\n".encode()

blocked = {}
blocked_xread = {}


def handle_client(connection):
    with connection:
        while True:
            data = connection.recv(1024)
            if not data:
                break
            print(f"Received data: {data}")

            decoded_data = resp_parser(data)
            # PING
            if decoded_data[0] == "PING":
                response = simple_string_encoder("PONG")
                connection.sendall(response)
            # ECHO
            elif decoded_data[0].upper() == "ECHO" and len(decoded_data) > 1:
                response = resp_encoder(decoded_data[1])
                connection.sendall(response)
            # GET
            elif decoded_data[0].upper() == "GET":
                response = resp_encoder(getter(decoded_data[1]))
                print(f"GET response: {response}")
                connection.sendall(response)
            # SET
            elif decoded_data[0].upper() == "SET" and len(decoded_data) > 2:
                setter(decoded_data[1:])
                response = "+OK\r\n".encode()
                connection.sendall(response)
            # RPUSH
            elif decoded_data[0].upper() == "RPUSH" and len(decoded_data) > 2:
                # For simplicity, we treat RPUSH similar to SET in this implementation
                size = rpush(decoded_data[1:], blocked)
                response = resp_encoder(size)
                connection.sendall(response)
            # LRANGE
            elif decoded_data[0].upper() == "LRANGE" and len(decoded_data) > 3:
                response = resp_encoder(lrange(decoded_data[1:]))
                connection.sendall(response)
            # LPUSH
            elif decoded_data[0].upper() == "LPUSH":
                size = lpush(decoded_data[1:])
                response = resp_encoder(size)
                connection.sendall(response)
            # LLEN
            elif decoded_data[0].upper() == "LLEN" and len(decoded_data) > 1:
                size = llen(decoded_data[1])
                response = resp_encoder(size)
                connection.sendall(response)
            # LPOP
            elif decoded_data[0].upper() == "LPOP" and len(decoded_data) > 1:
                response = resp_encoder(lpop(decoded_data[1:]))
                connection.sendall(response)
            # BLPOP
            elif decoded_data[0].upper() == "BLPOP" and len(decoded_data) > 2:
                response = blpop(decoded_data[1:], connection, blocked)
                if response is None:
                    continue
                else:
                    response = resp_encoder(response)
                    connection.sendall(response)
            # TYPE
            elif decoded_data[0].upper() == "TYPE" and len(decoded_data) > 1:
                response = type_getter_lists(decoded_data[1])
                print(f"TYPE response: {response}")
                if response == "none":
                    response2 = simple_string_encoder(
                        type_getter_streams(decoded_data[1])
                    )
                    connection.sendall(response2)
                else:
                    response = simple_string_encoder(response)
                    connection.sendall(response)

            # XADD
            elif decoded_data[0].upper() == "XADD" and len(decoded_data) > 4:
                result = xadd(decoded_data[1:], blocked_xread)
                print(f"XADD result: {result}")
                if result[0] == "id":
                    response = resp_encoder(result[1])
                    connection.sendall(response)
                else:
                    response = error_encoder(result[1])
                    connection.sendall(response)
            # XRANGE
            elif decoded_data[0].upper() == "XRANGE" and len(decoded_data) >= 4:
                # print(f"XRANGE called with data: {decoded_data}")
                result = xrange(decoded_data[1:])
                response = resp_encoder(result)
                connection.sendall(response)
            # XREAD STREAMS
            elif decoded_data[0].upper() == "XREAD" and len(decoded_data) >= 4:
                if decoded_data[1].upper() == "BLOCK":
                    result = blocks_xread(decoded_data[2:], connection, blocked_xread)
                    if result is None:
                        continue
                    # response = resp_encoder(result)
                else:
                    result = xread(decoded_data[2:])
                    response = resp_encoder(result)
                    connection.sendall(response)
            # ERR
            else:
                response = error_encoder("ERR")
                connection.sendall(response)


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("localhost", 6379))
    server_socket.listen(10)

    while True:
        client_socket, _ = server_socket.accept()
        client_thread = threading.Thread(target=handle_client, args=(client_socket,))
        client_thread.daemon = True
        client_thread.start()


if __name__ == "__main__":
    main()
