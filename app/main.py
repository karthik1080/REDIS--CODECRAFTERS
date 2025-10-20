# main.py
import socket
import threading
import time
from .redis_list import rpush, lpush, lpop, llen, lrange

# Registry of blocked clients waiting on keys
blocked = {}            # key -> list of waiter dicts [{'cond': Condition, 'result': None, 'served': False}]
blocked_lock = threading.Lock()


def wake_waiters_for_key(store: dict, key: str):
    """
    Called when RPUSH adds elements to 'key'. Wake blocked waiters FIFO,
    delivering one list element per waiter (if available).
    """
    with blocked_lock:
        waiters = blocked.get(key)
        if not waiters:
            return

        # While there are waiters and elements available, serve them FIFO
        while waiters and store.get(key) and len(store[key]) > 0:
            waiter = waiters.pop(0)
            # pop leftmost element for the waiter
            value = store[key].pop(0)
            with waiter['cond']:
                waiter['result'] = value
                waiter['served'] = True
                waiter['cond'].notify()

        # clean up empty list entry
        if not waiters:
            blocked.pop(key, None)


def handle_command(client: socket.socket, store: dict):
    try:
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
                    values = []
                    for i in range(6, 6 + (num_args - 2) * 2, 2):
                        values.append(lines[i])
                    # push values
                    rpush(store, key, values)
                    # after pushing, wake blocked clients (if any)
                    wake_waiters_for_key(store, key)
                    response = f":{len(store[key])}\r\n".encode()

                elif command == "LPUSH":
                    key = lines[4]
                    num_args = int(lines[0][1:])
                    values = []
                    for i in range(6, 6 + (num_args - 2) * 2, 2):
                        values.append(lines[i])
                    response = lpush(store, key, values).encode()

                elif command == "LPOP":
                    key = lines[4]
                    if len(lines) > 6:
                        count = int(lines[6])
                    else:
                        count = 1
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
                    # parse key and timeout
                    key = lines[4]
                    timeout = float(lines[6])  # seconds; 0 means wait indefinitely

                    # If list has element now -> return immediately
                    if key in store and isinstance(store[key], list) and len(store[key]) > 0:
                        val = store[key].pop(0)
                        resp = f"*2\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n"
                        response = resp.encode()
                    else:
                        # Need to block the client. Create waiter and wait on condition.
                        waiter = {'cond': threading.Condition(), 'result': None, 'served': False}
                        with blocked_lock:
                            blocked.setdefault(key, []).append(waiter)

                        # Wait on the condition (release lock inside wait)
                        cond = waiter['cond']
                        with cond:
                            if timeout == 0:
                                # wait indefinitely until notified
                                cond.wait()
                            else:
                                cond.wait(timeout=timeout)

                        # After waking up, check if served (RPUSH notified us)
                        if waiter['served'] and waiter['result'] is not None:
                            val = waiter['result']
                            resp = f"*2\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n"
                            response = resp.encode()
                        else:
                            # Timeout or spurious wake — we must remove this waiter from queue if still present
                            with blocked_lock:
                                waiters = blocked.get(key, [])
                                # remove this waiter if it is still there
                                try:
                                    waiters.remove(waiter)
                                except ValueError:
                                    pass
                                if not waiters:
                                    blocked.pop(key, None)
                            # timeout -> null array
                            response = b"*-1\r\n"

                else:
                    response = b"-ERR unknown command\r\n"

                client.send(response)

            else:
                client.send(b"-ERR invalid request\r\n")
    finally:
        try:
            client.close()
        except Exception:
            pass


def main():
    print("Redis server starting...")

    server_address = ("localhost", 6379)
    server_socket = socket.create_server(server_address, reuse_port=True)

    store = {}  # shared store for strings and lists

    while True:
        client_conn, _ = server_socket.accept()
        threading.Thread(target=handle_command, args=(client_conn, store), daemon=True).start()


if __name__ == "__main__":
    main()
