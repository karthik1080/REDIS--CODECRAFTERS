import socket
import threading
from .redis_list import rpush, lpush, lpop, llen, lrange, blpop
from .redis_streams import get_type, xadd, xrange_cmd, xread, xread_blocking
import time
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
                fields = [lines[i] for i in range(8, len(lines),2)]
                response = xadd(store, key, entry_id, fields).encode()
            elif command == "XRANGE":
                stream_key = lines[4]
                start_id = lines[6]
                end_id = lines[8]
                response = xrange_cmd(store, stream_key, start_id, end_id).encode()
            elif command == "XREAD":
                remaining = lines[5:]
                block_time_ms = None

                # Check if BLOCK is specified
                if lines[4].upper() == "BLOCK":
                    block_time_ms = int(lines[6])
                    remaining = lines[7:]
                num_streams = len(remaining) // 4
                stream_keys = remaining[1:num_streams*2:2]
                entry_ids = remaining[num_streams*2+1::2]

                if block_time_ms is not None:
                    response = xread_blocking(store, stream_keys, entry_ids, block_time_ms)
                else:
                    resp_list = []
                    for stream_key, entry_id in zip(stream_keys, entry_ids):
                        resp_list.append(xread(store, stream_key, entry_id))
                    if not resp_list:
                        response = "*0\r\n"  # null array
                    else:
                        response = f"*{len(resp_list)}\r\n" + "".join(resp_list)
                response = response.encode()


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


# redis_streams.py
from typing import Dict, List
import time
import threading
def get_type(store: dict, key: str) -> str:

    if key not in store:
        return "+none\r\n"

    value = store[key]

    if isinstance(value, str):
        return "+string\r\n"
    elif isinstance(value, list):
        # Could be a list or stream, check structure
        if len(value) > 0 and isinstance(value[0], dict) and "id" in value[0]:
            return "+stream\r\n"
        else:
            return "+list\r\n"
    elif isinstance(value, set):
        return "+set\r\n"
    elif isinstance(value, dict):
        return "+hash\r\n"
    elif isinstance(value, tuple):
        return "+zset\r\n"
    else:
        return "+unknown\r\n"

def xadd(store: Dict, stream_key: str, entry_id: str, fields: List[str]) -> str:
    """
    Add an entry to a Redis stream.
    fields: list like [field1, value1, field2, value2, ...]
    Returns the entry ID as a RESP bulk string.
    """
    if stream_key not in store:
        store[stream_key] = []

    if not isinstance(store[stream_key], list):
        return "-ERR wrong type\r\n"

    # Handle full auto ID: "*"
    if entry_id == "*":
        ms_time = int(time.time() * 1000)  # current unix time in ms

        if not store[stream_key]:
            # New stream, first entry
            entry_id = f"{ms_time}-0"
        else:
            last_entry_id = store[stream_key][-1]['id']
            last_ms, last_seq = map(int, last_entry_id.split('-'))

            if last_ms == ms_time:
                entry_id = f"{ms_time}-{last_seq + 1}"
            else:
                entry_id = f"{ms_time}-0"

    # Handle "time-*"
    elif "-" in entry_id and entry_id.endswith("*"):
        time_part = int(entry_id.split("-")[0])

        if store[stream_key]:
            last_entry_id = store[stream_key][-1]['id']
            last_ms, last_seq = map(int, last_entry_id.split('-'))
            if last_ms == time_part:
                seq = last_seq + 1
            else:
                seq = 0 if time_part != 0 else 1
        else:
            seq = 0 if time_part != 0 else 1

        entry_id = f"{time_part}-{seq}"

    # Handle invalid 0-0 case
    elif entry_id == "0-0":
        return "-ERR The ID specified in XADD must be greater than 0-0\r\n"

    # Validate ID ordering
    if store[stream_key]:
        last_entry_id = store[stream_key][-1]['id']
        if not is_valid_id(entry_id, last_entry_id):
            return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"

    # Store fields
    entry = {"id": entry_id}
    for i in range(0, len(fields), 2):
        field_key = fields[i]
        field_val = fields[i + 1] if i + 1 < len(fields) else ""
        entry[field_key] = field_val

    store[stream_key].append(entry)
    with blocked_lock:
        if stream_key in blocked_streams:
            for event in blocked_streams[stream_key]:
                event.set()  # wake them
            blocked_streams[stream_key] = []

    return f"${len(entry_id)}\r\n{entry_id}\r\n"


def is_valid_id(new_id: str, last_id: str) -> bool:

    new_ms, new_seq = map(int, new_id.split('-'))
    last_ms, last_seq = map(int, last_id.split('-'))

    if new_ms > last_ms:
        return True
    if new_ms == last_ms and new_seq > last_seq:
        return True
    return False

def xrange_cmd(store, stream_key, start_id, end_id):

    if stream_key not in store or not isinstance(store[stream_key], list):
        return "*0\r\n"
    if start_id == "-":
        start_id = "0-0"
    if end_id == "+":
        end_id = f"{store[stream_key][-1]['id']}"
    if "-" not in start_id:
        start_id = f"{start_id}-0"
    if "-" not in end_id:
        end_id = f"{end_id}-9999999999999"  # big seq for max

    start_ms, start_seq = map(int, start_id.split('-'))
    end_ms, end_seq = map(int, end_id.split('-'))

    result_entries = []
    for entry in store[stream_key]:
        ms, seq = map(int, entry["id"].split('-'))
        if (ms > start_ms or (ms == start_ms and seq >= start_seq)) and \
           (ms < end_ms or (ms == end_ms and seq <= end_seq)):
            result_entries.append(entry)

    response = f"*{len(result_entries)}\r\n"
    for entry in result_entries:
        response += "*2\r\n"
        response += f"${len(entry['id'])}\r\n{entry['id']}\r\n"
        fields = [k for k in entry.keys() if k != "id"]
        response += f"*{len(fields) * 2}\r\n"
        for field in fields:
            value = entry[field]
            response += f"${len(field)}\r\n{field}\r\n"
            response += f"${len(value)}\r\n{value}\r\n"
    return response

def xread(store: Dict, stream_key: str, entry_id: str) -> str:
    if stream_key not in store:
        return "*0\r\n"

    if not isinstance(store[stream_key], list):
        return "-ERR wrong type\r\n"
    entries = []
    for e in store[stream_key]:
        if is_valid_id(e['id'], entry_id):
            entries.append(e)
    resp = ["*2\r\n"]
    resp.append(f"${len(stream_key)}\r\n{stream_key}\r\n")

    resp.append(f"*{len(entries)}\r\n")
    for entry in entries:
        resp.append(f"*2\r\n")
        resp.append(f"${len(entry['id'])}\r\n{entry['id']}\r\n")
        fields = [f for k,v in entry.items() if k != "id" for f in (k,v)]
        resp.append(f"*{len(fields)}\r\n")
        for f in fields:
            resp.append(f"${len(str(f))}\r\n{f}\r\n")

    return "".join(resp)# concats all the resp strings which are in the form of list into 1 proper string

blocked_streams = {}
blocked_lock = threading.Lock()

def xread_blocking(store: dict, stream_keys: list, entry_ids: list, timeout_ms: int) -> str:
    """
    Blocking XREAD: waits until new entries are available or timeout expires.
    stream_keys: list of stream keys
    entry_ids: corresponding last IDs read for each stream
    timeout_ms: maximum time to wait in milliseconds
    """
    start_time = time.time()
    timeout_sec = timeout_ms / 1000 if timeout_ms else 0

    while True:
        resp_list = []
        any_new_entries = False
        for stream_key, entry_id in zip(stream_keys, entry_ids):
            entries = []
            if stream_key in store and isinstance(store[stream_key], list):
                for e in store[stream_key]:
                    if is_valid_id(e['id'], entry_id):
                        entries.append(e)
            if entries:
                any_new_entries = True
            resp_list.append(xread(store, stream_key, entry_id))

        if any_new_entries:
            # At least one new entry; return
            response = f"*{len(resp_list)}\r\n" + "".join(resp_list)
            return response

        # No entries yet: block
        if timeout_sec == 0:
            # Wait indefinitely
            event = threading.Event()
            with blocked_lock:
                for stream_key in stream_keys:
                    blocked_streams.setdefault(stream_key, []).append(event)
            event.wait()
        else:
            elapsed = time.time() - start_time
            remaining = timeout_sec - elapsed
            if remaining <= 0:
                return "*0\r\n"  # timeout expired
            # Wait a small interval to poll again
            time.sleep(min(0.05, remaining))
