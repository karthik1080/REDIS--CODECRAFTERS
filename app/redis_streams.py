# redis_streams.py
from typing import Dict, List
import time
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
