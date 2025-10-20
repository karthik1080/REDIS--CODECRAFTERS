# redis_streams.py
from typing import Dict, List

def get_type(store: dict, key: str) -> str:
    """
    Return the type of value stored at key.
    Supported Redis types:
        - string
        - list
        - set
        - zset
        - hash
        - stream
        - vectorset
        - none (if key doesn't exist)
    RESP simple string encoded.
    """
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
        store[stream_key] = []  # create a new stream

    if not isinstance(store[stream_key], list):
        return "-ERR wrong type\r\n"

    # Convert list of fields into a dictionary for this entry
    entry = {"id": entry_id}
    for i in range(0, len(fields), 2):
        key = fields[i]
        print(key,'done',i)
        value = fields[i + 1] if i + 1 < len(fields) else ""
        entry[key] = value
    store[stream_key].append(entry)
    print(store,'blah',entry)

    # Return the entry ID in RESP bulk string format
    return f"${len(entry_id)}\r\n{entry_id}\r\n"
