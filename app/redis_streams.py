# redis_type.py

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
        return "+list\r\n"
    elif isinstance(value, set):
        return "+set\r\n"
    elif isinstance(value, dict):
        # We'll assume dict can be hash, zset, stream, or vectorset depending on how you implement them later
        # For now, just call it a hash
        return "+hash\r\n"
    elif isinstance(value, tuple):
        # Example: zset or vectorset placeholder
        return "+zset\r\n"  # or +vectorset later if you store differently
    else:
        return "+unknown\r\n"
