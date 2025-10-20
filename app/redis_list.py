import time
import threading

def rpush(store: dict, key: str, values: list) -> str:
    """
    Append values to the end of the list stored at key.
    """
    if key not in store:
        store[key] = []
    if not isinstance(store[key], list):
        return "-ERR wrong type\r\n"
    for v in values:
        store[key].append(v)
    return f":{len(store[key])}\r\n"


def lpush(store: dict, key: str, values: list) -> str:
    """
    Insert values at the head of the list stored at key.
    LPUSH inserts elements from left, so last value appears first.
    Example: LPUSH key a b -> list becomes ["b", "a"]
    """
    if key not in store:
        store[key] = []
    if not isinstance(store[key], list):
        return "-ERR wrong type\r\n"
    for v in values:
        store[key].insert(0, v)
    return f":{len(store[key])}\r\n"


def lpop(store: dict, key: str, count: int = 1) -> str:
    """
    Pop elements from the start (head) of the list.
    Supports optional count like: LPOP key 2
    Returns RESP Array of popped elements.
    """
    if key not in store or not isinstance(store[key], list) or len(store[key]) == 0:
        return f"*0\r\n"

    popped = []
    if count == 1:
        value = store[key].pop(0)
        return f"${len(value)}\r\n{value}\r\n"
    for _ in range(count):
        if store[key]:
            popped.append(store[key].pop(0))
            print(popped)

    resp = f"*{len(popped)}\r\n"
    for item in popped:
        resp += f"${len(item)}\r\n{item}\r\n"
    return resp


def llen(store: dict, key: str) -> str:
    """
    Return the length of the list.
    """
    if key not in store or not isinstance(store[key], list):
        return ":0\r\n"
    return f":{len(store[key])}\r\n"


def lrange(store: dict, key: str, start: int, stop: int) -> str:
    """
    Return elements between start and stop (inclusive).
    Supports negative indexes properly.
    """
    if key not in store or not isinstance(store[key], list):
        return "*0\r\n"

    arr = store[key]
    n = len(arr)

    if start < 0:
        start = n + start
    if stop < 0:
        stop = n + stop

    start = max(start, 0)
    stop = min(stop, n - 1)

    if start > stop or start >= n:
        return "*0\r\n"

    sublist = arr[start:stop + 1]

    resp = f"*{len(sublist)}\r\n"
    for item in sublist:
        resp += f"${len(item)}\r\n{item}\r\n"
    return resp

def blpop(store, key, timeout):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if key in store and len(store[key]) > 0:
            value = store[key].pop(0)
            return f"*2\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
        time.sleep(0.05)  # sleep briefly, avoid CPU hog
    return "*-1\r\n"  # timeout: null array
