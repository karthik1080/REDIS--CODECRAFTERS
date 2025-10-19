def rpush(li: list , value: str) -> str:
    li.append(value)
    return f":{len(li)}\r\n"


def lrange(li:list, start:int,stop:int)->list:
    """Return a sublist in RESP format"""
    start = int(start)
    stop = int(stop)
    print(li)
    sublist = li[start:stop+1]  # +1 because Python slicing excludes end
    resp = f"*{len(sublist)}\r\n"
    for item in sublist:
        resp += f"${len(item)}\r\n{item}\r\n"
    return resp
