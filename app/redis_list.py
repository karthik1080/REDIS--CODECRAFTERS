def rpush(li: list , value: str) -> str:
    li.append(value)
    return f":{len(li)}\r\n"


def lrange(li:list, start:int,stop:int)->list:
    return li[start:stop:1]
