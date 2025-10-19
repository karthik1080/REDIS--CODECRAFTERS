def rpush(li: list , value: str) -> str:
    li.append(value)
    return f":{len(li)}\r\n"
