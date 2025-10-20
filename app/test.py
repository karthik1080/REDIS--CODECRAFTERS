def xrange_cmd(store, stream_key, start_id, end_id):
    if stream_key not in store or not isinstance(store[stream_key], list):
        return "*0\r\n"  # Empty array

    # Normalize start and end IDs
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

    # Build RESP array response
    response = f"*{len(result_entries)}\r\n"
    for entry in result_entries:
        response += "*2\r\n"
        # Entry ID
        response += f"${len(entry['id'])}\r\n{entry['id']}\r\n"
        # Fields
        fields = [k for k in entry.keys() if k != "id"]
        response += f"*{len(fields) * 2}\r\n"
        for field in fields:
            value = entry[field]
            response += f"${len(field)}\r\n{field}\r\n"
            response += f"${len(value)}\r\n{value}\r\n"
    return response
