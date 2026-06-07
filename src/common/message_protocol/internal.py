import json
import zlib


def serialize(message):
    rows = message.get("rows")

    if rows and isinstance(rows, list):
        keys = tuple(rows[0].keys())
        columnar = {k: [row.get(k) for row in rows] for k in keys}

        msg_to_send = {k: v for k, v in message.items() if k != "rows"}
        msg_to_send["rows"] = columnar
        msg_to_send["_is_columnar"] = True
    else:
        msg_to_send = message
    json_str = json.dumps(msg_to_send, separators=(',', ':'))    
    return zlib.compress(json_str.encode("utf-8"), level=3)


def deserialize(payload):
    json_str = zlib.decompress(payload).decode("utf-8")
    msg = json.loads(json_str)
    if msg.get("_is_columnar") and "rows" in msg:
        columnar = msg["rows"]
        keys = tuple(columnar.keys())
        msg["rows"] = [
            dict(zip(keys, row_values)) 
            for row_values in zip(*columnar.values())
        ]
        del msg["_is_columnar"]
    return msg