from asyncio import IncompleteReadError

from . import external_serializer


class MsgType:
    TRANSACTION = 1
    ACCOUNT = 2
    ACK = 3
    END_OF_RECODS = 4


# Receiving
def _recv_sized(socket, size):
    """
    Receives exactly 'num_bytes' bytes through the provided socket.
    If no bytes are read from the socket IncompleteReadError is raised
    """
    buf = bytearray(size)
    pos = 0
    while pos < size:
        n = socket.recv_into(memoryview(buf)[pos:])
        if n == 0:
            raise IncompleteReadError(bytes(buf[:pos]), size)
        pos += n
    return bytes(buf)


def _recv_fruit_record(socket):
    fruit_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    fruit = external_serializer.deserialize_string(_recv_sized(socket, fruit_size))
    amount = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    return (fruit, amount)


def _recv_fruit_top(socket):
    fruit_top_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    fruit_top = []
    for i in range(fruit_top_size):
        fruit_record = _recv_fruit_record(socket)
        fruit_top.append(fruit_record)
    return fruit_top


def _recv_empty(socket):
    return None


RECV_MSG_HANDLERS = {
    MsgType.TRANSACTION: _recv_fruit_record,
    MsgType.ACCOUNT: _recv_fruit_top,
    MsgType.ACK: _recv_empty,
    MsgType.END_OF_RECODS: _recv_empty,
}


def recv_msg(socket):
    msg_type = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    msg_handler = RECV_MSG_HANDLERS[msg_type]
    return (msg_type, msg_handler(socket))


# Sending

## Transactions
def _serialize_transaction(timestamp, origin_bank, origin_acc,
                      dest_bank , dest_acc, amount_received, receiving_currency,
                      amount_paid, payment_currency, payment_fmt):
    return b"".join(
        [
            external_serializer.serialize_uint8(len(timestamp)),
            external_serializer.serialize_string(timestamp),
            external_serializer.serialize_uint8(len(origin_bank)),
            external_serializer.serialize_string(origin_bank),
            external_serializer.serialize_uint8(len(origin_acc)),
            external_serializer.serialize_string(origin_acc),
            external_serializer.serialize_uint8(len(dest_bank)),
            external_serializer.serialize_string(dest_bank),
            external_serializer.serialize_uint8(len(dest_acc)),
            external_serializer.serialize_string(dest_acc),
            external_serializer.serialize_uint8(len(amount_received)),
            external_serializer.serialize_string(receiving_currency),
            external_serializer.serialize_uint8(len(amount_paid)),
            external_serializer.serialize_string(payment_currency),
            external_serializer.serialize_uint8(len(payment_fmt)),
            external_serializer.serialize_string(payment_fmt),
        ]
    )


def _send_transaction(socket, timestamp, origin_bank, origin_acc,
                      dest_bank , dest_acc, amount_received, receiving_currency,
                      amount_paid, payment_currency, payment_fmt):

    msg = external_serializer.serialize_uint32(MsgType.FRUIT_RECORD)
    msg += _serialize_transaction(timestamp, origin_bank, origin_acc,
                      dest_bank , dest_acc, amount_received, receiving_currency,
                      amount_paid, payment_currency, payment_fmt)

    socket.sendall(msg)


## Accounts
def _send_account(socket, bank_name, bank_id, acc_number, entity_id, entity_name):
    pass


## ACK
def _send_ack(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.ACK))


# EOF
def _send_end_of_records(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.END_OF_RECODS))


SEND_MSG_HANDLERS = {
    MsgType.TRANSACTION: _send_transaction,
    MsgType.ACCOUNT: _send_account,
    MsgType.ACK: _send_ack,
    MsgType.END_OF_RECODS: _send_end_of_records,
}


def send_msg(socket, msg_type, *args):
    msg_handler = SEND_MSG_HANDLERS[msg_type]
    msg_handler(socket, *args)
