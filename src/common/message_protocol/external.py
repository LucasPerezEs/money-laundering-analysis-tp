import datetime
import enum

from asyncio import IncompleteReadError

from . import external_serializer


class MsgType(enum.Enum):
    TRANSACTION_BATCH = enum.auto()     # Transactions batch
    ACCOUNT_BATCH = enum.auto()         # Accounts batch
    ACK = enum.auto()                   # ACk of batch
    WAIT = enum.auto()                  # Wait for gateway to process batch
    CHECK = enum.auto()                 # Ask gateway if batch is processed
    END_OF_RECORDS = enum.auto()        # Signal no more records to send


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

def _deserialize_string(socket):
    str_size = external_serializer.deserialize_string(_recv_sized(socket, external_serializer.UINT8_SIZE))
    return external_serializer.deserialize_string(_recv_sized(socket, str_size))


def _recv_transactions_batch(socket):
    batch_size = external_serializer.deserialize_uint16(
        _recv_sized(socket, external_serializer.UINT16_SIZE)
    )

    # Read batch
    batch = []
    for i in range(batch_size):
        # Timestamp
        timestamp = external_serializer.deserialize_string(_recv_sized(socket, external_serializer.UINT64_SIZE))
        
        # Origin account
        origin_bank = external_serializer.deserialize_uint32(
            _recv_sized(socket, external_serializer.UINT32_SIZE)
        )
        origin_acc = _deserialize_string(socket)

        # Destination account
        dest_bank = external_serializer.deserialize_uint32(
            _recv_sized(socket, external_serializer.UINT32_SIZE)
        )
        dest_acc = _deserialize_string(socket)

        # Amount received
        amount_received = _deserialize_string(socket)

        # Received currency
        received_currency = _deserialize_string(socket)

        # Amount paid
        amount_paid = _deserialize_string(socket)

        # Payment currency
        payment_currency = _deserialize_string(socket)

        # Payment method
        payment_fmt = _deserialize_string(socket)

        batch.append([timestamp, origin_bank, origin_acc,
                        dest_bank, dest_acc, amount_received,
                        received_currency, amount_paid,
                        payment_currency, payment_fmt])

    return batch

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
    MsgType.TRANSACTION_BATCH: _recv_transactions_batch,
    MsgType.ACK: _recv_empty,
    MsgType.END_OF_RECORDS: _recv_empty,
}


def recv_msg(socket):
    msg_type = external_serializer.deserialize_uint8(
        _recv_sized(socket, external_serializer.UINT8_SIZE)
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
            external_serializer.serialize_uint64(int(datetime.strptime(timestamp, "%Y-%m-%d %H:%M")).timestamp()),
            external_serializer.serialize_uint32(int(origin_bank)),
            external_serializer.serialize_uint8(len(origin_acc)),
            external_serializer.serialize_string(origin_acc),
            external_serializer.serialize_uint32(int(dest_bank)),
            external_serializer.serialize_uint8(len(dest_acc)),
            external_serializer.serialize_string(dest_acc),
            external_serializer.serialize_uint8(len(amount_received)),
            external_serializer.serialize_string(amount_received),
            external_serializer.serialize_uint8(len(receiving_currency)),
            external_serializer.serialize_string(receiving_currency),
            external_serializer.serialize_uint8(len(amount_paid)),
            external_serializer.serialize_string(amount_paid),
            external_serializer.serialize_uint8(len(payment_currency)),
            external_serializer.serialize_string(payment_currency),
            external_serializer.serialize_uint8(len(payment_fmt)),
            external_serializer.serialize_string(payment_fmt),
        ]
    )


def _send_transactions_batch(socket, batch):
    # Send start of transaction batch
    msg = external_serializer.serialize_uint8(MsgType.TRANSACTION_BATCH)
    msg += external_serializer.serialize_uint16(len(batch))

    # Add transactions to batch
    for fields in batch:
        msg += _serialize_transaction(*fields)

    socket.sendall(msg)

## Accounts
def _send_accounts_batch(socket, batch_max_size, account_fields_gen):
    # Send start of transaction batch
    msg = external_serializer.serialize_uint8(MsgType.ACCOUNT_BATCH)
    socket.sendall(msg)

    # Add accounts to batch
    current_batch_size = 0
    for fields in account_fields_gen:
        msg += _serialize_transaction(*fields)
        current_batch_size += 1

        # If max size was reached
        if current_batch_size == batch_max_size:
            break

    # Send EOB
    msg += external_serializer.serialize_uint8(MsgType.END_OF_BATCH)


## ACK
def _send_ack(socket):
    socket.sendall(external_serializer.serialize_uint8(MsgType.ACK))


## EOF
def _send_end_of_records(socket):
    socket.sendall(external_serializer.serialize_uint8(MsgType.END_OF_RECORDS))


## Send message handlers
SEND_MSG_HANDLERS = {
    MsgType.TRANSACTION_BATCH: _send_transactions_batch,
    MsgType.ACCOUNT_BATCH: _send_accounts_batch,
    MsgType.ACK: _send_ack,
    MsgType.END_OF_RECORDS: _send_end_of_records,
}


# Send message function
def send_msg(socket, msg_type, *args):
    msg_handler = SEND_MSG_HANDLERS[msg_type]
    msg_handler(socket, *args)
