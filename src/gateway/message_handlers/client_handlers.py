import logging
import traceback
import socket
import threading
import queue
import os
import random

from message_handlers import message_handler
from common import message_protocol
from common.middleware.middleware_sharded import ShardedExchangeProducer

def _rows_to_transactions(client_id, rows, transaction_columns):
    for row in rows:
        if len(row) != len(transaction_columns):
            logging.warning("Transaction row has unexpected length %s", len(row))
            continue
        transaction = dict(zip(transaction_columns, row))
        transaction["client_id"] = client_id
        yield transaction

def _chunks(items, size):
    chunk = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _build_output_queue(mom_host, output_queue, output_exchange, output_shards=1):
    if output_queue:
        from common import middleware
        return middleware.MessageMiddlewareQueueRabbitMQ(mom_host, output_queue)
    if output_exchange:
        return ShardedExchangeProducer(mom_host, output_exchange, output_shards)
    raise Exception("FATAL: no output given for data processing")

def _build_banks_output(mom_host, output_queue, output_exchange, output_shards=1):
    if output_queue:
        from common import middleware
        return middleware.MessageMiddlewareQueueRabbitMQ(mom_host, output_queue)
    if output_exchange:
        return ShardedExchangeProducer(mom_host, output_exchange, output_shards)
    raise Exception("FATAL: no output given for data processing")

def _normalize_bank_id(bank_id):
    normalized = str(bank_id).strip()
    return normalized or "0"

def client_dispatcher(client_id, client_socket, outbox, ack_queue, send_lock, ready_to_dispatch):
    ready_to_dispatch.wait()  
    while True:
        try:
            msg_type, payload = outbox.get()
            with send_lock:
                if msg_type == "ROWS":
                    query_id, rows = payload
                    message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.QUERY_RESULT_BATCH, client_id, query_id, rows)
                elif msg_type == "END_QUERY":
                    query_id = payload
                    message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.END_QUERY, client_id, query_id)
                elif msg_type == "END_RESULTS":
                    message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.END_RESULTS, client_id)

            ack_payload = ack_queue.get()
            if ack_payload != client_id:
                logging.error(f"Error de ACK en despachador para {client_id}")
                break

            if msg_type == "END_RESULTS":
                break
        except Exception as e:
            logging.error(f"Error enviando al cliente {client_id}: {e}")
            break

    client_socket.close()

def handle_client_request(
    client_socket, 
    client_sockets, 
    client_ready_events, 
    client_outboxes,
    mom_host, 
    output_queue, 
    output_exchange, 
    banks_out_queue, 
    banks_out_exchange,
    transaction_columns,
    client_checkpoints, 
    client_semaphores, 
    checkpoint_barriers, 
    checkpoint_lock,
    client_ack_queues, 
    client_send_locks
):
    handler = message_handler.MessageHandler()
    client_id = None
    output_shards = int(os.environ.get("OUTPUT_SHARDS", "1"))
    gateway_output_batch_size = int(os.environ.get("GATEWAY_OUTPUT_BATCH_SIZE", "2000"))
    max_in_flight_batches = int(os.environ.get("MAX_IN_FLIGHT_BATCHES", "3"))
    
    banks_output_shards = int(os.environ.get("BANKS_OUTPUT_SHARDS", "1"))
    
    output = _build_output_queue(mom_host, output_queue, output_exchange, output_shards)
    banks_output = _build_banks_output(mom_host, banks_out_queue, banks_out_exchange, banks_output_shards)

    sent_bank_ids = set()

    try:
        client_socket.setblocking(True)
        while True:
            try:
                msg_type, payload = message_protocol.external.recv_msg(client_socket)
            except Exception as e:
                client_socket.close() 
                raise e

            if msg_type == message_protocol.external.MsgType.ACK:
                if client_id is not None:
                    client_ack_queues[client_id].put(payload)
                continue

            if msg_type in (message_protocol.external.MsgType.ACCOUNTS_BATCH, message_protocol.external.MsgType.TRANSACTIONS_BATCH, message_protocol.external.MsgType.END_ACCOUNTS, message_protocol.external.MsgType.END_TRANSACTIONS):
                msg_client_id = payload[0] if isinstance(payload, tuple) else payload
                if client_id is None:
                    client_id = msg_client_id
                    client_sockets[client_id] = client_socket
                    client_ready_events[client_id] = threading.Event()
                    client_outboxes[client_id] = queue.Queue()
                    client_ack_queues[client_id] = queue.Queue()
                    client_send_locks[client_id] = threading.Lock()
                    client_semaphores[client_id] = threading.Semaphore(max_in_flight_batches)

                    t_disp = threading.Thread(
                        target=client_dispatcher,
                        args=(client_id, client_socket, client_outboxes[client_id], client_ack_queues[client_id], client_send_locks[client_id], client_ready_events[client_id])
                    )
                    t_disp.daemon = True
                    t_disp.start()
                elif msg_client_id != client_id:
                    raise ValueError("Client id mismatch in request stream")

            if msg_type == message_protocol.external.MsgType.ACCOUNTS_BATCH:
                _, rows = payload
                account_dicts = []
                for row in rows:
                    bank_id = _normalize_bank_id(row[1])
                    if bank_id in sent_bank_ids:
                        continue
                    sent_bank_ids.add(bank_id)
                    account_dicts.append({
                        "bank_name": row[0],
                        "bank_id": bank_id,
                        "client_id": client_id,
                    })
                for chunk in _chunks(account_dicts, gateway_output_batch_size):
                    serialized_message = handler.serialize_rows_message(client_id, chunk)
                    if isinstance(banks_output, ShardedExchangeProducer):
                        for shard in range(banks_output_shards):
                            banks_output.send_to_shard(serialized_message, shard)
                    else:
                        banks_output.send(serialized_message)
                with client_send_locks[client_id]:
                    message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK, client_id)
                continue

            if msg_type == message_protocol.external.MsgType.END_ACCOUNTS:
                # Propagamos el EOF de cuentas
                serialized_eof = handler.serialize_eof_message(client_id)
                if isinstance(banks_output, ShardedExchangeProducer):
                    banks_output.send_eof_to_all(serialized_eof)
                else:
                    banks_output.send(serialized_eof)

                with client_send_locks[client_id]:
                    message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK, client_id)
                continue

            if msg_type == message_protocol.external.MsgType.TRANSACTIONS_BATCH:
                _, rows = payload
                client_semaphores[client_id].acquire()
                if output is None:
                    output = _build_output_queue(mom_host, output_queue, output_exchange, output_shards)

                transactions = _rows_to_transactions(client_id, rows, transaction_columns)
                for transactions_chunk in _chunks(transactions, gateway_output_batch_size):
                    serialized_message = handler.serialize_rows_message(client_id, transactions_chunk)
                    if isinstance(output, ShardedExchangeProducer):
                        shard = random.randint(0, output_shards - 1)
                        output.send_to_shard(serialized_message, shard)
                    else:
                        output.send(serialized_message)

                with checkpoint_lock:
                    current_checkpoint = client_checkpoints.get(client_id, 0) + 1
                    client_checkpoints[client_id] = current_checkpoint
                    checkpoint_barriers[(client_id, current_checkpoint)] = set()

                checkpoint_msg = handler.serialize_checkpoint_message(client_id, current_checkpoint)
                if isinstance(output, ShardedExchangeProducer):
                    output.send_eof_to_all(checkpoint_msg)
                else:
                    output.send(checkpoint_msg)
                
                with client_send_locks[client_id]:
                    message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK, client_id)
                continue

            if msg_type == message_protocol.external.MsgType.END_TRANSACTIONS:
                if output is None:
                    output = _build_output_queue(mom_host, output_queue, output_exchange, output_shards)
                serialized_message = handler.serialize_eof_message(client_id)
                if isinstance(output, ShardedExchangeProducer):
                    output.send_eof_to_all(serialized_message)
                else:
                    output.send(serialized_message)

                output.close()
                output = None
                
                client_ready_events[client_id].set()
                
                with client_send_locks[client_id]:
                    message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK, client_id)
                continue 

            raise TypeError(f"Unexpected message type: {msg_type}")
    except Exception as e:
        error_name = type(e).__name__
        if error_name == 'IncompleteReadError' and getattr(e, 'partial', None) == b'':
            logging.info(f"El cliente {client_id} cerró el socket tras finalizar.")
        elif error_name in ('ConnectionResetError', 'ConnectionAbortedError', 'BrokenPipeError', 'OSError'):
            logging.info(f"Conexión finalizada con el cliente {client_id}.")
        else:
            logging.error(f"Handler error for client {client_id}: {e}")
            logging.error(traceback.format_exc())
        try:
            client_socket.close() 
        except:
            pass
    finally:
        if output is not None:
            output.close()
        if banks_output is not None:
            banks_output.close()
        logging.info(f"Terminó la recepción de datos de entrada para el cliente {client_id}.")