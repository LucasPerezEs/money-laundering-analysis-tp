import os
import logging
import socket
import signal
import multiprocessing
import message_handler
from common import middleware, message_protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE_PREFIX = os.environ["INPUT_QUEUE_PREFIX"]
OUTPUT_QUEUE = os.environ.get("OUTPUT_QUEUE", "")
OUTPUT_EXCHANGE = os.environ.get("OUTPUT_EXCHANGE", "")

TOTAL_QUERIES = os.environ["TOTAL_QUERIES"]

POSSIBLE_QUERIES = {
    0: message_protocol.external.MsgType.Q1_RESULTS_BATCH,
}

def handle_client_request(client_socket, message_handler):
    # Client's state
    output_queue = None
    banks = {}

    # Build output
    if OUTPUT_QUEUE != "":
        output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
    elif OUTPUT_EXCHANGE != "":
        output_queue = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, OUTPUT_EXCHANGE, ["gateway_data", "eof"])
    else:
        raise Exception("FATAL: no output given for data processing")

    # Read from socket
    try:
        # Accept accounts batches
        while True:
            logging.info("Espero mensaje...")
            message = message_protocol.external.recv_msg(client_socket)

            if message[0] == message_protocol.external.MsgType.ACCOUNT_BATCH:
                logging.info("Batch de cuentas recibido!")
                
                # Iterate over batch
                batch = message[1]
                for acc in batch:
                    # Check if bank is not stored
                    bank_name = acc[0]
                    bank_id = acc[1]
                    if bank_id not in banks:
                        banks[bank_id] = bank_name

                # Send ACK
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )
            elif message[0] == message_protocol.external.MsgType.END_OF_RECORDS:
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )
                break

        # Accept transactions batches
        while True:
            logging.info("Espero mensaje...")
            message = message_protocol.external.recv_msg(client_socket)

            if message[0] == message_protocol.external.MsgType.TRANSACTION_BATCH:
                logging.info("Batch de transacciones recibido!")

                print("TODO: Send transactions to processing")

                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )
            elif message[0] == message_protocol.external.MsgType.END_OF_RECORDS:
                serialized_message = message_handler.serialize_eof_message(message[1])

                print("TODO: Send EOF")

                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )
                break

    except socket.error:
        logging.error("The connection with the server was lost")
    except Exception as e:
        logging.error(e)
    finally:
        output_queue.close()


def handle_client_response(client_list, query_number):
    print("handle_client_response")

    input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, f"INPUT_QUEUE_{query_number}")

    def _consume_result(message, ack, nack):
        client_index = 0
        try:
            for [message_handler_instance, client_socket] in client_list:
                deserialized_message = (
                    message_handler_instance.deserialize_result_message(message)
                )

                if not deserialized_message:
                    client_index += 1
                    continue

                print("TODO: Envío de datos procesados al cliente")
                break
            client_list.pop(client_index)
            ack()
        except socket.error:
            logging.error("The connection with the server was lost")
            client_list.pop(client_index)
            ack()
        except Exception as e:
            logging.error(e)
            nack()
            input_queue.stop_consuming()

    input_queue.start_consuming(_consume_result)
    input_queue.close()


def handle_sigterm(server_socket, client_list, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for [_, client_socket] in client_list:
        client_socket.shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1


def main():
    logging.basicConfig(level=logging.INFO)

    with multiprocessing.Manager() as manager:
        client_list = manager.list()
        sigterm_received = manager.Value("c_short", 0)
        with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool:
            # Create handlers by queries
            for query_number in range(TOTAL_QUERIES):
                processes_pool.apply_async(handle_client_response, (client_list, query_number+1))

            # Listen to new clients
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                logging.info("Listening to connections")
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: handle_sigterm(
                        server_socket, client_list, sigterm_received
                    ),
                )
                while True:
                    try:
                        client_socket, _ = server_socket.accept()

                        logging.info("A new client has connected")
                        message_handler_instance = message_handler.MessageHandler()
                        client_list.append([message_handler_instance, client_socket])
                        processes_pool.apply_async(
                            handle_client_request,
                            (client_socket, message_handler_instance),
                        )
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("The connection with the client was lost")
                            return 1
                        else:
                            return 0
                    except Exception as e:
                        logging.error(e)
                        return 2
    return 0


if __name__ == "__main__":
    main()
