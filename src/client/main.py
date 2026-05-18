import os
import logging
import csv
import socket
import signal
import time
import json

from common import message_protocol

TOTAL_QUERIES = 1
BATCH_SIZE = 50
ACCOUNTS_INPUT_FILE = os.environ["ACCOUNTS_INPUT_FILE"]
TRANSACTIONS_INPUT_FILE = os.environ["TRANSACTIONS_INPUT_FILE"]
OUTPUT_FILE = os.environ["OUTPUT_FILE"]
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])


RECONNECTION_TIME_SECS = 0.5

class Client:

    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Recieved SIGTERM signal")
        self.closed = True
        self.disconnect()

        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def open_output_files(self):
        self.q1_results = open("q1.json", "w")

    def close_output_files(self):
        self.q1_results.close()

    def connect(self, server_host, server_port):
        while True:
            try:
                self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.server_socket.connect((server_host, server_port))
            except:
                time.sleep(RECONNECTION_TIME_SECS)

    def disconnect(self):
        if self.server_socket:
            self.server_socket.shutdown(socket.SHUT_RDWR)

    def _store_results_from_batch(output_file, batch):
        for result in batch:
            json.dump(result, output_file)

    def _store_query_results(self, msg_type, batch):
        if msg_type == message_protocol.external.MsgType.Q1_RESULTS_BATCH:
            self._store_results_from_batch(self.q1_results, batch)

    def _recv_ack(self):
        while True:
            msg_type, values = message_protocol.external.recv_msg(self.server_socket)

            if msg_type == message_protocol.external.MsgType.ACK:
                break
            else:
                self._store_query_results(msg_type, values)

    def _send_csv_data_batches(self, csv_input_file, data_type, data_preparation_func):
        with open(csv_input_file, newline="\n") as input_file:
            csv_reader = csv.reader(input_file, delimiter=",", quotechar='"')
            next(csv_reader)

            # Fill batch
            batch = []
            for data_row in csv_reader:
                batch.append(data_preparation_func(data_row))
                
                # If max size was reached
                if len(batch) == BATCH_SIZE:
                    message_protocol.external.send_msg(
                        self.server_socket,
                        data_type,
                        batch
                    )
                    batch.clear()
                    self._recv_ack()

            # Check if remaining data is left
            if len(batch) > 0:
                message_protocol.external.send_msg(
                    self.server_socket,
                    data_type,
                    batch
                )
                batch.clear()
                self._recv_ack()


    def _get_next_account(self, row):
        return row

    def send_bank_accounts_information(self, accounts_input_file):
        logging.info("Sending accounts records...")

        # Send accounts
        self._send_csv_data_batches(
            accounts_input_file,
            message_protocol.external.MsgType.ACCOUNT_BATCH,
            self._get_next_account
            )

        # Send EOF
        message_protocol.external.send_msg(
            self.server_socket, message_protocol.external.MsgType.END_OF_RECORDS
        )
        self._recv_ack()
        logging.info("Accounts batch sent")


    def _get_next_transaction(self, row):
        row.pop()
        return row

    def send_transactions_records(self, transactions_input_file):
        logging.info("Sending transactions records...")

        # Send transactions
        self._send_csv_data_batches(
            transactions_input_file,
            message_protocol.external.MsgType.TRANSACTION_BATCH,
            self._get_next_transaction
            )

        # Send EOF
        message_protocol.external.send_msg(
            self.server_socket, message_protocol.external.MsgType.END_OF_RECORDS
        )
        self._recv_ack()

        logging.info("Transactions batch sent")

    def recv_queries_results(self):
        logging.info("Receiving results...")
        while True:
            msg_type, values = message_protocol.external.recv_msg(self.server_socket)

            if msg_type == message_protocol.external.MsgType.END_OF_RECORDS:
                break
            else:
                logging.info("Query results arrived")
                self._store_query_results(msg_type, values)


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client()

    try:
        client.connect(SERVER_HOST, SERVER_PORT)
        client.send_bank_accounts_information(ACCOUNTS_INPUT_FILE)
        client.send_transactions_records(TRANSACTIONS_INPUT_FILE)
        client.recv_queries_results(OUTPUT_FILE)
    except socket.error:
        if not client.closed:
            logging.error("The connection with the server was lost")
            return 1
    except Exception as e:
        logging.error(e)
        return 2
    finally:
        if not client.closed:
            client.disconnect()

    return 0


if __name__ == "__main__":
    main()
