import os
import logging
import csv
import socket
import signal

from common import message_protocol

BATCH_SIZE = os.environ["BATCH_SIZE"]
ACCOUNTS_INPUT_FILE = os.environ["ACCOUNTS_INPUT_FILE"]
TRANSACTIONS_INPUT_FILE = os.environ["TRANSACTIONS_INPUT_FILE"]
OUTPUT_FILE = os.environ["OUTPUT_FILE"]
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])


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

    def connect(self, server_host, server_port):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.connect((server_host, server_port))

    def disconnect(self):
        if self.server_socket:
            self.server_socket.shutdown(socket.SHUT_RDWR)

    def send_bank_accounts_information(self, accounts_input_file):
        pass

    def send_transactions_records(self, transactions_input_file):
        logging.info("Sending transactions records...")

        # Send transactions
        with open(transactions_input_file, newline="\n") as transactions_file:
            csv_reader = csv.reader(transactions_file, delimiter=",", quotechar='"')
            for row in csv_reader:
                [fruit, amount] = row
                message_protocol.external.send_msg(
                    self.server_socket,
                    message_protocol.external.MsgType.FRUIT_RECORD,
                    fruit,
                    int(amount),
                )
                message_protocol.external.recv_msg(self.server_socket)

        # Send EOF
        message_protocol.external.send_msg(
            self.server_socket, message_protocol.external.MsgType.END_OF_RECODS
        )

    def recv_queries_results(self, output_file):
        logging.info("Receiving results...")
        raise Exception("TODO: Receive queries results")


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
