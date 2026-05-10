import os
import logging
import signal
import time

from common import middleware, message_protocol, transaction_id

# Environment variables
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
OUTPUT_BATCH_SIZE = os.environ["OUTPUT_BATCH_SIZE"]

# Constants
ORIGIN_ACC_DATA_POS = 0
DESTINATION_ACC_DATA_POS = 1
TRANSACTION_TYPE_POS = 2

TRANSACTION_BANK_POS = 0
TRANSACTION_ACC_POS = 1

class PathsCreator:

    def __init__(self):
        # Create input queue
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE,
        )

        # Create output queue
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE,
        )

        # Create storage for edges of nodes
        self.incoming_edges = {}
        self.outgoing_edges = {}

        # Assign sigterm handler
        signal.signal(signalnum=signal.SIGTERM, handler=self._sigterm_handler)

    # Sigterm handler
    def _sigterm_handler(self, signum, frame):
        self.shutdown()

    # Get shutdowm retry time
    def __get_shutdown_retry_backoff(self, current_retries):
        RETRY_SHUT_DOWN_TIME_SEC = 0.1
        return RETRY_SHUT_DOWN_TIME_SEC

    # Shutdown function
    def shutdown(self):
        MAX_SHUTDOWN_RETRIES = 3
        current_retries = 0

        # Try up to MAX_SHUTDOWN_RETRIES
        while current_retries < MAX_SHUTDOWN_RETRIES:
            try:
                logging.info("IMPLEMENTAR APAGADO CORRRECTO!!!!!!!!!!")

            except:
                retry_time = self.__get_shutdown_retry_backoff(current_retries)
                time.sleep(retry_time)
                current_retries += 1

    # Process data message
    def _process_data_batch(self, transactions_batch):
        logging.info("Batch de datos recibido")
        # For each transaction
        for transaction in transactions_batch:
            # Transaction origin
            origin_acc_data = transaction[ORIGIN_ACC_DATA_POS]
            origin = transaction_id.TransactionID(origin_acc_data[TRANSACTION_BANK_POS], origin_acc_data[TRANSACTION_ACC_POS])

            # Transaction destination
            destination_acc_data = transaction[DESTINATION_ACC_DATA_POS]
            destination = transaction_id.TransactionID(destination_acc_data[TRANSACTION_BANK_POS], destination_acc_data[TRANSACTION_ACC_POS])

            # Get tag of edge
            tag = transaction[TRANSACTION_TYPE_POS]

            # Store according if it is an "incoming" edge, where the destination node is stored here,
            # or if it is an "outgoing" edge, where the origin node is stored here
            if tag == "i":
                if destination not in self.incoming_edges:
                    self.incoming_edges[destination] = set()
                self.incoming_edges[destination].add(origin)
            else:
                if origin not in self.outgoing_edges:
                    self.outgoing_edges[origin] = set()
                self.outgoing_edges[origin].add(destination)

        logging.info("Batch de datos procesado")

    # Serialize and send output batch
    def _send_output_batch(self, transactions_batch):
        message = message_protocol.internal.serialize(transactions_batch)
        transactions_batch.clear()
        self.output_queue.send(message)

    # Process EOF
    def _process_eof(self):
        logging.info("EOF recibido")

        # For each node with incoming edges
        batch = []
        for node in self.incoming_edges:
            # Check if there are outgoing edges
            if node in self.outgoing_edges:
                # Get neighbours
                incoming_edges_neighbours = self.incoming_edges[node]
                outgoing_edges_neighbours = self.outgoing_edges[node]

                # Create paths
                for inc_neighbour in incoming_edges_neighbours:
                    for out_neighbour in outgoing_edges_neighbours:
                        new_path = [inc_neighbour.as_tuple(), node.as_tuple(), out_neighbour.as_tuple()]
                        batch.append(new_path)

                        if len(batch) == OUTPUT_BATCH_SIZE:
                            self._send_output_batch(batch)

        if len(batch) > 0:
            self._send_output_batch(batch)

        logging.info("EOF procesado: datos enviados")

    # Process message that arrived
    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)

        if len(fields) > 1:
            self._process_data_batch(fields)
            ack()
        elif len(fields) == 1:
            self._process_eof(*fields)
            ack()
        else:
            nack()

    # Start creator execution
    def start(self):
        logging.info("Empieza ejecución")
        self.input_queue.start_consuming(self.process_message)
