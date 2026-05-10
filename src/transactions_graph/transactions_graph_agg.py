import os
import logging
import signal
import time

from common import middleware, message_protocol, transaction_id
from graph.graph_class import DirectedGraph

# Environment variables
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
OUTPUT_BATCH_EDGES = os.environ["OUTPUT_BATCH_EDGES"]

# Constants
TRANSACTION_ORIGIN_BANK_POS = 0
TRANSACTION_ORIGIN_ACC_POS = 1
TRANSACTION_DESTINATION_BANK_POS = 2
TRANSACTION_DESTINATION_ACC_POS = 3

EDGES_TAGS = ["i", "o"]

class TransactionsGraphAgg:

    def __init__(self):
        # Create input queue
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE,
        )

        # Create output queue
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE,
        )

        # Create graph
        self.graph = DirectedGraph()

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
        logging.info("Batch de transacciones recibido")
        # For each transaction on the batch
        for transaction in transactions_batch:
            # Get origin account
            origin = transaction_id.TransactionID(
                        transaction[TRANSACTION_ORIGIN_BANK_POS],
                        transaction[TRANSACTION_ORIGIN_ACC_POS]
                        )
            
            # Get destination account
            destination = transaction_id.TransactionID(
                        transaction[TRANSACTION_DESTINATION_BANK_POS],
                        transaction[TRANSACTION_DESTINATION_ACC_POS]
                        )

            # Add nodes and edge
            self.graph.add_node(origin)
            self.graph.add_node(destination)
            self.graph.add_edge(origin, destination)
        
        logging.info("Batch de transacciones procesado")

    # Serialize and send output batch
    def _send_output_batch(self, transactions_batch):
        message = message_protocol.internal.serialize(transactions_batch)
        transactions_batch.clear()
        self.output_queue.send(message)

    # Process EOF
    def _process_eof(self):
        logging.info("EOF recibido")
        # New batch
        transactions_batch = []

        # For each node
        for origin in self.graph.get_nodes():

            # For each neighbour
            for destination in self.graph.get_neighbors(origin):

                # For each tag to send
                for tag in EDGES_TAGS:

                    # Append edge
                    transactions_batch.append((origin, destination, tag))
                    # If limit was reached, then send
                    if len(transactions_batch) == OUTPUT_BATCH_EDGES:
                        self._send_output_batch(transactions_batch)
                        logging.info("Batch de aristas enviado")

        # If there are leftover edges, send them
        self._send_output_batch(transactions_batch)
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
            logging.error("Mensaje erróneo recibido")
            nack()

    # Start aggregator execution
    def start(self):
        logging.info("Empieza ejecución")
        self.input_queue.start_consuming(self.process_message)
