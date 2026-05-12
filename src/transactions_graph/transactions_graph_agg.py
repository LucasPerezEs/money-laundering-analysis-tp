import os
import logging
import signal
import time

from common import middleware, message_protocol, transaction_id
from graph.graph_class import DirectedGraph
from common.middleware.worker_base import WorkerBase

# Environment variables
OUTPUT_BATCH_EDGES = os.environ["OUTPUT_BATCH_EDGES"]

# Constants
TRANSACTION_ORIGIN_BANK_POS = 0
TRANSACTION_ORIGIN_ACC_POS = 1
TRANSACTION_DESTINATION_BANK_POS = 2
TRANSACTION_DESTINATION_ACC_POS = 3

EDGES_TAGS = ["i", "o"]

class TransactionsGraphAgg(WorkerBase):

    def __init__(self):
        # Create graph
        self.graph = DirectedGraph()

    # Process data message
    def process(self, transactions_batch):
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

    # Process EOF
    def on_eof(self):
        logging.info("EOF recibido")
        # New batch
        transactions_batch = []

        # For each node
        for origin in self.graph.get_nodes():

            # For each neighbour
            for destination in self.graph.get_neighbors(origin):

                # For each tag to send
                for tag in EDGES_TAGS:
                    yield (origin, destination, tag)

        logging.info("EOF procesado: datos enviados")
