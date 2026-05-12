import os
import logging
import signal
import time

from common import middleware, message_protocol, transaction_id
from common.middleware.worker_base import WorkerBase

# Environment variables
OUTPUT_BATCH_SIZE = os.environ["OUTPUT_BATCH_SIZE"]

# Constants
START_ACC_DATA_POS = 0
INTERMEDIATE_ACC_DATA_POS = 1
END_ACC_DATA_POS = 2

TRANSACTION_BANK_POS = 0
TRANSACTION_ACC_POS = 1

class UniquePathsCounter(WorkerBase):

    def __init__(self):
        # Create storage for intermediate nodes
        self.intermediate_nodes = {}

    # Process data message
    def process(self, transactions_batch):
        logging.info("Batch de datos recibido")
        # For each transaction
        for transaction in transactions_batch:
            # Path start
            start_acc_data = transaction[START_ACC_DATA_POS]
            start_node = transaction_id.TransactionID(
                            start_acc_data[TRANSACTION_BANK_POS],
                            start_acc_data[TRANSACTION_ACC_POS])

            # Path intermediate node
            intermediate_acc_data = transaction[INTERMEDIATE_ACC_DATA_POS]
            intermediate_node = transaction_id.TransactionID(
                                intermediate_acc_data[TRANSACTION_BANK_POS],
                                intermediate_acc_data[TRANSACTION_ACC_POS])

            # Path end
            end_acc_data = transaction[END_ACC_DATA_POS]
            end_node = transaction_id.TransactionID(
                            end_acc_data[TRANSACTION_BANK_POS],
                            end_acc_data[TRANSACTION_ACC_POS])

            # Add intermediate node
            intermediate_accs_set = self.intermediate_nodes.get((start_node, end_node), set())
            intermediate_accs_set.add(intermediate_node)

        logging.info("Batch de datos procesado")

    # Process EOF
    def on_eof(self):
        logging.info("EOF recibido")

        # For each node with incoming edges
        batch_data = []
        for (start_node, end_node) in self.intermediate_nodes:
            # Get total of unique paths
            yield len(self.intermediate_nodes[(start_node, end_node)])
            
        logging.info("EOF procesado: datos enviados")