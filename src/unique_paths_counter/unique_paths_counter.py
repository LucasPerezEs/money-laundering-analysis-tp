"""
UniquePathsCounter:

Este worker recibe caminos de dos saltos ya materializados y shardeados por el
par de extremos `(From Bank, Account, To Bank, Account.1)`. Cuenta cuantos
caminos existen para cada par de extremos, preservando la multiplicidad de las
etapas anteriores.

Al recibir EOF conserva los pares de extremos con mas de cinco caminos y emite
las cuentas origen y destino unicas involucradas en esos pares.
"""
import logging

from common import transaction_id
from common.middleware.worker_base import WorkerBase

# Constants
TRANSACTION_ORIGIN_BANK_KEY = "From Bank"
TRANSACTION_ORIGIN_ACC_KEY = "Account"
TRANSACTION_DESTINATION_BANK_KEY = "To Bank"
TRANSACTION_DESTINATION_ACC_KEY = "Account.1"
TRANSACTION_INTERMEDIATE_BANK_KEY = "Interm Bank"
TRANSACTION_INTERMEDIATE_ACC_KEY = "Interm Acc"

TOTAL_PATHS_KEY = "Total Paths"
MIN_TOTAL_PATHS = 5

class UniquePathsCounter(WorkerBase):
    """Cuenta caminos Q4 por par de extremos y emite las cuentas calificadas."""

    def __init__(self):
        super().__init__()
        self.path_counts_by_client_id = {}

    # Process data message
    def process(self, data):
        logging.debug("Leo nuevo camino")
        client_id = data["client_id"]
        path_counts = self.path_counts_by_client_id.get(client_id)
        if path_counts is None:
            path_counts = {}
            self.path_counts_by_client_id[client_id] = path_counts

        start_node = transaction_id.TransactionID(
                        data[TRANSACTION_ORIGIN_BANK_KEY],
                        data[TRANSACTION_ORIGIN_ACC_KEY])

        end_node = transaction_id.TransactionID(
                        data[TRANSACTION_DESTINATION_BANK_KEY],
                        data[TRANSACTION_DESTINATION_ACC_KEY])

        if start_node != end_node:
            path_counts[(start_node, end_node)] = (
                path_counts.get((start_node, end_node), 0) + 1
            )

        return []


    # Process EOF
    def on_eof(self, client_id=None):
        logging.info(f"EOF received for client_id={client_id}")
        path_counts = self.path_counts_by_client_id.pop(client_id, {})

        matching_accounts = set()
        for (start_node, end_node), total_paths in path_counts.items():
            if total_paths <= MIN_TOTAL_PATHS:
                continue

            matching_accounts.add(start_node)
            matching_accounts.add(end_node)

        for account_node in sorted(matching_accounts, key=lambda node: node.as_tuple()):
            # Get start node ID elements
            bank, account = account_node.as_tuple()

            yield {
                "client_id" : client_id,
                "Bank" : bank,
                "Account" : account,
                }
            
        logging.info("EOF procesado: datos enviados")
