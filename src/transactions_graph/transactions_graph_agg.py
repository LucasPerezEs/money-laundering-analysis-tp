"""
TransactionsGraphAgg:

Este worker recibe transacciones USD ya restringidas a la ventana temporal de
Q4 y shardeadas por cuenta origen. Durante el stream guarda las transacciones
de cada cliente y registra las cuentas destino distintas alcanzadas por cada
origen.

Al recibir EOF conserva solamente los origenes que transfirieron a mas de cinco
cuentas destino distintas. 

Cada transaccion que pasa ese filtro se emite dos veces:
- como vista de entrada, ruteada por la cuenta destino;
- como vista de salida, ruteada por la cuenta origen.

La etapa siguiente (`PathsCreator`) recibe ambas vistas para una misma cuenta
intermediaria y materializa caminos de dos saltos:
origen -> intermediaria -> destino.
"""
import logging
import hashlib

from common import transaction_id
from common.middleware.worker_base import WorkerBase

# Constants
TRANSACTION_ORIGIN_BANK_KEY = "From Bank"
TRANSACTION_ORIGIN_ACC_KEY = "Account"
TRANSACTION_DESTINATION_BANK_KEY = "To Bank"
TRANSACTION_DESTINATION_ACC_KEY = "Account.1"

NEW_DATA_EDGE_TAG_KEY = "Edge Type"
EDGES_INPUT_TAG = "i"
EDGES_OUTPUT_TAG = "o"
MIN_DISTINCT_DESTINATIONS = 5


class TransactionsGraphAgg(WorkerBase):
    """Filtra transacciones Q4 por origen activo y emite aristas de entrada/salida."""

    def __init__(self):
        super().__init__()
        self.transactions_by_client_id = {}
        self.destinations_by_source_by_client_id = {}

    def process(self, data):
        client_id = data["client_id"]

        origin = transaction_id.TransactionID(
            data[TRANSACTION_ORIGIN_BANK_KEY],
            data[TRANSACTION_ORIGIN_ACC_KEY],
        )
        destination = transaction_id.TransactionID(
            data[TRANSACTION_DESTINATION_BANK_KEY],
            data[TRANSACTION_DESTINATION_ACC_KEY],
        )

        self.transactions_by_client_id.setdefault(client_id, []).append(data)
        destinations_by_source = self.destinations_by_source_by_client_id.setdefault(
            client_id, {}
        )
        destinations_by_source.setdefault(origin, set()).add(destination)

        return []

    def on_eof(self, client_id=None):
        logging.info(f"EOF received for client_id={client_id}")
        transactions = self.transactions_by_client_id.pop(client_id, [])
        destinations_by_source = self.destinations_by_source_by_client_id.pop(
            client_id, {}
        )

        for data in transactions:
            origin = transaction_id.TransactionID(
                data[TRANSACTION_ORIGIN_BANK_KEY],
                data[TRANSACTION_ORIGIN_ACC_KEY],
            )
            if (
                len(destinations_by_source.get(origin, set()))
                <= MIN_DISTINCT_DESTINATIONS
            ):
                continue

            yield {
                "client_id": client_id,
                TRANSACTION_ORIGIN_BANK_KEY: data[TRANSACTION_ORIGIN_BANK_KEY],
                TRANSACTION_ORIGIN_ACC_KEY: data[TRANSACTION_ORIGIN_ACC_KEY],
                TRANSACTION_DESTINATION_BANK_KEY: data[
                    TRANSACTION_DESTINATION_BANK_KEY
                ],
                TRANSACTION_DESTINATION_ACC_KEY: data[
                    TRANSACTION_DESTINATION_ACC_KEY
                ],
                NEW_DATA_EDGE_TAG_KEY: EDGES_INPUT_TAG,
            }

            yield {
                "client_id": client_id,
                TRANSACTION_ORIGIN_BANK_KEY: data[TRANSACTION_ORIGIN_BANK_KEY],
                TRANSACTION_ORIGIN_ACC_KEY: data[TRANSACTION_ORIGIN_ACC_KEY],
                TRANSACTION_DESTINATION_BANK_KEY: data[
                    TRANSACTION_DESTINATION_BANK_KEY
                ],
                TRANSACTION_DESTINATION_ACC_KEY: data[
                    TRANSACTION_DESTINATION_ACC_KEY
                ],
                NEW_DATA_EDGE_TAG_KEY: EDGES_OUTPUT_TAG,
            }

        logging.info("EOF procesado: aristas enviadas")

    def _routing_key(self, msg: dict) -> str:
        """Shard numerico para que ambas vistas de un nodo lleguen al mismo worker."""
        if msg[NEW_DATA_EDGE_TAG_KEY] == EDGES_INPUT_TAG:
            key = (
                f"{msg[TRANSACTION_DESTINATION_BANK_KEY]}"
                f"{msg[TRANSACTION_DESTINATION_ACC_KEY]}"
            )
        else:
            key = (
                f"{msg[TRANSACTION_ORIGIN_BANK_KEY]}"
                f"{msg[TRANSACTION_ORIGIN_ACC_KEY]}"
            )
        return str(int(hashlib.md5(key.encode()).hexdigest(), 16) % self.output_shards)
