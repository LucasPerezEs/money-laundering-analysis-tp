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
import dbm
import logging
import os
import pickle
import zlib

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

    def __init__(self):
        super().__init__()
        self.db_path = f"/tmp/worker_edges_{self.shard_id}"
        self.edges_db = dbm.open(self.db_path, 'c')

    def process(self, data):
        client_id = str(data["client_id"])
        o_bank = str(data[TRANSACTION_ORIGIN_BANK_KEY])
        o_acc = str(data[TRANSACTION_ORIGIN_ACC_KEY])
        d_bank = str(data[TRANSACTION_DESTINATION_BANK_KEY])
        d_acc = str(data[TRANSACTION_DESTINATION_ACC_KEY])

        origin_key_str = f"{client_id}||{o_bank}||{o_acc}"
        origin_key_bytes = origin_key_str.encode('utf-8')
        destination = (d_bank, d_acc)

        try:
            destinations = pickle.loads(self.edges_db[origin_key_bytes])
            is_new_origin = False
        except KeyError:
            destinations = set()
            is_new_origin = True

        len_before = len(destinations)
        destinations.add(destination)

        if is_new_origin:
            log_path = f"/tmp/origins_shard_{self.shard_id}_client_{client_id}.log"
            with open(log_path, 'a') as f:
                f.write(f"{origin_key_str}\n")

        if len(destinations) > len_before:
            self.edges_db[origin_key_bytes] = pickle.dumps(destinations)

        return []

    def on_eof(self, client_id=None):
        client_id_str = str(client_id)
        logging.info(f"EOF recibido para client_id={client_id_str}")
        
        log_path = f"/tmp/origins_shard_{self.shard_id}_client_{client_id_str}.log"
        
        if not os.path.exists(log_path):
            logging.info("No se encontraron transacciones para este cliente.")
            return

        with open(log_path, 'r') as f:
            for line in f:
                origin_key_str = line.strip()
                origin_key_bytes = origin_key_str.encode('utf-8')

                if origin_key_bytes not in self.edges_db:
                    continue
                    
                destinations = pickle.loads(self.edges_db[origin_key_bytes])
                
                # Filtro de lógica de negocio
                if len(destinations) > MIN_DISTINCT_DESTINATIONS:
                    parts = origin_key_str.split('||')
                    o_bank = parts[1]
                    o_acc = parts[2]
                    
                    for d_bank, d_acc in destinations:
                        yield {
                            "client_id": client_id_str,
                            TRANSACTION_ORIGIN_BANK_KEY: o_bank,
                            TRANSACTION_ORIGIN_ACC_KEY: o_acc,
                            TRANSACTION_DESTINATION_BANK_KEY: d_bank,
                            TRANSACTION_DESTINATION_ACC_KEY: d_acc,
                            NEW_DATA_EDGE_TAG_KEY: EDGES_INPUT_TAG,
                        }
                        yield {
                            "client_id": client_id_str,
                            TRANSACTION_ORIGIN_BANK_KEY: o_bank,
                            TRANSACTION_ORIGIN_ACC_KEY: o_acc,
                            TRANSACTION_DESTINATION_BANK_KEY: d_bank,
                            TRANSACTION_DESTINATION_ACC_KEY: d_acc,
                            NEW_DATA_EDGE_TAG_KEY: EDGES_OUTPUT_TAG,
                        }
                del self.edges_db[origin_key_bytes]

        os.remove(log_path)
        logging.info("EOF procesado: archivo log temporal eliminado.")

    def _routing_key(self, msg: dict) -> str:
        if msg[NEW_DATA_EDGE_TAG_KEY] == EDGES_INPUT_TAG:
            key = f"{msg[TRANSACTION_DESTINATION_BANK_KEY]}{msg[TRANSACTION_DESTINATION_ACC_KEY]}"
        else:
            key = f"{msg[TRANSACTION_ORIGIN_BANK_KEY]}{msg[TRANSACTION_ORIGIN_ACC_KEY]}"

        return str(zlib.crc32(key) % self.output_shards)