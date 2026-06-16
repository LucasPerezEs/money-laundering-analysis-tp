import logging
import os

from bank_name_adder_logger import BankNameAdderLogger
from common.middleware.double_io_worker_base import WorkerBaseDoubleIO

def _normalize_bank_id(bank_id):
    if bank_id is None:
        return "0"
    normalized = str(bank_id).strip().lstrip("0")
    return normalized if normalized else "0"

class BankNameAdder(WorkerBaseDoubleIO):

    LOGGER_CLASS = BankNameAdderLogger

    def waits_for_both_pipeline_eofs(self) -> bool:
        return True

    def __init__(self):
        super().__init__()
        self.sec_batch_size = 1000

        shard_id = os.environ.get("SHARD_ID", "-1")
        worker_dir = os.path.join(self.WORKER_LOGS_DIR, f"{self.consumer_group}_{shard_id}")
        os.makedirs(worker_dir, exist_ok=True)
        
        temp_logger = self.LOGGER_CLASS(os.path.join(worker_dir, "temp"))
        self._rec_cache, self._rec_pending = temp_logger.recover_bank_state()
        temp_logger.close()
        
        self._state_restored = False
        
        logging.info(f"BankNameAdder recuperó {len(self._rec_cache)} bancos cacheados y {len(self._rec_pending)} pendientes.")

    def process_main_input(self, data: dict) -> tuple[list, list]:
        # Update recovered state if was read
        if not self._state_restored:
            with self._shared_lock:
                if not self._state_restored:
                    self._shared_cache.update(self._rec_cache)
                    self._shared_pending.update(self._rec_pending)
                    self._state_restored = True

        bank_id = _normalize_bank_id(data.get("From Bank"))

        # Get shared lock for shared elements
        with self._shared_lock:
            # Check if bank name is in cache
            if bank_id in self._shared_cache:
                data["Bank Name"] = self._shared_cache[bank_id]
                return ([], [data]) 
            else:   # If not, add element as pending data
                pending_list = self._shared_pending.get(bank_id, [])
                pending_list.append(data)
                self._shared_pending[bank_id] = pending_list
                return ([], [])

    def process_secondary_input(self, data: dict) -> tuple[list, list]:
        # Update recovered state if was read
        if not self._state_restored:
            with self._shared_lock:
                if not self._state_restored:
                    self._shared_cache.update(self._rec_cache)
                    self._shared_pending.update(self._rec_pending)
                    self._state_restored = True

        bank_id = _normalize_bank_id(data.get("bank_id", data.get("From Bank")))
        bank_name = data.get("bank_name", data.get("Bank Name"))

        resolved_messages = []

        # Get shared lock for shared elements
        with self._shared_lock:
            self._shared_cache[bank_id] = bank_name

            # Send elements if there are any pending name changes
            if bank_id in self._shared_pending:
                pending_list = self._shared_pending.pop(bank_id)
                for msg in pending_list:
                    msg["Bank Name"] = bank_name
                    resolved_messages.append(msg)

            self._emit_sec_output(resolved_messages)

        return ([], [])
    
    def on_main_batch_complete(self):
        if hasattr(self, "node_logger"):
            self.node_logger.save_bank_state(
                dict(self._shared_cache), 
                dict(self._shared_pending)
            )

    def on_sec_batch_complete(self):
        if hasattr(self, "node_logger"):
            self.node_logger.save_bank_state(
                dict(self._shared_cache), 
                dict(self._shared_pending)
            )

    def on_main_input_eof(self, client_id=None) -> list:
        unmatched = []
        with self._shared_lock:
            for bank_id, rows in self._shared_pending.items():
                for row in rows:
                    row["Bank Name"] = bank_id
                    unmatched.append(row)
            self._shared_pending.clear()

        if unmatched:
            self._emit_sec_output(unmatched)

        if hasattr(self, "node_logger"):
            self.node_logger.save_bank_state(
                dict(self._shared_cache), 
                dict(self._shared_pending)
            )

        return []

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bank_name_adder = BankNameAdder()
    bank_name_adder.run()
