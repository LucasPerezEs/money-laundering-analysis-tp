import logging
from common.middleware.double_io_worker_base import WorkerBaseDoubleIO


def _normalize_bank_id(bank_id):
    if bank_id is None:
        return "0"
    normalized = str(bank_id).strip()
    return normalized if normalized else "0"


class BankNameAdder(WorkerBaseDoubleIO):

    def process_main_input(self, data: dict) -> tuple[list, list]:
        bank_id = _normalize_bank_id(data.get("From Bank"))
        with self._shared_lock:
            if bank_id in self._shared_cache:
                data["Bank Name"] = self._shared_cache[bank_id]
                return ([], [data])
            pending = self._shared_pending.get(bank_id, [])
            pending.append(data)
            self._shared_pending[bank_id] = pending
            return ([], [])

    def on_main_input_eof(self, client_id=None) -> list:
        return []  # _send_main_output_eof hace el trabajo real

    def process_secondary_input(self, data: dict) -> tuple[list, list]:
        bank_id = _normalize_bank_id(data.get("bank_id"))
        bank_name = data.get("bank_name")
        resolved = []
        with self._shared_lock:
            self._shared_cache[bank_id] = bank_name
            if bank_id in self._shared_pending:
                for msg in self._shared_pending.pop(bank_id):
                    msg["Bank Name"] = bank_name
                    resolved.append(msg)
        return ([], resolved)

    def on_secondary_input_eof(self, client_id=None) -> list:
            logging.info(f"[BNA] secondary EOF client={client_id}")
            with self._eof_lock:
                sec_done = self._clients_eof_sec_input.get(client_id, 0) >= self.sec_n_upstream
                main_done = self._clients_eof_main_input.get(client_id, 0) >= self.main_n_upstream
                key = f"_bna_{client_id}"
                if sec_done and main_done and not self._clients_joined.get(key, False):
                    self._clients_joined[key] = True
                    should_flush = True
                else:
                    should_flush = False
            if should_flush:
                self._flush_and_send_eof(client_id)
            return []

    # ── Overrides de EOF ──────────────────────────────────────────────────────

    def _send_main_output_eof(self, client_id=None):
            """Llamado por la base (proceso main) después de recibir todos los EOFs main."""
            logging.info(f"[BNA] main EOF client={client_id}")
            with self._eof_lock:
                sec_done = self._clients_eof_sec_input.get(client_id, 0) >= self.sec_n_upstream
                main_done = self._clients_eof_main_input.get(client_id, 0) >= self.main_n_upstream
                key = f"_bna_{client_id}"
                if sec_done and main_done and not self._clients_joined.get(key, False):
                    self._clients_joined[key] = True
                    should_flush = True
                else:
                    should_flush = False
            if should_flush:
                self._flush_and_send_eof(client_id)

    def _send_sec_output_eof(self, client_id=None):
        pass  # suprimido — _flush_and_send_eof lo maneja

    # ── Resolución final ──────────────────────────────────────────────────────

    def _flush_and_send_eof(self, client_id):
        resolved = []
        with self._shared_lock:
            for bank_id in list(self._shared_pending.keys()):
                all_rows = self._shared_pending[bank_id]
                mine = [r for r in all_rows if r.get("client_id") == client_id]
                rest = [r for r in all_rows if r.get("client_id") != client_id]
                if mine:
                    name = self._shared_cache.get(bank_id, bank_id)
                    for row in mine:
                        row["Bank Name"] = name
                        resolved.append(row)
                if rest:
                    self._shared_pending[bank_id] = rest
                else:
                    self._shared_pending.pop(bank_id, None)
        if resolved:
            self._emit_sec_output(resolved)
        super()._send_sec_output_eof(client_id)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    BankNameAdder().run()