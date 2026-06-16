import json
import os
from common.logger.base_node_logger import BaseNodeLogger

class EdgesFilterLogger(BaseNodeLogger):
    def __init__(self, base_filepath: str):
        super().__init__(base_filepath)
        self.active_clients_filepath = f"{base_filepath}_active_clients.json"
        self.disk_sets_base_dir = f"{base_filepath}_disk_sets"
        os.makedirs(self.disk_sets_base_dir, exist_ok=True)

    def save_active_clients(self, client_ids: list):
        tmp_path = self.active_clients_filepath + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(client_ids, f)
        os.replace(tmp_path, self.active_clients_filepath)

    def recover_active_clients(self) -> list:
        if not os.path.exists(self.active_clients_filepath):
            return []
        try:
            with open(self.active_clients_filepath, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return []