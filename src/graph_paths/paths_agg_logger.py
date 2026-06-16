import json
import os
from common.logger.base_node_logger import BaseNodeLogger

class PathsAggregatorLogger(BaseNodeLogger):
    def __init__(self, base_filepath: str):
        super().__init__(base_filepath)
        self.paths_state_filepath = f"{base_filepath}_paths_state.json"

    def save_paths_state(self, state: dict):
        # state es {client_id: {(b1, a1, b2, a2): count}}
        # Convertimos la tupla clave a un string JSON seguro
        serializable_state = {}
        for client_id, pair_counts in state.items():
            serializable_state[client_id] = {
                json.dumps(pair): count for pair, count in pair_counts.items()
            }

        tmp_path = self.paths_state_filepath + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(serializable_state, f)
        os.replace(tmp_path, self.paths_state_filepath)

    def recover_paths_state(self) -> dict:
        if not os.path.exists(self.paths_state_filepath):
            return {}
        try:
            with open(self.paths_state_filepath, "r") as f:
                raw_state = json.load(f)
                
            # Reconstruimos la tupla a partir del string guardado
            recovered_state = {}
            for client_id, pair_counts in raw_state.items():
                recovered_state[client_id] = {
                    tuple(json.loads(pair_str)): count for pair_str, count in pair_counts.items()
                }
            return recovered_state
        except (json.JSONDecodeError, IOError):
            return {}