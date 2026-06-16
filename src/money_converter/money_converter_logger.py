import json
import os
from common.logger.base_node_logger import BaseNodeLogger

class MoneyConverterLogger(BaseNodeLogger):
    def __init__(self, base_filepath: str):
        super().__init__(base_filepath)
        worker_dir = os.path.dirname(base_filepath)
        self.converter_state_filepath = os.path.join(worker_dir, "converter_state.json")

    def save_converter_state(self, cache: dict, pending: dict):
        state = {
            "cache": dict(cache),
            "pending": dict(pending)
        }
        tmp_path = self.converter_state_filepath + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(state, f)
        os.replace(tmp_path, self.converter_state_filepath)

    def recover_converter_state(self) -> tuple:
        if not os.path.exists(self.converter_state_filepath):
            return {}, {}
        try:
            with open(self.converter_state_filepath, "r") as f:
                state = json.load(f)
                return state.get("cache", {}), state.get("pending", {})
        except (json.JSONDecodeError, IOError):
            return {}, {}