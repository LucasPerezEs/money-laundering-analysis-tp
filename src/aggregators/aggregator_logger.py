import json
import os
from common.logger.base_node_logger import BaseNodeLogger

class AggregatorLogger(BaseNodeLogger):
    def __init__(self, base_filepath: str):
        super().__init__(base_filepath)
        self.agg_state_filepath = f"{base_filepath}_agg_state.json"

    def save_aggregator_state(self, state: dict):
        tmp_path = self.agg_state_filepath + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(state, f)
        os.replace(tmp_path, self.agg_state_filepath)

    def recover_aggregator_state(self) -> dict:
        if not os.path.exists(self.agg_state_filepath):
            return {}
        try:
            with open(self.agg_state_filepath, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}