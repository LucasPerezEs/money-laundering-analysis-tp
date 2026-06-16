import json
import os
from common.logger.base_node_logger import BaseNodeLogger

class BarrierFilterLogger(BaseNodeLogger):
    def __init__(self, base_filepath: str):
        super().__init__(base_filepath)
        worker_dir = os.path.dirname(base_filepath)
        self.barrier_state_filepath = os.path.join(worker_dir, "barrier_state.json")

    def save_barrier_state(self, thresholds: dict, ready_flags: dict):
        state = {
            "thresholds": dict(thresholds),
            "ready_flags": dict(ready_flags)
        }
        tmp_path = self.barrier_state_filepath + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(state, f)
        os.replace(tmp_path, self.barrier_state_filepath)

    def recover_barrier_state(self) -> tuple:
        if not os.path.exists(self.barrier_state_filepath):
            return {}, {}
        try:
            with open(self.barrier_state_filepath, "r") as f:
                state = json.load(f)
                return state.get("thresholds", {}), state.get("ready_flags", {})
        except (json.JSONDecodeError, IOError):
            return {}, {}