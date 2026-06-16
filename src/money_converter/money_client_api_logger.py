import json
import os
from common.logger.base_node_logger import BaseNodeLogger

class ConversionClientAPILogger(BaseNodeLogger):
    def __init__(self, base_filepath: str):
        super().__init__(base_filepath)
        self.rates_state_filepath = f"{base_filepath}_rates_state.json"

    def save_rates_state(self, rates: dict):
        serializable = {}
        for date, pairs in rates.items():
            serializable[date] = {
                f"{orig}_{dest}": rate for (orig, dest), rate in pairs.items()
            }
            
        tmp_path = self.rates_state_filepath + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(serializable, f)
        os.replace(tmp_path, self.rates_state_filepath)

    def recover_rates_state(self) -> dict:
        if not os.path.exists(self.rates_state_filepath):
            return {}
        try:
            with open(self.rates_state_filepath, "r") as f:
                raw = json.load(f)
                
            recovered = {}
            for date, pairs in raw.items():
                recovered[date] = {}
                for pair_str, rate in pairs.items():
                    orig, dest = pair_str.split("_")
                    recovered[date][(orig, dest)] = rate
            return recovered
        except (json.JSONDecodeError, IOError, ValueError):
            return {}