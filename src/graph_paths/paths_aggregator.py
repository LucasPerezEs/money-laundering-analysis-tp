import logging
import os
from collections import defaultdict
from common.middleware.worker_base import WorkerBase

class PathsAggregator(WorkerBase):

    def __init__(self):
        super().__init__()
        self.min_paths = int(os.environ.get("MIN_PATHS", "5"))
        self.total_pair_counts = defaultdict(lambda: defaultdict(int))

    def process(self, data):
        c_id = str(data["client_id"])
        
        pair = (data["Origin"], data["Dest"])
        count = data["PathsCount"]
        
        self.total_pair_counts[c_id][pair] += count

        return []

    def on_eof(self, client_id=None):
        if client_id is None:
            for c_id in list(self.total_pair_counts.keys()):
                yield from self.on_eof(c_id)
            return

        c_id = str(client_id)
        if c_id not in self.total_pair_counts:
            return

        valid_accounts = set()

        for (origen, destino), total in self.total_pair_counts[c_id].items():
            if total > self.min_paths:
                valid_accounts.add(origen)
                valid_accounts.add(destino)

        for acc in sorted(list(valid_accounts)):
            bank, acc_id = acc.split(',')
            yield {
                "client_id": c_id,
                "Bank": bank,
                "Account": acc_id
            }

        del self.total_pair_counts[c_id]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    paths_aggregator = PathsAggregator()
    paths_aggregator.run()