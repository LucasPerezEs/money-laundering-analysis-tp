import json
import logging
import os
import zlib
from collections import defaultdict
from common.middleware.worker_base import WorkerBase

class IncomingEdgesFilter(WorkerBase):

    def __init__(self):
        super().__init__()
        self.min_incoming = int(os.environ.get("MIN_INCOMING", "0"))

        self.unique_origins_by_client = defaultdict(lambda: defaultdict(set))

        self.tmp_dir = f"/tmp/inc_filter_{self.shard_id}"
        os.makedirs(self.tmp_dir, exist_ok=True)

    def process(self, data):
        client_id = str(data["client_id"])
        
        origin_bank, origin_acc = data["From Bank"], data["Account"]
        dest_bank, dest_acc = data["To Bank"], data["Account.1"]
        
        origen = f"{origin_bank},{origin_acc}"
        destino = f"{dest_bank},{dest_acc}"
        
        unique_set = self.unique_origins_by_client[client_id][destino]

        if len(unique_set) >= self.min_incoming:
            data["Role"] = "outgoing_from_interm"
            return [data]

        unique_set.add(origen)

        dest_hash = abs(zlib.crc32(destino.encode('utf-8')))
        filepath = os.path.join(self.tmp_dir, f"{client_id}_{dest_hash}.jsonl")

        if len(unique_set) == self.min_incoming:
            results = []

            data["Role"] = "outgoing_from_interm"
            results.append(data)

            if os.path.exists(filepath):
                with open(filepath, "r") as f:
                    for line in f:
                        old_data = json.loads(line)
                        old_data["Role"] = "outgoing_from_interm"
                        results.append(old_data)

                os.remove(filepath)
                
            return results

        with open(filepath, "a") as f:
            f.write(json.dumps(data) + "\n")
            
        return []

    def on_eof(self, client_id=None):
        if client_id is None:
            for origin_client_id in list(self.unique_origins_by_client.keys()):
                yield from self.on_eof(origin_client_id)
            return

        if client_id not in self.unique_origins_by_client:
            return

        for destino, unique_set in self.unique_origins_by_client[client_id].items():
            if len(unique_set) < self.min_incoming:
                dest_hash = abs(zlib.crc32(destino.encode('utf-8')))
                filepath = os.path.join(self.tmp_dir, f"{client_id}_{dest_hash}.jsonl")
                if os.path.exists(filepath):
                    os.remove(filepath)

        del self.unique_origins_by_client[client_id]

    def _routing_key(self, msg: dict) -> str:
        key = f"{msg['From Bank']}{msg['Account']}"
        return str(zlib.crc32(key.encode('utf-8')) % self.output_shards)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    paths_aggregator = IncomingEdgesFilter()
    paths_aggregator.run()