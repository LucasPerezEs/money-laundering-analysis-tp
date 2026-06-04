import json
import logging
import os
import zlib
from collections import defaultdict
from common.middleware.worker_base import WorkerBase

class OutgoingEdgesFilter(WorkerBase):

    def __init__(self):
        super().__init__()
        self.min_outgoing = int(os.environ.get("MIN_OUTGOING", "5"))
        
        self.unique_dests_by_client = defaultdict(lambda: defaultdict(set))
        
        self.tmp_dir = f"/tmp/out_filter_{self.shard_id}"
        os.makedirs(self.tmp_dir, exist_ok=True)

    def process(self, data):
        c_id = str(data["client_id"])
        
        o_bank, o_acc = data["From Bank"], data["Account"]
        d_bank, d_acc = data["To Bank"], data["Account.1"]
        
        origen = f"{o_bank},{o_acc}"
        destino = f"{d_bank},{d_acc}"
        
        unique_set = self.unique_dests_by_client[c_id][origen]

        if len(unique_set) >= self.min_outgoing:
            data["Role"] = "incoming_to_interm"
            return [data]

        unique_set.add(destino)
        
        orig_hash = abs(zlib.crc32(origen.encode('utf-8')))
        filepath = os.path.join(self.tmp_dir, f"{c_id}_{orig_hash}.jsonl")

        if len(unique_set) == self.min_outgoing:
            results = []
            
            data["Role"] = "incoming_to_interm"
            results.append(data)
            
            if os.path.exists(filepath):
                with open(filepath, "r") as f:
                    for line in f:
                        old_data = json.loads(line)
                        old_data["Role"] = "incoming_to_interm"
                        results.append(old_data)
                os.remove(filepath)
                
            return results

        with open(filepath, "a") as f:
            f.write(json.dumps(data) + "\n")
            
        return []

    def on_eof(self, client_id=None):
        if client_id is None:
            for c_id in list(self.unique_dests_by_client.keys()):
                yield from self.on_eof(c_id)
            return

        c_id = str(client_id)
        if c_id not in self.unique_dests_by_client:
            return

        for origen, unique_set in self.unique_dests_by_client[c_id].items():
            if len(unique_set) < self.min_outgoing:
                orig_hash = abs(zlib.crc32(origen.encode('utf-8')))
                filepath = os.path.join(self.tmp_dir, f"{c_id}_{orig_hash}.jsonl")
                if os.path.exists(filepath):
                    os.remove(filepath)

        del self.unique_dests_by_client[c_id]

    def _routing_key(self, msg: dict) -> str:
        key = f"{msg['To Bank']}{msg['Account.1']}"
        return str(zlib.crc32(key.encode('utf-8')) % self.output_shards)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    paths_aggregator = OutgoingEdgesFilter()
    paths_aggregator.run()