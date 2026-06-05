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
        return []

    def on_eof(self, client_id=None):
        return []

    def _routing_key(self, msg: dict) -> str:
        key = f"{msg['From Bank']}{msg['Account']}"
        return str(zlib.crc32(key.encode('utf-8')) % self.output_shards)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    paths_aggregator = IncomingEdgesFilter()
    paths_aggregator.run()