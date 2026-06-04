import logging
import os
import zlib
from collections import defaultdict
from common.middleware.worker_base import WorkerBase

class PathsCreator(WorkerBase):

    def __init__(self):
        super().__init__()
        self.incoming_edges = defaultdict(lambda: defaultdict(set))
        self.outgoing_edges = defaultdict(lambda: defaultdict(set))

    def process(self, data):
        client_id = data["client_id"]
        role = data.get("Role")
        
        o_bank, origin_acc = data["From Bank"], data["Account"]
        dest_bank, dest_acc = data["To Bank"], data["Account.1"]
        
        origen = f"{o_bank},{origin_acc}"
        destino = f"{dest_bank},{dest_acc}"

        if role == "incoming_to_interm":
            self.incoming_edges[client_id][destino].add(origen)
        elif role == "outgoing_from_interm":
            self.outgoing_edges[client_id][origen].add(destino)

        return []

    def on_eof(self, client_id=None):
        if client_id is None:
            for c_id in list(self.incoming_edges.keys()) + list(self.outgoing_edges.keys()):
                yield from self.on_eof(c_id)
            return

        local_pair_counts = defaultdict(int)

        interms = set(self.incoming_edges[client_id].keys()).intersection(self.outgoing_edges[c_id].keys())
        
        for interm in interms:
            for origen in self.incoming_edges[client_id][interm]:
                for destino in self.outgoing_edges[client_id][interm]:
                    if origen != destino:
                        local_pair_counts[(origen, destino)] += 1

        for (origen, destino), count in local_pair_counts.items():
            yield {
                "client_id": client_id,
                "Origin": origen,
                "Dest": destino,
                "PathsCount": count
            }

        self.incoming_edges.pop(client_id, None)
        self.outgoing_edges.pop(client_id, None)

    def _routing_key(self, msg: dict) -> str:
        key = f"{msg['Origin']}||{msg['Dest']}"
        return str(zlib.crc32(key.encode('utf-8')) % self.output_shards)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    paths_creator = PathsCreator()
    paths_creator.run()