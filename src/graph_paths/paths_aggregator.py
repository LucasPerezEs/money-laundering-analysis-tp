import logging
from common.middleware.worker_base import WorkerBase

MIN_TOTAL_PATHS = 5


class PathsAggregator(WorkerBase):

    def __init__(self):
        super().__init__()
        self.total_pair_counts = {}

    def process(self, data):
        client_id = data["client_id"]
        origin_and_dest = (data["From Bank"], data["Account"], data["To Bank"], data["Account.1"])
        if client_id not in self.total_pair_counts:
            self.total_pair_counts[client_id] = {}
        if origin_and_dest not in self.total_pair_counts[client_id]:
            self.total_pair_counts[client_id][origin_and_dest] = 1
        else:
            self.total_pair_counts[client_id][origin_and_dest] += 1

        return []

    def on_eof(self, client_id=None):
        if client_id is None:
            return []

        pair_counts = self.total_pair_counts.pop(client_id, {})
        unique_accounts = set()
        qualified_pairs = 0
        max_paths = 0

        for (origin_bank, origin_acc, dest_bank, dest_acc), count in pair_counts.items():
            max_paths = max(max_paths, count)
            if count <= MIN_TOTAL_PATHS:
                continue

            qualified_pairs += 1
            unique_accounts.add((origin_bank, origin_acc))
            unique_accounts.add((dest_bank, dest_acc))

        for bank, account in sorted(unique_accounts):
            yield {
                "client_id": client_id,
                "Bank": bank,
                "Account": account
            }

        logging.info(
            "EOF procesado: "
            f"pairs={len(pair_counts)} max_paths_for_pair={max_paths} "
            f"qualified_pairs={qualified_pairs} emitted_accounts={len(unique_accounts)}"
        )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    paths_aggregator = PathsAggregator()
    paths_aggregator.run()
