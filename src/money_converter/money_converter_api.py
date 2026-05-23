from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from common.message_protocol import internal
from common.middleware.worker_base import WorkerBase

import logging
import os
import requests
import signal


class MoneyConversionClient(WorkerBase):
    def __init__(self):
        super().__init__()
        self._currency_rates_by_date = {}

    def _request_api(self, day, from_currency, to_currency):
        url = f"https://api.frankfurter.app/{day}?from={from_currency}&to={to_currency}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()["rates"]["USD"]

    def process(self, data):
        if "Type" not in data:
            datetime = data["timestamp"]
            origin_curr = data["origin"]
            dest_curr = data["destination"]
            if datetime not in self._currency_rates_by_date and \
                    (origin_curr, dest_curr) not in self._currency_rates_by_date[datetime]:
                conversion_rate = self._request_api(datetime, origin_curr, dest_curr)
            data_copy = data.copy()
            data_copy["conversion_rate"] = conversion_rate
            return [data_copy]

        return [{"Type" : "eob"}]

    def on_eof(self, client_id=None):
        return []


if __name__ == "__main__":
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)
    conversion_client = MoneyConversionClient()
    conversion_client.start()