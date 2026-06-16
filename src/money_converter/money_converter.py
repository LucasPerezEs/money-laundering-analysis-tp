import csv
import logging
import os

from money_converter_logger import MoneyConverterLogger
from common.middleware.double_io_worker_base import WorkerBaseDoubleIO

TARGET_CURRENCY_TAG = "TARGET_CURRENCY"
BTC_RATES_PATH_TAG = "BTC_RATES_PATH"
CONVERSION_LOG_SAMPLES_TAG = "CONVERSION_LOG_SAMPLES"

CURRENCY_CODES = {
    "US Dollar": "USD", "Euro": "EUR", "Yuan": "CNY",
    "Ruble": "RUB", "Yen": "JPY", "UK Pound": "GBP",
    "Swiss Franc": "CHF", "Australian Dollar": "AUD",
    "Canadian Dollar": "CAD", "Mexican Peso": "MXN",
    "Brazil Real": "BRL", "Rupee": "INR", "Saudi Riyal": "SAR",
    "Bitcoin": "BTC",
    "Shekel": "ILS",
}

class MoneyConverter(WorkerBaseDoubleIO):

    LOGGER_CLASS = MoneyConverterLogger

    def __init__(self):
        super().__init__()

        # Get environment variables
        self._target_currency = os.environ[TARGET_CURRENCY_TAG]

        # Currency rates by date
        self._currency_rates_by_date = {}
        self._btc_rates_by_day = self._load_btc_rates()
        self._log_samples_remaining = int(os.environ.get(CONVERSION_LOG_SAMPLES_TAG, "20"))

        # Recover state
        shard_id = os.environ.get("SHARD_ID", "-1")
        worker_dir = os.path.join(self.WORKER_LOGS_DIR, f"{self.consumer_group}_{shard_id}")
        
        temp_logger = self.LOGGER_CLASS(os.path.join(worker_dir, "temp"))
        self._rec_cache, self._rec_pending = temp_logger.recover_converter_state()
        temp_logger.close()
        
        self._state_restored = False

    def _log_conversion(self, day, origin_code, target_code, amount_in, rate, amount_out):
        if self._log_samples_remaining <= 0:
            return
        logging.info(
            "Conversion sample: day=%s origin=%s target=%s amount_in=%s rate=%s amount_out=%s",
            day,
            origin_code,
            target_code,
            amount_in,
            rate,
            amount_out,
        )
        self._log_samples_remaining -= 1

    def _load_btc_rates(self):
        path = os.environ.get(BTC_RATES_PATH_TAG, "/btc_rates.csv")
        rates = {}
        try:
            with open(path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    day = str(row.get("date", "")).strip().replace("/", "-")
                    rate = row.get("rate")
                    if day and rate:
                        rates[day] = float(rate)
        except Exception:
            logging.exception("No se pudo cargar BTC rates")
        return rates

    def _generate_consult_currency_rate(self, timestamp, origin_curr, dest_curr):
        return {"timestamp": timestamp, "origin": origin_curr, "destination": dest_curr, "sender_id": self.shard_id}

    def process_main_input(self, data: dict) -> tuple[list, list]:
        # Read once time the state that was restored
        if not self._state_restored:
            with self._shared_lock:
                if not self._state_restored:
                    self._shared_cache.update(self._rec_cache)
                    self._shared_pending.update(self._rec_pending)
                    self._state_restored = True

        # Get data elements
        data_copy = data.copy()
        timestamp = data["Timestamp"]
        day = str(timestamp).split(" ")[0].replace("/", "-")
        origin_curr = data["Payment Currency"]
        origin_code = CURRENCY_CODES.get(origin_curr, origin_curr)
        target_code = CURRENCY_CODES.get(self._target_currency, self._target_currency)
        
        rate_key = f"{day}_{origin_code}_{target_code}"

        if origin_code == target_code:
            data_copy["Payment Currency"] = self._target_currency
            return ([], [data_copy])

        if origin_code == "BTC" and target_code == "USD":
            rate = self._btc_rates_by_day.get(day)
            if rate is None:
                logging.info("BTC rate no disponible para %s", day)
                return ([], [])
            data_copy["Payment Currency"] = self._target_currency
            data_copy["Amount Paid"] = float(data["Amount Paid"]) / rate
            self._log_conversion(day, origin_code, target_code, data["Amount Paid"], rate, data_copy["Amount Paid"])
            return ([], [data_copy])

        if day in self._currency_rates_by_date and (origin_code, target_code) in self._currency_rates_by_date[day]:
            rate = self._currency_rates_by_date[day][(origin_code, target_code)]
            data_copy["Payment Currency"] = self._target_currency
            data_copy["Amount Paid"] = float(data["Amount Paid"]) * rate
            self._log_conversion(day, origin_code, target_code, data["Amount Paid"], rate, data_copy["Amount Paid"])
            return ([], [data_copy])

        with self._shared_lock:
            if rate_key in self._shared_cache:
                rate = self._shared_cache[rate_key]

                self._currency_rates_by_date.setdefault(day, {})
                self._currency_rates_by_date[day][(origin_code, target_code)] = rate
                data_copy["Payment Currency"] = self._target_currency
                data_copy["Amount Paid"] = float(data["Amount Paid"]) * rate
                self._log_conversion(day, origin_code, target_code, data["Amount Paid"], rate, data_copy["Amount Paid"])
                return ([], [data_copy])
            
            is_first_request = rate_key not in self._shared_pending
            pending_list = self._shared_pending.get(rate_key, [])
            pending_list.append(data_copy)
            self._shared_pending[rate_key] = pending_list

        if is_first_request:
            req = self._generate_consult_currency_rate(day, origin_code, target_code)
            return ([req], [])

        return ([], [])


    def process_secondary_input(self, data: dict) -> tuple[list, list]:
        # Read once time the state that was restored
        if not self._state_restored:
            with self._shared_lock:
                if not self._state_restored:
                    self._shared_cache.update(self._rec_cache)
                    self._shared_pending.update(self._rec_pending)
                    self._state_restored = True

        new_data_list = []

        if "Type" not in data:
            day = data["timestamp"]
            origin_code = data["origin"]
            target_code = data["destination"]
            currency_rate = data["conversion_rate"]
            rate_key = f"{day}_{origin_code}_{target_code}"

            with self._shared_lock:
                self._shared_cache[rate_key] = currency_rate
                pending_txs = self._shared_pending.pop(rate_key, [])

            for row in pending_txs:
                amount_in = row["Amount Paid"]
                amount_out = currency_rate * float(amount_in)
                row["Amount Paid"] = str(amount_out)
                row["Payment Currency"] = self._target_currency
                self._log_conversion(day, origin_code, target_code, amount_in, currency_rate, amount_out)
                new_data_list.append(row)

        return ([], new_data_list)
    
    def on_main_batch_complete(self):
        if hasattr(self, "node_logger"):
            self.node_logger.save_converter_state(
                dict(self._shared_cache), 
                dict(self._shared_pending)
            )

    def on_sec_batch_complete(self):
        if hasattr(self, "node_logger"):
            self.node_logger.save_converter_state(
                dict(self._shared_cache), 
                dict(self._shared_pending)
            )

if __name__ == "__main__":
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)
    money_converter = MoneyConverter()
    money_converter.run()
