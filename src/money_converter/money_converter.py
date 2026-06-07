import csv
import logging
import os
import time

import requests

from common.middleware.worker_base import WorkerBase

TARGET_CURRENCY_TAG = "TARGET_CURRENCY"
BTC_RATES_PATH_TAG = "BTC_RATES_PATH"

CURRENCY_CODES = {
    "US Dollar": "USD", "Euro": "EUR", "Yuan": "CNY",
    "Ruble": "RUB", "Yen": "JPY", "UK Pound": "GBP",
    "Swiss Franc": "CHF", "Australian Dollar": "AUD",
    "Canadian Dollar": "CAD", "Mexican Peso": "MXN",
    "Brazil Real": "BRL", "Rupee": "INR", "Saudi Riyal": "SAR",
    "Bitcoin": "BTC", "Shekel": "ILS",
}


class MoneyConverter(WorkerBase):

    def __init__(self):
        super().__init__()
        self._target_currency = os.environ[TARGET_CURRENCY_TAG]
        self._rates = {}
        self._btc_rates = self._load_btc_rates()

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

    def _fetch_rate(self, day: str, origin_code: str, target_code: str):
        key = (day, origin_code, target_code)
        if key in self._rates:
            return self._rates[key]
        url = f"https://api.frankfurter.dev/v2/rate/{origin_code}/{target_code}?date={day}"
        for attempt in range(3):
            try:
                resp = requests.get(url, timeout=15)
                resp.raise_for_status()
                rate = float(resp.json()["rate"])
                self._rates[key] = rate
                return rate
            except Exception as e:
                logging.warning(f"API error {origin_code}/{target_code} on {day} (attempt {attempt+1}): {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
        return None

    def process(self, data: dict) -> list:
        data_copy = data.copy()
        day = str(data["Timestamp"]).split(" ")[0].replace("/", "-")
        origin_code = CURRENCY_CODES.get(data["Payment Currency"], data["Payment Currency"])
        target_code = CURRENCY_CODES.get(self._target_currency, self._target_currency)

        if origin_code == target_code:
            data_copy["Payment Currency"] = self._target_currency
            return [data_copy]

        if origin_code == "BTC" and target_code == "USD":
            rate = self._btc_rates.get(day)
            if rate is None:
                return []
            data_copy["Amount Paid"] = float(data["Amount Paid"]) * rate
            data_copy["Payment Currency"] = self._target_currency
            return [data_copy]

        rate = self._fetch_rate(day, origin_code, target_code)
        if rate is None:
            return []

        data_copy["Amount Paid"] = float(data["Amount Paid"]) * rate
        data_copy["Payment Currency"] = self._target_currency
        return [data_copy]

    def on_eof(self, client_id=None):
        return []


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    MoneyConverter().run()


