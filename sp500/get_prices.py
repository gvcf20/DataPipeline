from __future__ import annotations

import pandas as pd

try:
    from .pipeline import DEFAULT_PRICES_PATH, update_sp500_dataset
except ImportError:
    from pipeline import DEFAULT_PRICES_PATH, update_sp500_dataset


def download_prices(start_date: str | None = None) -> pd.DataFrame:
    parsed_start_date = pd.Timestamp(start_date).date() if start_date else None
    update_sp500_dataset(start_date=parsed_start_date)
    return pd.read_csv(DEFAULT_PRICES_PATH)


def download_data(start_date: str | None = None) -> pd.DataFrame:
    return download_prices(start_date=start_date)


if __name__ == "__main__":
    prices = download_prices()
    print(prices.tail())
