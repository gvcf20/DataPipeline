from __future__ import annotations

import pandas as pd

try:
    from .pipeline import DEFAULT_PRICES_PATH
except ImportError:
    from pipeline import DEFAULT_PRICES_PATH


def read_csv_prices_table() -> pd.DataFrame:
    prices = pd.read_csv(DEFAULT_PRICES_PATH, low_memory=False)
    print(prices.head())
    return prices


if __name__ == "__main__":
    read_csv_prices_table()
