from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from sp500.pipeline import (
    PRICE_COLUMNS,
    align_prices_to_required_pairs,
    build_missing_required_pairs,
    load_required_price_pairs,
    normalize_symbol_for_yahoo,
    reshape_yfinance_prices,
    standardize_prices_frame,
    upsert_component_snapshot,
)


class Sp500PipelineTests(unittest.TestCase):
    def test_normalize_symbol_for_yahoo(self) -> None:
        self.assertEqual(normalize_symbol_for_yahoo("BRK.B"), "BRK-B")
        self.assertEqual(normalize_symbol_for_yahoo(" BF.B "), "BF-B")

    def test_reshape_yfinance_prices_with_multiindex_input(self) -> None:
        raw = pd.DataFrame(
            {
                ("AAPL", "Open"): [100.0, 101.0],
                ("AAPL", "High"): [102.0, 103.0],
                ("AAPL", "Low"): [99.0, 100.0],
                ("AAPL", "Close"): [101.0, 102.0],
                ("AAPL", "Adj Close"): [101.0, 102.0],
                ("AAPL", "Volume"): [1000, 1200],
                ("MSFT", "Open"): [200.0, 201.0],
                ("MSFT", "High"): [202.0, 203.0],
                ("MSFT", "Low"): [199.0, 200.0],
                ("MSFT", "Close"): [201.0, 202.0],
                ("MSFT", "Adj Close"): [201.0, 202.0],
                ("MSFT", "Volume"): [2000, 2200],
            },
            index=pd.to_datetime(["2026-03-06", "2026-03-09"]),
        )

        reshaped = reshape_yfinance_prices(
            raw_prices=raw,
            symbol_lookup={"AAPL": "AAPL", "MSFT": "MSFT"},
        )

        self.assertEqual(list(reshaped.columns), PRICE_COLUMNS)
        self.assertEqual(len(reshaped), 4)
        self.assertEqual(sorted(reshaped["symbol"].unique().tolist()), ["AAPL", "MSFT"])
        self.assertTrue((reshaped["price_status"] == "ok").all())

    def test_upsert_component_snapshot_replaces_same_day_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            history_path = Path(tmpdir) / "components.csv"
            pd.DataFrame(
                [
                    {"date": "2026-03-09", "tickers": "AAPL,MSFT"},
                    {"date": "2026-03-10", "tickers": "AAPL,MSFT"},
                ]
            ).to_csv(history_path, index=False)

            constituents = pd.DataFrame({"symbol": ["AAPL", "NVDA"]})
            updated = upsert_component_snapshot(
                constituents=constituents,
                output_path=history_path,
                snapshot_date=dt.date(2026, 3, 10),
            )

            self.assertEqual(len(updated), 2)
            self.assertEqual(
                updated.loc[updated["date"] == "2026-03-10", "tickers"].iloc[0],
                "AAPL,NVDA",
            )

    def test_load_required_price_pairs_explodes_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            history_path = Path(tmpdir) / "components.csv"
            pd.DataFrame(
                [
                    {"date": "2026-03-09", "tickers": "AAPL,MSFT"},
                    {"date": "2026-03-10", "tickers": "AAPL,NVDA"},
                ]
            ).to_csv(history_path, index=False)

            pairs = load_required_price_pairs(
                component_history_path=history_path,
                start_date=dt.date(2026, 3, 10),
            )

            self.assertEqual(len(pairs), 2)
            self.assertEqual(sorted(pairs["symbol"].tolist()), ["AAPL", "NVDA"])

    def test_align_prices_to_required_pairs_marks_missing_rows(self) -> None:
        required_pairs = pd.DataFrame(
            [
                {"date": "2026-03-07", "symbol": "AAPL", "symbol_yahoo": "AAPL"},
                {"date": "2026-03-08", "symbol": "AAPL", "symbol_yahoo": "AAPL"},
            ]
        )
        fetched_prices = standardize_prices_frame(
            pd.DataFrame(
                [
                    {
                        "date": "2026-03-07",
                        "symbol": "AAPL",
                        "symbol_yahoo": "AAPL",
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 100.5,
                        "adj_close": 100.5,
                        "volume": 1000,
                        "data_source": "yfinance",
                        "updated_at_utc": "2026-03-07T00:00:00+00:00",
                        "price_status": "ok",
                    }
                ]
            )
        )

        aligned = align_prices_to_required_pairs(required_pairs, fetched_prices)

        self.assertEqual(len(aligned), 2)
        self.assertEqual(aligned.iloc[0]["price_status"], "ok")
        self.assertEqual(aligned.iloc[1]["price_status"], "non_trading_day")

    def test_build_missing_required_pairs_excludes_existing_rows(self) -> None:
        required_pairs = pd.DataFrame(
            [
                {"date": "2026-03-07", "symbol": "AAPL", "symbol_yahoo": "AAPL"},
                {"date": "2026-03-07", "symbol": "MSFT", "symbol_yahoo": "MSFT"},
            ]
        )
        existing_prices = pd.DataFrame(
            [
                {
                    "date": "2026-03-07",
                    "symbol": "AAPL",
                    "symbol_yahoo": "AAPL",
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "adj_close": 1.0,
                    "volume": 1,
                    "price_status": "ok",
                    "data_source": "yfinance",
                    "updated_at_utc": "2026-03-07T00:00:00+00:00",
                }
            ]
        )

        missing = build_missing_required_pairs(required_pairs, existing_prices)
        self.assertEqual(len(missing), 1)
        self.assertEqual(missing.iloc[0]["symbol"], "MSFT")


if __name__ == "__main__":
    unittest.main()
