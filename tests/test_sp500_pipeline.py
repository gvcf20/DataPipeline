from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from sp500.pipeline import (
    PRICE_COLUMNS,
    fill_component_history_gaps,
    get_missing_pairs,
    load_required_price_pairs,
    normalize_symbol_for_yahoo,
    reshape_yfinance_prices,
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
            raw=raw,
            symbol_lookup={"AAPL": "AAPL", "MSFT": "MSFT"},
        )

        self.assertEqual(list(reshaped.columns), PRICE_COLUMNS)
        self.assertEqual(len(reshaped), 4)
        self.assertEqual(sorted(reshaped["symbol"].unique().tolist()), ["AAPL", "MSFT"])

    def test_upsert_component_snapshot_replaces_same_day_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            history_path = Path(tmpdir) / "components.csv"
            pd.DataFrame([
                {"date": "2026-03-09", "tickers": "AAPL,MSFT"},
                {"date": "2026-03-10", "tickers": "AAPL,MSFT"},
            ]).to_csv(history_path, index=False)

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
            pd.DataFrame([
                {"date": "2026-03-09", "tickers": "AAPL,MSFT"},
                {"date": "2026-03-10", "tickers": "AAPL,NVDA"},
            ]).to_csv(history_path, index=False)

            pairs = load_required_price_pairs(
                component_history_path=history_path,
                start_date=dt.date(2026, 3, 10),
            )

            self.assertEqual(len(pairs), 2)
            self.assertEqual(sorted(pairs["symbol"].tolist()), ["AAPL", "NVDA"])

    def test_get_missing_pairs_excludes_existing_rows(self) -> None:
        required = pd.DataFrame([
            {"date": "2026-03-07", "symbol": "AAPL", "symbol_yahoo": "AAPL"},
            {"date": "2026-03-07", "symbol": "MSFT", "symbol_yahoo": "MSFT"},
        ])

        with tempfile.TemporaryDirectory() as tmpdir:
            prices_path = Path(tmpdir) / "prices.csv"
            pd.DataFrame([
                {"date": "2026-03-07", "symbol": "AAPL", "symbol_yahoo": "AAPL",
                 "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
                 "adj_close": 1.0, "volume": 1},
            ]).to_csv(prices_path, index=False)

            missing = get_missing_pairs(required, prices_path)

        self.assertEqual(len(missing), 1)
        self.assertEqual(missing.iloc[0]["symbol"], "MSFT")

    def test_get_missing_pairs_returns_all_when_no_csv(self) -> None:
        required = pd.DataFrame([
            {"date": "2026-03-07", "symbol": "AAPL", "symbol_yahoo": "AAPL"},
            {"date": "2026-03-07", "symbol": "MSFT", "symbol_yahoo": "MSFT"},
        ])

        with tempfile.TemporaryDirectory() as tmpdir:
            missing = get_missing_pairs(required, Path(tmpdir) / "prices.csv")

        self.assertEqual(len(missing), 2)


    def test_fill_component_history_gaps_fills_missing_business_days(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            history_path = Path(tmpdir) / "components.csv"
            # Gap between 2026-03-06 (Friday) and 2026-03-10 (Tuesday) — Mon/Tue missing
            pd.DataFrame([
                {"date": "2026-03-06", "tickers": "AAPL,MSFT"},
                {"date": "2026-03-10", "tickers": "AAPL,NVDA"},
            ]).to_csv(history_path, index=False)

            result = fill_component_history_gaps(history_path, end_date=dt.date(2026, 3, 10))

            dates = result["date"].tolist()
            self.assertIn("2026-03-09", dates)  # Monday filled
            # Monday should forward-fill from Friday's tickers
            monday_tickers = result.loc[result["date"] == "2026-03-09", "tickers"].iloc[0]
            self.assertEqual(monday_tickers, "AAPL,MSFT")

    def test_fill_component_history_gaps_no_op_when_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            history_path = Path(tmpdir) / "components.csv"
            pd.DataFrame([
                {"date": "2026-03-09", "tickers": "AAPL,MSFT"},
                {"date": "2026-03-10", "tickers": "AAPL,MSFT"},
            ]).to_csv(history_path, index=False)

            before = pd.read_csv(history_path)
            fill_component_history_gaps(history_path, end_date=dt.date(2026, 3, 10))
            after = pd.read_csv(history_path)

            self.assertEqual(len(before), len(after))


if __name__ == "__main__":
    unittest.main()
