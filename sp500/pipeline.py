from __future__ import annotations

import argparse
import datetime as dt
import re
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import yfinance as yf

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_CONSTITUENTS_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
DEFAULT_CONSTITUENTS_PATH = ROOT_DIR / "tickers" / "sp500" / "sp500_constituents.csv"
DEFAULT_COMPONENT_HISTORY_PATH = (
    ROOT_DIR / "tickers" / "sp500" / "sp_500_historical_components.csv"
)
DEFAULT_PRICES_PATH = ROOT_DIR / "sp500" / "sp500_daily_prices.csv"
DEFAULT_DOWNLOAD_BATCH_SIZE = 100

PRICE_COLUMNS = [
    "date",
    "symbol",
    "symbol_yahoo",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
]
NUMERIC_PRICE_COLUMNS = ["open", "high", "low", "close", "adj_close", "volume"]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _snake_case(value: Any) -> str:
    text = str(value).strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def parse_date(value: str | None) -> dt.date | None:
    if value is None:
        return None
    return dt.date.fromisoformat(value)


def normalize_symbol_for_yahoo(symbol: str) -> str:
    return symbol.strip().replace(".", "-")


def ensure_parent_directory(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def chunked_symbols(symbols: list[str], batch_size: int) -> list[list[str]]:
    return [symbols[i : i + batch_size] for i in range(0, len(symbols), batch_size)]


def parse_ticker_list(value: Any) -> list[str]:
    if pd.isna(value):
        return []
    text = str(value).strip().strip("[]").replace("'", "")
    return [t.strip() for t in text.split(",") if t.strip()]


# ---------------------------------------------------------------------------
# Constituents
# ---------------------------------------------------------------------------

def fetch_sp500_constituents(url: str = DEFAULT_CONSTITUENTS_URL) -> pd.DataFrame:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    tables = pd.read_html(StringIO(response.text))
    if not tables:
        raise ValueError(f"No HTML tables found at {url}.")

    constituents = tables[0].copy()
    constituents.columns = [_snake_case(c) for c in constituents.columns]
    if "symbol" not in constituents.columns:
        raise ValueError("S&P 500 table has no 'symbol' column.")

    constituents["symbol"] = constituents["symbol"].astype(str).str.strip()
    constituents = constituents[constituents["symbol"].ne("")]
    constituents["symbol_yahoo"] = constituents["symbol"].map(normalize_symbol_for_yahoo)
    constituents["fetched_at_utc"] = pd.Timestamp.now(tz="UTC").isoformat()
    constituents = constituents.sort_values("symbol").reset_index(drop=True)

    preferred = [
        "symbol", "symbol_yahoo", "security", "gics_sector", "gics_sub_industry",
        "headquarters_location", "date_added", "cik", "founded", "fetched_at_utc",
    ]
    other = [c for c in constituents.columns if c not in preferred]
    return constituents[preferred + other]


def save_constituents(constituents: pd.DataFrame, output_path: Path) -> None:
    ensure_parent_directory(output_path)
    constituents.to_csv(output_path, index=False)


def upsert_component_snapshot(
    constituents: pd.DataFrame,
    output_path: Path,
    snapshot_date: dt.date,
) -> pd.DataFrame:
    ensure_parent_directory(output_path)
    snapshot = pd.DataFrame({
        "date": [snapshot_date.isoformat()],
        "tickers": [",".join(constituents["symbol"].tolist())],
    })

    if output_path.exists():
        history = pd.read_csv(output_path, usecols=["date", "tickers"])
        history["date"] = pd.to_datetime(history["date"]).dt.strftime("%Y-%m-%d")
        history = history[history["date"] != snapshot_date.isoformat()]
        history = pd.concat([history, snapshot], ignore_index=True)
    else:
        history = snapshot

    history = history.sort_values("date").reset_index(drop=True)
    history.to_csv(output_path, index=False)
    return history


def fill_component_history_gaps(
    component_history_path: Path,
    end_date: dt.date,
) -> pd.DataFrame:
    """Fill missing business days in the historical components file up to end_date.

    For each missing business day, the constituent list is forward-filled from
    the most recent known snapshot.  This ensures the pipeline can run from
    scratch on any future date without manual intervention.
    """
    history = pd.read_csv(component_history_path, usecols=["date", "tickers"])
    history["date"] = pd.to_datetime(history["date"]).dt.date
    history = history.sort_values("date").reset_index(drop=True)

    all_bdates = pd.bdate_range(start=history["date"].min(), end=end_date)
    existing_dates = set(history["date"])
    missing_dates = [d.date() for d in all_bdates if d.date() not in existing_dates]

    if not missing_dates:
        return history

    # Forward-fill: align all business days, ffill tickers, keep only the new rows
    all_df = pd.DataFrame({"date": [d.date() for d in all_bdates]})
    merged = all_df.merge(history, on="date", how="left")
    merged["tickers"] = merged["tickers"].ffill()
    merged = merged.dropna(subset=["tickers"])

    new_rows = merged[merged["date"].isin(missing_dates)].copy()
    new_rows["date"] = new_rows["date"].astype(str)

    history["date"] = history["date"].astype(str)
    history = pd.concat([history, new_rows], ignore_index=True)
    history = history.sort_values("date").reset_index(drop=True)
    history.to_csv(component_history_path, index=False)

    print(f"Adicionados {len(new_rows)} dias úteis ao histórico de componentes.", flush=True)
    return history


def load_required_price_pairs(
    component_history_path: Path,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
) -> pd.DataFrame:
    """Return a DataFrame with one row per (date, symbol) from the historical components."""
    history = pd.read_csv(component_history_path, usecols=["date", "tickers"])
    history["date"] = pd.to_datetime(history["date"]).dt.date

    if start_date is not None:
        history = history[history["date"] >= start_date]
    if end_date is not None:
        history = history[history["date"] <= end_date]

    exploded = history.assign(symbol=history["tickers"].map(parse_ticker_list)).explode("symbol")
    exploded["symbol"] = exploded["symbol"].astype(str).str.strip()
    exploded = exploded[exploded["symbol"].ne("")]
    exploded["date"] = pd.to_datetime(exploded["date"]).dt.strftime("%Y-%m-%d")
    exploded["symbol_yahoo"] = exploded["symbol"].map(normalize_symbol_for_yahoo)

    return exploded[["date", "symbol", "symbol_yahoo"]].drop_duplicates().reset_index(drop=True)


# ---------------------------------------------------------------------------
# Price download
# ---------------------------------------------------------------------------

def reshape_yfinance_prices(
    raw: pd.DataFrame,
    symbol_lookup: dict[str, str],
) -> pd.DataFrame:
    """Convert yfinance output to a long-format DataFrame aligned with PRICE_COLUMNS."""
    if raw.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    if isinstance(raw.columns, pd.MultiIndex):
        try:
            long = raw.stack(level=0, future_stack=True).reset_index()
        except TypeError:
            long = raw.stack(level=0).reset_index()
        idx_cols = list(long.columns[:2])
        long = long.rename(columns={idx_cols[0]: "date", idx_cols[1]: "symbol_yahoo"})
    else:
        only_symbol = next(iter(symbol_lookup))
        long = raw.reset_index().copy()
        if "Date" not in long.columns and "date" not in long.columns:
            long = long.rename_axis("Date").reset_index()
        long["symbol_yahoo"] = only_symbol

    long = long.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    })
    long["date"] = pd.to_datetime(long["date"]).dt.strftime("%Y-%m-%d")
    long["symbol"] = long["symbol_yahoo"].map(symbol_lookup).fillna(long["symbol_yahoo"])

    for col in NUMERIC_PRICE_COLUMNS:
        if col not in long.columns:
            long[col] = pd.NA

    # Drop rows where all price columns are NaN (yfinance sometimes returns empty rows)
    long = long.dropna(how="all", subset=NUMERIC_PRICE_COLUMNS)

    return long[PRICE_COLUMNS].sort_values(["date", "symbol"]).reset_index(drop=True)


def download_batch_prices(
    batch_symbols: list[str],
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    """Download OHLCV for a batch of Yahoo-format symbols over [start_date, end_date]."""
    if not batch_symbols:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    symbol_lookup = {s: s for s in batch_symbols}
    raw = yf.download(
        tickers=batch_symbols if len(batch_symbols) > 1 else batch_symbols[0],
        start=start_date.isoformat(),
        end=(end_date + dt.timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
        actions=False,
        group_by="ticker",
        progress=False,
        threads=False,
    )
    prices = reshape_yfinance_prices(raw, symbol_lookup)

    # Retry individually any symbols that failed in the batch call
    downloaded = set(prices["symbol_yahoo"].unique())
    retry_frames: list[pd.DataFrame] = []
    for sym in batch_symbols:
        if sym not in downloaded:
            raw_retry = yf.download(
                tickers=sym,
                start=start_date.isoformat(),
                end=(end_date + dt.timedelta(days=1)).isoformat(),
                interval="1d",
                auto_adjust=False,
                actions=False,
                progress=False,
                threads=False,
            )
            retried = reshape_yfinance_prices(raw_retry, {sym: sym})
            if not retried.empty:
                retry_frames.append(retried)

    if retry_frames:
        prices = pd.concat([prices, *retry_frames], ignore_index=True)
        prices = prices.drop_duplicates(subset=["date", "symbol_yahoo"], keep="last")

    return prices.sort_values(["date", "symbol"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def get_missing_pairs(
    required_pairs: pd.DataFrame,
    prices_path: Path,
) -> pd.DataFrame:
    """Return rows from required_pairs that are not yet in the prices CSV."""
    if not prices_path.exists():
        return required_pairs.copy()

    existing = pd.read_csv(prices_path, usecols=["date", "symbol"])
    existing["date"] = existing["date"].astype(str)
    existing["symbol"] = existing["symbol"].astype(str)

    missing = required_pairs.merge(existing, on=["date", "symbol"], how="left", indicator=True)
    missing = missing[missing["_merge"] == "left_only"].drop(columns="_merge")
    return missing.reset_index(drop=True)


def build_prices_dataset(
    component_history_path: Path,
    prices_path: Path,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
    batch_size: int = DEFAULT_DOWNLOAD_BATCH_SIZE,
) -> dict[str, int]:
    """
    For every (date, symbol) pair in the historical components file, fetch OHLCV
    from yfinance and append to prices_path.  Pairs already in the CSV are skipped,
    so the run can be safely interrupted and resumed.
    """
    required_pairs = load_required_price_pairs(component_history_path, start_date, end_date)
    missing_pairs = get_missing_pairs(required_pairs, prices_path)

    already_done = len(required_pairs) - len(missing_pairs)
    symbols_needed = missing_pairs["symbol_yahoo"].drop_duplicates().tolist()
    batches = chunked_symbols(symbols_needed, batch_size)
    total_batches = len(batches)
    total_written = 0

    print(
        f"{len(required_pairs):,} required pairs | "
        f"{already_done:,} already in CSV | "
        f"{len(missing_pairs):,} to download "
        f"({len(symbols_needed):,} symbols, {total_batches} batches)",
        flush=True,
    )

    ensure_parent_directory(prices_path)
    write_header = not prices_path.exists()

    for i, batch_symbols in enumerate(batches, 1):
        batch_pairs = missing_pairs[missing_pairs["symbol_yahoo"].isin(batch_symbols)].copy()
        batch_start = pd.to_datetime(batch_pairs["date"]).min().date()
        batch_end = pd.to_datetime(batch_pairs["date"]).max().date()

        print(
            f"[{i}/{total_batches}] {len(batch_symbols)} symbols | {batch_start} → {batch_end}",
            flush=True,
        )

        downloaded = download_batch_prices(batch_symbols, batch_start, batch_end)

        # Left-join: every required pair for this batch gets a row;
        # pairs with no price data will have NaN in the OHLCV columns.
        aligned = batch_pairs.merge(
            downloaded.drop(columns=["symbol"], errors="ignore"),
            on=["date", "symbol_yahoo"],
            how="left",
        )[PRICE_COLUMNS]

        aligned.to_csv(prices_path, mode="a", header=write_header, index=False)
        write_header = False
        total_written += len(aligned)

        ok = int(aligned["close"].notna().sum())
        print(
            f"  {ok}/{len(aligned)} rows with prices | {total_written:,} total written so far",
            flush=True,
        )

    return {
        "required_pairs": len(required_pairs),
        "already_downloaded": already_done,
        "new_rows_written": total_written,
    }


def update_sp500_dataset(
    start_date: dt.date | None = None,
    as_of_date: dt.date | None = None,
    constituents_url: str = DEFAULT_CONSTITUENTS_URL,
    constituents_path: Path = DEFAULT_CONSTITUENTS_PATH,
    component_history_path: Path = DEFAULT_COMPONENT_HISTORY_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    skip_prices: bool = False,
    batch_size: int = DEFAULT_DOWNLOAD_BATCH_SIZE,
) -> dict[str, Any]:
    run_date = as_of_date or dt.date.today()

    constituents = fetch_sp500_constituents(url=constituents_url)
    save_constituents(constituents, constituents_path)
    upsert_component_snapshot(
        constituents=constituents,
        output_path=component_history_path,
        snapshot_date=run_date,
    )
    component_history = fill_component_history_gaps(
        component_history_path=component_history_path,
        end_date=run_date,
    )

    stats: dict[str, Any] = {
        "run_date": run_date.isoformat(),
        "constituents_count": len(constituents),
        "component_history_rows": len(component_history),
    }

    if not skip_prices:
        price_stats = build_prices_dataset(
            component_history_path=component_history_path,
            prices_path=prices_path,
            start_date=start_date,
            end_date=run_date,
            batch_size=batch_size,
        )
        stats.update(price_stats)

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh S&P 500 constituents and build a daily prices CSV "
            "covering all date/ticker pairs in the historical components file."
        )
    )
    parser.add_argument(
        "--start-date", default=None,
        help="YYYY-MM-DD lower bound for historical component dates to include.",
    )
    parser.add_argument(
        "--as-of-date", default=None,
        help="Override today's date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--skip-prices", action="store_true",
        help="Only refresh constituent files; skip price download.",
    )
    parser.add_argument(
        "--prices-path", default=str(DEFAULT_PRICES_PATH),
        help="Output CSV path for daily prices.",
    )
    parser.add_argument(
        "--batch-size", default=DEFAULT_DOWNLOAD_BATCH_SIZE, type=int,
        help="Number of Yahoo tickers to download per batch (default 100).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    stats = update_sp500_dataset(
        start_date=parse_date(args.start_date),
        as_of_date=parse_date(args.as_of_date),
        prices_path=Path(args.prices_path),
        skip_prices=args.skip_prices,
        batch_size=args.batch_size,
    )

    print("\nS&P 500 pipeline completed.")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
