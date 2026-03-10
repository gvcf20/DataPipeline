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
    "price_status",
    "data_source",
    "updated_at_utc",
]
NUMERIC_PRICE_COLUMNS = ["open", "high", "low", "close", "adj_close", "volume"]


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
    return [symbols[index : index + batch_size] for index in range(0, len(symbols), batch_size)]


def parse_ticker_list(value: Any) -> list[str]:
    if pd.isna(value):
        return []

    text = str(value).strip()
    text = text.strip("[]")
    text = text.replace("'", "")
    return [ticker.strip() for ticker in text.split(",") if ticker.strip()]


def standardize_prices_frame(frame: pd.DataFrame) -> pd.DataFrame:
    standardized = frame.copy()

    for column in PRICE_COLUMNS:
        if column not in standardized.columns:
            standardized[column] = pd.NA

    standardized = standardized[PRICE_COLUMNS]
    if not standardized.empty:
        standardized["date"] = pd.to_datetime(standardized["date"]).dt.strftime("%Y-%m-%d")

        inferred_ok_mask = standardized["close"].notna() | standardized["adj_close"].notna()
        standardized.loc[standardized["price_status"].isna() & inferred_ok_mask, "price_status"] = "ok"

    return standardized.sort_values(["date", "symbol"]).reset_index(drop=True)


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
        raise ValueError(f"No HTML tables were found at {url}.")

    constituents = tables[0].copy()
    constituents.columns = [_snake_case(column) for column in constituents.columns]
    if "symbol" not in constituents.columns:
        raise ValueError("The S&P 500 table does not contain a 'symbol' column.")

    constituents["symbol"] = constituents["symbol"].astype(str).str.strip()
    constituents = constituents[constituents["symbol"].ne("")].copy()
    constituents["symbol_yahoo"] = constituents["symbol"].map(normalize_symbol_for_yahoo)
    constituents["fetched_at_utc"] = pd.Timestamp.now(tz="UTC").isoformat()
    constituents = constituents.sort_values("symbol").reset_index(drop=True)

    preferred_order = [
        "symbol",
        "symbol_yahoo",
        "security",
        "gics_sector",
        "gics_sub_industry",
        "headquarters_location",
        "date_added",
        "cik",
        "founded",
        "fetched_at_utc",
    ]
    other_columns = [column for column in constituents.columns if column not in preferred_order]
    return constituents[preferred_order + other_columns]


def save_constituents(constituents: pd.DataFrame, output_path: Path) -> None:
    ensure_parent_directory(output_path)
    constituents.to_csv(output_path, index=False)


def upsert_component_snapshot(
    constituents: pd.DataFrame,
    output_path: Path,
    snapshot_date: dt.date,
) -> pd.DataFrame:
    ensure_parent_directory(output_path)
    snapshot = pd.DataFrame(
        {
            "date": [snapshot_date.isoformat()],
            "tickers": [",".join(constituents["symbol"].tolist())],
        }
    )

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


def load_required_price_pairs(
    component_history_path: Path,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
) -> pd.DataFrame:
    history = pd.read_csv(component_history_path, usecols=["date", "tickers"])
    history["date"] = pd.to_datetime(history["date"]).dt.date

    if start_date is not None:
        history = history[history["date"] >= start_date]
    if end_date is not None:
        history = history[history["date"] <= end_date]

    exploded = history.assign(symbol=history["tickers"].map(parse_ticker_list)).explode("symbol")
    exploded["symbol"] = exploded["symbol"].astype(str).str.strip()
    exploded = exploded[exploded["symbol"].ne("")].copy()
    exploded["date"] = pd.to_datetime(exploded["date"]).dt.strftime("%Y-%m-%d")
    exploded["symbol_yahoo"] = exploded["symbol"].map(normalize_symbol_for_yahoo)

    return exploded[["date", "symbol", "symbol_yahoo"]].drop_duplicates().reset_index(drop=True)


def reshape_yfinance_prices(
    raw_prices: pd.DataFrame,
    symbol_lookup: dict[str, str],
) -> pd.DataFrame:
    if raw_prices.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    if isinstance(raw_prices.columns, pd.MultiIndex):
        try:
            long_prices = raw_prices.stack(level=0, future_stack=True).reset_index()
        except TypeError:
            long_prices = raw_prices.stack(level=0).reset_index()

        index_columns = list(long_prices.columns[:2])
        long_prices = long_prices.rename(
            columns={
                index_columns[0]: "date",
                index_columns[1]: "symbol_yahoo",
            }
        )
    else:
        only_symbol = next(iter(symbol_lookup))
        long_prices = raw_prices.reset_index().copy()
        if "Date" not in long_prices.columns and "date" not in long_prices.columns:
            long_prices = long_prices.rename_axis("Date").reset_index()
        long_prices["symbol_yahoo"] = only_symbol

    long_prices = long_prices.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )
    long_prices["date"] = pd.to_datetime(long_prices["date"]).dt.strftime("%Y-%m-%d")
    long_prices["symbol"] = long_prices["symbol_yahoo"].map(symbol_lookup).fillna(
        long_prices["symbol_yahoo"]
    )

    for column in NUMERIC_PRICE_COLUMNS:
        if column not in long_prices.columns:
            long_prices[column] = pd.NA

    long_prices["price_status"] = "ok"
    long_prices["data_source"] = "yfinance"
    long_prices["updated_at_utc"] = pd.Timestamp.now(tz="UTC").isoformat()
    long_prices = long_prices[PRICE_COLUMNS]
    long_prices = long_prices.dropna(how="all", subset=NUMERIC_PRICE_COLUMNS)
    return long_prices.sort_values(["date", "symbol"]).reset_index(drop=True)


def download_batch_prices(
    batch_symbols: list[str],
    start_date: dt.date,
    end_date: dt.date,
) -> pd.DataFrame:
    if not batch_symbols:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    batch_lookup = {symbol: symbol for symbol in batch_symbols}
    raw_prices = yf.download(
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
    batch_prices = reshape_yfinance_prices(raw_prices, symbol_lookup=batch_lookup)

    downloaded_symbols = set(batch_prices["symbol_yahoo"].unique())
    missing_symbols = [symbol for symbol in batch_symbols if symbol not in downloaded_symbols]

    retry_frames: list[pd.DataFrame] = []
    for missing_symbol in missing_symbols:
        retry_raw_prices = yf.download(
            tickers=missing_symbol,
            start=start_date.isoformat(),
            end=(end_date + dt.timedelta(days=1)).isoformat(),
            interval="1d",
            auto_adjust=False,
            actions=False,
            group_by="ticker",
            progress=False,
            threads=False,
        )
        retry_prices = reshape_yfinance_prices(
            retry_raw_prices,
            symbol_lookup={missing_symbol: missing_symbol},
        )
        if not retry_prices.empty:
            retry_frames.append(retry_prices)

    if retry_frames:
        batch_prices = pd.concat([batch_prices, *retry_frames], ignore_index=True)
        batch_prices = batch_prices.drop_duplicates(subset=["date", "symbol_yahoo"], keep="last")

    return standardize_prices_frame(batch_prices)


def align_prices_to_required_pairs(
    required_pairs: pd.DataFrame,
    fetched_prices: pd.DataFrame,
) -> pd.DataFrame:
    if required_pairs.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    fetched_subset = standardize_prices_frame(fetched_prices)[
        ["date", "symbol_yahoo", *NUMERIC_PRICE_COLUMNS, "data_source", "updated_at_utc"]
    ].drop_duplicates(subset=["date", "symbol_yahoo"], keep="last")

    aligned = required_pairs.merge(
        fetched_subset,
        how="left",
        on=["date", "symbol_yahoo"],
    )
    aligned["date"] = pd.to_datetime(aligned["date"]).dt.strftime("%Y-%m-%d")

    has_price = aligned["close"].notna() | aligned["adj_close"].notna()
    weekday = pd.to_datetime(aligned["date"]).dt.dayofweek
    aligned["price_status"] = "missing_from_source"
    aligned.loc[weekday >= 5, "price_status"] = "non_trading_day"
    aligned.loc[has_price, "price_status"] = "ok"

    aligned.loc[aligned["price_status"] != "ok", "data_source"] = pd.NA
    aligned.loc[aligned["price_status"] != "ok", "updated_at_utc"] = pd.Timestamp.now(
        tz="UTC"
    ).isoformat()

    return standardize_prices_frame(aligned)


def load_existing_required_prices(
    prices_path: Path,
    required_pairs: pd.DataFrame,
) -> pd.DataFrame:
    if not prices_path.exists():
        return pd.DataFrame(columns=PRICE_COLUMNS)

    existing = standardize_prices_frame(pd.read_csv(prices_path))
    required_keys = required_pairs[["date", "symbol"]].drop_duplicates()
    existing = existing.merge(required_keys, how="inner", on=["date", "symbol"])
    return standardize_prices_frame(existing)


def build_missing_required_pairs(
    required_pairs: pd.DataFrame,
    existing_prices: pd.DataFrame,
) -> pd.DataFrame:
    existing_keys = existing_prices[["date", "symbol"]].drop_duplicates()
    missing = required_pairs.merge(existing_keys, how="left", on=["date", "symbol"], indicator=True)
    missing = missing[missing["_merge"] == "left_only"].drop(columns="_merge")
    return missing.sort_values(["date", "symbol"]).reset_index(drop=True)


def download_prices_for_required_pairs(
    required_pairs: pd.DataFrame,
    batch_size: int = DEFAULT_DOWNLOAD_BATCH_SIZE,
) -> pd.DataFrame:
    if required_pairs.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    unique_symbols = required_pairs["symbol_yahoo"].drop_duplicates().tolist()
    collected_frames: list[pd.DataFrame] = []

    for batch_symbols in chunked_symbols(unique_symbols, batch_size=batch_size):
        batch_required = required_pairs[required_pairs["symbol_yahoo"].isin(batch_symbols)].copy()
        batch_start = pd.to_datetime(batch_required["date"]).min().date()
        batch_end = pd.to_datetime(batch_required["date"]).max().date()
        fetched_prices = download_batch_prices(
            batch_symbols=batch_symbols,
            start_date=batch_start,
            end_date=batch_end,
        )
        aligned_prices = align_prices_to_required_pairs(
            required_pairs=batch_required,
            fetched_prices=fetched_prices,
        )
        collected_frames.append(aligned_prices)

    combined = pd.concat(collected_frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["date", "symbol"], keep="last")
    return standardize_prices_frame(combined)


def write_prices_dataset(prices_path: Path, prices: pd.DataFrame) -> pd.DataFrame:
    ensure_parent_directory(prices_path)
    prices = standardize_prices_frame(prices)
    prices.to_csv(prices_path, index=False)
    return prices


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
    component_history = upsert_component_snapshot(
        constituents=constituents,
        output_path=component_history_path,
        snapshot_date=run_date,
    )

    required_pairs = load_required_price_pairs(
        component_history_path=component_history_path,
        start_date=start_date,
        end_date=run_date,
    )

    existing_prices = load_existing_required_prices(
        prices_path=prices_path,
        required_pairs=required_pairs,
    )
    new_prices = pd.DataFrame(columns=PRICE_COLUMNS)

    if not skip_prices:
        missing_pairs = build_missing_required_pairs(
            required_pairs=required_pairs,
            existing_prices=existing_prices,
        )
        new_prices = download_prices_for_required_pairs(
            required_pairs=missing_pairs,
            batch_size=batch_size,
        )
    else:
        missing_pairs = build_missing_required_pairs(
            required_pairs=required_pairs,
            existing_prices=existing_prices,
        )

    if existing_prices.empty:
        all_prices = new_prices.copy()
    elif new_prices.empty:
        all_prices = existing_prices.copy()
    else:
        all_prices = pd.concat([existing_prices, new_prices], ignore_index=True)
    all_prices = all_prices.drop_duplicates(subset=["date", "symbol"], keep="last")
    all_prices = all_prices.merge(
        required_pairs[["date", "symbol"]].drop_duplicates(),
        how="inner",
        on=["date", "symbol"],
    )
    all_prices = write_prices_dataset(prices_path=prices_path, prices=all_prices)

    return {
        "run_date": run_date.isoformat(),
        "constituents_count": int(len(constituents)),
        "component_history_rows": int(len(component_history)),
        "required_price_rows": int(len(required_pairs)),
        "missing_pairs_before_download": int(len(missing_pairs)),
        "new_price_rows": int(len(new_prices)),
        "total_price_rows": int(len(all_prices)),
        "ok_price_rows": int((all_prices["price_status"] == "ok").sum()),
        "non_trading_rows": int((all_prices["price_status"] == "non_trading_day").sum()),
        "missing_source_rows": int((all_prices["price_status"] == "missing_from_source").sum()),
        "constituents_path": str(constituents_path),
        "component_history_path": str(component_history_path),
        "prices_path": str(prices_path),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh S&P 500 constituents and ensure the prices CSV covers all "
            "date/ticker pairs in the historical components file."
        ),
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional lower bound in YYYY-MM-DD for the historical components dates to include.",
    )
    parser.add_argument(
        "--as-of-date",
        default=None,
        help="Override the run date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--skip-prices",
        action="store_true",
        help="Only refresh the constituent files and skip the price backfill.",
    )
    parser.add_argument(
        "--prices-path",
        default=str(DEFAULT_PRICES_PATH),
        help="CSV output path for the normalized daily prices file.",
    )
    parser.add_argument(
        "--batch-size",
        default=DEFAULT_DOWNLOAD_BATCH_SIZE,
        type=int,
        help="Number of Yahoo tickers to request per batch.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    summary = update_sp500_dataset(
        start_date=parse_date(args.start_date),
        as_of_date=parse_date(args.as_of_date),
        prices_path=Path(args.prices_path),
        skip_prices=args.skip_prices,
        batch_size=args.batch_size,
    )

    print("S&P 500 pipeline refresh completed.")
    print(f"Run date: {summary['run_date']}")
    print(f"Constituents: {summary['constituents_count']}")
    print(f"Required price rows: {summary['required_price_rows']}")
    print(f"Missing pairs before download: {summary['missing_pairs_before_download']}")
    print(f"New rows written: {summary['new_price_rows']}")
    print(f"Rows with prices: {summary['ok_price_rows']}")
    print(f"Rows on non-trading days: {summary['non_trading_rows']}")
    print(f"Rows still missing from source: {summary['missing_source_rows']}")
    print(f"Total rows in price CSV: {summary['total_price_rows']}")
    print(f"Constituents CSV: {summary['constituents_path']}")
    print(f"Historical components CSV: {summary['component_history_path']}")
    print(f"Daily prices CSV: {summary['prices_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
