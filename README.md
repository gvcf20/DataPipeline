# DataPipeline

Repository for online data mining.

## S&P 500 pipeline

The repository now includes a daily S&P 500 pipeline in [sp500](/Users/gabri/Classes/Research/DataPipeline/sp500) that uses only free public sources:

- Wikipedia for the latest S&P 500 constituent table
- Yahoo Finance through `yfinance` for daily OHLCV prices

### What the pipeline updates

- [tickers/sp500/sp500_constituents.csv](/Users/gabri/Classes/Research/DataPipeline/tickers/sp500/sp500_constituents.csv): latest constituent snapshot
- [tickers/sp500/sp_500_historical_components.csv](/Users/gabri/Classes/Research/DataPipeline/tickers/sp500/sp_500_historical_components.csv): one row per day with the constituent list for that day
- [sp500/sp500_daily_prices.csv](/Users/gabri/Classes/Research/DataPipeline/sp500/sp500_daily_prices.csv): normalized price table with one row for every `(date, ticker)` pair in the historical components file

The price file is driven by [tickers/sp500/sp_500_historical_components.csv](/Users/gabri/Classes/Research/DataPipeline/tickers/sp500/sp_500_historical_components.csv). On each run, the pipeline explodes that file into all required `(date, ticker)` pairs and fills any missing rows in the prices CSV.

Rows where Yahoo Finance returns a quote are marked with `price_status=ok`. Weekend rows are marked `non_trading_day`. If a symbol/date pair is still unavailable from the free source, the row is still written with `price_status=missing_from_source` so the CSV keeps complete coverage against the historical-components file.

### Run manually

```bash
python3 -m sp500
```

Useful options:

- `--start-date YYYY-MM-DD`: optional lower bound for the historical-components dates to include
- `--as-of-date YYYY-MM-DD`: run the refresh for a specific date
- `--skip-prices`: refresh only the constituent files
- `--prices-path PATH`: write the normalized daily prices CSV to a custom location
- `--batch-size N`: number of Yahoo tickers to request per batch

### Schedule it daily

Example `cron` entry to run every weekday at 18:00:

```cron
0 18 * * 1-5 cd /Users/gabri/Classes/Research/DataPipeline && /usr/bin/python3 -m sp500
```

If you use the project virtual environment, replace `/usr/bin/python3` with the interpreter from `venv/bin/python`.
