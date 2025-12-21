import pandas as pd
import yfinance as yf

def download_data():

    csv_path = "../tickers/sp500/sp_500_historical_components.csv"

    df = pd.read_csv(csv_path, parse_dates=['date'])
    print(f"Componentes do S&P 500 carregados de '{csv_path}'")
    df['tickers'] = df['tickers'].apply(lambda x: x.strip("[]").replace("'", "").split(','))

    df_expl = df.explode('tickers')

    df_expl['tickers'] = df_expl['tickers'].str.strip()

    all_tickers = df_expl['tickers'].unique().tolist()

    start_date = df['date'].iloc[0]
    end_date = df['date'].iloc[-1]

    all_prices = pd.DataFrame(yf.download(all_tickers, start=start_date, end=end_date))

    print(all_prices)
    return all_prices


if __name__ == "__main__":
    download_data()