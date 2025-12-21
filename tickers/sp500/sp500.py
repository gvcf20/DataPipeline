import requests
import pandas as pd
from io import StringIO

def get_sp500_table(url):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/117.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status() 
    dfs = pd.read_html(resp.text, header=0)
    if not dfs:
        raise ValueError("Nenhuma tabela encontrada na página.")
    df = dfs[0].rename(columns=str.lower)
    return df

def main():
    sp_500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies" 
    sp_500_constituents = get_sp500_table(sp_500_url)
    
    sp_500_constituents.to_csv("sp500_constituents.csv", index=False)
    print("Tabela S&P 500 salva em 'sp500_constituents.csv'.")

if __name__ == "__main__":
    main()
