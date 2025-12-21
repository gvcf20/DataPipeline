import pandas as pd




def read_csv_prices_table():



    prices = pd.read_csv("sp500prices.csv", low_memory=False)

    print(prices)

    return prices



if __name__ == "__main__":

    read_csv_prices_table()