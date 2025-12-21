import pandas as pd
from pathlib import Path


from get_prices import download_prices


def create_csv():

    path = Path("sp500prices.csv")

    if path.is_file():
        print("CSV existe")
    else:
        print("CSV não existe")
        print("*********** Creating CSV file ***********")
        data = download_prices()
        data.to_csv(path)

        print("CSV file was created sucessfully")

    return None



if __name__ == "__main__":

    create_csv()