import pandas as pd
import datetime

def get_missing_business_days():

    csv_path = "sp_500_historical_components.csv"
    df = pd.read_csv(csv_path)
    print(f"Componentes históricos do S&P 500 carregados de '{csv_path}':")

    df['date'] = pd.to_datetime(df['date']).dt.date

    last_date = df['date'].max()
    print(f"Última data disponível: {last_date}")

    today = datetime.date.today()
    print(f"Data de hoje: {today}")

    business_days = pd.bdate_range(
        start=last_date + datetime.timedelta(days=1), 
        end=today
    ).date

    print("Dias úteis faltando:")
    print(business_days)

    return business_days


def main():

    missing_days = get_missing_business_days()

if __name__ == "__main__":
    main()
