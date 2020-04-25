import pandas as pd
import numpy as np
import fastparquet


def import_monthly_weather(fpath: str) -> pd.DataFrame:
    """
    Imports a monthly weather .csv into a DataFrame
    """
    pd.read_csv(fpath)


# if drill.is_active():

def main():
    import_monthly_weather(f"Data\weather.20160201.csv")