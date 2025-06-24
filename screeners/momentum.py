# screeners/momentum.py

import sqlite3
import pandas as pd

def filter_by_momentum(db_path, table='price_data_normalized',
                       lookback_days=90, threshold=0.1):
    """
    Return list of tickers whose normalized return over lookback_days
    exceeds threshold.
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(
        "SELECT Date, Ticker, Close_Normalized AS Normalized "
        "FROM price_data_normalized",
        conn,
        parse_dates=["Date"]
    )
    df = df.set_index("Date")
    conn.close()

    # pivot to wide: dates × tickers
    wide = df.pivot(index='Date', columns='Ticker', values='Normalized')
    recent = wide.iloc[-lookback_days:]
    momentum = recent.iloc[-1] - recent.iloc[0]
    return momentum[momentum >= threshold].index.tolist()
