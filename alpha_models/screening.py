def filter_by_momentum(db_path, lookback_days=90, threshold=0.1):
    """Return list of tickers whose normalized return over lookback_days > threshold."""
    # SELECT from price_data_normalized,
    # compute pct change, WHERE ...; return list.
