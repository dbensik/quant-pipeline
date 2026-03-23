import sqlite3
import pandas as pd
import pytest
from data_pipeline.time_series_normalizer import TimeSeriesNormalizer, SqlitePriceRepository
from config.settings import DB_PRICE_TABLE, DB_NORMALIZED_TABLE

@pytest.fixture
def db_connection():
    """Creates an in-memory sqlite connection for testing."""
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()

def test_normalize_all_tickers_success(db_connection):
    """Test that normalization correctly calculates percentage change from first value."""
    # 1. Setup: Create tables and insert dummy data
    df = pd.DataFrame({
        "Timestamp": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-01", "2023-01-02"]),
        "Ticker": ["AAPL", "AAPL", "GOOG", "GOOG"],
        "Close": [100.0, 110.0, 200.0, 100.0]
    })
    df.to_sql(DB_PRICE_TABLE, db_connection, index=False)
    
    # 2. Act: Run normalization using the repository
    repo = SqlitePriceRepository(db_connection)
    normalizer = TimeSeriesNormalizer(repo)
    normalizer.normalize_all_tickers()
    
    # 3. Assert: Check the normalized table
    result = pd.read_sql(f"SELECT * FROM {DB_NORMALIZED_TABLE}", db_connection)
    
    # Expected:
    # AAPL: 100 -> 100.0 (100/100*100)
    # AAPL: 110 -> 110.0 (110/100*100)
    # GOOG: 200 -> 100.0 (200/200*100)
    # GOOG: 100 -> 50.0  (100/200*100)
    
    aapl_res = result[result["Ticker"] == "AAPL"]["Normalized"].tolist()
    goog_res = result[result["Ticker"] == "GOOG"]["Normalized"].tolist()
    
    assert aapl_res == pytest.approx([100.0, 110.0])
    assert goog_res == pytest.approx([100.0, 50.0])

def test_normalize_division_by_zero(db_connection):
    """Test that normalization handles initial price of 0 gracefully."""
    # 1. Setup: Initial price is 0
    df = pd.DataFrame({
        "Timestamp": pd.to_datetime(["2023-01-01", "2023-01-02"]),
        "Ticker": ["BAD_TICKER", "BAD_TICKER"],
        "Close": [0.0, 10.0]
    })
    df.to_sql(DB_PRICE_TABLE, db_connection, index=False)
    
    # 2. Act
    repo = SqlitePriceRepository(db_connection)
    normalizer = TimeSeriesNormalizer(repo)
    normalizer.normalize_all_tickers()
    
    # 3. Assert
    result = pd.read_sql(f"SELECT * FROM {DB_NORMALIZED_TABLE}", db_connection)
    normalized_vals = result["Normalized"].tolist()
    
    # Expected: 0.0 for both because we force 0 if first value is 0
    assert normalized_vals == [0.0, 0.0]

def test_normalize_empty_table(db_connection):
    """Test that running on an empty table doesn't crash."""
    # 1. Setup: Create empty price table
    pd.DataFrame(columns=["Timestamp", "Ticker", "Close"]).to_sql(DB_PRICE_TABLE, db_connection, index=False)
    
    # 2. Act
    repo = SqlitePriceRepository(db_connection)
    normalizer = TimeSeriesNormalizer(repo)
    normalizer.normalize_all_tickers()
    
    # 3. Assert: Normalized table might not exist or be empty. Implementation returns early.
    pass # If no exception is raised, test passes.
