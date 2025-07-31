import sqlite3

import pandas as pd
import pytest

from cli.run_pipeline import PipelineOrchestrator


@pytest.fixture
def temp_db_and_universe(tmp_path):
    """
    A pytest fixture that creates a temporary, in-memory database
    and a temporary universe.csv file for testing.
    It yields the connection and file path, then handles cleanup.
    """
    # Setup: Create a temporary universe file
    universe_file = tmp_path / "universe.csv"
    universe_df = pd.DataFrame(
        [
            {"Ticker": "GOOG", "AssetType": "Equity"},
            {"Ticker": "ETH-USD", "AssetType": "Crypto"},
        ]
    )
    universe_df.to_csv(universe_file, index=False)

    # Setup: Create an in-memory SQLite database for speed
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE universe_metadata (
            Ticker TEXT PRIMARY KEY,
            AssetType TEXT,
            MarketCap REAL
        )
    """
    )
    # Pre-load the DB with one existing asset
    cursor.execute(
        "INSERT INTO universe_metadata (Ticker, AssetType) VALUES ('AAPL', 'Equity')"
    )
    conn.commit()

    # Yield the resources to the test
    yield conn, universe_file

    # Teardown: The connection is closed automatically
    conn.close()


def test_discover_universe(temp_db_and_universe, mocker):
    """
    Tests the _discover_universe method to ensure it correctly identifies
    new and existing assets from multiple sources.
    This is an INTEGRATION TEST because it uses a real (but temporary)
    database and file system component.
    """
    # 1. Arrange
    conn, universe_file = temp_db_and_universe

    # Mock the dynamic universe functions to return predictable data
    mocker.patch("cli.run_pipeline.get_sp500_tickers", return_value=["AAPL", "MSFT"])
    mocker.patch(
        "cli.run_pipeline.get_dow_jones_tickers", return_value=[]
    )  # Return empty to keep it simple
    mocker.patch("cli.run_pipeline.get_nasdaq100_tickers", return_value=[])
    mocker.patch(
        "cli.run_pipeline.get_top_100_crypto_tickers", return_value=["BTC-USD"]
    )

    # Override the settings to point to our temporary universe file
    mocker.patch("config.settings.UNIVERSE_FILE", universe_file)

    # 2. Act
    # We don't need a real requests.Session for this test
    orchestrator = PipelineOrchestrator(conn, session=None)
    orchestrator._discover_universe()

    # 3. Assert
    # Existing assets found in the DB
    assert orchestrator.db_equities == {"AAPL"}
    assert orchestrator.db_cryptos == set()

    # All assets discovered from all sources (dynamic + static file)
    assert orchestrator.universe_equities == {"AAPL", "MSFT", "GOOG"}
    assert orchestrator.universe_cryptos == {"BTC-USD", "ETH-USD"}

    # The final list of *new* assets to be processed
    assert orchestrator.new_equities == ["GOOG", "MSFT"]  # Note: sorted list
    assert orchestrator.new_cryptos == ["BTC-USD", "ETH-USD"]
