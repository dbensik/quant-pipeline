from unittest.mock import patch

import pytest

# The class we are testing
from data_pipeline.universe_fetcher import UniverseFetcher


@pytest.fixture
def fetcher():
    """Provides a UniverseFetcher instance for testing."""
    return UniverseFetcher()


def test_crypto_normalization(fetcher):
    """Tests that various crypto formats are correctly normalized."""
    assert fetcher._normalize_ticker("bitcoin") == "BTC-USD"
    assert fetcher._normalize_ticker("ETH") == "ETH-USD"
    assert fetcher._normalize_ticker("solana") == "SOL-USD"
    assert fetcher._normalize_ticker("XRP-USD") == "XRP-USD"  # Already correct


def test_equity_tickers_remain_unchanged(fetcher):
    """Tests that standard equity tickers are not modified."""
    assert fetcher._normalize_ticker("AAPL") == "AAPL"
    assert fetcher._normalize_ticker("MSFT") == "MSFT"


@patch.object(UniverseFetcher, "_fetch_top_crypto")
def test_run_method_returns_unique_normalized_tickers(mock_fetch_crypto, fetcher):
    """
    Tests that the main run method correctly processes a raw list of tickers,
    normalizing them and removing duplicates.
    """
    # Simulate the messy data we get from the source
    mock_fetch_crypto.return_value = [
        "BTC-USD",
        "bitcoin",
        "ETH",
        "ethereum",
        "SOL",
        "AAPL",
    ]

    # Run the method for the crypto source
    result = fetcher.run(source="Top Crypto")

    # Assert the output is clean, sorted, and unique
    expected = ["AAPL", "BTC-USD", "ETH-USD", "SOL-USD"]
    assert result == expected
