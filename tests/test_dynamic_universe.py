from unittest.mock import Mock

import pytest
import requests

# The class we are testing
from data_pipeline.dynamic_universe import DynamicUniverse


@pytest.fixture
def universe_fetcher() -> DynamicUniverse:
    """Provides a DynamicUniverse instance for each test."""
    return DynamicUniverse(timeout=5)


def test_get_sp500_tickers_success(mocker, universe_fetcher):
    """
    Tests that get_tickers('sp500') correctly parses a mocked HTML response.
    """
    # 1. Arrange: Set up the mock environment with realistic HTML
    mock_html_content = """
    <html>
        <body>
            <table id="constituents">
                <tbody>
                    <tr><th>Symbol</th><th>Security</th></tr>
                    <tr><td><a href="#">AAPL</a></td><td>Apple Inc.</td></tr>
                    <tr><td>MSFT</td><td>Microsoft</td></tr>
                    <tr><td><a href="#">AMZN</a></td><td>Amazon</td></tr>
                </tbody>
            </table>
        </body>
    </html>
    """
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = mock_html_content
    # The method uses a session object, so we patch the session's get method
    mocker.patch("requests.Session.get", return_value=mock_response)

    # 2. Act: Call the public method on the class instance
    tickers = universe_fetcher.get_tickers("sp500")

    # 3. Assert: The test should now pass
    assert isinstance(tickers, list)
    assert len(tickers) == 3
    assert tickers == ["AAPL", "MSFT", "AMZN"]


def test_get_tickers_request_fails(mocker, universe_fetcher):
    """
    Tests that get_tickers returns an empty list if the web request fails.
    """
    # 1. Arrange: Mock the 'get' method to raise a connection error
    mocker.patch(
        "requests.Session.get", side_effect=requests.exceptions.RequestException
    )

    # 2. Act
    tickers = universe_fetcher.get_tickers("sp500")

    # 3. Assert
    assert tickers == []


def test_get_sp500_tickers_parsing_error(mocker, universe_fetcher):
    """
    Tests that the function returns an empty list if the HTML is valid
    but does not contain the expected table.
    """
    # 1. Arrange: HTML is missing the <table id="constituents">
    mock_html_content = "<html><body><h1>Page Not Found</h1></body></html>"
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = mock_html_content
    mocker.patch("requests.Session.get", return_value=mock_response)

    # 2. Act
    tickers = universe_fetcher.get_tickers("sp500")

    # 3. Assert
    assert tickers == []


def test_get_crypto_tickers_success(mocker, universe_fetcher):
    """
    Tests that get_tickers('crypto') correctly parses a mocked JSON response.
    """
    # 1. Arrange: Mock a JSON response from the CoinGecko API
    mock_json_data = [
        {"symbol": "BTC", "name": "Bitcoin"},
        {"symbol": "ETH", "name": "Ethereum"},
    ]
    mock_response = Mock()
    mock_response.status_code = 200
    # The .json() method of the response needs to be mocked
    mock_response.json.return_value = mock_json_data
    mocker.patch("requests.Session.get", return_value=mock_response)

    # 2. Act
    tickers = universe_fetcher.get_tickers("crypto")

    # 3. Assert
    assert tickers == ["BTC-USD", "ETH-USD"]


def test_get_unsupported_source(universe_fetcher):
    """
    Tests that an unsupported source returns an empty list.
    """
    # 1. Act
    tickers = universe_fetcher.get_tickers("unsupported_source")

    # 2. Assert
    assert tickers == []


# ---------------------------------------------------------------------------
# Dow Jones
# ---------------------------------------------------------------------------
# The DJIA scrape moved off Wikipedia on 2026-08-25. The page had carried a
# components table until roughly 2026-08-16, then became a navbox with no table
# at all — so this returned empty every morning for ten days and those
# index-days are permanently lost, because snapshots cannot be backdated.

import pandas as pd

DOW_30 = [
    "AAPL", "AMGN", "AMZN", "AXP", "BA", "CAT", "CRM", "CSCO", "CVX", "DIS",
    "GOOGL", "GS", "HD", "HON", "IBM", "JNJ", "JPM", "KO", "MCD", "MMM",
    "MRK", "MSFT", "NKE", "NVDA", "PG", "SHW", "TRV", "UNH", "V", "WMT",
]


def _dow_table(symbols):
    return [pd.DataFrame({"Company": [f"Co {s}" for s in symbols], "Symbol": symbols})]


def test_dow_jones_returns_all_thirty(mocker, universe_fetcher):
    mocker.patch("pandas.read_html", return_value=_dow_table(DOW_30))
    assert universe_fetcher.get_tickers("dow_jones") == DOW_30


def test_a_short_dow_scrape_is_rejected(mocker, universe_fetcher):
    """
    THE DANGEROUS CASE, and the reason for the count check.

    An EMPTY result is already handled — the caller refuses to record it. A
    SHORT result is worse: 28 tickers looks exactly like a healthy snapshot, so
    two constituents would silently vanish from point-in-time membership with
    nothing in the log to say so. The Dow is 30 by definition, so any other
    number means the wrong table was parsed.
    """
    mocker.patch("pandas.read_html", return_value=_dow_table(DOW_30[:28]))
    assert universe_fetcher.get_tickers("dow_jones") == []


def test_an_over_long_dow_scrape_is_rejected(mocker, universe_fetcher):
    """Too many means a different table was matched — equally not the Dow."""
    mocker.patch("pandas.read_html", return_value=_dow_table(DOW_30 + ["EXTRA"]))
    assert universe_fetcher.get_tickers("dow_jones") == []


def test_dow_jones_missing_symbol_column_is_empty_not_an_exception(
    mocker, universe_fetcher
):
    """
    Exactly what happened when the page changed: no 'Symbol' column anywhere.
    Must degrade to [] so the caller records nothing, rather than raising and
    taking the other indexes down with it.
    """
    mocker.patch(
        "pandas.read_html",
        return_value=[pd.DataFrame({"Year": [2026], "Closing value": [1.0]})],
    )
    assert universe_fetcher.get_tickers("dow_jones") == []


def test_dow_jones_blank_rows_are_dropped_before_counting(mocker, universe_fetcher):
    """
    A trailing NaN row would make 30 real tickers count as 31 and be rejected,
    turning a cosmetic parsing artifact into a failed index-day.
    """
    mocker.patch("pandas.read_html", return_value=_dow_table(DOW_30 + [float("nan")]))
    assert universe_fetcher.get_tickers("dow_jones") == DOW_30
