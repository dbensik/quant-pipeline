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
