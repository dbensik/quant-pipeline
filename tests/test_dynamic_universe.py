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
    # 1. Arrange: Set up the mock environment with realistic HTML.
    #
    # The first three rows are the ones under test: they cover both shapes the
    # real page uses, a linked symbol and a bare one. The filler exists purely
    # so the row COUNT is plausible — since 2026-08-25 a scrape returning far
    # fewer names than the index holds is discarded rather than trusted, and a
    # three-row S&P 500 was never a realistic fixture anyway.
    filler = "".join(
        f"<tr><td>FILL{i}</td><td>Filler {i}</td></tr>" for i in range(497)
    )
    mock_html_content = f"""
    <html>
        <body>
            <table id="constituents">
                <tbody>
                    <tr><th>Symbol</th><th>Security</th></tr>
                    <tr><td><a href="#">AAPL</a></td><td>Apple Inc.</td></tr>
                    <tr><td>MSFT</td><td>Microsoft</td></tr>
                    <tr><td><a href="#">AMZN</a></td><td>Amazon</td></tr>
                    {filler}
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
    assert len(tickers) == 500
    # Both markup shapes parse, which is what this test is actually for.
    assert tickers[:3] == ["AAPL", "MSFT", "AMZN"]


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


# ---------------------------------------------------------------------------
# S&P 500 plausibility range
# ---------------------------------------------------------------------------

from config.settings import SP500_EXPECTED_RANGE


def _sp500_html(n):
    rows = "".join(f"<tr><td>T{i}</td><td>Co {i}</td></tr>" for i in range(n))
    return (
        '<html><body><table id="constituents"><tbody>'
        "<tr><th>Symbol</th><th>Security</th></tr>" + rows + "</tbody></table></body></html>"
    )


def _mock_sp500(mocker, n):
    resp = Mock()
    resp.status_code = 200
    resp.text = _sp500_html(n)
    mocker.patch("requests.Session.get", return_value=resp)


def test_a_truncated_sp500_scrape_is_rejected(mocker, universe_fetcher):
    """
    The failure this exists for. An empty scrape is already refused by the
    caller; a HALF-READ one is not — 250 names looks like a healthy snapshot,
    gets recorded, and silently drops half the index from point-in-time
    membership. Membership cannot be backdated, so that is unrecoverable.
    """
    _mock_sp500(mocker, 250)
    assert universe_fetcher.get_tickers("sp500") == []


def test_an_implausibly_large_sp500_scrape_is_rejected(mocker, universe_fetcher):
    """Too many means a different table was matched — the Russell, say."""
    _mock_sp500(mocker, 900)
    assert universe_fetcher.get_tickers("sp500") == []


def test_the_real_world_count_is_comfortably_inside_the_range(
    mocker, universe_fetcher
):
    """
    503 is what the live page returns today: the index targets 500 COMPANIES
    but lists more SECURITIES, because a few have two share classes (GOOG/GOOGL,
    FOX/FOXA, NWS/NWSA). A check that rejected the actual current value would be
    deleted within a day, so pin it.
    """
    _mock_sp500(mocker, 503)
    assert len(universe_fetcher.get_tickers("sp500")) == 503


def test_the_range_bounds_themselves_are_inclusive(mocker, universe_fetcher):
    """Guards an off-by-one that would reject a legitimate edge count."""
    low, high = SP500_EXPECTED_RANGE
    for n in (low, high):
        _mock_sp500(mocker, n)
        assert len(universe_fetcher.get_tickers("sp500")) == n, f"rejected {n}"
    for n in (low - 1, high + 1):
        _mock_sp500(mocker, n)
        assert universe_fetcher.get_tickers("sp500") == [], f"accepted {n}"


# ---------------------------------------------------------------------------
# CoinGecko API key — header only, CoinGecko only
# ---------------------------------------------------------------------------
# From 2026-09-29 CoinGecko answered 403 to keyless requests from this machine
# and the top_100_crypto snapshot failed. The environment variable wins over
# .env, so these hold whether or not a real key is in .env.

from config.settings import COINGECKO_KEY_HEADER  # noqa: E402

KEY = "test-demo-key"


def _crypto_response():
    response = Mock()
    response.status_code = 200
    response.json.return_value = [{"symbol": "BTC", "name": "Bitcoin"}]
    return response


def test_the_key_is_sent_as_a_header_on_the_coingecko_call(mocker, monkeypatch, universe_fetcher):
    monkeypatch.setenv("COINGECKO_API_KEY", KEY)
    get = mocker.patch("requests.Session.get", return_value=_crypto_response())
    assert universe_fetcher.get_tickers("crypto") == ["BTC-USD"]
    kwargs = get.call_args.kwargs
    assert kwargs["headers"] == {COINGECKO_KEY_HEADER: KEY}


def test_the_key_never_appears_in_the_url_or_query(mocker, monkeypatch, universe_fetcher):
    """Request URLs are logged on failure — the 2026-09-29 403 was."""
    monkeypatch.setenv("COINGECKO_API_KEY", KEY)
    get = mocker.patch("requests.Session.get", return_value=_crypto_response())
    universe_fetcher.get_tickers("crypto")
    args, kwargs = get.call_args
    assert KEY not in str(args) and KEY not in str(kwargs.get("params"))


def test_no_key_means_no_header(mocker, monkeypatch, universe_fetcher):
    monkeypatch.setenv("COINGECKO_API_KEY", "")
    get = mocker.patch("requests.Session.get", return_value=_crypto_response())
    universe_fetcher.get_tickers("crypto")
    assert get.call_args.kwargs["headers"] == {}


def test_the_key_is_never_sent_to_the_index_scrapes(mocker, monkeypatch):
    """
    The same session scrapes Wikipedia; a session-wide header would leak it.
    Built AFTER the key is set, so a key attached at construction is caught.
    """
    monkeypatch.setenv("COINGECKO_API_KEY", KEY)
    fetcher = DynamicUniverse(timeout=5)
    _mock_sp500(mocker, 503)
    get = requests.Session.get
    fetcher.get_tickers("sp500")
    assert get.called
    for call in get.call_args_list:
        assert KEY not in str(call)
    assert KEY not in str(dict(fetcher.session.headers))


def test_a_keyless_403_says_how_to_fix_it(mocker, monkeypatch, universe_fetcher, caplog):
    monkeypatch.setenv("COINGECKO_API_KEY", "")
    refused = Mock()
    refused.status_code = 403
    refused.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "403 Client Error: Forbidden", response=refused
    )
    mocker.patch("requests.Session.get", return_value=refused)
    with caplog.at_level("ERROR"):
        assert universe_fetcher.get_tickers("crypto") == []
    assert any("COINGECKO_API_KEY" in m for m in caplog.messages)
