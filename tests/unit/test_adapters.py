"""
Adapter tests — no external network calls.

Exit criteria verified here:
  1. fetch() always returns List[MarketDataRecord] (never a DataFrame)
  2. yf.download and requests.get are fully mocked
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from core.models import MarketDataRecord
from core.adapters import yfinance_adapter, coingecko_adapter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _multi_ticker_df() -> pd.DataFrame:
    """Simulate yf.download output for two tickers (MultiIndex columns)."""
    idx = pd.DatetimeIndex(["2024-01-02", "2024-01-03"], name="Date")
    cols = pd.MultiIndex.from_tuples(
        [
            ("Close",  "AAPL"), ("Close",  "MSFT"),
            ("High",   "AAPL"), ("High",   "MSFT"),
            ("Low",    "AAPL"), ("Low",    "MSFT"),
            ("Open",   "AAPL"), ("Open",   "MSFT"),
            ("Volume", "AAPL"), ("Volume", "MSFT"),
        ],
        names=["Price", "Ticker"],
    )
    data = [
        [150.0, 300.0, 155.0, 305.0, 148.0, 298.0, 149.0, 299.0, 1e6, 2e6],
        [152.0, 302.0, 156.0, 306.0, 150.0, 300.0, 151.0, 301.0, 1.1e6, 2.1e6],
    ]
    return pd.DataFrame(data, index=idx, columns=cols)


def _single_ticker_df() -> pd.DataFrame:
    """Simulate yf.download output for one ticker (flat columns)."""
    idx = pd.DatetimeIndex(["2024-01-02"], name="Date")
    return pd.DataFrame(
        {"Close": [150.0], "High": [155.0], "Low": [148.0], "Open": [149.0], "Volume": [1e6]},
        index=idx,
    )


def _coingecko_rows() -> list:
    """Simulate CoinGecko OHLC API response."""
    return [
        [1704153600000, 42000.0, 43000.0, 41000.0, 42500.0],
        [1704240000000, 42500.0, 44000.0, 42000.0, 43800.0],
    ]


# ---------------------------------------------------------------------------
# yfinance_adapter
# ---------------------------------------------------------------------------

class TestYFinanceAdapter:

    @patch("core.adapters.yfinance_adapter.yf.download")
    def test_returns_list_of_market_data_records(self, mock_dl):
        mock_dl.return_value = _multi_ticker_df()
        result = yfinance_adapter.fetch(["AAPL", "MSFT"], "2024-01-01", "2024-01-05")
        assert isinstance(result, list)
        assert all(isinstance(r, MarketDataRecord) for r in result)

    @patch("core.adapters.yfinance_adapter.yf.download")
    def test_no_dataframe_leaks(self, mock_dl):
        mock_dl.return_value = _multi_ticker_df()
        result = yfinance_adapter.fetch(["AAPL", "MSFT"], "2024-01-01", "2024-01-05")
        assert not isinstance(result, pd.DataFrame)

    @patch("core.adapters.yfinance_adapter.yf.download")
    def test_multi_ticker_record_count(self, mock_dl):
        mock_dl.return_value = _multi_ticker_df()
        result = yfinance_adapter.fetch(["AAPL", "MSFT"], "2024-01-01", "2024-01-05")
        # 2 dates × 2 tickers = 4 records
        assert len(result) == 4

    @patch("core.adapters.yfinance_adapter.yf.download")
    def test_multi_ticker_asset_fields(self, mock_dl):
        mock_dl.return_value = _multi_ticker_df()
        result = yfinance_adapter.fetch(["AAPL", "MSFT"], "2024-01-01", "2024-01-05")
        symbols = {r.asset.symbol for r in result}
        assert symbols == {"AAPL", "MSFT"}
        for r in result:
            assert r.asset.asset_class == "equity"
            assert r.asset.source == "yfinance"

    @patch("core.adapters.yfinance_adapter.yf.download")
    def test_multi_ticker_ohlcv_values(self, mock_dl):
        mock_dl.return_value = _multi_ticker_df()
        result = yfinance_adapter.fetch(["AAPL", "MSFT"], "2024-01-01", "2024-01-05")
        aapl_first = next(r for r in result if r.asset.symbol == "AAPL")
        assert aapl_first.ohlcv.close == 150.0
        assert aapl_first.ohlcv.open == 149.0
        assert aapl_first.ohlcv.volume == 1e6

    @patch("core.adapters.yfinance_adapter.yf.download")
    def test_single_ticker_no_multiindex(self, mock_dl):
        mock_dl.return_value = _single_ticker_df()
        result = yfinance_adapter.fetch(["AAPL"], "2024-01-01", "2024-01-03")
        assert len(result) == 1
        assert result[0].asset.symbol == "AAPL"

    @patch("core.adapters.yfinance_adapter.yf.download")
    def test_dot_in_ticker_sanitised(self, mock_dl):
        df = _single_ticker_df()
        mock_dl.return_value = df
        result = yfinance_adapter.fetch(["BRK.B"], "2024-01-01", "2024-01-03")
        # BRK.B → BRK-B passed to yfinance; symbol on record reflects yfinance name
        assert mock_dl.call_args.kwargs["tickers"] == ["BRK-B"]

    @patch("core.adapters.yfinance_adapter.yf.download")
    def test_empty_dataframe_returns_empty_list(self, mock_dl):
        mock_dl.return_value = pd.DataFrame()
        result = yfinance_adapter.fetch(["AAPL"], "2024-01-01", "2024-01-03")
        assert result == []

    @patch("core.adapters.yfinance_adapter.yf.download", side_effect=RuntimeError("network"))
    def test_exception_returns_empty_list(self, mock_dl):
        result = yfinance_adapter.fetch(["AAPL"], "2024-01-01", "2024-01-03")
        assert result == []

    @patch("core.adapters.yfinance_adapter.yf.download")
    def test_timestamp_is_utc_aware(self, mock_dl):
        mock_dl.return_value = _single_ticker_df()
        result = yfinance_adapter.fetch(["AAPL"], "2024-01-01", "2024-01-03")
        assert result[0].ohlcv.timestamp.utc.tzinfo is not None


# ---------------------------------------------------------------------------
# coingecko_adapter
# ---------------------------------------------------------------------------

class TestCoinGeckoAdapter:

    def _mock_response(self, json_data):
        resp = MagicMock()
        resp.json.return_value = json_data
        resp.raise_for_status.return_value = None
        return resp

    @patch("core.adapters.coingecko_adapter.requests.get")
    def test_returns_list_of_market_data_records(self, mock_get):
        mock_get.return_value = self._mock_response(_coingecko_rows())
        result = coingecko_adapter.fetch(["bitcoin"])
        assert isinstance(result, list)
        assert all(isinstance(r, MarketDataRecord) for r in result)

    @patch("core.adapters.coingecko_adapter.requests.get")
    def test_no_dataframe_leaks(self, mock_get):
        mock_get.return_value = self._mock_response(_coingecko_rows())
        result = coingecko_adapter.fetch(["bitcoin"])
        assert not isinstance(result, pd.DataFrame)

    @patch("core.adapters.coingecko_adapter.requests.get")
    def test_record_count(self, mock_get):
        mock_get.return_value = self._mock_response(_coingecko_rows())
        result = coingecko_adapter.fetch(["bitcoin"])
        assert len(result) == 2

    @patch("core.adapters.coingecko_adapter.requests.get")
    def test_asset_fields(self, mock_get):
        mock_get.return_value = self._mock_response(_coingecko_rows())
        result = coingecko_adapter.fetch(["bitcoin"])
        for r in result:
            assert r.asset.symbol == "BITCOIN"
            assert r.asset.asset_class == "crypto"
            assert r.asset.source == "coingecko"
            assert r.asset.metadata["vs_currency"] == "usd"

    @patch("core.adapters.coingecko_adapter.requests.get")
    def test_ohlcv_values(self, mock_get):
        mock_get.return_value = self._mock_response(_coingecko_rows())
        result = coingecko_adapter.fetch(["bitcoin"])
        assert result[0].ohlcv.open == 42000.0
        assert result[0].ohlcv.high == 43000.0
        assert result[0].ohlcv.low == 41000.0
        assert result[0].ohlcv.close == 42500.0
        assert result[0].ohlcv.volume == 0.0  # not provided by OHLC endpoint

    @patch("core.adapters.coingecko_adapter.requests.get")
    def test_timestamp_from_epoch_ms(self, mock_get):
        mock_get.return_value = self._mock_response(_coingecko_rows())
        result = coingecko_adapter.fetch(["bitcoin"])
        expected = datetime.fromtimestamp(1704153600000 / 1000, tz=timezone.utc)
        assert result[0].ohlcv.timestamp.utc == expected

    @patch("core.adapters.coingecko_adapter.requests.get")
    def test_multiple_coins(self, mock_get):
        mock_get.return_value = self._mock_response(_coingecko_rows())
        result = coingecko_adapter.fetch(["bitcoin", "ethereum"])
        # 2 coins × 2 rows each = 4 records; get called twice
        assert len(result) == 4
        assert mock_get.call_count == 2

    @patch("core.adapters.coingecko_adapter.requests.get", side_effect=RuntimeError("timeout"))
    def test_network_error_returns_empty_list(self, mock_get):
        result = coingecko_adapter.fetch(["bitcoin"])
        assert result == []

    @patch("core.adapters.coingecko_adapter.requests.get")
    def test_http_error_returns_empty_list(self, mock_get):
        resp = MagicMock()
        resp.raise_for_status.side_effect = Exception("429 Too Many Requests")
        mock_get.return_value = resp
        result = coingecko_adapter.fetch(["bitcoin"])
        assert result == []

    @patch("core.adapters.coingecko_adapter.requests.get")
    def test_empty_response_returns_empty_list(self, mock_get):
        mock_get.return_value = self._mock_response([])
        result = coingecko_adapter.fetch(["bitcoin"])
        assert result == []

    @patch("core.adapters.coingecko_adapter.requests.get")
    def test_custom_vs_currency(self, mock_get):
        mock_get.return_value = self._mock_response(_coingecko_rows())
        result = coingecko_adapter.fetch(["bitcoin"], vs_currency="eur")
        assert result[0].asset.metadata["vs_currency"] == "eur"
        call_params = mock_get.call_args.kwargs["params"]
        assert call_params["vs_currency"] == "eur"


# ---------------------------------------------------------------------------
# yfinance_adapter — served values and corporate actions (phase 3)
# ---------------------------------------------------------------------------

def _unadjusted_df() -> pd.DataFrame:
    """What yf.download(auto_adjust=False, actions=True) returns: MO around its
    2026-06-15 ex-date, $1.06 dividend, as served on 2026-09-27."""
    idx = pd.DatetimeIndex(["2026-06-12", "2026-06-15"], name="Date")
    cols = pd.MultiIndex.from_tuples(
        [(c, "MO") for c in ("Adj Close", "Close", "Dividends", "High", "Low",
                             "Open", "Stock Splits", "Volume")],
        names=["Price", "Ticker"],
    )
    data = [
        [69.7658, 71.94, 0.00, 72.10, 71.20, 71.50, 0.0, 8_409_100],
        [68.4960, 69.59, 1.06, 70.00, 69.10, 69.80, 0.0, 11_461_600],
    ]
    return pd.DataFrame(data, index=idx, columns=cols)


class TestYFinanceServedValues:
    @patch("core.adapters.yfinance_adapter.yf.download")
    def test_asks_for_unadjusted_prices_with_actions(self, mock_dl):
        mock_dl.return_value = _unadjusted_df()
        yfinance_adapter.fetch(["MO"], "2026-06-12", "2026-06-16")
        kwargs = mock_dl.call_args.kwargs
        assert kwargs["auto_adjust"] is False and kwargs["actions"] is True

    @patch("core.adapters.yfinance_adapter.yf.download")
    def test_ohlcv_is_still_the_adjusted_bar_and_served_is_the_raw_one(self, mock_dl):
        """
        Adjusted = served x Adj Close / Close, which is exactly auto_adjust=True
        (verified to the bit against Yahoo). Volume is never scaled.
        """
        mock_dl.return_value = _unadjusted_df()
        first = yfinance_adapter.fetch(["MO"], "2026-06-12", "2026-06-16")[0]
        ratio = 69.7658 / 71.94
        assert first.served.close == pytest.approx(71.94)
        assert first.ohlcv.close == pytest.approx(69.7658)
        assert first.ohlcv.open == pytest.approx(71.50 * ratio)
        assert first.ohlcv.volume == first.served.volume == 8_409_100

    @patch("core.adapters.yfinance_adapter.yf.download")
    def test_actions_are_reported_only_where_they_happened(self, mock_dl):
        mock_dl.return_value = _unadjusted_df()
        records = yfinance_adapter.fetch(["MO"], "2026-06-12", "2026-06-16")
        assert [(r.dividend, r.split_ratio) for r in records] == [(None, None), (1.06, None)]

    @patch("core.adapters.yfinance_adapter.yf.download")
    def test_every_record_of_a_fetch_shares_one_aware_fetch_time(self, mock_dl):
        mock_dl.return_value = _unadjusted_df()
        records = yfinance_adapter.fetch(["MO"], "2026-06-12", "2026-06-16")
        assert len({r.fetched_at for r in records}) == 1
        assert records[0].fetched_at.tzinfo is not None

    @patch("core.adapters.yfinance_adapter.yf.download")
    def test_a_frame_without_adj_close_is_taken_as_unadjusted(self, mock_dl):
        mock_dl.return_value = _single_ticker_df()
        record = yfinance_adapter.fetch(["AAPL"], "2024-01-02", "2024-01-03")[0]
        assert record.ohlcv.close == record.served.close == 150.0
        assert record.dividend is None and record.split_ratio is None


@patch("core.adapters.yfinance_adapter.yf.download", side_effect=RuntimeError("429"))
def test_a_failed_download_can_be_raised_instead_of_returned_empty(mock_dl):
    """A backfill must tell 'Yahoo has nothing' from 'the request failed'."""
    with pytest.raises(RuntimeError):
        yfinance_adapter.fetch(["MO"], "2026-01-01", "2026-02-01", raise_errors=True)
    assert yfinance_adapter.fetch(["MO"], "2026-01-01", "2026-02-01") == []


@patch("core.adapters.coingecko_adapter.requests.get")
def test_coingecko_adapter_sends_the_key_as_a_header(mock_get, monkeypatch):
    monkeypatch.setenv("COINGECKO_API_KEY", "test-demo-key")
    mock_get.return_value = MagicMock(status_code=200, json=MagicMock(return_value=_coingecko_rows()))
    coingecko_adapter.fetch(["bitcoin"], "2024-01-01", "2024-01-03")
    kwargs = mock_get.call_args.kwargs
    assert kwargs["headers"] == {"x-cg-demo-api-key": "test-demo-key"}
    assert "test-demo-key" not in str(mock_get.call_args.args) + str(kwargs.get("params"))
