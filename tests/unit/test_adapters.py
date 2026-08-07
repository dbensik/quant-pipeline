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
