"""100% coverage on core/models/__init__.py — construction and validation."""
import pytest
from datetime import datetime, timezone

from core.models import Asset, MarketDataRecord, OHLCV, Timestamp


# ---------------------------------------------------------------------------
# Timestamp
# ---------------------------------------------------------------------------

class TestTimestamp:
    def test_default_tz(self):
        dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        ts = Timestamp(utc=dt)
        assert ts.tz == "UTC"

    def test_custom_tz(self):
        dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        ts = Timestamp(utc=dt, tz="America/New_York")
        assert ts.tz == "America/New_York"

    def test_utc_field(self):
        dt = datetime(2024, 6, 15, 12, 30, tzinfo=timezone.utc)
        ts = Timestamp(utc=dt)
        assert ts.utc == dt


# ---------------------------------------------------------------------------
# OHLCV
# ---------------------------------------------------------------------------

class TestOHLCV:
    def _ts(self) -> Timestamp:
        return Timestamp(utc=datetime(2024, 1, 1, tzinfo=timezone.utc))

    def test_construction(self):
        ohlcv = OHLCV(open=100.0, high=110.0, low=95.0, close=105.0, volume=1_000_000.0, timestamp=self._ts())
        assert ohlcv.open == 100.0
        assert ohlcv.high == 110.0
        assert ohlcv.low == 95.0
        assert ohlcv.close == 105.0
        assert ohlcv.volume == 1_000_000.0

    def test_timestamp_attached(self):
        ts = self._ts()
        ohlcv = OHLCV(open=1.0, high=2.0, low=0.5, close=1.5, volume=500.0, timestamp=ts)
        assert ohlcv.timestamp is ts

    def test_zero_volume(self):
        ohlcv = OHLCV(open=1.0, high=1.0, low=1.0, close=1.0, volume=0.0, timestamp=self._ts())
        assert ohlcv.volume == 0.0


# ---------------------------------------------------------------------------
# Asset
# ---------------------------------------------------------------------------

class TestAsset:
    def test_equity(self):
        asset = Asset(symbol="AAPL", asset_class="equity", source="yfinance")
        assert asset.symbol == "AAPL"
        assert asset.asset_class == "equity"
        assert asset.source == "yfinance"
        assert asset.metadata == {}

    def test_crypto(self):
        asset = Asset(symbol="BTC-USD", asset_class="crypto", source="coingecko")
        assert asset.asset_class == "crypto"
        assert asset.source == "coingecko"

    def test_metadata_default_is_independent(self):
        a1 = Asset(symbol="A", asset_class="equity", source="yfinance")
        a2 = Asset(symbol="B", asset_class="equity", source="yfinance")
        a1.metadata["key"] = "value"
        assert "key" not in a2.metadata

    def test_custom_metadata(self):
        asset = Asset(
            symbol="ETH",
            asset_class="crypto",
            source="coingecko",
            metadata={"vs_currency": "usd"},
        )
        assert asset.metadata["vs_currency"] == "usd"

    def test_all_asset_classes(self):
        for cls in ("equity", "crypto", "option", "future"):
            asset = Asset(symbol="X", asset_class=cls, source="yfinance")
            assert asset.asset_class == cls


# ---------------------------------------------------------------------------
# MarketDataRecord
# ---------------------------------------------------------------------------

class TestMarketDataRecord:
    def _make(self, symbol: str = "AAPL") -> MarketDataRecord:
        ts = Timestamp(utc=datetime(2024, 1, 2, tzinfo=timezone.utc))
        ohlcv = OHLCV(open=150.0, high=155.0, low=149.0, close=153.0, volume=5_000_000.0, timestamp=ts)
        asset = Asset(symbol=symbol, asset_class="equity", source="yfinance")
        return MarketDataRecord(asset=asset, ohlcv=ohlcv)

    def test_construction(self):
        rec = self._make()
        assert rec.asset.symbol == "AAPL"
        assert rec.ohlcv.close == 153.0

    def test_asset_and_ohlcv_are_linked(self):
        rec = self._make("MSFT")
        assert rec.asset.symbol == "MSFT"
        assert rec.ohlcv.timestamp.tz == "UTC"

    def test_equality(self):
        r1 = self._make("GOOG")
        r2 = self._make("GOOG")
        assert r1 == r2

    def test_inequality_different_symbol(self):
        assert self._make("AAPL") != self._make("MSFT")