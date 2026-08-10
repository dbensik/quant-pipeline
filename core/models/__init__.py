from dataclasses import dataclass, field
from datetime import datetime
from abc import ABC, abstractmethod
from typing import Optional


@dataclass
class Timestamp:
    utc: datetime
    tz: str = 'UTC'


@dataclass
class OHLCV:
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: Timestamp


@dataclass
class Asset:
    symbol: str
    asset_class: str  # 'equity' | 'crypto' | 'option' | 'future'
    source: str       # 'yfinance' | 'coingecko'
    metadata: dict = field(default_factory=dict)
    #: Set when the provider stopped serving this symbol. Carried on the domain
    #: object so `core.ingest` can skip it without a second query — before this
    #: existed the flag was write-only, and eleven dead symbols were re-fetched
    #: every run. Defaulted last so existing positional construction is unaffected.
    delisted_at: Optional[datetime] = None


@dataclass
class MarketDataRecord:
    asset: Asset
    ohlcv: OHLCV