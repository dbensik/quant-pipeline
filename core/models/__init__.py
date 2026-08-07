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


@dataclass
class MarketDataRecord:
    asset: Asset
    ohlcv: OHLCV