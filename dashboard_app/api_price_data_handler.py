"""
dashboard_app/api_price_data_handler.py

Drop-in replacement for PriceDataHandler that reads prices over the Phase 3
REST API instead of opening SQLite directly.

This is the Phase 3 vertical slice on the consumer side. It deliberately
mirrors PriceDataHandler's public interface exactly — same two methods, same
argument names, same return shapes — so `dashboard.py` can swap one for the
other behind a flag with no other code change, and so the eventual cutover is
a deletion rather than a rewrite.

Enable with QUANT_USE_API=1 (see config/settings.py).

Phase 3 — FastAPI routers for the React UI
"""

import logging
from typing import Dict, List

import pandas as pd
import requests

from config.settings import (
    QUANT_API_BASE_URL,
    QUANT_API_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)


class ApiPriceDataHandler:
    """
    Fetches historical price data from the quant-pipeline REST API.

    Interface-compatible with PriceDataHandler. Errors are logged and returned
    as empty results rather than raised — matching PriceDataHandler's existing
    behaviour, so a dead API degrades the dashboard the same way a missing
    ticker already does instead of crashing it.
    """

    def __init__(
        self,
        base_url: str = QUANT_API_BASE_URL,
        timeout: float = QUANT_API_TIMEOUT_SECONDS,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        # Reuse one connection across the many per-ticker calls below.
        self._session = requests.Session()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _fetch_bars(self, ticker: str, start_date: str, end_date: str) -> List[dict]:
        """
        Return raw bar dicts for one ticker, or [] on any failure.

        NOTE: the API is one-symbol-per-request, while PriceDataHandler issued a
        single multi-ticker SQL query. For the tens of tickers the dashboard
        uses this is fine; a batch endpoint is the obvious optimisation if a
        screener ever pushes hundreds of symbols through here.
        """
        url = f"{self.base_url}/api/v1/ohlcv/{ticker}"
        try:
            resp = self._session.get(
                url,
                params={"start": start_date, "end": end_date},
                timeout=self.timeout,
            )
            if resp.status_code == 404:
                logger.warning("Ticker %s is not registered with the API.", ticker)
                return []
            resp.raise_for_status()
            return resp.json().get("bars", [])
        except requests.RequestException as exc:
            logger.exception("API request failed for %s: %s", ticker, exc)
            return []

    # ------------------------------------------------------------------
    # PriceDataHandler-compatible interface
    # ------------------------------------------------------------------

    def get_prices(
        self, tickers: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Close prices for a list of tickers.

        Returns a DataFrame indexed by date, one column per ticker — identical
        in shape to PriceDataHandler.get_prices.
        """
        if not tickers:
            return pd.DataFrame()

        frames = {}
        for ticker in tickers:
            bars = self._fetch_bars(ticker, start_date, end_date)
            if not bars:
                continue
            series = pd.Series(
                {pd.Timestamp(b["time"]): b["close"] for b in bars}, name=ticker
            )
            frames[ticker] = series

        if not frames:
            logger.warning(
                "No 'Close' price data found for tickers %s in the given date range.",
                tickers,
            )
            return pd.DataFrame()

        price_df = pd.DataFrame(frames)
        price_df.index.name = "Timestamp"
        return price_df

    def get_full_data_for_tickers(
        self, tickers: List[str], start_date: str, end_date: str
    ) -> Dict[str, pd.DataFrame]:
        """
        Full OHLCV per ticker, as a dict of DataFrames.

        Column names are capitalised (Open/High/Low/Close/Volume) to match the
        legacy SQLite schema that backtesting and screening already expect.
        """
        if not tickers:
            return {}

        out: Dict[str, pd.DataFrame] = {}
        for ticker in tickers:
            bars = self._fetch_bars(ticker, start_date, end_date)
            if not bars:
                continue
            df = pd.DataFrame(bars)
            df["Timestamp"] = pd.to_datetime(df["time"])
            df = df.set_index("Timestamp").drop(columns=["time"])
            df = df.rename(
                columns={
                    "open": "Open",
                    "high": "High",
                    "low": "Low",
                    "close": "Close",
                    "volume": "Volume",
                }
            )
            out[ticker] = df

        if not out:
            logger.warning(
                "No full OHLCV data found for tickers %s in the given date range.",
                tickers,
            )
        return out
