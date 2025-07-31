import logging

import pandas as pd
import yfinance as yf

from .base_fetcher import BaseDataFetcher

logger = logging.getLogger(__name__)


class YFinanceFetcher(BaseDataFetcher):
    """A concrete data fetcher for Yahoo Finance."""

    def fetch_data(
        self,
        tickers: list[str],
        start_date: str,
        end_date: str,
        granularity: str = "1d",
    ) -> pd.DataFrame:
        logger.info(
            f"Fetching {granularity} data for {len(tickers)} tickers from yfinance..."
        )
        try:
            data_wide = yf.download(
                tickers=tickers,
                start=start_date,
                end=end_date,
                interval=granularity,
                progress=False,
                auto_adjust=True,
            )
            if data_wide.empty:
                return pd.DataFrame()

            # Reshape data from wide to long format
            data_long = (
                data_wide.stack(level=1, future_stack=True)
                .rename_axis(["Date", "Ticker"])
                .reset_index()
            )
            return data_long
        except Exception as e:
            logger.error(f"yfinance download failed: {e}")
            return pd.DataFrame()
