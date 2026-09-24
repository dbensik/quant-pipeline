import logging
from typing import List

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- Centralized Configuration Import ---
from config.settings import (
    URL_COINGECKO_API,
    DOWJONES_EXPECTED_COUNT,
    URL_DOWJONES_CONSTITUENTS,
    SP500_EXPECTED_RANGE,
    URL_NASDAQ100_WIKIPEDIA,
    URL_SP500_WIKIPEDIA,
)

logger = logging.getLogger(__name__)


def _implausible(name: str, tickers: list, low: int, high: int, source: str) -> bool:
    """
    True when a constituent count cannot be right, so the caller should discard.

    WHY THIS EXISTS. An EMPTY scrape is already safe — snapshot_universes refuses
    to record it, because claiming an index has no members is worse than
    recording nothing. A WRONG-BUT-NONEMPTY scrape is not: 28 Dow tickers or 120
    S&P names looks exactly like a healthy snapshot, gets written, and quietly
    corrupts point-in-time membership with nothing in the log.

    That distinction is the point. Discarding costs one index-day — bad, but
    visible, and now a non-zero exit. Recording a wrong list corrupts history
    silently, and snapshots cannot be backdated to repair it.

    Bounds are deliberately loose: they catch a changed source shape, not index
    turnover. A check that fires on legitimate reconstitution gets ignored, then
    deleted.
    """
    count = len(tickers)
    if low <= count <= high:
        return False
    expected = f"{low}" if low == high else f"{low}-{high}"
    logger.error(
        "%s scrape returned %d tickers, expected %s (%s). Refusing to report a "
        "constituent list that cannot be right.",
        name, count, expected, source,
    )
    return True


class DynamicUniverse:
    """
    A class to fetch dynamic stock and crypto universes from various web sources.
    It provides a single interface to access multiple ticker lists.
    """

    def __init__(self, timeout: int = 10):
        """
        Initializes the DynamicUniverse fetcher.

        Args:
            timeout (int): The timeout in seconds for web requests.
        """
        self.session = requests.Session()
        # Wikipedia returns 403 to requests' default User-Agent, so every
        # constituent fetch failed and returned [] — which callers could not
        # distinguish from "this index is empty". Identifying the client is
        # what their robot policy asks for.
        self.session.headers.update(
            {
                "User-Agent": (
                    "quant-pipeline/1.0 (research tool; "
                    "https://github.com/dbensik/quant-pipeline)"
                )
            }
        )
        self.timeout = timeout
        # A mapping of source keys to their respective fetch methods.
        # Aliases included deliberately. api/routers/ingest.py advertised
        # "dow_jones" and "top_100_crypto" — names taken from the private
        # method names rather than these keys — so those two sources silently
        # returned [] and surfaced as a 503 "came back empty".
        self._source_map = {
            "sp500": self._fetch_sp500_tickers,
            "dowjones": self._fetch_dow_jones_tickers,
            "dow_jones": self._fetch_dow_jones_tickers,
            "nasdaq100": self._fetch_nasdaq100_tickers,
            "crypto": self._fetch_top_100_crypto_tickers,
            "top_100_crypto": self._fetch_top_100_crypto_tickers,
        }

    def get_tickers(self, source: str) -> List[str]:
        """
        Public method to get tickers from a specified source.

        Args:
            source (str): The source to fetch from.
                          Supported: 'sp500', 'dowjones', 'nasdaq100', 'crypto'.

        Returns:
            List[str]: A list of ticker symbols, or an empty list on failure.
        """
        fetch_function = self._source_map.get(source.lower())
        if fetch_function:
            logger.info(f"Fetching dynamic universe for source: '{source}'...")
            return fetch_function()
        else:
            logger.warning(f"Unsupported dynamic universe source: '{source}'")
            return []

    def _fetch_sp500_tickers(self) -> List[str]:
        """Fetches the list of S&P 500 tickers from Wikipedia."""
        try:
            response = self.session.get(URL_SP500_WIKIPEDIA, timeout=self.timeout)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table", {"id": "constituents"})
            if not table:
                logger.error(
                    "Could not find the constituents table on the S&P 500 Wikipedia page."
                )
                return []

            tickers = [
                row.find("td").text.strip()
                for row in table.find_all("tr")[1:]
                if row.find("td")
            ]
            if _implausible(
                "S&P 500", tickers, *SP500_EXPECTED_RANGE, URL_SP500_WIKIPEDIA
            ):
                return []

            logger.info(f"Successfully fetched {len(tickers)} S&P 500 tickers.")
            return tickers
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch S&P 500 tickers: {e}")
            return []
        except Exception as e:
            logger.error(f"An error occurred while parsing S&P 500 tickers: {e}")
            return []

    def _fetch_dow_jones_tickers(self) -> List[str]:
        """
        Fetch the 30 DJIA constituents.

        NOT from Wikipedia. That page carried a components table until about
        2026-08-16 and then became a navbox with no table at all, so the scrape
        returned empty every morning for ten days. Because universe snapshots
        cannot be backdated, those ten index-days are permanently lost. The
        source moved rather than the parsing breaking, so a column tweak would
        not have helped.

        Returns [] on ANY problem, which the caller treats as a failed source
        and refuses to record — an empty snapshot would assert the index has no
        members, which is worse than no snapshot at all.
        """
        try:
            # storage_options, because pd.read_html makes its own request and
            # never sees self.session's headers.
            tables = pd.read_html(
                URL_DOWJONES_CONSTITUENTS, storage_options=self.session.headers
            )
            dow_table = next((tbl for tbl in tables if "Symbol" in tbl.columns), None)
            if dow_table is None:
                logger.error(
                    "Could not find a table with a 'Symbol' column at %s. The "
                    "source page has changed shape — this needs a human, not a "
                    "retry.",
                    URL_DOWJONES_CONSTITUENTS,
                )
                return []

            tickers = [
                str(ticker).split(":")[-1].strip().upper()
                for ticker in dow_table["Symbol"].tolist()
                if str(ticker).strip() and str(ticker).lower() != "nan"
            ]

            # The Dow is 30 stocks by definition, so anything else means the
            # scrape picked up the wrong table or a partial one. A SHORT read is
            # the dangerous case: unlike an empty result it looks like a healthy
            # snapshot, and would quietly drop constituents from point-in-time
            # membership with nothing to indicate it.
            if _implausible(
                "Dow Jones", tickers,
                DOWJONES_EXPECTED_COUNT, DOWJONES_EXPECTED_COUNT,
                URL_DOWJONES_CONSTITUENTS,
            ):
                return []

            logger.info(f"Successfully fetched {len(tickers)} Dow Jones tickers.")
            return tickers
        except Exception as e:
            logger.error(f"Could not fetch Dow Jones tickers: {e}")
            return []

    def _fetch_nasdaq100_tickers(self) -> List[str]:
        """Scrapes the Wikipedia page for NASDAQ-100 constituents."""
        try:
            tables = pd.read_html(
                URL_NASDAQ100_WIKIPEDIA, storage_options=self.session.headers
            )
            nasdaq_table = next(
                (tbl for tbl in tables if "Ticker" in tbl.columns), None
            )
            if nasdaq_table is None:
                logger.error(
                    "Could not find a table with 'Ticker' column for NASDAQ-100."
                )
                return []

            tickers = nasdaq_table["Ticker"].tolist()
            logger.info(f"Successfully fetched {len(tickers)} NASDAQ-100 tickers.")
            return tickers
        except Exception as e:
            logger.error(f"Could not fetch NASDAQ-100 tickers: {e}")
            return []

    def _coingecko_get(self, params: dict):
        """
        One CoinGecko call, retrying a 429 with exponential backoff.

        A throttled call must NOT fail soft. `fetch_crypto_references` swallowing
        a 429 is what produced a bad audit on 2026-09-20: the reference set came
        back short, so `mantle-staked-ether` and `the-open-network` read as
        coins that do not exist, and both symbols were recorded UNVERIFIABLE
        when they are in fact wrong assets. "We were throttled" and "this coin
        is not in the reference set" must never look the same.
        """
        import time

        from config.settings import (
            COINGECKO_MAX_RETRIES,
            COINGECKO_THROTTLE_BACKOFF_SECONDS,
        )

        wait = COINGECKO_THROTTLE_BACKOFF_SECONDS
        for attempt in range(1, COINGECKO_MAX_RETRIES + 1):
            response = self.session.get(
                URL_COINGECKO_API, params=params, timeout=self.timeout
            )
            if response.status_code != 429:
                response.raise_for_status()
                return response.json()
            if attempt == COINGECKO_MAX_RETRIES:
                break
            logger.warning(
                "CoinGecko throttled (429); waiting %.0fs (attempt %d/%d)",
                wait, attempt, COINGECKO_MAX_RETRIES,
            )
            time.sleep(wait)
            wait *= 2
        raise requests.exceptions.RequestException(
            f"CoinGecko still throttling after {COINGECKO_MAX_RETRIES} attempts"
        )

    def fetch_crypto_references(self, pages: int = 2) -> List["CoinReference"]:
        """
        The top coins WITH their stable ids, names and prices.

        A sibling of `_fetch_top_100_crypto_tickers`, not a replacement: that
        method returns List[str] and `get_tickers()` feeds
        `scripts/snapshot_universes.py`, a daily job that now exits non-zero on
        partial failure. Changing its shape to fix an identity bug would risk
        the point-in-time membership record, which cannot be backdated.

        WHY THIS EXISTS. The symbol-only path throws away exactly the fields
        that identify a coin. CoinGecko's `id` ("mantle") is stable; its
        SYMBOL ("mnt") is not unique across providers, and Yahoo's MNT-USD is
        a micro-cap called MINTY. See core/crypto_identity.py.

        `pages` is the reference breadth, 100 coins each. Two pages leaves 26
        of our 99 assets with no reference at all — mostly liquid-staking
        derivatives sitting below the top 200 — and those can only be recorded
        as unverified. Widen it to shrink that bucket.
        """
        from core.crypto_identity import CoinReference

        import time

        from config.settings import COINGECKO_REQUEST_DELAY_SECONDS

        references: List[CoinReference] = []
        for page in range(1, pages + 1):
            if page > 1:
                # The free tier throttles, and a throttled page does not raise
                # loudly — it just shortens the reference set, which downstream
                # reads as "this coin does not exist".
                time.sleep(COINGECKO_REQUEST_DELAY_SECONDS)
            params = {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 100,
                "page": page,
                "sparkline": "false",
            }
            try:
                data = self._coingecko_get(params)
            except requests.exceptions.RequestException as e:
                logger.error("CoinGecko page %d failed: %s", page, e)
                break
            if not data:
                break
            for item in data:
                symbol = (item.get("symbol") or "").upper()
                coin_id = item.get("id")
                if not symbol or not coin_id:
                    continue
                references.append(
                    CoinReference(
                        coingecko_id=coin_id,
                        symbol=symbol,
                        name=item.get("name") or "",
                        price=item.get("current_price"),
                    )
                )
        logger.info("Fetched %d crypto references.", len(references))
        return references

    def fetch_crypto_references_by_id(self, coin_ids) -> List["CoinReference"]:
        """
        References for specific CoinGecko ids, bypassing market-cap paging.

        Needed because `fetch_crypto_references` walks /coins/markets, which is
        ORDERED BY market cap: a coin with `market_cap_rank = None` is on no
        page at all, so no amount of paging reaches it. Measured 2026-09-20 —
        `mantle-staked-ether` is unranked, and `the-open-network` now carries
        the symbol GRAM after Toncoin's rename, so neither is findable by
        symbol however deep you page.
        """
        from core.crypto_identity import CoinReference

        import time

        from config.settings import COINGECKO_REQUEST_DELAY_SECONDS

        coin_ids = list(coin_ids)
        if not coin_ids:
            return []
        time.sleep(COINGECKO_REQUEST_DELAY_SECONDS)
        try:
            data = self._coingecko_get(
                {"vs_currency": "usd", "ids": ",".join(coin_ids)}
            )
        except requests.exceptions.RequestException as e:
            logger.error("CoinGecko id lookup failed: %s", e)
            return []
        return [
            CoinReference(
                coingecko_id=item["id"],
                symbol=(item.get("symbol") or "").upper(),
                name=item.get("name") or "",
                price=item.get("current_price"),
            )
            for item in data
            if item.get("id")
        ]

    def fetch_crypto_bounds_by_id(self, coin_ids) -> List["CoinBounds"]:
        """
        All-time low and high for specific CoinGecko ids, in ONE request.

        `/coins/markets` carries `atl` and `ath` alongside the price and
        accepts up to 250 ids, so the whole crypto universe costs a single
        call rather than one per coin — which matters, because this API
        throttles hard and a per-coin loop would spend most of its time in
        backoff.
        """
        from core.price_bounds import CoinBounds

        coin_ids = [c for c in coin_ids if c]
        if not coin_ids:
            return []
        out: List[CoinBounds] = []
        for start in range(0, len(coin_ids), 250):
            batch = coin_ids[start : start + 250]
            try:
                data = self._coingecko_get(
                    {"vs_currency": "usd", "ids": ",".join(batch), "per_page": 250}
                )
            except requests.exceptions.RequestException as e:
                logger.error("CoinGecko bounds lookup failed: %s", e)
                continue
            for item in data:
                low, high = item.get("atl"), item.get("ath")
                if not item.get("id") or not low or not high or low <= 0:
                    continue
                out.append(
                    CoinBounds(coingecko_id=item["id"], low=float(low), high=float(high))
                )
        return out

    def _fetch_top_100_crypto_tickers(self) -> List[str]:
        """
        Fetches the top 100 cryptocurrencies by market cap from CoinGecko.

        RETURNS SYMBOLS ONLY, and that is the known weakness: a symbol is not
        an identifier across providers, so `SYM-USD` may resolve at the price
        provider to an entirely different coin (22 of 99 did, audited
        2026-09-17). Callers that need to KNOW what they are registering must
        use `fetch_crypto_references` and check with `core/crypto_identity.py`.
        The signature stays as-is because `get_tickers()` and the daily
        snapshot job depend on it.
        """
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 100,
            "page": 1,
            "sparkline": "false",
        }
        try:
            response = self.session.get(
                URL_COINGECKO_API, params=params, timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            tickers = []
            for item in data:
                symbol = item.get("symbol", "").upper()
                if not symbol:
                    continue
                clean_symbol = symbol.split("-")[0].split(" ")[0]
                tickers.append(f"{clean_symbol}-USD")

            logger.info(f"Successfully fetched {len(tickers)} crypto tickers.")
            return tickers
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch crypto tickers from CoinGecko: {e}")
            return []
        except Exception as e:
            logger.error(f"An error occurred while parsing crypto tickers: {e}")
            return []
