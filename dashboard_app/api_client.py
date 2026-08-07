"""
dashboard_app/api_client.py

The dashboard's single door to the quant-pipeline REST API.

Phase 3's exit gate is "Streamlit has no direct pipeline imports". This client
is what replaces them: the strategy catalogue and single-symbol backtests now
come over HTTP instead of by importing alpha_models and backtesting directly.

Deliberately NOT covered here (no endpoint exists yet — see the Phase 3 task
note for the remaining delta): screeners, statistical analysis / PCA, portfolio
optimisation, risk metrics, and multi-asset portfolio backtests. Those still
run in-process.

Phase 3 — FastAPI routers for the React UI
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st  # used by ApiClient's error reporting below

from config.settings import QUANT_API_BASE_URL, QUANT_API_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ApiClient — pre-existing results-file client (restored)
# ---------------------------------------------------------------------------
# NOTE: this predates Phase 3 and is a different concern from QuantApiClient
# below — it loads *saved analysis files*, not market data. It is used by the
# sidebar's "API Mode" data source (ui_components/sidebar.py).
#
# CAVEAT: it calls /results and /results/{filename}, which api/main.py does NOT
# implement — so "API Mode" currently returns an error. Left as-is rather than
# silently deleted; either add those endpoints or drop the sidebar option.

class ApiClient:
    """
    Client for interacting with the Quant Pipeline API.
    Handles fetching results and deserializing them back into usable Python objects.
    """
    
    def __init__(self, base_url: str = "http://127.0.0.1:8001"):
        self.base_url = base_url.rstrip("/")

    def get_result_files(self) -> List[str]:
        """Fetches the list of available result files from the API."""
        try:
            response = requests.get(f"{self.base_url}/results", timeout=5)
            if response.status_code == 200:
                return response.json()
            else:
                st.error(f"API Error: {response.status_code} - {response.text}")
                return []
        except requests.exceptions.RequestException as e:
            st.error(f"Could not connect to API at {self.base_url}: {e}")
            return []

    def get_result_data(self, filename: str) -> Optional[Any]:
        """Fetches and deserializes a specific result file."""
        try:
            response = requests.get(f"{self.base_url}/results/{filename}", timeout=10)
            if response.status_code == 200:
                data = response.json()
                return self._deserialize_data(data)
            else:
                st.error(f"API Error: {response.status_code} - {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            st.error(f"Could not connect to API: {e}")
            return None

    def _deserialize_data(self, data: Any) -> Any:
        """
        Recursively converts JSON structures back into Pandas objects where appropriate.
        Expects DataFrames to be serialized with orient='split'.
        """
        if isinstance(data, dict):
            # Check if this dict represents a serialized DataFrame
            # Pandas 'split' orientation has 'index', 'columns', 'data' keys (and sometimes 'name')
            if all(k in data for k in ("index", "columns", "data")):
                try:
                    df = pd.DataFrame(
                        data=data["data"],
                        index=data["index"],
                        columns=data["columns"]
                    )
                    # Attempt to convert index to datetime if it looks like one
                    if "Timestamp" in str(df.index.name) or (len(df.index) > 0 and isinstance(df.index[0], str) and df.index[0].count("-") == 2):
                       try:
                           df.index = pd.to_datetime(df.index)
                       except:
                           pass
                    return df
                except Exception:
                    # If conversion fails, treat as normal dict
                    pass
            
            # Recursive step for normal dicts
            return {k: self._deserialize_data(v) for k, v in data.items()}
        
        elif isinstance(data, list):
            return [self._deserialize_data(v) for v in data]
        
        return data


# ---------------------------------------------------------------------------
# QuantApiClient — Phase 3 market-data / strategy / backtest client
# ---------------------------------------------------------------------------

class ApiUnavailable(RuntimeError):
    """The API could not be reached or returned an error the caller must surface."""


# Strategy catalogue cache, keyed by base URL. Module level rather than
# per-instance because Streamlit reruns the whole script on every widget
# interaction and rebuilds the client each time — an instance cache would never
# be hit, refetching the catalogue on every keystroke. Call
# get_strategies(refresh=True) after registering a new strategy.
_STRATEGY_CACHE: Dict[str, List[Dict[str, Any]]] = {}


class QuantApiClient:
    """
    Thin, synchronous client. Streamlit reruns top to bottom on every
    interaction, so an async client would buy nothing here.

    Read methods degrade to empty results and log, matching the dashboard's
    existing behaviour. run_backtest raises ApiUnavailable instead, because a
    silently empty backtest looks like "the strategy made no trades" rather
    than "the request failed" — a wrong answer is worse than an error.
    """

    def __init__(
        self,
        base_url: str = QUANT_API_BASE_URL,
        timeout: float = QUANT_API_TIMEOUT_SECONDS,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    def is_available(self) -> bool:
        """True when the API answers its readiness probe (DB reachable included)."""
        try:
            resp = self._session.get(
                f"{self.base_url}/api/v1/health/ready", timeout=min(self.timeout, 5)
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False

    # ------------------------------------------------------------------
    # Strategies
    # ------------------------------------------------------------------

    def get_strategies(
        self, input_contract: Optional[str] = None, refresh: bool = False
    ) -> List[Dict[str, Any]]:
        """
        The strategy catalogue, including each strategy's parameter schema.

        Cached process-wide by base URL (see _STRATEGY_CACHE) — the catalogue
        only changes when the server restarts. Pass refresh=True to reload it.
        """
        cached = _STRATEGY_CACHE.get(self.base_url)
        if cached is None or refresh:
            try:
                resp = self._session.get(
                    f"{self.base_url}/api/v1/strategies", timeout=self.timeout
                )
                resp.raise_for_status()
                cached = resp.json().get("strategies", [])
                _STRATEGY_CACHE[self.base_url] = cached
            except requests.RequestException as exc:
                logger.exception("Could not load strategy catalogue: %s", exc)
                return []

        if input_contract:
            return [
                s for s in cached if s.get("input_contract") == input_contract
            ]
        return list(cached)

    def get_strategy(self, strategy_id: str) -> Optional[Dict[str, Any]]:
        for spec in self.get_strategies():
            if spec["id"] == strategy_id:
                return spec
        return None

    # ------------------------------------------------------------------
    # Backtest
    # ------------------------------------------------------------------

    def run_backtest(
        self,
        symbol: str,
        strategy_id: str,
        start: str,
        end: str,
        params: Optional[Dict[str, Any]] = None,
        initial_capital: float = 100_000.0,
        transaction_cost: float = 0.001,
        seed: Optional[int] = 42,
    ) -> Tuple[pd.DataFrame, Dict[str, Any], pd.DataFrame, Optional[str]]:
        """
        Run one single-symbol backtest.

        Returns (portfolio, stats, trade_log, caveat) in the same shapes the
        in-process Backtester produced, so callers downstream are unchanged:
          portfolio — DatetimeIndex'd frame with total/cash/holdings/position/
                      signal/returns
          stats     — the metrics dict
          trade_log — one row per fill
          caveat    — non-null when the strategy is known to be unsound

        `returns` is recomputed here rather than sent over the wire: it is
        exactly total.pct_change(), so transmitting it would just be a second
        copy that could disagree with the first.
        """
        payload = {
            "symbol": symbol,
            "strategy_id": strategy_id,
            "start": start,
            "end": end,
            "params": params or {},
            "initial_capital": initial_capital,
            "transaction_cost": transaction_cost,
            "seed": seed,
            "include_equity_curve": True,
            "include_trades": True,
        }
        try:
            resp = self._session.post(
                f"{self.base_url}/api/v1/backtest", json=payload, timeout=self.timeout
            )
        except requests.RequestException as exc:
            raise ApiUnavailable(
                f"Could not reach the API at {self.base_url}: {exc}"
            ) from exc

        if resp.status_code >= 400:
            try:
                detail = resp.json().get("detail", resp.text)
            except ValueError:
                detail = resp.text
            raise ApiUnavailable(f"Backtest failed ({resp.status_code}): {detail}")

        body = resp.json()
        curve = body.get("equity_curve", [])
        if curve:
            portfolio = pd.DataFrame(curve)
            portfolio["time"] = pd.to_datetime(portfolio["time"])
            portfolio = portfolio.set_index("time")
            portfolio.index.name = "Timestamp"
            # Match Backtester.run(): first bar has no prior value, so 0 not NaN.
            portfolio["returns"] = portfolio["total"].pct_change().fillna(0)
        else:
            portfolio = pd.DataFrame(
                columns=["total", "cash", "holdings", "position", "signal", "returns"]
            )

        trades = body.get("trades", [])
        trade_log = pd.DataFrame(trades) if trades else pd.DataFrame()

        return portfolio, body.get("metrics", {}), trade_log, body.get("caveat")

    # ------------------------------------------------------------------
    # Assets
    # ------------------------------------------------------------------

    def get_assets(
        self, asset_class: Optional[str] = None, search: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        if asset_class:
            params["asset_class"] = asset_class
        if search:
            params["search"] = search
        try:
            resp = self._session.get(
                f"{self.base_url}/api/v1/assets", params=params, timeout=self.timeout
            )
            resp.raise_for_status()
            return resp.json().get("assets", [])
        except requests.RequestException as exc:
            logger.exception("Could not list assets: %s", exc)
            return []
