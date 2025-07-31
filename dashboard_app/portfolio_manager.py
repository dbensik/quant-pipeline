import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

# --- Project Imports ---
from config.settings import PORTFOLIOS_FILE_PATH

# --- Setup Logger ---
logger = logging.getLogger(__name__)


# --- Helper: Custom JSON Encoder for Pandas Objects ---
class PandasEncoder(json.JSONEncoder):
    """
    A custom JSON encoder to handle pandas DataFrames and Timestamps
    by converting them into a serializable format.
    """

    def default(self, obj):
        if isinstance(obj, pd.DataFrame):
            # Store DataFrame with a special key to identify it during decoding
            return {"__dataframe__": obj.to_json(orient="split", date_format="iso")}
        if isinstance(obj, (pd.Timestamp, pd.Timedelta)):
            return str(obj)
        # Let the base class default method raise the TypeError
        return super().default(obj)


# --- Helper: Custom Object Hook for Decoding ---
def portfolio_object_hook(dct: dict) -> Any:
    """
    A custom object_hook for json.load to reconstruct pandas DataFrames
    from our specific format.
    """
    if "__dataframe__" in dct:
        return pd.read_json(dct["__dataframe__"], orient="split")
    return dct


class PortfolioManager:
    """
    A class to manage the lifecycle (CRUD) of portfolios, saving them to
    and loading them from a JSON file. It handles complex pandas objects.
    """

    def __init__(self, file_path: str = PORTFOLIOS_FILE_PATH):
        """
        Initializes the PortfolioManager.

        Args:
            file_path: The path to the JSON file where portfolios are stored.
        """
        self.file_path = Path(file_path)
        self.portfolios = self.load()

    def load(self) -> Dict[str, Any]:
        """
        Loads the portfolios from the JSON file. If the file doesn't exist,
        it returns an empty dictionary. Returns None on a parsing error.
        """
        if not self.file_path.exists():
            logger.info(
                f"Portfolio file not found at {self.file_path}. Initializing with empty portfolios."
            )
            return {}
        try:
            with open(self.file_path, "r") as f:
                # Use the object_hook to reconstruct pandas objects on the fly
                return json.load(f, object_hook=portfolio_object_hook)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(
                f"Failed to load or parse portfolios from {self.file_path}: {e}"
            )
            # Return None to signal a failure to the caller
            return None

    def save(self):
        """Saves the current state of portfolios to the JSON file."""
        try:
            with open(self.file_path, "w") as f:
                # Use the custom PandasEncoder to handle DataFrames
                json.dump(self.portfolios, f, indent=4, cls=PandasEncoder)
            logger.info(f"Successfully saved portfolios to {self.file_path}")
        except (IOError, TypeError) as e:
            logger.error(f"Failed to save portfolios to {self.file_path}: {e}")

    def add_or_update(self, name: str, portfolio_data: Dict[str, Any]):
        """
        Adds a new portfolio or updates an existing one by name.

        Args:
            name: The unique name of the portfolio.
            portfolio_data: The dictionary containing portfolio details.
        """
        self.portfolios[name] = portfolio_data
        self.save()

    def delete(self, name: str):
        """
        Deletes a portfolio by its name.

        Args:
            name: The name of the portfolio to delete.
        """
        if name in self.portfolios:
            del self.portfolios[name]
            self.save()
            logger.info(f"Deleted portfolio: {name}")
        else:
            logger.warning(f"Attempted to delete non-existent portfolio: {name}")

    def get_all_portfolios(self) -> dict:
        """Returns all currently loaded portfolios."""
        return self.portfolios

    def save_portfolio(
        self,
        name: str,
        constituents: list,
        weights: Optional[dict] = None,
        trades: Optional[list] = None,
    ):
        """
        Saves a portfolio's definition, including constituents and optional weights.
        This is a more structured way to add/update a portfolio.
        """
        self.portfolios[name] = {
            "constituents": constituents,
            "weights": weights if weights is not None else {},  # Store weights
            "trades": trades if trades is not None else [],
        }
        self.save()  # Use the corrected save method


# --- Analysis Function (Moved out of the Manager class) ---


def calculate_open_positions_pl(
    backtest_results: Dict[str, Any], latest_price_data: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Calculates the Profit/Loss for positions that are still open at the
    end of a backtest, based on the latest available price data.
    """
    # This function's logic is sound, but it belongs in an analysis or
    # reporting module, not inside the PortfolioManager.
    # Keeping the implementation the same as your original.
    open_positions = []

    for ticker, result in backtest_results.items():
        portfolio = result.get("portfolio")
        if portfolio is None or portfolio.empty:
            logger.debug(f"No portfolio data for {ticker}, skipping P/L calculation.")
            continue

        last_position_row = portfolio.iloc[-1]
        position_size = last_position_row["position"]
        if position_size == 0:
            continue

        if ticker not in latest_price_data or latest_price_data[ticker].empty:
            logger.warning(f"No latest price data for open position in {ticker}.")
            continue

        trade_log = portfolio[portfolio["trades"] != 0]
        if trade_log.empty:
            logger.warning(f"Open position for {ticker}, but no trade log.")
            continue

        latest_price = latest_price_data[ticker]["Close"].iloc[-1]
        entry_price = trade_log.iloc[-1]["total"] / trade_log.iloc[-1]["position"]
        current_value = position_size * latest_price
        cost_basis = position_size * entry_price
        unrealized_pl = current_value - cost_basis
        unrealized_pl_pct = (unrealized_pl / cost_basis) if cost_basis != 0 else 0

        open_positions.append(
            {
                "Ticker": ticker,
                "Position Size": position_size,
                "Avg Entry Price": f"${entry_price:,.2f}",
                "Latest Price": f"${latest_price:,.2f}",
                "Cost Basis": f"${cost_basis:,.2f}",
                "Current Value": f"${current_value:,.2f}",
                "Unrealized P/L": f"${unrealized_pl:,.2f}",
                "Unrealized P/L (%)": f"{unrealized_pl_pct:.2%}",
            }
        )

    if not open_positions:
        return pd.DataFrame()

    return pd.DataFrame(open_positions).set_index("Ticker")
