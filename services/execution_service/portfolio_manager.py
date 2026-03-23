import json
import os
from typing import Dict, Any

class PortfolioManager:
    """
    Manages multiple paper trading portfolios serialized to a JSON file.
    Structure:
    {
        "Default Portfolio": { "cash": 100000.0, "positions": {...} },
        "Dividend Growth": { "cash": 50000.0, "positions": {...} }
    }
    """
    def __init__(self, file_path: str = "portfolios.json"):
        self.file_path = file_path
        self.portfolios = self._load_portfolios()
        
    def _load_portfolios(self) -> Dict[str, Any]:
        """Load all portfolios from file."""
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, "r") as f:
                    data = json.load(f)
                    
                    # Migration: If loading old single-portfolio format
                    if "cash" in data and "positions" in data:
                        return {"Default Portfolio": data}
                    
                    return data
            except Exception:
                pass
        
        # Default initialization
        return {
            "Default Portfolio": {
                "cash": 100000.0,
                "positions": {}
            }
        }
        
    def _save_portfolios(self):
        """Persist all portfolios to file."""
        with open(self.file_path, "w") as f:
            json.dump(self.portfolios, f, indent=2)

    def get_all_portfolios(self) -> Dict[str, Any]:
        return self.portfolios

    def get_portfolio_state(self, name: str = None) -> Dict[str, Any]:
        """
        Returns state for a specific portfolio.
        If name is None, returns the first available portfolio (for backward compat).
        """
        if name is None:
            # Fallback to first available
            if not self.portfolios:
                return {}
            name = list(self.portfolios.keys())[0]
            
        return self.portfolios.get(name, {})

    def create_portfolio(self, name: str, initial_cash: float = 100000.0):
        if name in self.portfolios:
            return # Already exists
        self.portfolios[name] = {"cash": initial_cash, "positions": {}}
        self._save_portfolios()

    def delete_portfolio(self, name: str):
        if name in self.portfolios:
            del self.portfolios[name]
            self._save_portfolios()

    def add_or_update(self, name: str, data: dict):
        """Adds or updates a portfolio entry."""
        self.portfolios[name] = data
        self._save_portfolios()

    def execute_trade(self, symbol: str, action: str, quantity: float, price: float, portfolio_name: str = "Default Portfolio") -> str:
        """
        Updates specific portfolio based on trade execution.
        """
        if portfolio_name not in self.portfolios:
            # Fallback for easier MVP usage if default name used in UI doesn't match
            if len(self.portfolios) == 1:
                portfolio_name = list(self.portfolios.keys())[0]
            else:
                raise ValueError(f"Portfolio '{portfolio_name}' not found.")
            
        portfolio = self.portfolios[portfolio_name]
        
        if quantity <= 0:
            raise ValueError("Quantity must be positive")

        # Normalize action
        if action.upper() == "BUY":
            direction = 1.0
        elif action.upper() in ("SELL", "SHORT"):
            direction = -1.0
        else:
            raise ValueError(f"Unknown action: {action}")

        signed_quantity = quantity * direction
        transaction_amount = signed_quantity * price

        portfolio["cash"] -= transaction_amount

        pos = portfolio["positions"].get(symbol, {"quantity": 0.0, "average_price": 0.0})
        current_qty = pos["quantity"]
        current_avg = pos["average_price"]

        new_qty = current_qty + signed_quantity

        if current_qty == 0.0:
            new_avg = price
        elif (new_qty > 0 and current_qty < 0) or (new_qty < 0 and current_qty > 0):
            new_avg = price
        elif abs(new_qty) > abs(current_qty):
            total_cost = (abs(current_qty) * current_avg) + (abs(signed_quantity) * price)
            new_avg = total_cost / abs(new_qty)
        else:
            new_avg = current_avg

        if new_qty == 0.0:
            if symbol in portfolio["positions"]:
                del portfolio["positions"][symbol]
        else:
            pos["quantity"] = new_qty
            pos["average_price"] = new_avg
            portfolio["positions"][symbol] = pos

        self._save_portfolios()
        
        verb = "BOUGHT" if direction > 0 else "SOLD"
        return f"{verb} {quantity} {symbol} @ {price}"

    def generate_rebalancing_orders(self, portfolio_name: str, target_weights: Dict[str, float], current_prices: Dict[str, float]) -> list:
        """
        Generates a list of orders to rebalance the portfolio to the target weights.
        """
        portfolio = self.get_portfolio_state(portfolio_name)
        if not portfolio:
            return []
            
        # 1. Calculate Total Equity
        cash = portfolio.get("cash", 0.0)
        positions = portfolio.get("positions", {})
        
        equity = cash
        for symbol, pos in positions.items():
            price = current_prices.get(symbol)
            if price:
                equity += pos["quantity"] * price
        
        if equity <= 0:
            return []
            
        orders = []
        
        # 2. Process Target Weights
        # Use a copy of weights to modify if needed, or iterate
        all_tickers = set(target_weights.keys()) | set(positions.keys())
        
        for ticker in all_tickers:
            target_weight = target_weights.get(ticker, 0.0)
            current_price = current_prices.get(ticker)
            
            if not current_price or current_price <= 0:
                continue # Cannot rebalance without price
                
            # Current value
            pos = positions.get(ticker, {"quantity": 0.0})
            current_qty = pos["quantity"]
            current_val = current_qty * current_price
            
            # Target value
            target_val = equity * target_weight
            
            # Difference
            diff_val = target_val - current_val
            
            # Generate Order if difference is significant (e.g., > $10)
            if abs(diff_val) > 10.0:
                action = "BUY" if diff_val > 0 else "SELL"
                # Calculate quantity
                qty = int(abs(diff_val) / current_price)
                
                if qty > 0:
                    orders.append({
                        "ticker": ticker,
                        "action": action,
                        "quantity": qty,
                        "price": current_price,
                        "be_value": diff_val, # for display/debugging
                        "current_pct": current_val / equity,
                        "target_pct": target_weight
                    })
                    
        return orders
