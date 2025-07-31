import uuid
from datetime import datetime

import pandas as pd
import streamlit as st

from dashboard_app.portfolio_manager import PortfolioManager
from dashboard_app.price_data_handler import PriceDataHandler


class PortfolioTab:
    """
    Renders the portfolio management tab, providing a full CRUD interface for
    trades and high-level analytics for the selected portfolio.
    """

    def __init__(
        self, price_handler: PriceDataHandler, portfolio_manager: PortfolioManager
    ):
        """
        Initializes the PortfolioTab with the required service managers.

        Args:
            price_handler: An instance of PriceHandler for price operations.
            portfolio_manager: An instance of PortfolioManager for portfolio file operations.
        """
        self.price_handler = price_handler
        self.portfolio_manager = portfolio_manager

    def render(self, selections: dict):
        """
        Main render method for the tab.

        Args:
            selections: A dictionary of user selections from the sidebar.
        """
        portfolio_name = selections.get("selected_portfolio_to_manage")

        if not portfolio_name:
            st.info(
                "📈 Select a portfolio from the sidebar to manage its trades and view analytics."
            )
            return

        st.header(f"Managing Portfolio: {portfolio_name}")

        # Load the portfolio data
        portfolio_data = self.portfolio_manager.portfolios.get(
            portfolio_name, {"trades": []}
        )
        trades = portfolio_data.get("trades", [])

        # --- 1. Render Portfolio Analytics ---
        self._render_portfolio_summary(trades)

        # --- 2. Render the Trade Editor and Add/Delete Forms ---
        with st.expander("Trade Management", expanded=False):
            self._render_trade_editor(portfolio_name, portfolio_data, trades)
            self._render_add_trade_form(portfolio_name, portfolio_data)

        # --- 3. Render the Delete Portfolio Section ---
        self._render_delete_portfolio(portfolio_name)

    def _render_portfolio_summary(self, trades: list):
        """Calculates and displays a summary of current holdings."""
        st.subheader("Portfolio Summary")

        if not trades:
            st.write("No trades found. Add a trade below to get started.")
            return

        # Calculate current holdings from the trade log
        holdings = {}
        for trade in trades:
            ticker = trade["ticker"]
            quantity = float(trade["quantity"])
            action = trade["action"]

            if action == "Buy":
                holdings[ticker] = holdings.get(ticker, 0) + quantity
            elif action == "Sell":
                holdings[ticker] = holdings.get(ticker, 0) - quantity

        # Filter out closed positions
        current_holdings = {ticker: qty for ticker, qty in holdings.items() if qty > 0}

        if not current_holdings:
            st.write("All positions are closed.")
            return

        # Fetch latest prices for current holdings
        tickers_list = list(current_holdings.keys())
        # latest_prices = self.db_manager.get_latest_prices(tickers_list)
        end_date = datetime.now()
        start_date = end_date - pd.Timedelta(days=7)

        price_df = self.price_handler.get_prices(
            tickers=tickers_list,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
        )
        latest_prices = {}
        if not price_df.empty:
            latest_prices = price_df.ffill().iloc[-1].to_dict()

        # Prepare data for display
        summary_data = []
        total_portfolio_value = 0
        for ticker, quantity in current_holdings.items():
            price = latest_prices.get(ticker)
            market_value = quantity * price if price is not None else 0
            total_portfolio_value += market_value
            summary_data.append(
                {
                    "Ticker": ticker,
                    "Quantity": quantity,
                    "Current Price": f"${price:,.2f}" if price is not None else "N/A",
                    "Market Value": (
                        f"${market_value:,.2f}" if price is not None else "N/A"
                    ),
                }
            )

        # Display Metrics and Holdings Table
        st.metric("Total Portfolio Value", f"${total_portfolio_value:,.2f}")
        st.dataframe(pd.DataFrame(summary_data), use_container_width=True)

    def _render_trade_editor(
        self, portfolio_name: str, portfolio_data: dict, trades: list
    ):
        """Renders an editable data grid for managing trades."""
        st.info(
            "You can directly edit, add, or delete trades in the table below. Changes are saved automatically."
        )

        if not trades:
            trades_df = pd.DataFrame(
                columns=[
                    "trade_id",
                    "date",
                    "ticker",
                    "action",
                    "direction",
                    "quantity",
                    "price",
                    "costs",
                    "broker",
                    "notes",
                ]
            )
        else:
            trades_df = pd.DataFrame(trades)

        # Use st.data_editor for a spreadsheet-like experience
        edited_df = st.data_editor(
            trades_df,
            key=f"editor_{portfolio_name}",
            num_rows="dynamic",  # Allow adding/deleting rows
            column_config={
                "trade_id": st.column_config.TextColumn("Trade ID", disabled=True),
                "date": st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
                "action": st.column_config.SelectboxColumn(
                    "Action", options=["Buy", "Sell"], required=True
                ),
                "direction": st.column_config.SelectboxColumn(
                    "Direction", options=["Long", "Short"]
                ),
                "quantity": st.column_config.NumberColumn(
                    "Quantity", format="%.8f", required=True
                ),
                "price": st.column_config.NumberColumn(
                    "Price", format="$%.4f", required=True
                ),
                "costs": st.column_config.NumberColumn("Costs", format="$%.2f"),
            },
            use_container_width=True,
            hide_index=True,
        )

        # --- Logic to detect and save changes ---
        if not edited_df.equals(trades_df):
            # Convert DataFrame back to a list of dictionaries
            updated_trades = edited_df.to_dict("records")
            # Assign new UUIDs to any new rows (which will have a None/NaN trade_id)
            for trade in updated_trades:
                if pd.isna(trade.get("trade_id")):
                    trade["trade_id"] = str(uuid.uuid4())

            portfolio_data["trades"] = updated_trades
            self.portfolio_manager.add_or_update(portfolio_name, portfolio_data)
            st.toast(f"Changes to '{portfolio_name}' saved!", icon="💾")
            st.rerun()

    def _render_add_trade_form(self, portfolio_name: str, portfolio_data: dict):
        """Renders a form to add a new trade to the portfolio."""
        st.subheader("Add New Trade")
        with st.form(f"add_trade_form_{portfolio_name}", clear_on_submit=True):
            cols = st.columns([1, 1, 1, 1])
            ticker = cols[0].text_input("Ticker").upper()
            trade_date = cols[1].date_input("Date", datetime.now())
            action = cols[2].selectbox("Action", ["Buy", "Sell"])
            direction = cols[3].selectbox("Direction", ["Long", "Short"])

            cols = st.columns([1, 1, 1, 1])
            quantity = cols[0].number_input("Quantity", min_value=0.0, format="%.8f")
            price = cols[1].number_input("Price", min_value=0.0, format="%.4f")
            costs = cols[2].number_input(
                "Costs (e.g., commission)", min_value=0.0, value=0.0, format="%.2f"
            )
            broker = cols[3].text_input("Broker")

            notes = st.text_area("Notes")

            submitted = st.form_submit_button("Add Trade")
            if submitted:
                if ticker and quantity > 0 and price > 0:
                    new_trade = {
                        "trade_id": str(uuid.uuid4()),
                        "date": trade_date.strftime("%Y-%m-%d"),
                        "ticker": ticker,
                        "action": action,
                        "direction": direction,
                        "quantity": quantity,
                        "price": price,
                        "costs": costs,
                        "broker": broker,
                        "notes": notes,
                    }
                    if "trades" not in portfolio_data:
                        portfolio_data["trades"] = []
                    portfolio_data["trades"].append(new_trade)
                    self.portfolio_manager.add_or_update(portfolio_name, portfolio_data)
                    st.success(f"Trade for {ticker} added to '{portfolio_name}'.")
                    st.rerun()
                else:
                    st.warning("Please fill in Ticker, Quantity, and Price.")

    def _render_delete_portfolio(self, portfolio_name: str):
        """Renders the UI for deleting the entire portfolio."""
        st.divider()
        st.subheader("Delete Portfolio")
        st.warning(f"This action is permanent and cannot be undone.")
        if st.button(f"❌ Delete Portfolio '{portfolio_name}'", type="secondary"):
            self.portfolio_manager.delete(portfolio_name)
            st.success(f"Portfolio '{portfolio_name}' has been deleted.")
            # We don't rerun here, to allow the user to see the message before the tab disappears.
            # The app will naturally reset on the next interaction.
