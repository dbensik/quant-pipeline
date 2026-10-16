import numpy as np
import pandas as pd

from analysis.performance_analyzer import PerformanceAnalyzer, periods_per_year
from execution.simulated_handler import SimulatedExecutionHandler, to_lot

from .events import OrderEvent
from .risk_manager import PortfolioRiskManager


class PortfolioBacktester:
    """
    A realistic, event-driven portfolio backtester that orchestrates signals,
    risk, and execution at a portfolio level.
    """

    def __init__(
        self,
        initial_capital: float = 100000.0,
        execution_handler: SimulatedExecutionHandler = None,
        risk_manager: PortfolioRiskManager = None,
        enable_vol_targeting: bool = False,
        target_volatility: float = 0.15,
        seed: int = None,
        transaction_cost: float = 0.001,
    ):
        """
        seed: passed to the default execution handler's slippage RNG. Without
        it, identical portfolio backtests return different numbers — the same
        defect fixed for the single-symbol Backtester, and the same
        consequence: saved results cannot be reproduced and comparing two runs
        partly compares random draws.

        transaction_cost: proportional cost per fill as a fraction of its value
        (0.001 = 10 bps), on top of the default handler's $1 fee; the same
        default as Backtester. There was none before 2026-10-12.

        Both are ignored when an explicit execution_handler is supplied, since
        that handler owns its own RNG and costs.
        """
        self.initial_capital = initial_capital
        self.execution_handler = execution_handler or SimulatedExecutionHandler(
            seed=seed, commission_rate=transaction_cost
        )
        self.risk_manager = risk_manager or PortfolioRiskManager()
        self.enable_vol_targeting = enable_vol_targeting
        self.target_volatility = target_volatility
        self.results = None
        #: Per-bar cash interest of the last run, aligned to its results.
        self.cash_accrual = None

    def run(
        self,
        price_data: dict,
        signals_data: dict,
        target_weights: dict,
        *,
        cash_accrual: pd.Series,
        lot_sizes: dict,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Runs the portfolio backtest using an efficient event-driven loop.

        Args:
            price_data: A dictionary mapping tickers to their OHLCV DataFrame.
            signals_data: A dictionary mapping tickers to their signals DataFrame.
            target_weights: A dictionary mapping tickers to their target weights.
            cash_accrual: Interest one unit of cash earns on each bar, indexed
                by timestamp (see Backtester.run). Keyed by date rather than
                position because the timeline is built here, as the union of
                every symbol's dates; a bar with no accrual is refused.
            lot_sizes: smallest tradable quantity per ticker
                (config.settings.lot_size_for): 1.0 for shares, 1e-8 for
                crypto, so a basket can hold whole AAPL shares beside a
                fraction of a BTC. Every ticker must have one; all 1.0
                reproduces the old whole-unit runs exactly.

        FILLS AT THE NEXT BAR'S OPEN (decision 31, 2026-10-15), as in
        Backtester: the signals dated on one bar of the timeline are acted on
        at the next, at that bar's Open, and orders are sized — and the risk
        manager judged — on equity valued at that open. Until 2026-10-15 they
        filled at the signal bar's own close. Measured before switching
        (research/fill-timing-2026-10-15.md): the allocation rules lost
        0.02-0.07 Sharpe 2011-2026; static rebalancing did not move. Holdings
        are still marked at the close. Every frame needs an Open column.
        """
        missing_open = sorted(t for t, f in price_data.items() if "Open" not in f.columns)
        if missing_open:
            raise ValueError(
                f"price_data has no Open column for {', '.join(missing_open)}: "
                "orders fill at the next bar's open"
            )
        missing_lots = sorted(set(price_data) - set(lot_sizes))
        if missing_lots:
            raise ValueError(f"lot_sizes has no entry for {', '.join(missing_lots)}")
        # --- 1. ARCHITECTURAL REFACTOR: Create an event-driven loop ---
        # Get all unique timestamps from all signal sources. This is more efficient
        # than iterating over every calendar day.
        all_signal_dates = set()
        for signal_df in signals_data.values():
            all_signal_dates.update(signal_df.index)

        # Combine all price data indices to create the master timeline for the portfolio
        all_price_dates = set()
        for price_df in price_data.values():
            all_price_dates.update(price_df.index)

        # The full timeline is the union of all price and signal dates, sorted.
        full_timeline = sorted(list(all_price_dates.union(all_signal_dates)))

        if not full_timeline:
            # If there are no dates at all, return an empty result.
            return pd.DataFrame(), pd.DataFrame()

        accrual = cash_accrual.reindex(full_timeline)
        if accrual.isna().any():
            missing = accrual.index[accrual.isna()]
            raise ValueError(
                f"cash_accrual has no value for {len(missing)} bar(s), first {missing[0]}"
            )
        accrual_values = accrual.to_numpy(dtype=float)
        self.cash_accrual = accrual_values

        # --- 2. Initialize Portfolio State ---
        all_tickers = list(price_data.keys())
        portfolio = pd.DataFrame(index=full_timeline)
        portfolio["cash"] = self.initial_capital
        portfolio["holdings_value"] = 0.0
        portfolio["total"] = self.initial_capital
        for ticker in all_tickers:
            portfolio[f"{ticker}_pos"] = 0.0

        trade_log = []

        # --- 3. Main Event Loop ---
        # Iterate through each day in our master timeline
        for i, timestamp in enumerate(full_timeline):
            # Carry forward the previous day's state
            if i > 0:
                prev_timestamp = full_timeline[i - 1]
                portfolio.loc[timestamp] = portfolio.loc[prev_timestamp]
                # Interest on idle cash for the period ending at this bar.
                portfolio.loc[timestamp, "cash"] *= 1.0 + accrual_values[i]

            # Update holdings value with current prices
            current_holdings_value = 0.0
            current_prices = {}
            for ticker in all_tickers:
                # Use asof to get the most recent price if the current timestamp is a holiday
                price_series = price_data[ticker]["Close"]
                current_price = price_series.asof(timestamp)

                if pd.notna(current_price):
                    current_prices[ticker] = current_price
                    current_holdings_value += (
                        portfolio.loc[timestamp, f"{ticker}_pos"] * current_price
                    )

            portfolio.loc[timestamp, "holdings_value"] = current_holdings_value
            portfolio.loc[timestamp, "total"] = (
                portfolio.loc[timestamp, "cash"] + current_holdings_value
            )

            # Act on the PREVIOUS bar's signals, at this bar's open.
            if i == 0:
                continue
            signal_timestamp = full_timeline[i - 1]
            if signal_timestamp not in all_signal_dates:
                continue

            open_prices = {}
            for ticker in all_tickers:
                open_price = price_data[ticker]["Open"].asof(timestamp)
                if pd.notna(open_price):
                    open_prices[ticker] = open_price
            # Equity as it stands at the open: what sizing and the risk
            # manager may know. The close-valued total above is not known yet.
            open_state = portfolio.loc[timestamp].copy()
            open_state["total"] = open_state["cash"] + sum(
                open_state[f"{t}_pos"] * open_prices.get(t, current_prices.get(t, 0.0))
                for t in all_tickers
            )

            # --- 4. Process Signals for the Day ---
            # This logic now correctly handles both portfolio-level and ticker-level signals.
            portfolio_signal_df = signals_data.get("Portfolio")
            portfolio_signal = (
                portfolio_signal_df.loc[signal_timestamp, "signal"]
                if portfolio_signal_df is not None and signal_timestamp in portfolio_signal_df.index
                else 0
            )

            bar_orders = []
            for ticker in all_tickers:
                ticker_signal_df = signals_data.get(ticker)
                ticker_signal = (
                    ticker_signal_df.loc[signal_timestamp, "signal"]
                    if ticker_signal_df is not None and signal_timestamp in ticker_signal_df.index
                    else 0
                )

                # Combine signals (portfolio signal takes precedence if it exists)
                signal = portfolio_signal if portfolio_signal != 0 else ticker_signal

                if signal == 0:
                    continue

                current_position = portfolio.loc[timestamp, f"{ticker}_pos"]
                current_price = open_prices.get(ticker)
                order = None

                if not current_price or pd.isna(current_price):
                    continue

                # --- Signal Execution Logic ---
                if signal == 1 and np.isclose(current_position, 0):  # Directional Buy
                    quantity = 0

                    if self.enable_vol_targeting:
                        # Calculate rolling volatility (20-day)
                        price_series = price_data[ticker]["Close"]
                        # Get data up to current timestamp
                        # Using searchsorted to find position efficiently, or just slicing if index is datetime
                        # Assuming index is datetime and sorted

                        # Optimization: We already have current price. We need historical.
                        # This slice might be slow inside a loop, but fine for MVP.
                        # Closes BEFORE this bar: its own close is not known
                        # at the open the order fills at.
                        hist_slice = price_series[price_series.index < timestamp].tail(
                            21
                        )  # 20 days returns needs 21 prices

                        if len(hist_slice) > 10:
                            returns = hist_slice.pct_change().dropna()
                            asset_vol = returns.std() * np.sqrt(periods_per_year(hist_slice.index))

                            quantity = self.risk_manager.calculate_volatility_adjusted_size(
                                portfolio_equity=open_state["total"],
                                asset_volatility=asset_vol,
                                target_volatility=self.target_volatility,
                                asset_price=current_price,
                                lot_size=lot_sizes[ticker],
                            )
                    else:
                        # Fixed Weight Sizing
                        target_value = open_state["total"] * target_weights.get(ticker, 0)
                        quantity = to_lot(target_value / current_price, lot_sizes[ticker])

                    if quantity > 0:
                        order = OrderEvent(timestamp, ticker, "MKT", quantity, "BUY")

                elif signal == -1 and current_position > 0:  # Directional Sell
                    # The whole holding: a fractional position floored to a
                    # lot could leave dust that is never sold.
                    quantity = (
                        int(current_position) if lot_sizes[ticker] == 1.0 else current_position
                    )
                    order = OrderEvent(timestamp, ticker, "MKT", quantity, "SELL")

                elif signal == 2:  # Rebalance Signal
                    # Rebalancing logic for vol targeting is complex (needs to re-eval everything).
                    # For now, we only apply vol targeting on INITIAL ENTRY (Signal 1).
                    # Standard rebalance uses fixed weights.

                    target_value = open_state["total"] * target_weights.get(ticker, 0)
                    lot = lot_sizes[ticker]
                    target_quantity = to_lot(target_value / current_price, lot)
                    trade_quantity = target_quantity - current_position

                    if trade_quantity > 0:
                        order = OrderEvent(
                            timestamp, ticker, "MKT", to_lot(trade_quantity, lot), "BUY"
                        )
                    elif trade_quantity < 0:
                        order = OrderEvent(
                            timestamp, ticker, "MKT", to_lot(abs(trade_quantity), lot), "SELL"
                        )

                if order:
                    bar_orders.append((ticker, order))

            # --- 5. Risk and Execution: sells first, then buys ---
            # Sales fund purchases, as in a real rebalance. Executed in ticker
            # order instead, a buy could precede the sale paying for it: in
            # real baskets (2016-2025) 27-75 bars a run went overdrawn mid-bar
            # and back above zero by the close, which the cash check below
            # would have turned into under-bought rebalances. sorted() is
            # stable, so tickers keep their order within each side.
            for ticker, order in sorted(bar_orders, key=lambda item: item[1].direction != "SELL"):
                portfolio.loc[timestamp, f"{order.symbol}_price"] = open_prices[ticker]
                open_state[f"{order.symbol}_price"] = open_prices[ticker]
                approved_order = self.risk_manager.assess_order(
                    order, open_state, lot_size=lot_sizes[ticker]
                )
                if not approved_order:
                    continue

                fill_event = self.execution_handler.execute_order(
                    approved_order, open_prices[ticker]
                )

                if fill_event and fill_event.direction == "BUY":
                    # Never spend more cash than there is. Orders are sized on
                    # equity x weight at the open, so adverse slippage and
                    # the commission could overdraw the account: real
                    # AAPL+TQQQ baskets ended 60 bars below zero, by up to $70
                    # (2016-2025). Same rule as Backtester: buy what the cash
                    # covers at THIS fill (no second slippage draw), or skip.
                    cash_now = portfolio.loc[timestamp, "cash"]
                    if fill_event.total_cost > cash_now:
                        affordable = self.execution_handler.affordable_quantity(
                            cash_now, fill_event.fill_price, lot_sizes[ticker]
                        )
                        if affordable <= 0:
                            continue
                        fill_event = self.execution_handler.resize(fill_event, affordable)

                if fill_event:
                    if fill_event.direction == "BUY":
                        portfolio.loc[timestamp, "cash"] -= fill_event.total_cost
                        portfolio.loc[timestamp, f"{ticker}_pos"] += fill_event.quantity
                    elif fill_event.direction == "SELL":
                        portfolio.loc[timestamp, "cash"] += fill_event.total_cost
                        portfolio.loc[timestamp, f"{ticker}_pos"] -= fill_event.quantity

                    # Convert to dict and add the computed property 'total_cost'
                    trade_data = vars(fill_event).copy()
                    trade_data["total_cost"] = fill_event.total_cost
                    trade_log.append(trade_data)

        # --- Final Calculations ---
        portfolio["returns"] = portfolio["total"].pct_change().fillna(0)
        # Fills on each bar, for the Trade Count. Added after the loop: the loop
        # carries each row forward, which would repeat a bar's fills.
        fill_times = pd.DatetimeIndex([t["timestamp"] for t in trade_log])
        fills = pd.Series(1, index=fill_times).groupby(level=0).sum()
        portfolio["trades"] = fills.reindex(portfolio.index, fill_value=0).astype(int).to_numpy()
        self.results = portfolio

        return portfolio, pd.DataFrame(trade_log)

    def get_performance_metrics(self) -> dict:
        """Calculates performance metrics by delegating to the PerformanceAnalyzer."""
        if self.results is None or self.results.empty:
            return {}
        analyzer = PerformanceAnalyzer(
            self.results,
            initial_capital=self.initial_capital,
            risk_free_accrual=self.cash_accrual,
        )
        return analyzer.calculate_all_metrics()
