import math

import numpy as np
import pandas as pd

from analysis.performance_analyzer import PerformanceAnalyzer, periods_per_year
from core import ledger
from core.ledger import ShortTerms
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
        #: Bar at which a target run's equity reached 0 with shorts held, else None.
        self.ruined_at = None

    def run(
        self,
        price_data: dict,
        signals_data: dict,
        target_weights: dict,
        *,
        cash_accrual: pd.Series,
        lot_sizes: dict,
        short_terms: ShortTerms | None = None,
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

        TARGET SIGNALS (decision 48, research/short-engine-2026-10-19.md): a
        signal frame with a `target` column instead of `signal` gives the
        position wanted, as a SIGNED FRACTION OF EQUITY (-0.5 = short half the
        equity), NaN = keep the current quantity. Every frame must then have
        one; `target_weights` is not read. See _run_targets. A negative
        target needs `short_terms` (core/ledger.py), which no event-shaped run
        takes: events keep the path below untouched.
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
        has_target = {key: "target" in frame.columns for key, frame in signals_data.items()}
        target_mode = any(has_target.values())
        if target_mode and not all(has_target.values()):
            raise ValueError(
                "Signal frames mix `target` and `signal` columns: "
                f"targets for {sorted(k for k, v in has_target.items() if v)}, "
                f"events for {sorted(k for k, v in has_target.items() if not v)}"
            )
        if short_terms is not None and not target_mode:
            raise ValueError("short_terms applies to target signals only; events cannot short")
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

        if target_mode:
            portfolio, trade_log = self._run_targets(
                price_data,
                signals_data,
                full_timeline,
                accrual_values,
                lot_sizes,
                short_terms,
            )
            self.results = portfolio
            return portfolio, pd.DataFrame(trade_log)

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

            # The bar's equity after its fills, at the close. Valued before
            # them (above), a fill showed a bar late: the old holdings marked
            # at this close, and the new ones only from the next (decision 52,
            # research/portfolio-equity-lag-2026-10-20.md).
            if bar_orders:
                current_holdings_value = sum(
                    portfolio.loc[timestamp, f"{t}_pos"] * price
                    for t, price in current_prices.items()
                )
                portfolio.loc[timestamp, "holdings_value"] = current_holdings_value
                portfolio.loc[timestamp, "total"] = (
                    portfolio.loc[timestamp, "cash"] + current_holdings_value
                )

        # --- Final Calculations ---
        portfolio["returns"] = portfolio["total"].pct_change().fillna(0)
        # Fills on each bar, for the Trade Count. Added after the loop: the loop
        # carries each row forward, which would repeat a bar's fills.
        fill_times = pd.DatetimeIndex([t["timestamp"] for t in trade_log])
        fills = pd.Series(1, index=fill_times).groupby(level=0).sum()
        portfolio["trades"] = fills.reindex(portfolio.index, fill_value=0).astype(int).to_numpy()
        self.results = portfolio

        return portfolio, pd.DataFrame(trade_log)

    def _run_targets(
        self,
        price_data: dict,
        signals_data: dict,
        timeline: list,
        accrual_values: np.ndarray,
        lot_sizes: dict,
        short_terms: ShortTerms | None,
    ) -> tuple[pd.DataFrame, list]:
        """
        The target-signal loop: long AND short positions, with the accounting
        in core/ledger.py. A separate path, so event-shaped runs are untouched.

        Each bar, in order:
          1. Borrow on the shorts (value at the previous close, calendar
             days) and interest on FREE cash (cash less the shorts'
             collateral). With no short held, interest is exactly the event
             path's `cash *= 1 + accrual`.
          2. At this bar's open: a margin cut flagged at the previous close,
             pro rata on every position (fills logged `reason="margin"`);
             then the targets emitted up to the previous bar. All closing
             fills (sells of longs, covers of shorts) before any opening one,
             so a flip from long to short is two fills. A ticker with no
             Open on this bar keeps its target pending.
          3. Marked at the close; if equity is below
             `maintenance` x gross, the next open cuts. Equity at or below 0
             is ruin: positions are dropped, the total is 0 from there on,
             and `self.ruined_at` names the bar.

        Sizing: |target| x equity at the open / the open, floored to lots.
        A closing fill takes the whole holding when the target is 0 or the
        other side (no dust). A long is paid from free cash only (no margin
        loan); a short, and a long when short terms are set, is bounded by
        gross <= max_gross x equity. The risk manager judges OPENING orders
        only, long or short, as if each were a buy: a cover is a buy, and a
        drawdown halt that blocked covers would leave a short unrecoverable.
        """
        if self.enable_vol_targeting:
            raise ValueError("Volatility targeting does not apply to target signals")
        if "Portfolio" in signals_data:
            raise ValueError("Target signals are per ticker; a 'Portfolio' frame has no ticker")
        tickers = list(price_data.keys())
        index = pd.DatetimeIndex(timeline)
        n = len(index)
        closes = {t: price_data[t]["Close"].reindex(index).ffill().to_numpy(float) for t in tickers}
        # Opens are NOT carried forward: a ticker trades only on its own bars.
        opens = {t: price_data[t]["Open"].reindex(index).to_numpy(float) for t in tickers}
        targets = {
            t: (
                signals_data[t]["target"].reindex(index).to_numpy(float)
                if t in signals_data
                else np.full(n, np.nan)
            )
            for t in tickers
        }
        if short_terms is None:
            negative = sorted(t for t in tickers if np.nanmin(targets[t], initial=0.0) < 0)
            if negative:
                raise ValueError(
                    f"Negative targets for {negative} need short_terms (borrow, margin)"
                )
        days = np.concatenate([[0.0], np.diff(index.asi8) / 86_400e9])

        cash = float(self.initial_capital)
        pos = {t: 0.0 for t in tickers}
        pending: dict = {}
        cut = 0.0
        borrow_paid = 0.0
        self.ruined_at = None
        cols = {
            name: np.zeros(n)
            for name in (
                "cash",
                "holdings_value",
                "total",
                "long_value",
                "short_value",
                "gross",
                "borrow_paid",
            )
        }
        pos_cols = {t: np.zeros(n) for t in tickers}
        trade_log: list = []

        def held_prices(i: int) -> dict:
            return {t: closes[t][i] for t in tickers if pos[t] != 0}

        def apply(fill, reason: str) -> None:
            nonlocal cash
            if fill.direction == "BUY":
                cash -= fill.total_cost
                pos[fill.symbol] += fill.quantity
            else:
                cash += fill.total_cost
                pos[fill.symbol] -= fill.quantity
            row = vars(fill).copy()
            row["total_cost"] = fill.total_cost
            row["reason"] = reason
            trade_log.append(row)

        def execute(ticker, quantity, direction, price, ts, reason):
            order = OrderEvent(ts, ticker, "MKT", quantity, direction)
            apply(self.execution_handler.execute_order(order, price), reason)

        for i in range(n):
            ts = timeline[i]
            if self.ruined_at is not None:
                continue
            if i > 0:
                prev = held_prices(i - 1)
                shorts_held = any(q < 0 for q in pos.values())
                if shorts_held:
                    interest = ledger.free_cash(cash, pos, prev) * accrual_values[i]
                    charge = ledger.borrow_charge(pos, prev, days[i], short_terms.borrow_rate)
                    cash += interest - charge
                    borrow_paid += charge
                else:
                    cash *= 1.0 + accrual_values[i]

                for t in tickers:
                    if not np.isnan(targets[t][i - 1]):
                        pending[t] = targets[t][i - 1]

                open_now = {t: opens[t][i] for t in tickers if opens[t][i] > 0}
                if cut > 0:
                    for t in tickers:
                        if pos[t] == 0 or t not in open_now:
                            continue
                        lot = lot_sizes[t]
                        quantity = min(abs(pos[t]), math.ceil(abs(pos[t]) * cut / lot) * lot)
                        direction = "SELL" if pos[t] > 0 else "BUY"
                        execute(t, quantity, direction, open_now[t], ts, "margin")
                    cut = 0.0

                mark = {t: open_now.get(t, closes[t][i - 1]) for t in tickers if pos[t] != 0}
                equity_open = ledger.equity(cash, pos, mark)
                wanted = {}
                for t in list(pending):
                    if t not in open_now:
                        continue
                    target = pending.pop(t)
                    size = to_lot(abs(target) * max(equity_open, 0.0) / open_now[t], lot_sizes[t])
                    wanted[t] = math.copysign(size, target) if size else 0.0

                # Closing fills first: they free cash and gross for the openings.
                for t, desired in wanted.items():
                    current = pos[t]
                    if current > 0 and desired < current:
                        execute(t, current - max(desired, 0.0), "SELL", open_now[t], ts, "signal")
                    elif current < 0 and desired > current:
                        execute(t, -current - max(-desired, 0.0), "BUY", open_now[t], ts, "signal")

                for t, desired in wanted.items():
                    current = pos[t]
                    lot = lot_sizes[t]
                    if desired > max(current, 0.0):
                        quantity = desired - max(current, 0.0)
                        direction = "BUY"
                    elif desired < min(current, 0.0):
                        quantity = -desired + min(current, 0.0)
                        direction = "SELL"
                    else:
                        continue
                    price = open_now[t]
                    mark = {u: open_now.get(u, closes[u][i - 1]) for u in tickers if pos[u] != 0}
                    equity_now = ledger.equity(cash, pos, mark)
                    if short_terms is not None:
                        room = ledger.short_room(
                            equity_now,
                            ledger.gross_exposure(pos, mark),
                            short_terms.max_gross,
                            price,
                            lot,
                        )
                        quantity = min(quantity, room)
                    probe = OrderEvent(ts, t, "MKT", quantity, "BUY")
                    state = pd.Series({"total": equity_now, f"{t}_price": price})
                    approved = (
                        self.risk_manager.assess_order(probe, state, lot_size=lot)
                        if quantity > 0
                        else None
                    )
                    if not approved:
                        continue
                    order = OrderEvent(ts, t, "MKT", approved.quantity, direction)
                    fill = self.execution_handler.execute_order(order, price)
                    if direction == "BUY":
                        spendable = ledger.free_cash(cash, pos, mark)
                        if fill.total_cost > spendable:
                            affordable = self.execution_handler.affordable_quantity(
                                spendable, fill.fill_price, lot
                            )
                            if affordable <= 0:
                                continue
                            fill = self.execution_handler.resize(fill, affordable)
                    apply(fill, "signal")

            close_prices = held_prices(i)
            value = sum(q * p for q, p in ((pos[t], close_prices[t]) for t in close_prices))
            total = cash + value
            long_v = ledger.long_value(pos, close_prices)
            short_v = ledger.short_value(pos, close_prices)
            if short_v > 0 and total <= 0:
                self.ruined_at = ts
                for t in tickers:
                    pos[t] = 0.0
                cash, value, total, long_v, short_v = 0.0, 0.0, 0.0, 0.0, 0.0
            elif short_terms is not None:
                cut = ledger.margin_cut(total, long_v + short_v, short_terms.maintenance)
            cols["cash"][i] = cash
            cols["holdings_value"][i] = value
            cols["total"][i] = total
            cols["long_value"][i] = long_v
            cols["short_value"][i] = short_v
            cols["gross"][i] = long_v + short_v
            cols["borrow_paid"][i] = borrow_paid
            for t in tickers:
                pos_cols[t][i] = pos[t]

        portfolio = pd.DataFrame(cols, index=timeline)
        for t in tickers:
            portfolio[f"{t}_pos"] = pos_cols[t]
        portfolio["returns"] = (
            portfolio["total"].pct_change().fillna(0).replace([np.inf, -np.inf], 0)
        )
        fill_times = pd.DatetimeIndex([row["timestamp"] for row in trade_log])
        fills = pd.Series(1, index=fill_times).groupby(level=0).sum()
        portfolio["trades"] = fills.reindex(portfolio.index, fill_value=0).astype(int).to_numpy()
        return portfolio, trade_log

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
