from typing import Optional

import pandas as pd
import streamlit as st

from dashboard_app.api_client import ApiUnavailable, QuantApiClient

from services.execution_service.portfolio_manager import PortfolioManager
from dashboard_app.price_data_handler import PriceDataHandler

# Phase 3 exit gate: single-symbol backtests and every strategy definition now
# come from the API (QuantApiClient), so this module no longer imports
# alpha_models or backtesting.Backtester.
#
# The imports below are the documented remainder — each covers functionality
# with NO API endpoint yet, so it still runs in-process. See the Phase 3 task
# note for the delta:
#   PortfolioBacktester — multi-asset portfolio backtests (/api/v1/backtest is
#                         single-symbol only)
#   RiskManager         — VaR/CVaR and related risk metrics
# They are imported lazily inside the methods that need them so that importing
# this controller does not pull the compute layer into the Streamlit process.


class AnalysisController:
    """
    Handles logic for running backtests (individual and portfolio).
    """

    def __init__(
        self,
        price_handler: PriceDataHandler,
        portfolio_manager: PortfolioManager,
        api_client: QuantApiClient = None,
    ):
        self.price_handler = price_handler
        self.portfolio_manager = portfolio_manager
        self.api = api_client or QuantApiClient()

    def resolve_strategy(self, selections: dict) -> tuple:
        """
        Return (strategy_id, params) for the API from the sidebar's selections.

        Replaces the old create_strategy_model() if/elif chain, which mapped 13
        display names onto strategy classes with bespoke param keys and was the
        third copy of strategy identity in the codebase. The sidebar now emits
        the registry id and registry-named params directly, so this is a lookup
        rather than a translation table.
        """
        strategy_id = selections.get("strategy_id")
        params = selections.get("strategy_params", {}) or {}

        if not strategy_id:
            st.error(
                "No strategy selected. If the sidebar's strategy list is empty, "
                "the API is unreachable — start it with "
                "`poetry run uvicorn api.main:app --port 8001`."
            )
            return None, {}

        # Optimisation emits <name>_range keys; those belong to the in-process
        # optimiser, not to a single backtest run.
        params = {k: v for k, v in params.items() if not k.endswith("_range")}
        return strategy_id, params

    def _build_local_model(self, strategy_id: str, params: dict, selections: dict):
        """
        Build a strategy object in-process, for the multi-asset portfolio path only.

        Everything single-symbol goes through the API. This exists because
        /api/v1/backtest is single-symbol, so a portfolio backtest still needs a
        real model object locally. The import is lazy and the registry is the
        same source of truth the API serves from, so the two cannot diverge.
        """
        from alpha_models import registry

        params = dict(params)
        if strategy_id == "cointegrated_mean_reversion":
            # Requires Johansen weights the registry cannot default.
            portfolio_data = self.portfolio_manager.portfolios.get(
                selections.get("source_name"), {}
            )
            weights = portfolio_data.get("weights")
            if not weights:
                st.error(
                    "Cointegrated Mean Reversion requires a portfolio with weights "
                    "from a Johansen test."
                )
                return None
            spec = registry.get(strategy_id)
            return spec.cls(weights=weights, **{
                p.name: params.get(p.name, p.default) for p in spec.params
            })

        try:
            return registry.build(strategy_id, params)
        except (KeyError, ValueError) as exc:
            st.error(f"Could not build strategy '{strategy_id}': {exc}")
            return None

    def run_individual_backtest(self, selections: dict):
        """Runs a standard backtest on a set of symbols."""
        selected_symbols = selections.get("selected_symbols", [])
        if not selected_symbols:
            st.warning(f"Please select at least one ticker for analysis. (Debug: Received {selected_symbols}, Selections keys: {list(selections.keys())})")
            return

        start_date, end_date = selections["start_date"].strftime("%Y-%m-%d"), selections[
            "end_date"
        ].strftime("%Y-%m-%d")
        with st.spinner(
            f"Fetching data and running backtests for {len(selected_symbols)} ticker(s)..."
        ):
            backtest_data_dict = self.price_handler.get_full_data_for_tickers(
                selected_symbols, start_date, end_date
            )
            spy_df = self.price_handler.get_prices(["SPY"], start_date, end_date)
            benchmarks = (
                {"SPY": pd.DataFrame(spy_df["SPY"]).rename(columns={"SPY": "total"})}
                if not spy_df.empty
                else {}
            )

            # RiskManager has no API endpoint yet — imported lazily so this
            # module's import does not drag the compute layer into Streamlit.
            from portfolio.risk_manager import RiskManager

            results_data = {}
            strategy_id, strategy_params = self.resolve_strategy(selections)
            if not strategy_id:
                return

            caveat_shown = False
            for symbol, data in backtest_data_dict.items():
                try:
                    portfolio, stats, trade_log, caveat = self.api.run_backtest(
                        symbol=symbol,
                        strategy_id=strategy_id,
                        start=start_date,
                        end=end_date,
                        params=strategy_params,
                        initial_capital=selections.get("initial_capital", 100000.0),
                        transaction_cost=selections.get("commission", 0.0) or 0.0,
                    )
                except ApiUnavailable as exc:
                    # Surface loudly: an empty result would read as "the strategy
                    # made no trades" rather than "the request failed".
                    st.error(f"Backtest failed for {symbol}: {exc}")
                    continue

                if caveat and not caveat_shown:
                    st.warning(caveat, icon="⚠️")
                    caveat_shown = True

                risk_metrics = {}
                if not portfolio.empty and not portfolio["returns"].empty:
                    risk_manager = RiskManager(portfolio_returns=portfolio["returns"])
                    risk_metrics = risk_manager.get_all_risk_metrics()

                results_data[symbol] = {
                    "portfolio": portfolio,
                    "stats": stats,
                    "risk_metrics": risk_metrics,
                    "trade_log": trade_log,
                }
                benchmarks[f"Buy & Hold {symbol}"] = pd.DataFrame(data["Close"]).rename(
                    columns={"Close": "total"}
                )

            if not results_data:
                st.error(
                    "Could not generate backtest results for any selected tickers."
                )
                return
            st.session_state.backtest_run = {
                "results": results_data,
                "benchmarks": benchmarks,
            }

    def run_portfolio_backtest(self, selections: dict):
        """Runs a single backtest on a portfolio of assets."""
        selected_symbols = selections.get("selected_symbols", [])
        if (
            selections.get("strategy_type") == "Pairs Trading"
            and len(selected_symbols) != 2
        ):
            st.warning("Pairs Trading requires exactly two tickers to be selected.")
            return

        if len(selected_symbols) < 2 and selections.get("strategy_type") != "Pairs Trading":
            st.warning("Portfolio analysis requires at least two tickers.")
            return

        start_date, end_date = selections["start_date"].strftime("%Y-%m-%d"), selections[
            "end_date"
        ].strftime("%Y-%m-%d")
        with st.spinner(f"Fetching data for portfolio and benchmarks..."):
            price_data = self.price_handler.get_full_data_for_tickers(
                selected_symbols, start_date, end_date
            )
            if not price_data:
                st.error(
                    "No data available for the selected tickers in the given date range."
                )
                return

            spy_df = self.price_handler.get_prices(["SPY"], start_date, end_date)
            benchmarks = (
                {"SPY": pd.DataFrame(spy_df["SPY"]).rename(columns={"SPY": "total"})}
                if not spy_df.empty
                else {}
            )

        with st.spinner("Generating trading signals and running backtest..."):
            # Multi-asset portfolio backtests have no API endpoint
            # (/api/v1/backtest is single-symbol), so this path builds the model
            # and runs the backtest in-process. Both imports are lazy and local
            # to this method — see the module docstring's delta note.
            from backtesting.portfolio_backtester import PortfolioBacktester

            strategy_id, strategy_params = self.resolve_strategy(selections)
            if not strategy_id:
                return
            model = self._build_local_model(strategy_id, strategy_params, selections)
            if not model:
                return

            backtester = PortfolioBacktester(
                initial_capital=selections.get("initial_capital", 100000.0),
                enable_vol_targeting=selections.get("enable_vol_targeting", False),
                target_volatility=selections.get("target_volatility", 0.15)
            )
            portfolio_weights = {
                symbol: 1.0 / len(price_data) for symbol in price_data.keys()
            }
            if selections.get("source_type") == "portfolio":
                portfolio_data = self.portfolio_manager.portfolios.get(
                    selections.get("source_name"), {}
                )
                if portfolio_data.get("weights"):
                    portfolio_weights = portfolio_data["weights"]

            signals_data = {}
            if strategy_id == "pairs_trading":
                price_df = pd.DataFrame(
                    {symbol: data["Close"] for symbol, data in price_data.items()}
                ).dropna()
                signals_df = model.generate_signals(price_df)
                signals_data = {
                    col: signals_df[[col]].rename(columns={col: "signal"})
                    for col in signals_df.columns
                }
            elif strategy_id == "basket_trading":
                any_ticker_data = next(iter(price_data.values()))
                rebalance_signals = model.generate_signals(any_ticker_data)
                signals_data = {
                    symbol: rebalance_signals for symbol in price_data.keys()
                }
            elif strategy_id == "index_rebalancing":
                any_ticker_data = next(iter(price_data.values()))
                rebalance_signals = model.generate_signals(any_ticker_data)
                signals_data = {
                    symbol: rebalance_signals for symbol in price_data.keys()
                }
            elif strategy_id == "cointegrated_mean_reversion":
                price_df = pd.DataFrame(
                    {symbol: data["Close"] for symbol, data in price_data.items()}
                ).dropna()
                signals_df = model.generate_signals(price_df)
                signals_data = {"Portfolio": signals_df}
            else:
                signals_data = {
                    symbol: model.generate_signals(data)
                    for symbol, data in price_data.items()
                }

            portfolio_df, trade_log_df = backtester.run(
                price_data, signals_data, portfolio_weights
            )

            risk_metrics = {}
            if not portfolio_df["returns"].empty:
                risk_manager = RiskManager(portfolio_returns=portfolio_df["returns"])
                risk_metrics = risk_manager.get_all_risk_metrics()

            st.session_state.backtest_run = {
                "results": {
                    "Portfolio": {
                        "portfolio": portfolio_df,
                        "trade_log": trade_log_df,
                        "stats": backtester.get_performance_metrics(),
                        "risk_metrics": risk_metrics,
                    }
                },
                "benchmarks": benchmarks,
            }

    def run_strategy_comparison(self, selections: dict):
        """
        Runs multiple strategies on the same selected ticker(s) and aggregates results
        for side-by-side comparison.
        """
        selected_strategies = selections.get("comparison_strategies", [])
        if len(selected_strategies) < 2:
            st.warning("Please select at least two strategies to compare.")
            return

        selected_symbols = selections.get("selected_symbols", [])
        if not selected_symbols:
            st.warning("Please select a ticker for comparison.")
            return

        start_date, end_date = selections["start_date"].strftime("%Y-%m-%d"), selections[
            "end_date"
        ].strftime("%Y-%m-%d")

        with st.spinner(f"Fetching data for comparison..."):
            # Fetch data once
            data = self.price_handler.get_full_data_for_tickers(
                selected_symbols, start_date, end_date
            )
            
            bench_symbol = selections.get("selected_benchmark")
            comparison_results = {}
            benchmarks = {}
            
            if bench_symbol and bench_symbol != "None":
                 bench_df = self.price_handler.get_prices([bench_symbol], start_date, end_date)
                 
                 # FALLBACK: If DB has no data for benchmark, try fetching live
                 if bench_df.empty:
                    try:
                        import yfinance as yf
                        ticker_obj = yf.Ticker(bench_symbol)
                        # Fetch history
                        hist = ticker_obj.history(start=start_date, end=end_date)
                        if not hist.empty:
                            bench_df = pd.DataFrame({bench_symbol: hist["Close"]})
                    except Exception as e:
                        st.warning(f"Could not fetch benchmark data for {bench_symbol}: {e}")

                 if not bench_df.empty:
                    # Store for plotting
                    benchmarks[bench_symbol] = pd.DataFrame(bench_df[bench_symbol]).rename(columns={bench_symbol: "total"})
                    
                    # Calculate metrics for the benchmark to show in table
                    # We need a 'returns' column for the analyzer
                    bench_data = benchmarks[bench_symbol].copy()
                    bench_data["returns"] = bench_data["total"].pct_change().fillna(0)
                    
                    # NORMALIZE: Scale benchmark to start at 100k so "Total Return" calc is correct
                    # (Analyzer expects 'total' to be portfolio value, not raw price)
                    initial_price = bench_data["total"].iloc[0]
                    if initial_price > 0:
                        bench_data["total"] = (bench_data["total"] / initial_price) * 100000.0

                    # Use analyzer to get stats
                    from analysis.performance_analyzer import PerformanceAnalyzer
                    analyzer = PerformanceAnalyzer(bench_data, 100000.0)
                    bench_stats = analyzer.calculate_all_metrics()
                    
                    # Add to comparison results so it appears in the table
                    # We treat it like a "strategy" for the table display
                    comparison_results[f"Benchmark ({bench_symbol})"] = {
                        "portfolio": None, # No portfolio object needed for table, just stats
                        "stats": bench_stats
                    }

        progress_bar = st.progress(0)
        
        with st.spinner("Running strategies..."):
            target_symbol = selected_symbols[0]
            for i, strategy_name in enumerate(selected_strategies):
                strategy_id = self._id_for_display_name(strategy_name)
                if not strategy_id:
                    st.warning(f"Strategy '{strategy_name}' is not in the catalogue; skipping.")
                    continue

                # --- AUTO-OPTIMIZATION ---
                # Grid-search the best parameters so the comparison is fair.
                best_params = self._find_optimal_params(
                    strategy_id, target_symbol, start_date, end_date
                )

                try:
                    portfolio, stats, _trades, _caveat = self.api.run_backtest(
                        symbol=target_symbol,
                        strategy_id=strategy_id,
                        start=start_date,
                        end=end_date,
                        params=best_params,
                    )
                except ApiUnavailable as exc:
                    st.warning(f"{strategy_name}: {exc}")
                    continue

                # Show which parameters produced the result.
                if best_params:
                    stats = {**stats, **best_params}

                comparison_results[strategy_name] = {
                    "portfolio": portfolio,
                    "stats": stats
                }

                progress_bar.progress((i + 1) / len(selected_strategies))

        if not comparison_results:
            st.error("Could not generate results for comparison.")
            return

        st.session_state.comparison_results = {
            "symbol": selected_symbols[0],
            "strategies": comparison_results,
            "benchmarks": benchmarks
        }

    def _id_for_display_name(self, display_name: str) -> Optional[str]:
        """Map a sidebar display name back to its registry id via the catalogue."""
        for spec in self.api.get_strategies():
            if spec["display_name"] == display_name:
                return spec["id"]
        return None

    # Grid searched per strategy when comparing. Kept small deliberately: each
    # point is a full backtest over HTTP, so the cost is points x symbols.
    _OPTIMIZATION_GRIDS = {
        "mean_reversion": {"window": [10, 20, 30, 40, 50], "threshold": [1.0, 1.5, 2.0, 2.5]},
        "ma_crossover": {"short_window": [10, 20, 30], "long_window": [50, 100, 200]},
    }

    def _find_optimal_params(
        self, strategy_id: str, symbol: str, start_date: str, end_date: str
    ) -> dict:
        """
        Grid-search parameters maximising Sharpe, running each point via the API.

        Previously this constructed strategy objects and a Backtester directly —
        two of the imports the Phase 3 exit gate removes. Running the grid over
        HTTP is slower per point but keeps the dashboard free of the compute
        layer, and the API seeds slippage so the comparison between grid points
        is deterministic rather than partly comparing random draws.

        Returns {} for strategies with no grid, meaning "use registry defaults".
        """
        grid = self._OPTIMIZATION_GRIDS.get(strategy_id)
        if not grid:
            return {}

        names = list(grid)
        best_sharpe, best_params = -float("inf"), {}

        def combos(idx: int, current: dict):
            if idx == len(names):
                yield dict(current)
                return
            for value in grid[names[idx]]:
                current[names[idx]] = value
                yield from combos(idx + 1, current)

        for params in combos(0, {}):
            # Respect the strategy's own constraint rather than discovering it
            # via a 422 on every invalid combination.
            if strategy_id == "ma_crossover" and params["short_window"] >= params["long_window"]:
                continue
            try:
                _pf, stats, _t, _c = self.api.run_backtest(
                    symbol=symbol, strategy_id=strategy_id,
                    start=start_date, end=end_date, params=params,
                )
            except ApiUnavailable:
                continue
            sharpe = stats.get("Sharpe Ratio")
            if sharpe is not None and sharpe > best_sharpe:
                best_sharpe, best_params = sharpe, params

        return best_params
