import logging
import pandas as pd
from data_pipeline.data_pipeline import DataPipeline
from alpha_models.mean_reversion import MeanReversionStrategy
from backtesting.backtester import Backtester

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("StrategyRunner")

def run_strategy(ticker, start_date, end_date, strategy_class, strategy_kwargs=None):
    strategy_kwargs = strategy_kwargs or {}

    logger.info(f"Running strategy for {ticker} from {start_date} to {end_date}")

    # Fetch and clean data
    pipeline = DataPipeline(ticker=ticker, start_date=start_date, end_date=end_date)
    pipeline.fetch_data()
    pipeline.clean_data()
    data = pipeline.data

    # Instantiate and run strategy
    strategy = strategy_class(**strategy_kwargs)
    signals = strategy.generate_signals(data)

    # Run backtest
    backtester = Backtester(data, signals)
    portfolio = backtester.run_backtest()
    backtester.print_performance()

    return portfolio

if __name__ == "__main__":
    portfolio = run_strategy(
        ticker="SPY",
        start_date="2018-01-01",
        end_date="2024-12-31",
        strategy_class=MeanReversionStrategy,
        strategy_kwargs={"window": 20, "threshold": 0.05}
    )
