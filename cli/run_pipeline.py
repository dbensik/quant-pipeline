# cli/run_pipeline.py
from data_pipeline.data_pipeline import DataPipeline
from alpha_models.mean_reversion import MeanReversionStrategy
from backtesting.backtester import Backtester
import matplotlib.pyplot as plt
import sys
import pandas as pd

def main():
    # Instantiate the pipeline
    pipeline = DataPipeline(
        ticker="SPY",
        start_date="2018-01-01",
        end_date="2024-12-31"
    )
    
    # Run the pipeline steps
    pipeline.fetch_data()
    pipeline.clean_data()
    pipeline.save_data(db_path="../quant_pipeline.db", table_name="price_data")

    # Use the processed data from the pipeline
    data = pipeline.data

    # Run a sample query to get summary statistics (e.g., average closing price)
    query = "SELECT AVG(Close) AS avg_close FROM price_data WHERE Ticker = 'SPY'"
    result = pipeline.query_data(query, db_path="../quant_pipeline.db")
    print("Average closing price:", result.iloc[0]['avg_close'])

     # Generate trading signals using the Mean Reversion Strategy
    strategy = MeanReversionStrategy(window=20, threshold=0.05)
    signals = strategy.generate_signals(data)
    print("Signals generated.")

    # Count and print the number of Buy and Sell signals
    num_buy = (signals == 1).sum()
    num_sell = (signals == -1).sum()
    print(f"Number of Buy Signals: {num_buy}")
    print(f"Number of Sell Signals: {num_sell}")

    # Run the backtesting framework using the data and generated signals
    backtester = Backtester(data, signals)
    portfolio = backtester.run_backtest()
    backtester.print_performance()
    
    # Plot the portfolio value over time
    plt.figure(figsize=(10,6))
    plt.plot(portfolio.index, portfolio, label="Portfolio Value")
    plt.title("Backtested Portfolio Value")
    plt.xlabel("Date")
    plt.ylabel("Portfolio Value")
    plt.legend()
    
    # Only display the plot if not running in a Jupyter/IPython environment
    if 'ipykernel' not in sys.modules:
        plt.show()
    
    print("Pipeline run complete!")

if __name__ == "__main__":
    main()