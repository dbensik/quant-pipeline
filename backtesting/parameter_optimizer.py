import pandas as pd
from backtesting.backtester import Backtester
from alpha_models.mean_reversion import MeanReversionStrategy
from alpha_models.moving_average_crossover import MovingAverageCrossoverStrategy

class ParameterOptimizer:
    """
    Runs a grid search optimization for a given strategy and parameter set.
    """

    def __init__(self, price_data: pd.DataFrame, strategy_type: str, param_grid: list, metric: str = "Sharpe Ratio"):
        self.price_data = price_data
        self.strategy_type = strategy_type
        self.param_grid = param_grid
        self.metric = metric
        self.backtester = Backtester()

    def run_optimization(self) -> pd.DataFrame:
        """
        Iterates through the parameter grid, runs backtests, and collects metrics.
        Returns a DataFrame of results.
        """
        results = []
        
        for params in self.param_grid:
            model = self._create_model(params)
            if not model:
                continue
                
            # Run backtest
            portfolio = self.backtester.run(self.price_data, model)
            stats = self.backtester.get_performance_metrics()
            
            # Combine params and stats into one dict record
            result_record = params.copy()
            result_record.update(stats)
            results.append(result_record)
            
        self.results_df = pd.DataFrame(results)
        return self.results_df

    def get_best_parameters(self) -> dict:
        """Returns the parameters that maximized the target metric."""
        if self.results_df is None or self.results_df.empty:
            return {}
            
        # Check if metric exists in results
        if self.metric not in self.results_df.columns:
            return {}
            
        # Find the row with the maximum value for the metric
        best_row = self.results_df.loc[self.results_df[self.metric].idxmax()]
        
        # Filter out the metric columns to return only params
        # We assume known metrics keys from PerformanceAnalyzer
        metric_keys = [
            "Total Return", "Annualized Return", "Annualized Volatility", 
            "Sharpe Ratio", "Sortino Ratio", "Max Drawdown"
        ]
        
        best_params = {
            k: v for k, v in best_row.items() 
            if k not in metric_keys
        }
        return best_params 

    def _create_model(self, params: dict):
        """Factory to create strategy instances from params."""
        if self.strategy_type == "Mean Reversion":
            return MeanReversionStrategy(
                window=params.get("window"),
                threshold=params.get("threshold")
            )
        elif self.strategy_type == "Moving Average Crossover":
            return MovingAverageCrossoverStrategy(
                short_window=params.get("short_window"),
                long_window=params.get("long_window")
            )
        return None
