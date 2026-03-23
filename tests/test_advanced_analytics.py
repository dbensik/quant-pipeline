import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import numpy as np
import streamlit as st
from dashboard_app.controllers.statistics_controller import StatisticsController
from dashboard_app.price_data_handler import PriceDataHandler

class TestAdvancedAnalytics(unittest.TestCase):
    def setUp(self):
        # Mock Session State
        if not hasattr(st, "session_state"):
            st.session_state = {}
        
        # Mock Price Data Handler
        self.mock_price_handler = MagicMock(spec=PriceDataHandler)
        self.controller = StatisticsController(self.mock_price_handler)
        
    def test_monte_carlo_simulation(self):
        # Setup mock data (1 asset, 100 days)
        dates = pd.date_range(start="2023-01-01", periods=100)
        data = pd.DataFrame({
            "AAPL": np.linspace(100, 150, 100) + np.random.normal(0, 1, 100)
        }, index=dates)
        
        # Mock get_prices return
        self.mock_price_handler.get_prices.return_value = data
        
        selections = {
            "selected_symbols": ["AAPL"],
            "start_date": dates[0],
            "end_date": dates[-1],
            "mc_simulations": 100,
            "mc_horizon": 30
        }
        
        # Run
        self.controller.run_monte_carlo(selections)
        
        # Verify
        results = st.session_state.get("stat_test_run")
        self.assertIsNotNone(results)
        self.assertEqual(results["test_type"], "Monte Carlo Simulation")
        self.assertIn("paths", results["results"])
        self.assertIn("stats", results["results"])
        
        # Check integrity
        # Horizon is 30, so paths should have 31 rows (including t=0)
        # Note: Implementation logic was: 
        # price_paths = np.vstack([np.ones((1, n_simulations)) * last_price, np.zeros((time_horizon, n_simulations))])
        # So final shape is (hor+1, sim)
        self.assertEqual(results["results"]["paths"].shape, (31, 100))

    def test_cluster_analysis(self):
        # Setup mock data (4 assets to allow 2 clusters)
        dates = pd.date_range(start="2023-01-01", periods=50)
        # Group 1: Correlated
        base_1 = np.linspace(100, 200, 50)
        # Group 2: Inverse
        base_2 = np.linspace(200, 100, 50)
        
        data = pd.DataFrame({
            "A": base_1 + np.random.normal(0, 2, 50),
            "B": base_1 + np.random.normal(0, 2, 50),
            "C": base_2 + np.random.normal(0, 2, 50),
            "D": base_2 + np.random.normal(0, 2, 50),
        }, index=dates)
        
        # Mock return (get_prices returns dict of DFs usually, but here controller reconstructs it)
        # Wait, get_prices returns dict of DataFrames (cols=OHLCV).
        # But controller does: pd.DataFrame(price_df_dict).dropna() which might fail if structure isn't perfect.
        # Controller expects get_prices to return dict {Ticker: DF(Close)}.
        # Let's fix mock for get_prices to return dict of DFs as expected by controller's get_test_data helper?
        # Actually controller's get_test_data calls self.price_handler.get_prices(all_symbols...)
        # and expects it to return a dict where values are Series or DF with 1 col?
        # Let's check PriceDataHandler.get_prices in file... 
        # It returns "A pandas DataFrame where the index is the date and each column represents the 'Close' price"
        # Wait, check controller line 52:
        # price_df_dict = self.price_handler.get_prices(...)
        # if not price_df_dict: ...
        # df = pd.DataFrame(price_df_dict).dropna()
        # If get_prices returns a single DF with cols as tickers (which it does based on Step 1135), then 
        # pd.DataFrame(df) works fine.
        
        # However, mock return needs to match that.
        self.mock_price_handler.get_prices.return_value = data
        
        selections = {
            "selected_symbols": ["A", "B", "C", "D"],
            "start_date": dates[0],
            "end_date": dates[-1],
            "cluster_k": 2
        }
        
        # Run
        self.controller.run_cluster_analysis(selections)
        
        # Verify
        results = st.session_state.get("stat_test_run")
        self.assertIsNotNone(results)
        self.assertEqual(results["test_type"], "Cluster Analysis (K-Means)")
        
        clusters = results["results"]["clusters"]
        self.assertEqual(len(clusters), 4) # 4 tickers
        self.assertIn("Cluster", clusters.columns)
        self.assertIn("PC1", clusters.columns)

if __name__ == "__main__":
    unittest.main()
