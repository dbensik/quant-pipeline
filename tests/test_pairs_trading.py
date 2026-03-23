import pandas as pd
import numpy as np
import pytest
from alpha_models.pairs_trading import PairsTradingStrategy

@pytest.fixture
def strategy():
    return PairsTradingStrategy(window=5, threshold=1.0)

def test_initialization():
    """Test proper initialization and parameter inputs."""
    strategy = PairsTradingStrategy(window=10, threshold=2.0)
    assert strategy.window == 10
    assert strategy.threshold == 2.0

    with pytest.raises(ValueError, match="Window must be greater than 1"):
        PairsTradingStrategy(window=1)

    with pytest.raises(ValueError, match="Threshold must be a positive number"):
        PairsTradingStrategy(threshold=-1)

def test_input_validation(strategy):
    """Test that the strategy validates input dataframe columns."""
    # Dataframe with 1 column (should fail)
    df_one = pd.DataFrame({"A": [1, 2, 3]})
    with pytest.raises(ValueError, match="requires a DataFrame with exactly two price columns"):
        strategy.generate_signals(df_one)

    # Dataframe with 3 columns (should fail)
    df_three = pd.DataFrame({"A": [1, 2], "B": [1, 2], "C": [1, 2]})
    with pytest.raises(ValueError, match="requires a DataFrame with exactly two price columns"):
        strategy.generate_signals(df_three)

def test_signal_generation_logic(strategy):
    """
    Test the core signal logic:
    Spread = A / B
    Z-Score = (Spread - Mean) / Std
    """
    # Create controlled data
    # Asset B is constant 1.0, so Spread = Asset A price.
    # We want a sequence that moves:
    # 1. Start at mean
    # 2. Move up (Spread > Mean) -> Z > Threshold (1.0) -> Short Signal
    # 3. Move down back to mean -> Exit Signal
    # 4. Move down (Spread < Mean) -> Z < -Threshold (-1.0) -> Long Signal
    
    # Window is 5.
    
    # Index: 0, 1, 2, 3, 4 -> Rolling window calc starts at index 4
    # Prices: 10, 10, 10, 10, 10 -> Mean=10, Std=0 (NaN z-score handling check?)
    # Let's make it vary slightly to avoid div by zero if std=0, though pandas handles it (NaN).
    
    data = {
        "A": [10.0, 10.0, 10.0, 10.0, 10.0,  # Stable period (Mean ~10)
              12.0, 12.0,                    # Spike up (Z-score should rise)
              10.0,                          # Return to mean
              8.0, 8.0],                     # Spike down
        "B": [1.0] * 10
    }
    df = pd.DataFrame(data)
    
    # We need to ensure we don't get NaNs that break everything, or that we handle them.
    # The first (window-1) values will be NaN for rolling metrics.
    
    signals = strategy.generate_signals(df)
    
    assert "A" in signals.columns
    assert "B" in signals.columns
    
    # Check stable period (index 0-4) -> Should be 0 signals (or NaN/0 at start)
    assert signals.iloc[4]["A"] == 0.0 
    
    # Note: Precise numeric testing of Z-score requires exact std dev calc.
    # Instead, let's verify directionality if we force a massive move.
    
    # Massive spike up -> Short A (-1), Long B (1)
    # 10,10,10,10,10 -> Mean=10, Std=0.
    # Next is 20. Mean=(10+10+10+10+20)/5 = 12. Std approx 4.47.
    # Spread=20. Z = (20-12)/4.47 = 1.78 > Threshold(1.0). Signal: Short Spread.
    
    df_extreme = pd.DataFrame({
        "A": [10, 10, 10, 10, 10, 20, 10, 0],
        "B": [1,  1,  1,  1,  1,  1,  1, 1]
    })
    
    sigs_extreme = strategy.generate_signals(df_extreme)
    
    # Index 5 (Value 20): Spread=20. Z > 1.0. 
    # Action: Short Spread -> Sell A, Buy B.
    # Signal is the DIFF of positions. Position goes 0 -> -1. Signal = -1.
    assert sigs_extreme.iloc[5]["A"] == -1.0 # Short A
    assert sigs_extreme.iloc[5]["B"] == 1.0  # Long B
    
    # Index 6 (Value 10): Back to mean. Z ~ 0. Exit condition |Z| < 0.5.
    # Position goes -1 -> 0. Signal (Diff) = +1 (Cover Short).
    assert sigs_extreme.iloc[6]["A"] == 1.0  # Buy back A
    
    # Index 7 (Value 0): Crash. Spread=0. Mean=(10+10+10+20+0)/5 = 10. Spread < Mean.
    # Z will be negative.
    # Position 0 -> 1 (Long Spread -> Buy A). Signal = 1.
    assert sigs_extreme.iloc[7]["A"] == 1.0
    assert sigs_extreme.iloc[7]["B"] == -1.0

def test_zero_variance_handling(strategy):
    """Ensure strategy doesn't crash if price is constant (std=0)."""
    df_const = pd.DataFrame({
        "A": [10.0] * 20,
        "B": [1.0] * 20
    })
    # Z-score will be NaN because std=0.
    # np.where comparisons with NaN usually return False.
    # Signals should remain 0.
    signals = strategy.generate_signals(df_const)
    assert (signals["A"] == 0).all()
