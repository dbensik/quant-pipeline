import numpy as np
import pandas as pd
from .base_model import BaseAlphaModel

try:
    from sklearn.ensemble import RandomForestClassifier
except ImportError:
    RandomForestClassifier = None

class RandomForestStrategy(BaseAlphaModel):
    """
    Supervised Learning Strategy using Random Forest.
    
    Logic:
    - Features: Lagged Returns, Volatility, Momentum.
    - Target: Next day's return sign (Up/Down).
    - Train on past data, Predict tomorrow.
    """

    def __init__(self, n_estimators: int = 100, lookback_window: int = 5):
        self.n_estimators = n_estimators
        self.lookback_window = lookback_window
        self.model = None

    def prepare_features(self, df: pd.DataFrame):
        data = df.copy()
        data["returns"] = data["Close"].pct_change()
        
        # Features
        for lag in range(1, self.lookback_window + 1):
            data[f"lag_{lag}"] = data["returns"].shift(lag)
        
        data["volatility"] = data["returns"].rolling(window=20).std()
        data["momentum"] = data["Close"] / data["Close"].shift(20) - 1
        
        # Target: 1 if next day return is positive, 0 otherwise
        data["target"] = np.where(data["returns"].shift(-1) > 0, 1, 0)
        
        return data.dropna()

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        if RandomForestClassifier is None:
            raise ImportError("scikit-learn is required for this strategy.")

        # 1. Prepare Data
        features_df = self.prepare_features(price_data)
        
        feature_cols = [c for c in features_df.columns if c not in ["target", "returns"]]
        X = features_df[feature_cols]
        y = features_df["target"]
        
        # 2. Train (For template, we train on the entire history to demonstrate mechanism)
        # Note: In production, should use Walk-Forward execution to avoid look-ahead bias
        self.model = RandomForestClassifier(n_estimators=self.n_estimators, random_state=42)
        self.model.fit(X, y)
        
        # 3. Predict
        predictions = self.model.predict(X)
        
        # 4. Construct Signals
        signals = pd.DataFrame(index=features_df.index)
        signals["position"] = predictions # 1 (Long) or 0 (Flat)
        
        # Re-index to match original timeframe (fill NANs for initial lookback)
        signals = signals.reindex(price_data.index).fillna(0)
        
        final_signals = pd.DataFrame(index=price_data.index)
        final_signals["signal"] = signals["position"].diff().fillna(0)
        
        return final_signals
