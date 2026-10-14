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
    - Walk-forward: every `retrain_every` bars, retrain on every row whose
      target is already known (expanding window), then predict only bars the
      model has not seen. Nothing is predicted before `train_window` rows.

    Until 2026-10-14 it trained once on the ENTIRE history and predicted that
    same history: with positions held (the event-signal fix, same day) SPY
    2016-2025 reached a Sharpe of 11.3 and TQQQ $3.7e17.
    """

    def __init__(
        self,
        n_estimators: int = 100,
        lookback_window: int = 5,
        train_window: int = 252,
        retrain_every: int = 63,
    ):
        if train_window < 2 or retrain_every < 1:
            raise ValueError("train_window must be >= 2 and retrain_every >= 1")
        self.n_estimators = n_estimators
        self.lookback_window = lookback_window
        self.train_window = train_window
        self.retrain_every = retrain_every
        self.model = None

    def prepare_features(self, df: pd.DataFrame):
        data = df.copy()
        data["returns"] = data["Close"].pct_change()

        # Features
        for lag in range(1, self.lookback_window + 1):
            data[f"lag_{lag}"] = data["returns"].shift(lag)

        data["volatility"] = data["returns"].rolling(window=20).std()
        data["momentum"] = data["Close"] / data["Close"].shift(20) - 1

        # Target: 1 if next day return is positive, 0 otherwise. The last row's
        # is unknown (filled 0 here): walk-forward never trains on it.
        data["target"] = np.where(data["returns"].shift(-1) > 0, 1, 0)

        return data.dropna()

    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        if RandomForestClassifier is None:
            raise ImportError("scikit-learn is required for this strategy.")

        features_df = self.prepare_features(price_data)
        feature_cols = [c for c in features_df.columns if c not in ["target", "returns"]]
        X = features_df[feature_cols].to_numpy()
        y = features_df["target"].to_numpy()

        # Row k is predicted at bar k's close by a model fitted on rows [0, k):
        # row k-1's target is bar k's return, known by then; row k's is not.
        # Retrain points are counted from the FIRST row, so they do not move
        # when later bars are added (the look-ahead contract test).
        position = np.zeros(len(features_df))
        for start in range(self.train_window, len(features_df), self.retrain_every):
            if len(np.unique(y[:start])) < 2:
                continue  # one class only: nothing to learn yet, stay flat
            self.model = RandomForestClassifier(
                n_estimators=self.n_estimators, random_state=42, n_jobs=-1
            )
            self.model.fit(X[:start], y[:start])
            stop = min(start + self.retrain_every, len(features_df))
            position[start:stop] = self.model.predict(X[start:stop])

        signals = pd.DataFrame({"position": position}, index=features_df.index)
        signals = signals.reindex(price_data.index).fillna(0)

        final_signals = pd.DataFrame(index=price_data.index)
        final_signals["signal"] = signals["position"].diff().fillna(0)

        return final_signals
