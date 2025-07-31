from abc import ABC, abstractmethod

import pandas as pd


class BaseMLModel(ABC):
    """
    An abstract base class for all machine learning models, ensuring a
    consistent API for training, prediction, and persistence.
    """

    def __init__(self, model_name: str, parameters: dict):
        self.model_name = model_name
        self.parameters = parameters
        self.model = None  # This will hold the trained model object (e.g., from sklearn, tensorflow)

    @abstractmethod
    def create_features(
        self, price_data: pd.DataFrame, fundamental_data: pd.DataFrame
    ) -> pd.DataFrame:
        """Prepares the feature set (X) and target (y) for the model."""
        pass

    @abstractmethod
    def train(self, X: pd.DataFrame, y: pd.Series):
        """Trains the machine learning model."""
        pass

    @abstractmethod
    def predict(self, X: pd.DataFrame) -> pd.Series:
        """Generates predictions from a trained model."""
        pass

    def save(self, path: str):
        """Saves the trained model to a file."""
        # Logic to serialize the model object (e.g., using pickle or joblib)...
        pass

    @classmethod
    def load(cls, path: str):
        """Loads a trained model from a file."""
        # Logic to deserialize a model object...
        pass
