from typing import Any
import pandas as pd

class DataSerializer:
    """Helper to serialize complex objects (like DataFrames) for JSON response."""

    @staticmethod
    def serialize(data: Any) -> Any:
        if isinstance(data, pd.DataFrame):
            # precise mode preserves indices and types better for frontend reconstruction
            return data.to_dict(orient="split")
        elif isinstance(data, dict):
            return {
                k: DataSerializer.serialize(v) for k, v in data.items()
            }
        elif isinstance(data, list):
            return [DataSerializer.serialize(v) for v in data]
        return data
