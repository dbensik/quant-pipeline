import requests
import pandas as pd
import streamlit as st
from typing import Optional, List, Any

class ApiClient:
    """
    Client for interacting with the Quant Pipeline API.
    Handles fetching results and deserializing them back into usable Python objects.
    """
    
    def __init__(self, base_url: str = "http://127.0.0.1:8001"):
        self.base_url = base_url.rstrip("/")

    def get_result_files(self) -> List[str]:
        """Fetches the list of available result files from the API."""
        try:
            response = requests.get(f"{self.base_url}/results", timeout=5)
            if response.status_code == 200:
                return response.json()
            else:
                st.error(f"API Error: {response.status_code} - {response.text}")
                return []
        except requests.exceptions.RequestException as e:
            st.error(f"Could not connect to API at {self.base_url}: {e}")
            return []

    def get_result_data(self, filename: str) -> Optional[Any]:
        """Fetches and deserializes a specific result file."""
        try:
            response = requests.get(f"{self.base_url}/results/{filename}", timeout=10)
            if response.status_code == 200:
                data = response.json()
                return self._deserialize_data(data)
            else:
                st.error(f"API Error: {response.status_code} - {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            st.error(f"Could not connect to API: {e}")
            return None

    def _deserialize_data(self, data: Any) -> Any:
        """
        Recursively converts JSON structures back into Pandas objects where appropriate.
        Expects DataFrames to be serialized with orient='split'.
        """
        if isinstance(data, dict):
            # Check if this dict represents a serialized DataFrame
            # Pandas 'split' orientation has 'index', 'columns', 'data' keys (and sometimes 'name')
            if all(k in data for k in ("index", "columns", "data")):
                try:
                    df = pd.DataFrame(
                        data=data["data"],
                        index=data["index"],
                        columns=data["columns"]
                    )
                    # Attempt to convert index to datetime if it looks like one
                    if "Timestamp" in str(df.index.name) or (len(df.index) > 0 and isinstance(df.index[0], str) and df.index[0].count("-") == 2):
                       try:
                           df.index = pd.to_datetime(df.index)
                       except:
                           pass
                    return df
                except Exception:
                    # If conversion fails, treat as normal dict
                    pass
            
            # Recursive step for normal dicts
            return {k: self._deserialize_data(v) for k, v in data.items()}
        
        elif isinstance(data, list):
            return [self._deserialize_data(v) for v in data]
        
        return data
