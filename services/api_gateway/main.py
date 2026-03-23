import pickle
import os
import pandas as pd
from typing import List, Any
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from config.settings import RESULTS_DIR

app = FastAPI(title="Quant Pipeline API", version="1.0.0")

# Enable CORS for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def serialize_for_api(data: Any) -> Any:
    """
    Recursively converts Pandas objects to JSON-friendly dicts (split orientation).
    """
    if isinstance(data, pd.DataFrame):
        # Convert index to string if it's datetime, to ensure JSON serializability
        df = data.copy()
        if isinstance(df.index, pd.DatetimeIndex):
             df.index = df.index.strftime("%Y-%m-%d %H:%M:%S")
        
        # 'split' gives {index: [], columns: [], data: []}
        return df.to_dict(orient="split")
    
    elif isinstance(data, pd.Series):
        if isinstance(data.index, pd.DatetimeIndex):
            data.index = data.index.strftime("%Y-%m-%d %H:%M:%S")
        return data.to_dict() # Series to dict is usually key-value
        
    elif isinstance(data, dict):
        return {k: serialize_for_api(v) for k, v in data.items()}
    
    elif isinstance(data, list):
        return [serialize_for_api(v) for v in data]
        
    return data

@app.get("/")
def health_check():
    return {"status": "ok", "service": "Quant Pipeline API Gateway"}

@app.get("/results", response_model=List[str])
def list_results():
    """Returns a list of available result files."""
    if not os.path.isdir(RESULTS_DIR):
        return []
    
    files = [f for f in os.listdir(RESULTS_DIR) if f.endswith(".pkl")]
    return sorted(files)

@app.get("/results/{filename}")
def get_result(filename: str):
    """Loads a result file, deserializes pickle, and returns JSON."""
    if not filename.endswith(".pkl"):
        filename += ".pkl"
        
    filepath = RESULTS_DIR / filename
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="Result file not found")
        
    try:
        with open(filepath, "rb") as f:
            data = pickle.load(f)
        
        # Convert to JSON-safe format
        json_data = serialize_for_api(data)
        return json_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load result: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    # Run on port 8000 as expected by ApiClient
    uvicorn.run(app, host="0.0.0.0", port=8000)
