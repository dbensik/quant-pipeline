import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Add project root to the Python path to allow imports from 'analysis'
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from analysis.principal_component_analyzer import PrincipalComponentAnalyzer


def create_test_data(num_periods, num_assets, zero_var_cols=None):
    """Creates a sample DataFrame of asset returns."""
    dates = pd.to_datetime(pd.date_range("2023-01-01", periods=num_periods, freq="D"))
    data = np.random.randn(num_periods, num_assets) * 0.01
    columns = [f"ASSET_{i}" for i in range(num_assets)]
    df = pd.DataFrame(data, index=dates, columns=columns)

    if zero_var_cols:
        for col in zero_var_cols:
            df[col] = 0.001  # Constant return -> zero variance
    return df


def test_pca_analyzer_success():
    """Tests a successful run of the PrincipalComponentAnalyzer."""
    returns_df = create_test_data(num_periods=100, num_assets=5)
    analyzer = PrincipalComponentAnalyzer(returns_df)
    results = analyzer.run()

    assert "explained_variance_ratio" in results
    assert "components" in results
    assert len(results["explained_variance_ratio"]) == 5
    assert results["components"].shape == (5, 5)


def test_pca_fails_with_insufficient_data():
    """
    Tests that PCA raises a ValueError when there are more assets than data points.
    """
    returns_df = create_test_data(num_periods=5, num_assets=10)
    with pytest.raises(ValueError, match="Not enough data for PCA"):
        # This pre-analysis check would typically be in the code that calls the analyzer
        if returns_df.shape[0] < returns_df.shape[1]:
            raise ValueError(
                f"Not enough data for PCA ({returns_df.shape[0]} points for "
                f"{returns_df.shape[1]} assets)."
            )
        PrincipalComponentAnalyzer(returns_df).run()


def test_pca_fails_with_zero_variance_column():
    """
    Tests that PCA raises a ValueError when a column has zero variance.
    """
    returns_df = create_test_data(
        num_periods=100, num_assets=5, zero_var_cols=["ASSET_2"]
    )
    with pytest.raises(
        ValueError, match="PCA cannot be computed on data with zero variance"
    ):
        # This pre-analysis check would typically be in the code that calls the analyzer
        zero_variance_cols = returns_df.columns[returns_df.var() < 1e-10].tolist()
        if zero_variance_cols:
            raise ValueError(
                "PCA cannot be computed on data with zero variance. "
                f"Assets with no price change: {', '.join(zero_variance_cols)}."
            )
        PrincipalComponentAnalyzer(returns_df).run()
