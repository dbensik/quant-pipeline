"""
StatisticalAnalyzer — stationarity and integration order.

These use series whose integration order is known BY CONSTRUCTION rather than
real market data, so the assertions are about correctness of the test, not
about what AAPL happened to do.

Phase 5 — decommissioning Streamlit
"""

import numpy as np
import pandas as pd
import pytest

from analysis.statistical_analyzer import StatisticalAnalyzer

N = 600


@pytest.fixture
def analyzer() -> StatisticalAnalyzer:
    return StatisticalAnalyzer()


@pytest.fixture
def index() -> pd.DatetimeIndex:
    return pd.bdate_range("2023-01-01", periods=N)


@pytest.fixture
def random_walk(index) -> pd.Series:
    """I(1) — non-stationary in levels, stationary once differenced. Like a price."""
    rng = np.random.default_rng(0)
    return pd.Series(100 * np.cumprod(1 + rng.normal(0, 0.015, N)), index=index)


@pytest.fixture
def white_noise(index) -> pd.Series:
    """I(0) — stationary in levels."""
    rng = np.random.default_rng(1)
    return pd.Series(100 + rng.normal(0, 1, N), index=index)


@pytest.fixture
def double_integrated(index) -> pd.Series:
    """I(2) — still non-stationary after one difference."""
    rng = np.random.default_rng(2)
    return pd.Series(np.cumsum(np.cumsum(rng.normal(0, 1, N))), index=index)


def test_random_walk_is_not_reported_stationary(analyzer, random_walk):
    """
    THE regression. run_adf_test used to difference its input before testing —
    "returns are more likely to be stationary", which they are, almost always.
    So it reported "likely stationary" for EVERY input, including this random
    walk, and could never answer the question it is named for.
    """
    result = analyzer.run_adf_test(random_walk)
    assert result["is_stationary"] is False
    assert result["p-value"] > 0.05


def test_random_walk_is_identified_as_I1(analyzer, random_walk):
    """
    I(1) is what Engle-Granger and Johansen assume of their inputs. The old
    implementation could not establish it at all.
    """
    result = analyzer.run_adf_test(random_walk)
    assert result["integration_order"] == "I(1)"
    assert result["diff_is_stationary"] is True


def test_white_noise_is_identified_as_I0(analyzer, white_noise):
    result = analyzer.run_adf_test(white_noise)
    assert result["is_stationary"] is True
    assert result["integration_order"] == "I(0)"


def test_doubly_integrated_series_is_flagged_as_unsuitable(analyzer, double_integrated):
    """
    Neither levels nor first differences are stationary, so cointegration tests
    would be invalid — the result says so rather than reporting a bare p-value
    a caller might act on.
    """
    result = analyzer.run_adf_test(double_integrated)
    assert result["integration_order"] == "I(2) or higher"
    assert "not valid" in result["interpretation"]


def test_the_test_distinguishes_its_inputs(analyzer, random_walk, white_noise):
    """
    Guards against any future change that makes the verdict constant again —
    the exact failure mode of the original, which returned the same answer for
    everything.
    """
    walk = analyzer.run_adf_test(random_walk)
    noise = analyzer.run_adf_test(white_noise)
    assert walk["is_stationary"] != noise["is_stationary"]
    assert walk["integration_order"] != noise["integration_order"]


def test_interpretation_reports_both_p_values(analyzer, random_walk):
    result = analyzer.run_adf_test(random_walk)
    assert "Levels p-value" in result["interpretation"]
    assert "first-difference p-value" in result["interpretation"]


def test_keys_the_dashboard_reads_are_preserved(analyzer, random_walk):
    """
    dashboard_app/ui_components/statistical_analysis_tab.py formats
    "Test Statistic" and "p-value"; the fix must not break the Streamlit view
    while it still exists.
    """
    result = analyzer.run_adf_test(random_walk)
    for key in ("Test Statistic", "p-value", "Critical Values", "is_stationary"):
        assert key in result


def test_empty_series_returns_an_error(analyzer):
    assert "error" in analyzer.run_adf_test(pd.Series(dtype=float))


def test_short_series_returns_an_error(analyzer, index):
    assert "error" in analyzer.run_adf_test(pd.Series(range(5), index=index[:5]))
