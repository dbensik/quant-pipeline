"""
AnalysisController tests.

Rewritten for the Phase 3 cutover. The controller no longer builds strategy
objects from display names — that if/elif chain (create_strategy_model) was one
of three duplicated copies of strategy identity and is gone. Single-symbol
backtests now go through the API, and the sidebar emits registry ids and
registry-named parameters directly.

What is tested here:
  * resolve_strategy   — translating sidebar selections into (id, params)
  * _build_local_model — the multi-asset portfolio path, which has no API
                         endpoint and so still builds a model in-process
"""

from unittest.mock import MagicMock, Mock, patch

import pytest

from alpha_models.buy_and_hold import BuyAndHoldStrategy
from alpha_models.index_rebalancing import IndexRebalancingStrategy
from alpha_models.mean_reversion import MeanReversionStrategy
from dashboard_app.controllers.analysis_controller import AnalysisController


@pytest.fixture
def controller():
    price_handler = Mock()
    portfolio_manager = Mock()
    api_client = MagicMock()
    return AnalysisController(price_handler, portfolio_manager, api_client=api_client)


# ---------------------------------------------------------------------------
# resolve_strategy
# ---------------------------------------------------------------------------

def test_resolve_strategy_passes_id_and_params_through(controller):
    """The sidebar now supplies registry ids and registry-named params."""
    selections = {
        "strategy_id": "mean_reversion",
        "strategy_params": {"window": 30, "threshold": 2.5},
    }
    strategy_id, params = controller.resolve_strategy(selections)
    assert strategy_id == "mean_reversion"
    assert params == {"window": 30, "threshold": 2.5}


def test_resolve_strategy_drops_optimization_ranges(controller):
    """
    Optimisation emits <name>_range keys for the in-process grid search. They
    are not valid backtest params — the API rejects unknown names with a 422 —
    so a single run must not forward them.
    """
    selections = {
        "strategy_id": "mean_reversion",
        "strategy_params": {"window": 20, "window_range": (5, 40)},
    }
    _, params = controller.resolve_strategy(selections)
    assert params == {"window": 20}


def test_resolve_strategy_without_selection_errors(controller):
    """
    An empty strategy list means the API is unreachable. That must surface as an
    error, not a silent no-op that looks like an empty result.
    """
    with patch("dashboard_app.controllers.analysis_controller.st") as mock_st:
        strategy_id, params = controller.resolve_strategy({})
    assert strategy_id is None
    assert params == {}
    mock_st.error.assert_called_once()


# ---------------------------------------------------------------------------
# _build_local_model — multi-asset portfolio path only
# ---------------------------------------------------------------------------

def test_build_local_model_uses_registry_defaults(controller):
    model = controller._build_local_model("buy_and_hold", {}, {})
    assert isinstance(model, BuyAndHoldStrategy)


def test_build_local_model_applies_params(controller):
    model = controller._build_local_model(
        "mean_reversion", {"window": 30, "threshold": 2.5}, {}
    )
    assert isinstance(model, MeanReversionStrategy)
    assert model.window == 30
    assert model.threshold == 2.5


def test_build_local_model_string_param(controller):
    model = controller._build_local_model(
        "index_rebalancing", {"rebalance_frequency": "W"}, {}
    )
    assert isinstance(model, IndexRebalancingStrategy)
    assert model.rebalance_frequency == "W"


def test_build_local_model_unknown_strategy_returns_none(controller):
    with patch("dashboard_app.controllers.analysis_controller.st") as mock_st:
        model = controller._build_local_model("no_such_strategy", {}, {})
    assert model is None
    mock_st.error.assert_called_once()


def test_build_local_model_invalid_params_returns_none(controller):
    """Strategies validate their own params; the controller surfaces that."""
    with patch("dashboard_app.controllers.analysis_controller.st") as mock_st:
        model = controller._build_local_model(
            "ma_crossover", {"short_window": 100, "long_window": 50}, {}
        )
    assert model is None
    mock_st.error.assert_called_once()


def test_cointegrated_requires_portfolio_weights(controller):
    """
    Cointegrated Mean Reversion needs Johansen weights the registry cannot
    default. Without them the controller must refuse rather than construct a
    meaningless model.
    """
    controller.portfolio_manager.portfolios = {"My Portfolio": {}}
    with patch("dashboard_app.controllers.analysis_controller.st") as mock_st:
        model = controller._build_local_model(
            "cointegrated_mean_reversion", {}, {"source_name": "My Portfolio"}
        )
    assert model is None
    mock_st.error.assert_called_once()


def test_cointegrated_builds_with_weights(controller):
    controller.portfolio_manager.portfolios = {
        "My Portfolio": {"weights": {"AAPL": 0.6, "MSFT": -0.4}}
    }
    model = controller._build_local_model(
        "cointegrated_mean_reversion",
        {"window": 25, "threshold": 1.8},
        {"source_name": "My Portfolio"},
    )
    assert model is not None
    assert model.weights == {"AAPL": 0.6, "MSFT": -0.4}
    assert model.window == 25
