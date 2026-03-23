from unittest.mock import Mock

import pytest
from dashboard_app.controllers.analysis_controller import AnalysisController
from alpha_models.buy_and_hold import BuyAndHoldStrategy
from alpha_models.mean_reversion import MeanReversionStrategy
from alpha_models.index_rebalancing import IndexRebalancingStrategy


@pytest.fixture
def controller():
    price_handler = Mock()
    portfolio_manager = Mock()
    return AnalysisController(price_handler, portfolio_manager)


def test_create_strategy_buy_and_hold(controller):
    """Test creation of BuyAndHoldStrategy."""
    params = {"strategy_type": "Buy and Hold"}
    model = controller.create_strategy_model(params)
    assert isinstance(model, BuyAndHoldStrategy)


def test_create_strategy_mean_reversion(controller):
    """Test creation of MeanReversionStrategy with custom params."""
    params = {
        "strategy_type": "Mean Reversion",
        "mr_window": 30,
        "mr_threshold": 2.5
    }
    model = controller.create_strategy_model(params)
    assert isinstance(model, MeanReversionStrategy)
    assert model.window == 30
    assert model.threshold == 2.5


def test_create_strategy_index_rebalancing(controller):
    """Test creation of IndexRebalancingStrategy."""
    params = {
        "strategy_type": "Index Rebalancing",
        "rebalance_freq": "W"
    }
    model = controller.create_strategy_model(params)
    assert isinstance(model, IndexRebalancingStrategy)
    assert model.rebalance_frequency == "W"


def test_create_unknown_strategy(controller):
    """Test that unknown strategy type returns None."""
    params = {"strategy_type": "Unknown Strategy"}
    model = controller.create_strategy_model(params)
    assert model is None
