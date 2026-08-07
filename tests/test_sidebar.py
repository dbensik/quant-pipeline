from unittest.mock import MagicMock, patch

import pytest
import streamlit as st

# --- The class we are testing ---
from dashboard_app.ui_components.sidebar import Sidebar


# Pytest fixture to set up a mock environment for each test
@pytest.fixture
def mock_env():
    """Sets up a mocked environment for testing the Sidebar."""
    # Mock the managers that the Sidebar depends on
    mock_db_manager = MagicMock()
    mock_results_manager = MagicMock()
    mock_watchlist_manager = MagicMock()
    mock_portfolio_manager = MagicMock()

    # Configure the mocks to return some sample data
    mock_db_manager.get_tickers_by_asset_type.return_value = ["AAPL", "MSFT", "GOOG"]
    mock_watchlist_manager.load.return_value = {"My Watchlist": ["TSLA", "NVDA"]}
    mock_portfolio_manager.get_all_portfolios.return_value = {
        "My Portfolio": {"constituents": ["BTC-USD", "ETH-USD"]}
    }
    mock_portfolio_manager.get_portfolio_state.return_value = {
        "cash": 100000.0,
        "positions": {"AAPL": {"quantity": 10, "average_price": 150.0}}
    }
    all_db_tickers = [
        "AAPL",
        "MSFT",
        "GOOG",
        "TSLA",
        "NVDA",
        "BTC-USD",
        "ETH-USD",
        "SPY",
    ]

    # Before each test, clear Streamlit's session state
    st.session_state.clear()
    st.session_state["selections"] = {}

    # The strategy dropdown and its parameter widgets are now generated from the
    # API's registry-backed catalogue (Phase 3), so the sidebar needs a client.
    # A stub keeps this a unit test — no live API required.
    mock_api_client = MagicMock()
    mock_api_client.get_strategies.return_value = [
        {
            "id": "buy_and_hold",
            "display_name": "Buy and Hold",
            "description": "baseline",
            "input_contract": "single",
            "params": [],
            "caveat": None,
        },
        {
            "id": "mean_reversion",
            "display_name": "Mean Reversion",
            "description": "fade extremes",
            "input_contract": "single",
            "params": [
                {"name": "window", "type": "int", "default": 20, "label": "Window",
                 "description": "", "minimum": 2, "maximum": 500},
            ],
            "caveat": None,
        },
    ]

    sidebar = Sidebar(
        db_manager=mock_db_manager,
        results_manager=mock_results_manager,
        watchlist_manager=mock_watchlist_manager,
        portfolio_manager=mock_portfolio_manager,
        all_db_tickers=all_db_tickers,
        api_client=mock_api_client,
    )
    return sidebar


# --- Test Cases ---


def test_sidebar_initialization(mock_env):
    """Tests if the sidebar initializes correctly with its dependencies."""
    assert mock_env is not None
    assert mock_env.db_manager is not None
    assert "My Watchlist" in mock_env.watchlists
    assert "My Portfolio" in mock_env.portfolios


@patch("streamlit.sidebar")  # Mock all calls to st.sidebar
@patch("streamlit.button")   # Mock generic st.button used inside tabs
@patch("streamlit.radio")
def test_run_backtest_button_click(mock_radio, mock_button, mock_sidebar, mock_env):
    """
    Tests if the 'run_analysis_request' flag is set in session_state
    when the user clicks the 'Run Backtest' button.
    """
    # Fix: Mock columns to return two items so unpacking doesn't fail
    mock_sidebar.columns.return_value = [MagicMock(), MagicMock()]
    # Fix: Mock tabs to return 5 items (Dash, Trade, Charts, Lab, Settings)
    mock_sidebar.tabs.return_value = [MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()]

    # Simulate the user selecting "Backtest" in Lab Mode, then "Individual Ticker" in Backtest Mode
    # "Backtest" (Lab Mode) -> "Individual Ticker" (Backtest Mode) -> "Buy and Hold" (Strategy)
    mock_radio.side_effect = ["Backtest", "Individual Ticker", "Buy and Hold"]
    
    # Simulate the user clicking the "Run Backtest" button
    mock_button.return_value = True

    # Run the render method
    mock_env.render()

    # Assert that the correct flag was set in the session state
    assert "run_analysis_request" in st.session_state
    assert st.session_state["run_analysis_request"] is True


@patch("streamlit.sidebar")  # Mock all calls to st.sidebar
@patch("streamlit.button")   # Mock generic st.button used inside tabs
@patch("streamlit.radio")
def test_run_stat_test_button_click(mock_radio, mock_button, mock_sidebar, mock_env):
    """
    Tests if the 'run_stat_test_request' flag is set when the
    'Run Statistical Test' button is clicked.
    """
    # Fix: Mock columns to return two items so unpacking doesn't fail
    mock_sidebar.columns.return_value = [MagicMock(), MagicMock()]
    # Fix: Mock tabs to return 5 items
    mock_sidebar.tabs.return_value = [MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()]

    # Simulate selecting "Stats" in Lab mode, then "Augmented Dickey-Fuller Test" in test type
    mock_radio.side_effect = ["Stats", "Augmented Dickey-Fuller Test"]
    
    # Simulate the user clicking the button
    mock_button.return_value = True

    mock_env.render()

    assert "run_stat_test_request" in st.session_state
    assert st.session_state["run_stat_test_request"] is True


@patch("streamlit.sidebar")
def test_create_portfolio_form_submission(mock_sidebar, mock_env):
    """
    Tests if a portfolio creation request is correctly handled
    when the user submits the 'create_portfolio_form'.
    """
    # Fix: Mock columns to return two items so unpacking doesn't fail
    mock_sidebar.columns.return_value = [MagicMock(), MagicMock()]
    mock_sidebar.tabs.return_value = [MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()]

    # To test a form, we need to mock the form context manager
    # and the widgets inside it.
    with patch("streamlit.sidebar.form") as mock_form, \
         patch("streamlit.form_submit_button") as mock_submit, \
         patch("streamlit.text_input") as mock_text_input:
        # Simulate user typing a name and submitting
        mock_text_input.return_value = "My New Portfolio"
        # Simulate the submit button being clicked
        mock_submit.return_value = True

        mock_env.render()

        # Check that the session state was updated with the request
        assert "create_portfolio_request" in st.session_state
        assert st.session_state["create_portfolio_request"] == "My New Portfolio"
