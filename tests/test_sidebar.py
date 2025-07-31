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

    sidebar = Sidebar(
        db_manager=mock_db_manager,
        results_manager=mock_results_manager,
        watchlist_manager=mock_watchlist_manager,
        portfolio_manager=mock_portfolio_manager,
        all_db_tickers=all_db_tickers,
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
def test_run_backtest_button_click(mock_sidebar, mock_env):
    """
    Tests if the 'run_analysis_request' flag is set in session_state
    when the user clicks the 'Run Backtest' button.
    """
    # Simulate the user selecting the "Backtest / Screener" radio button
    mock_sidebar.radio.return_value = "Backtest / Screener"
    # Simulate the user clicking the "Run Backtest" button
    mock_sidebar.button.return_value = True

    # Run the render method
    mock_env.render()

    # Assert that the correct flag was set in the session state
    assert "run_analysis_request" in st.session_state
    assert st.session_state["run_analysis_request"] is True


@patch("streamlit.sidebar")  # Mock all calls to st.sidebar
def test_run_stat_test_button_click(mock_sidebar, mock_env):
    """
    Tests if the 'run_stat_test_request' flag is set when the
    'Run Statistical Test' button is clicked.
    """
    # Simulate the user selecting the "Statistical Tests" radio button
    mock_sidebar.radio.return_value = "Statistical Tests"
    # Simulate the user clicking the button
    mock_sidebar.button.return_value = True

    mock_env.render()

    assert "run_stat_test_request" in st.session_state
    assert st.session_state["run_stat_test_request"] is True


@patch("streamlit.sidebar")
def test_create_portfolio_form_submission(mock_sidebar, mock_env):
    """
    Tests if a portfolio creation request is correctly handled
    when the user submits the 'create_portfolio_form'.
    """
    # To test a form, we need to mock the form context manager
    # and the widgets inside it.
    with patch("streamlit.sidebar.form") as mock_form:
        # Simulate user typing a name and submitting
        mock_sidebar.text_input.return_value = "My New Portfolio"
        mock_sidebar.form_submit_button.return_value = True

        mock_env.render()

        # Check that the session state was updated with the request
        assert "create_portfolio_request" in st.session_state
        assert st.session_state["create_portfolio_request"] == "My New Portfolio"
