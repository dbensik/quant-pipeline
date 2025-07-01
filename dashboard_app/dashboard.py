from sqlite3 import Connection

import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from backtesting.backtester import Backtester
from alpha_models.mean_reversion import MeanReversionStrategy

# --- Configuration ---
DB_PATH = 'quant_pipeline.db'
RAW_TABLE = 'price_data'
NORM_TABLE = 'price_data_normalized'

st.set_page_config(page_title="Quant Pipeline Dashboard", layout="wide")


# --- Caching & Data Loading Functions ---

@st.cache_resource
def get_db_connection(db_path: object) -> Connection:
    """Creates and caches a single database connection."""
    return sqlite3.connect(db_path, check_same_thread=False)


@st.cache_data
def get_available_tickers(_conn):
    """
    Fetches and caches the list of unique tickers from the database.
    The _conn argument ensures this function reruns if the connection changes.
    """
    return pd.read_sql(f"SELECT DISTINCT Ticker FROM {RAW_TABLE}", _conn)['Ticker'].tolist()


@st.cache_data
def load_price_data(_conn, ticker, start_date, end_date):
    """
    Loads raw and normalized price data for a given ticker and date range.
    Caches the result to avoid re-fetching from the database.
    """
    query = f"""
    SELECT
        r.Date,
        r.Open,
        r.High,
        r.Low,
        r.Close,
        r.Volume,
        n.Normalized
    FROM
        {RAW_TABLE} r
    LEFT JOIN
        {NORM_TABLE} n ON r.Ticker = n.Ticker AND r.Date = n.Date
    WHERE
        r.Ticker = ? AND r.Date BETWEEN ? AND ?
    ORDER BY
        r.Date;
    """
    # Use parameterized queries to prevent SQL injection
    df = pd.read_sql(query, _conn, params=(ticker, start_date, end_date), parse_dates=['Date'])
    return df.set_index('Date')


# --- Main App ---

st.title("📊 Quant Pipeline Dashboard")

# Establish a single, cached database connection
conn = get_db_connection(DB_PATH)

# --- Sidebar Controls ---
st.sidebar.header("Configuration")

# Load symbols automatically and cache them
available_tickers = get_available_tickers(conn)
selected_symbols = st.sidebar.multiselect(
    "Select Symbols (Equity/Crypto)", available_tickers, default=available_tickers[:1]
)

# Use pd.Timestamp for robust date handling
start_date, end_date = st.sidebar.date_input(
    "Date Range",
    [pd.to_datetime("2020-01-01"), pd.to_datetime("2023-12-31")],
)
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

st.sidebar.markdown("---")
st.sidebar.header("Strategy Parameters")
strategy_name = st.sidebar.selectbox("Strategy", ["Mean Reversion"], index=0)
mr_window = st.sidebar.slider("Mean Reversion Window", 5, 100, 20)
mr_threshold = st.sidebar.slider("Mean Reversion Threshold", 0.01, 0.20, 0.05, 0.01)

# --- Main Content ---

# --- Main Content ---

if not selected_symbols:
    st.warning("Please select at least one symbol from the sidebar.")
else:
    # Display Normalized Price Series Charts first
    st.header("Normalized Price Series")
    for symbol in selected_symbols:
        st.subheader(f"{symbol}")
        price_data = load_price_data(conn, symbol, start_date_str, end_date_str)

        if price_data.empty:
            st.write("No data available for the selected date range.")
            continue

        # --- IMPROVEMENT: Check for valid normalized data and provide a fallback ---
        if price_data['Normalized'].isnull().all():
            st.warning(
                f"No 'Normalized' data found for **{symbol}**. "
                "This can happen if the normalization step hasn't been run. "
                "Displaying raw 'Close' price as a fallback."
            )
            # Plot the raw 'Close' price instead
            st.line_chart(price_data['Close'], height=250, use_container_width=True)
        else:
            # Display the normalized price chart as intended
            st.line_chart(price_data['Normalized'], height=250, use_container_width=True)

    st.markdown("---")

    # Backtesting Section
    st.header("Backtest Results")
    if st.button("Run Backtest for Selected Symbols"):
        for symbol in selected_symbols:
            with st.container(border=True):  # Use a container for each result
                st.subheader(f"Backtest for: {symbol}")

                # Load data for the backtest (will be retrieved from cache)
                backtest_data = load_price_data(conn, symbol, start_date_str, end_date_str)

                if backtest_data.empty:
                    st.write("No data to backtest.")
                    continue

                # Run strategy with user-defined parameters
                strategy = MeanReversionStrategy(window=mr_window, threshold=mr_threshold)
                signals = strategy.generate_signals(backtest_data)

                backtester = Backtester(backtest_data, signals)
                portfolio = backtester.run_backtest()
                # POTENTIAL BUG FIX: Verify this method name in your Backtester class
                stats = backtester.calculate_performance_metrics()

                # Display results in two columns
                col1, col2 = st.columns([2, 1])

                with col1:
                    st.write("Portfolio Value Over Time")
                    fig, ax = plt.subplots(figsize=(10, 5))
                    portfolio.plot(ax=ax, title=f"{symbol} Portfolio Value")
                    ax.set_xlabel("Date")
                    ax.set_ylabel("Portfolio Value")
                    plt.grid(True)
                    st.pyplot(fig)

                with col2:
                    st.write("Performance Metrics")
                    # Use st.metric for a nicer display
                    st.metric("Final Portfolio Value", f"${stats['Final Value']:,.2f}")
                    st.metric("Total Return", f"{stats['Total Return']:.2%}")
                    st.metric("Max Drawdown", f"{stats['Max Drawdown']:.2%}")
                    st.metric("Sharpe Ratio", f"{stats['Sharpe Ratio']:.2f}")

st.sidebar.markdown("---")
st.sidebar.info("Data is loaded from the pipeline database and cached for performance.")
