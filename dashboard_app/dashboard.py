import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from backtesting.backtester import Backtester
from alpha_models.mean_reversion import MeanReversionStrategy

# Config
DB_PATH = 'quant_pipeline.db'
RAW_TABLE = 'price_data'
NORM_TABLE = 'price_data_normalized'

st.set_page_config(page_title="Quant Pipeline Dashboard", layout="wide")

st.title("📊 Quant Pipeline Dashboard")

# Sidebar controls
st.sidebar.header("Configuration")
symbols = st.sidebar.multiselect(
    "Select Symbols (Equity/Crypto)", [], help="Available tickers will appear once data is loaded.")
date_range = st.sidebar.date_input(
    "Date Range",
    [pd.to_datetime("2018-01-01"), pd.to_datetime("2024-12-31")]
)
strategy_name = st.sidebar.selectbox(
    "Strategy", ["Mean Reversion"], index=0
)

if st.sidebar.button("Load Symbols"):
    # Fetch unique tickers from db
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"SELECT DISTINCT Ticker FROM {RAW_TABLE}", conn)
    conn.close()
    symbols = df['Ticker'].tolist()
    st.sidebar.success("Loaded symbols from database.")

st.sidebar.markdown("---")
if st.sidebar.button("Run Backtest"):
    if not symbols:
        st.error("No symbols selected.")
    else:
        st.header("Backtest Results")
        for symbol in symbols:
            # Load raw data
            conn = sqlite3.connect(DB_PATH)
            df_raw = pd.read_sql(
                f"SELECT * FROM {RAW_TABLE} WHERE Ticker='{symbol}'", conn, parse_dates=['Date']
            ).set_index('Date')
            conn.close()

            # Run strategy
            strategy = MeanReversionStrategy(window=20, threshold=0.05)
            signals = strategy.generate_signals(df_raw)
            backtester = Backtester(df_raw, signals)
            portfolio = backtester.run_backtest()

            # Display chart
            st.subheader(f"{symbol} Portfolio Value")
            fig, ax = plt.subplots()
            portfolio.plot(ax=ax)
            ax.set_xlabel("Date")
            ax.set_ylabel("Portfolio Value")
            st.pyplot(fig)

# Main metrics view
st.header("Normalized Price Series")
for symbol in symbols:
    conn = sqlite3.connect(DB_PATH)
    df_norm = pd.read_sql(
        f"SELECT Date, Normalized FROM {NORM_TABLE} WHERE Ticker='{symbol}'", conn, parse_dates=['Date']
    ).set_index('Date')
    conn.close()
    st.line_chart(df_norm, height=250, use_container_width=True)

st.sidebar.write("Data last updated on pipeline run.")
