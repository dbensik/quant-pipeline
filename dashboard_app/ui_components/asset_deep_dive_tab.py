import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf
from plotly.subplots import make_subplots

from dashboard_app.database_manager import DatabaseManager


class AssetDeepDiveTab:
    """
    Renders a dedicated, multi-faceted tab for in-depth research on a single asset,
    combining public data, technical indicators from our pipeline, and user notes.
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def render(self):
        """Main render method for the tab."""
        st.header("🔎 Asset Deep Dive")

        # Fetch the list of tickers *inside* the render method to ensure it's always fresh.
        all_db_tickers = self.db_manager.get_universe_tickers()

        selected_ticker = st.selectbox(
            "Select Ticker for Deep Dive",
            options=[""] + all_db_tickers,
            index=0,
            key="deep_dive_ticker_selector",
            help="Select an asset to view its profile, financials, and technicals.",
        )

        if not selected_ticker:
            st.info(
                "Please select a ticker from the dropdown above to begin your deep dive. "
                "You can add new tickers to the universe via the 'Data Pipeline & Universe' "
                "section in the sidebar."
            )
            return

        # --- Data Fetching and Validation ---
        try:
            # Use cached functions to avoid re-fetching data on every interaction
            info = self._get_ticker_info(selected_ticker)
            hist_df = self._get_history(selected_ticker, period="5y")

            if not info or "symbol" not in info:
                st.error(
                    f"Could not retrieve complete data for {selected_ticker}. The ticker may be delisted or invalid."
                )
                return

        except Exception as e:
            st.error(
                f"An error occurred while fetching data for {selected_ticker}: {e}"
            )
            return

        st.divider()

        # --- Main Layout ---
        self._render_profile_header(info)

        # --- REFACTOR: Use tabs for a cleaner, more organized layout ---
        tab1, tab2, tab3, tab4 = st.tabs(
            ["📈 Technicals", "💰 Financials", "📰 News", "📝 My Notes"]
        )

        with tab1:
            self._render_technical_analysis(selected_ticker, hist_df)
        with tab2:
            self._render_financial_statements(selected_ticker)
        with tab3:
            self._render_news(selected_ticker)
        with tab4:
            self._render_research_notes(selected_ticker)

    @st.cache_data(ttl=3600)  # Cache yfinance info call for an hour
    def _get_ticker_info(_self, ticker: str) -> dict:
        """Cached function to fetch and return the .info dictionary from yfinance."""
        return yf.Ticker(ticker).info

    @st.cache_data(ttl=3600)
    def _get_history(_self, ticker: str, period: str = "5y") -> pd.DataFrame:
        """Cached function to fetch historical price data."""
        return yf.Ticker(ticker).history(period=period)

    @st.cache_data(ttl=3600)
    def _get_financials(_self, ticker: str, quarterly: bool = False) -> tuple:
        """Cached function to fetch all financial statements."""
        ticker_obj = yf.Ticker(ticker)
        if quarterly:
            return (
                ticker_obj.quarterly_financials,
                ticker_obj.quarterly_balance_sheet,
                ticker_obj.quarterly_cashflow,
            )
        return ticker_obj.financials, ticker_obj.balance_sheet, ticker_obj.cashflow

    @st.cache_data(ttl=3600)
    def _get_news(_self, ticker: str) -> list:
        """Cached function to fetch news."""
        return yf.Ticker(ticker).news

    def _render_profile_header(self, info: dict):
        """Displays the company header, description, and key financial metrics."""
        st.subheader(f"{info.get('longName', 'N/A')} ({info.get('symbol')})")
        st.caption(
            f"{info.get('sector', 'N/A')} | {info.get('industry', 'N/A')} | {info.get('fullTimeEmployees', 0):,} Employees"
        )

        with st.expander("Business Summary"):
            st.write(info.get("longBusinessSummary", "No summary available."))

        # Key Metrics
        cols = st.columns(4)
        cols[0].metric("Market Cap", f"${info.get('marketCap', 0) / 1e9:.2f}B")
        cols[1].metric("P/E Ratio (TTM)", f"{info.get('trailingPE', 0):.2f}")
        cols[2].metric("Forward P/E", f"{info.get('forwardPE', 0):.2f}")
        cols[3].metric("Dividend Yield", f"{info.get('dividendYield', 0):.2%}")

        # In dashboard_app/ui_components/asset_deep_dive_tab.py

    def _render_technical_analysis(self, ticker: str, hist_df: pd.DataFrame):
        """
        Displays the price chart with technical indicators from our own pipeline.
        """
        st.markdown("##### Historical Performance & Technicals")

        # --- FEATURE: Fetch enriched data from our database ---
        sql_query = "SELECT * FROM price_data_daily WHERE Ticker = ?"
        try:
            enriched_data = pd.read_sql(
                sql_query,
                self.db_manager._get_connection(),
                params=(ticker,),
                index_col="Timestamp",
                parse_dates=["Timestamp"],
            )
        except Exception as e:
            # It's possible the pipeline hasn't run for this ticker yet.
            st.info(
                f"No enriched data found in the database for {ticker}. "
                f"Run the data pipeline to generate technical indicators. Error: {e}"
            )
            enriched_data = pd.DataFrame()

        # FIX: Ensure timezone consistency before joining. `yfinance` often returns
        # a timezone-aware index, while our database stores timezone-naive datetimes.
        # This causes a TypeError on join. We can fix this by making the aware
        # index naive. Using tz_convert(None) is safer than modifying the index
        # in-place, especially since `hist_df` comes from a cached function.
        if hist_df.index.tz is not None:
            hist_df = hist_df.tz_convert(None)

        # Combine historical data with our enriched data. We only join the new
        # columns from the database to avoid the "columns overlap" error.
        if not enriched_data.empty:
            enriched_cols_to_add = enriched_data.columns.difference(hist_df.columns)
            combined_df = hist_df.join(enriched_data[enriched_cols_to_add], how="left")
        else:
            # If there's no enriched data, just use the historical data
            combined_df = hist_df.copy()

        # Create a figure with a secondary y-axis for indicators like RSI
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05)

        # Candlestick chart
        fig.add_trace(
            go.Candlestick(
                x=combined_df.index,
                open=combined_df["Open"],
                high=combined_df["High"],
                low=combined_df["Low"],
                close=combined_df["Close"],
                name="Price",
            ),
            row=1,
            col=1,
        )

        # --- FEATURE: Plot technical indicators if they exist ---
        if "rsi_14d" in combined_df.columns:
            fig.add_trace(
                go.Scatter(
                    x=combined_df.index,
                    y=combined_df["rsi_14d"],
                    name="RSI (14d)",
                    line=dict(color="purple", width=1),
                ),
                row=2,
                col=1,
            )
            fig.add_hline(y=70, line_dash="dash", row=2, col=1, line_color="red")
            fig.add_hline(y=30, line_dash="dash", row=2, col=1, line_color="green")

        fig.update_layout(
            title_text=f"Price Chart for {ticker}",
            xaxis_rangeslider_visible=False,
            margin=dict(l=20, r=20, t=40, b=20),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )
        fig.update_yaxes(title_text="Price (USD)", row=1, col=1)
        fig.update_yaxes(title_text="RSI", row=2, col=1)

        st.plotly_chart(fig, use_container_width=True)

    def _render_financial_statements(self, ticker: str):
        """Fetches and displays financial statements in separate tabs."""
        st.markdown("##### Financial Statements")
        is_quarterly = st.toggle("Show Quarterly Data", key=f"quarterly_{ticker}")

        try:
            income, balance, cashflow = self._get_financials(
                ticker, quarterly=is_quarterly
            )

            # --- FEATURE: Add a helper to format large financial numbers ---
            def display_formatted_df(df: pd.DataFrame):
                """Applies comma formatting to all numeric columns for display."""
                if df.empty:
                    st.info("No data available for this view.")
                    return

                # Format date columns to remove the time part for cleaner display
                if isinstance(df.columns, pd.DatetimeIndex):
                    # Create a copy to avoid modifying a cached object from yfinance
                    df = df.copy()
                    df.columns = df.columns.strftime("%Y-%m-%d")

                # Use Pandas Styler for robust formatting of large numbers.
                # This adds comma separators and provides a clear representation for NaNs.
                st.dataframe(
                    df.style.format("{:,.0f}", na_rep="--"), use_container_width=True
                )

            fin_tab1, fin_tab2, fin_tab3 = st.tabs(
                ["Income Statement", "Balance Sheet", "Cash Flow"]
            )

            with fin_tab1:
                display_formatted_df(income)
            with fin_tab2:
                display_formatted_df(balance)
            with fin_tab3:
                display_formatted_df(cashflow)

        except Exception as e:
            st.warning(
                f"Could not retrieve financial statements. This is common for ETFs or certain asset types. Error: {e}"
            )

    def _render_news(self, ticker: str):
        """Fetches and displays recent news articles."""
        st.markdown("##### Recent News")
        try:
            news = self._get_news(ticker)
            if not news:
                st.info("No recent news found for this ticker.")
                return
        except Exception as e:
            st.warning(
                f"Could not retrieve news. The API may be temporarily unavailable. Error: {e}"
            )
            return

        for item in news[:8]:  # Display top 8 articles
            link_url = item.get("link")
            title = item.get("title")

            if link_url and title:
                st.markdown(
                    f"**<a href='{link_url}' target='_blank' style='text-decoration: none;'>📄 {title}</a>**",
                    unsafe_allow_html=True,
                )
                publisher = item.get("publisher", "N/A")
                publish_time = item.get("providerPublishTime")
                caption_text = f"Publisher: {publisher}"
                if publish_time:
                    caption_text += f" | {pd.to_datetime(publish_time, unit='s').strftime('%Y-%m-%d')}"
                st.caption(caption_text)
                st.markdown("---")

    def _render_research_notes(self, ticker: str):
        """Provides a text area for user-generated research and saves it to the DB."""
        st.markdown("##### My Research Notes")
        st.info(
            "Your notes are saved automatically to the database for future reference.",
            icon="ℹ️",
        )

        current_notes = self.db_manager.load_research_notes(ticker)
        notes = st.text_area(
            "Investment Thesis, Analysis, and Notes",
            value=current_notes,
            height=300,
            key=f"notes_{ticker}",
        )

        if st.button("Save Notes", key=f"save_notes_{ticker}"):
            self.db_manager.save_research_notes(ticker, notes)
            st.toast(f"✅ Notes for {ticker} saved!", icon="📝")
