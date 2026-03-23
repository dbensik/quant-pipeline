import streamlit as st
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor

class NewsWidget:
    """
    A widget to display market news with filtering capabilities.
    Supports filtering by:
    - Broad Market (default)
    - Specific Portfolio
    - Specific Watchlist
    - Individual Ticker (within the above context)
    """
    def __init__(self):
        pass

    def render(self, portfolios: dict, watchlists: dict):
        st.subheader("📰 Market Intelligence")
        
        # 1. Source Selection
        col1, col2 = st.columns([1, 2])
        
        filter_options = ["Broad Market"]
        if portfolios:
             filter_options.extend([f"Portfolio: {k}" for k in portfolios.keys()])
        if watchlists:
             filter_options.extend([f"Watchlist: {k}" for k in watchlists.keys()])
             
        selected_filter = col1.selectbox("News Source", filter_options, key="news_filter_source")
        
        # 2. Derive Ticker List based on Source
        target_tickers = []
        if selected_filter == "Broad Market":
            target_tickers = ["SPY", "QQQ", "DIA", "BTC-USD"] # Proxies for market news
        elif selected_filter.startswith("Portfolio:"):
            p_name = selected_filter.replace("Portfolio: ", "")
            port = portfolios.get(p_name, {})
            # Extract tickers from positions
            target_tickers = list(port.get("positions", {}).keys())
        elif selected_filter.startswith("Watchlist:"):
            w_name = selected_filter.replace("Watchlist: ", "")
            target_tickers = watchlists.get(w_name, [])
            
        # 3. Individual Asset Filter (Optional)
        selected_asset = col2.selectbox(
            "Filter by Asset (Optional)", 
            options=["All Assets"] + sorted(target_tickers),
            key="news_filter_asset"
        )
        
        # 4. Fetch News
        news_items = []
        if selected_asset != "All Assets":
            # Fetch for specific asset
            news_items = self._fetch_news_for_tickers([selected_asset])
        else:
            # Fetch for all target tickers (limit to avoid slow load)
            # For general market, we use the proxies. For lists, we might limit to top 10.
            subset = target_tickers[:10] if len(target_tickers) > 10 else target_tickers
            news_items = self._fetch_news_for_tickers(subset)
            
        # 5. Display
        self._display_news_feed(news_items)

    @st.cache_data(ttl=600, show_spinner="Fetching News...")
    def _fetch_news_for_tickers(_self, tickers: list) -> list:
        """
        Fetches news for a list of tickers in parallel.
        Returns a sorted list of news items (dicts).
        """
        all_news = []
        
        def fetch_one(t):
            try:
                return yf.Ticker(t).news
            except Exception:
                return []

        # Parallel fetch
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = executor.map(fetch_one, tickers)
            
        for res in results:
            if res:
                all_news.extend(res)
                
        # Deduplicate by link/uuid
        seen_links = set()
        unique_news = []
        for item in all_news:
            link = item.get("link")
            if link not in seen_links:
                seen_links.add(link)
                unique_news.append(item)
                
        # Sort by publish time descending
        unique_news.sort(key=lambda x: x.get("providerPublishTime", 0), reverse=True)
        return unique_news

    def _display_news_feed(self, news_items: list):
        if not news_items:
            st.info("No news found for the selected criteria.")
            return

        # Display loop
        for item in news_items[:20]: # Limit to 20
            # Simplify display
            title = item.get("title")
            link = item.get("link")
            publisher = item.get("publisher", "Unknown")
            pub_time = item.get("providerPublishTime", 0)
            
            # Format time
            time_str = pd.to_datetime(pub_time, unit='s').strftime("%Y-%m-%d %H:%M")
            
            with st.container():
                st.markdown(f"**[{title}]({link})**")
                st.caption(f"{publisher} • {time_str}")
                # Use related tickers if available to show context?
                related = item.get("relatedTickers", [])
                if related:
                     st.caption(f"Related: {', '.join(related[:5])}")
                st.divider()
