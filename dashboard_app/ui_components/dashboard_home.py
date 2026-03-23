import streamlit as st
from dashboard_app.ui_components.news_widget import NewsWidget
from services.execution_service.portfolio_manager import PortfolioManager

class DashboardHome:
    """
    The main landing view of the dashboard.
    Displays:
    - High-level Portfolio Summary (Active Portfolio)
    - Market News Intelligence
    """
    def __init__(self, portfolio_manager: PortfolioManager, watchlist_manager):
        self.portfolio_manager = portfolio_manager
        self.watchlist_manager = watchlist_manager
        self.news_widget = NewsWidget()

    def render(self, selections: dict):
        st.title("📊 Dashboard Overview")
        
        # --- 1. Portfolio Summary Section ---
        # Get selected portfolio from global state/selections or default
        selected_portfolio = selections.get("selected_portfolio_to_manage")
        port_state = self.portfolio_manager.get_portfolio_state(selected_portfolio)
        
        st.markdown(f"### 💼 Portfolio Status: {selected_portfolio or 'Default'}")
        
        # Quick Jump / Search
        c_search, c_btn = st.columns([3, 1])
        search_ticker = c_search.text_input("🔍 Quick Deep Dive (Ticker)", placeholder="AAPL, TSLA...").upper().strip()
        if c_btn.button("Go", key="home_quick_search_btn") and search_ticker:
             st.session_state.selections["selected_symbols_chart"] = [search_ticker]
             st.rerun()
        
        col1, col2, col3 = st.columns(3)
        cash = port_state.get("cash", 0.0)
        positions = port_state.get("positions", {})
        
        equity = cash
        for _, pos in positions.items():
            equity += pos["quantity"] * pos["average_price"]
            
        unrealized_pnl = 0.0 # Calculate if live prices available (Future work)
        
        col1.metric("Total Equity", f"${equity:,.2f}")
        col2.metric("Cash Balance", f"${cash:,.2f}")
        col3.metric("Positions Count", len(positions))
        
        if positions:
             with st.expander("View Active Positions", expanded=True):
                 # Create a grid layout for better interaction than a plain dataframe
                 cols = st.columns([1, 1, 1, 1, 1])
                 cols[0].markdown("**Symbol**")
                 cols[1].markdown("**Qty**")
                 cols[2].markdown("**Avg Price**")
                 cols[3].markdown("**Cost Basis**")
                 cols[4].markdown("**Action**")
                 
                 for sym, pos in positions.items():
                     c = st.columns([1, 1, 1, 1, 1])
                     c[0].write(sym)
                     c[1].write(f"{pos['quantity']}")
                     c[2].write(f"${pos['average_price']:.2f}")
                     c[3].write(f"${pos['quantity'] * pos['average_price']:,.2f}")
                     if c[4].button("🔎 Analyze", key=f"analyze_pos_{sym}"):
                         st.session_state.selections["selected_symbols_chart"] = [sym]
                         st.rerun()

        st.divider()

        # --- 2. News Section ---
        # Pass portfolios and watchlists names for filtering
        all_ports = self.portfolio_manager.get_all_portfolios()
        all_watchlists = self.watchlist_manager.load()
        
        self.news_widget.render(all_ports, all_watchlists)
