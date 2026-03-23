import streamlit as st
import pandas as pd

class PaperPortfolioViewer:
    """
    Renders the Paper Trading Portfolio (Cash, Equity, Positions).
    """
    def __init__(self, controller):
        self.controller = controller

    def render(self):
        """
        Fetches and displays the portfolio state.
        """
        portfolio = self.controller.fetch_portfolio()
        
        if not portfolio:
            st.warning("⚠️ Could not load portfolio data.")
            return

        st.markdown("### 💼 Paper Portfolio")
        
        # summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Equity", f"${portfolio['totalEquity']:,.2f}")
        with col2:
            st.metric("Cash Balance", f"${portfolio['cashBalance']:,.2f}")
        with col3:
            # simple PnL calc if we had cost basis, for now just equity
            pass

        # Positions Table
        positions = portfolio.get("positions", [])
        if positions:
            st.markdown("#### Current Holdings")
            df = pd.DataFrame(positions)
            
            # Format columns
            df['quantity'] = df['quantity'].apply(lambda x: f"{x:,.4f}")
            df['averagePrice'] = df['averagePrice'].apply(lambda x: f"${x:,.2f}")
            df['currentValue'] = df['currentValue'].apply(lambda x: f"${x:,.2f}")
            
            # Rename for display
            df = df.rename(columns={
                "symbol": "Symbol", 
                "quantity": "Quantity", 
                "averagePrice": "Avg Price", 
                "currentValue": "Value"
            })
            
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No active positions.")
