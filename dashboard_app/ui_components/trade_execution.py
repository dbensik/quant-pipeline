import streamlit as st
import time
from datetime import datetime
import dateutil.parser
import yfinance as yf

class TradeExecutionComponent:
    """
    Renders the interactive "Review & Execute" flow for a signal.
    """
    def __init__(self, controller):
        self.controller = controller

    def render(self, signal_data: dict, symbol: str):
        """
        Renders the trade execution UI for a given signal.
        """
        signal_val = signal_data.get("value", 0)
        
        # Default to Buy for Hold signals in test mode, or determine based on sentiment
        if signal_val > 0:
            action = "BUY"
        elif signal_val < 0:
            action = "SELL"
        else:
            action = "BUY" # Default for manual override on Neutral
            
        # --- State Management ---
        # We need to track if the user is currently reviewing a trade for this specific symbol/signal
        # Use a unique key for session state based on the signal timestamp or signature
        signal_id = signal_data.get("signature")[:10]
        review_key = f"reviewing_{signal_id}"
        
        if review_key not in st.session_state:
            st.session_state[review_key] = False
            
        # --- 1. Initial "Trade" Button ---
        if not st.session_state[review_key]:
            if st.button(f"Trade {action} {symbol}", key=f"btn_init_{signal_id}"):
                st.session_state[review_key] = True
                st.rerun()
                
        # --- 2. Review & Countdown Interface ---
        else:
            with st.container():
                st.markdown("### 📋 Review Order")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Action", action)
                    quantity = st.number_input("Quantity", min_value=1, value=10, key=f"qty_{signal_id}")
                with col2:
                    # Fetch live price
                    try:
                        ticker_obj = yf.Ticker(symbol)
                        # live price is often in .info['regularMarketPrice'] or history
                        # history is more reliable for free tier
                        hist = ticker_obj.history(period="1d")
                        if not hist.empty:
                            current_price = float(hist["Close"].iloc[-1])
                        else:
                            current_price = 100.00 # Fallback
                    except Exception as e:
                        current_price = 100.00
                        
                    st.metric("Est. Price", f"${current_price:,.2f}")
                    st.metric("Total", f"${(current_price * quantity):,.2f}")
                
                # Countdown Logic
                # In a real app, the "Quote" would have a fixed expiry from the server.
                # Here we simulate a 30s window from the moment they clicked "Trade".
                
                st.warning("⚠️ Quote expires in 30s")
                
                # Execute Button
                if st.button("🚀 CONFIRM EXECUTION", type="primary", key=f"btn_exec_{signal_id}"):
                    with st.spinner("Executing Trade..."):
                        # Call Backend via Controller
                        result = self.controller.execute_trade(
                            symbol=symbol,
                            action=action,
                            quantity=quantity,
                            price=current_price,
                            timestamp=datetime.utcnow().isoformat()
                        )
                        
                        if result.get("success"):
                            st.success(f"✅ Trade Executed! ID: {result.get('transaction_id')}")
                            # Reset review state
                            st.session_state[review_key] = False
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error(f"❌ Execution Failed: {result.get('message')}")
                            
                if st.button("Cancel", key=f"btn_cancel_{signal_id}"):
                    st.session_state[review_key] = False
                    st.rerun()
