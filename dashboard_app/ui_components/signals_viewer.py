
import streamlit as st
import json

class SignalsViewer:
    """
    Renders the verifiable signal card in the Streamlit UI.
    
    This component is responsible for visualizing:
    1. The core trading signal (Buy/Sell/Hold) with confidence.
    2. The cryptographic proofs (Signature, Hash) that guarantee authenticity.
    3. The raw underlying data payload for transparency.
    """
    def __init__(self, controller=None):
        self.controller = controller
        if self.controller:
            from dashboard_app.ui_components.trade_execution import TradeExecutionComponent
            self.trade_executor = TradeExecutionComponent(controller)

    def render(self, signal_data: dict, symbol: str = None):
        """
        Render the signal card.
        
        Args:
           signal_data (dict): The dictionary returned by the GraphQL 'signal' query.
           symbol (str): The ticker symbol (required for trading).
        """
        if not signal_data:
            return

        st.markdown("## 📡 Generated Signal")
        
        # Determine color/action based on signal value
        # > 0 : BUY (Green)
        # < 0 : SELL (Red)
        # = 0 : HOLD (Gray)
        value = signal_data.get("value", 0)
        confidence = signal_data.get("confidence", 0)
        
        if value > 0:
            action = "BUY"
            color = "green"
            icon = "🟢"
        elif value < 0:
            action = "SELL"
            color = "red"
            icon = "🔴"
        else:
            action = "HOLD"
            color = "gray"
            icon = "⚪"

        # --- Top Card ---
        # Display the main call-to-action prominently
        col1, col2, col3 = st.columns([2, 5, 2])
        
        with col2:
            st.markdown(
                f"""
                <div style="border: 2px solid {color}; padding: 20px; border-radius: 10px; text-align: center; background-color: rgba(0,0,0,0.2);">
                    <h3>{icon} {action}</h3>
                    <p style="font-size: 1.2em;">Confidence: <b>{confidence*100:.1f}%</b></p>
                </div>
                """, 
                unsafe_allow_html=True
            )
        
        # --- Trade Execution (Interactive) ---
        if hasattr(self, 'trade_executor') and symbol:
            st.divider()
            self.trade_executor.render(signal_data, symbol)
            
        st.markdown("### 🔐 Cryptographic Verification")
        
        # Show the proofs that allow offline verification
        with st.expander("View Proofs", expanded=True):
            cols = st.columns(2)
            with cols[0]:
                st.markdown("**Signature (Ed25519)**")
                st.code(signal_data.get("signature"), language="text")
                st.caption(f"Signed by Key ID: {signal_data.get('publicKeyId')}")
            
            with cols[1]:
                st.markdown("**Payload Hash (SHA256)**")
                st.code(signal_data.get("featuresHash"), language="text")
        
            st.success("✅ Signature Verified by Gateway")
            st.info("ℹ️ This signal has been logged to the immutable audit trail and can be verified offline.")

        # --- Raw JSON ---
        with st.expander("Raw Payload"):
            try:
                payload = json.loads(signal_data.get("payloadJson", "{}"))
                st.json(payload)
            except:
                st.text(signal_data.get("payloadJson"))
