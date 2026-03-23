
import streamlit as st
from strawberry.scalars import JSON
from typing import Optional
import requests
import json
import logging
import tenacity
from services.config import ServiceConfig
from services.logging_config import configure_logging

logger = configure_logging("SignalsController")

class SignalsController:
    """
    Handles interactions with the GraphQL Gateway for signal generation.
    
    This controller acts as the bridge between the Streamlit frontend and the 
    backend services. It abstracts away the complexity of the GraphQL query
    and provides a clean interface for the UI.
    """
    def __init__(self, gateway_url: str = None):
        self.gateway_url = gateway_url or ServiceConfig.get_graphql_url()

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError),
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_fixed(1)
    )
    def fetch_signal(self, symbol: str) -> Optional[dict]:
        """
        Queries the GraphQL gateway for a signed signal.
        
        This constructs a GraphQL query asking for the signal's value and
        all necessary cryptographic proofs (signature, hash, public key ID).
        
        Args:
            symbol (str): The ticker symbol to request (e.g., "AAPL").
            
        Returns:
            Optional[dict]: The signal data dictionary if successful, None if failed.
        """
        # The GraphQL query requesting the exact fields needed by the Viewer
        query = """
        query getSignal($symbol: String!) {
            signal(symbol: $symbol) {
                value
                confidence
                featuresHash
                signature
                publicKeyId
                timestamp
                payloadJson
            }
        }
        """
        
        variables = {"symbol": symbol}
        
        try:
            with st.spinner(f"Requesting Signed Signal for {symbol}..."):
                # Send HTTP POST request to the Gateway
                response = requests.post(
                    self.gateway_url, 
                    json={"query": query, "variables": variables},
                    timeout=30
                )
                
                if response.status_code == 404:
                    st.error("Error: GraphQL Gateway not found. Is the server running?")
                    return None
                
                response.raise_for_status()
                
                result = response.json()
                
                # Check for GraphQL-level errors
                if "errors" in result:
                     # Parse the first error message for user friendliness
                    err_msg = result['errors'][0].get('message', 'Unknown Error')
                    st.error(f"Signal Generation Failed: {err_msg}")
                    logger.error(f"GraphQL Error: {result['errors']}")
                    return None
                
                # Return the specific signal object
                signal_data = result.get("data", {}).get("signal")
                return signal_data

        except requests.exceptions.ReadTimeout:
            st.error("Error: Server timed out. Try again later.")
            logger.error(f"Timeout connecting to {self.gateway_url}")
            return None
        except requests.exceptions.ConnectionError:
            # Let tenacity handle retries; if all fail, it raises RetryError
            raise
        except requests.exceptions.RequestException as e:
            # Handle other network errors (500s, etc)
            st.error(f"Network Error: {e}")
            logger.error(f"Gateway connection error: {e}")
            return None
        except tenacity.RetryError:
             st.error(f"Failed to connect to Gateway after 3 attempts. Is it running at {self.gateway_url}?")
             return None

    def execute_trade(self, symbol: str, action: str, quantity: float, price: float, timestamp: str) -> dict:
        """
        Executes a paper trade via the GraphQL API.
        """
        mutation = """
        mutation executeTrade($symbol: String!, $action: String!, $quantity: Float!, $price: Float!, $timestamp: String!) {
            executeTrade(symbol: $symbol, action: $action, quantity: $quantity, price: $price, timestamp: $timestamp) {
                success
                message
                transactionId
                filledPrice
            }
        }
        """
        
        variables = {
            "symbol": symbol,
            "action": action,
            "quantity": float(quantity),
            "price": float(price),
            "timestamp": timestamp
        }
        
        try:
            response = requests.post(
                self.gateway_url,
                json={"query": mutation, "variables": variables},
                timeout=10
            )
            response.raise_for_status()
            result = response.json()
            
            if "errors" in result:
                return {"success": False, "message": result['errors'][0]['message']}
                
            return result.get("data", {}).get("executeTrade", {"success": False, "message": "No data returned"})
            
        except Exception as e:
            logger.error(f"Trade Execution Failed: {e}")
            return {"success": False, "message": str(e)}

    def fetch_portfolio(self) -> Optional[dict]:
        """
        Fetches the current paper trading portfolio.
        """
        query = """
        query {
            portfolio {
                cashBalance
                totalEquity
                positions {
                    symbol
                    quantity
                    averagePrice
                    currentValue
                }
            }
        }
        """
        
        try:
            response = requests.post(
                self.gateway_url,
                json={"query": query},
                timeout=10
            )
            response.raise_for_status()
            result = response.json()
            
            if "errors" in result:
                logger.error(f"Portfolio Fetch Error: {result['errors']}")
                return None
                
            return result.get("data", {}).get("portfolio")
            
        except Exception as e:
            logger.error(f"Failed to fetch portfolio: {e}")
            return None
