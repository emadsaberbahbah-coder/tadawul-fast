# ==============================================================================
# EODHD FINANCIAL DATA RETRIEVAL MODULE
# ==============================================================================
#
# PREREQUISITES:
# 1. Python 3.7+ installed.
# 2. Libraries installed via PIP:
#    Command: python3 -m pip install eodhd pandas -U
#
# DESCRIPTION:
# This script initializes the official EODHD API client, authenticates using
# an API key, and retrieves historical stock market data. It converts the
# raw API response into a structured Pandas DataFrame for immediate analysis.
# ==============================================================================

import sys
import pandas as pd
from eodhd import APIClient

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------
# NOTE: In a production environment, use environment variables for keys.
# For this script, replace 'YOUR_API_KEY' with your actual EODHD API key.
# A 'demo' key is available for testing specific tickers like 'AAPL' or 'TSLA'.
API_KEY = 'YOUR_API_KEY' 

def fetch_and_display_data(ticker_symbol='AAPL', period='d', start_date='2024-01-01'):
    """
    Fetches historical data for a given ticker and displays it.
    
    Parameters:
    - ticker_symbol (str): The stock symbol (e.g., 'AAPL', 'EURUSD.FOREX').
    - period (str): The data interval ('d' = daily, 'w' = weekly, 'm' = monthly).
    - start_date (str): The start date for data retrieval (YYYY-MM-DD).
    """
    
    print(f"--- Initializing EODHD Client for {ticker_symbol} ---")
    
    try:
        # 1. Client Initialization
        # The APIClient class is the primary entry point.
        # Reference:  - "Importing Required Libraries"
        client = APIClient(API_KEY)
        
        # 2. Data Retrieval
        # We request historical data. The library handles the HTTP request/response.
        # Reference:  - "End of the Day Historical Stock Market Data"
        print(f"Requesting {period}-level data from {start_date}...")
        resp = client.get_historical_data(
            symbol=ticker_symbol, 
            period=period, 
            from_date=start_date
        )
        
        # 3. Data Validation and Processing
        # The response is typically a list of dictionaries or JSON.
        # We convert this immediately to a Pandas DataFrame for analysis.
        if resp:
            df = pd.DataFrame(resp)
            
            # Optional: Set the date as the index for time-series analysis
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
            
            print("\n Data Retrieved Successfully:")
            print("-" * 50)
            print(df.head()) # Display the first 5 rows
            print("-" * 50)
            print(f"Total Rows: {len(df)}")
            
            return df
        else:
            print(" The API returned an empty response.")
            return None

    except Exception as e:
        # Error handling is critical for network-bound scripts.
        print(f"\n An exception occurred during execution: {e}")
        print("troubleshooting Tips:")
        print("1. Check if your API key is valid.")
        print("2. Verify the ticker symbol exists.")
        print("3. Ensure you have internet connectivity.")
        return None

# ------------------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # Execute the function with default parameters
    fetch_and_display_data()
