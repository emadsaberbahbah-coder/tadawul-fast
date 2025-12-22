import os
import requests
import pandas as pd
import numpy as np
from prophet import Prophet
from datetime import datetime, timedelta

class FinancialExpert:
    def __init__(self):
        # Load API key from environment variables for security
        self.api_token = os.environ.get("EODHD_API_TOKEN")
        self.base_url = "https://eodhd.com/api"
        
        if not self.api_token:
            raise ValueError("EODHD_API_TOKEN environment variable not set.")

    def _fetch_json(self, endpoint, **kwargs):
        """Helper to handle API requests"""
        params = {"api_token": self.api_token, "fmt": "json"}
        params.update(kwargs)
        try:
            response = requests.get(f"{self.base_url}/{endpoint}", params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching {endpoint}: {str(e)}")
            return None

    def screen_shariah(self, ticker):
        """
        Performs AAOIFI Shariah Screening:
        1. Sector Screening (Qualitative)
        2. Financial Ratio Screening (Quantitative - 33% Rule)
        """
        data = self._fetch_json(f"fundamentals/{ticker}")
        if not data:
            return {"compliant": False, "reason": "Data unavailable"}

        # --- 1. Sector Screening ---
        general = data.get("General", {})
        sector = general.get("Sector", "").lower()
        industry = general.get("Industry", "").lower()
        
        forbidden_terms = [
            "bank", "tobacco", "alcohol", "gambling", "casino", 
            "insurance", "beer", "defense", "weapon", "pork"
        ]
        
        for term in forbidden_terms:
            if term in sector or term in industry:
                return {
                    "compliant": False, 
                    "reason": f"Sector violation: {sector} / {industry}"
                }

        # --- 2. Financial Ratio Screening (AAOIFI Standards) ---
        # We need the latest quarterly balance sheet
        try:
            financials = data.get("Financials", {}).get("Balance_Sheet", {}).get("quarterly", {})
            if not financials:
                return {"compliant": False, "reason": "Financial data missing"}
            
            # Get latest available quarter (dates are keys)
            latest_date = sorted(financials.keys(), reverse=True)
            balance_sheet = financials[latest_date]
            
            # Parse values (handle None/Strings)
            def get_val(key):
                val = balance_sheet.get(key, 0)
                return float(val) if val is not None else 0.0

            total_assets = get_val("totalAssets")
            if total_assets == 0:
                return {"compliant": False, "reason": "Total Assets is 0"}

            # Calculate Ratios
            # Debt Ratio: Total Interest Bearing Debt / Total Assets
            total_debt = get_val("shortLongTermDebtTotal") + get_val("longTermDebt")
            debt_ratio = total_debt / total_assets

            # Liquidity Ratio: (Cash + Interest Bearing Securities) / Total Assets
            cash_total = get_val("cashAndEquivalents") + get_val("shortTermInvestments")
            cash_ratio = cash_total / total_assets

            # Receivables Ratio: Accounts Receivables / Total Assets
            receivables = get_val("netReceivables")
            receivables_ratio = receivables / total_assets

            # Check Thresholds (33%)
            is_compliant = (debt_ratio < 0.33) and (cash_ratio < 0.33) and (receivables_ratio < 0.33)
            
            return {
                "compliant": is_compliant,
                "debt_ratio": f"{debt_ratio:.2%}",
                "cash_ratio": f"{cash_ratio:.2%}",
                "receivables_ratio": f"{receivables_ratio:.2%}",
                "reason": "Passed" if is_compliant else "Ratio Violation"
            }

        except Exception as e:
            return {"compliant": False, "reason": f"Calculation Error: {str(e)}"}

    def get_forecast(self, ticker, days=30):
        """
        Generates AI forecast using Facebook Prophet.
        Returns trend direction and 30-day expected price.
        """
        # Fetch historical data (daily)
        hist_data = self._fetch_json(f"eod/{ticker}", period="d", order="a")
        
        if not hist_data or len(hist_data) < 100:
            return {"trend": "Insufficient Data", "expected_price": 0}

        # Format for Prophet
        df = pd.DataFrame(hist_data)
        df['ds'] = pd.to_datetime(df['date'])
        df['y'] = df['close']
        df = df[['ds', 'y']]

        # Initialize and train AI model
        model = Prophet(daily_seasonality=True, yearly_seasonality=True)
        model.fit(df)

        # Predict future
        future = model.make_future_dataframe(periods=days)
        forecast = model.predict(future)

        # Extract metrics
        current_price = df['y'].iloc[-1]
        future_price = forecast['yhat'].iloc[-1]
        
        trend = "Bullish (Positive Impact)" if future_price > current_price else "Bearish (Negative Impact)"
        roi_forecast = ((future_price - current_price) / current_price) * 100

        return {
            "trend": trend,
            "current_price": round(current_price, 2),
            "expected_price_30d": round(future_price, 2),
            "expected_roi_30d": f"{roi_forecast:.2f}%",
            "confidence_lower": round(forecast['yhat_lower'].iloc[-1], 2),
            "confidence_upper": round(forecast['yhat_upper'].iloc[-1], 2)
        }

    def get_market_data_batch(self, tickers):
        """
        Fetches real-time data for multiple tickers (for Portfolio page).
        Optimized to use bulk request.
        """
        ticker_str = ",".join(tickers)
        # Note: EODHD real-time endpoint usually takes one main ticker and 's' parameter for others
        # We will split if list is too long, but here is simple implementation
        if not tickers:
            return
            
        main_ticker = tickers
        others = ",".join(tickers[1:])
        
        data = self._fetch_json(f"real-time/{main_ticker}", s=others)
        return data if isinstance(data, list) else [data]
