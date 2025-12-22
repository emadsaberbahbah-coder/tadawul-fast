import pandas as pd
import requests
import datetime
from prophet import Prophet
import os

class FinanceEngine:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://eodhd.com/api"

    def get_stock_data(self, ticker):
        """Fetches live fundamental data and historical data."""
        # 1. Get Fundamentals (Valuation, Sector, etc.)
        fund_url = f"{self.base_url}/fundamentals/{ticker}?api_token={self.api_key}&fmt=json"
        fund_data = requests.get(fund_url).json()

        # 2. Get Historical Prices (for AI forecasting)
        hist_url = f"{self.base_url}/eod/{ticker}?api_token={self.api_key}&fmt=json&from={str(datetime.date.today() - datetime.timedelta(days=365*2))}"
        hist_data = requests.get(hist_url).json()

        return fund_data, hist_data

    def check_shariah_compliance(self, fundamentals):
        """
        Basic Shariah screening based on standard financial ratios.
        Note: This is a technical approximation.
        1. Debt to Equity < 33% (or Debt to Market Cap depending on standard)
        2. Cash/Interest Bearing Securities to Market Cap < 33%
        """
        try:
            val = fundamentals.get('Valuation', {})
            bs = fundamentals.get('Financials', {}).get('Balance_Sheet', {}).get('quarterly', {})
            
            # Get latest quarter data
            last_q = list(bs.keys())[0]
            total_debt = float(bs[last_q].get('longTermDebt', 0) or 0) + float(bs[last_q].get('shortTermDebt', 0) or 0)
            cash = float(bs[last_q].get('cash', 0) or 0)
            market_cap = float(val.get('MarketCapitalization', 1))

            debt_ratio = (total_debt / market_cap) * 100
            cash_ratio = (cash / market_cap) * 100

            # Logic: Pass if Debt < 33% of Market Cap AND Cash < 33%
            is_compliant = debt_ratio < 33 and cash_ratio < 33
            
            return {
                "compliant": is_compliant,
                "debt_ratio": round(debt_ratio, 2),
                "cash_ratio": round(cash_ratio, 2)
            }
        except:
            return {"compliant": False, "note": "Insufficient Data"}

    def run_ai_forecasting(self, hist_data):
        """
        Uses Facebook Prophet to forecast stock price 30 days into the future.
        Returns expected ROI and Trend.
        """
        try:
            df = pd.DataFrame(hist_data)
            df['ds'] = pd.to_datetime(df['date'])
            df['y'] = df['close']
            df = df[['ds', 'y']]

            # Initialize and fit model
            m = Prophet(daily_seasonality=True)
            m.fit(df)

            # Create future dataframe
            future = m.make_future_dataframe(periods=30)
            forecast = m.predict(future)

            # Calculate metrics
            current_price = df.iloc[-1]['y']
            predicted_price = forecast.iloc[-1]['yhat']
            roi = ((predicted_price - current_price) / current_price) * 100

            trend = "POSITIVE" if roi > 0 else "NEGATIVE"
            
            return {
                "current_price": round(current_price, 2),
                "predicted_price_30d": round(predicted_price, 2),
                "expected_roi_pct": round(roi, 2),
                "trend": trend
            }
        except Exception as e:
            print(f"AI Error: {e}")
            return {"error": "Forecasting failed"}

    def generate_score(self, fundamentals, ai_data):
        """Generates a 0-100 score for the stock."""
        score = 50 # Base score
        
        # Fundamental Scoring
        try:
            pe = float(fundamentals['Valuation']['TrailingPE'])
            if 0 < pe < 25: score += 10
            elif pe > 50: score -= 10
        except: pass

        # AI Scoring
        if ai_data.get('trend') == "POSITIVE": score += 20
        else: score -= 20

        return max(0, min(100, score))
