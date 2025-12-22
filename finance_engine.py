import os
import requests
import pandas as pd
import numpy as np
from prophet import Prophet
from datetime import datetime, timedelta

class FinancialExpert:
    def __init__(self):
        self.api_token = os.environ.get("EODHD_API_TOKEN")
        self.base_url = "https://eodhd.com/api"

    def _fetch_json(self, endpoint, **kwargs):
        """Helper to fetch data from EODHD safely"""
        params = {"api_token": self.api_token, "fmt": "json"}
        params.update(kwargs)
        try:
            resp = requests.get(f"{self.base_url}/{endpoint}", params=params, timeout=10)
            return resp.json() if resp.status_code == 200 else None
        except:
            return None

    def calculate_technicals(self, df):
        """
        Calculates Advanced Technical Indicators:
        1. RSI (Relative Strength Index) to find Overbought/Oversold.
        2. SMA (Simple Moving Average) for trend confirmation.
        """
        if len(df) < 50: return 50 # Not enough data for analysis
        
        # Calculate RSI (14-day)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        current_rsi = df['rsi'].iloc[-1]

        # Calculate Trends (50-day vs 200-day Moving Average)
        df['sma_50'] = df['close'].rolling(window=50).mean()
        df['sma_200'] = df['close'].rolling(window=200).mean()
        
        # Scoring Logic
        tech_score = 50
        # RSI Logic
        if current_rsi < 30: tech_score += 20 # Oversold = Buy Signal
        elif current_rsi > 70: tech_score -= 20 # Overbought = Sell Signal
        
        # Trend Logic
        if df['close'].iloc[-1] > df['sma_50'].iloc[-1]: tech_score += 15 # Price above average
        
        return min(max(tech_score, 0), 100)

    def get_forecast_and_score(self, ticker):
        """
        Generates the 'Emad Expert Score' (0-100) using AI + Technicals.
        """
        # Fetch 2 years of daily history for robust AI training
        start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
        data = self._fetch_json(f"eod/{ticker}", period="d", from_date=start_date)
        
        if not data or len(data) < 100:
            return {"trend": "Neutral", "exp_price": 0, "score": 0, "signal": "No Data", "current_price": 0}

        df = pd.DataFrame(data)
        current_price = df['close'].iloc[-1]
        
        # --- 1. Prophet AI Forecasting ---
        # We rename columns to 'ds' and 'y' as required by the Prophet library 
        df_prophet = df[['date', 'close']].rename(columns={'date': 'ds', 'close': 'y'})
        m = Prophet(daily_seasonality=True)
        m.fit(df_prophet)
        
        # Predict 30 days into the future
        future = m.make_future_dataframe(periods=30)
        forecast = m.predict(future)
        future_price = forecast['yhat'].iloc[-1]
        
        # Calculate Expected ROI
        roi_pred = ((future_price - current_price) / current_price) * 100
        
        # --- 2. Technical Analysis ---
        tech_score = self.calculate_technicals(df)
        
        # --- 3. Composite Scoring ---
        # Weighted Average: 40% AI Prediction, 60% Technical Reality
        forecast_score = 50 + (roi_pred * 2) 
        final_score = (forecast_score * 0.4) + (tech_score * 0.6)
        final_score = int(min(max(final_score, 0), 100))
        
        # Determine Signal
        if final_score >= 80: signal = "STRONG BUY"
        elif final_score >= 60: signal = "BUY"
        elif final_score <= 40: signal = "SELL"
        else: signal = "HOLD"

        return {
            "trend": "Bullish" if roi_pred > 0 else "Bearish",
            "exp_price": round(future_price, 2),
            "roi_30d": f"{roi_pred:.2f}%",
            "ai_score": final_score,
            "signal": signal,
            "current_price": current_price
        }

    def screen_shariah(self, ticker):
        """
        Performs AAOIFI Shariah Compliance Check (Debt < 33% of Assets)
        """
        fund = self._fetch_json(f"fundamentals/{ticker}")
        if not fund: return {"compliant": False, "reason": "No Data"}
        
        try:
            # 1. Sector Screening (No Alcohol, Gambling, Banks)
            sector = fund.get("General", {}).get("Sector", "").lower()
            industry = fund.get("General", {}).get("Industry", "").lower()
            forbidden = ['alcohol', 'tobacco', 'gambling', 'bank', 'insurance', 'defense', 'pork']
            
            if any(x in sector or x in industry for x in forbidden):
                return {"compliant": False, "reason": f"Forbidden Sector: {sector}"}

            # 2. Financial Ratio Screening (AAOIFI Standard: < 33%) 
            bs = fund['Financials']['quarterly']
            latest_date = max(bs.keys())
            latest = bs[latest_date]
            
            # Use 0 if data is missing (safe default)
            assets = float(latest.get('totalAssets', 0))
            debt = float(latest.get('shortLongTermDebtTotal', 0))
            cash = float(latest.get('cashAndEquivalents', 0))
            receivables = float(latest.get('netReceivables', 0))
            
            if assets == 0: return {"compliant": False, "reason": "Assets Zero"}
            
            debt_r = debt / assets
            cash_r = cash / assets
            rec_r = receivables / assets
            
            # Threshold Check
            is_halal = (debt_r < 0.33) and (cash_r < 0.33) and (rec_r < 0.33)
            
            return {
                "compliant": is_halal,
                "ratios": f"Debt: {debt_r:.2f} | Cash: {cash_r:.2f}",
                "reason": "Passed" if is_halal else "Financial Ratios Failed"
            }
        except:
            return {"compliant": False, "reason": "Calculation Error"}
