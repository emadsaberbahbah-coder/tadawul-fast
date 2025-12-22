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
        params = {"api_token": self.api_token, "fmt": "json"}
        params.update(kwargs)
        try:
            resp = requests.get(f"{self.base_url}/{endpoint}", params=params, timeout=10)
            return resp.json() if resp.status_code == 200 else None
        except:
            return None

    def calculate_technicals(self, df):
        """Calculates RSI and Moving Averages for Technical Score"""
        if len(df) < 50: return 50 # Not enough data
        
        # RSI Calculation
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        current_rsi = df['rsi'].iloc[-1]

        # Moving Averages
        df['sma_50'] = df['close'].rolling(window=50).mean()
        df['sma_200'] = df['close'].rolling(window=200).mean()
        
        # Scoring Logic
        tech_score = 50
        if current_rsi < 30: tech_score += 20 # Oversold (Buy Signal)
        elif current_rsi > 70: tech_score -= 20 # Overbought (Sell Signal)
        
        if df['close'].iloc[-1] > df['sma_50'].iloc[-1]: tech_score += 15 # Bullish Trend
        if df['sma_50'].iloc[-1] > df['sma_200'].iloc[-1]: tech_score += 15 # Golden Cross
        
        return min(max(tech_score, 0), 100)

    def get_forecast_and_score(self, ticker):
        """
        Performs Deep AI Analysis:
        1. Prophet Forecast (Future Price)
        2. Technical Analysis (RSI, Trends)
        3. Generates a Composite 'AI Score' (0-100)
        """
        # Fetch 2 years of history
        data = self._fetch_json(f"eod/{ticker}", period="d", from_date=(datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d'))
        
        if not data or len(data) < 100:
            return {"trend": "Neutral", "exp_price": 0, "score": 0, "signal": "No Data"}

        df = pd.DataFrame(data)
        
        # --- 1. Prophet AI Forecast ---
        df_prophet = df[['date', 'close']].rename(columns={'date': 'ds', 'close': 'y'})
        m = Prophet(daily_seasonality=True)
        m.fit(df_prophet)
        future = m.make_future_dataframe(periods=30)
        forecast = m.predict(future)
        
        current_price = df['close'].iloc[-1]
        future_price = forecast['yhat'].iloc[-1]
        roi_pred = ((future_price - current_price) / current_price) * 100
        
        # --- 2. Technical Score ---
        tech_score = self.calculate_technicals(df)
        
        # --- 3. Composite AI Score ---
        # Weighting: 40% AI Forecast, 60% Technicals
        forecast_score = 50 + (roi_pred * 2) # +5% ROI adds 10 points
        final_score = (forecast_score * 0.4) + (tech_score * 0.6)
        final_score = int(min(max(final_score, 0), 100))
        
        signal = "STRONG BUY" if final_score > 80 else "BUY" if final_score > 60 else "SELL" if final_score < 40 else "HOLD"

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
        Strict AAOIFI Shariah Compliance Check
        """
        fund = self._fetch_json(f"fundamentals/{ticker}")
        if not fund: return {"compliant": False, "reason": "No Data"}
        
        try:
            # 1. Business Activity Screen
            sector = fund.get("General", {}).get("Sector", "").lower()
            industry = fund.get("General", {}).get("Industry", "").lower()
            forbidden = ['alcohol', 'tobacco', 'gambling', 'bank', 'insurance', 'defense', 'pork']
            if any(x in sector or x in industry for x in forbidden):
                return {"compliant": False, "reason": f"Sector: {sector}"}

            # 2. Financial Ratios (AAOIFI < 33%)
            bs = fund['Financials']['quarterly']
            latest = next(iter(bs.values()))
            
            assets = float(latest.get('totalAssets', 0))
            debt = float(latest.get('shortLongTermDebtTotal', 0))
            cash = float(latest.get('cashAndEquivalents', 0))
            receivables = float(latest.get('netReceivables', 0))
            
            if assets == 0: return {"compliant": False, "reason": "Assets 0"}
            
            debt_r = debt / assets
            cash_r = cash / assets
            rec_r = receivables / assets
            
            is_halal = (debt_r < 0.33) and (cash_r < 0.33) and (rec_r < 0.33)
            
            return {
                "compliant": is_halal,
                "ratios": f"D:{debt_r:.2f} C:{cash_r:.2f} R:{rec_r:.2f}",
                "reason": "Passed" if is_halal else "Ratios Failed"
            }
        except:
            return {"compliant": False, "reason": "Calc Error"}
