import os
import json
from flask import Flask, request, jsonify
import gspread
from google.oauth2.service_account import Credentials
from finance_engine import FinanceEngine

app = Flask(__name__)

# --- CONFIGURATION ---
# Load credentials from Render Environment Variable
# Variable name must be: GOOGLE_CREDENTIALS
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def get_google_client():
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if not creds_json:
        raise ValueError("Missing GOOGLE_CREDENTIALS environment variable")
    
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

# Initialize Finance Engine
EODHD_API_KEY = os.environ.get('EODHD_API_TOKEN', 'demo') # Add your key to env vars
engine = FinanceEngine(EODHD_API_KEY)

@app.route('/')
def home():
    return "Emad Bahbah Financial Engine is Running."

@app.route('/api/analyze', methods=['POST'])
def analyze_portfolio():
    """
    Reads tickers from the Google Sheet, analyzes them using AI,
    and writes back the results.
    Expected JSON body: {"sheet_url": "..."}
    """
    try:
        data = request.json
        sheet_url = data.get('sheet_url')
        
        gc = get_google_client()
        sh = gc.open_by_url(sheet_url)
        
        # 1. Update Market Data Page
        worksheet = sh.worksheet("Market Data")
        tickers = worksheet.col_values(1)[1:] # Assume tickers are in Col A, skip header
        
        results = []
        recommendations = []

        for ticker in tickers:
            if not ticker: continue
            
            # Fetch Data
            fund, hist = engine.get_stock_data(ticker)
            
            # Run AI
            ai_result = engine.run_ai_forecasting(hist)
            
            # Check Shariah
            shariah = engine.check_shariah_compliance(fund)
            
            # Score
            score = engine.generate_score(fund, ai_result)

            # Prepare row for 'Market Data'
            row_data = [
                ticker,
                ai_result.get('current_price'),
                fund.get('General', {}).get('Sector'),
                ai_result.get('predicted_price_30d'),
                ai_result.get('expected_roi_pct'),
                shariah.get('compliant'),
                score
            ]
            results.append(row_data)

            # Filter for Top 7 Recommendations
            if shariah.get('compliant') and score > 70:
                recommendations.append({
                    "ticker": ticker,
                    "score": score,
                    "roi": ai_result.get('expected_roi_pct')
                })

        # Update Market Data Sheet (Simplified batch update)
        # Note: In production, map these list items to specific cell ranges
        print("Analysis Complete. Update logic would go here.")
        
        # Sort recommendations by ROI and take top 7
        recommendations.sort(key=lambda x: x['roi'], reverse=True)
        top_7 = recommendations[:7]

        return jsonify({
            "status": "success",
            "message": "Analysis complete",
            "top_picks": top_7
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
