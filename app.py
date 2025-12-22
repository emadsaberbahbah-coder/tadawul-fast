import os
import json
from flask import Flask, request, jsonify
import gspread
from google.oauth2.service_account import Credentials
from finance_engine import FinanceEngine

app = Flask(__name__)

# --- CONFIGURATION ---
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
EODHD_API_KEY = os.environ.get('EODHD_API_TOKEN', 'demo') 
engine = FinanceEngine(EODHD_API_KEY)

@app.route('/')
def home():
    return "Emad Bahbah Financial Engine is Running."

@app.route('/api/analyze', methods=['POST'])
def analyze_portfolio():
    """
    Reads tickers from Col A, runs AI, writes results to Cols B-G.
    """
    try:
        data = request.json
        sheet_url = data.get('sheet_url')
        
        gc = get_google_client()
        sh = gc.open_by_url(sheet_url)
        
        # 1. Update Market Data Page
        worksheet = sh.worksheet("Market Data")
        
        # Get all tickers from Column A (skipping header)
        tickers = worksheet.col_values(1)[1:] 
        
        results = []
        recommendations = []

        print(f"Analyzing {len(tickers)} tickers...")

        for ticker in tickers:
            if not ticker: 
                continue
            
            print(f"Processing: {ticker}")
            
            # Fetch Data
            fund, hist = engine.get_stock_data(ticker)
            
            # Run AI Forecasting
            ai_result = engine.run_ai_forecasting(hist)
            
            # Check Shariah
            shariah = engine.check_shariah_compliance(fund)
            
            # Generate Score
            score = engine.generate_score(fund, ai_result)

            # Prepare row data [Ticker, Price, Sector, Forecast, ROI, Compliant, Score]
            # Note: We rewrite the ticker in Col A to ensure alignment
            row_data = [
                ticker,
                ai_result.get('current_price'),
                fund.get('General', {}).get('Sector', 'N/A'),
                ai_result.get('predicted_price_30d'),
                f"{ai_result.get('expected_roi_pct')}%",
                "YES" if shariah.get('compliant') else "NO",
                score
            ]
            results.append(row_data)

            # Filter for Top 7 Recommendations
            if shariah.get('compliant') and score > 60:
                recommendations.append({
                    "ticker": ticker,
                    "score": score,
                    "roi": ai_result.get('expected_roi_pct')
                })

        # --- CRITICAL UPDATE: WRITE DATA TO SHEET ---
        if results:
            # We update range A2 to G(end)
            end_row = 1 + len(results)
            range_name = f"A2:G{end_row}"
            # This pushes the calculated data into the cells
            worksheet.update(values=results, range_name=range_name)
        
        # Sort recommendations by ROI and take top 7
        recommendations.sort(key=lambda x: x['roi'], reverse=True)
        top_7 = recommendations[:7]

        return jsonify({
            "status": "success",
            "message": "Analysis complete",
            "top_picks": top_7
        })

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
