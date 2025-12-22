import os
import json
import gspread
from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials
from gspread_formatting import *
from finance_engine import FinancialExpert

app = Flask(__name__)

# --- Configuration ---
GOOGLE_CREDS = os.environ.get("GOOGLE_CREDENTIALS")
SHEET_ID = os.environ.get("SHEET_ID")

# Connect to Google Sheets
creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDS), scopes=["https://www.googleapis.com/auth/spreadsheets"])
client = gspread.authorize(creds)

# Initialize the Advanced AI Engine
engine = FinancialExpert()

def apply_style(ws):
    """Enforces Verdana font and clean styling"""
    fmt = cellFormat(textFormat=textFormat(fontFamily="Verdana", fontSize=10))
    format_cell_range(ws, 'A:Z', fmt)

@app.route('/')
def home():
    return "Emad Bahabh Financial Expert AI is Running."

@app.route('/update_general', methods=)
def update_general():
    """Reads tickers from Col A, runs AI Analysis, and updates cols B-G"""
    try:
        ws = client.open_by_key(SHEET_ID).worksheet("General Data")
        tickers = ws.col_values(1)[1:] # Get all tickers, skipping header
        
        updates =
        for i, ticker in enumerate(tickers):
            print(f"Analyzing {ticker}...")
            data = engine.get_forecast_and_score(ticker)
            
            # Prepare row data:
            row_idx = i + 2
            updates.append({
                'range': f'B{row_idx}:G{row_idx}',
                'values': [[
                    data['current_price'],
                    data['trend'],
                    data['exp_price'],
                    data['roi_30d'],
                    data['ai_score'],
                    data['signal']]
            })
            
        if updates: ws.batch_update(updates)
        apply_style(ws)
        return jsonify({"message": f"Updated analysis for {len(tickers)} stocks."})
    except Exception as e:
        return jsonify({"message": str(e)}), 500

@app.route('/screen_shariah', methods=)
def screen_shariah():
    """Finds top 7 Halal stocks ranked by AI Score"""
    try:
        ws = client.open_by_key(SHEET_ID).worksheet("Recommendations")
        ws.clear()
        ws.append_row()
        
        # In a real scenario, you'd loop through a full index (e.g., S&P 500).
        # For this demo, we check a predefined list of popular tech/energy stocks.
        candidates =
        
        compliant_list =
        
        for ticker in candidates:
            compliance = engine.screen_shariah(ticker)
            if compliance['compliant']:
                # If Halal, get the AI Score
                analysis = engine.get_forecast_and_score(ticker)
                compliant_list.append([
                    ticker,
                    "HALAL",
                    compliance['ratios'],
                    analysis['ai_score'],
                    analysis['signal'],
                    analysis['roi_30d'])
        
        # Sort by AI Score (Highest first) and keep top 7
        compliant_list.sort(key=lambda x: x[1], reverse=True)
        top_7 = compliant_list[:7]
        
        for stock in top_7:
            ws.append_row(stock)
            
        apply_style(ws)
        return jsonify({"message": "Top 7 Shariah Opportunities Generated."})
    except Exception as e:
        return jsonify({"message": str(e)}), 500

@app.route('/update_portfolio', methods=)
def update_portfolio():
    """Updates P/L for owned stocks"""
    try:
        ws = client.open_by_key(SHEET_ID).worksheet("Portfolio")
        rows = ws.get_all_values()[1:] # Skip header
        updates =
        
        for i, row in enumerate(rows):
            ticker = row
            status = row
            if status == "SOLD": continue
            
            qty = float(row[2])
            cost = float(row[1])
            
            # Get live price via AI engine
            analysis = engine.get_forecast_and_score(ticker)
            curr_price = analysis['current_price']
            
            market_val = qty * curr_price
            pl = market_val - (qty * cost)
            pl_pct = (pl / (qty * cost))
            
            r = i + 2
            updates.append({
                'range': f'E{r}:H{r}',
                'values': [[curr_price, market_val, f"{pl_pct:.2%}", analysis['signal']]]
            })
            
        if updates: ws.batch_update(updates)
        apply_style(ws)
        return jsonify({"message": "Portfolio updated with real-time P/L."})
    except Exception as e:
        return jsonify({"message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
