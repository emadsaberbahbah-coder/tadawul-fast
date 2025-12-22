import os
import json
import gspread
from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials
from gspread_formatting import *
from finance_engine import FinancialExpert

app = Flask(__name__)

# --- Configuration ---
# Load Google Credentials from Environment Variable (safer than file)
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDENTIALS")
SHEET_ID = os.environ.get("SHEET_ID")

if not GOOGLE_CREDS_JSON or not SHEET_ID:
    raise ValueError("Missing GOOGLE_CREDENTIALS or SHEET_ID in environment variables.")

# Authenticate with Google Sheets
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDS_JSON), scopes=scopes)
client = gspread.authorize(creds)

# Initialize Financial Engine
engine = FinancialExpert()

# --- Helper Functions ---

def apply_verdana_font(worksheet):
    """Enforces Verdana font on the entire sheet."""
    fmt = cellFormat(
        textFormat=textFormat(fontFamily="Verdana", fontSize=10)
    )
    format_cell_range(worksheet, 'A1:Z1000', fmt)

def apply_conditional_formatting(worksheet, range_str):
    """Applies Green/Red formatting for positive/negative values."""
    # Green for > 0
    rule_positive = ConditionalFormatRule(
        ranges=,
        booleanRule=BooleanRule(
            condition=BooleanCondition('NUMBER_GREATER', ['0']),
            format=CellFormat(
                backgroundColor=Color(0.85, 0.93, 0.83), # Light Green
                textFormat=textFormat(foregroundColor=Color(0, 0.5, 0), bold=True)
            )
        )
    )
    # Red for < 0
    rule_negative = ConditionalFormatRule(
        ranges=,
        booleanRule=BooleanRule(
            condition=BooleanCondition('NUMBER_LESS', ['0']),
            format=CellFormat(
                backgroundColor=Color(0.96, 0.85, 0.85), # Light Red
                textFormat=textFormat(foregroundColor=Color(0.8, 0, 0), bold=True)
            )
        )
    )
    rules = get_conditional_format_rules(worksheet)
    rules.append(rule_positive)
    rules.append(rule_negative)
    rules.save()

# --- API Routes ---

@app.route('/')
def home():
    return "Emad Bahabh Financial Expert System is Running."

@app.route('/update_general', methods=)
def update_general():
    """Page 1: General Data & Forecasting"""
    try:
        sh = client.open_by_key(SHEET_ID)
        ws = sh.worksheet("General Data") # Ensure tab name matches
        
        # Get Tickers from Column A (skip header)
        tickers = ws.col_values(1)[1:] 
        updates =
        
        for i, ticker in enumerate(tickers):
            row_idx = i + 2
            print(f"Processing {ticker}...")
            
            # 1. Get Forecast
            forecast = engine.get_forecast(ticker)
            
            # 2. Get Basic Info (Sector, etc) via EODHD
            # (Simplified here to just update forecast columns)
            
            # Append update for specific columns (Assuming cols B, C, D are for output)
            # Column C: Trend, Column D: Exp Price, Column E: ROI
            updates.append({
                'range': f'C{row_idx}:E{row_idx}',
                'values': [[
                    forecast['trend'], 
                    forecast['expected_price_30d'],
                    forecast['expected_roi_30d']]
            })

        # Batch update
        ws.batch_update(updates)
        
        # Formatting
        apply_verdana_font(ws)
        apply_conditional_formatting(ws, "E2:E100") # Color ROI column
        
        return jsonify({"status": "success", "message": "General Data Updated"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/screen_shariah', methods=)
def screen_shariah():
    """Page 2: Top 7 Shariah Opportunities"""
    try:
        sh = client.open_by_key(SHEET_ID)
        ws = sh.worksheet("Recommendations")
        ws.clear() # Clear old data
        
        # Header
        ws.append_row()
        
        # Candidate List (In production, fetch S&P500 list, here we use a sample list for speed)
        candidates =
        
        compliant_stocks =
        
        for ticker in candidates:
            # Check Shariah
            compliance = engine.screen_shariah(ticker)
            
            if compliance['compliant']:
                # If compliant, get Analyst Upside
                # (Mocking upside calculation for speed if API call limit is strict)
                # In real scenario: fetch target price from fundamentals
                upside_score = engine.get_forecast(ticker)['expected_price_30d'] # Using forecast as proxy for potential
                
                compliant_stocks.append([
                    ticker, 
                    "HALAL", 
                    compliance['debt_ratio'], 
                    compliance['cash_ratio'],
                    upside_score,
                    "STRONG BUY" # Logic can be enhanced
                ])
        
        # Sort by Upside (descending) and take top 7
        compliant_stocks.sort(key=lambda x: x[1], reverse=True)
        top_7 = compliant_stocks[:7]
        
        # Write to sheet
        for stock in top_7:
            ws.append_row(stock)
            
        apply_verdana_font(ws)
        return jsonify({"status": "success", "message": "Top 7 Generated"})
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/update_portfolio', methods=)
def update_portfolio():
    """Page 3: Portfolio P/L"""
    try:
        sh = client.open_by_key(SHEET_ID)
        ws = sh.worksheet("Portfolio")
        
        # Read All Data (Assumes Headers: Ticker, Status, Qty, Buy Price, Current Price, P/L $, P/L %)
        data = ws.get_all_values()
        headers = data
        rows = data[1:]
        
        updates =
        
        # Extract tickers for batch fetch
        active_tickers = [row for row in rows if row.[2]lower() == 'active']
        
        if active_tickers:
            # Batch fetch prices (implementation depends on engine support, here iterative for safety)
            pass 

        for i, row in enumerate(rows):
            row_idx = i + 2
            ticker = row
            status = row.[2]lower()
            qty = float(row[3])
            buy_price = float(row[4])
            
            if status == 'sold':
                continue # Skip calculation, keep last update
                
            # Fetch Live Price
            # Note: For efficiency, we should batch this, but for clarity:
            live_data = engine.get_market_data_batch([ticker])
            # EODHD returns list, get first item
            current_price = live_data['close'] if live_data else 0
            
            # Calcs
            market_value = qty * current_price
            pl_amount = market_value - (qty * buy_price)
            pl_percent = (pl_amount / (qty * buy_price)) * 100
            
            # Update columns E, F, G (Current Price, P/L $, P/L %)
            updates.append({
                'range': f'E{row_idx}:G{row_idx}',
                'values': [[current_price, pl_amount, f"{pl_percent:.2f}%"]]
            })
            
        ws.batch_update(updates)
        
        apply_verdana_font(ws)
        apply_conditional_formatting(ws, "F2:G100") # Color P/L columns
        
        return jsonify({"status": "success", "message": "Portfolio Updated"})
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
