import os
import json
import gspread
from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials
from gspread_formatting import *
from finance_engine import FinancialExpert

app = Flask(__name__)

# Config
GOOGLE_CREDS = os.environ.get("GOOGLE_CREDENTIALS")
SHEET_ID = os.environ.get("SHEET_ID")
creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDS), scopes=["https://www.googleapis.com/auth/spreadsheets"])
client = gspread.authorize(creds)
engine = FinancialExpert()

def format_sheet(ws):
    apply_format(ws, "A:Z", cellFormat(textFormat=textFormat(fontFamily="Verdana", fontSize=10)))

@app.route('/')
def home(): return "Emad Financial Brain 2.0 Active"

@app.route('/update_general', methods=)
def update_general():
    ws = client.open_by_key(SHEET_ID).worksheet("General Data")
    tickers = ws.col_values(1)[1:] # Skip header
    
    updates =
    for i, ticker in enumerate(tickers):
        try:
            analysis = engine.get_forecast_and_score(ticker)
            # Row index is i + 2 (1-based + header)
            # We assume columns: A=Ticker, B=Price, C=Trend, D=Exp Price, E=ROI, F=AI Score, G=Signal
            row = i + 2
            updates.append({
                'range': f'B{row}:G{row}',
                'values': [[
                    analysis['current_price'],
                    analysis['trend'],
                    analysis['exp_price'],
                    analysis['roi_30d'],
                    analysis['ai_score'],
                    analysis['signal']]
            })
        except: continue
        
    if updates: ws.batch_update(updates)
    format_sheet(ws)
    
    # Color Rules
    rule_green = ConditionalFormatRule(
        ranges=,
        booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['60']), format=CellFormat(backgroundColor=Color(0.8,1,0.8)))
    )
    set_conditional_format_rules(ws, [rule_green])
    
    return jsonify({"message": "General Analysis Complete"})

@app.route('/screen_shariah', methods=)
def screen_shariah():
    ws = client.open_by_key(SHEET_ID).worksheet("Recommendations")
    ws.clear()
    ws.append_row()
    
    # For demo, scanning a small list. In production, scan a full index.
    candidates =
    
    results =
    for ticker in candidates:
        compliance = engine.screen_shariah(ticker)
        if compliance['compliant']:
            analysis = engine.get_forecast_and_score(ticker)
            results.append([
                ticker, 
                "HALAL", 
                compliance['ratios'], 
                analysis['ai_score'], 
                analysis['signal'], 
                analysis['roi_30d'])
            
    # Sort by AI Score (Highest first) and take top 7
    results.sort(key=lambda x: x[1], reverse=True)
    
    for row in results[:7]:
        ws.append_row(row)
        
    format_sheet(ws)
    return jsonify({"message": "Top 7 Opportunities Generated"})

@app.route('/update_portfolio', methods=)
def update_portfolio():
    ws = client.open_by_key(SHEET_ID).worksheet("Portfolio")
    data = ws.get_all_values()[1:] # Skip header
    updates =
    
    for i, row in enumerate(data):
        ticker = row
        qty = float(row[2])
        buy_price = float(row[1])
        
        # Get live data
        analysis = engine.get_forecast_and_score(ticker)
        curr = analysis['current_price']
        
        pl = (curr - buy_price) * qty
        pl_pct = ((curr - buy_price) / buy_price)
        
        r_idx = i + 2
        updates.append({
            'range': f'E{r_idx}:H{r_idx}',
            'values': [[curr, pl, f"{pl_pct:.2%}", analysis['signal']]]
        })
        
    if updates: ws.batch_update(updates)
    format_sheet(ws)
    return jsonify({"message": "Portfolio & Signals Updated"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
