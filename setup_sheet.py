import gspread
from gspread_formatting import *
import os
import json
from google.oauth2.service_account import Credentials

# --- CONFIGURATION ---
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def setup_google_sheet(sheet_url):
    # Auth
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
    
    sh = gc.open_by_url(sheet_url)

    # Define Header Format (Vendana, Bold, Blue Background)
    header_fmt = cellFormat(
        textFormat=textFormat(bold=True, fontFamily="Verdana", fontSize=10),
        backgroundColor=color(0.8, 0.9, 1.0), # Light Blue
        horizontalAlignment='CENTER'
    )
    
    # Define Positive/Negative Conditional Logic
    green_fmt = cellFormat(
        textFormat=textFormat(foregroundColor=color(0, 0.5, 0), fontFamily="Verdana"),
        backgroundColor=color(0.9, 1.0, 0.9)
    )
    red_fmt = cellFormat(
        textFormat=textFormat(foregroundColor=color(0.8, 0, 0), fontFamily="Verdana"),
        backgroundColor=color(1.0, 0.9, 0.9)
    )

    # --- 1. MARKET DATA PAGE ---
    try:
        ws1 = sh.add_worksheet(title="Market Data", rows=100, cols=20)
    except:
        ws1 = sh.worksheet("Market Data")
    
    headers_1 = ["Ticker", "Current Price", "Sector", "AI Forecast (30d)", "Exp. ROI %", "Shariah Compliant", "Score (0-100)"]
    ws1.update('A1:G1', [headers_1])
    format_cell_range(ws1, 'A1:G1', header_fmt)

    # --- 2. RECOMMENDATIONS (Top 7) ---
    try:
        ws2 = sh.add_worksheet(title="Recommendations", rows=50, cols=10)
    except:
        ws2 = sh.worksheet("Recommendations")
        
    headers_2 = ["Rank", "Ticker", "Deep Analysis", "Exp. Profit", "Why Selected?"]
    ws2.update('A1:E1', [headers_2])
    format_cell_range(ws2, 'A1:E1', header_fmt)

    # --- 3. MY INVESTMENT ---
    try:
        ws3 = sh.add_worksheet(title="My Investment", rows=50, cols=10)
    except:
        ws3 = sh.worksheet("My Investment")
        
    headers_3 = ["Stock", "Buy Price", "Qty", "Current Value", "P/L", "Status (Active/Sold)", "Last Update"]
    ws3.update('A1:G1', [headers_3])
    format_cell_range(ws3, 'A1:G1', header_fmt)

    print("Sheet Structure Built Successfully with Verdana Font.")

if __name__ == "__main__":
    # You would typically run this locally or trigger via a separate route
    # setup_google_sheet("YOUR_SHEET_URL")
    pass
