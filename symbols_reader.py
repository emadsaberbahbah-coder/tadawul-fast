# symbols_reader.py
# Updated to work with the current Google Sheets column structure

import gspread
from google.oauth2.service_account import Credentials

# --- CONFIG ---
CREDENTIALS_FILE = r"gcp-credentials-saudi-stocks.json"
SHEET_ID = "19oloY3fehdFnSRMysqd-EZ2l7FL-GRAd8GJhYUt8tmw"
TAB_NAME = "Market_Leaders"
TAB_GID = 971823989  # fallback by gid

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Updated column mapping based on your actual Google Sheets structure
COLUMN_MAPPING = {
    "symbol": "col_1",        # Contains values like "7201.SR"
    "company_name": "col_2",  # Contains values like "Saudi Company 7201"
    "trading_sector": "col_3", # Contains values like "Materials"
    "financial_market": "col_4" # Contains values like "Tadawul"
}

def fetch_symbols(limit: int | None = None) -> dict:

    """Return dict with symbols data using the actual column structure."""
    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID)

        # Prefer by name, fallback by gid
        try:
            ws = sh.worksheet(TAB_NAME)
        except gspread.WorksheetNotFound:
            ws = None
            for w in sh.worksheets():
                if getattr(w, "id", None) == TAB_GID:
                    ws = w
                    break
            if ws is None:
                return {
                    "sheet_title": sh.title, 
                    "worksheet": "Not Found", 
                    "count": 0, 
                    "data": [],
                    "info": {"error": f"Worksheet '{TAB_NAME}' not found"}
                }

        rows = ws.get_all_values()
        if not rows or len(rows) <= 3:  # Check if we have data beyond header rows
            return {
                "sheet_title": sh.title, 
                "worksheet": ws.title, 
                "count": 0, 
                "data": [],
                "info": {"warning": "No data rows found"}
            }

        # Start from row 4 (index 3) to skip header rows
        data = []
        for row in rows[3:]:  # Skip first 3 header rows
            # Skip empty rows
            if not any(cell.strip() for cell in row):
                continue
                
            symbol = row[0].strip() if len(row) > 0 else ""  # col_1
            company = row[1].strip() if len(row) > 1 else ""  # col_2
            sector = row[2].strip() if len(row) > 2 else ""   # col_3
            market = row[3].strip() if len(row) > 3 else ""   # col_4

            # Only include rows that have a symbol
            if symbol:
                data.append({
                    "symbol": symbol,
                    "company_name": company,
                    "trading_sector": sector,
                    "financial_market": market
                })

        return {
            "sheet_title": sh.title,
            "worksheet": ws.title,
            "count": len(data),
            "data": data,
            "info": {
                "source": "direct_column_mapping",
                "header_row_skipped": 3,
                "columns_used": list(COLUMN_MAPPING.values())
            }
        }

    except Exception as e:
        return {
            "sheet_title": "Error",
            "worksheet": "Error", 
            "count": 0,
            "data": [],
            "info": {"error": f"Failed to read symbols: {str(e)}"}
        }
