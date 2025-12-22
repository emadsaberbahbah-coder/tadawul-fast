import gspread
import os
import json
from google.oauth2.service_account import Credentials
from gspread_formatting import *

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# Ensure you have your Google Cloud JSON key file in the same folder, named 'credentials.json'
CREDENTIALS_FILE = 'credentials.json' 

# Replace this with your actual Google Sheet ID (found in the URL of your sheet)
# Example: docs.google.com/spreadsheets/d/1BxiMVs0XRA5nPO... -> "1BxiMVs0XRA5nPO..."
SHEET_ID = 'YOUR_SHEET_ID_HERE'

def setup_structure():
    print("--- Starting Sheet Setup ---")
    
    # 1. Authenticate
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        
        # Open the sheet
        sh = client.open_by_key(SHEET_ID)
        print(f"Connected to: {sh.title}")
    except Exception as e:
        print(f"Error connecting: {e}")
        print("Make sure 'credentials.json' is in the folder and you shared the sheet with the client_email inside it.")
        return

    # 2. Define Structures
    tabs = {
        "General Data":,
        "Recommendations":,
        "Portfolio":
    }

    # 3. Create/Reset Tabs
    existing_titles = [ws.title for ws in sh.worksheets()]
    
    for tab_name, headers in tabs.items():
        try:
            # Check if exists
            if tab_name in existing_titles:
                ws = sh.worksheet(tab_name)
                print(f"Updating existing tab: {tab_name}")
                # Optional: ws.clear() # Uncomment if you want to wipe data
            else:
                ws = sh.add_worksheet(title=tab_name, rows=100, cols=20)
                print(f"Created new tab: {tab_name}")
            
            # Update Headers (Row 1)
            ws.update(range_name='A1', values=[headers])
            
            # Apply Formatting (Verdana, Bold Headers)
            fmt_header = cellFormat(
                textFormat=textFormat(fontFamily="Verdana", fontSize=10, bold=True),
                backgroundColor=color(0.9, 0.9, 0.9), # Light Grey background
                horizontalAlignment='CENTER'
            )
            format_cell_range(ws, 'A1:Z1', fmt_header)
            
            # Set Column Widths (Approx 150 pixels)
            set_column_width(ws, 'A:H', 150)

        except Exception as e:
            print(f"Error on {tab_name}: {e}")

    # 4. Cleanup
    # Try to delete default 'Sheet1' if it's empty and unused
    if "Sheet1" in existing_titles and len(sh.worksheets()) > 1:
        try:
            sh.del_worksheet(sh.worksheet("Sheet1"))
            print("Deleted default 'Sheet1'")
        except:
            pass

    print("--- Setup Complete. Your Sheet is ready for the Python Brain. ---")

if __name__ == "__main__":
    setup_structure()
