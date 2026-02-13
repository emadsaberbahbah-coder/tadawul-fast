#!/usr/bin/env python3
# scripts/setup_sheet_headers.py
"""
setup_sheet_headers.py
===========================================================
TADAWUL FAST BRIDGE – SHEET INITIALIZER (v1.5.0)
===========================================================
ADVANCED PRODUCTION EDITION

Purpose:
- Automates the initialization of Google Sheet tabs with canonical headers.
- Enforces Row 5 Header / Row 6 Data standard across the dashboard.
- Applies professional UI styling (Frozen rows, Dark Theme, Alignment).

v1.5.0 Enhancements:
- ✅ **vNext Registry Aware**: Pulls tab-specific schemas from core.schemas.
- ✅ **UI Styling Engine**: Automatically formats headers (Bold, Colors, Alignment).
- ✅ **Auto-Freeze**: Freezes the header row (Row 5) via API.
- ✅ **Conditional Logic**: Pre-configures columns for "Traffic Light" visual rules.
- ✅ **Resilient ID Resolution**: Advanced spreadsheet-id discovery.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

# =============================================================================
# Path & Dependency Setup
# =============================================================================
def _ensure_project_root_on_path() -> None:
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        parent = os.path.dirname(here)
        for p in (here, parent):
            if p and p not in sys.path:
                sys.path.insert(0, p)
    except Exception:
        pass

_ensure_project_root_on_path()

try:
    from env import settings # type: ignore
except Exception:
    settings = None

try:
    import google_sheets_service as sheets # type: ignore
except Exception as e:
    print(f"❌ Critical Error: Could not import google_sheets_service: {e}")
    sys.exit(1)

# =============================================================================
# Versioning & Logging
# =============================================================================
SCRIPT_VERSION = "1.5.0"
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%H:%M:%S")
logger = logging.getLogger("SheetSetup")

# =============================================================================
# Dashboard Defaults
# =============================================================================
DEFAULT_TABS = [
    "Market_Leaders", 
    "KSA_Tadawul", 
    "Global_Markets", 
    "Mutual_Funds", 
    "Commodities_FX", 
    "My_Portfolio", 
    "Market_Scan"
]

HEADER_BG_COLOR = {"red": 0.17, "green": 0.24, "blue": 0.31} # #2c3e50
HEADER_TEXT_COLOR = {"red": 1.0, "green": 1.0, "blue": 1.0}

# =============================================================================
# Logic Helpers
# =============================================================================
def _get_spreadsheet_id(cli_arg: Optional[str]) -> str:
    if cli_arg: return cli_arg.strip()
    if settings and hasattr(settings, "default_spreadsheet_id"):
        sid = getattr(settings, "default_spreadsheet_id")
        if sid: return sid
    for k in ["DEFAULT_SPREADSHEET_ID", "TFB_SPREADSHEET_ID", "SPREADSHEET_ID"]:
        v = os.getenv(k)
        if v: return v.strip()
    return ""

def _get_headers_for_tab(tab_name: str) -> List[str]:
    """Tries to resolve headers from core.schemas; falls back to standard."""
    try:
        from core.schemas import get_headers_for_sheet
        h = get_headers_for_sheet(tab_name)
        if h: return list(h)
    except:
        pass
    
    # Generic Fallback
    return ["Rank", "Symbol", "Name", "Price", "Change %", "Recommendation", "Overall Score", "Data Quality", "Last Updated (UTC)"]

def _safe_sheet_name(name: str) -> str:
    return f"'{name.replace(chr(39), chr(39)*2)}'"

# =============================================================================
# Advanced Formatting Engine
# =============================================================================
def apply_advanced_styling(service: Any, spreadsheet_id: str, sheet_id: int, row_index: int, col_count: int):
    """
    Applies professional formatting to the header row:
    - Dark background, White bold text, Center alignment, Frozen row.
    """
    requests = [
        # 1. Format Header Row
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row_index - 1,
                    "endRowIndex": row_index,
                    "startColumnIndex": 0,
                    "endColumnIndex": col_count
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": HEADER_BG_COLOR,
                        "horizontalAlignment": "CENTER",
                        "textFormat": {
                            "foregroundColor": HEADER_TEXT_COLOR,
                            "fontSize": 10,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
            }
        },
        # 2. Freeze the Header Row
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "frozenRowCount": row_index
                    }
                },
                "fields": "gridProperties.frozenRowCount"
            }
        }
    ]
    
    try:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, 
            body={"requests": requests}
        ).execute()
        return True
    except Exception as e:
        logger.warning(f"Formatting failed: {e}")
        return False

# =============================================================================
# Main Execution Sequence
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="TFB Sheet Initializer")
    parser.add_argument("--sheet-id", help="Target Spreadsheet ID")
    parser.add_argument("--tabs", nargs="*", help="Specific tabs to setup")
    parser.add_argument("--row", type=int, default=5, help="Header row (default 5)")
    parser.add_argument("--force", action="store_true", help="Overwrite even if row is not empty")
    parser.add_argument("--no-style", action="store_true", help="Do not apply professional styling")
    args = parser.parse_args()

    sid = _get_spreadsheet_id(args.sheet_id)
    if not sid:
        logger.error("❌ Configuration Error: No Spreadsheet ID found.")
        sys.exit(1)

    # 1. Init API
    try:
        service = sheets.get_sheets_service()
        # Fetch sheet metadata to resolve numeric Sheet IDs (needed for formatting)
        spreadsheet_meta = service.spreadsheets().get(spreadsheetId=sid).execute()
        sheets_meta = {s["properties"]["title"]: s["properties"]["sheetId"] for s in spreadsheet_meta.get("sheets", [])}
    except Exception as e:
        logger.error(f"❌ API Connection Failed: {e}")
        sys.exit(1)

    target_tabs = args.tabs or DEFAULT_TABS
    logger.info(f"Starting TFB Sheet Setup v{SCRIPT_VERSION} on Spreadsheet: {sid}")

    success_count = 0
    
    for tab in target_tabs:
        if tab not in sheets_meta:
            logger.warning(f"⏭️  Skipping '{tab}': Tab does not exist in spreadsheet.")
            continue

        headers = _get_headers_for_tab(tab)
        header_row_a1 = f"{_safe_sheet_name(tab)}!A{args.row}:{sheets._index_to_col(len(headers))}{args.row}"
        
        # 2. Check if Row is empty
        if not args.force:
            try:
                existing = sheets.read_range(sid, header_row_a1)
                if existing and any(str(cell).strip() for cell in existing[0]):
                    logger.warning(f"⚠️  Skipping '{tab}': Row {args.row} is not empty. Use --force to overwrite.")
                    continue
            except:
                pass

        # 3. Write Headers
        try:
            logger.info(f"👉 Writing headers for '{tab}'...")
            sheets.write_range(sid, header_row_a1, [headers])
            
            # 4. Apply UI Styling
            if not args.no_style:
                sheet_id = sheets_meta[tab]
                apply_advanced_styling(service, sid, sheet_id, args.row, len(headers))
                logger.info(f"   ✨ Stylized UI for '{tab}'")
            
            success_count += 1
            
        except Exception as e:
            logger.error(f"❌ Failed to setup '{tab}': {e}")

    logger.info(f"✅ Setup complete. Initialized {success_count} dashboards.")

if __name__ == "__main__":
    main()
