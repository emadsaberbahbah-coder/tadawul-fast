#!/usr/bin/env python3
# scripts/setup_sheet_headers.py
"""
setup_sheet_headers.py
===========================================================
TADAWUL FAST BRIDGE – SHEET INITIALIZER (v2.2.0)
===========================================================
PRODUCTION-HARDENED + SCHEMA-AWARE + UI-STYLED (ROW5/ROW6 STANDARD)

Purpose
- Initializes Google Sheet tabs with canonical headers (Row 5).
- Enforces: Row 5 = Headers, Row 6+ = Data.
- Applies professional UI styling (freeze, filter, banding, formats, widths).
- Schema-aware: pulls headers from core.schemas.get_headers_for_sheet(tab) if available.
- Safe-by-default: never wipes data; only writes header row (unless --force).

Key upgrades vs v1.5.1
- ✅ Preflight: validates Sheets API + spreadsheet access
- ✅ Schema-first header resolution with strong fallbacks per tab type
- ✅ Idempotent + safe: skip non-empty header row unless --force
- ✅ Robust banding: removes existing banding ranges before re-adding
- ✅ Column formats: percent/currency/datetime format mapping by header name
- ✅ Filters + frozen rows + header row height
- ✅ Column widths: smart defaults by header semantics (Rank/Symbol/Name/Notes/Error)
- ✅ Deployment safety: exits 0 on missing deps in CI unless STRICT mode enabled

Exit Codes
- 0: success (or skipped due to missing deps when not strict)
- 1: fatal config/API error
- 2: partial (some tabs failed)

Usage
- python scripts/setup_sheet_headers.py
- python scripts/setup_sheet_headers.py --tabs Market_Leaders Global_Markets
- python scripts/setup_sheet_headers.py --create-missing
- python scripts/setup_sheet_headers.py --force
- python scripts/setup_sheet_headers.py --no-style
- python scripts/setup_sheet_headers.py --dry-run

Strict mode
- Set env SHEET_SETUP_STRICT=1 to fail CI on missing dependencies/errors.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
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

# Deferred Imports for Resilience
settings = None
sheets = None

def _strict_mode() -> bool:
    return str(os.getenv("SHEET_SETUP_STRICT") or "").strip() in {"1", "true", "yes", "on"}

try:
    from env import settings as _settings  # type: ignore
    settings = _settings
except Exception:
    settings = None

try:
    import google_sheets_service as _sheets  # type: ignore
    sheets = _sheets
except Exception as e:
    if _strict_mode():
        print(f"❌ STRICT: Could not import google_sheets_service: {e}")
        raise SystemExit(1)
    # In CI/CD or limited envs, do not break builds.
    print(f"⚠️  setup_sheet_headers skipped (deps missing): {e}")
    raise SystemExit(0)

# =============================================================================
# Versioning & Logging
# =============================================================================
SCRIPT_VERSION = "2.2.0"
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%H:%M:%S")
logger = logging.getLogger("SheetSetup")

# =============================================================================
# Dashboard Defaults
# =============================================================================
DEFAULT_TABS: List[str] = [
    "Market_Leaders",
    "KSA_Tadawul",
    "Global_Markets",
    "Mutual_Funds",
    "Commodities_FX",
    "My_Portfolio",
    "Market_Scan",
    "Insights_Analysis",
]

# UI theme defaults (dark header)
HEADER_BG_COLOR = {"red": 0.17, "green": 0.24, "blue": 0.31}  # #2c3e50
HEADER_TEXT_COLOR = {"red": 1.0, "green": 1.0, "blue": 1.0}

# Banding defaults
BAND_1 = {"red": 1.0, "green": 1.0, "blue": 1.0}
BAND_2 = {"red": 0.96, "green": 0.96, "blue": 0.96}

# =============================================================================
# Helpers
# =============================================================================
def _get_spreadsheet_id(cli_arg: Optional[str]) -> str:
    if cli_arg:
        return cli_arg.strip()

    if settings and hasattr(settings, "default_spreadsheet_id"):
        sid = str(getattr(settings, "default_spreadsheet_id") or "").strip()
        if sid:
            return sid

    for k in ("DEFAULT_SPREADSHEET_ID", "TFB_SPREADSHEET_ID", "SPREADSHEET_ID"):
        v = str(os.getenv(k) or "").strip()
        if v:
            return v

    return ""

def _safe_sheet_name(name: str) -> str:
    # Escape single quote for A1 notation: 'O''Reilly'
    return f"'{name.replace(chr(39), chr(39) * 2)}'"

def _index_to_col(n: int) -> str:
    # 1-based: 1->A, 26->Z, 27->AA
    if n <= 0:
        return "A"
    s = ""
    x = n
    while x > 0:
        x, r = divmod(x - 1, 26)
        s = chr(65 + r) + s
    return s

def _read_range_safe(spreadsheet_id: str, a1: str) -> List[List[Any]]:
    try:
        return sheets.read_range(spreadsheet_id, a1) or []
    except Exception:
        return []

def _write_range_safe(spreadsheet_id: str, a1: str, values: List[List[Any]]) -> None:
    sheets.write_range(spreadsheet_id, a1, values)

def _clear_range_safe(service: Any, spreadsheet_id: str, a1: str) -> None:
    # Prefer helper if present
    fn = getattr(sheets, "clear_range", None)
    if callable(fn):
        fn(spreadsheet_id, a1)
        return
    # Raw Sheets API clear
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=a1,
        body={},
    ).execute()

def _tab_kind(tab_name: str) -> str:
    n = (tab_name or "").strip().lower()
    if "portfolio" in n:
        return "portfolio"
    if "scan" in n:
        return "scan"
    if "insight" in n or "analysis" in n:
        return "insights"
    if "ksa" in n or "tadawul" in n:
        return "ksa"
    if "commod" in n or "fx" in n:
        return "commod_fx"
    if "mutual" in n:
        return "funds"
    return "quotes"

def _get_headers_from_core(tab_name: str) -> Optional[List[str]]:
    try:
        from core.schemas import get_headers_for_sheet  # type: ignore
        h = get_headers_for_sheet(tab_name)
        if isinstance(h, (list, tuple)) and len(h) > 0:
            return [str(x) for x in h]
    except Exception:
        return None
    return None

def _fallback_headers(tab_name: str) -> List[str]:
    """
    Strong fallbacks aligned to your dashboard philosophy.
    If core.schemas is unavailable, we still initialize stable headers safely.
    """
    kind = _tab_kind(tab_name)

    if kind in {"quotes", "ksa", "commod_fx", "funds"}:
        return [
            "Rank",
            "Symbol",
            "Origin",
            "Name",
            "Sector",
            "Sub Sector",
            "Market",
            "Currency",
            "Price",
            "Prev Close",
            "Change",
            "Change %",
            "Day High",
            "Day Low",
            "52W High",
            "52W Low",
            "52W Position %",
            "Volume",
            "Value Traded",
            "Market Value",
            "Volatility (30D)",
            "RSI (14)",
            "Fair Value",
            "Upside %",
            "Valuation Label",
            "Momentum Score",
            "Risk Score",
            "Overall Score",
            "Recommendation",
            "Rec Badge",
            "Forecast Price (1M)",
            "Expected ROI % (1M)",
            "Forecast Price (3M)",
            "Expected ROI % (3M)",
            "Forecast Price (12M)",
            "Expected ROI % (12M)",
            "Forecast Confidence",
            "Forecast Updated (UTC)",
            "Forecast Updated (Riyadh)",
            "Error",
            "Data Source",
            "Data Quality",
            "Last Updated (UTC)",
            "Last Updated (Riyadh)",
        ]

    if kind == "portfolio":
        return [
            "Rank",
            "Symbol",
            "Origin",
            "Name",
            "Market",
            "Currency",
            "Asset Type",
            "Portfolio Group",
            "Broker/Account",
            "Quantity",
            "Avg Cost",
            "Cost Value",
            "Target Weight %",
            "Notes",
            "Price",
            "Prev Close",
            "Change",
            "Change %",
            "Market Value",
            "Unrealized P/L",
            "Unrealized P/L %",
            "Weight %",
            "Rebalance Δ",
            "Fair Value",
            "Upside %",
            "Valuation Label",
            "Forecast Price (1M)",
            "Expected ROI % (1M)",
            "Forecast Price (3M)",
            "Expected ROI % (3M)",
            "Forecast Price (12M)",
            "Expected ROI % (12M)",
            "Forecast Confidence",
            "Forecast Updated (UTC)",
            "Forecast Updated (Riyadh)",
            "Recommendation",
            "Rec Badge",
            "Risk Score",
            "Overall Score",
            "Volatility (30D)",
            "RSI (14)",
            "Error",
            "Data Source",
            "Data Quality",
            "Last Updated (UTC)",
            "Last Updated (Riyadh)",
        ]

    if kind == "scan":
        return [
            "Rank",
            "Symbol",
            "Origin",
            "Name",
            "Market",
            "Currency",
            "Price",
            "Change %",
            "Opportunity Score",
            "Overall Score",
            "Recommendation",
            "Rec Badge",
            "Reasoning",
            "Fair Value",
            "Upside %",
            "Expected ROI % (1M)",
            "Expected ROI % (3M)",
            "Expected ROI % (12M)",
            "Forecast Confidence",
            "RSI (14)",
            "MA200",
            "Volatility (30D)",
            "Risk Score",
            "Quality Score",
            "Momentum Score",
            "Data Quality",
            "Last Updated (UTC)",
            "Last Updated (Riyadh)",
            "Error",
        ]

    # insights
    return [
        "Section",
        "Rank",
        "Symbol",
        "Name",
        "Market",
        "Price",
        "Recommendation",
        "Rec Badge",
        "Reason (Explain)",
        "Expected ROI % (1M)",
        "Expected ROI % (3M)",
        "Expected ROI % (12M)",
        "Risk Bucket",
        "Confidence Bucket",
        "Advisor Score",
        "Allocated Amount (SAR)",
        "Expected Gain/Loss 1M (SAR)",
        "Expected Gain/Loss 3M (SAR)",
        "Data Quality",
        "Last Updated (UTC)",
        "Last Updated (Riyadh)",
        "Error",
    ]

def _resolve_headers(tab_name: str) -> List[str]:
    h = _get_headers_from_core(tab_name)
    if h:
        return h
    return _fallback_headers(tab_name)

def _is_row_non_empty(row: Sequence[Any]) -> bool:
    for c in row:
        if str(c or "").strip():
            return True
    return False

def _col_width_by_header(h: str) -> Optional[int]:
    """
    Pixel width suggestion by semantic header name.
    Return None for "no opinion".
    """
    s = (h or "").strip().lower()
    if s in {"rank"}:
        return 60
    if s in {"symbol", "ticker"}:
        return 110
    if "name" in s:
        return 260
    if "notes" in s or "reason" in s or "explain" in s:
        return 320
    if "error" in s:
        return 280
    if "sector" in s or "sub sector" in s:
        return 160
    if "market" in s or "currency" in s:
        return 110
    if "updated" in s or "time" in s:
        return 190
    if "forecast" in s:
        return 170
    if "%" in h:
        return 130
    return 140

def _num_format_for_header(h: str) -> Optional[Dict[str, str]]:
    """
    Returns Google Sheets numberFormat dict if this header matches.
    """
    s = (h or "").strip().lower()

    # Datetimes
    if "last updated" in s or "forecast updated" in s:
        return {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}

    # Percent-ish
    if "%" in h or "roi" in s or "upside" in s or "weight" in s or "position" in s:
        return {"type": "NUMBER", "pattern": "0.00"}  # keep as numeric percent points (e.g., 12.34)

    # Money/value
    if "price" in s or "value" in s or "cost" in s or "p/l" in s or "amount" in s or "fair value" in s:
        return {"type": "NUMBER", "pattern": "#,##0.00"}

    # Volume (integer-ish)
    if "volume" in s or "points" in s:
        return {"type": "NUMBER", "pattern": "#,##0"}

    # Scores
    if "score" in s or "rsi" in s or "volatility" in s:
        return {"type": "NUMBER", "pattern": "0.00"}

    return None

# =============================================================================
# Styling (BatchUpdate)
# =============================================================================
@dataclass(frozen=True)
class StyleOptions:
    font: str = "Nunito"
    header_font_size: int = 10
    header_row_height: int = 32
    apply_banding: bool = True
    apply_filter: bool = True
    apply_widths: bool = True
    apply_number_formats: bool = True
    protect_headers_warning_only: bool = True

def _collect_banding_ids(spreadsheet_meta: Dict[str, Any], sheet_id: int) -> List[int]:
    ids: List[int] = []
    try:
        for sh in spreadsheet_meta.get("sheets", []) or []:
            props = sh.get("properties") or {}
            if int(props.get("sheetId") or -1) != int(sheet_id):
                continue
            brs = sh.get("bandedRanges") or []
            for br in brs:
                bid = br.get("bandedRangeId")
                if isinstance(bid, int):
                    ids.append(bid)
    except Exception:
        return []
    return ids

def _build_style_requests(
    *,
    spreadsheet_meta: Dict[str, Any],
    sheet_id: int,
    header_row_1based: int,
    col_count: int,
    headers: List[str],
    style: StyleOptions,
) -> List[Dict[str, Any]]:
    """
    Build batchUpdate requests:
    - remove existing banding
    - style header row
    - freeze rows
    - apply basic filter
    - add banding (data rows)
    - set column widths
    - set column number formats
    - protect header row (warning-only by default)
    """
    reqs: List[Dict[str, Any]] = []

    # Remove existing banding to avoid duplicates
    if style.apply_banding:
        for bid in _collect_banding_ids(spreadsheet_meta, sheet_id):
            reqs.append({"deleteBanding": {"bandedRangeId": bid}})

    header_row0 = max(0, header_row_1based - 1)

    # Header formatting
    reqs.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": header_row0,
                    "endRowIndex": header_row0 + 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": col_count,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": HEADER_BG_COLOR,
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "wrapStrategy": "WRAP",
                        "textFormat": {
                            "foregroundColor": HEADER_TEXT_COLOR,
                            "fontFamily": style.font,
                            "fontSize": int(style.header_font_size),
                            "bold": True,
                        },
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)",
            }
        }
    )

    # Row height for header row
    reqs.append(
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": header_row0,
                    "endIndex": header_row0 + 1,
                },
                "properties": {"pixelSize": int(style.header_row_height)},
                "fields": "pixelSize",
            }
        }
    )

    # Freeze rows (freeze through header row, so row 1..header_row are frozen)
    reqs.append(
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": int(header_row_1based)}},
                "fields": "gridProperties.frozenRowCount",
            }
        }
    )

    # Apply filter over header row (A{row}:{end}{row})
    if style.apply_filter:
        reqs.append(
            {
                "setBasicFilter": {
                    "filter": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": header_row0,
                            "endRowIndex": header_row0 + 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": col_count,
                        }
                    }
                }
            }
        )

    # Banding for data rows (start AFTER header row)
    if style.apply_banding:
        reqs.append(
            {
                "addBanding": {
                    "bandedRange": {
                        "range": {"sheetId": sheet_id, "startRowIndex": header_row0 + 1, "startColumnIndex": 0, "endColumnIndex": col_count},
                        "rowProperties": {"firstBandColor": BAND_1, "secondBandColor": BAND_2},
                    }
                }
            }
        )

    # Column widths
    if style.apply_widths and headers:
        for idx, h in enumerate(headers):
            w = _col_width_by_header(h)
            if not w:
                continue
            reqs.append(
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": idx, "endIndex": idx + 1},
                        "properties": {"pixelSize": int(w)},
                        "fields": "pixelSize",
                    }
                }
            )

    # Column number formats (apply to data rows downwards; exclude header row)
    if style.apply_number_formats and headers:
        data_row0 = header_row0 + 1
        for idx, h in enumerate(headers):
            fmt = _num_format_for_header(h)
            if not fmt:
                continue
            reqs.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": data_row0,
                            "startColumnIndex": idx,
                            "endColumnIndex": idx + 1,
                        },
                        "cell": {"userEnteredFormat": {"numberFormat": fmt, "textFormat": {"fontFamily": style.font, "fontSize": 10}}},
                        "fields": "userEnteredFormat(numberFormat,textFormat)",
                    }
                }
            )

    # Protect header row (warning-only by default)
    reqs.append(
        {
            "addProtectedRange": {
                "protectedRange": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": header_row0,
                        "endRowIndex": header_row0 + 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": col_count,
                    },
                    "description": "TFB: Header row protection",
                    "warningOnly": bool(style.protect_headers_warning_only),
                }
            }
        }
    )

    return reqs

# =============================================================================
# Main
# =============================================================================
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="TFB Sheet Initializer (Row5/Row6 Standard)")
    ap.add_argument("--sheet-id", help="Target Spreadsheet ID")
    ap.add_argument("--tabs", nargs="*", help="Specific tabs to setup (default: dashboard set)")
    ap.add_argument("--all-existing", action="store_true", help="Initialize ALL existing tabs in spreadsheet")
    ap.add_argument("--create-missing", action="store_true", help="Auto-create missing tabs from the target list")
    ap.add_argument("--row", type=int, default=5, help="Header row (1-based). Default 5.")
    ap.add_argument("--force", action="store_true", help="Overwrite header row even if non-empty")
    ap.add_argument("--clear-header-range", action="store_true", help="Clear header row range before writing headers")
    ap.add_argument("--no-style", action="store_true", help="Do not apply UI styling")
    ap.add_argument("--dry-run", action="store_true", help="Resolve actions only; do not write or style")
    ap.add_argument("--font", default="Nunito", help="Font family (default: Nunito)")
    ap.add_argument("--no-banding", action="store_true", help="Disable alternating row banding")
    ap.add_argument("--no-filter", action="store_true", help="Disable basic filter on header row")
    ap.add_argument("--no-widths", action="store_true", help="Disable smart column widths")
    ap.add_argument("--no-numfmt", action="store_true", help="Disable number format application")
    ap.add_argument("--protect-hard", action="store_true", help="Protect header row (not warning-only)")
    args = ap.parse_args(argv)

    sid = _get_spreadsheet_id(args.sheet_id)
    if not sid:
        logger.error("❌ Configuration Error: No Spreadsheet ID found.")
        return 1

    header_row = int(args.row or 5)
    if header_row < 1:
        logger.error("❌ Invalid --row. Must be >= 1.")
        return 1

    # Init API
    try:
        service = sheets.get_sheets_service()
        spreadsheet_meta = service.spreadsheets().get(spreadsheetId=sid).execute()
    except Exception as e:
        logger.error("❌ Google Sheets API connection failed: %s", e)
        return 1

    # Map titles -> sheetId
    sheets_meta: Dict[str, int] = {}
    try:
        for sh in spreadsheet_meta.get("sheets", []) or []:
            props = sh.get("properties") or {}
            title = str(props.get("title") or "")
            sheet_id = int(props.get("sheetId") or -1)
            if title and sheet_id >= 0:
                sheets_meta[title] = sheet_id
    except Exception:
        pass

    # Target tabs selection
    if args.all_existing:
        target_tabs = list(sheets_meta.keys())
    else:
        target_tabs = args.tabs or list(DEFAULT_TABS)

    logger.info("Starting TFB Sheet Setup v%s | Spreadsheet: %s", SCRIPT_VERSION, sid)
    logger.info("Target tabs: %s", ", ".join(target_tabs))

    style = StyleOptions(
        font=str(args.font or "Nunito").strip() or "Nunito",
        apply_banding=not bool(args.no_banding),
        apply_filter=not bool(args.no_filter),
        apply_widths=not bool(args.no_widths),
        apply_number_formats=not bool(args.no_numfmt),
        protect_headers_warning_only=not bool(args.protect_hard),
    )

    ok_count = 0
    skip_count = 0
    fail_count = 0

    for tab in target_tabs:
        tab = str(tab or "").strip()
        if not tab:
            continue

        # Create missing tab if requested
        if tab not in sheets_meta:
            if not args.create_missing:
                logger.warning("⚠️  Missing tab '%s' (use --create-missing to auto-create). Skipping.", tab)
                skip_count += 1
                continue

            if args.dry_run:
                logger.info("[DRY] Would create tab: '%s'", tab)
                # fake sheetId for dry run
                sheets_meta[tab] = -1
            else:
                try:
                    req = {"requests": [{"addSheet": {"properties": {"title": tab}}}]}
                    resp = service.spreadsheets().batchUpdate(spreadsheetId=sid, body=req).execute()
                    new_sheet_id = int(resp["replies"][0]["addSheet"]["properties"]["sheetId"])
                    sheets_meta[tab] = new_sheet_id
                    # refresh meta so banding removal sees correct structures
                    spreadsheet_meta = service.spreadsheets().get(spreadsheetId=sid).execute()
                    logger.info("🆕 Created tab '%s' (sheetId=%s)", tab, new_sheet_id)
                except Exception as e:
                    logger.error("❌ Failed to create tab '%s': %s", tab, e)
                    fail_count += 1
                    continue

        headers = _resolve_headers(tab)
        col_count = len(headers)
        if col_count <= 0:
            logger.error("❌ Resolved EMPTY headers for '%s' (unsafe). Skipping.", tab)
            fail_count += 1
            continue

        end_col = _index_to_col(col_count)
        header_range_a1 = f"{_safe_sheet_name(tab)}!A{header_row}:{end_col}{header_row}"

        # Check existing header row
        existing = _read_range_safe(sid, header_range_a1)
        existing_row = existing[0] if existing and isinstance(existing[0], list) else []

        if not args.force and existing_row and _is_row_non_empty(existing_row):
            logger.warning("⚠️  Skipping '%s': Row %d not empty. Use --force to overwrite.", tab, header_row)
            skip_count += 1
            continue

        if args.dry_run:
            logger.info("[DRY] Would write %d headers to %s", col_count, header_range_a1)
            if not args.no_style:
                logger.info("[DRY] Would apply styling to '%s' (freeze/filter/banding/widths/numfmt)", tab)
            ok_count += 1
            continue

        # Optional: clear the header range before writing (helps remove stale trailing headers)
        if args.clear_header_range:
            try:
                _clear_range_safe(service, sid, header_range_a1)
            except Exception as e:
                logger.warning("Header clear warning for '%s': %s", tab, e)

        # Write headers
        try:
            _write_range_safe(sid, header_range_a1, [headers])
            logger.info("✅ Wrote headers for '%s' (%d cols)", tab, col_count)
        except Exception as e:
            logger.error("❌ Failed writing headers for '%s': %s", tab, e)
            fail_count += 1
            continue

        # Styling
        if not args.no_style:
            sheet_id = int(sheets_meta.get(tab) or -1)
            if sheet_id < 0:
                logger.warning("Styling skipped for '%s': sheetId not found.", tab)
            else:
                try:
                    requests = _build_style_requests(
                        spreadsheet_meta=spreadsheet_meta,
                        sheet_id=sheet_id,
                        header_row_1based=header_row,
                        col_count=col_count,
                        headers=headers,
                        style=style,
                    )
                    service.spreadsheets().batchUpdate(
                        spreadsheetId=sid,
                        body={"requests": requests},
                    ).execute()
                    logger.info("✨ Styled '%s' (freeze/filter/banding/widths/numfmt/protect)", tab)
                except Exception as e:
                    logger.warning("Styling warning for '%s': %s", tab, e)

        ok_count += 1

    logger.info("--- Summary ---")
    logger.info("OK: %d | Skipped: %d | Failed: %d", ok_count, skip_count, fail_count)

    if fail_count > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
