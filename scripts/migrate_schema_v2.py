#!/usr/bin/env python3
# scripts/migrate_schema_v2.py
"""
================================================================================
Schema Migration Tool — v2.0.0 (ONE-TIME / SAFE / SCHEMA-DRIVEN)
================================================================================
Tadawul Fast Bridge (TFB)

What this tool does (in strict order):
1) Safely handle legacy/old tabs:
   - KSA_Tadawul (forbidden) -> archive (default) OR delete (--delete-legacy)
   - Advisor_Criteria (forbidden) -> archive (default) OR delete (--delete-legacy)
   - Any non-canonical alias tabs -> rename to canonical when safe; otherwise archive duplicates

2) Ensure canonical tabs exist (creates missing):
   Market_Leaders, Global_Markets, Commodities_FX, Mutual_Funds, My_Portfolio,
   Insights_Analysis, Top_10_Investments, Data_Dictionary

3) Re-seed headers for ALL tabs from core/sheets/schema_registry.py
   - Instrument tables: write header row (row 1)
   - Insights_Analysis: write criteria block at top, then insights header row below
   - Data_Dictionary: fully rebuild and write immediately (headers + rows)

4) Apply minimal formatting:
   - Freeze header row for instrument tables
   - Add basic filter on header row for instrument tables

Safety defaults:
- DRY-RUN by default (no changes). Use --apply to actually write.
- Legacy tabs are ARCHIVED by default (renamed), not deleted.
- No network calls at import-time. All I/O happens inside main().

Dependencies:
- google-api-python-client
- google-auth (service account)

Credentials:
- Uses core.config.normalize_google_credentials() if available
- Or GOOGLE_APPLICATION_CREDENTIALS file path env
- Or default ADC if running in a GCP environment

Usage examples:
  python scripts/migrate_schema_v2.py --spreadsheet-id <ID> --apply
  python scripts/migrate_schema_v2.py --apply --delete-legacy
  python scripts/migrate_schema_v2.py --apply --archive-prefix "_ARCHIVE_"

================================================================================
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

# ---- Phase 1 modules (required for correct migration) ----
from core.sheets.schema_registry import SCHEMA_REGISTRY, get_sheet_headers
from core.sheets.data_dictionary import build_data_dictionary_values
from core.sheets.page_catalog import (
    CANONICAL_PAGES,
    FORBIDDEN_PAGES,
    normalize_page_name,
)

# core.config is optional for spreadsheet id / creds normalization
try:
    from core.config import normalize_google_credentials, strip_value  # type: ignore
except Exception:

    def strip_value(v: Any) -> str:
        try:
            return str(v).strip() if v is not None else ""
        except Exception:
            return ""

    def normalize_google_credentials():  # type: ignore
        return (None, None)


# =============================================================================
# Google Sheets API helpers (lazy import)
# =============================================================================

def _build_sheets_service():
    """
    Build a Google Sheets API service using service account credentials or ADC.
    """
    try:
        from google.oauth2.service_account import Credentials  # type: ignore
        from googleapiclient.discovery import build  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "Missing Google dependencies. Ensure google-api-python-client and google-auth are installed."
        ) from e

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    # Prefer normalized credentials from core.config
    creds_json, creds_dict = normalize_google_credentials()
    credentials = None

    if isinstance(creds_dict, dict) and creds_dict:
        credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)

    elif creds_json and isinstance(creds_json, str) and creds_json.strip().startswith("{"):
        try:
            d = json.loads(creds_json)
            if isinstance(d, dict) and d:
                credentials = Credentials.from_service_account_info(d, scopes=scopes)
        except Exception:
            pass

    # Fallback to GOOGLE_APPLICATION_CREDENTIALS file path
    if credentials is None:
        fp = strip_value(os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "")
        if fp and os.path.exists(fp):
            credentials = Credentials.from_service_account_file(fp, scopes=scopes)

    # Final fallback: ADC (may work on GCP/Cloud Run)
    if credentials is None:
        try:
            import google.auth  # type: ignore

            credentials, _ = google.auth.default(scopes=scopes)
        except Exception as e:
            raise RuntimeError(
                "Could not load Google credentials. Provide GOOGLE_APPLICATION_CREDENTIALS "
                "or GOOGLE_SHEETS_CREDENTIALS(_B64) or configure ADC."
            ) from e

    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


# =============================================================================
# Models / Result logging
# =============================================================================

@dataclass
class Action:
    kind: str
    detail: str
    request_count: int = 0


@dataclass
class MigrationPlan:
    spreadsheet_id: str
    dry_run: bool
    delete_legacy: bool
    archive_prefix: str
    actions: List[Action]


# =============================================================================
# Spreadsheet operations
# =============================================================================

def _now_tag() -> str:
    return _dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def _safe_title_token(title: str) -> str:
    t = (title or "").strip()
    t = re.sub(r"\s+", "_", t)
    t = re.sub(r"[^A-Za-z0-9_]+", "", t)
    return t[:80] if len(t) > 80 else t


def _get_spreadsheet_metadata(service, spreadsheet_id: str) -> Dict[str, Any]:
    return (
        service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, includeGridData=False)
        .execute()
    )


def _sheets_by_title(meta: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for s in meta.get("sheets", []) or []:
        props = s.get("properties", {}) or {}
        title = props.get("title")
        if title:
            out[title] = props
    return out


def _sheet_id(props: Dict[str, Any]) -> int:
    return int(props.get("sheetId"))


def _batch_update(service, spreadsheet_id: str, requests: List[Dict[str, Any]]) -> Dict[str, Any]:
    body = {"requests": requests}
    return service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def _values_update(service, spreadsheet_id: str, a1_range: str, values_2d: List[List[Any]]) -> Dict[str, Any]:
    body = {"values": values_2d}
    return (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=a1_range,
            valueInputOption="RAW",
            body=body,
        )
        .execute()
    )


def _values_clear(service, spreadsheet_id: str, a1_range: str) -> Dict[str, Any]:
    return (
        service.spreadsheets()
        .values()
        .clear(spreadsheetId=spreadsheet_id, range=a1_range, body={})
        .execute()
    )


# =============================================================================
# Planning: legacy handling + ensure canonical sheets
# =============================================================================

def _is_forbidden_title(title: str) -> bool:
    # raw match
    if title in FORBIDDEN_PAGES:
        return True
    # normalized token match (covers e.g. "KSA Tadawul")
    tok = _safe_title_token(title).lower()
    return tok in { "ksa_tadawul", "advisor_criteria" }


def _resolve_canonical_or_none(title: str) -> Optional[str]:
    try:
        return normalize_page_name(title, allow_output_pages=True)
    except Exception:
        return None


def _plan_legacy_actions(
    existing_titles: Sequence[str],
    archive_prefix: str,
    delete_legacy: bool,
) -> Tuple[List[Tuple[str, str, str]], List[str]]:
    """
    Returns:
      rename_ops: list of (old_title, new_title, reason)
      delete_titles: list of titles to delete
    """
    rename_ops: List[Tuple[str, str, str]] = []
    delete_titles: List[str] = []

    existing_set = set(existing_titles)

    # 1) forbidden legacy tabs: archive or delete
    for t in existing_titles:
        if _is_forbidden_title(t):
            if delete_legacy:
                delete_titles.append(t)
            else:
                new_name = f"{archive_prefix}{_safe_title_token(t)}_{_now_tag()}"
                # avoid collisions
                if new_name in existing_set:
                    new_name = f"{new_name}_{uuid4_short()}"
                rename_ops.append((t, new_name, "archive_forbidden_legacy"))

    # 2) alias tabs: rename to canonical when safe
    # For every sheet title, if it normalizes to a canonical different title:
    for t in existing_titles:
        if _is_forbidden_title(t):
            continue
        canonical = _resolve_canonical_or_none(t)
        if not canonical:
            continue
        if canonical == t:
            continue

        if canonical not in existing_set:
            rename_ops.append((t, canonical, "rename_alias_to_canonical"))
            existing_set.add(canonical)
        else:
            # canonical exists already; archive the alias to avoid duplicates
            new_name = f"{archive_prefix}{_safe_title_token(t)}_{_now_tag()}"
            if new_name in existing_set:
                new_name = f"{new_name}_{uuid4_short()}"
            rename_ops.append((t, new_name, "archive_duplicate_alias"))

    return rename_ops, delete_titles


def uuid4_short() -> str:
    import uuid
    return uuid.uuid4().hex[:8]


def _plan_missing_canonical_sheets(existing_titles: Sequence[str]) -> List[str]:
    existing_set = set(existing_titles)
    missing = [s for s in CANONICAL_PAGES if s not in existing_set]
    return missing


# =============================================================================
# Execution: apply plan
# =============================================================================

def _apply_rename(service, spreadsheet_id: str, sheets_props: Dict[str, Dict[str, Any]], old_title: str, new_title: str) -> int:
    props = sheets_props.get(old_title)
    if not props:
        return 0
    sid = _sheet_id(props)
    req = {
        "updateSheetProperties": {
            "properties": {"sheetId": sid, "title": new_title},
            "fields": "title",
        }
    }
    _batch_update(service, spreadsheet_id, [req])
    return 1


def _apply_delete(service, spreadsheet_id: str, sheets_props: Dict[str, Dict[str, Any]], title: str) -> int:
    props = sheets_props.get(title)
    if not props:
        return 0
    sid = _sheet_id(props)
    req = {"deleteSheet": {"sheetId": sid}}
    _batch_update(service, spreadsheet_id, [req])
    return 1


def _apply_add_sheet(service, spreadsheet_id: str, title: str) -> int:
    req = {"addSheet": {"properties": {"title": title}}}
    _batch_update(service, spreadsheet_id, [req])
    return 1


def _apply_freeze_row1_and_filter(service, spreadsheet_id: str, sheet_id: int) -> int:
    """
    Freeze row 1 and set a basic filter over row 1 across a wide column range.
    """
    requests: List[Dict[str, Any]] = []

    requests.append(
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }
        }
    )

    # Set basic filter for row 1 (endColumnIndex left large; Sheets will clamp)
    requests.append(
        {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": 200,  # generous
                    }
                }
            }
        }
    )

    _batch_update(service, spreadsheet_id, requests)
    return len(requests)


# =============================================================================
# Seeding: headers + insights criteria + data dictionary
# =============================================================================

def _seed_instrument_headers(service, spreadsheet_id: str, sheet_title: str) -> int:
    headers = get_sheet_headers(sheet_title)
    # write row1 from A1
    a1 = f"{sheet_title}!A1"
    _values_update(service, spreadsheet_id, a1, [headers])
    return 1


def _seed_insights_analysis(service, spreadsheet_id: str, sheet_title: str = "Insights_Analysis") -> int:
    """
    Writes:
      - Criteria block at top (A1:D?)
      - Insights header row below

    Layout:
      Row 1: ["Advisor Criteria", "", "", ""]
      Row 2: ["Key", "Value", "Label", "Notes"]
      Rows 3..: criteria fields
      Blank row
      Insights headers row (from schema_registry for Insights_Analysis) in column A
    """
    spec = SCHEMA_REGISTRY.get(sheet_title)
    if not spec:
        return 0

    criteria = list(spec.criteria_fields or [])
    insights_headers = get_sheet_headers(sheet_title)

    values: List[List[Any]] = []
    values.append(["Advisor Criteria", "", "", ""])
    values.append(["Key", "Value", "Label", "Notes"])

    for cf in criteria:
        values.append([cf.key, cf.default, cf.label, cf.notes])

    # one blank row
    values.append(["", "", "", ""])

    # insights header row starts at col A
    values.append(insights_headers)

    # Clear a reasonable top area first (to avoid mixing with old layouts)
    # We clear A1:Z200 (safe and quick).
    _values_clear(service, spreadsheet_id, f"{sheet_title}!A1:Z200")
    _values_update(service, spreadsheet_id, f"{sheet_title}!A1", values)

    return 2  # clear + update


def _seed_data_dictionary(service, spreadsheet_id: str, sheet_title: str = "Data_Dictionary") -> int:
    # Build 2D array including header row
    values = build_data_dictionary_values(include_header_row=True, include_meta_sheet=True)

    # Clear a wide range then write
    _values_clear(service, spreadsheet_id, f"{sheet_title}!A1:Z5000")
    _values_update(service, spreadsheet_id, f"{sheet_title}!A1", values)
    return 2


# =============================================================================
# Main migration
# =============================================================================

def build_plan(
    spreadsheet_id: str,
    *,
    dry_run: bool,
    delete_legacy: bool,
    archive_prefix: str,
) -> MigrationPlan:
    actions: List[Action] = []
    return MigrationPlan(
        spreadsheet_id=spreadsheet_id,
        dry_run=dry_run,
        delete_legacy=delete_legacy,
        archive_prefix=archive_prefix,
        actions=actions,
    )


def run_migration(plan: MigrationPlan) -> int:
    service = _build_sheets_service()

    meta = _get_spreadsheet_metadata(service, plan.spreadsheet_id)
    props_by_title = _sheets_by_title(meta)
    existing_titles = list(props_by_title.keys())

    # Plan legacy actions
    rename_ops, delete_titles = _plan_legacy_actions(
        existing_titles,
        archive_prefix=plan.archive_prefix,
        delete_legacy=plan.delete_legacy,
    )

    # Plan missing canonical sheets
    missing = _plan_missing_canonical_sheets(existing_titles)

    # Print plan summary
    print("\n=== MIGRATION PLAN (Schema v2) ===")
    print(f"Spreadsheet: {plan.spreadsheet_id}")
    print(f"Dry-run: {plan.dry_run}")
    print(f"Delete legacy: {plan.delete_legacy}")
    print(f"Archive prefix: {plan.archive_prefix}")
    print("---------------------------------")

    if rename_ops:
        print("\nRename/Archive operations:")
        for old, new, reason in rename_ops:
            print(f"  - {old}  ->  {new}   ({reason})")
    else:
        print("\nRename/Archive operations: (none)")

    if delete_titles:
        print("\nDelete operations:")
        for t in delete_titles:
            print(f"  - delete: {t}")
    else:
        print("\nDelete operations: (none)")

    if missing:
        print("\nMissing canonical sheets to create:")
        for t in missing:
            print(f"  - add: {t}")
    else:
        print("\nMissing canonical sheets to create: (none)")

    # Apply if not dry-run
    if plan.dry_run:
        print("\nDRY-RUN: no changes applied.")
        return 0

    print("\nApplying changes...")

    request_count = 0

    # Refresh props each time after structural changes (rename/add/delete)
    def refresh_props() -> Dict[str, Dict[str, Any]]:
        m = _get_spreadsheet_metadata(service, plan.spreadsheet_id)
        return _sheets_by_title(m)

    # 1) Renames/archives
    props_by_title = refresh_props()
    for old, new, reason in rename_ops:
        if old not in props_by_title:
            continue
        _apply_rename(service, plan.spreadsheet_id, props_by_title, old, new)
        request_count += 1
        props_by_title = refresh_props()

    # 2) Deletes (if enabled)
    props_by_title = refresh_props()
    for t in delete_titles:
        if t not in props_by_title:
            continue
        _apply_delete(service, plan.spreadsheet_id, props_by_title, t)
        request_count += 1
        props_by_title = refresh_props()

    # 3) Ensure canonical sheets exist
    props_by_title = refresh_props()
    for t in CANONICAL_PAGES:
        if t not in props_by_title:
            _apply_add_sheet(service, plan.spreadsheet_id, t)
            request_count += 1
            props_by_title = refresh_props()

    # 4) Seed headers + format
    props_by_title = refresh_props()

    for sheet_title in CANONICAL_PAGES:
        spec = SCHEMA_REGISTRY.get(sheet_title)
        if not spec:
            continue

        if sheet_title == "Insights_Analysis":
            request_count += _seed_insights_analysis(service, plan.spreadsheet_id, sheet_title)
            continue

        if sheet_title == "Data_Dictionary":
            # rebuilt after we ensure all headers exist
            continue

        # Instrument tables incl Top_10_Investments and My_Portfolio etc.
        request_count += _seed_instrument_headers(service, plan.spreadsheet_id, sheet_title)

        # Minimal formatting (freeze + filter)
        props = props_by_title.get(sheet_title)
        if props:
            request_count += _apply_freeze_row1_and_filter(service, plan.spreadsheet_id, _sheet_id(props))

    # 5) Rebuild Data_Dictionary immediately
    request_count += _seed_data_dictionary(service, plan.spreadsheet_id, "Data_Dictionary")

    print("\n✅ Migration complete.")
    print(f"Total API request batches (approx): {request_count}")
    return 0


# =============================================================================
# CLI
# =============================================================================

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="TFB Schema v2 migration tool (tabs + headers + data dictionary).")
    p.add_argument("--spreadsheet-id", default="", help="Target Google Spreadsheet ID. Defaults to DEFAULT_SPREADSHEET_ID env.")
    p.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes. If omitted, runs in dry-run mode.",
    )
    p.add_argument(
        "--delete-legacy",
        action="store_true",
        help="Delete legacy forbidden tabs instead of archiving them (dangerous).",
    )
    p.add_argument(
        "--archive-prefix",
        default="_ARCHIVE_",
        help="Prefix used when archiving old/duplicate tabs.",
    )
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    spreadsheet_id = strip_value(args.spreadsheet_id) or strip_value(os.getenv("DEFAULT_SPREADSHEET_ID") or "")
    if not spreadsheet_id:
        print("ERROR: Missing spreadsheet id. Provide --spreadsheet-id or set DEFAULT_SPREADSHEET_ID env.")
        return 2

    dry_run = not bool(args.apply)
    plan = build_plan(
        spreadsheet_id=spreadsheet_id,
        dry_run=dry_run,
        delete_legacy=bool(args.delete_legacy),
        archive_prefix=strip_value(args.archive_prefix) or "_ARCHIVE_",
    )

    try:
        return run_migration(plan)
    except Exception as e:
        print("\n❌ Migration failed:", str(e))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
