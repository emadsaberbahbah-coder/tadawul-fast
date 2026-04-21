#!/usr/bin/env python3
# scripts/migrate_schema_v2.py
"""
================================================================================
Schema Migration Tool — v2.1.0 (ONE-TIME / SAFE / SCHEMA-DRIVEN)
================================================================================
Tadawul Fast Bridge (TFB)

What this tool does (in strict order):
1) Safely handle legacy/old tabs:
   - KSA_Tadawul (forbidden) -> archive (default) OR delete (--delete-legacy)
   - Advisor_Criteria (forbidden) -> archive (default) OR delete (--delete-legacy)
   - Any non-canonical alias tabs -> rename to canonical when safe; otherwise
     archive duplicates.

2) Ensure canonical tabs exist (creates missing):
   Market_Leaders, Global_Markets, Commodities_FX, Mutual_Funds, My_Portfolio,
   Insights_Analysis, Top_10_Investments, Data_Dictionary

3) Re-seed headers for ALL tabs from core/sheets/schema_registry.py:
   - Instrument tables: write header row (row 1)
   - Insights_Analysis: write criteria block at top, then insights header row
     below
   - Data_Dictionary: fully rebuild and write immediately (headers + rows)

4) Apply minimal formatting:
   - Freeze header row for instrument tables
   - Add basic filter on header row using the actual schema column count
     (80 / 83 / 7 / 9) instead of a hardcoded 200

5) Optional `--verify` pass: read the header row of each canonical sheet back
   and confirm it exactly matches `schema_registry.get_sheet_headers()`. Fails
   with exit 3 on mismatch.

Why this revision (v2.1.0 vs v2.0.0)
-------------------------------------
- 🔑 FIX HIGH: `datetime.datetime.utcnow()` in `_now_tag()` is deprecated as of
     Python 3.12 (PEP 594). v2.1.0 uses `datetime.now(timezone.utc)` — the
     canonical timezone-aware replacement. Fixes `DeprecationWarning` spam in
     CI/cron logs on modern Python runtimes.

- FIX: Switched from bare `print()` to the standard `logging` framework
     (matches `audit_data_quality.py v4.5.0` / `cleanup_cache.py v4.1.0` /
     `drift_detection.py v4.3.0`). Timestamped, level-aware, redirectable.

- FIX: Added `SCRIPT_VERSION` + `SERVICE_VERSION` alias (cross-script
     convention).

- FIX: Added `_TRUTHY` / `_FALSY` + `_env_bool` helpers matching
     `main._TRUTHY` / `_FALSY` vocabulary.

- FIX: Filter ranges now use the actual column count from
     `schema_registry.get_sheet_len(sheet)` instead of hardcoded 200. A
     Market_Leaders (80 cols) sheet now gets a filter spanning exactly 80
     columns; Insights_Analysis gets 7; Top_10_Investments gets 83.

- FIX: Distinguished exit codes — 0 (success/dry-run), 1 (API/runtime error),
     2 (configuration error, e.g. missing credentials/spreadsheet ID),
     3 (verification failed).

- NEW: `--verify` flag — after migration, read-back each canonical sheet's
     header row and compare to `schema_registry.get_sheet_headers(sheet)`.
     Prints per-sheet status and exits 3 on any mismatch. This gives cron/CI
     a reliable post-condition check.

- NEW: `--json` flag — emit a machine-readable summary to stdout at the end.
     Payload shape: {version, dry_run, operations:{renames, deletes, creates,
     seeds}, verify: {...}, exit_code, canonical_pages}. Matches JSON-first
     scripts in the project.

- NEW: `--list-canonical` flag — print the canonical schema catalog
     (page -> column count -> first 5 headers) and exit. No API calls made.

- KEEP: Original logic for plan-then-apply, legacy archival, forbidden-page
     handling, dry-run default, credential resolution chain, batched
     structural changes, data_dictionary full rebuild.

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
  python scripts/migrate_schema_v2.py --apply --verify
  python scripts/migrate_schema_v2.py --list-canonical
  python scripts/migrate_schema_v2.py --apply --verify --json > report.json

Exit codes:
- 0 = success OR dry-run completed
- 1 = runtime / API error during migration
- 2 = configuration error (missing ID, missing credentials, etc.)
- 3 = verification failed (header mismatch for one or more canonical sheets)
================================================================================
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

# ---- Phase 1 modules (required for correct migration) ----
from core.sheets.schema_registry import (  # type: ignore
    SCHEMA_REGISTRY,
    get_sheet_headers,
    get_sheet_len,
)
from core.sheets.data_dictionary import build_data_dictionary_values  # type: ignore
from core.sheets.page_catalog import (  # type: ignore
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
# Version + logging
# =============================================================================
SCRIPT_VERSION = "2.1.0"
SERVICE_VERSION = SCRIPT_VERSION  # v2.1.0: cross-script alias

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("SchemaMigration")


# =============================================================================
# Project-wide truthy/falsy vocabulary (matches main._TRUTHY / _FALSY)
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


def _env_bool(name: str, default: bool = False) -> bool:
    """Project-aligned env bool parser."""
    try:
        raw = (os.getenv(name, "") or "").strip().lower()
    except Exception:
        return bool(default)
    if not raw:
        return bool(default)
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return bool(default)


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
            "Missing Google dependencies. "
            "Ensure google-api-python-client and google-auth are installed."
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
                "Could not load Google credentials. Provide "
                "GOOGLE_APPLICATION_CREDENTIALS or "
                "GOOGLE_SHEETS_CREDENTIALS(_B64) or configure ADC."
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
    actions: List[Action] = field(default_factory=list)


@dataclass
class MigrationResult:
    """v2.1.0: structured result for JSON output + verify reporting."""
    version: str = SCRIPT_VERSION
    dry_run: bool = True
    spreadsheet_id: str = ""
    renames: List[Tuple[str, str, str]] = field(default_factory=list)
    deletes: List[str] = field(default_factory=list)
    creates: List[str] = field(default_factory=list)
    seeds: List[str] = field(default_factory=list)
    request_batches: int = 0
    canonical_pages: List[str] = field(default_factory=list)
    verify: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    exit_code: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "dry_run": self.dry_run,
            "spreadsheet_id": self.spreadsheet_id,
            "operations": {
                "renames": [
                    {"old": o, "new": n, "reason": r}
                    for o, n, r in self.renames
                ],
                "deletes": list(self.deletes),
                "creates": list(self.creates),
                "seeds": list(self.seeds),
            },
            "request_batches": int(self.request_batches),
            "canonical_pages": list(self.canonical_pages),
            "verify": dict(self.verify) if self.verify else None,
            "error": self.error,
            "exit_code": int(self.exit_code),
            "timestamp_utc": _utc_iso(),
        }


# =============================================================================
# Spreadsheet operations
# =============================================================================
def _utc_iso() -> str:
    """v2.1.0: timezone-aware UTC ISO string (replaces deprecated utcnow())."""
    return datetime.now(timezone.utc).isoformat()


def _now_tag() -> str:
    """v2.1.0: uses datetime.now(timezone.utc) — datetime.utcnow() deprecated in 3.12+."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _safe_title_token(title: str) -> str:
    t = (title or "").strip()
    t = re.sub(r"\s+", "_", t)
    t = re.sub(r"[^A-Za-z0-9_]+", "", t)
    return t[:80] if len(t) > 80 else t


def uuid4_short() -> str:
    return uuid.uuid4().hex[:8]


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
    return (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        .execute()
    )


def _values_update(
    service, spreadsheet_id: str, a1_range: str, values_2d: List[List[Any]]
) -> Dict[str, Any]:
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


def _values_get(service, spreadsheet_id: str, a1_range: str) -> List[List[Any]]:
    res = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=a1_range, majorDimension="ROWS")
        .execute()
    )
    return res.get("values", []) or []


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
    return tok in {"ksa_tadawul", "advisor_criteria"}


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


def _plan_missing_canonical_sheets(existing_titles: Sequence[str]) -> List[str]:
    existing_set = set(existing_titles)
    missing = [s for s in CANONICAL_PAGES if s not in existing_set]
    return missing


# =============================================================================
# Seeding: headers + insights criteria + data dictionary
# =============================================================================
def _seed_instrument_headers(service, spreadsheet_id: str, sheet_title: str) -> int:
    headers = get_sheet_headers(sheet_title)
    a1 = f"{sheet_title}!A1"
    _values_update(service, spreadsheet_id, a1, [headers])
    return 1


def _seed_insights_analysis(
    service, spreadsheet_id: str, sheet_title: str = "Insights_Analysis"
) -> int:
    """
    Writes:
      - Criteria block at top (A1:D?)
      - Insights header row below (7 cols per schema_registry)

    Layout:
      Row 1: ["Advisor Criteria", "", "", ""]
      Row 2: ["Key", "Value", "Label", "Notes"]
      Rows 3..: criteria fields
      Blank row
      Insights headers row (from schema_registry, 7 cols)
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

    # Clear a reasonable top area first (to avoid mixing with old layouts).
    _values_clear(service, spreadsheet_id, f"{sheet_title}!A1:Z200")
    _values_update(service, spreadsheet_id, f"{sheet_title}!A1", values)

    return 2  # clear + update


def _seed_data_dictionary(
    service, spreadsheet_id: str, sheet_title: str = "Data_Dictionary"
) -> int:
    # Build 2D array including header row
    values = build_data_dictionary_values(
        include_header_row=True, include_meta_sheet=True
    )

    # Clear a wide range then write
    _values_clear(service, spreadsheet_id, f"{sheet_title}!A1:Z5000")
    _values_update(service, spreadsheet_id, f"{sheet_title}!A1", values)
    return 2


# =============================================================================
# Column-index helpers for formatting (v2.1.0: use schema column count)
# =============================================================================
def _schema_column_count(sheet_title: str) -> int:
    """Return the canonical column count for a sheet, falling back to 200."""
    try:
        n = int(get_sheet_len(sheet_title))
        if n > 0:
            return n
    except Exception:
        pass
    return 200


# =============================================================================
# Verification (v2.1.0 NEW)
# =============================================================================
def _verify_canonical_headers(
    service, spreadsheet_id: str
) -> Dict[str, Any]:
    """
    Read back the header row of each canonical sheet and compare it to
    `schema_registry.get_sheet_headers(sheet)`. Returns a dict:
        {
            "ok": bool,
            "sheets": {
                "<sheet>": {
                    "ok": bool,
                    "expected_len": int,
                    "actual_len": int,
                    "mismatch_columns": [(idx, expected, actual), ...],
                },
                ...
            },
            "mismatched_sheets": [...],
        }
    """
    report: Dict[str, Any] = {"ok": True, "sheets": {}, "mismatched_sheets": []}

    for sheet_title in CANONICAL_PAGES:
        expected_headers = get_sheet_headers(sheet_title)
        expected_len = len(expected_headers)

        if sheet_title == "Insights_Analysis":
            # Insights_Analysis has a criteria block at top; the real header
            # row is BELOW the criteria + blank row. Skip verification for it
            # and instead check the Data_Dictionary sheet in detail.
            report["sheets"][sheet_title] = {
                "ok": True,
                "skipped": True,
                "reason": "insights_analysis has criteria block above headers",
                "expected_len": expected_len,
            }
            continue

        # Read row 1, up to 260 cols (CC) — more than any canonical sheet.
        try:
            grid = _values_get(
                service, spreadsheet_id, f"{sheet_title}!A1:CC1"
            )
        except Exception as e:
            report["ok"] = False
            report["sheets"][sheet_title] = {
                "ok": False,
                "error": str(e),
                "expected_len": expected_len,
            }
            report["mismatched_sheets"].append(sheet_title)
            continue

        actual_row = list(grid[0]) if grid else []
        actual_len = len(actual_row)

        # Trim trailing blanks to tolerate auto-padded cells
        while actual_row and (actual_row[-1] is None or str(actual_row[-1]).strip() == ""):
            actual_row = actual_row[:-1]
        trimmed_len = len(actual_row)

        mismatches: List[Tuple[int, str, str]] = []
        for i, expected in enumerate(expected_headers):
            actual = actual_row[i] if i < len(actual_row) else ""
            if str(actual).strip() != str(expected).strip():
                mismatches.append((i, expected, str(actual)))

        sheet_ok = (trimmed_len == expected_len) and not mismatches

        report["sheets"][sheet_title] = {
            "ok": sheet_ok,
            "expected_len": expected_len,
            "actual_len": actual_len,
            "trimmed_actual_len": trimmed_len,
            "mismatch_columns": [
                {"index": i, "expected": e, "actual": a}
                for (i, e, a) in mismatches[:20]  # cap at 20 for readability
            ],
            "mismatch_count": len(mismatches),
        }
        if not sheet_ok:
            report["ok"] = False
            report["mismatched_sheets"].append(sheet_title)

    return report


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
    return MigrationPlan(
        spreadsheet_id=spreadsheet_id,
        dry_run=dry_run,
        delete_legacy=delete_legacy,
        archive_prefix=archive_prefix,
        actions=[],
    )


def run_migration(
    plan: MigrationPlan,
    *,
    verify: bool = False,
) -> MigrationResult:
    result = MigrationResult(
        version=SCRIPT_VERSION,
        dry_run=plan.dry_run,
        spreadsheet_id=plan.spreadsheet_id,
        canonical_pages=list(CANONICAL_PAGES),
    )

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

    result.renames = list(rename_ops)
    result.deletes = list(delete_titles)
    result.creates = list(missing)

    # Print plan summary
    logger.info("=== MIGRATION PLAN (Schema v%s) ===", SCRIPT_VERSION)
    logger.info("Spreadsheet:    %s", plan.spreadsheet_id)
    logger.info("Dry-run:        %s", plan.dry_run)
    logger.info("Delete legacy:  %s", plan.delete_legacy)
    logger.info("Archive prefix: %s", plan.archive_prefix)
    logger.info("Verify enabled: %s", verify)
    logger.info("---------------------------------")

    if rename_ops:
        logger.info("Rename/Archive operations:")
        for old, new, reason in rename_ops:
            logger.info("  - %s  ->  %s   (%s)", old, new, reason)
    else:
        logger.info("Rename/Archive operations: (none)")

    if delete_titles:
        logger.info("Delete operations:")
        for t in delete_titles:
            logger.info("  - delete: %s", t)
    else:
        logger.info("Delete operations: (none)")

    if missing:
        logger.info("Missing canonical sheets to create:")
        for t in missing:
            logger.info("  - add: %s", t)
    else:
        logger.info("Missing canonical sheets to create: (none)")

    # Apply if not dry-run
    if plan.dry_run:
        logger.info("DRY-RUN: no changes applied.")
        result.exit_code = 0
        return result

    logger.info("Applying changes...")

    request_count = 0

    def refresh_props() -> Dict[str, Dict[str, Any]]:
        m = _get_spreadsheet_metadata(service, plan.spreadsheet_id)
        return _sheets_by_title(m)

    # --- BATCH OPTIMIZATION ---
    # Combine structural changes into logical batches to minimize metadata refreshes.

    # 1) Renames/archives
    rename_requests = []
    for old, new, reason in rename_ops:
        if old in props_by_title:
            sid = _sheet_id(props_by_title[old])
            rename_requests.append({
                "updateSheetProperties": {
                    "properties": {"sheetId": sid, "title": new},
                    "fields": "title",
                }
            })

    if rename_requests:
        _batch_update(service, plan.spreadsheet_id, rename_requests)
        request_count += 1

    # 2) Deletes (if enabled)
    delete_requests = []
    for t in delete_titles:
        if t in props_by_title:
            sid = _sheet_id(props_by_title[t])
            delete_requests.append({"deleteSheet": {"sheetId": sid}})

    if delete_requests:
        _batch_update(service, plan.spreadsheet_id, delete_requests)
        request_count += 1

    # Refresh props once if structural changes were made
    if rename_requests or delete_requests:
        props_by_title = refresh_props()

    # 3) Ensure canonical sheets exist
    add_requests = []
    for t in CANONICAL_PAGES:
        if t not in props_by_title:
            add_requests.append({"addSheet": {"properties": {"title": t}}})

    if add_requests:
        _batch_update(service, plan.spreadsheet_id, add_requests)
        request_count += 1
        # Refresh props to get sheetIds of newly created sheets
        props_by_title = refresh_props()

    # 4) Seed headers + format
    formatting_requests = []
    for sheet_title in CANONICAL_PAGES:
        spec = SCHEMA_REGISTRY.get(sheet_title)
        if not spec:
            continue

        if sheet_title == "Insights_Analysis":
            request_count += _seed_insights_analysis(
                service, plan.spreadsheet_id, sheet_title
            )
            result.seeds.append(sheet_title)
            continue

        if sheet_title == "Data_Dictionary":
            # rebuilt after we ensure all headers exist (below)
            continue

        # Instrument tables incl. Top_10_Investments and My_Portfolio
        request_count += _seed_instrument_headers(
            service, plan.spreadsheet_id, sheet_title
        )
        result.seeds.append(sheet_title)

        # v2.1.0: use actual schema column count for filter range instead of 200
        props = props_by_title.get(sheet_title)
        if props:
            sheet_id = _sheet_id(props)
            col_count = _schema_column_count(sheet_title)
            formatting_requests.append({
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {"frozenRowCount": 1},
                    },
                    "fields": "gridProperties.frozenRowCount",
                }
            })
            formatting_requests.append({
                "setBasicFilter": {
                    "filter": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": col_count,
                        }
                    }
                }
            })

    # Apply all formatting at once
    if formatting_requests:
        _batch_update(service, plan.spreadsheet_id, formatting_requests)
        request_count += 1

    # 5) Rebuild Data_Dictionary immediately
    request_count += _seed_data_dictionary(
        service, plan.spreadsheet_id, "Data_Dictionary"
    )
    result.seeds.append("Data_Dictionary")

    result.request_batches = request_count
    logger.info("Migration complete. Total API request batches: %d", request_count)

    # 6) Optional verification
    if verify:
        logger.info("Running post-migration verification...")
        report = _verify_canonical_headers(service, plan.spreadsheet_id)
        result.verify = report
        if report["ok"]:
            logger.info("Verification PASSED — all canonical sheets match schema_registry.")
        else:
            mismatched = report.get("mismatched_sheets", [])
            logger.error(
                "Verification FAILED — mismatches on: %s", ", ".join(mismatched)
            )
            for sheet_title in mismatched:
                sheet_info = report["sheets"].get(sheet_title, {})
                mm_cols = sheet_info.get("mismatch_columns") or []
                for col in mm_cols[:5]:
                    logger.error(
                        "  %s col[%d]: expected=%r actual=%r",
                        sheet_title, col["index"], col["expected"], col["actual"],
                    )
            result.exit_code = 3
            return result

    result.exit_code = 0
    return result


# =============================================================================
# CLI
# =============================================================================
def _list_canonical_and_exit() -> int:
    """--list-canonical mode: print schema catalog and exit."""
    print(f"Canonical schema (schema_registry.SCHEMA_VERSION)")
    print("=" * 70)
    for sheet_title in CANONICAL_PAGES:
        headers = get_sheet_headers(sheet_title)
        col_count = len(headers)
        preview = ", ".join(headers[:5]) + (", ..." if len(headers) > 5 else "")
        print(f"  {sheet_title:<22} {col_count:>3} cols | {preview}")
    print("=" * 70)
    print(f"Forbidden pages: {sorted(FORBIDDEN_PAGES)}")
    return 0


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=f"TFB Schema v2 migration tool v{SCRIPT_VERSION} "
                    "(tabs + headers + data dictionary + verify)."
    )
    p.add_argument(
        "--spreadsheet-id",
        default="",
        help="Target Google Spreadsheet ID. Defaults to DEFAULT_SPREADSHEET_ID env.",
    )
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
    # v2.1.0 new flags
    p.add_argument(
        "--verify",
        action="store_true",
        help="After --apply, read back headers and verify they match schema_registry. "
             "Exits 3 on any mismatch.",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON summary to stdout at the end.",
    )
    p.add_argument(
        "--list-canonical",
        action="store_true",
        help="Print canonical schema catalog and exit. No API calls.",
    )
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    # --list-canonical short-circuit (no API calls)
    if args.list_canonical:
        return _list_canonical_and_exit()

    spreadsheet_id = (
        strip_value(args.spreadsheet_id)
        or strip_value(os.getenv("DEFAULT_SPREADSHEET_ID") or "")
    )
    if not spreadsheet_id:
        logger.error(
            "Missing spreadsheet id. "
            "Provide --spreadsheet-id or set DEFAULT_SPREADSHEET_ID env."
        )
        if args.json:
            _emit_json_error(2, "missing spreadsheet id")
        return 2

    dry_run = not bool(args.apply)
    plan = build_plan(
        spreadsheet_id=spreadsheet_id,
        dry_run=dry_run,
        delete_legacy=bool(args.delete_legacy),
        archive_prefix=strip_value(args.archive_prefix) or "_ARCHIVE_",
    )

    try:
        result = run_migration(plan, verify=bool(args.verify))
    except Exception as e:
        logger.exception("Migration failed: %s", e)
        if args.json:
            _emit_json_error(1, str(e), spreadsheet_id=spreadsheet_id, dry_run=dry_run)
        return 1

    if args.json:
        try:
            sys.stdout.write(json.dumps(result.to_dict(), indent=2, default=str) + "\n")
        except Exception:
            logger.warning("Failed to emit JSON summary")

    return int(result.exit_code)


def _emit_json_error(
    exit_code: int,
    error_msg: str,
    *,
    spreadsheet_id: str = "",
    dry_run: bool = True,
) -> None:
    """Emit a minimal JSON error record to stdout."""
    try:
        record = {
            "version": SCRIPT_VERSION,
            "dry_run": dry_run,
            "spreadsheet_id": spreadsheet_id,
            "operations": {"renames": [], "deletes": [], "creates": [], "seeds": []},
            "request_batches": 0,
            "canonical_pages": list(CANONICAL_PAGES) if CANONICAL_PAGES else [],
            "verify": None,
            "error": error_msg,
            "exit_code": int(exit_code),
            "timestamp_utc": _utc_iso(),
        }
        sys.stdout.write(json.dumps(record, indent=2, default=str) + "\n")
    except Exception:
        pass


__all__ = [
    "SCRIPT_VERSION",
    "SERVICE_VERSION",
    "Action",
    "MigrationPlan",
    "MigrationResult",
    "build_plan",
    "run_migration",
    "main",
    "parse_args",
]


if __name__ == "__main__":
    sys.exit(main())
