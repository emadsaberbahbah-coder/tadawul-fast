#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
TFB REPAIR TOOL - identity corruption + learning-store junk - v1.0.0 (Build 2)
================================================================================
ONE-TIME remediation companion to run_dashboard_sync v6.24.0 (ID-FIREWALL)
and track_performance v6.20.0 (SYMBOL-GUARD). Prevention without repair is
why the 2026-07-06 poisoning survived three guard generations: the guards
stopped NEW poison while KEEP-LAST-GOOD faithfully re-published the OLD.
This tool removes the old.

WHAT IT DOES (evidence: v32 export forensics, 2026-07-14 audit):
  PHASE A - LEARNING-STORE JUNK PURGE
    Signal_History + Signal_Trends (+ Performance_Log scan): delete rows
    whose Symbol is (a) not ticker-shaped (^[A-Z0-9^][A-Z0-9.=^-]{0,14}$,
    no whitespace, no leading underscore) OR (b) a whitespace token of the
    _Portfolio_CostBasis title/status/legend cells (read LIVE from A1:A3,
    tokenized) - the exact source of the 561 junk snapshots (17 tokens x
    33 Decision_Coverage runs) traced in the audit. Deterministic by
    construction: the blocklist is derived from the very cells that leaked.
  PHASE B - IDENTITY REPAIR (Market_Leaders, Global_Markets)
    A row is CORRUPT when any of:
      (B1) P/E-identity broken: stated P/E disagrees with Price/EPS beyond
           the same tolerance and GBX/GBP unit band run_dashboard_sync's
           L3b uses (constants mirrored below with attribution);
      (B2) Name blank while the row carries a price (stub damage, e.g. the
           NMM.US blank-name row);
      (B3) Name over-assigned: the same non-blank Name on >= 3 distinct
           symbols on the page ("Microsoft Corporation" on MSFT.US, MSFT,
           DD.US and HSBA.L in the v32 export) - every carrier is cleared;
           the next guarded sync rewrites the true owner correctly.
    REPAIR = blank every cell EXCEPT Symbol; set the Warnings column (when
    present) to 'identity_repaired:v1.0.0'. The row becomes an honest stub
    that v6.24.0's FW-1 can no longer "keep-last-good" back to poison, and
    the next healthy sync repopulates it from the guarded backend.
  PHASE C - Performance_Log stale KPI ghost: clear A1:E4 (also handled by
    track_performance v6.20.0 on its next run; done here for immediacy).
  VERDICT - one [REPAIR-VERDICT v1.0.0] line appended to _Run_Log with
    per-phase counts, DRY/APPLY mode, and sample symbols.

SAFETY MODEL:
  - DRY-RUN BY DEFAULT: REPAIR_APPLY unset/0 -> full scan, full report,
    ZERO writes (except the verdict line, tagged mode=DRY). Set
    REPAIR_APPLY=1 to execute. Run DRY first, read the verdict, then APPLY.
  - Fail-open everywhere: any per-phase error is reported, never raised
    past the phase; partial progress is preserved and counted.
  - Never touches: Symbol cells, header rows, _Portfolio_CostBasis,
    My_Portfolio, Portfolio_Decision, Top_10_Investments, or any page not
    named in REPAIR_PAGES/REPAIR_STORES.

ENV:
  GOOGLE_SHEETS_CREDENTIALS / GOOGLE_SHEETS_CREDENTIALS_B64  (service acct)
  TARGET_SHEET_ID       spreadsheet id (required)
  REPAIR_APPLY          "1" to write; default DRY
  REPAIR_PAGES          default "Market_Leaders,Global_Markets"
  REPAIR_STORES         default "Signal_History,Signal_Trends,Performance_Log"
  REPAIR_MAX_DELETES    safety ceiling per store (default 2000)
================================================================================
"""
from __future__ import annotations

import base64
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

# =============================================================================
# v1.1.0 (2026-08-01, CG-7) — PHASE B4: REGISTERED-ISSUER SWEEP + KNOWN-DEFECT
# SEED REPORT. The engine LKG is process-memory only (_FUND_LKG_STORE) and the
# KLG keep-gate restores from the SHEET GRID itself — so the "last-good store"
# to sweep IS the market pages, exactly this tool's Phase-B domain. B1-B3
# catch structurally broken rows; the 2026-08-01 live poisonings (BF-A
# carrying Biofrontera, DENN.US carrying Amer Sports) are NAMED and COHERENT,
# invisible to all three. B4 consults critical_symbol_identity v1.1.0
# (identity_contradiction: the SAME registry run_dashboard_sync v6.31.0 now
# enforces at its FW seams — one registry, one truth): registered-issuer
# mismatch, venue contradiction, malformed .SR. Rows that are ALREADY clean
# stubs (blank name AND blank price) are skipped — a stub is the repair
# outcome, not a contradiction. APPLY first copies each offending row to
# _Identity_Quarantine (Timestamp/Page/Row/Symbol/Reason/Name/Price/RowJSON;
# tab auto-created) and only then blanks-to-stub with
# identity_repaired:issuer:v1.1.0 — reversible by construction, never a
# delete. Plus a report-only status of the six known-defect symbols (BK.US,
# BRK-B.US, FISV.US, 8270.SR, 4328.SR, 3001.SR) with the exact one-dispatch
# TFB_SYNC_FORCE_REFETCH_SYMBOLS re-seed instructions printed when needed.
# Kill-switch REPAIR_ISSUER_SWEEP (default ON; 0/false/off/no -> B4 reports
# skipped and v1.0.0 output is byte-identical). csi unavailable -> B4
# SKIPPED with reason, v1.0.0 phases untouched (fail-open). DRY remains the
# default for everything. Zero functions removed.
# =============================================================================
TOOL_VERSION = "1.1.0"
__version__ = TOOL_VERSION  # verify_deployment fallback attribute
TAG = f"[REPAIR-VERDICT v{TOOL_VERSION}]"

# ---- constants mirrored from run_dashboard_sync v6.24.0 (attribution) ------
_COH_REL_TOL = 0.05
_COH_FX_UNIT_LO = 50.0
_COH_FX_UNIT_HI = 200.0
_SYMBOL_SHAPE_RE = re.compile(r"^[A-Z0-9^][A-Z0-9.\-=^]{0,14}$")

_SYMBOL_ALIASES = {"symbol", "ticker", "code"}
_NAME_ALIASES = {"name", "companyname", "company", "longname", "shortname"}
_PRICE_ALIASES = {"price", "currentprice", "lastprice", "last", "close"}
_EPS_ALIASES = {"epsttm", "eps", "epsttmusd", "earningspershare"}
_PE_ALIASES = {"pettm", "pe", "peratio", "pricetoearnings"}
_WARN_ALIASES = {"warnings", "warning"}
# v1.1.0 B4: issuer/venue columns + registry import (fail-open).
_EXCH_ALIASES = {"exchange", "market"}
_CCY_ALIASES = {"currency"}
_CTRY_ALIASES = {"country"}
try:
    from scripts.critical_symbol_identity import identity_contradiction as _csi_identity
except Exception:
    try:
        from critical_symbol_identity import identity_contradiction as _csi_identity
    except Exception:
        _csi_identity = None  # B4 reports SKIPPED; v1.0.0 phases unaffected

_KNOWN_DEFECT_SYMBOLS: Tuple[str, ...] = (
    "BK.US", "BRK-B.US", "FISV.US", "8270.SR", "4328.SR", "3001.SR")


def _issuer_sweep_enabled() -> bool:
    """v1.1.0 kill-switch (default ON). 0/false/off/no -> B4 skipped and
    v1.0.0 output byte-identical."""
    return (os.getenv("REPAIR_ISSUER_SWEEP") or "1").strip().lower() \
        not in {"0", "false", "off", "no"}


def _issuer_violations(H: List[Any], grid: List[List[Any]]
                       ) -> List[Tuple[int, str, str, str, str]]:
    """v1.1.0 B4 core (pure, testable): (1-based row, sym, reason, name,
    price) for every row whose registered identity contradicts the csi
    registry. Already-stubbed rows (blank name AND blank price) are
    skipped. Empty list when csi is unavailable."""
    out: List[Tuple[int, str, str, str, str]] = []
    if _csi_identity is None:
        return out
    si = _find_col(H, _SYMBOL_ALIASES)
    ni = _find_col(H, _NAME_ALIASES)
    pi = _find_col(H, _PRICE_ALIASES)
    xi = _find_col(H, _EXCH_ALIASES)
    ci = _find_col(H, _CCY_ALIASES)
    yi = _find_col(H, _CTRY_ALIASES)
    if si < 0:
        return out
    for r in range(1, len(grid)):
        row = grid[r]
        sym = (row[si] if si < len(row) else "").strip().upper()
        if not sym:
            continue
        nm = (row[ni] if 0 <= ni < len(row) else "").strip()
        px = row[pi] if 0 <= pi < len(row) else ""
        if not nm and _blank(px):
            continue  # clean stub: the repair OUTCOME, not a contradiction
        try:
            reason = _csi_identity(
                sym, nm, px,
                row[xi] if 0 <= xi < len(row) else "",
                row[ci] if 0 <= ci < len(row) else "",
                row[yi] if 0 <= yi < len(row) else "")
        except Exception:
            reason = ""
        if reason:
            out.append((r + 1, sym, reason, nm, str(px)))
    return out


def _known_defect_status(H: List[Any], grid: List[List[Any]]) -> List[Dict[str, Any]]:
    """v1.1.0 B4 report (pure): presence/stub state of the six known-defect
    symbols on one page. needs_seed = absent OR blank stub."""
    si = _find_col(H, _SYMBOL_ALIASES)
    ni = _find_col(H, _NAME_ALIASES)
    pi = _find_col(H, _PRICE_ALIASES)
    found: Dict[str, Dict[str, Any]] = {}
    if si >= 0:
        for r in range(1, len(grid)):
            row = grid[r]
            sym = (row[si] if si < len(row) else "").strip().upper()
            if sym in _KNOWN_DEFECT_SYMBOLS and sym not in found:
                nm = (row[ni] if 0 <= ni < len(row) else "").strip()
                px = (str(row[pi]) if 0 <= pi < len(row) else "").strip()
                found[sym] = {"symbol": sym, "row": r + 1, "name": nm,
                              "price": px,
                              "blank_stub": (not nm and not px)}
    out: List[Dict[str, Any]] = []
    for sym in _KNOWN_DEFECT_SYMBOLS:
        e = found.get(sym, {"symbol": sym, "row": None, "name": "",
                            "price": "", "blank_stub": False})
        e["present"] = e["row"] is not None
        e["needs_seed"] = (not e["present"]) or bool(e["blank_stub"])
        out.append(e)
    return out


def _norm(h: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(h or "").strip().lower())


def _find_col(headers: List[Any], aliases: Set[str]) -> int:
    for i, h in enumerate(headers):
        if _norm(h) in aliases:
            return i
    return -1


def _blank(v: Any) -> bool:
    return v is None or str(v).strip() == ""


def _f(v: Any) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        x = float(v)
        return None if (x != x or x in (float("inf"), float("-inf"))) else x
    s = str(v).strip().replace(",", "").replace("%", "").replace("\u2212", "-")
    if not s or s in {"-", "--", "\u2014", "N/A", "n/a", "NA", "null", "None"}:
        return None
    try:
        x = float(s)
    except Exception:
        return None
    return None if (x != x or x in (float("inf"), float("-inf"))) else x


def _valid_symbol(s: Any) -> bool:
    t = str(s or "").strip().upper()
    if not t or t.startswith("_"):
        return False
    return bool(_SYMBOL_SHAPE_RE.match(t))


def _row_identity_broken(px: Any, eps: Any, pe: Any) -> bool:
    """Single-row P/E == Price/EPS test; mirrors L3b's per-row rules."""
    p, e, q = _f(px), _f(eps), _f(pe)
    if p is None or e is None or q is None:
        return False
    if abs(e) < 0.01 or q <= 0.0 or p <= 0.0:
        return False
    implied = p / e
    if implied <= 0.0:
        return False
    rel = abs(implied - q) / abs(q)
    if rel < _COH_REL_TOL:
        return False
    if _COH_FX_UNIT_LO <= (implied / q) <= _COH_FX_UNIT_HI:
        return False  # GBX/GBP pence convention - healthy
    return True


def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# ---------------------------------------------------------------- gspread --
def _load_creds() -> Optional[dict]:
    raw = (os.getenv("GOOGLE_SHEETS_CREDENTIALS") or "").strip()
    if not raw:
        b64 = (os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64") or "").strip()
        if b64:
            try:
                raw = base64.b64decode(b64).decode("utf-8", errors="replace").strip()
            except Exception:
                raw = ""
    if not raw:
        return None
    if not raw.startswith("{"):
        try:
            dec = base64.b64decode(raw).decode("utf-8", errors="replace").strip()
            if dec.startswith("{"):
                raw = dec
        except Exception:
            pass
    try:
        d = json.loads(raw)
        if isinstance(d, dict) and "private_key" in d:
            d["private_key"] = str(d["private_key"]).replace("\\n", "\n")
            return d
    except Exception:
        return None
    return None


def _open_sheet():
    import gspread
    from google.oauth2 import service_account

    creds_d = _load_creds()
    if not creds_d:
        print("FATAL: no service-account credentials in env", flush=True)
        sys.exit(2)
    sid = (os.getenv("TARGET_SHEET_ID") or "").strip()
    if not sid:
        print("FATAL: TARGET_SHEET_ID not set", flush=True)
        sys.exit(2)
    creds = service_account.Credentials.from_service_account_info(
        creds_d, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(sid)


def _ws(sheet, name: str):
    try:
        return sheet.worksheet(name)
    except Exception:
        return None


def _delete_rows(ws, sheet, one_based_rows: List[int], apply: bool) -> int:
    """Grouped bottom-up deleteDimension. Returns rows deleted (or would-be)."""
    if not one_based_rows:
        return 0
    rows = sorted(set(one_based_rows), reverse=True)
    if not apply:
        return len(rows)
    reqs = []
    start = rows[0]
    prev = rows[0]
    for r in rows[1:]:
        if r == prev - 1:
            prev = r
            continue
        reqs.append((prev, start))
        start = prev = r
    reqs.append((prev, start))
    body = {
        "requests": [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "ROWS",
                        "startIndex": lo - 1,
                        "endIndex": hi,
                    }
                }
            }
            for lo, hi in reqs
        ]
    }
    sheet.batch_update(body)
    return len(rows)


def _append_runlog(sheet, level: str, status: str, msg: str, details: dict) -> None:
    try:
        ws = _ws(sheet, "_Run_Log")
        if ws is None:
            return
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row(
            [ts, level, "repair_stores", "MULTI", status, msg, "", "", "",
             json.dumps(details)[:45000]],
            value_input_option="USER_ENTERED",
        )
    except Exception as e:
        print(f"run-log verdict skipped: {e}", flush=True)


# ------------------------------------------------------------------ phases --
def phase_a_junk(sheet, apply: bool, max_del: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {"stores": {}, "blocklist_size": 0}
    # deterministic blocklist from the exact leaking cells
    block: Set[str] = set()
    led = _ws(sheet, "_Portfolio_CostBasis")
    if led is not None:
        try:
            for cell in (led.get_values("A1:A3") or []):
                for tok in re.split(r"\s+", str(cell[0] if cell else "")):
                    t = tok.strip().upper()
                    if t:
                        block.add(t)
        except Exception as e:
            out["ledger_read_error"] = str(e)
    out["blocklist_size"] = len(block)

    stores = [s.strip() for s in
              (os.getenv("REPAIR_STORES")
               or "Signal_History,Signal_Trends,Performance_Log").split(",")
              if s.strip()]
    for name in stores:
        ws = _ws(sheet, name)
        rep: Dict[str, Any] = {"present": ws is not None}
        out["stores"][name] = rep
        if ws is None:
            continue
        try:
            grid = ws.get_all_values()
            hdr_r = -1
            sym_i = -1
            for r in range(min(len(grid), 8)):
                i = _find_col(grid[r], _SYMBOL_ALIASES)
                if i >= 0:
                    hdr_r, sym_i = r, i
                    break
            if hdr_r < 0:
                rep["error"] = "symbol header not found"
                continue
            doomed: List[int] = []
            samples: List[str] = []
            for r in range(hdr_r + 1, len(grid)):
                row = grid[r]
                sym = (row[sym_i] if sym_i < len(row) else "").strip().upper()
                if not sym:
                    continue
                if (not _valid_symbol(sym)) or (sym in block):
                    doomed.append(r + 1)  # 1-based
                    if len(samples) < 12:
                        samples.append(sym)
            if len(doomed) > max_del:
                rep["error"] = (f"refusing: {len(doomed)} deletions exceed "
                                f"REPAIR_MAX_DELETES={max_del}")
                rep["would_delete"] = len(doomed)
                rep["samples"] = samples
                continue
            n = _delete_rows(ws, sheet, doomed, apply)
            rep["deleted" if apply else "would_delete"] = n
            rep["samples"] = samples
        except Exception as e:
            rep["error"] = str(e)
    return out


def phase_b_identity(sheet, apply: bool) -> Dict[str, Any]:
    out: Dict[str, Any] = {"pages": {}}
    pages = [p.strip() for p in
             (os.getenv("REPAIR_PAGES") or "Market_Leaders,Global_Markets").split(",")
             if p.strip()]
    for page in pages:
        ws = _ws(sheet, page)
        rep: Dict[str, Any] = {"present": ws is not None}
        out["pages"][page] = rep
        if ws is None:
            continue
        try:
            grid = ws.get_all_values()
            if not grid:
                rep["error"] = "empty page"
                continue
            H = grid[0]
            si = _find_col(H, _SYMBOL_ALIASES)
            ni = _find_col(H, _NAME_ALIASES)
            pi = _find_col(H, _PRICE_ALIASES)
            ei = _find_col(H, _EPS_ALIASES)
            qi = _find_col(H, _PE_ALIASES)
            wi = _find_col(H, _WARN_ALIASES)
            if si < 0 or ni < 0:
                rep["error"] = "Symbol/Name column not found"
                continue
            width = len(H)
            # pass 1: name census for B3
            name_owners: Dict[str, Set[str]] = {}
            for r in range(1, len(grid)):
                row = grid[r]
                sym = (row[si] if si < len(row) else "").strip().upper()
                nm = (row[ni] if ni < len(row) else "").strip()
                if sym and nm:
                    name_owners.setdefault(nm, set()).add(sym)
            over = {nm for nm, owners in name_owners.items() if len(owners) >= 3}
            # pass 2: verdicts
            corrupt: List[Tuple[int, str, str]] = []  # (1-based row, sym, reason)
            for r in range(1, len(grid)):
                row = grid[r]
                sym = (row[si] if si < len(row) else "").strip().upper()
                if not sym:
                    continue
                nm = (row[ni] if ni < len(row) else "").strip()
                px = row[pi] if 0 <= pi < len(row) else ""
                eps = row[ei] if 0 <= ei < len(row) else ""
                pe = row[qi] if 0 <= qi < len(row) else ""
                reason = ""
                if min(pi, ei, qi) >= 0 and _row_identity_broken(px, eps, pe):
                    reason = "B1:pe_identity"
                elif not nm and not _blank(px):
                    reason = "B2:blank_name"
                elif nm in over:
                    reason = "B3:name_x%d" % len(name_owners.get(nm, set()))
                if reason:
                    corrupt.append((r + 1, sym, reason))
            rep["corrupt"] = len(corrupt)
            rep["by_reason"] = {}
            for _, _, why in corrupt:
                k = why.split(":")[0]
                rep["by_reason"][k] = rep["by_reason"].get(k, 0) + 1
            rep["samples"] = [f"{s}({w})" for _, s, w in corrupt[:15]]
            if apply and corrupt:
                updates = []
                last_col = _col_letter(width)
                for r1, sym, _why in corrupt:
                    row = ["" for _ in range(width)]
                    row[si] = sym
                    if 0 <= wi < width:
                        row[wi] = f"identity_repaired:v{TOOL_VERSION}"
                    updates.append({
                        "range": f"A{r1}:{last_col}{r1}",
                        "values": [row],
                    })
                CH = 200
                for i in range(0, len(updates), CH):
                    ws.batch_update(updates[i:i + CH],
                                    value_input_option="USER_ENTERED")
                rep["repaired"] = len(corrupt)
        except Exception as e:
            rep["error"] = str(e)
    return out


def phase_b4_issuer(sheet, apply: bool) -> Dict[str, Any]:
    """v1.1.0 B4: registered-issuer sweep + known-defect seed report.
    Mirrors phase_b_identity write discipline exactly; APPLY additionally
    copies every offending row to _Identity_Quarantine BEFORE the stub
    write (reversible, never a delete)."""
    out: Dict[str, Any] = {"pages": {}}
    if not _issuer_sweep_enabled():
        out["skipped"] = "REPAIR_ISSUER_SWEEP=0"
        return out
    if _csi_identity is None:
        out["skipped"] = "critical_symbol_identity unavailable"
        return out
    pages = [p.strip() for p in
             (os.getenv("REPAIR_PAGES") or "Market_Leaders,Global_Markets").split(",")
             if p.strip()]
    seed_needed: List[str] = []
    for page in pages:
        ws = _ws(sheet, page)
        rep: Dict[str, Any] = {"present": ws is not None}
        out["pages"][page] = rep
        if ws is None:
            continue
        try:
            grid = ws.get_all_values()
            if not grid:
                rep["error"] = "empty page"
                continue
            H = grid[0]
            si = _find_col(H, _SYMBOL_ALIASES)
            wi = _find_col(H, _WARN_ALIASES)
            if si < 0:
                rep["error"] = "Symbol column not found"
                continue
            width = len(H)
            viols = _issuer_violations(H, grid)
            rep["issuer_corrupt"] = len(viols)
            rep["by_reason"] = {}
            for _, _, why, _, _ in viols:
                k = why.split(" ")[0].split("(")[0][:24]
                rep["by_reason"][k] = rep["by_reason"].get(k, 0) + 1
            rep["samples"] = [f"{sym}[{why[:40]}]" for _, sym, why, _, _ in viols[:15]]
            kd = _known_defect_status(H, grid)
            rep["known_defects"] = kd
            seed_needed.extend(f"{page}:{e['symbol']}" for e in kd if e["needs_seed"])
            if apply and viols:
                # (1) quarantine copies FIRST — reversible record
                try:
                    try:
                        qws = sheet.worksheet("_Identity_Quarantine")
                    except Exception:
                        qws = sheet.add_worksheet(title="_Identity_Quarantine",
                                                  rows=2000, cols=10)
                        qws.append_row(["Timestamp", "Page", "Row", "Symbol",
                                        "Reason", "Name", "Price", "RowJSON"],
                                       value_input_option="RAW")
                    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
                    qrows = []
                    for r1, sym, why, nm, px in viols:
                        src = grid[r1 - 1] if 0 <= r1 - 1 < len(grid) else []
                        qrows.append([stamp, page, r1, sym, why, nm, px,
                                      json.dumps(src)[:9000]])
                    CH = 100
                    for i in range(0, len(qrows), CH):
                        qws.append_rows(qrows[i:i + CH],
                                        value_input_option="RAW")
                    rep["quarantined"] = len(qrows)
                except Exception as qe:
                    rep["quarantine_error"] = str(qe)[:120]
                # (2) blank-to-stub, identical discipline to phase B
                updates = []
                last_col = _col_letter(width)
                for r1, sym, _why, _nm, _px in viols:
                    row = ["" for _ in range(width)]
                    row[si] = sym
                    if 0 <= wi < width:
                        row[wi] = f"identity_repaired:issuer:v{TOOL_VERSION}"
                    updates.append({"range": f"A{r1}:{last_col}{r1}",
                                    "values": [row]})
                CH = 200
                for i in range(0, len(updates), CH):
                    ws.batch_update(updates[i:i + CH],
                                    value_input_option="USER_ENTERED")
                rep["repaired"] = len(viols)
        except Exception as e:
            rep["error"] = str(e)
    out["seed_needed"] = seed_needed
    if seed_needed:
        out["seed_instructions"] = (
            "ONE-DISPATCH RE-SEED: add TFB_SYNC_FORCE_REFETCH_SYMBOLS="
            + ",".join(sorted({t.split(":", 1)[1] for t in seed_needed}))
            + " to daily_sync.yml env, run ONE workflow dispatch, verify the "
              "[FORCE-REFETCH] lines in _Run_Log, then REMOVE the flag block. "
              "Never let the flag survive a second run.")
    return out


def phase_c_ghost_panel(sheet, apply: bool) -> Dict[str, Any]:
    rep: Dict[str, Any] = {}
    ws = _ws(sheet, "Performance_Log")
    rep["present"] = ws is not None
    if ws is None:
        return rep
    try:
        vals = ws.get_values("A1:E4") or []
        has = any(any(str(c).strip() for c in row) for row in vals)
        rep["ghost_present"] = has
        if has and apply:
            ws.batch_clear(["A1:E4"])
            rep["cleared"] = True
    except Exception as e:
        rep["error"] = str(e)
    return rep


def main() -> int:
    apply = (os.getenv("REPAIR_APPLY") or "0").strip().lower() in {"1", "true", "yes", "on"}
    max_del = max(1, int(os.getenv("REPAIR_MAX_DELETES") or "2000"))
    mode = "APPLY" if apply else "DRY"
    print(f"{TAG} starting - mode={mode}", flush=True)
    sheet = _open_sheet()

    a = phase_a_junk(sheet, apply, max_del)
    b = phase_b_identity(sheet, apply)
    b4 = phase_b4_issuer(sheet, apply)          # v1.1.0 CG-7
    c = phase_c_ghost_panel(sheet, apply)

    summary = {"mode": mode, "phase_a": a, "phase_b": b, "phase_b4": b4,
               "phase_c": c, "version": TOOL_VERSION}
    print(json.dumps(summary, indent=2)[:20000], flush=True)

    a_n = sum(int(v.get("deleted", v.get("would_delete", 0)) or 0)
              for v in a.get("stores", {}).values() if isinstance(v, dict))
    b_n = sum(int(v.get("corrupt", 0) or 0)
              for v in b.get("pages", {}).values() if isinstance(v, dict))
    b4_n = sum(int(v.get("issuer_corrupt", 0) or 0)
               for v in b4.get("pages", {}).values() if isinstance(v, dict))
    seed_n = len(b4.get("seed_needed", []) or [])
    errs = []
    for grp in (a.get("stores", {}), b.get("pages", {}), b4.get("pages", {})):
        for k, v in grp.items():
            if isinstance(v, dict) and v.get("error"):
                errs.append(f"{k}:{str(v['error'])[:60]}")
    if c.get("error"):
        errs.append(f"Performance_Log:{str(c['error'])[:60]}")

    verb = "repaired/deleted" if apply else "WOULD repair/delete"
    msg = (f"{TAG} mode={mode} | junk_rows {verb}: {a_n} | "
           f"identity_corrupt rows: {b_n} | issuer_corrupt rows: {b4_n} | "
           f"known_defects_needing_seed: {seed_n} | ghost_panel: "
           f"{c.get('cleared', c.get('ghost_present', False))}"
           f"{' | ERRORS: ' + '; '.join(errs[:4]) if errs else ''}")
    if b4.get("seed_instructions"):
        print(TAG + " " + b4["seed_instructions"], flush=True)
    print(msg, flush=True)
    _append_runlog(sheet, "WARNING" if errs else "INFO",
                   "OK" if not errs else "PARTIAL", msg, summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
