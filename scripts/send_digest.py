#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/send_digest.py  —  TFB Post-Sync Digest (EMAIL ONLY, v1.1.0)
=====================================================================

WHAT THIS IS
------------
A standalone, schedule-driven notifier that emails the operator the single
best BUY, best SELL, and best SWAP currently visible on the dashboard. It is
DECOUPLED from the sync: it READS the latest already-synced sheet (no provider
fetch, no backend load) and sends an email. It is meant to be fired by its own
GitHub Actions cron at 11:00 and 17:30 Riyadh (08:00 / 14:30 UTC).

WHY IT READS THE SHEET (not the backend)
----------------------------------------
The locked design is "summarize the latest sync." Reading the rendered sheet
via the same read_range path the sync/validator use means the digest reflects
exactly what the operator sees, adds zero load on the rate-limited providers,
and keeps working even when the backend enrichment route is degraded.

WHY COLUMNS ARE RESOLVED BY HEADER, NOT HARDCODED
-------------------------------------------------
The /mnt/project schema snapshot is stale relative to the live registry
(2.15.0): it lacks final_action / investable / block_reason, which the live
validator clearly checks. Hardcoding column indices would silently break on the
next schema bump. Instead we read row 1 (headers) from the live sheet and locate
the handful of fields we need by normalized alias match. Whatever the live label
is ("Final Action", "Block Reason", "Investable", ...), the resolver finds it.

WHY BEST-BUY EXCLUDES BLOCKED ROWS
----------------------------------
The dashboard validator has repeatedly failed on rows whose RAW recommendation
is BUY while the row also carries a block_reason (the row is not actually
investable). Emailing such a row as "best BUY" would be wrong. So best-BUY
requires: action is BUY-family AND (investable is truthy, if that column exists)
AND (block_reason empty, if that column exists) AND a usable price. This is the
final-decision view, not the raw signal.

WHY v1.1.0 — LEDGER-AWARE SELL + HELD-EXCLUSION + FRESHNESS (2026-07-11)
------------------------------------------------------------------------
Evidence: the 2026-07-10 13:26 digest. (D1) Best SELL fired on 1211.SR — a
STALE My_Portfolio row for a position the Investment Ledger had already
marked Inactive (sold). The page-level source is fixed by 10_My_Portfolio
v1.5.9, but the digest reads pages independently and needs its own gate:
holdings rows whose symbol is Inactive in _Portfolio_CostBasis (or whose
Qty is <= 0) are no longer SELL/SWAP candidates. Fail-open: if the ledger
page is unreadable the filter contributes nothing (v1.0.0 behavior), and
DIGEST_LEDGER_FILTER=0 disables it outright. (D2) Best BUY had NO held
awareness at all: it could recommend a symbol already in the book (NMM.US
was INVEST on Market_Leaders while held) and did recommend BBDO.US while
BBD.US — the same issuer, Banco Bradesco S.A., in a different ADR share
class — was held. Exact-held symbols are now excluded from BUY candidacy
(DIGEST_EXCLUDE_HELD=0 restores v1.0.0); a pick whose NAME matches a held
issuer under a different symbol is kept but explicitly flagged '⚠ same
issuer as held X — different share class'; and a SWAP whose sell and buy
legs share one issuer (SELL BBD -> BUY BBDO churn) is suppressed. (D3)
Zero freshness disclosure: nothing in the email said the quoted rows were
days old (Global_Markets carried 07-08 vintages on 07-10). Each pick now
shows its row's 'Last Updated (Riyadh)' stamp, stamps older than
DIGEST_STALE_HOURS (default 24) are marked STALE, and a header notice
appears when any shown pick is stale. All additions fail-safe: a missing
column/timestamp simply renders nothing.

SAFETY / OPERABILITY
--------------------
- Kill switch: DIGEST_ENABLE=0 -> exits 0 without sending.
- --dry-run prints the composed email and never connects to SMTP (safe to test).
- If the latest sync produced no qualifying picks (e.g. providers still
  throttled), the digest SENDS a short "no qualifying picks" note so the silence
  is VISIBLE — unless DIGEST_SKIP_IF_EMPTY=1, which suppresses the send instead.
- No secret is ever logged. SMTP settings come entirely from env.
- Designed to run NON-BLOCKING in CI (a digest hiccup must never fail anything).

ENV (all read at runtime; SMTP_* belong in GitHub Actions Secrets, NOT Render):
  TARGET_SHEET_ID | DEFAULT_SPREADSHEET_ID | TFB_SHEET_ID   (spreadsheet id)
  SMTP_HOST  SMTP_PORT  SMTP_USER  SMTP_PASSWORD            (transport)
  SMTP_USE_TLS=1 (STARTTLS; port 465 auto-uses SSL)         (transport)
  DIGEST_FROM   DIGEST_TO (comma-separated)                 (addresses)
  DIGEST_CANDIDATE_PAGES="Market_Leaders"                   (BUY universe)
  DIGEST_HOLDINGS_PAGE="My_Portfolio"                       (SELL/SWAP universe)
  DIGEST_ENABLE=1   DIGEST_SKIP_IF_EMPTY=0                  (gates)
  DIGEST_LEDGER_PAGE="_Portfolio_CostBasis"                 (v1.1.0 ledger tab)
  DIGEST_LEDGER_FILTER=1   DIGEST_EXCLUDE_HELD=1            (v1.1.0 gates)
  DIGEST_STALE_HOURS=24                                     (v1.1.0 freshness)
"""

from __future__ import annotations

import argparse
import os
import re
import smtplib
import ssl
import sys
import traceback
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional, Tuple

__version__ = "1.1.0"

RIYADH_TZ = timezone(timedelta(hours=3))  # Saudi Arabia is fixed UTC+3 (no DST).


# --------------------------------------------------------------------------- #
# env helpers
# --------------------------------------------------------------------------- #
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if (v is not None and str(v).strip() != "") else default


def _env_any(names: List[str], default: str = "") -> str:
    for n in names:
        v = _env(n)
        if v:
            return v
    return default


def _env_bool(name: str, default: bool) -> bool:
    v = _env(name, "").strip().lower()
    if v == "":
        return default
    return v in ("1", "true", "yes", "on", "y")


def _log(msg: str) -> None:
    print(f"[send_digest v{__version__}] {msg}", flush=True)


# --------------------------------------------------------------------------- #
# sheet reader — prefer the project's read_range; fall back to a local reader
# so the script is not hostage to one import path.
# --------------------------------------------------------------------------- #
def _get_read_range():
    for path in (
        "integrations.google_sheets_service",
        "google_sheets_service",
        "core.sheets.google_sheets_service",
        "services.google_sheets_service",
    ):
        try:
            mod = __import__(path, fromlist=["read_range"])
            fn = getattr(mod, "read_range", None)
            if callable(fn):
                _log(f"using read_range from {path}")
                return fn
        except Exception:
            continue
    _log("project read_range not importable; using local Sheets reader")
    return _local_read_range


def _local_creds_info() -> Dict[str, Any]:
    import base64
    import json

    raw = _env_any(["GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS"])
    if not raw:
        b64 = _env_any(["GOOGLE_SHEETS_CREDENTIALS_B64", "GOOGLE_CREDENTIALS_B64"])
        if b64:
            raw = base64.b64decode(b64).decode("utf-8", "replace")
    if not raw:
        path = _env_any(["GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_SHEETS_CREDENTIALS_FILE"])
        if path and os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as fh:
                raw = fh.read()
    if not raw:
        raise RuntimeError(
            "No Google credentials found (GOOGLE_SHEETS_CREDENTIALS / _B64 / "
            "GOOGLE_APPLICATION_CREDENTIALS)."
        )
    return json.loads(raw)


def _local_read_range(spreadsheet_id: str, range_name: str) -> List[List[Any]]:
    from google.oauth2.service_account import Credentials  # type: ignore
    from googleapiclient.discovery import build  # type: ignore

    creds = Credentials.from_service_account_info(
        _local_creds_info(),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name, majorDimension="ROWS")
        .execute()
    )
    return result.get("values", []) or []


# --------------------------------------------------------------------------- #
# header resolution + value parsing
# --------------------------------------------------------------------------- #
def _norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(s or "").strip().lower()).strip()


def _resolve_col(headers: List[str], aliases: List[str]) -> int:
    """Return the index of the first header matching an alias.

    Two passes: exact normalized equality first (precise), then substring
    containment (tolerant). Returns -1 if nothing matches.
    """
    norm = [_norm(h) for h in headers]
    for a in aliases:
        na = _norm(a)
        for i, h in enumerate(norm):
            if h == na:
                return i
    for a in aliases:
        na = _norm(a)
        for i, h in enumerate(norm):
            if na and na in h:
                return i
    return -1


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip().replace(",", "").replace("%", "").replace("SAR", "").replace("$", "")
    s = s.strip()
    if s == "" or s.lower() in ("n/a", "na", "none", "-", "—"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _is_truthy(v: Any) -> bool:
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "investable", "ok", "pass", "eligible")


def _is_blank(v: Any) -> bool:
    s = str(v or "").strip().lower()
    return s in ("", "-", "—", "none", "n/a", "na", "null", "0", "false", "no")


# The dashboard's FINAL ACTION verdict vocabulary (authoritative investability
# decision): INVEST / WATCH / DO_NOT_INVEST. INVEST already means "investable +
# not blocked", so it needs no extra gate filtering. The raw-recommendation
# words are kept as a FALLBACK for any page/version that exposes BUY/SELL text.
INVEST_ACTION = "INVEST"
DO_NOT_INVEST_ACTION = "DO_NOT_INVEST"
RAW_BUY_ACTIONS = {"BUY", "STRONG BUY", "STRONGBUY", "ACCUMULATE", "ACCUM", "ADD"}
RAW_SELL_ACTIONS = {"SELL", "STRONG SELL", "REDUCE", "TRIM", "AVOID", "EXIT"}


# --------------------------------------------------------------------------- #
# page loading into resolved-row dicts
# --------------------------------------------------------------------------- #
# field -> ordered aliases (label candidates across schema versions)
FIELD_ALIASES: Dict[str, List[str]] = {
    "symbol": ["symbol", "ticker", "code"],
    "name": ["company name", "company", "name", "security name", "instrument"],
    "action": ["final action", "final_action", "decision", "recommendation", "action", "signal", "call"],
    "investable": ["investable", "is investable", "investability"],
    "block_reason": ["block reason", "block_reason", "blocked reason", "blocked", "reason blocked"],
    "score": ["overall score", "opportunity score", "composite score", "total score", "score"],
    "price": ["current price", "last price", "price", "market price"],
    "roi": ["expected roi 12m", "expected roi 3m", "expected roi 1m", "expected roi", "roi 12m", "roi"],
    "reason": ["recommendation reason", "reason", "rationale", "note", "comment"],
    "qty": ["qty", "quantity", "shares", "units", "position qty"],
    # v1.1.0 (D3): row freshness stamp for the per-pick 'data as of' line.
    "updated": ["last updated (riyadh)", "last updated (utc)", "last updated"],
}


def _load_page(read_range, sheet_id: str, page: str) -> List[Dict[str, Any]]:
    """Read one tab and return list of dicts with the resolved fields we need."""
    try:
        values = read_range(sheet_id, f"'{page}'!A1:ZZ5000")
    except Exception as exc:  # noqa: BLE001
        _log(f"WARN could not read '{page}': {exc}")
        return []
    if not values or len(values) < 2:
        _log(f"WARN '{page}' has no data rows")
        return []

    headers = [str(x) for x in values[0]]
    idx = {f: _resolve_col(headers, al) for f, al in FIELD_ALIASES.items()}
    if idx.get("symbol", -1) < 0:
        idx["symbol"] = 0  # symbol is always the first column by schema contract
    if idx.get("action", -1) < 0:
        _log(f"WARN '{page}': no action/recommendation column resolved; skipping page")
        return []

    # Auditability: show which live header each needed field resolved to, so the
    # operator can confirm the digest latched onto final_action/block_reason/etc.
    resolved = []
    for f in FIELD_ALIASES:
        i = idx.get(f, -1)
        resolved.append(f"{f}->'{headers[i]}'[{i}]" if 0 <= i < len(headers) else f"{f}->MISSING")
    _log(f"'{page}' columns: " + ", ".join(resolved))

    rows: List[Dict[str, Any]] = []
    for raw in values[1:]:
        def cell(field: str) -> Any:
            i = idx.get(field, -1)
            return raw[i] if (0 <= i < len(raw)) else ""

        sym = str(cell("symbol")).strip()
        if not sym:
            continue
        rows.append(
            {
                "page": page,
                "symbol": sym,
                "name": str(cell("name")).strip(),
                "action_raw": str(cell("action")).strip(),
                "action": str(cell("action")).strip().upper(),
                "investable_cell": cell("investable"),
                "block_cell": cell("block_reason"),
                "has_investable_col": idx.get("investable", -1) >= 0,
                "has_block_col": idx.get("block_reason", -1) >= 0,
                "score": _to_float(cell("score")),
                "price": _to_float(cell("price")),
                "roi": _to_float(cell("roi")),
                "reason": str(cell("reason")).strip(),
                "qty": _to_float(cell("qty")),
                "updated": str(cell("updated")).strip(),  # v1.1.0 (D3)
            }
        )
    _log(f"'{page}': {len(rows)} symbol rows")
    return rows


def _is_investable(r: Dict[str, Any]) -> bool:
    if r["has_investable_col"] and not _is_truthy(r["investable_cell"]):
        return False
    if r["has_block_col"] and not _is_blank(r["block_cell"]):
        return False
    return True


# --------------------------------------------------------------------------- #
# v1.1.0 helpers — ledger gate (D1), held/issuer awareness (D2), freshness (D3)
# --------------------------------------------------------------------------- #
_LEDGER_INACTIVE_TOKENS = {"inactive", "closed", "sold", "exited"}


def _issuer_key(name: Any) -> str:
    """v1.1.0 (D2): normalized issuer-name key — lowercase alphanumerics only,
    so 'Banco Bradesco S.A.' and 'BANCO BRADESCO SA' collide."""
    return re.sub(r"[^a-z0-9]+", "", str(name or "").lower())


def _stale_hours() -> float:
    """v1.1.0 (D3): DIGEST_STALE_HOURS, default 24, floor 1."""
    try:
        return max(1.0, float(_env("DIGEST_STALE_HOURS", "24") or "24"))
    except Exception:
        return 24.0


def _parse_ts(raw: Any) -> Optional[datetime]:
    """v1.1.0 (D3): parse a sheet 'Last Updated (Riyadh)' cell (naive
    'YYYY-MM-DD HH:MM[:SS]' or date-only). None when unparseable."""
    t = str(raw or "").strip()[:19]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(t[: len(datetime.now().strftime(fmt))], fmt)
        except Exception:
            continue
    return None


def _is_stale(r: Optional[Dict[str, Any]]) -> bool:
    """v1.1.0 (D3): True iff the row carries a parseable stamp older than
    DIGEST_STALE_HOURS (Riyadh clock). Unstamped rows are never 'stale' —
    they simply render no data line."""
    if not r:
        return False
    ts = _parse_ts(r.get("updated"))
    if ts is None:
        return False
    now_r = datetime.now(RIYADH_TZ).replace(tzinfo=None)
    return (now_r - ts).total_seconds() / 3600.0 >= _stale_hours()


def _load_inactive_symbols(read_range, sheet_id: str) -> set:
    """v1.1.0 (D1): symbols the Investment Ledger marks Inactive/closed.

    Reads DIGEST_LEDGER_PAGE (default '_Portfolio_CostBasis', the 21_Portfolio_
    Ledger v2 layout: banner/status/hint rows precede the header row), locates
    the first row within the top 15 that carries BOTH a Symbol-like and a
    Status-like header, and collects every symbol whose Status normalizes into
    _LEDGER_INACTIVE_TOKENS. FAIL-OPEN by design: any read/layout problem
    returns an empty set (v1.0.0 behavior) — this gate may only ever REMOVE a
    known-closed position from SELL candidacy, never block the digest."""
    if not _env_bool("DIGEST_LEDGER_FILTER", True):
        _log("ledger filter disabled (DIGEST_LEDGER_FILTER=0)")
        return set()
    page = _env("DIGEST_LEDGER_PAGE", "_Portfolio_CostBasis") or "_Portfolio_CostBasis"
    try:
        values = read_range(sheet_id, f"'{page}'!A1:M200")
    except Exception as exc:  # noqa: BLE001
        _log(f"WARN ledger '{page}' unreadable ({exc}) — SELL gate inactive")
        return set()
    if not values:
        return set()
    sym_i = st_i = hdr_r = -1
    for r_i, row in enumerate(values[:15]):
        cells = [_norm(c) for c in (row or [])]
        try:
            si = next(i for i, c in enumerate(cells) if c in ("symbol", "ticker", "code"))
            ti = next(i for i, c in enumerate(cells) if c in ("status", "state", "lot status"))
        except StopIteration:
            continue
        sym_i, st_i, hdr_r = si, ti, r_i
        break
    if sym_i < 0:
        _log(f"WARN ledger '{page}': Symbol/Status header row not found — SELL gate inactive")
        return set()
    out: set = set()
    for row in values[hdr_r + 1:]:
        if not row or sym_i >= len(row):
            continue
        sym = str(row[sym_i] or "").strip().upper()
        if not sym:
            continue
        status = _norm(row[st_i]) if st_i < len(row) else ""
        if status in _LEDGER_INACTIVE_TOKENS:
            out.add(sym)
    _log(f"ledger '{page}': {len(out)} inactive symbol(s)" + (f" ({', '.join(sorted(out))})" if out else ""))
    return out


def _filter_holdings(rows: List[Dict[str, Any]], inactive: set) -> List[Dict[str, Any]]:
    """v1.1.0 (D1): the SELL/SWAP universe = rows that are (a) not ledger-
    Inactive and (b) not zero/negative Qty when the Qty column resolved.
    Rows without a parseable Qty pass (blank cells must not hide a holding)."""
    kept: List[Dict[str, Any]] = []
    dropped: List[str] = []
    for r in rows:
        sym = r["symbol"].upper()
        if sym in inactive:
            dropped.append(f"{sym}(ledger-inactive)")
            continue
        q = r.get("qty")
        if q is not None and q <= 0:
            dropped.append(f"{sym}(qty<=0)")
            continue
        kept.append(r)
    if dropped:
        _log("holdings excluded from SELL/SWAP: " + ", ".join(dropped))
    return kept


def _held_issuers(holdings: List[Dict[str, Any]]) -> Tuple[set, Dict[str, List[str]]]:
    """v1.1.0 (D2): (held symbol set, issuer-name key -> sorted held symbols)
    from the FILTERED holdings (a ledger-retired zombie is not 'held')."""
    syms: set = set()
    names: Dict[str, List[str]] = {}
    for r in holdings:
        sym = r["symbol"].upper()
        syms.add(sym)
        k = _issuer_key(r.get("name"))
        if k:
            names.setdefault(k, []).append(sym)
    return syms, {k: sorted(set(v)) for k, v in names.items()}


def _annotate_sibling(pick: Optional[Dict[str, Any]],
                      held_syms: set, held_names: Dict[str, List[str]]) -> None:
    """v1.1.0 (D2): mark a BUY pick whose ISSUER is already held under a
    different symbol (share-class sibling). Selection is unaffected — the
    flag is disclosure, not exclusion."""
    if not pick:
        return
    sym = pick["symbol"].upper()
    k = _issuer_key(pick.get("name"))
    if not k or sym in held_syms:
        return
    sibs = [x for x in held_names.get(k, []) if x != sym]
    if sibs:
        pick["sibling"] = ", ".join(sibs)


# --------------------------------------------------------------------------- #
# selection
# --------------------------------------------------------------------------- #
def _pick_best_buy(candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    def _score(r: Dict[str, Any]) -> float:
        return r["score"] if r["score"] is not None else -1e9

    # Primary: Final Action == INVEST (already the investable verdict).
    invest = [
        r for r in candidates
        if r["action"] == INVEST_ACTION and (r["price"] is not None and r["price"] > 0)
    ]
    if invest:
        return max(invest, key=_score)
    # Fallback: raw BUY-family text, but only if investable and not blocked.
    raw = [
        r for r in candidates
        if r["action"] in RAW_BUY_ACTIONS
        and _is_investable(r)
        and (r["price"] is not None and r["price"] > 0)
    ]
    return max(raw, key=_score) if raw else None


def _pick_best_sell(holdings: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    def _score(r: Dict[str, Any]) -> float:
        return r["score"] if r["score"] is not None else 1e9

    # You can only sell what you own -> exit signals come from holdings.
    # Primary: Final Action == DO_NOT_INVEST; fallback: raw SELL-family text.
    dni = [r for r in holdings if r["action"] == DO_NOT_INVEST_ACTION]
    if dni:
        return min(dni, key=_score)   # worst score = most urgent exit
    raw = [r for r in holdings if r["action"] in RAW_SELL_ACTIONS]
    return min(raw, key=_score) if raw else None


def _pick_best_swap(
    best_sell: Optional[Dict[str, Any]], best_buy: Optional[Dict[str, Any]]
) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    if not best_sell or not best_buy:
        return None
    sb = best_sell["score"] if best_sell["score"] is not None else None
    bb = best_buy["score"] if best_buy["score"] is not None else None
    # Only propose a swap when the buy is clearly better than the holding being
    # exited; swapping into something not materially stronger is just noise.
    if sb is not None and bb is not None and bb <= sb:
        return None
    # v1.1.0 (D2): a swap whose two legs are the SAME issuer (SELL BBD.US ->
    # BUY BBDO.US) is share-class churn, not a portfolio improvement.
    ks, kb = _issuer_key(best_sell.get("name")), _issuer_key(best_buy.get("name"))
    if ks and ks == kb:
        _log(f"swap suppressed: {best_sell['symbol']} -> {best_buy['symbol']} share one issuer")
        return None
    return (best_sell, best_buy)


# --------------------------------------------------------------------------- #
# email composition
# --------------------------------------------------------------------------- #
def _fmt_num(v: Optional[float], suffix: str = "") -> str:
    if v is None:
        return "—"
    return f"{v:,.2f}{suffix}"


def _line(label: str, r: Optional[Dict[str, Any]]) -> str:
    if not r:
        return f"{label}: none in the latest sync"
    bits = [r["symbol"]]
    if r["name"]:
        bits.append(f"({r['name']})")
    bits.append(f"— {r['action_raw'] or r['action']}")
    extra = []
    if r["price"] is not None:
        extra.append(f"price {_fmt_num(r['price'])}")
    if r["score"] is not None:
        extra.append(f"score {_fmt_num(r['score'])}")
    if r["roi"] is not None:
        extra.append(f"ROI {_fmt_num(r['roi'], '%')}")
    if r.get("updated"):  # v1.1.0 (D3)
        extra.append(f"data {str(r['updated'])[:16]}" + (" STALE" if _is_stale(r) else ""))
    if extra:
        bits.append("[" + ", ".join(extra) + "]")
    s = f"{label}: " + " ".join(bits)
    if r.get("sibling"):  # v1.1.0 (D2)
        s += f"\n    \u26a0 same issuer as held {r['sibling']} — different share class"
    if r["reason"]:
        s += f"\n    {r['reason']}"
    return s


def _compose(
    session: str,
    best_buy: Optional[Dict[str, Any]],
    best_sell: Optional[Dict[str, Any]],
    swap: Optional[Tuple[Dict[str, Any], Dict[str, Any]]],
) -> Tuple[str, str, str]:
    now = datetime.now(RIYADH_TZ).strftime("%Y-%m-%d %H:%M")
    any_pick = bool(best_buy or best_sell)

    if any_pick:
        subj_bits = []
        if best_buy:
            subj_bits.append(f"BUY {best_buy['symbol']}")
        if best_sell:
            subj_bits.append(f"SELL {best_sell['symbol']}")
        subject = f"TFB Digest — {session} — " + " | ".join(subj_bits)
    else:
        subject = f"TFB Digest — {session} — no qualifying picks"

    swap_line = "Swap: none suggested"
    if swap:
        s_out, s_in = swap
        swap_line = (
            f"Swap: SELL {s_out['symbol']} "
            f"(score {_fmt_num(s_out['score'])}) → BUY {s_in['symbol']} "
            f"(score {_fmt_num(s_in['score'])})"
        )

    # v1.1.0 (D3): one visible notice when any shown pick rides stale data.
    stale_note = ""
    if _is_stale(best_buy) or _is_stale(best_sell):
        stale_note = (f"\u26a0 DATA VINTAGE: at least one pick below is older than "
                      f"{_stale_hours():.0f}h — prices reflect the last completed sync.")

    text = "\n".join(
        [
            f"TFB Decision Digest — {session}",
            f"Generated {now} Riyadh (from the latest synced dashboard).",
            *([stale_note] if stale_note else []),
            "",
            _line("Best BUY ", best_buy),
            _line("Best SELL", best_sell),
            swap_line,
            "",
            "Source: latest sheet sync. Recommendations are model output, not",
            "investment advice. Verify on the dashboard before acting.",
            f"send_digest v{__version__}",
        ]
    )

    def _row_html(label: str, r: Optional[Dict[str, Any]]) -> str:
        if not r:
            return f"<tr><td><b>{label}</b></td><td colspan='4'>none in the latest sync</td></tr>"
        return (
            "<tr>"
            f"<td><b>{label}</b></td>"
            f"<td>{r['symbol']}{(' — ' + r['name']) if r['name'] else ''}"
            + (f"<div style='color:#B26B00;font-size:11px'>&#9888; same issuer as held {r['sibling']}"
               f" — different share class</div>" if r.get("sibling") else "")
            + (f"<div style='color:#888;font-size:11px'>data {str(r['updated'])[:16]}"
               f"{' <b>STALE</b>' if _is_stale(r) else ''}</div>" if r.get("updated") else "")
            + "</td>"
            f"<td>{r['action_raw'] or r['action']}</td>"
            f"<td>{_fmt_num(r['price'])}</td>"
            f"<td>{_fmt_num(r['score'])}{(' / ROI ' + _fmt_num(r['roi'], '%')) if r['roi'] is not None else ''}</td>"
            "</tr>"
        )

    html = (
        f"<div style='font-family:Arial,Helvetica,sans-serif;font-size:14px'>"
        f"<h2 style='margin:0 0 4px'>TFB Decision Digest</h2>"
        f"<div style='color:#555'>{session} · generated {now} Riyadh · latest synced dashboard</div>"
        + (f"<div style='background:#FCF4E3;border:1px solid #E8D5A8;padding:8px 10px;"
           f"margin-top:8px;color:#7A5A17;font-size:12px'>{stale_note}</div>" if stale_note else "")
        + f"<table cellpadding='6' cellspacing='0' "
        f"style='border-collapse:collapse;margin-top:12px' border='1'>"
        f"<tr style='background:#f2f2f2'><th>Pick</th><th>Symbol</th>"
        f"<th>Action</th><th>Price</th><th>Score / ROI</th></tr>"
        f"{_row_html('Best BUY', best_buy)}"
        f"{_row_html('Best SELL', best_sell)}"
        f"</table>"
        f"<p style='margin-top:10px'>{swap_line}</p>"
        f"<p style='color:#888;font-size:12px;margin-top:16px'>"
        f"Source: latest sheet sync. Model output, not investment advice — "
        f"verify on the dashboard before acting. send_digest v{__version__}</p>"
        f"</div>"
    )
    return subject, text, html


# --------------------------------------------------------------------------- #
# SMTP send
# --------------------------------------------------------------------------- #
def _send_email(subject: str, text: str, html: str, dry_run: bool) -> int:
    sender = _env("DIGEST_FROM") or _env("SMTP_USER")
    recips = [a.strip() for a in _env("DIGEST_TO").split(",") if a.strip()]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender or "tfb-digest@localhost"
    msg["To"] = ", ".join(recips) if recips else ""
    msg.attach(MIMEText(text, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))

    if dry_run:
        _log("DRY RUN — email not sent. Preview below:")
        print("-" * 70)
        print(f"Subject: {subject}")
        print(f"From: {msg['From']}")
        print(f"To: {msg['To']}")
        print(text)
        print("-" * 70)
        return 0

    host = _env("SMTP_HOST")
    port = int(_env("SMTP_PORT", "587") or "587")
    user = _env("SMTP_USER")
    pwd = _env("SMTP_PASSWORD")
    if not host:
        _log("ERROR SMTP_HOST not set — cannot send.")
        return 2
    if not recips:
        _log("ERROR DIGEST_TO not set — no recipients.")
        return 2

    ctx = ssl.create_default_context()
    try:
        if port == 465:
            with smtplib.SMTP_SSL(host, port, context=ctx, timeout=30) as s:
                if user:
                    s.login(user, pwd)
                s.sendmail(msg["From"], recips, msg.as_string())
        else:
            with smtplib.SMTP(host, port, timeout=30) as s:
                if _env_bool("SMTP_USE_TLS", True):
                    s.starttls(context=ctx)
                if user:
                    s.login(user, pwd)
                s.sendmail(msg["From"], recips, msg.as_string())
        _log(f"email sent to {len(recips)} recipient(s): {subject}")
        return 0
    except Exception as exc:  # noqa: BLE001
        _log(f"ERROR sending email: {exc}")
        return 2


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="TFB post-sync digest (email only).")
    ap.add_argument("--sheet-id", default="")
    ap.add_argument("--session", default="", help="Label for the run, e.g. '11:00 Riyadh'.")
    ap.add_argument("--dry-run", action="store_true", help="Print the email; do not send.")
    args = ap.parse_args(argv)

    if not _env_bool("DIGEST_ENABLE", True):
        _log("DIGEST_ENABLE=0 — disabled; exiting without sending.")
        return 0

    sheet_id = args.sheet_id or _env_any(
        ["TARGET_SHEET_ID", "DEFAULT_SPREADSHEET_ID", "TFB_SHEET_ID", "SHEET_ID"]
    )
    if not sheet_id:
        _log("ERROR no spreadsheet id (TARGET_SHEET_ID / DEFAULT_SPREADSHEET_ID).")
        return 2

    session = args.session or datetime.now(RIYADH_TZ).strftime("%H:%M Riyadh")
    read_range = _get_read_range()

    candidate_pages = [
        p.strip() for p in _env("DIGEST_CANDIDATE_PAGES", "Market_Leaders").split(",") if p.strip()
    ]
    holdings_page = _env("DIGEST_HOLDINGS_PAGE", "My_Portfolio")

    candidates: List[Dict[str, Any]] = []
    for p in candidate_pages:
        candidates.extend(_load_page(read_range, sheet_id, p))
    holdings_raw = _load_page(read_range, sheet_id, holdings_page)

    # v1.1.0 (D1): the SELL/SWAP universe is the ledger-ACTIVE book only.
    inactive = _load_inactive_symbols(read_range, sheet_id)
    holdings = _filter_holdings(holdings_raw, inactive)

    # v1.1.0 (D2): BUY may not re-recommend a symbol already held; a held
    # ISSUER under another symbol stays eligible but is flagged below.
    held_syms, held_names = _held_issuers(holdings)
    buy_pool = candidates
    if _env_bool("DIGEST_EXCLUDE_HELD", True) and held_syms:
        buy_pool = [c for c in candidates if c["symbol"].upper() not in held_syms]
        n_excl = len(candidates) - len(buy_pool)
        if n_excl:
            _log(f"BUY pool: {n_excl} held symbol(s) excluded")

    best_buy = _pick_best_buy(buy_pool)
    _annotate_sibling(best_buy, held_syms, held_names)
    best_sell = _pick_best_sell(holdings)
    swap = _pick_best_swap(best_sell, best_buy)

    if not (best_buy or best_sell) and _env_bool("DIGEST_SKIP_IF_EMPTY", False):
        _log("No qualifying picks and DIGEST_SKIP_IF_EMPTY=1 — not sending.")
        return 0

    subject, text, html = _compose(session, best_buy, best_sell, swap)
    return _send_email(subject, text, html, args.dry_run)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001  (never crash a CI step hard)
        traceback.print_exc()
        sys.exit(2)
