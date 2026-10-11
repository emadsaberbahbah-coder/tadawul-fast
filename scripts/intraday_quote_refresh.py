#!/usr/bin/env python3
# scripts/intraday_quote_refresh.py
"""
================================================================================
Intraday Quote Refresh — v1.0.3 (2026-10-10)
================================================================================
NEW script. Closes the largest recoverable loss in the system.

THE PROBLEM, PROVEN ARITHMETICALLY
    TFB_TICKET_MAX_QUOTE_AGE_MIN = 15   (in-session limit)
    daily_sync cron `0 */4 * * *`   ->  Riyadh 03 07 11 15 19 23
    Tadawul session                 ->  10:00 - 15:00  (ONE sync inside it)

        Top_10 run 09:58  ->  last sync 07:00  ->  quote age 178 min
        Top_10 run 13:40  ->  last sync 11:00  ->  quote age 160 min
        engine reported                            178m / 156m   EXACT MATCH

    The Quote-Freshness gate blocked 125 of 300 candidates (41.7%) on
    2026-07-27. The gate is CORRECT. The cadence cannot serve it.

WHY NOT JUST RUN daily_sync MORE OFTEN
    It walks up to 7,000 symbols over a 2-leg matrix, TFB_SYNC_TIME_BUDGET_SEC
    =3600, timeout-minutes 115; the workflow's own notes record the GM leg
    taking ~69 minutes. A full cycle cannot fit in 15 minutes at any cadence.

WHAT THIS DOES
    Refreshes ONLY the decision symbols (~30-70, not 7,000) and writes ONLY
    quote cells per symbol: Current Price, retrieval stamps and Warnings.
    This is a partial quote refresh, not a current full-model evaluation.
    Successful source evidence is required. The carried model is explicitly
    preserved/unready, and conflicting derived return displays are blanked.

WHY THIS CANNOT WIPE A PAGE — the design constraint that shaped it
    daily_sync writes by CLEAR-AND-REWRITE, and this workbook has already lost
    two pages to a run cancelled mid-write. This script therefore:
      * never clears anything
      * never writes a whole row
      * never appends or deletes rows
      * writes ONLY into cells whose row it has re-verified carries the
        expected symbol immediately beforehand (symbol-keyed, NEVER positional
        -- the positional-zip lesson)
    A complete raw header/row fingerprint is rechecked before each row-atomic
    batch. Sheets has no compare-and-swap; changes after that read remain a
    narrow race, so this script never claims transactional isolation.

STALENESS IS ONE-WAY
    A cell is written only when the incoming quote is STRICTLY NEWER than the
    stamp already there. This script can never move a page backwards in time.

USAGE
    python scripts/intraday_quote_refresh.py --selftest
    python scripts/intraday_quote_refresh.py --scan      # default, no writes
    python scripts/intraday_quote_refresh.py --apply

ENV
    TARGET_SHEET_ID / DEFAULT_SPREADSHEET_ID     workbook id
    TFB_BACKEND_URL / BACKEND_URL                backend base url
    GOOGLE_SHEETS_CREDENTIALS(_B64) | GOOGLE_APPLICATION_CREDENTIALS
    TFB_IQR_SYMBOL_PAGES   default "Top_10_Investments,My_Portfolio,Shadow_Board"
    TFB_IQR_TARGET_PAGES   default "Market_Leaders,Global_Markets"
    TFB_IQR_MAX_SYMBOLS    default 400   hard ceiling on the fetch
    TFB_IQR_TIMEOUT_SEC    default 45
================================================================================
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
SCRIPT_VERSION = "1.0.3"
# v1.0.3: share supplier quote-time parsing with acquisition validity so an
# EODHD integer epoch serialized as a string retains its actual quote receipt.

RUN_LOG_TAB = "_Run_Log"
PRICE_HEADERS = ("Current Price", "Price")
STAMP_HEADERS = ("Last Updated (Riyadh)", "Last Updated (UTC)", "Last Updated")
SYMBOL_HEADERS = ("Symbol", "Ticker")
ALLOWED_TARGET_PAGES = frozenset({"Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds"})
WARNING_KEYS = frozenset({"warnings", "warning", "flags", "rowwarnings"})
ACQUISITION_KEYS = frozenset({"acquisitionstatus", "acquisitionacquiredat", "acquisitionquoteasof", "acquisitionprovider"})
RETURN_KEYS = frozenset({"expectedroi1m", "expectedroi3m", "expectedroi12m", "upsidedownsidepct", "upsidedownside", "upsidepct", "upside"})
PAIR_KEYS = RETURN_KEYS | frozenset({"targetprice", "analysttargetprice", "intrinsicvalue", "forecastprice1m", "forecastprice3m", "forecastprice12m"})


# --------------------------------------------------------------------------- #
# small helpers                                                                #
# --------------------------------------------------------------------------- #
def _s(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _f(v: Any) -> Optional[float]:
    if isinstance(v, bool):
        return None
    try:
        t = _s(v).replace(",", "")
        value = float(t) if t else None
        return value if value is not None and math.isfinite(value) else None
    except Exception:
        return None


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _env(name: str, default: str = "") -> str:
    return _s(os.getenv(name)) or default


def _env_int(name: str, default: int) -> int:
    try:
        return int(_s(os.getenv(name)) or default)
    except Exception:
        return default


def _col_letter(idx0: int) -> str:
    n, out = idx0 + 1, ""
    while n:
        n, r = divmod(n - 1, 26)
        out = chr(65 + r) + out
    return out


def _parse_ts(v: Any, header: str = "Last Updated (UTC)") -> Optional[datetime]:
    """Read a target retrieval cell using its declared UTC/Riyadh basis."""
    basis = timezone.utc if _key(header) == "lastupdatedutc" else timezone(timedelta(hours=3))
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        if not math.isfinite(v) or not 20000 < v < 80000 or v == int(v):
            return None
        return (datetime(1899, 12, 30, tzinfo=basis) + timedelta(days=v)).astimezone(timezone.utc)
    t = _s(v)
    if not t:
        return None
    t = t.replace("Z", "+00:00")
    for fmt in (None, "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
                "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            d = datetime.fromisoformat(t) if fmt is None \
                else datetime.strptime(t, fmt)
            if ":" not in t:
                return None
            return (d if d.tzinfo else d.replace(tzinfo=basis)).astimezone(timezone.utc)
        except Exception:
            continue
    return None


def _key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", _s(value).lower())


def _clock() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class QuoteEvidence:
    symbol: str
    price: float
    currency: str
    provider: str
    acquired_at: datetime
    quote_asof: datetime
    retrieved_at: datetime


def _currency(values: Sequence[Any]) -> Optional[str]:
    raw = [_s(value) for value in values if _s(value)]
    # Yahoo's GBp is pence, not GBP. Never infer/convert the price unit.
    if not raw or any(value == "GBp" or not re.fullmatch(r"[A-Za-z]{3}", value) for value in raw):
        return None
    normalized = {value.upper() for value in raw}
    return next(iter(normalized)) if len(normalized) == 1 else None


def _fresh_quote(evidence: QuoteEvidence, now: datetime) -> bool:
    from core.analysis import opportunity_builder as ob
    from core.data_validity import row_acquisition
    if not isinstance(evidence, QuoteEvidence):
        return False
    try:
        stamps = (now, evidence.acquired_at, evidence.quote_asof, evidence.retrieved_at)
        if any(not isinstance(stamp, datetime) or stamp.tzinfo is None or stamp.utcoffset() is None for stamp in stamps):
            return False
        if evidence.retrieved_at > now + timedelta(seconds=60) or evidence.quote_asof > evidence.retrieved_at + timedelta(seconds=60):
            return False
        source = {"symbol": evidence.symbol, "current_price": evidence.price, "data_provider": evidence.provider,
                  "acquisition_status": "success", "acquisition_provider": evidence.provider,
                  "acquisition_acquired_at": evidence.acquired_at.isoformat(),
                  "acquisition_quote_asof": evidence.quote_asof.isoformat()}
        proof = row_acquisition(source, now, ob._env_freshness_fallback_h() * 3600)
        passed, _reason, _detail = ob._quote_freshness_assessment({"symbol": evidence.symbol, "quote_evidence": {
            "status": proof.status, "reason": proof.reason,
            "acquired_at": evidence.acquired_at.isoformat(), "quote_asof": evidence.quote_asof.isoformat()}})
        return proof.successful and passed
    except Exception:
        return False


def _source_quote(row: Dict[str, Any], requested: str, retrieved: datetime) -> Optional[QuoteEvidence]:
    from core.analysis import opportunity_builder as ob
    from core.data_validity import acquisition_tokens, row_acquisition, source_quote_instant
    try:
        grouped: Dict[str, List[Any]] = {}
        for name, value in row.items():
            grouped.setdefault(_key(name), []).append(value)
        symbols = {_s(value).upper() for key in ("symbol", "ticker") for value in grouped.get(key, []) if _s(value)}
        if symbols != {requested}:
            return None
        tokens = {}
        for key in WARNING_KEYS:
            for value in grouped.get(key, []):
                tokens.update(acquisition_tokens(value))
        for name in ("acquisition_status", "acquisition_provider", "acquisition_acquired_at", "acquisition_quote_asof"):
            for value in grouped.get(_key(name), []):
                tokens[name] = _s(value)
        if tokens.get("acquisition_status", "").lower() != "success":
            return None
        proof = row_acquisition(row, retrieved, ob._env_freshness_fallback_h() * 3600)
        if not proof.successful or proof.quote_asof is None or proof.acquired_at is None:
            return None
        for key in ("pricebarts", "quotetimestamp", "regularmarkettime"):
            for value in grouped.get(key, []):
                if value in (None, ""):
                    continue
                stamp = source_quote_instant(value)
                if stamp is None or stamp != proof.quote_asof:
                    return None
        currency = _currency(grouped.get("currency", []))
        if currency is None:
            return None
        prices = [_f(value) for key in ("currentprice", "price", "lastprice")
                  for value in grouped.get(key, []) if _s(value)]
        if not prices or any(value is None or value <= 0 for value in prices) or len(set(prices)) != 1:
            return None
        provider = tokens.get("acquisition_provider", "")
        evidence = QuoteEvidence(requested, prices[0], currency, provider, proof.acquired_at, proof.quote_asof, retrieved)
        return evidence if _fresh_quote(evidence, retrieved) else None
    except Exception:
        return None


def _fingerprint(header: Sequence[Any], row: Sequence[Any]) -> str:
    raw = json.dumps([list(header), list(row)], ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def _partial_warnings(original: Any, quote: QuoteEvidence, displayed: Any) -> str:
    def parts(value):
        items = value if isinstance(value, (list, tuple)) else [value]
        return [part.strip() for item in items for part in _s(item).split(";") if part.strip()]
    retained = [part for part in parts(original) + parts(displayed)
                if _key(part.partition(":")[0]) not in ACQUISITION_KEYS
                and not part.lower().startswith("intraday_quote_")]
    receipt = ["acquisition_status:preserved", "acquisition_provider:" + quote.provider,
               "acquisition_acquired_at:" + quote.retrieved_at.isoformat(),
               "acquisition_quote_asof:" + quote.quote_asof.isoformat(),
               "intraday_quote_source_acquired_at:" + quote.acquired_at.isoformat(),
               "intraday_quote_currency:" + quote.currency, "intraday_quote_model:preserved"]
    return "; ".join(dict.fromkeys(retained + receipt))


def _pick(header: Sequence[str], names: Sequence[str]) -> Optional[int]:
    low = {h.strip().lower(): i for i, h in enumerate(header) if _s(h)}
    for n in names:
        i = low.get(n.strip().lower())
        if i is not None:
            return i
    return None


# --------------------------------------------------------------------------- #
# PURE CORE — no IO, fully selftestable                                        #
# --------------------------------------------------------------------------- #
def _symbol_columns(values: Sequence[Sequence[Any]]) -> List[Tuple[int, int]]:
    """v1.0.1 — find EVERY (header_row_index, symbol_col_index) pair in a page.

    A page is not guaranteed to put its header on row 1. Top_10_Investments
    carries a summary block first and its 300-row candidate audit header sits
    at row 51; reading row 1 harvests nothing and silently misses the entire
    gated candidate pool — which is precisely the set this script exists to
    refresh. So every row is scanned for a header signature, and a page may
    legitimately yield several blocks.
    """
    out: List[Tuple[int, int]] = []
    for i, row in enumerate(values):
        header = [_s(c) for c in row]
        if not any(header):
            continue
        si = _pick(header, SYMBOL_HEADERS)
        if si is None:
            continue
        # A header row names other columns too; a data row that merely happens
        # to contain the word "Symbol" does not.
        named = sum(1 for h in header if _s(h))
        if named >= 2:
            out.append((i, si))
    return out


def harvest_symbols(pages: Dict[str, Sequence[Sequence[Any]]],
                    limit: int = 400) -> List[str]:
    """Union of decision symbols across the source pages, order-stable and
    de-duplicated. Scans EVERY header block on each page (see _symbol_columns)."""
    out: List[str] = []
    seen = set()
    for _name, values in pages.items():
        if not values:
            continue
        blocks = _symbol_columns(values)
        if not blocks:
            continue
        starts = [b[0] for b in blocks] + [len(values)]
        for bi, (hdr_i, si) in enumerate(blocks):
            stop = starts[bi + 1]
            for row in values[hdr_i + 1:stop]:
                if si >= len(row):
                    continue
                sym = _s(row[si]).upper()
                if not sym or sym in seen:
                    continue
                # cheap shape filter: a ticker has no spaces and is short
                if " " in sym or len(sym) > 15:
                    continue
                seen.add(sym)
                out.append(sym)
                if len(out) >= limit:
                    return out
    return out


def plan_page_updates(page: str,
                      values: Sequence[Sequence[Any]],
                      quotes: Dict[str, QuoteEvidence]
                      ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Build the surgical cell plan for one page.

    Quotes require source evidence and native currency agreement. The carried
    model is unready; only owned quote cells and conflicted return displays
    can change. Every plan binds its complete original raw header and row.
    """
    stats = {"rows": 0, "matched": 0, "planned": 0,
             "skipped_not_newer": 0, "skipped_no_price": 0, "skipped_unverified": 0,
             "skipped_currency": 0, "skipped_schema": 0}
    if not values or page not in ALLOWED_TARGET_PAGES:
        return [], stats
    header = [_s(c) for c in values[0]]
    if any(sum(_key(name) == key for name in header) > 1 for key in PAIR_KEYS):
        stats["skipped_schema"] = max(0, len(values)-1)
        return [], stats
    si = _pick(header, SYMBOL_HEADERS)
    pi = _pick(header, PRICE_HEADERS)
    stamp_cols = [i for i, name in enumerate(header) if _key(name) in {"lastupdated", "lastupdatedutc", "lastupdatedriyadh"}]
    warning_cols = [i for i, name in enumerate(header) if _key(name) in WARNING_KEYS]
    currency_cols = [i for i, name in enumerate(header) if _key(name) == "currency"]
    if si is None or pi is None or not stamp_cols or len(warning_cols) != 1 or not currency_cols or \
            sum(_key(name) in {"currentprice", "price"} for name in header) != 1 or \
            sum(_key(name) in {"symbol", "ticker"} for name in header) != 1 or \
            any(_key(name) in ACQUISITION_KEYS for name in header):
        stats["skipped_schema"] = max(0, len(values)-1)
        return [], stats
    ti, wi = stamp_cols[0], warning_cols[0]
    from core.data_validity import acquisition_tokens, precise_utc
    from core.sheet_presentation import present_instrument_row

    plan: List[Dict[str, Any]] = []
    for r_off, row in enumerate(values[1:], start=2):
        stats["rows"] += 1
        if si >= len(row):
            continue
        sym = _s(row[si]).upper()
        q = quotes.get(sym)
        if not sym or q is None:
            continue
        stats["matched"] += 1

        if not isinstance(q, QuoteEvidence) or q.symbol != sym or not _fresh_quote(q, _clock()):
            stats["skipped_unverified"] += 1
            continue
        currency = _currency([row[i] if i < len(row) else None for i in currency_cols])
        if currency is None or currency != q.currency:
            stats["skipped_currency"] += 1
            continue
        existing_stamps = [(_parse_ts(row[i], header[i]), row[i]) for i in stamp_cols if i < len(row) and _s(row[i])]
        # The canonical producer samples UTC/Riyadh clocks separately. Accept
        # subsecond jitter only within the SAME UTC second; bind the newest
        # actual instant, never truncate the monotonicity comparison itself.
        if any(stamp is None for stamp, raw in existing_stamps) or \
                len({stamp.replace(microsecond=0) for stamp, raw in existing_stamps if stamp is not None}) > 1:
            stats["skipped_schema"] += 1
            continue
        old_ts = max(stamp for stamp, raw in existing_stamps) if existing_stamps else None
        tokens = acquisition_tokens(row[wi] if wi < len(row) else "")
        old_quote = precise_utc(tokens.get("acquisition_quote_asof"))
        if tokens.get("acquisition_status") == "conflict" or (tokens.get("acquisition_quote_asof") and old_quote is None):
            stats["skipped_schema"] += 1
            continue
        if (old_ts is not None and q.retrieved_at <= old_ts) or (old_quote is not None and q.quote_asof <= old_quote):
            stats["skipped_not_newer"] += 1
            continue
        original = {name: row[i] if i < len(row) else "" for i, name in enumerate(header)}
        # Only the shared return-display boundary is projected. Do not change
        # margins, horizons, model prices, scores or any manual input cell.
        subset = {name: value for name, value in original.items() if _key(name) in PAIR_KEYS}
        subset.update(current_price=q.price, warnings=original[header[wi]])
        displayed = present_instrument_row(subset)
        changes = {pi: q.price, wi: _partial_warnings(original[header[wi]], q, displayed.get("warnings"))}
        for i in stamp_cols:
            tz = timezone.utc if _key(header[i]) == "lastupdatedutc" else timezone(timedelta(hours=3))
            changes[i] = q.retrieved_at.astimezone(tz).isoformat()
        for i, name in enumerate(header):
            if _key(name) in RETURN_KEYS and original[name] not in (None, "") and displayed.get(name) is None:
                changes[i] = ""  # RAW explicit blank; omission would retain stale ROI.
        try:
            fingerprint = _fingerprint(values[0], row)
        except (ValueError, TypeError):
            stats["skipped_schema"] += 1
            continue

        plan.append({
            "page": page, "sheet_row": r_off, "symbol": sym,
            "symbol_col": si, "price_col": pi, "stamp_col": ti,
            "price_old": _s(row[pi]) if pi < len(row) else "",
            "price_new": q.price,
            "stamp_old": _s(row[ti]) if ti < len(row) else "",
            "stamp_new": changes[ti], "changes": changes,
            "fingerprint": fingerprint, "quote": q,
        })
        stats["planned"] += 1
    return plan, stats


# --------------------------------------------------------------------------- #
# IO                                                                           #
# --------------------------------------------------------------------------- #
def _open_sheet(cli_id: Optional[str]):
    import gspread                                       # noqa: WPS433
    from google.oauth2.service_account import Credentials  # noqa: WPS433

    sid = None
    for v in (cli_id, os.getenv("TARGET_SHEET_ID"),
              os.getenv("DEFAULT_SPREADSHEET_ID"), os.getenv("SPREADSHEET_ID")):
        if _s(v):
            sid = _s(v)
            break
    if not sid:
        raise SystemExit("No spreadsheet id (--sheet-id or TARGET_SHEET_ID).")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    raw = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    b64 = os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64")
    if raw or b64:
        if b64 and not raw:
            import base64
            raw = base64.b64decode(b64).decode("utf-8")
        creds = Credentials.from_service_account_info(json.loads(raw),
                                                      scopes=scopes)
    elif path:
        creds = Credentials.from_service_account_file(path, scopes=scopes)
    else:
        raise SystemExit("No Google credentials in environment.")
    return gspread.authorize(creds).open_by_key(sid)


def fetch_quotes(base_url: str, page: str, symbols: Sequence[str],
                 timeout: int = 45) -> Dict[str, QuoteEvidence]:
    """GET /v1/analysis/sheet-rows?page=..&symbols=..

    The route resolves 'requested symbols by each row's OWN declared symbol —
    NEVER by position', so the response is safe to index by symbol.
    """
    import urllib.parse
    import urllib.request

    if not base_url or not symbols:
        return {}
    qs = urllib.parse.urlencode({"page": page,
                                 "symbols": ",".join(symbols),
                                 "limit": str(len(symbols))})
    url = "%s/v1/analysis/sheet-rows?%s" % (base_url.rstrip("/"), qs)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:                              # noqa: BLE001
        print("  [FETCH-FAIL] %s: %s" % (page, type(exc).__name__))
        return {}

    retrieved = _clock()
    if not isinstance(payload, dict):
        return {}
    # The real sheet-rows route has matrix rows and dictionary data/row_objects.
    # Collect only dictionary aliases; contradictory source duplicates fail
    # closed even when returned in different aliases in the same envelope.
    rows = [row for name in ("row_objects", "rows", "data", "items", "records", "quotes", "results")
            for row in (payload.get(name) if isinstance(payload.get(name), list) else []) if isinstance(row, dict)]
    requested = {_s(symbol).upper() for symbol in symbols}
    grouped: Dict[str, List[Optional[QuoteEvidence]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        declared = {_s(value).upper() for name, value in row.items() if _key(name) in {"symbol", "ticker"} and _s(value)}
        for sym in declared & requested:
            grouped.setdefault(sym, []).append(_source_quote(row, sym, retrieved))
    return {symbol: evidence[0] for symbol, evidence in grouped.items()
            if evidence[0] is not None and len(set(evidence)) == 1}


def apply_page_plan(ws: Any, page: str, plan: Sequence[Dict[str, Any]]) -> Tuple[int, int, bool]:
    """Recheck complete raw inputs before each row-atomic, <=100-cell batch."""
    if page not in ALLOWED_TARGET_PAGES:
        return 0, len(plan), False
    groups: List[List[Dict[str, Any]]] = []
    batch: List[Dict[str, Any]] = []
    cells = 0
    for item in plan:
        size = len(item["changes"])
        if batch and cells + size > 100:
            groups.append(batch)
            batch, cells = [], 0
        batch.append(item)
        cells += size
    if batch:
        groups.append(batch)
    written = abandoned = 0
    for group in groups:
        try:
            fresh = ws.get_all_values(value_render_option="UNFORMATTED_VALUE")
        except Exception:
            return written, abandoned, True
        updates = []
        for item in group:
            r = item["sheet_row"]
            if not fresh or r > len(fresh):
                abandoned += 1
                continue
            try:
                row = fresh[r-1]
                if _fingerprint(fresh[0], row) != item["fingerprint"]:
                    abandoned += 1
                    continue
                quote = item["quote"]
                rebuilt, _ = plan_page_updates(page, [fresh[0], row], {item["symbol"]: quote})
                if not rebuilt or rebuilt[0]["changes"] != item["changes"] or quote.price != item["price_new"]:
                    abandoned += 1
                    continue
                updates.extend({"range": "%s%d" % (_col_letter(col), r), "values": [[value]]}
                               for col, value in sorted(item["changes"].items()))
            except Exception:
                abandoned += 1
        if updates:
            try:
                ws.batch_update(updates, value_input_option="RAW")
            except Exception:
                return written, abandoned, True
            written += len(updates)
    return written, abandoned, False


# --------------------------------------------------------------------------- #
# MAIN                                                                         #
# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--sheet-id")
    ap.add_argument("--backend", default="")
    args = ap.parse_args()

    if args.selftest:
        return _selftest()

    base = args.backend or _env("TFB_BACKEND_URL") or _env("BACKEND_URL")
    src_pages = [p.strip() for p in _env(
        "TFB_IQR_SYMBOL_PAGES",
        "Top_10_Investments,My_Portfolio,Shadow_Board").split(",") if p.strip()]
    tgt_pages = [p.strip() for p in _env(
        "TFB_IQR_TARGET_PAGES",
        "Market_Leaders,Global_Markets").split(",") if p.strip()]
    denied = [page for page in tgt_pages if page not in ALLOWED_TARGET_PAGES]
    tgt_pages = list(dict.fromkeys(page for page in tgt_pages if page in ALLOWED_TARGET_PAGES))
    if denied:
        print("  [SCOPE] refused %d unsupported target page(s)" % len(denied))
    max_syms = _env_int("TFB_IQR_MAX_SYMBOLS", 400)
    timeout = _env_int("TFB_IQR_TIMEOUT_SEC", 45)

    sh = _open_sheet(args.sheet_id)

    src: Dict[str, Sequence[Sequence[Any]]] = {}
    for p in src_pages:
        try:
            src[p] = sh.worksheet(p).get_all_values(value_render_option="UNFORMATTED_VALUE")
        except Exception as exc:                          # noqa: BLE001
            print("  [SRC-MISS] %s: %s" % (p, exc))
    symbols = harvest_symbols(src, limit=max_syms)

    mode = "APPLY" if args.apply else "DRY-RUN"
    print("[IQR v%s] decision_symbols=%d backend=%s mode=%s"
          % (SCRIPT_VERSION, len(symbols), "set" if base else "MISSING", mode))
    if not symbols:
        print("  no decision symbols harvested — nothing to do.")
        return 0
    if not base:
        print("  no backend url — cannot fetch. Set TFB_BACKEND_URL.")
        return 2

    total_written = 0
    write_failed = False
    for page in tgt_pages:
        try:
            ws = sh.worksheet(page)
            values = ws.get_all_values(value_render_option="UNFORMATTED_VALUE")
        except Exception as exc:                          # noqa: BLE001
            print("  [PAGE-MISS] %s: %s" % (page, exc))
            continue

        quotes = fetch_quotes(base, page, symbols, timeout=timeout)
        plan, stats = plan_page_updates(page, values, quotes)
        print("  %-16s rows=%-6d quotes=%-4d matched=%-4d planned=%-4d "
              "not_newer=%-4d no_price=%d"
              % (page, stats["rows"], len(quotes), stats["matched"],
                 stats["planned"], stats["skipped_not_newer"],
                 stats["skipped_no_price"]))
        for p in plan[:8]:
            print("     row %5d %-10s %s -> %s   stamp %s -> %s"
                  % (p["sheet_row"], p["symbol"], p["price_old"],
                     p["price_new"], p["stamp_old"] or "(blank)",
                     p["stamp_new"]))
        if len(plan) > 8:
            print("     ... %d more" % (len(plan) - 8))

        if not args.apply or not plan:
            continue

        written, abandoned, failed = apply_page_plan(ws, page, plan)
        write_failed = write_failed or failed
        if abandoned:
            print("     [ROW-CHANGED] %d row(s) abandoned — raw input fingerprint/evidence changed" % abandoned)
        if failed:
            print("     [WRITE-FAIL] page batch could not be confirmed")
        total_written += written
        print("     wrote %d cell(s)" % written)

    if args.apply:
        try:
            sh.worksheet(RUN_LOG_TAB).append_row(
                [_now_utc(), "INFO", "intraday_quote_refresh", ",".join(tgt_pages),
                 "PARTIAL" if write_failed else "OK", "[IQR v%s] symbols=%d cells=%d"
                 % (SCRIPT_VERSION, len(symbols), total_written),
                 "", "", "", json.dumps({"version": SCRIPT_VERSION})],
                value_input_option="RAW")
        except Exception:
            pass
        print("[IQR v%s] APPLIED cells=%d" % (SCRIPT_VERSION, total_written))
    else:
        print("  (dry-run: nothing written; re-run with --apply)")
    return 2 if write_failed else 0


# --------------------------------------------------------------------------- #
# SELFTEST — offline                                                           #
# --------------------------------------------------------------------------- #
def _selftest() -> int:
    checks: List[Tuple[str, bool]] = []

    src = {
        "Top_10_Investments": [["Symbol", "Name"], ["AAPL", "Apple"],
                               ["1150.SR", "Alinma"]],
        "My_Portfolio": [["Symbol", "Name"], ["AAPL", "Apple"],
                         ["NTES", "NetEase"]],
    }
    syms = harvest_symbols(src)
    checks.append(("harvest unions and de-duplicates, order-stable",
                   syms == ["AAPL", "1150.SR", "NTES"]))
    checks.append(("harvest honours the ceiling",
                   harvest_symbols(src, limit=2) == ["AAPL", "1150.SR"]))

    # v1.0.1: a page whose real header is NOT row 1 (the Top_10 layout)
    multi = {"Top_10_Investments": [
        ["Decision Top 10", ""], ["generated", "2026-07-27"], ["", ""],
        ["Symbol", "Name", "Ticket"], ["MRP.US", "Millrose", "19773"],
        ["", ""],
        ["Symbol", "Name", "Market", "Verdict"],
        ["1120.SR", "Al Rajhi", "TASI", "BLOCKED"],
        ["EXE.US", "Expand", "NYSE", "BLOCKED"]]}
    got = harvest_symbols(multi)
    checks.append(("late header block is found, not silently skipped",
                   got == ["MRP.US", "1120.SR", "EXE.US"]))
    checks.append(("a title row is not mistaken for a header",
                   "DECISION TOP 10" not in got))

    now = _clock()
    stamp = now.isoformat()
    def source(symbol, price):
        return _source_quote({"symbol": symbol, "current_price": price, "currency": "USD", "data_provider": "EODHD",
                              "acquisition_status": "success", "acquisition_provider": "EODHD",
                              "acquisition_acquired_at": stamp,
                              "acquisition_quote_asof": (now-timedelta(minutes=2)).isoformat()}, symbol, now)
    hdr = ["Symbol", "Name", "Current Price", "Last Updated (Riyadh)", "Currency", "Warnings", "Forecast Price (1M)", "Expected ROI (1M)"]
    page = [hdr,
            ["AAPL", "Apple", 100.0, "2026-07-27 11:00:00", "USD", "", 110, .1],
            ["MSFT", "Microsoft", 380.0, (now+timedelta(days=1)).isoformat(), "USD", "", 400, .0526316],
            ["ZZZZ", "Other", 5.0, "2026-07-27 11:00:00", "USD", "", 6, .2]]
    quotes = {
        "AAPL": source("AAPL", 333.02),
        "MSFT": source("MSFT", 381.70),
        "NVDA": source("NVDA", 206.84),
    }
    plan, st = plan_page_updates("Market_Leaders", page, quotes)

    checks.append(("only the strictly-newer quote is planned",
                   [p["symbol"] for p in plan] == ["AAPL"]))
    checks.append(("an OLDER incoming stamp is refused (one-way staleness)",
                   st["skipped_not_newer"] == 1))
    checks.append(("a symbol absent from the page is never inserted",
                   all(p["symbol"] != "NVDA" for p in plan)))
    checks.append(("a page symbol absent from quotes is untouched",
                   all(p["symbol"] != "ZZZZ" for p in plan)))
    checks.append(("plan targets owned price/stamp/provenance/return cells",
                   set(plan[0]["changes"]) == {2, 3, 5, 7}))
    checks.append(("carried model is preserved and contradictory ROI is explicitly blank",
                   "acquisition_status:preserved" in plan[0]["changes"][5] and plan[0]["changes"][7] == ""))
    checks.append(("sheet row is 1-based and correct",
                   plan[0]["sheet_row"] == 2))

    checks.append(("a zero/absent price cannot produce source evidence", source("AAPL", 0) is None))

    blank = [hdr, ["AAPL", "Apple", 100.0, "", "USD", "", 110, .1]]
    bplan, _ = plan_page_updates("Market_Leaders", blank, quotes)
    checks.append(("a blank existing stamp counts as older",
                   len(bplan) == 1))

    noc, _ = plan_page_updates("Market_Leaders", [["Symbol", "Name"], ["AAPL", "x"]], quotes)
    checks.append(("missing price/stamp columns -> empty plan, no crash",
                   noc == []))
    checks.append(("empty page -> empty plan, no crash",
                   plan_page_updates("Market_Leaders", [], quotes)[0] == []))
    checks.append(("column letters", _col_letter(0) == "A"
                   and _col_letter(26) == "AA"))

    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print("[intraday_quote_refresh v%s] SELFTEST %d/%d"
          % (SCRIPT_VERSION, passed, len(checks)))
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    sys.exit(main())
