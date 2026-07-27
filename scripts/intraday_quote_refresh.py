#!/usr/bin/env python3
# scripts/intraday_quote_refresh.py
"""
================================================================================
Intraday Quote Refresh — v1.0.1 (2026-07-27)
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
    two cells per symbol: Current Price and Last Updated. The freshness gate
    reads exactly those two fields off the page row
    (opportunity_builder._quote_freshness_assessment -> engine_gate.last_updated),
    so nothing downstream needs to change. No new tab, no read-side hook,
    no environment variable, no gate modification.

WHY THIS CANNOT WIPE A PAGE — the design constraint that shaped it
    daily_sync writes by CLEAR-AND-REWRITE, and this workbook has already lost
    two pages to a run cancelled mid-write. This script therefore:
      * never clears anything
      * never writes a whole row
      * never appends or deletes rows
      * writes ONLY into cells whose row it has re-verified carries the
        expected symbol immediately beforehand (symbol-keyed, NEVER positional
        -- the positional-zip lesson)
    Worst case under a race: daily_sync later overwrites a cell with its own
    value and behaviour reverts to today's. The downside floor is zero.

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
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

SCRIPT_VERSION = "1.0.1"

RUN_LOG_TAB = "_Run_Log"
PRICE_HEADERS = ("Current Price", "Price")
STAMP_HEADERS = ("Last Updated (Riyadh)", "Last Updated (UTC)", "Last Updated")
SYMBOL_HEADERS = ("Symbol", "Ticker")


# --------------------------------------------------------------------------- #
# small helpers                                                                #
# --------------------------------------------------------------------------- #
def _s(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _f(v: Any) -> Optional[float]:
    try:
        t = _s(v).replace(",", "")
        return float(t) if t else None
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


def _parse_ts(v: Any) -> Optional[datetime]:
    """Tolerant timestamp parse -> aware UTC. Unparseable -> None (treated as
    'no stamp', which makes the incoming quote strictly newer)."""
    t = _s(v)
    if not t:
        return None
    t = t.replace("Z", "+00:00")
    for fmt in (None, "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
                "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d"):
        try:
            d = datetime.fromisoformat(t) if fmt is None \
                else datetime.strptime(t, fmt)
            return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


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
                      quotes: Dict[str, Dict[str, Any]]
                      ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Build the surgical cell plan for one page.

    A row is planned ONLY when:
      * the row's own Symbol cell matches a fetched symbol (symbol-keyed), AND
      * the incoming quote carries a usable price, AND
      * the incoming stamp is STRICTLY NEWER than the stamp already present.

    Returns (plan, stats). Every entry names the row, the symbol it verified,
    and both cells — so --apply can re-verify before writing.
    """
    stats = {"rows": 0, "matched": 0, "planned": 0,
             "skipped_not_newer": 0, "skipped_no_price": 0}
    if not values:
        return [], stats
    header = [_s(c) for c in values[0]]
    si = _pick(header, SYMBOL_HEADERS)
    pi = _pick(header, PRICE_HEADERS)
    ti = _pick(header, STAMP_HEADERS)
    if si is None or pi is None or ti is None:
        return [], stats

    plan: List[Dict[str, Any]] = []
    for r_off, row in enumerate(values[1:], start=2):
        stats["rows"] += 1
        if si >= len(row):
            continue
        sym = _s(row[si]).upper()
        q = quotes.get(sym)
        if not sym or not q:
            continue
        stats["matched"] += 1

        px = _f(q.get("price"))
        if px is None or px <= 0:
            stats["skipped_no_price"] += 1
            continue

        new_ts = _parse_ts(q.get("last_updated"))
        old_ts = _parse_ts(row[ti] if ti < len(row) else "")
        if new_ts is None:
            stats["skipped_not_newer"] += 1
            continue
        if old_ts is not None and new_ts <= old_ts:
            stats["skipped_not_newer"] += 1
            continue

        plan.append({
            "page": page, "sheet_row": r_off, "symbol": sym,
            "symbol_col": si, "price_col": pi, "stamp_col": ti,
            "price_old": _s(row[pi]) if pi < len(row) else "",
            "price_new": px,
            "stamp_old": _s(row[ti]) if ti < len(row) else "",
            "stamp_new": _s(q.get("last_updated")),
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
                 timeout: int = 45) -> Dict[str, Dict[str, Any]]:
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
        print("  [FETCH-FAIL] %s: %s" % (page, exc))
        return {}

    rows = payload.get("rows") or payload.get("data") or []
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        sym = _s(row.get("symbol") or row.get("ticker")).upper()
        if not sym:
            continue
        # Never trust a placeholder as a quote (v6.32.0 marker contract).
        blob = " ".join([_s(row.get("data_provider")), _s(row.get("warnings")),
                         _s(row.get("recommendation_reason"))]).lower()
        if "placeholder" in blob or "no live data" in blob:
            continue
        out[sym] = {"price": row.get("current_price") or row.get("price"),
                    "last_updated": row.get("last_updated")
                    or row.get("last_updated_riyadh")}
    return out


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
    max_syms = _env_int("TFB_IQR_MAX_SYMBOLS", 400)
    timeout = _env_int("TFB_IQR_TIMEOUT_SEC", 45)

    sh = _open_sheet(args.sheet_id)

    src: Dict[str, Sequence[Sequence[Any]]] = {}
    for p in src_pages:
        try:
            src[p] = sh.worksheet(p).get_all_values()
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
    for page in tgt_pages:
        try:
            ws = sh.worksheet(page)
            values = ws.get_all_values()
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

        # RE-VERIFY the symbol cell immediately before writing. The grid was
        # read seconds ago; daily_sync may have rewritten rows since. A row
        # whose symbol no longer matches is abandoned, never written blind.
        updates: List[Dict[str, Any]] = []
        abandoned = 0
        fresh = ws.get_all_values()
        for p in plan:
            r = p["sheet_row"]
            if r - 1 >= len(fresh):
                abandoned += 1
                continue
            row_now = fresh[r - 1]
            si = p["symbol_col"]
            if si >= len(row_now) or _s(row_now[si]).upper() != p["symbol"]:
                abandoned += 1
                continue
            updates.append({"range": "%s%d" % (_col_letter(p["price_col"]), r),
                            "values": [[p["price_new"]]]})
            updates.append({"range": "%s%d" % (_col_letter(p["stamp_col"]), r),
                            "values": [[p["stamp_new"]]]})
        if abandoned:
            print("     [ROW-MOVED] %d row(s) abandoned — symbol no longer "
                  "matches; never written blind" % abandoned)
        for i in range(0, len(updates), 100):
            ws.batch_update(updates[i:i + 100])
        total_written += len(updates)
        print("     wrote %d cell(s)" % len(updates))

    if args.apply:
        try:
            sh.worksheet(RUN_LOG_TAB).append_row(
                [_now_utc(), "INFO", "intraday_quote_refresh", ",".join(tgt_pages),
                 "OK", "[IQR v%s] symbols=%d cells=%d"
                 % (SCRIPT_VERSION, len(symbols), total_written),
                 "", "", "", json.dumps({"version": SCRIPT_VERSION})],
                value_input_option="RAW")
        except Exception:
            pass
        print("[IQR v%s] APPLIED cells=%d" % (SCRIPT_VERSION, total_written))
    else:
        print("  (dry-run: nothing written; re-run with --apply)")
    return 0


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

    hdr = ["Symbol", "Name", "Current Price", "Last Updated (Riyadh)"]
    page = [hdr,
            ["AAPL", "Apple", "100.0", "2026-07-27 11:00:00"],
            ["MSFT", "Microsoft", "380.0", "2026-07-27 11:00:00"],
            ["ZZZZ", "Other", "5.0", "2026-07-27 11:00:00"]]
    quotes = {
        "AAPL": {"price": 333.02, "last_updated": "2026-07-27 13:45:00"},
        "MSFT": {"price": 381.70, "last_updated": "2026-07-27 10:00:00"},
        "NVDA": {"price": 206.84, "last_updated": "2026-07-27 13:45:00"},
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
    checks.append(("plan targets exactly two columns",
                   plan[0]["price_col"] == 2 and plan[0]["stamp_col"] == 3))
    checks.append(("sheet row is 1-based and correct",
                   plan[0]["sheet_row"] == 2))

    nopx = plan_page_updates("X", page, {"AAPL": {"price": 0,
                                                  "last_updated":
                                                  "2026-07-27 13:45:00"}})[0]
    checks.append(("a zero/absent price is never written", nopx == []))

    blank = [hdr, ["AAPL", "Apple", "100.0", ""]]
    bplan, _ = plan_page_updates("X", blank, quotes)
    checks.append(("a blank existing stamp counts as older",
                   len(bplan) == 1))

    noc, _ = plan_page_updates("X", [["Symbol", "Name"], ["AAPL", "x"]], quotes)
    checks.append(("missing price/stamp columns -> empty plan, no crash",
                   noc == []))
    checks.append(("empty page -> empty plan, no crash",
                   plan_page_updates("X", [], quotes)[0] == []))
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
