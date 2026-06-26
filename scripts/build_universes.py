#!/usr/bin/env python3
"""
scripts/build_universes.py
==========================
Pull VALIDATED symbol universes from EODHD and write paste-ready CSVs, one per
dashboard page. Re-runnable to refresh.

WHY: the dashboard reads each page's tab as its universe. A ticker the providers
can't resolve becomes a blank-price / blank-sector row that leaks into Top_10
selection. Pulling each list from EODHD's own exchange-symbol-list endpoint
guarantees every symbol is one EODHD can resolve, already in the canonical Yahoo
format the sheet expects (NNNN.SR for Saudi, plain for US).

Pages produced:
  - Market_Leaders : EODHD exchange=SR, Type="Common Stock"   (~230 Saudi names)
  - Mutual_Funds   : EODHD exchange=SR, Type in {ETF, FUND}    (Saudi LISTED
                     ETFs/REITs only — open-end mutual funds are NOT exchange
                     listed and exist in NO provider, so they are intentionally
                     excluded rather than added as blank rows)
  - Global_Markets : EODHD exchange=US, "Common Stock", ranked by market cap via
                     the screener (top liquid names first), filled from the
                     symbol-list if the screener is capped, then truncated to
                     --max-global

Reads the SAME env vars as eodhd_provider:
  EODHD_API_KEY (or EODHD_API_TOKEN / EODHD_KEY)
  EODHD_BASE_URL  (default https://eodhistoricaldata.com/api)

Usage (Render Shell, from the repo root):
  python3 scripts/build_universes.py
  python3 scripts/build_universes.py --max-global 2500 --out /tmp/universes
Then paste each CSV's SYMBOL column into the matching tab.

===========================================================================
CHANGELOG / SAFETY UPDATE
===========================================================================
v1.1.0 (2026-06-26) — EMPTY-UNIVERSE WIPE GUARD
  Everything above this line is preserved verbatim as the original intent. The
  reality below overrides the "guarantees every symbol is one EODHD can resolve"
  premise FOR SAUDI ONLY, and makes the script refuse to emit a CSV that could
  wipe a populated tab.

  WHAT WENT WRONG (root cause, evidence-based):
    EODHD carries ZERO Saudi coverage on the current plan. Verified 2026-06-26
    against BOTH EODHD hosts (the live env pins EODHD_BASE_URL=eodhd.com; the
    code default is eodhistoricaldata.com):
        real-time/2222.SR  (Aramco, most-traded Saudi stock) -> "Ticker Not Found"
        real-time/1120.SR  (Al Rajhi)                         -> "Ticker Not Found"
        exchange-symbol-list/SR                               -> "Exchange Not Found."
    Same wall track_performance.py already hit and patched in v6.10.1
    ("EODHD returns 404 for every .SR ticker").

  THE LANDMINE THIS GUARD REMOVES:
    fetch_exchange_symbols("SR") raises (non-JSON / 404), main() swallows it and
    sets sr=[], build_from_symbols([]) returns [], and the ORIGINAL code called
    write_csv() UNCONDITIONALLY -> it wrote universe_Market_Leaders.csv and
    universe_Mutual_Funds.csv containing ONLY a header row, zero symbols. The
    closing instructions still said "Paste each file's SYMBOL column into the
    matching tab." Following that pastes an empty column and WIPES the ~230-name
    Saudi universe (and the Mutual_Funds tab).

  THE FIX:
    - safe_write_csv() refuses to write any page whose row count is below a sane
      floor (_MIN_EXPECTED). An empty / header-only CSV is NEVER written, for ANY
      page (this also protects Global_Markets if the US screener/list ever fails).
    - The SR-empty case prints an explicit, honest diagnostic instead of a silent
      "0 Saudi common stocks".
    - Pages that are skipped are tracked; the closing paste-instructions list ONLY
      pages that were actually written, and main() returns non-zero if any
      expected page was skipped, so the failure is impossible to miss.

  WHAT THIS DOES NOT (AND CANNOT) DO:
    It cannot regenerate the Saudi universe. No provider currently available can
    hand over the full Saudi roster: EODHD-SR is gone, Yahoo has no
    exchange-symbol-list endpoint (per-symbol lookups only), and Tadawul/Argaam
    are unconfigured URL-template providers. Market_Leaders / Mutual_Funds must
    therefore be maintained manually or from a Saudi-native source. Global_Markets
    generation (US via EODHD) is unaffected and still works.
"""
import os
import sys
import csv
import json
import time
import argparse
import urllib.parse
import urllib.request
import urllib.error

BUILD_UNIVERSES_VERSION = "1.1.0"

# Minimum sane row count per page. Below this, the source fetch almost certainly
# failed (or returned a degenerate result), so we REFUSE to write a CSV that
# could wipe a populated tab if pasted. An empty (header-only) CSV is never a
# legitimate output of this script.
_MIN_EXPECTED = {
    "Market_Leaders": 50,    # ~230 in practice; < 50 => SR fetch failed
    "Mutual_Funds": 1,       # few Saudi listed ETFs/REITs; 0 => SR fetch failed / none matched
    "Global_Markets": 500,   # ~3000 in practice; < 500 => US fetch failed
}

# --------------------------------------------------------------------------- #
# Config / env (mirrors eodhd_provider)
# --------------------------------------------------------------------------- #
def _key() -> str:
    for k in ("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_KEY"):
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return ""


def _base() -> str:
    return (os.getenv("EODHD_BASE_URL") or "https://eodhistoricaldata.com/api").rstrip("/")


# US boards we keep (drop OTC/PINK/GREY — illiquid, poor data)
_US_DROP_HINTS = ("OTC", "PINK", "GREY", "GRAY", "EXPERT")

# EODHD exchange code -> Yahoo suffix (extend as you add pages)
_YH_SUFFIX = {"US": "", "SR": ".SR", "LSE": ".L", "XETRA": ".DE",
              "PA": ".PA", "TO": ".TO", "HK": ".HK", "TSE": ".T"}


def _http_json(url: str, timeout: float = 90.0):
    req = urllib.request.Request(url, headers={"User-Agent": "TFB-UniverseBuilder/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def to_yahoo(code: str, exchange_code: str) -> str:
    code = (code or "").strip().upper()
    suffix = _YH_SUFFIX.get(exchange_code, "")
    return f"{code}{suffix}" if suffix else code


# --------------------------------------------------------------------------- #
# EODHD calls
# --------------------------------------------------------------------------- #
def fetch_exchange_symbols(exchange: str, key: str, base: str):
    """Full symbol list for an exchange: [{Code,Name,Country,Exchange,Type,...}]."""
    url = f"{base}/exchange-symbol-list/{exchange}?" + urllib.parse.urlencode(
        {"api_token": key, "fmt": "json"})
    data = _http_json(url)
    return data if isinstance(data, list) else []


def fetch_screener_top(exchange: str, key: str, base: str, want: int):
    """Top names by market cap via the screener (best liquid coverage).
    Paginates 100 at a time; tolerates plans that cap the offset (stops early)."""
    out = []
    seen = set()
    offset = 0
    page = 100
    while len(out) < want:
        filters = json.dumps([["exchange", "=", exchange]])
        url = f"{base}/screener?" + urllib.parse.urlencode({
            "api_token": key, "sort": "market_capitalization.desc",
            "filters": filters, "limit": page, "offset": offset})
        try:
            data = _http_json(url, timeout=60.0)
        except Exception as e:
            print(f"    screener stopped at offset {offset}: {e}")
            break
        rows = (data or {}).get("data") if isinstance(data, dict) else None
        if not rows:
            break
        for r in rows:
            code = (r.get("code") or r.get("Code") or "").strip()
            name = (r.get("name") or r.get("Name") or "").strip()
            if not code or code in seen:
                continue
            seen.add(code)
            out.append({"Code": code, "Name": name})
        if len(rows) < page:
            break
        offset += page
        time.sleep(0.25)  # be gentle on the rate limit
    return out


# --------------------------------------------------------------------------- #
# Page builders
# --------------------------------------------------------------------------- #
def build_from_symbols(rows, exchange_code, types=None, drop_us_otc=False, max_n=None):
    out, seen = [], set()
    for r in rows:
        t = (r.get("Type") or "").strip()
        if types and t not in types:
            continue
        if drop_us_otc:
            ex = (r.get("Exchange") or "").strip().upper()
            if any(h in ex for h in _US_DROP_HINTS):
                continue
        code = (r.get("Code") or "").strip()
        if not code:
            continue
        ysym = to_yahoo(code, exchange_code)
        if ysym in seen:
            continue
        seen.add(ysym)
        out.append((ysym, (r.get("Name") or "").strip()))
        if max_n and len(out) >= max_n:
            break
    return out


def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["SYMBOL", "NAME"])
        for s, n in rows:
            w.writerow([s, n])


def safe_write_csv(path, rows, page, written, skipped):
    """Write a page's CSV ONLY if its row count clears the page sanity floor.

    Refuses to emit an empty / suspiciously-small CSV that, if pasted into the
    tab, would wipe a populated universe. Outcomes are recorded in the caller's
    `written` / `skipped` lists as (page, n) tuples. Returns True if written.
    """
    floor = _MIN_EXPECTED.get(page, 1)
    n = len(rows)
    if n < floor:
        print(f"  !! REFUSING to write {page}: got {n} rows (floor {floor}). "
              f"An empty/too-small CSV would wipe a populated tab if pasted, so "
              f"NOT writing {os.path.basename(path)}. Paste NOTHING for this tab.")
        skipped.append((page, n))
        return False
    write_csv(path, rows)
    print(f"  OK  {page}: {n} rows -> {os.path.basename(path)}")
    written.append((page, n))
    return True


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(description="Build dashboard symbol universes from EODHD.")
    ap.add_argument("--out", default="./universes", help="output folder for CSVs")
    ap.add_argument("--max-global", type=int, default=3000, help="cap for Global_Markets")
    ap.add_argument("--max-leaders", type=int, default=0, help="cap for Market_Leaders (0=all)")
    args = ap.parse_args()

    print(f"build_universes v{BUILD_UNIVERSES_VERSION}")

    key = _key()
    if not key:
        print("ERROR: EODHD_API_KEY (or _API_TOKEN/_KEY) is not set in this environment.")
        return 2
    base = _base()
    os.makedirs(args.out, exist_ok=True)
    print(f"EODHD base={base}  out={args.out}")

    written, skipped = [], []

    # ---- Saudi exchange (serves Market_Leaders + Mutual_Funds) -------------- #
    print("Fetching Saudi (SR) symbol list ...")
    try:
        sr = fetch_exchange_symbols("SR", key, base)
    except Exception as e:
        print(f"ERROR fetching SR list: {e}")
        sr = []
    if not sr:
        print("  !! EODHD returned NO Saudi symbols. This plan has zero Saudi (.SR)")
        print("     coverage (exchange-symbol-list/SR -> 'Exchange Not Found'; even")
        print("     Aramco 2222.SR -> 'Ticker Not Found'). Market_Leaders / Mutual_Funds")
        print("     CANNOT be regenerated from EODHD and will be SKIPPED (not written),")
        print("     so your existing tabs are left untouched. Maintain those tabs")
        print("     manually or from a Saudi-native source.")
    leaders = build_from_symbols(sr, "SR", types={"Common Stock"},
                                 max_n=(args.max_leaders or None))
    funds = build_from_symbols(sr, "SR", types={"ETF", "FUND"})
    safe_write_csv(os.path.join(args.out, "universe_Market_Leaders.csv"),
                   leaders, "Market_Leaders", written, skipped)
    safe_write_csv(os.path.join(args.out, "universe_Mutual_Funds.csv"),
                   funds, "Mutual_Funds", written, skipped)

    # ---- Global_Markets: screener (top mkt-cap) then symbol-list fill ------- #
    print(f"Building Global_Markets (target {args.max_global}) ...")
    glob_rows = fetch_screener_top("US", key, base, args.max_global)
    print(f"    screener returned {len(glob_rows)} ranked names")
    glob = build_from_symbols(glob_rows, "US", max_n=args.max_global)
    if len(glob) < args.max_global:
        print("    filling remainder from US symbol-list (main boards only) ...")
        try:
            us = fetch_exchange_symbols("US", key, base)
        except Exception as e:
            print(f"    US symbol-list fetch failed: {e}")
            us = []
        have = {s for s, _ in glob}
        extra = build_from_symbols(us, "US", types={"Common Stock"}, drop_us_otc=True)
        for s, n in extra:
            if s not in have:
                glob.append((s, n))
                have.add(s)
                if len(glob) >= args.max_global:
                    break
    safe_write_csv(os.path.join(args.out, "universe_Global_Markets.csv"),
                   glob, "Global_Markets", written, skipped)

    # ---- Summary + SAFE paste instructions --------------------------------- #
    print(f"\nDone. Output folder: {args.out}")
    if written:
        print("WROTE (safe to paste each file's SYMBOL column into the matching tab,")
        print("row-5 header = SYMBOL):")
        for page, n in written:
            print(f"    {page}: {n} rows -> universe_{page}.csv")
        if any(p == "Global_Markets" for p, _ in written):
            print("IMPORTANT: load Global_Markets incrementally — try ~2500 first, confirm a full")
            print("           scan completes inside the ~100s edge timeout, then raise toward 3000")
            print("           (and raise LEGACY_MAX_SYMBOLS if it caps you below the row count).")
    if skipped:
        print("\n!! SKIPPED (NOT written — do NOT paste anything for these tabs; your")
        print("   existing tabs are intentionally left untouched):")
        for page, n in skipped:
            print(f"    {page}: only {n} rows (below sanity floor {_MIN_EXPECTED.get(page, 1)})")
        print("   Returning non-zero because at least one expected page could not be built.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
