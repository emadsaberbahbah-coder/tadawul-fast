#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_daily_brief.py — "Emad Bahbah — Daily Investment Brief" generator.

WHAT THIS DOES
    Reads the production workbook's decision + opportunity + market pages and renders
    a polished, color-coded HTML brief (the design approved 2026-06-30), then OPTIONALLY
    emails it (--send / TFB_BRIEF_SEND=1) over SMTP. Reading the sheet is READ-ONLY: it
    never writes to the sheet. WhatsApp delivery is a separate, later step.

WHY IT IS SHAPED THIS WAY
    * The same parser runs on (a) live `read_range` output and (b) a downloaded
      `.xlsx`, so the exact output can be verified locally without the live Google
      service-account credentials. CI uses the live path; humans can use the xlsx path.
    * Actions come from the AUTHORITATIVE decision layer (Portfolio_Decision), not the
      raw per-symbol recommendations on My_Portfolio. Portfolio_Decision confidence-gates
      signals and applies position/sector caps, so a low-reliability "SELL" is correctly
      shown as a capped HOLD, and an over-cap holding is shown as TRIM.
    * Opportunity upside is labelled "to fair value (a valuation target)" — never a
      forecast — matching the engine's own advisor-note language. Daily-horizon only;
      no intraday-timing claims.

ENV
    DEFAULT_SPREADSHEET_ID / SPREADSHEET_ID   production workbook id (live mode)
    TFB_BRIEF_OUTPUT_PATH                     output html path (default: ./daily_brief.html)
    TFB_BRIEF_LOCAL_XLSX                      if set, read pages from this xlsx instead of live
    TFB_BRIEF_OWNER_NAME                      masthead/sender name (default: "Emad Bahbah")
    --- delivery (only used with --send) ---
    TFB_MAIL_FROM        sender address (the SMTP login)
    TFB_MAIL_PASSWORD    SMTP app-password
    TFB_MAIL_TO          recipient address (comma-separated for several)
    TFB_MAIL_SMTP_HOST   default: smtp.gmail.com
    TFB_MAIL_SMTP_PORT   default: 587 (STARTTLS); 465 uses implicit SSL
    TFB_BRIEF_SEND       set to "1" to send without passing --send
    TFB_BRIEF_PDF        "1" (default) attach a PDF copy of the recommendations to the
                         email and write it next to --out; "0" disables (kill-switch)

v1.8.0 — MINIMUM TICKET SIZE (plan-level floor at the point of presentation)
--------------------------------------------------------------------------------
WHY (brief of 2026-07-12 19:03): the decision layer ordered "ADD BBD.US 1 sh
(~14 SAR)" — a ticket whose brokerage commission exceeds the position on any
broker. 1050.SR got 98 SAR. Economically meaningless tickets in an executable
plan. The sizing is AUTHORED upstream (Portfolio_Decision's Advisor Note); this
script parses and re-displays it — so the floor is enforced HERE, at the point
where the plan becomes the email the operator executes. The upstream author
gets the same rule in its own revision.

RULE (_enforce_min_ticket, applied inside extract_decision after the parse):
  floor = TFB_MIN_TICKET_SAR (default 200; <=0 disables the floor entirely and
  restores v1.7.0 byte-identically).
  * ADDs with sar >= floor are untouched.
  * Each sub-floor ADD is FOLDED into the largest surviving ADD: cash is
    conserved exactly (target.sar += small.sar); the target's share count is
    re-derived from its own implied price when derivable (price = sar/shares),
    else left as-is with the cash bump; the fold is written into the target's
    `note` — which every renderer (HTML / text / PDF) already prints, so all
    three formats stay consistent with ZERO renderer edits.
  * If ALL ADDs are sub-floor and even their combined cash misses the floor,
    they are dropped and recorded in out["dropped_small"]; the existing
    no-high-confidence-ADD hero path renders (freed cash stays unallocated —
    a 14-SAR order is not a plan).
  Totals: freed_cash comes from SELL/TRIM only and is untouched; the sum of
  ADD cash after folding equals the sum before (conservation asserted in the
  harness on the live Sunday case: 14 + 98 + 1,992 folds to one 2,104-SAR
  ticket).

send_digest.py is deliberately NOT touched: it carries no ticket sizes (its
Best-BUY is a name pick, not a cash amount), so there is nothing to floor.

v1.7.0 — DATA-VINTAGE PROVENANCE + ISSUER-SIBLING FLAGS (evidence: the
  2026-07-10 morning/evening workbook audits + the 13:26 Decision Digest)
  (A) DATA VINTAGE. The pages this brief quotes carried three different
      vintages at once on 2026-07-10 (ML 07-09→07-10 partial 721/897, GM
      2,480×07-08 + 1,187×07-09, CFX/MF 07-09) — and nothing in the email
      disclosed it, so a reader had no way to know a price was two days
      old. New _page_vintage() reads each market page's 'Last Updated
      (Riyadh)' column (UTC fallback) and the model now carries per-page
      date histograms + the newest timestamp. Rendered three ways:
      an amber DATA VINTAGE NOTICE banner at the top of the HTML when the
      newest market-page update is older than TFB_BRIEF_STALE_HOURS
      (default 24) at send time; a per-page 'Data: MM-DD×n · MM-DD×n'
      line inside the market-pages strip; and a one-line DATA VINTAGE
      summary in the plaintext part. Fail-safe: a page without the column
      contributes nothing and nothing renders (v1.6.0 output byte-
      identical). PDF unchanged by design (space-constrained card grid).
  (B) ISSUER-SIBLING FLAGS. The 13:26 digest recommended BUY BBDO.US while
      the book held BBD.US — same issuer (Banco Bradesco S.A.), different
      ADR share class; the brief's own candidate lists can surface the
      same trap (its held-exclusion is symbol-exact). build_model now
      derives {normalized issuer name -> held symbols} from My_Portfolio
      and annotates every candidate (top buys, rest-of-markets, per-page
      top names) whose NAME matches a held issuer under a DIFFERENT
      symbol. Rendered as an amber '⚠ Same issuer as held X — different
      share class' line on the buy card, a '(sibling of held X)' suffix
      in the rest-of-markets strip, a sentence in the market-pages strip,
      and a '[same issuer as held X]' suffix in the plaintext buys list.
      Name-blank rows never match; held symbols themselves are never
      flagged. New helpers: _stale_hours_threshold, _page_vintage,
      _issuer_key, _held_issuers (4 added, 0 removed); _opp_row/_rest_row/
      _page_row/build_model/render_html/render_text extended; everything
      else verbatim v1.6.0.

v1.6.0 — DISPLAY-CORRECTNESS FIVE-PACK (evidence: live email/PDF of
  2026-07-08 23:21 vs workbook export v37; every fix verified against both)
- B1 KILL VALUE-SNIFFING (the +150%/-100%/-140% bug): _pct(), the
  _glance_block bar scaler and _roi_color multiplied any |x|<=1.5 by 100 on
  the guess it was a fraction. In production it never is: read_pages_live
  returns Sheets FORMATTED values ("1.49%" -> 1.49 percent-POINTS via _num's
  %-strip), so the three true small P&Ls (1050.SR +1.49, 1180.SR -1.03,
  5023.SR -1.37) rendered as +150.0/-100.0/-140.0 and stretched every bar
  to a 150% scale. All three sites now treat inputs as POINTS, period.
- B1b XLSX PARITY: the raw workbook stores those same columns as FRACTIONS
  (0.0149), so the offline --xlsx verification path silently disagreed with
  the live path — the origin of the sniffing. read_pages_xlsx now normalizes
  the known percent-formatted columns (x100) at load so offline == live and
  no renderer ever needs to guess units again.
- B2 NATIVE-CURRENCY LABEL (PDF section 2): the Value column printed
  Position Value in the row's native currency with no label next to a SAR
  book total (FER 198, RCI 1,624, BBD 849 are USD -> RCI looked like 5.7%
  of the book while it is 21.2%). Metrics now capture the Currency column
  and the cell renders "1,624 USD" for non-SAR rows.
- B3 TOP-NAMES CLEANUP ("Blackrock, Banco, The, East" / "Innoviva,,
  Regency, Regency, ... WesBanco,."): the note used name.split()[0]
  verbatim (leading articles, trailing commas) and never deduped invest
  rows. extract_market_page now dedupes by symbol (best Opportunity Score
  wins) and _short_name() strips punctuation, drops a leading "The", takes
  up to two words (cap 18 chars) and falls back to the SYMBOL when the
  name cell is blank.
- B4 ONE-MOVE vs DECISION-LAYER CONTRADICTION: the hero said "add BBD.US,
  ~1,422 SAR stays dry powder" while the decision layer itself deployed
  2,482 across THREE adds. Hero, preheader, subject and the plaintext ONE
  MOVE now report ALL funded adds with their total and the true remainder.
- B5 ONE RELIABILITY PER CARD: the opportunity card header showed the
  Top_10 screen Rel (88) while its own outlook strip showed the forecast
  path reliability (85). The header now uses the SAME path reliability as
  the strip when available (Top_10 Rel is the fallback), so a card never
  argues with itself.
- New helpers: _short_name, _val_ccy, _xlsx_normalize_units,
  _XLSX_FRACTION_HEADERS. No functions removed; everything else
  byte-identical to v1.5.0.

v1.5.0 — PDF REDESIGN + PORTFOLIO FINANCIALS (owner request, 2026-07-05)
    * PDF fully redesigned: masthead band matching the email (ink/brass), a
      KPI strip (book value, CURRENT ROI, freed cash, holdings, new buys),
      right-aligned numerics, zebra tables, and page footers with numbers.
    * NEW 'Portfolio — current ROI & key ratios' table: one row per holding
      with Qty / Avg cost / Value / CURRENT ROI (Unrealized P/L %) next to
      the engine's Expected ROI 12M, plus P/E, P/B, D/E, Dividend Yield and
      Beta from the 115-column schema. Current ROI is colored red/green.
    * NEW 'Income-statement snapshot (TTM)' table per holding: Revenue,
      Revenue growth YoY, Gross/Operating/Net margins, EPS, Free Cash Flow.
      HONEST SCOPE: these are the income-statement LINE ITEMS available on
      the sheets; a full revenue->net-income statement is not on the
      115-column schema (backend work if ever wanted).
    * BUG FIX unlocking the above: ALL_PAGES never included My_Portfolio,
      so v1.3.0's 'My_Portfolio wins' metric scan was a silent no-op (the
      page was scanned but never fetched). My_Portfolio is now fetched
      (both live and xlsx paths already tolerate a missing/failed page),
      and extract_symbol_metrics() additionally collects Revenue (TTM),
      Revenue Growth YoY, Gross/Operating margins, P/E (TTM/Fwd), P/B, P/S,
      EV/EBITDA, PEG, Debt/Equity, FCF (TTM), Beta (5Y), Volatility 30D,
      Sharpe (1Y), Payout Ratio, and the position columns Unrealized P/L %,
      Position Qty, Avg Cost, Position Value. Missing values render as
      dashes — never a broken layout. HTML email is unchanged (approved
      design); the PDF is the deep-data artifact.

v1.4.0 — PDF ATTACHMENT + END-OF-BRIEF ACTION SUMMARY (owner request, 2026-07-05)
    * Every sent brief now ATTACHES a PDF copy of the recommendations:
      portfolio actions table, best-new-buys table, and an ACTION SUMMARY
      table at the end (counts, symbols, cash impact, book totals). Built
      with reportlab (pure-python; installed by the workflow, NOT added to
      Render requirements). Guarded end to end: if reportlab is missing or
      the PDF build fails, the email still sends unchanged — delivery of
      the brief is never blocked by the attachment.
    * The same ACTION SUMMARY table is appended near the end of the HTML
      email (before "When you can act") and to the plaintext part, so all
      three formats close with the same at-a-glance decision table.
    * Kill-switch: TFB_BRIEF_PDF=0 disables PDF generation + attachment.
      Honest-framing language ("valuation targets, not predictions") is
      carried into the PDF verbatim. All v1.3.0/v1.2.0 behavior unchanged.

v1.3.0 — DECISION-GRADE VISUAL UPGRADE (owner request, 2026-07-03)
    Adds email-safe visual analytics built ONLY from columns verified 100%%
    populated in the live workbook (audit of the 2026-07-03 export):
      * 1M / 3M / 12M OUTLOOK STRIP under every ADD action and every Best-new-buy
        row: Expected ROI 1M/3M/12M + Forecast Price 1M/3M/12M + reliability,
        color-coded. Framed honestly as the engine's valuation path — targets
        with stated reliability, never predictions (per the TFB honest-framing
        constraint; flow/candle signals carry no proven forward edge).
      * "YOUR BOOK AT A GLANCE": a diverging P&L bar chart (one bar per holding,
        red left / green right of a zero axis, scaled to the largest move) plus
        each holding's weight — the fastest possible read of where the pain and
        the winners sit.
      * FUNDAMENTALS SNAPSHOT per Best-new-buy: EPS (TTM), Market Cap (compact
        T/B/M), Dividend Yield, Profit Margin — the income-statement-level facts
        available on the 115-column schema today (a full revenue->net-income
        statement is NOT on the sheets; that would be backend work).
      * 52-WEEK RANGE BAR per Best-new-buy: where today's price sits in the
        52W range (position marker on a track).
      * MARKET BREADTH BAR in the per-market strip (investable share of names).
    TECHNIQUE: all "charts" are pure HTML/CSS table bars — email clients strip
    JavaScript and SVG (Gmail especially), so scripted/vector charts silently
    vanish; nested-table bars render in every major client with zero new
    dependencies and no image attachments. New model layer:
    extract_symbol_metrics() builds a symbol -> metrics lookup from My_Portfolio
    + the four market pages (My_Portfolio wins on conflict); render helpers
    _outlook_strip/_glance_block/_range52_bar/_fund_line/_mcap_compact/
    _num_str2/_roi_color. Plaintext part gains matching 1M/3M lines. Every
    existing section, the approved masthead design, and the v1.2.0 hero
    variants are carried verbatim.

v1.2.0 — NO-CANDIDATE HERO FIX
    When the decision layer produced no high-confidence ADD, the HTML hero still
    rendered the buy-day template with the em-dash placeholder: "then add —" /
    "Put ~0 SAR into —, the one buy the engine backs at high confidence" (and
    the hidden inbox preheader read "add —"). The plaintext part and the subject
    line already branched on the no-add case; the HTML hero now branches the
    same way (three variants: buy day / free-cash-only day / no-action day),
    and the exit/trim sentence is assembled from only the NON-EMPTY clauses so
    an empty sell or trim list can no longer print "Exit the weak names (—)" or
    "trim your overweight — back to cap". Rendering for a normal buy day is
    unchanged. No contract, ENV, or CLI change.

USAGE
    python run_daily_brief.py                         # live (CI): read sheet, write html
    python run_daily_brief.py --send                  # live: read sheet, write html, email it
    python run_daily_brief.py --xlsx file.xlsx        # local verification against a snapshot
    python run_daily_brief.py --xlsx file.xlsx --out brief.html
"""
from __future__ import annotations

__version__ = "1.8.0"

import argparse
import datetime as _dt
import html as _html
import io
import math
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

# ----------------------------------------------------------------------------- #
# Page names (override-able only via code; stable by design)
# ----------------------------------------------------------------------------- #
PAGE_DECISION = "Portfolio_Decision"
PAGE_TOP10 = "Top_10_Investments"
PAGE_PORTFOLIO = "My_Portfolio"  # v1.5.0: fetched for position ROI + ratios
MARKET_PAGES = ["Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds"]
ALL_PAGES = [PAGE_DECISION, PAGE_TOP10, PAGE_PORTFOLIO] + MARKET_PAGES

VALID_ACTIONS = {"EXIT", "SELL", "TRIM", "REDUCE", "ADD", "BUY", "HOLD"}
SELL_ACTIONS = {"EXIT", "SELL"}
TRIM_ACTIONS = {"TRIM", "REDUCE"}
ADD_ACTIONS = {"ADD", "BUY"}

# ----------------------------------------------------------------------------- #
# Small value helpers
# ----------------------------------------------------------------------------- #
def _s(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and math.isnan(v):
        return ""
    return str(v).strip()


def _num(v: Any) -> Optional[float]:
    s = _s(v)
    if not s:
        return None
    for t in ("%", "▲", "▼", ",", "+", "SAR", "USD"):
        s = s.replace(t, "")
    s = s.strip()
    try:
        return float(s)
    except ValueError:
        return None


def _pct(x: Optional[float], digits: int = 1) -> str:
    """Render a percent-POINTS value (1.49 -> "+1.5%"). NO unit guessing.

    v1.6.0 (B1): the old |x|<=1.5 fraction heuristic turned the three real
    small P&Ls of 2026-07-08 (+1.49 / -1.03 / -1.37) into +150/-100/-140 in
    the sent brief. In production every percent-like cell arrives as POINTS:
    read_pages_live returns Sheets FORMATTED values ("1.49%") and _num strips
    the % sign. The offline xlsx path is normalized to the same convention in
    read_pages_xlsx (_xlsx_normalize_units), so callers never pass fractions.
    """
    if x is None:
        return "—"
    return f"{x:+.{digits}f}%"


def _money(x: Optional[float]) -> str:
    if x is None:
        return "—"
    return f"{x:,.0f}"


def _esc(v: Any) -> str:
    return _html.escape(_s(v), quote=True)


def _pad(rows: List[List[Any]]) -> List[List[Any]]:
    width = max((len(r) for r in rows), default=0)
    return [list(r) + [None] * (width - len(r)) for r in rows]


def _find_header(rows: List[List[Any]], keywords: Tuple[str, ...], scan: int = 40) -> int:
    """Return the index of the first row containing all `keywords` (case-insensitive)."""
    kws = [k.lower() for k in keywords]
    for i in range(min(scan, len(rows))):
        cells = [_s(c).lower() for c in rows[i]]
        if all(any(k == c or k in c for c in cells) for k in kws):
            return i
    return -1


def _col_index(header: List[Any], *names: str) -> Optional[int]:
    norm = [_s(h).lower() for h in header]
    for n in names:
        nl = n.lower()
        for i, h in enumerate(norm):
            if h == nl:
                return i
    # loose contains-match as a fallback
    for n in names:
        nl = n.lower()
        for i, h in enumerate(norm):
            if nl in h:
                return i
    return None


def _cell(row: List[Any], idx: Optional[int]) -> Any:
    if idx is None or idx < 0 or idx >= len(row):
        return None
    return row[idx]


# ----------------------------------------------------------------------------- #
# Page reading: live (read_range) and local (xlsx) — both yield List[List]
# ----------------------------------------------------------------------------- #
def read_pages_live(spreadsheet_id: str, pages: List[str]) -> Dict[str, List[List[Any]]]:
    from integrations.google_sheets_service import read_range  # local import: CI only

    out: Dict[str, List[List[Any]]] = {}
    for p in pages:
        try:
            out[p] = read_range(spreadsheet_id, f"{p}!A1:DZ5000") or []
        except Exception as exc:  # one bad page must not kill the brief
            sys.stderr.write(f"[brief] WARN could not read {p}: {exc}\n")
            out[p] = []
    return out


# v1.6.0 (B1b): columns the WORKBOOK stores as FRACTIONS (0.0149) but the
# live Sheets API delivers as formatted percent-POINTS ("1.49%" -> 1.49).
# read_pages_xlsx multiplies these by 100 at load so the offline path is
# byte-equivalent to production and no renderer ever unit-guesses again.
# (Verified per page against export v37: identical fraction storage on
# My_Portfolio and all four market pages; Decision/Top_10 percent columns
# are already points in the raw file and are NOT listed here.)
_XLSX_FRACTION_HEADERS = frozenset({
    "dividend yield", "profit margin", "gross margin", "operating margin",
    "revenue growth yoy", "payout ratio", "expected roi 1m",
    "expected roi 3m", "expected roi 12m", "unrealized p/l %",
    "volatility 30d", "volatility 90d",
})


def _xlsx_normalize_units(rows: List[List[Any]]) -> List[List[Any]]:
    """x100 the known fraction-stored percent columns (numeric cells only)."""
    if not rows:
        return rows
    h = _find_header(rows, ("symbol", "name"))
    if h < 0:
        return rows
    hdr = rows[h]
    idxs = [i for i, c in enumerate(hdr)
            if _s(c).strip().lower() in _XLSX_FRACTION_HEADERS]
    if not idxs:
        return rows
    for r in rows[h + 1:]:
        if not isinstance(r, list):
            continue
        for i in idxs:
            if i < len(r) and isinstance(r[i], (int, float)) and not isinstance(r[i], bool):
                r[i] = r[i] * 100.0
    return rows


def read_pages_xlsx(path: str, pages: List[str]) -> Dict[str, List[List[Any]]]:
    import pandas as pd  # local import: verification path only

    xl = pd.ExcelFile(path)
    out: Dict[str, List[List[Any]]] = {}
    for p in pages:
        if p in xl.sheet_names:
            df = pd.read_excel(path, sheet_name=p, header=None, dtype=object)
            out[p] = _xlsx_normalize_units(df.where(df.notna(), None).values.tolist())
        else:
            out[p] = []
    return out


# ----------------------------------------------------------------------------- #
# Extractors
# ----------------------------------------------------------------------------- #
def extract_decision(rows: List[List[Any]]) -> Dict[str, Any]:
    """Parse Portfolio_Decision -> {actions, totals, freed_cash, add}."""
    rows = _pad(rows)
    h = _find_header(rows, ("action", "symbol"))
    out: Dict[str, Any] = {"sell": [], "trim": [], "add": [], "hold": [],
                           "cost": 0.0, "mv": 0.0, "freed_cash": 0.0}
    if h < 0:
        return out
    hdr = rows[h]
    ci = {
        "action": _col_index(hdr, "Action"),
        "symbol": _col_index(hdr, "Symbol"),
        "name": _col_index(hdr, "Name"),
        "pl": _col_index(hdr, "P&L %", "PnL %", "P&L%"),
        "weight": _col_index(hdr, "Weight %"),
        "eroi": _col_index(hdr, "Engine ROI %", "Expected ROI 12M"),
        "rel": _col_index(hdr, "Rel", "Forecast Reliability Score"),
        "conf": _col_index(hdr, "Conf", "Confidence Bucket"),
        "note": _col_index(hdr, "Advisor Note"),
        "mv": _col_index(hdr, "MV SAR"),
        "cost": _col_index(hdr, "Cost SAR"),
    }
    for r in rows[h + 1:]:
        action = _s(_cell(r, ci["action"])).upper()
        symbol = _s(_cell(r, ci["symbol"]))
        if action not in VALID_ACTIONS or not symbol or symbol.lower() == "symbol":
            # past the holdings block (section tables follow) -> stop
            if out["sell"] or out["trim"] or out["add"] or out["hold"]:
                break
            continue
        note = _s(_cell(r, ci["note"]))
        rec = {
            "symbol": symbol,
            "name": _s(_cell(r, ci["name"])),
            "pl": _num(_cell(r, ci["pl"])),
            "weight": _num(_cell(r, ci["weight"])),
            "eroi": _num(_cell(r, ci["eroi"])),
            "rel": _num(_cell(r, ci["rel"])),
            "conf": _s(_cell(r, ci["conf"])),
            "note": note,
            "sar": _note_sar(note),
        }
        mv, cost = _num(_cell(r, ci["mv"])), _num(_cell(r, ci["cost"]))
        if mv is not None:
            out["mv"] += mv
        if cost is not None:
            out["cost"] += cost
        if action in SELL_ACTIONS:
            out["sell"].append(rec)
            out["freed_cash"] += rec["sar"] or 0.0
        elif action in TRIM_ACTIONS:
            out["trim"].append(rec)
            out["freed_cash"] += rec["sar"] or 0.0
        elif action in ADD_ACTIONS:
            rec["shares"] = _note_shares(note)
            out["add"].append(rec)  # v1.8.0: floored post-loop by _enforce_min_ticket
        else:
            out["hold"].append(rec)
    out["pl_pct"] = ((out["mv"] - out["cost"]) / out["cost"] * 100.0) if out["cost"] else None
    # v1.8.0: minimum executable ticket (default 200 SAR; TFB_MIN_TICKET_SAR=0 disables)
    out["min_ticket"] = _min_ticket_sar()
    out["add"], out["folded"], out["dropped_small"] = _enforce_min_ticket(out["add"])
    return out


def _min_ticket_sar() -> float:
    """v1.8.0: minimum executable ADD ticket in SAR. Default 200.
    TFB_MIN_TICKET_SAR=0 (or any value <= 0) disables the floor and restores
    the v1.7.0 pass-through byte-identically."""
    raw = (os.getenv("TFB_MIN_TICKET_SAR") or "").strip()
    try:
        v = float(raw) if raw else 200.0
    except Exception:
        v = 200.0
    return v


def _enforce_min_ticket(adds: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Tuple[str, float, str]], List[Dict[str, Any]]]:
    """v1.8.0: fold sub-floor ADD tickets into the largest surviving ADD.

    Returns (kept_adds, folded, dropped_small) where folded is a list of
    (from_symbol, sar, into_symbol). Cash is conserved: sum(sar) over kept ==
    sum(sar) over input (unless everything is dropped). The fold is appended to
    the target's `note`, which every renderer already prints."""
    floor = _min_ticket_sar()
    if floor <= 0 or not adds:
        return adds, [], []
    keep = [r for r in adds if (r.get("sar") or 0.0) >= floor]
    small = [r for r in adds if (r.get("sar") or 0.0) < floor]
    if not small:
        return adds, [], []
    if not keep:
        # promote the largest small and try folding the rest into it
        small.sort(key=lambda r: r.get("sar") or 0.0, reverse=True)
        keep, small = [small[0]], small[1:]
    folded: List[Tuple[str, float, str]] = []
    target = max(keep, key=lambda r: r.get("sar") or 0.0)
    t_sar = target.get("sar") or 0.0
    t_sh = target.get("shares")
    price = (t_sar / t_sh) if (t_sh and t_sar > 0) else None
    for r in small:
        s = r.get("sar") or 0.0
        t_sar += s
        folded.append((r.get("symbol") or "?", s, target.get("symbol") or "?"))
    if folded:
        target["sar"] = t_sar
        if price and price > 0:
            target["shares"] = max(1, int(round(t_sar / price)))
        parts = ", ".join(f"+{s:,.0f} SAR from {sym}" for sym, s, _ in folded)
        target["note"] = ((target.get("note") or "").rstrip() +
                          f" | v1.8.0 min-ticket fold ({_min_ticket_sar():,.0f} SAR floor): {parts}").strip(" |")
    if (target.get("sar") or 0.0) < floor:
        # even the folded total misses the floor: a 14-SAR order is not a plan
        return [], [], keep + small if isinstance(small, list) else keep
    return keep, folded, []


def _note_sar(note: str) -> float:
    m = re.search(r"~?\s*([\d,]+(?:\.\d+)?)\s*SAR", note)
    return float(m.group(1).replace(",", "")) if m else 0.0


def _note_shares(note: str) -> Optional[int]:
    m = re.search(r"ADD\s+([\d,]+)\s*sh", note, re.IGNORECASE)
    return int(m.group(1).replace(",", "")) if m else None


def _infer_market(symbol: str, ccy: str = "", country: str = "") -> str:
    s = symbol.upper()
    if s.endswith(".SR") or country.lower().startswith("saudi"):
        return "Saudi"
    if s.endswith(".T"):
        return "Tokyo"
    if s.endswith(".HK"):
        return "Hong Kong"
    if s.endswith(".KW"):
        return "Kuwait"
    if s.endswith(".NSE"):
        return "India"
    if any(s.endswith(x) for x in (".PA", ".AS", ".MC", ".DE", ".L", ".XETRA", ".TO")):
        return "Europe"
    if s.endswith(".US") or "." not in s:
        return "United States"
    return _s(country) or "Global"


# ----------------------------------------------------------------------------- #
# v1.7.0 (A/B) — data-vintage + issuer-sibling helpers
# ----------------------------------------------------------------------------- #
_VINTAGE_COLS = ("Last Updated (Riyadh)", "Last Updated (UTC)", "Last Updated")
_PAGE_ABBREV = {"Market_Leaders": "ML", "Global_Markets": "GM",
                "Commodities_FX": "CFX", "Mutual_Funds": "MF"}


def _stale_hours_threshold() -> float:
    """v1.7.0 (A): banner threshold in hours (TFB_BRIEF_STALE_HOURS,
    default 24, floor 1). Unparseable values fall back to 24."""
    try:
        return max(1.0, float(os.getenv("TFB_BRIEF_STALE_HOURS") or "24"))
    except Exception:
        return 24.0


def _page_vintage(rows: List[List[Any]]) -> Dict[str, Any]:
    """v1.7.0 (A): per-page data vintage from the 'Last Updated (Riyadh)'
    column (UTC/plain fallbacks). Returns {"dates": [(YYYY-MM-DD, count)…
    dominant-first], "latest_ts": datetime|None, "total": rows counted}.
    Fail-safe: a page without the column (or without parseable stamps)
    yields the empty shape and nothing renders downstream."""
    rows = _pad(rows)
    out: Dict[str, Any] = {"dates": [], "latest_ts": None, "total": 0}
    h = _find_header(rows, ("symbol", "name"))
    if h < 0:
        return out
    ci = _col_index(rows[h], *_VINTAGE_COLS)
    si = _col_index(rows[h], "Symbol")
    if ci is None:
        return out
    counts: Dict[str, int] = {}
    latest: Optional[_dt.datetime] = None
    n = 0
    for r in rows[h + 1:]:
        if si is not None and not _s(_cell(r, si)):
            continue
        raw = _s(_cell(r, ci))
        if not re.match(r"^\d{4}-\d{2}-\d{2}", raw):
            continue
        d = raw[:10]
        counts[d] = counts.get(d, 0) + 1
        n += 1
        ts: Optional[_dt.datetime] = None
        try:
            ts = _dt.datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            try:
                ts = _dt.datetime.strptime(d, "%Y-%m-%d")
            except Exception:
                ts = None
        if ts is not None and (latest is None or ts > latest):
            latest = ts
    out["dates"] = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    out["latest_ts"] = latest
    out["total"] = n
    return out


def _issuer_key(name: Any) -> str:
    """v1.7.0 (B): normalized issuer-name key — lowercase, alphanumerics
    only, so 'Banco Bradesco S.A.' and 'BANCO BRADESCO SA' collide."""
    return re.sub(r"[^a-z0-9]+", "", _s(name).lower())


def _held_issuers(pages_data: Dict[str, List[List[Any]]]) -> Tuple[set, Dict[str, List[str]]]:
    """v1.7.0 (B): (held symbol set, issuer key -> sorted held symbols)
    from the My_Portfolio page (post-10 v1.5.9 this IS the ACTIVE ledger
    book). Blank names contribute no issuer key. Fail-safe: an unreadable
    page yields (set(), {}) and no candidate is ever flagged."""
    rows = _pad(pages_data.get(PAGE_PORTFOLIO, []) or [])
    syms: set = set()
    names: Dict[str, List[str]] = {}
    h = _find_header(rows, ("symbol", "name"))
    if h < 0:
        return syms, names
    si = _col_index(rows[h], "Symbol")
    ni = _col_index(rows[h], "Name")
    for r in rows[h + 1:]:
        sym = _s(_cell(r, si)).upper()
        if not sym or sym == "SYMBOL":
            continue
        syms.add(sym)
        k = _issuer_key(_cell(r, ni))
        if k:
            names.setdefault(k, []).append(sym)
    return syms, {k: sorted(set(v)) for k, v in names.items()}


def extract_top10(rows: List[List[Any]], exclude: Optional[set] = None,
                  top_n: int = 5) -> Dict[str, Any]:
    """Parse Top_10_Investments funded picks (Advisor Note starts with INVEST)."""
    rows = _pad(rows)
    exclude = {e.upper() for e in (exclude or set())}
    h = _find_header(rows, ("rank", "symbol"))
    if h < 0:
        h = _find_header(rows, ("symbol", "advisor note"))
    picks: List[Dict[str, Any]] = []
    if h < 0:
        return {"top": [], "rest": {}}
    hdr = rows[h]
    ci = {
        "symbol": _col_index(hdr, "Symbol"),
        "name": _col_index(hdr, "Name"),
        "sector": _col_index(hdr, "Sector"),
        "ccy": _col_index(hdr, "Ccy", "Currency"),
        "market": _col_index(hdr, "Market"),
        "roi": _col_index(hdr, "ROI %", "Upside %"),
        "rel": _col_index(hdr, "Rel", "Forecast Reliability Score"),
        "conf": _col_index(hdr, "Conf", "Confidence Bucket"),
        "note": _col_index(hdr, "Advisor Note"),
    }
    for r in rows[h + 1:]:
        note = _s(_cell(r, ci["note"]))
        symbol = _s(_cell(r, ci["symbol"]))
        if not symbol:
            continue
        if not note.upper().startswith("INVEST"):  # funded, executable tickets only
            continue
        if symbol.upper() in exclude:
            continue
        picks.append({
            "symbol": symbol,
            "name": _s(_cell(r, ci["name"])),
            "sector": _s(_cell(r, ci["sector"])),
            "market": _infer_market(symbol, _s(_cell(r, ci["ccy"])), _s(_cell(r, ci["market"]))),
            "roi": _num(_cell(r, ci["roi"])),
            "rel": _num(_cell(r, ci["rel"])),
            "conf": _s(_cell(r, ci["conf"])) or "—",
        })
    top = picks[:top_n]
    rest: Dict[str, List[Dict[str, Any]]] = {}
    for p in picks[top_n:]:
        rest.setdefault(p["market"], []).append(p)
    return {"top": top, "rest": rest}


def extract_market_page(rows: List[List[Any]], top_n: int = 5) -> Dict[str, Any]:
    """Parse a market page -> {total, invest, top INVEST names}."""
    rows = _pad(rows)
    h = _find_header(rows, ("symbol", "final action"))
    if h < 0:
        h = _find_header(rows, ("symbol", "name"))
    if h < 0:
        return {"total": 0, "invest": 0, "top": []}
    hdr = rows[h]
    ci = {
        "symbol": _col_index(hdr, "Symbol"),
        "name": _col_index(hdr, "Name"),
        "country": _col_index(hdr, "Country"),
        "fa": _col_index(hdr, "Final Action"),
        "opp": _col_index(hdr, "Opportunity Score"),
    }
    total = 0
    invest: List[Dict[str, Any]] = []
    for r in rows[h + 1:]:
        sym = _s(_cell(r, ci["symbol"]))
        if not sym or sym.lower() == "symbol":
            continue
        total += 1
        if _s(_cell(r, ci["fa"])).upper() == "INVEST":
            invest.append({
                "symbol": sym,
                "name": _s(_cell(r, ci["name"])),
                "country": _s(_cell(r, ci["country"])),
                "opp": _num(_cell(r, ci["opp"])) or 0.0,
            })
    invest.sort(key=lambda d: d["opp"], reverse=True)
    # v1.6.0 (B3): the poisoned/duplicated universe pages can carry the SAME
    # company twice — either the literal symbol repeated, or the dual-listing
    # convention pair (REG and REG.US both present; 82 such pairs counted on
    # Market_Leaders in export v37) — which printed "Regency, Regency" in the
    # 2026-07-08 brief. Dedupe AFTER the sort on the BASE symbol (a trailing
    # ".US" stripped; other exchange suffixes are real distinct listings and
    # are never touched) so the best Opportunity Score row represents it.
    seen: set = set()
    deduped: List[Dict[str, Any]] = []
    for d_ in invest:
        k = d_["symbol"].upper()
        if k.endswith(".US"):
            k = k[:-3]
        if k in seen:
            continue
        seen.add(k)
        deduped.append(d_)
    return {"total": total, "invest": len(deduped), "top": deduped[:top_n]}


def extract_symbol_metrics(pages_data: Dict[str, List[List[Any]]]) -> Dict[str, Dict[str, Any]]:
    """v1.3.0: symbol -> metrics lookup for the visual sections.

    Scans My_Portfolio FIRST (so a held name's own row wins), then the four
    market pages, and collects the columns the 2026-07-03 export audit showed
    fully populated: price, Expected ROI 1M/3M/12M, Forecast Price 1M/3M/12M,
    reliability, EPS (TTM), Market Cap, Dividend Yield, Profit Margin, 52W
    High/Low, Sector. Fail-safe: unreadable pages/columns simply contribute
    nothing; a missing symbol renders no strip (never a broken layout)."""
    out: Dict[str, Dict[str, Any]] = {}
    numeric = ("price", "roi1m", "roi3m", "roi12m", "fp1m", "fp3m", "fp12m",
               "rel", "eps", "mcap", "divy", "margin", "hi52", "lo52",
               # v1.5.0: ratios + income-statement line items + position columns
               "rev", "revg", "gm", "om", "pe", "pef", "pb", "ps", "ev", "peg",
               "de", "fcf", "beta", "vol30", "sharpe", "payout",
               "uplpct", "qty", "avgcost", "pval")
    for page in [PAGE_PORTFOLIO] + MARKET_PAGES:
        rows = _pad(pages_data.get(page, []) or [])
        h = _find_header(rows, ("symbol", "name"))
        if h < 0:
            continue
        hdr = rows[h]
        ci = {
            "symbol": _col_index(hdr, "Symbol"),
            "price": _col_index(hdr, "Current Price"),
            "roi1m": _col_index(hdr, "Expected ROI 1M"),
            "roi3m": _col_index(hdr, "Expected ROI 3M"),
            "roi12m": _col_index(hdr, "Expected ROI 12M"),
            "fp1m": _col_index(hdr, "Forecast Price 1M"),
            "fp3m": _col_index(hdr, "Forecast Price 3M"),
            "fp12m": _col_index(hdr, "Forecast Price 12M"),
            "rel": _col_index(hdr, "Forecast Reliability Score"),
            "eps": _col_index(hdr, "EPS (TTM)", "EPS"),
            "mcap": _col_index(hdr, "Market Cap"),
            "divy": _col_index(hdr, "Dividend Yield"),
            "margin": _col_index(hdr, "Profit Margin"),
            "hi52": _col_index(hdr, "52W High"),
            "lo52": _col_index(hdr, "52W Low"),
            # v1.5.0: ratios + income-statement line items + position columns
            "rev": _col_index(hdr, "Revenue (TTM)"),
            "revg": _col_index(hdr, "Revenue Growth YoY"),
            "gm": _col_index(hdr, "Gross Margin"),
            "om": _col_index(hdr, "Operating Margin"),
            "pe": _col_index(hdr, "P/E (TTM)"),
            "pef": _col_index(hdr, "P/E (Forward)"),
            "pb": _col_index(hdr, "P/B"),
            "ps": _col_index(hdr, "P/S"),
            "ev": _col_index(hdr, "EV/EBITDA"),
            "peg": _col_index(hdr, "PEG"),
            "de": _col_index(hdr, "Debt/Equity"),
            "fcf": _col_index(hdr, "Free Cash Flow (TTM)"),
            "beta": _col_index(hdr, "Beta (5Y)"),
            "vol30": _col_index(hdr, "Volatility 30D"),
            "sharpe": _col_index(hdr, "Sharpe (1Y)"),
            "payout": _col_index(hdr, "Payout Ratio"),
            "uplpct": _col_index(hdr, "Unrealized P/L %"),
            "qty": _col_index(hdr, "Position Qty"),
            "avgcost": _col_index(hdr, "Avg Cost"),
            "pval": _col_index(hdr, "Position Value"),
            "sector": _col_index(hdr, "Sector"),
            # v1.6.0 (B2): needed to label native-currency Position Values.
            "ccy": _col_index(hdr, "Currency"),
        }
        if ci["symbol"] is None:
            continue
        for r in rows[h + 1:]:
            sym = _s(_cell(r, ci["symbol"])).upper()
            if not sym or sym == "SYMBOL":
                continue
            if sym in out:
                continue
            m: Dict[str, Any] = {}
            for k in numeric:
                m[k] = _num(_cell(r, ci[k]))
            m["sector"] = _s(_cell(r, ci["sector"]))
            m["ccy"] = _s(_cell(r, ci["ccy"])).upper()  # v1.6.0 (B2)
            out[sym] = m
    return out


# ----------------------------------------------------------------------------- #
# Build model (parse all pages)
# ----------------------------------------------------------------------------- #
def build_model(pages_data: Dict[str, List[List[Any]]]) -> Dict[str, Any]:
    decision = extract_decision(pages_data.get(PAGE_DECISION, []))
    held = {r["symbol"].upper() for grp in ("sell", "trim", "add", "hold") for r in decision[grp]}
    top10 = extract_top10(pages_data.get(PAGE_TOP10, []), exclude=held)
    market = {p: extract_market_page(pages_data.get(p, [])) for p in MARKET_PAGES}
    metrics = extract_symbol_metrics(pages_data)  # v1.3.0 visual sections
    # v1.7.0 (A): per-page data vintage for provenance rendering.
    vintage = {p: _page_vintage(pages_data.get(p, []) or []) for p in MARKET_PAGES}
    # v1.7.0 (B): flag candidates whose ISSUER is already held under a
    # DIFFERENT symbol (share-class siblings, e.g. BBDO.US vs held BBD.US).
    held_syms, held_names = _held_issuers(pages_data)
    held_all = held | held_syms

    def _annotate(items: List[Dict[str, Any]]) -> None:
        for it in items:
            sym = _s(it.get("symbol")).upper()
            k = _issuer_key(it.get("name"))
            if not k or not sym or sym in held_all:
                continue
            sibs = [x for x in held_names.get(k, []) if x != sym]
            if sibs:
                it["sibling"] = ", ".join(sibs)

    _annotate(top10["top"])
    for _names_ in top10["rest"].values():
        _annotate(_names_)
    for _pg_ in MARKET_PAGES:
        _annotate(market[_pg_]["top"])
    return {"decision": decision, "top10": top10, "market": market,
            "metrics": metrics, "vintage": vintage}


# ----------------------------------------------------------------------------- #
# Render (matches the approved 2026-06-30 design)
# ----------------------------------------------------------------------------- #
INK = "#14213D"; BRASS = "#B8995A"
SELL_C = "#B23A2E"; TRIM_C = "#C57B1E"; ADD_C = "#128A5E"; HOLD_C = "#5B6B7A"; STEEL = "#2C5F7C"
SANS = "-apple-system,'Segoe UI',Helvetica,Arial,sans-serif"
SERIF = "Georgia,'Times New Roman',serif"


def _pill(label: str, color: str) -> str:
    return (f'<span style="display:inline-block; background:{color}; color:#fff; '
            f'font-family:{SANS}; font-size:10px; font-weight:bold; letter-spacing:1px; '
            f'padding:4px 8px; border-radius:3px;">{_esc(label)}</span>')


def _action_row(rec: Dict[str, Any], label: str, color: str, bg: str, border: str,
                reason: str, right_top: str, right_bot: str, rt_color: str) -> str:
    return f"""<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:7px; background:{bg}; border:1px solid {border};"><tr>
      <td style="padding:12px 14px; vertical-align:top; width:62px;">{_pill(label, color)}</td>
      <td style="padding:12px 8px; font-family:{SANS};">
        <div style="font-size:14px; color:#1A1A1A;"><strong>{_esc(rec['symbol'])}</strong> <span style="color:#777; font-size:12px;">{_esc(rec['name'])[:38]}</span></div>
        <div style="font-size:12px; color:#555; margin-top:3px; line-height:1.5;">{reason}</div>
      </td>
      <td align="right" style="padding:12px 14px; font-family:{SANS}; white-space:nowrap;">
        <div style="font-size:13px; color:{rt_color};"><strong>{right_top}</strong></div><div style="font-size:11px; color:#888;">{right_bot}</div>
      </td></tr></table>"""


def _opp_row(rank: int, p: Dict[str, Any], metrics: Optional[Dict[str, Dict[str, Any]]] = None) -> str:
    conf = p["conf"].title()
    cc = ADD_C if conf.lower() == "high" else (TRIM_C if conf.lower() in ("medium", "moderate") else "#888")
    m = (metrics or {}).get(p["symbol"].upper())  # v1.3.0
    extras = _fund_line(m) + _range52_bar(m)      # v1.3.0
    # v1.6.0 (B5): the card header and its outlook strip must quote the SAME
    # reliability. Prefer the forecast-path score the strip shows; the Top_10
    # screen Rel is only the fallback when the metrics row is missing.
    rel_show = m.get("rel") if (m and m.get("rel") is not None) else p["rel"]
    # v1.7.0 (B): issuer-sibling warning line on the buy card.
    sib = (f'<div style="font-size:11px; color:{TRIM_C}; margin-top:3px;">&#9888;&#65039; Same issuer as held '
           f'<strong>{_esc(p.get("sibling"))}</strong> &mdash; different share class</div>'
           ) if p.get("sibling") else ""
    return f"""<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:0; background:#E9F5EF; border:1px solid #C7E4D5;"><tr>
      <td style="padding:12px 12px; width:34px; vertical-align:top;"><div style="width:24px; height:24px; background:{ADD_C}; color:#fff; border-radius:50%; text-align:center; line-height:24px; font-family:{SANS}; font-size:12px; font-weight:bold;">{rank}</div></td>
      <td style="padding:11px 6px; font-family:{SANS};"><div style="font-size:14px; color:#1A1A1A;"><strong>{_esc(p['symbol'])}</strong> <span style="color:#777; font-size:12px;">{_esc(p['name'])[:30]}</span></div><div style="font-size:11px; color:#8A8A8A; margin-top:2px;">{_esc(p['sector'])} · {_esc(p['market'])}</div>{sib}{extras}</td>
      <td align="right" style="padding:11px 14px; font-family:{SANS}; white-space:nowrap; vertical-align:top;"><div style="font-size:13px; color:#0E7C5A;"><strong>{_pct(p['roi'],0)} to fair value</strong></div><div style="font-size:11px; color:#888;">Reliability {_num_str(rel_show)} · <span style="color:{cc};">{conf}</span></div></td></tr></table>{_outlook_strip(m)}<div style="height:7px; font-size:0; line-height:0;">&nbsp;</div>"""


def _num_str(x: Optional[float]) -> str:
    return "—" if x is None else f"{x:.0f}"


def _num_str2(x: Optional[float]) -> str:
    return "—" if x is None else f"{x:,.2f}"


def _mcap_compact(x: Optional[float]) -> str:
    """Compact market-cap: 3.2T / 850.4B / 45.0M."""
    if x is None:
        return "—"
    ax = abs(x)
    for div, suf in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if ax >= div:
            return f"{x / div:,.1f}{suf}"
    return f"{x:,.0f}"


def _roi_color(x: Optional[float]) -> str:
    # v1.6.0 (B1): input is percent-POINTS; sign alone decides the color.
    if x is None:
        return "#888"
    return ADD_C if x > 0 else (SELL_C if x < 0 else "#888")


def _outlook_strip(m: Optional[Dict[str, Any]]) -> str:
    """v1.3.0: 1M/3M/12M engine valuation path + reliability. HTML-only."""
    if not m:
        return ""
    cells = []
    for label, rk, fk in (("1M", "roi1m", "fp1m"), ("3M", "roi3m", "fp3m"), ("12M", "roi12m", "fp12m")):
        roi, fp = m.get(rk), m.get(fk)
        if roi is None and fp is None:
            continue
        cells.append(
            f'<td width="27%" style="padding:7px 12px; font-family:{SANS}; border-right:1px solid #E3EBE6;">'
            f'<div style="font-size:9px; color:#8A958E; letter-spacing:1.5px;">{label}&nbsp;OUTLOOK</div>'
            f'<div style="font-size:13px; color:{_roi_color(roi)}; margin-top:1px;"><strong>{_pct(roi, 1)}</strong></div>'
            f'<div style="font-size:10px; color:#9AA39C;">target {_num_str2(fp)}</div></td>')
    if not cells:
        return ""
    rel = m.get("rel")
    cells.append(
        f'<td style="padding:7px 12px; font-family:{SANS};">'
        f'<div style="font-size:9px; color:#8A958E; letter-spacing:1.5px;">RELIABILITY</div>'
        f'<div style="font-size:13px; color:{STEEL}; margin-top:1px;"><strong>{_num_str(rel)}</strong></div>'
        f'<div style="font-size:10px; color:#9AA39C;">valuation path, not a prediction</div></td>')
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            f'style="margin:-6px 0 8px 0; background:#F7FBF9; border:1px solid #DCE8E0; border-top:0;">'
            f'<tr>{"".join(cells)}</tr></table>')


def _fund_line(m: Optional[Dict[str, Any]]) -> str:
    """v1.3.0: income-statement-level facts available on the schema today."""
    if not m:
        return ""
    bits = []
    if m.get("eps") is not None:
        bits.append(f"EPS (TTM) {_num_str2(m['eps'])}")
    if m.get("mcap") is not None:
        bits.append(f"Mkt cap {_mcap_compact(m['mcap'])}")
    if m.get("divy") is not None:
        bits.append(f"Div yield {_pct(m['divy'], 1).lstrip('+')}")
    if m.get("margin") is not None:
        bits.append(f"Profit margin {_pct(m['margin'], 1).lstrip('+')}")
    if not bits:
        return ""
    return (f'<div style="font-family:{SANS}; font-size:11px; color:#6B7A6F; margin-top:4px;">'
            f'{" &nbsp;·&nbsp; ".join(bits)}</div>')


def _range52_bar(m: Optional[Dict[str, Any]]) -> str:
    """v1.3.0: where today's price sits inside the 52W range (marker on a track)."""
    if not m:
        return ""
    p, lo, hi = m.get("price"), m.get("lo52"), m.get("hi52")
    if p is None or lo is None or hi is None or hi <= lo:
        return ""
    pos = max(0.0, min(1.0, (p - lo) / (hi - lo))) * 100.0
    mpos = min(pos, 96.0)
    return (f'<div style="margin-top:6px;">'
            f'<div style="font-family:{SANS}; font-size:10px; color:#8A958E;">'
            f'52W RANGE&nbsp; {lo:,.2f} — {hi:,.2f} &nbsp;·&nbsp; now {p:,.2f} ({pos:.0f}% of range)</div>'
            f'<div style="height:6px; background:#E8E3D8; margin-top:3px; font-size:0; line-height:0;">'
            f'<div style="margin-left:{mpos:.0f}%; width:10px; height:6px; background:{INK}; font-size:0; line-height:0;">&nbsp;</div>'
            f'</div></div>')


def _glance_block(d: Dict[str, Any]) -> str:
    """v1.3.0: diverging unrealized-P&L bar per holding + weight column."""
    holdings = d["sell"] + d["trim"] + d["add"] + d["hold"]

    def v(r: Dict[str, Any]) -> float:
        # v1.6.0 (B1): percent-POINTS as stored; the old <=1.5 fraction guess
        # inflated real small P&Ls x100 and set the whole bar scale to 150%.
        return float(r["pl"])

    rows = sorted([r for r in holdings if r.get("pl") is not None], key=v, reverse=True)
    if not rows:
        return ""
    maxabs = max(abs(v(r)) for r in rows) or 1.0
    trs = []
    for r in rows:
        val = v(r)
        w = min(100.0, abs(val) / maxabs * 100.0)
        neg = (f'<div style="margin-left:{max(0.0, 100 - w):.0f}%; width:{w:.0f}%; height:10px; '
               f'background:{SELL_C}; font-size:0; line-height:0;">&nbsp;</div>') if val < 0 else ""
        pos_ = (f'<div style="width:{max(w, 1.0):.0f}%; height:10px; background:{ADD_C}; '
                f'font-size:0; line-height:0;">&nbsp;</div>') if val > 0 else ""
        wt_s = _pct(r.get("weight"), 1).lstrip("+") if r.get("weight") is not None else "—"
        col = SELL_C if val < 0 else ADD_C
        trs.append(
            f'<tr style="font-family:{SANS};">'
            f'<td style="padding:5px 10px; font-size:12px; color:#1A1A1A; width:92px; border-bottom:1px solid #EFECE4;"><strong>{_esc(r["symbol"])}</strong></td>'
            f'<td width="29%" style="padding:5px 0; border-bottom:1px solid #EFECE4; border-right:1px solid #C9C4B8;">{neg}</td>'
            f'<td width="29%" style="padding:5px 0; border-bottom:1px solid #EFECE4;">{pos_}</td>'
            f'<td align="right" style="padding:5px 10px; font-size:12px; color:{col}; width:66px; border-bottom:1px solid #EFECE4;"><strong>{val:+.1f}%</strong></td>'
            f'<td align="right" style="padding:5px 10px; font-size:11px; color:#8A8A8A; width:56px; border-bottom:1px solid #EFECE4;">{wt_s}</td>'
            f'</tr>')
    return f"""
  <tr><td style="padding:18px 32px 0 32px;"><div style="font-family:{SERIF}; font-size:16px; color:{INK}; border-bottom:2px solid {BRASS}; padding-bottom:6px;">Your book at a glance <span style="font-family:{SANS}; font-size:11px; color:#8C97A3; letter-spacing:0.5px;">UNREALIZED&nbsp;P&amp;L&nbsp;PER&nbsp;HOLDING&nbsp;·&nbsp;WEIGHT</span></div></td></tr>
  <tr><td style="padding:10px 32px 0 32px;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#FCFBF7; border:1px solid #E6E1D6;">{''.join(trs)}</table>
    <div style="font-family:{SANS}; font-size:10px; color:#A09B90; padding:5px 2px 0 2px;">Bars are scaled to the largest move ({maxabs:.0f}%). Right column is each name's share of the book.</div>
  </td></tr>"""


def render_html(model: Dict[str, Any], owner: str, when: _dt.datetime) -> str:
    d = model["decision"]; t = model["top10"]; mk = model["market"]
    metrics = model.get("metrics", {}) or {}  # v1.3.0
    date_long = when.strftime("%A").rstrip()
    date_short = when.strftime("%d %B %Y")

    # v1.7.0 (A): DATA VINTAGE NOTICE — amber banner when the NEWEST market-
    # page update is older than TFB_BRIEF_STALE_HOURS at send time. Naive
    # Riyadh timestamps on both sides; fail-safe: no vintage -> no banner.
    _vin_all = model.get("vintage", {}) or {}
    _newest_ts = None
    for _vv in _vin_all.values():
        _ts = (_vv or {}).get("latest_ts")
        if _ts is not None and (_newest_ts is None or _ts > _newest_ts):
            _newest_ts = _ts
    vintage_banner = ""
    if _newest_ts is not None:
        try:
            _age_h = max(0.0, (when - _newest_ts).total_seconds() / 3600.0)
        except Exception:
            _age_h = 0.0
        if _age_h >= _stale_hours_threshold():
            vintage_banner = f"""
  <tr><td style="padding:14px 32px 0 32px;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#FCF4E3; border:1px solid #E8D5A8; border-left:4px solid {TRIM_C};"><tr><td style="padding:11px 16px; font-family:{SANS}; font-size:12px; color:#7A5A17; line-height:1.6;">
      <strong>Data vintage notice.</strong> The newest market-page update is {_newest_ts.strftime('%Y-%m-%d %H:%M')} Riyadh (&asymp;{_age_h:.0f}h before this brief). Prices and scores below reflect the last completed sync, not live quotes.
    </td></tr></table>
  </td></tr>"""

    # ---- hero numbers ----
    # v1.6.0 (B4): the hero used to narrate ONLY d["add"][0] and call the rest
    # "dry powder" — while the decision layer it quotes was deploying that
    # same cash across THREE adds (2026-07-08: "add BBD.US, ~1,422 dry" vs
    # 2,482 deployed). The hero now reports every funded add, their total and
    # the TRUE remainder, so the one-move card can never contradict the
    # action table below it.
    freed = d["freed_cash"]
    adds = d["add"]
    add = adds[0] if adds else None
    adds_total = sum((r.get("sar") or 0.0) for r in adds)
    dry = max(freed - adds_total, 0.0)
    sell_syms = ", ".join(r["symbol"].split(".")[0] for r in d["sell"][:3]) or "—"
    trim_sym = d["trim"][0]["symbol"].split(".")[0] if d["trim"] else "—"
    add_sym = add["symbol"] if add else "—"
    adds_syms = ", ".join(r["symbol"] for r in adds) or "—"
    adds_list_html = ", ".join(
        f'<strong style="color:{ADD_C};">{_esc(r["symbol"])}</strong>'
        f'&nbsp;(~{_money(r.get("sar") or 0.0)})' for r in adds)

    # v1.2.0: build the hero conditionally instead of always rendering the
    # buy-day template. See the module docstring WHY block. The exit/trim
    # sentence is assembled from only the non-empty clauses.
    if d["sell"] and d["trim"]:
        freed_sentence = (f'Exit the weak names (<strong>{_esc(sell_syms)}</strong>) and trim your '
                          f'overweight <strong>{_esc(trim_sym)}</strong> back to cap')
    elif d["sell"]:
        freed_sentence = f'Exit the weak names (<strong>{_esc(sell_syms)}</strong>)'
    elif d["trim"]:
        freed_sentence = f'Trim your overweight <strong>{_esc(trim_sym)}</strong> back to cap'
    else:
        freed_sentence = ""

    if add:
        _n_adds = len(adds)
        _adds_word = add_sym if _n_adds == 1 else f"{_n_adds} adds"
        preheader = (f"Today's move: free ~{_money(freed)} SAR, fund {_adds_word} "
                     f"(~{_money(adds_total)} SAR)"
                     + (f", keep ~{_money(dry)} SAR as dry powder." if dry >= 1 else "."))
        hero_title = (f'Free up <strong style="color:{INK};">~{_money(freed)}&nbsp;SAR</strong>, '
                      f'then fund <strong style="color:{ADD_C};">{_esc(_adds_word)}</strong>')
        _dry_tail = (f' The remaining <strong style="color:{INK};">~{_money(dry)}&nbsp;SAR is '
                     f'dry powder</strong> for the new opportunities below.'
                     if dry >= 1 else
                     ' That deploys the freed cash almost to the riyal — the new '
                     'opportunities below are the next call on fresh capital.')
        hero_body = (f'{freed_sentence} — about <strong>{_money(freed)}&nbsp;SAR</strong> freed. '
                     f'The decision layer funds {adds_list_html} — '
                     f'<strong>~{_money(adds_total)}&nbsp;SAR</strong> in total, every one a '
                     f'high-confidence call.{_dry_tail}')
    elif freed_sentence:
        preheader = (f"Today's move: free ~{_money(freed)} SAR and hold it as dry powder — "
                     f"no buy clears the bar today.")
        hero_title = (f'Free up <strong style="color:{INK};">~{_money(freed)}&nbsp;SAR</strong> '
                      f'and hold it as dry powder')
        hero_body = (f'{freed_sentence} — about <strong>{_money(freed)}&nbsp;SAR</strong> freed. '
                     f'<strong>No buy clears the engine\'s high-confidence bar today</strong>, so the '
                     f'full <strong style="color:{INK};">~{_money(freed)}&nbsp;SAR stays as dry '
                     f'powder</strong> for the new opportunities below.')
    else:
        preheader = "No high-confidence portfolio action today — review the opportunities inside."
        hero_title = 'No portfolio action required today'
        hero_body = ('Nothing in your book meets the high-confidence bar for action this morning, and '
                     'no buy clears it either. Your holdings stay as they are; the opportunity list '
                     'below is for monitoring.')

    # ---- action rows ----
    sell_rows = "".join(
        _action_row(r, "EXIT" if r else "EXIT", SELL_C, "#FBEEEC", "#EBD5D1",
                    _exit_reason(r), _pct(r["pl"]), f'{_pct(r["weight"],1).lstrip("+")} of book', SELL_C)
        for r in d["sell"])
    trim_rows = "".join(
        _action_row(r, "TRIM", TRIM_C, "#FBF4E7", "#ECDDBE",
                    _trim_reason(r), _pct(r["pl"]), f'{_pct(r["weight"],1).lstrip("+")} of book', SELL_C)
        for r in d["trim"])
    add_rows = "".join(
        _action_row(r, "ADD", ADD_C, "#E9F5EF", "#C7E4D5",
                    _add_reason(r), f'{_pct(r["eroi"],0)} outlook', f'now {_pct(r["weight"],1).lstrip("+")}', "#0E7C5A")
        + _outlook_strip(metrics.get(r["symbol"].upper()))  # v1.3.0
        for r in d["add"])

    hold_rows = "".join(_hold_row(r) for r in d["hold"])

    # ---- opportunities ----
    opp_rows = "".join(_opp_row(i + 1, p, metrics) for i, p in enumerate(t["top"]))
    rest_rows = "".join(_rest_row(market, names) for market, names in t["rest"].items()) if t["rest"] else ""
    rest_block = f"""
    <div style="font-family:{SERIF}; font-size:13px; color:#0E7C5A; margin:14px 2px 8px 2px;">Across the rest of your markets</div>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#F2F8F5; border:1px solid #D5E8DE;">{rest_rows}</table>""" if rest_rows else ""

    # ---- per-page strip ----
    page_rows = "".join(_page_row(name, mk[name], (model.get("vintage", {}) or {}).get(name))
                        for name in MARKET_PAGES)

    # ---- v1.3.0: book-at-a-glance diverging P&L bars ----
    glance_block = _glance_block(d)

    # ---- v1.4.0: end-of-brief action summary table ----
    summary_table = _summary_table_html(model)

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{_esc(owner)} — Daily Investment Brief</title></head>
<body style="margin:0; padding:0; background:#E7E4DC; -webkit-text-size-adjust:100%;">
<div style="display:none; max-height:0; overflow:hidden; opacity:0; color:#E7E4DC; font-size:1px; line-height:1px;">{preheader}</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#E7E4DC; padding:24px 12px;"><tr><td align="center">
<table role="presentation" width="640" cellpadding="0" cellspacing="0" style="width:640px; max-width:640px; background:#FFFFFF; border:1px solid #D8D3C8;">

  <tr><td style="background:{INK}; padding:28px 32px 22px 32px;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
      <td style="font-family:{SERIF}; color:#F4F1EA; font-size:24px; letter-spacing:1px;">{_esc(owner)}</td>
      <td align="right" style="font-family:{SERIF}; color:{BRASS}; font-size:11px; letter-spacing:2px;">INVESTMENT&nbsp;BRIEF</td>
    </tr></table>
    <div style="height:1px; background:{BRASS}; margin:16px 0 14px 0; line-height:1px; font-size:0;">&nbsp;</div>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
      <td style="font-family:{SERIF}; color:#FFFFFF; font-size:25px;">Daily Investment Brief<div style="font-family:{SANS}; font-size:10px; color:#9CA8BA; letter-spacing:2px; margin-top:4px;">LOCAL&nbsp;&amp;&nbsp;GLOBAL&nbsp;MARKETS</div></td>
      <td align="right" style="font-family:{SANS}; color:#A9B4C4; font-size:12px;">{date_long}<br>{date_short}<br><span style="color:#7E8AA0;">before market open</span></td>
    </tr></table>
  </td></tr>

{vintage_banner}

  <tr><td style="padding:26px 32px 6px 32px;">
    <div style="font-family:{SANS}; font-size:11px; letter-spacing:2px; color:{INK}; font-weight:bold;">TODAY'S ONE MOVE</div>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-top:10px; border:1px solid #C9D2C4; border-left:4px solid {ADD_C}; background:#F2F8F4;"><tr><td style="padding:18px 20px;">
      <div style="font-family:{SERIF}; font-size:20px; color:#1A1A1A; line-height:1.4;">{hero_title}</div>
      <div style="font-family:{SANS}; font-size:14px; color:#4A4A4A; line-height:1.65; margin-top:8px;">{hero_body}</div>
      <div style="font-family:{SANS}; font-size:12px; color:#888; line-height:1.6; margin-top:10px; padding-top:9px; border-top:1px dashed #CDD8CC;">All high-confidence calls. A plan for your decision, not an instruction.</div>
    </td></tr></table>
  </td></tr>

  <tr><td style="padding:18px 32px 4px 32px;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#F6F4EF; border:1px solid #E6E1D6;"><tr><td style="padding:14px 18px; font-family:{SANS}; font-size:13px; color:#3A3A3A; line-height:1.7;">
      <strong style="font-family:{SERIF}; font-size:14px;">Book snapshot.</strong> {len(d['sell'])+len(d['trim'])+len(d['add'])+len(d['hold'])} holdings · <strong style="color:{SELL_C};">{_pct(d['pl_pct'])}</strong> overall ({_money(d['mv'])} vs {_money(d['cost'])}&nbsp;SAR). {len(d['hold'])} positions are <em>held only because the data is too weak to act</em> — not comfort holds. {_concentration(d)}
    </td></tr></table>
  </td></tr>

{glance_block}

  <tr><td style="padding:22px 32px 0 32px;"><div style="font-family:{SERIF}; font-size:16px; color:{INK}; border-bottom:2px solid {INK}; padding-bottom:6px;">Act now <span style="font-family:{SANS}; font-size:11px; color:#7E8AA0; letter-spacing:0.5px;">HIGH-CONFIDENCE CALLS</span></div></td></tr>
  <tr><td style="padding:10px 32px 0 32px;">{sell_rows}{trim_rows}{add_rows}</td></tr>

  <tr><td style="padding:18px 32px 0 32px;"><div style="font-family:{SERIF}; font-size:16px; color:#46535F; border-bottom:2px solid {HOLD_C}; padding-bottom:6px;">Hold &mdash; no action <span style="font-family:{SANS}; font-size:11px; color:#8C97A3; letter-spacing:0.5px;">DATA TOO WEAK TO ACT</span></div></td></tr>
  <tr><td style="padding:10px 32px 0 32px;">
    <div style="font-family:{SANS}; font-size:12px; color:#555; line-height:1.6; margin-bottom:9px;">The engine leans bearish on most of these, but their data reliability is too low to act on — so they're capped to hold. Verify before any move.</div>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#F0F2F4; border:1px solid #DDE2E7;">{hold_rows}</table>
  </td></tr>

  <tr><td style="padding:20px 32px 0 32px;"><div style="font-family:{SERIF}; font-size:16px; color:#0E7C5A; border-bottom:2px solid {ADD_C}; padding-bottom:6px;">Best new buys <span style="font-family:{SANS}; font-size:11px; color:#8A958E; letter-spacing:0.5px;">ALL MARKETS · WHERE YOUR DRY POWDER GOES</span></div></td></tr>
  <tr><td style="padding:10px 32px 0 32px;">
    <div style="font-family:{SANS}; font-size:12px; color:#555; line-height:1.6; margin-bottom:10px;">Ranked entry candidates you <strong>don't hold</strong>, screened across <strong>all your markets</strong>. The figure is the gap to estimated <strong>fair value — a valuation target, not a prediction</strong>; reliability and confidence show how much weight to give each. The 1M&nbsp;/&nbsp;3M&nbsp;/&nbsp;12M strip under each name is the engine's valuation path with its stated reliability — <strong>targets, not predictions</strong>. Full entry/stop/target levels are in your Top&nbsp;10 sheet.</div>
    {opp_rows}{rest_block}
    <div style="font-family:{SANS}; font-size:11px; color:#A09B90; line-height:1.6; padding:8px 2px 0 2px; font-style:italic;">Live brief refreshes the full cross-market list each morning and always excludes names you hold.</div>
  </td></tr>

  <tr><td style="padding:20px 32px 0 32px;"><div style="font-family:{SERIF}; font-size:16px; color:#46535F; border-bottom:2px solid {HOLD_C}; padding-bottom:6px;">Across your market pages</div></td></tr>
  <tr><td style="padding:10px 32px 0 32px;"><table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#F5F4F0; border:1px solid #E2DDD2;">{page_rows}</table></td></tr>

{summary_table}

  <tr><td style="padding:18px 32px 0 32px;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:{INK};"><tr><td style="padding:14px 18px; font-family:{SANS}; font-size:11px; color:#C7D0DC; line-height:1.8;">
      <span style="color:{BRASS}; font-family:{SERIF}; font-size:12px; letter-spacing:1px;">WHEN YOU CAN ACT</span> &nbsp;(Riyadh time)<br>
      <strong style="color:#fff;">Tadawul</strong> 10:00–15:00, Sun–Thu &nbsp;·&nbsp; <strong style="color:#fff;">US</strong> 16:30–23:00 &nbsp;·&nbsp; <strong style="color:#fff;">Europe</strong> ~10:00–19:30 &nbsp;·&nbsp; <strong style="color:#fff;">Tokyo/HK</strong> early morning
    </td></tr></table>
  </td></tr>

  <tr><td style="padding:22px 32px 28px 32px;">
    <div style="height:1px; background:#E0DBD0; line-height:1px; font-size:0; margin-bottom:14px;">&nbsp;</div>
    <div style="font-family:{SANS}; font-size:11px; color:#9A958A; line-height:1.7;">Prepared from your own screening engine for <strong>monitoring and decision support</strong> — not personalised financial advice or an instruction to trade. Recommendations are <strong>daily-horizon</strong>; the engine does not predict intraday timing, and modelled 12-month outlooks carry low-to-moderate reliability. You remain the decision-maker; verify before acting. P&amp;L is unrealised; book totals are converted to SAR.</div>
    <div style="font-family:{SERIF}; font-size:12px; color:{BRASS}; margin-top:14px; letter-spacing:1px;">— {_esc(owner)} · Daily Investment Brief</div>
  </td></tr>

</table>
<div style="font-family:{SANS}; font-size:10px; color:#A8A398; margin-top:14px;">Sent before market open · {date_short}, {when.strftime('%H:%M')} Riyadh</div>
</td></tr></table></body></html>"""


def _concentration(d: Dict[str, Any]) -> str:
    everyone = d["sell"] + d["trim"] + d["add"] + d["hold"]
    ranked = sorted([r for r in everyone if r.get("weight") is not None],
                    key=lambda r: r["weight"], reverse=True)[:2]
    if len(ranked) < 2:
        return ""
    a, b = ranked
    return (f'Concentration is the structural watch-item: <strong style="color:{STEEL};">'
            f'{_esc(a["symbol"])} is {a["weight"]:.1f}%</strong> of the book and '
            f'<strong style="color:{STEEL};">{_esc(b["symbol"])} {b["weight"]:.1f}%</strong> '
            f'— over half in two names.')


def _exit_reason(r: Dict[str, Any]) -> str:
    note = r["note"].lower()
    if "above" in note or "valuation roi -" in note or ("valuation roi" in note and "<=" in note):
        return (f'Full exit (~{_money(r["sar"])} SAR). Trades above the engine\'s fair value, '
                f'past the exit line; high confidence.')
    return (f'Full exit (~{_money(r["sar"])} SAR). Sell-tier, high confidence; '
            f'engine 12M outlook {_pct(r["eroi"],0)}.')


def _trim_reason(r: Dict[str, Any]) -> str:
    return (f'Trim ~{_money(r["sar"])} SAR. The company is fine — at {_pct(r["weight"],1).lstrip("+")} '
            f'it is over your position cap. Trim back to size.')


def _add_reason(r: Dict[str, Any]) -> str:
    sh = f'{r.get("shares")} shares ' if r.get("shares") else ""
    return (f'Add <strong>{sh}(~{_money(r["sar"])} SAR)</strong> from the freed cash. '
            f'Reliability {_num_str(r["rel"])}, room in the book. Engine 12M {_pct(r["eroi"],0)}.')


def _hold_row(r: Dict[str, Any]) -> str:
    pl = r.get("pl")
    col = ADD_C if (pl is not None and pl > 0) else SELL_C
    extra = " · income anchor" if "sukuk" in r["note"].lower() or r["symbol"].startswith("5023") else ""
    right = f'{_pct(pl)} <span style="color:#999;">P&amp;L</span>' if pl is not None else "—"
    return f"""<tr style="font-family:{SANS};"><td style="padding:9px 14px; font-size:13px; color:#1A1A1A; border-bottom:1px solid #E2E6EA;"><strong>{_esc(r['symbol'])}</strong> <span style="color:#888; font-size:11px;">{_esc(r['name'])[:30]}{extra}</span></td><td align="right" style="padding:9px 14px; font-size:12px; color:{col}; border-bottom:1px solid #E2E6EA;">{right}</td></tr>"""


def _rest_row(market: str, names: List[Dict[str, Any]]) -> str:
    # v1.7.0 (B): per-name issuer-sibling suffix in the rest-of-markets strip.
    parts: List[str] = []
    for n in names[:4]:
        piece = f"<strong>{_esc(n['symbol'])}</strong>"
        if n.get("sibling"):
            piece += (f' <span style="color:#B26B00; font-size:11px;">(sibling of held '
                      f'{_esc(n["sibling"])})</span>')
        parts.append(piece)
    label = ", ".join(parts)
    return f"""<tr style="font-family:{SANS};"><td style="padding:9px 14px; font-size:12px; color:#0E7C5A; font-weight:bold; width:110px; vertical-align:top; border-bottom:1px solid #E2EFE8;">{_esc(market)}</td><td style="padding:9px 14px; font-size:12px; color:#444; line-height:1.5; border-bottom:1px solid #E2EFE8;">{label}</td></tr>"""


def _breadth_pct(info: Dict[str, Any]) -> float:
    """v1.3.0: investable share of a market page for the breadth bar."""
    total = info.get("total") or 0
    inv = info.get("invest") or 0
    if not total:
        return 0.0
    return max(0.0, min(100.0, inv / total * 100.0))


def _short_name(name: str, symbol: str = "") -> str:
    """v1.6.0 (B3): compact display name for the market-page strip.

    The old name.split()[0] printed leading articles and trailing commas
    verbatim ("The", "East", "Innoviva,", "WesBanco,."). Strip punctuation,
    drop a leading "The", keep up to two words (18-char cap) and fall back
    to the SYMBOL when the name cell is blank."""
    words = [w.strip(" ,.;:—-") for w in _s(name).split()]
    words = [w for w in words if w]
    if words and words[0].lower() == "the":
        words = words[1:]
    short = " ".join(words[:2])[:18].rstrip(" ,.;:—-")
    return short if short else _s(symbol)


def _page_row(name: str, info: Dict[str, Any], vin: Optional[Dict[str, Any]] = None) -> str:
    pretty = name.replace("_", " ")
    inv = info["invest"]
    # v1.7.0 (A): compact per-page data-vintage line (dominant dates first).
    _vds = (vin or {}).get("dates") or []
    vin_html = ""
    if _vds:
        _vtxt = " &middot; ".join(f"{d[5:]}&times;{c:,}" for d, c in _vds[:2])
        vin_html = (f'<br><span style="font-size:10px; color:#B0AA9C;">Data: '
                    f'{_vtxt}</span>')
    # v1.7.0 (B): issuer-sibling sentence for this page's top names.
    _sibs = [t for t in (info.get("top") or [])[:5] if t.get("sibling")]
    sib_html = ""
    if _sibs:
        _s0 = _sibs[0]
        sib_html = (f' <span style="color:#B26B00;">{_esc(_s0["symbol"])} is a share-class '
                    f'sibling of held {_esc(_s0["sibling"])}.</span>')
    if inv == 0:
        note = "No signals clear the bar — <em>monitoring only</em>."
        ncol = "#777"
    elif inv <= 2:
        nm = (_short_name(info["top"][0]["name"], info["top"][0]["symbol"])
              if info["top"] else "")
        note = f"Only {_esc(nm)} clears — effectively <em>monitoring only</em>."
        ncol = "#777"
    else:
        # v1.6.0 (B3): the strip lists NAMES, so repeats of the same display
        # label collapse HERE (REGCP.US / REGCO.US are genuinely distinct
        # preferred series of one issuer — real securities the data layer must
        # keep — but "Regency Centers, Regency Centers" tells the reader
        # nothing). Walk the ranked list, keep the first row per label.
        _labels: List[str] = []
        _seen_lbl: set = set()
        for t in info["top"]:
            _lbl = _short_name(t["name"], t["symbol"])
            if _lbl.lower() in _seen_lbl:
                continue
            _seen_lbl.add(_lbl.lower())
            _labels.append(_esc(_lbl))
            if len(_labels) == 5:
                break
        names = ", ".join(_labels)
        note = f"Top names: {names}."
        ncol = "#555"
    note += sib_html
    return f"""<tr style="font-family:{SANS};"><td style="padding:10px 14px; font-size:13px; color:#1A1A1A; vertical-align:top; border-bottom:1px solid #E8E3D8; width:150px;"><strong>{_esc(pretty)}</strong><br><span style="font-size:11px; color:#999;">{info['total']:,} names · {inv} investable</span>{vin_html}<div style="height:5px; background:#E8E3D8; margin-top:5px; font-size:0; line-height:0;"><div style="width:{_breadth_pct(info):.0f}%; height:5px; background:{ADD_C}; font-size:0; line-height:0;">&nbsp;</div></div></td><td style="padding:10px 14px; font-size:12px; color:{ncol}; line-height:1.5; border-bottom:1px solid #E8E3D8;">{note}</td></tr>"""


# ----------------------------------------------------------------------------- #
# Plaintext fallback + subject + delivery (only used with --send)
# ----------------------------------------------------------------------------- #

# ----------------------------------------------------------------------------- #
# v1.4.0 — end-of-brief action summary (HTML) + PDF copy of the recommendations
# ----------------------------------------------------------------------------- #
def _grp_cash(recs: List[Dict[str, Any]]) -> float:
    return sum((r.get("sar") or 0.0) for r in recs)


def _summary_table_html(model: Dict[str, Any]) -> str:
    """v1.4.0: compact ACTION SUMMARY table rendered at the end of the brief."""
    d = model["decision"]; t = model["top10"]
    groups = (("EXIT / SELL", d["sell"], SELL_C, True),
              ("TRIM", d["trim"], TRIM_C, True),
              ("ADD / BUY", d["add"], ADD_C, True),
              ("HOLD", d["hold"], HOLD_C, False))
    body = ""
    for label, recs, color, cash in groups:
        syms = ", ".join(_esc(r["symbol"]) for r in recs) or "&mdash;"
        sar = _grp_cash(recs)
        sar_s = f"~{sar:,.0f}" if (cash and sar) else "&mdash;"
        body += (f'<tr>'
                 f'<td style="padding:7px 10px; font-family:{SANS}; font-size:11px; font-weight:bold; color:{color}; white-space:nowrap; border-bottom:1px solid #E6E1D6;">{label}</td>'
                 f'<td align="center" style="padding:7px 8px; font-family:{SANS}; font-size:11px; color:#3A3A3A; border-bottom:1px solid #E6E1D6;">{len(recs)}</td>'
                 f'<td style="padding:7px 10px; font-family:{SANS}; font-size:11px; color:#3A3A3A; line-height:1.5; border-bottom:1px solid #E6E1D6;">{syms}</td>'
                 f'<td align="right" style="padding:7px 10px; font-family:{SANS}; font-size:11px; color:#3A3A3A; white-space:nowrap; border-bottom:1px solid #E6E1D6;">{sar_s}</td>'
                 f'</tr>')
    buys = ", ".join(_esc(p["symbol"]) for p in t["top"]) or "&mdash;"
    body += (f'<tr>'
             f'<td style="padding:7px 10px; font-family:{SANS}; font-size:11px; font-weight:bold; color:{STEEL}; white-space:nowrap;">NEW BUY CANDIDATES</td>'
             f'<td align="center" style="padding:7px 8px; font-family:{SANS}; font-size:11px; color:#3A3A3A;">{len(t["top"])}</td>'
             f'<td style="padding:7px 10px; font-family:{SANS}; font-size:11px; color:#3A3A3A; line-height:1.5;">{buys}</td>'
             f'<td align="right" style="padding:7px 10px; font-family:{SANS}; font-size:11px; color:#8C97A3;">&mdash;</td>'
             f'</tr>')
    pl = _pct(d.get("pl_pct")) if d.get("pl_pct") is not None else "&mdash;"
    totals = (f'Book <strong>{_money(d["mv"])}&nbsp;SAR</strong> &nbsp;&middot;&nbsp; '
              f'P&amp;L <strong>{pl}</strong> &nbsp;&middot;&nbsp; '
              f'Freed cash <strong>~{_money(d["freed_cash"])}&nbsp;SAR</strong>')
    return f"""  <tr><td style="padding:20px 32px 0 32px;"><div style="font-family:{SERIF}; font-size:16px; color:{INK}; border-bottom:2px solid {INK}; padding-bottom:6px;">Action summary <span style="font-family:{SANS}; font-size:11px; color:#7E8AA0; letter-spacing:0.5px;">EVERYTHING ABOVE, AT A GLANCE</span></div></td></tr>
  <tr><td style="padding:10px 32px 0 32px;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#FBFAF7; border:1px solid #E6E1D6;">
      <tr>
        <td style="padding:7px 10px; font-family:{SANS}; font-size:10px; letter-spacing:1px; color:#FFFFFF; background:{INK};">ACTION</td>
        <td align="center" style="padding:7px 8px; font-family:{SANS}; font-size:10px; letter-spacing:1px; color:#FFFFFF; background:{INK};">#</td>
        <td style="padding:7px 10px; font-family:{SANS}; font-size:10px; letter-spacing:1px; color:#FFFFFF; background:{INK};">SYMBOLS</td>
        <td align="right" style="padding:7px 10px; font-family:{SANS}; font-size:10px; letter-spacing:1px; color:#FFFFFF; background:{INK};">CASH&nbsp;(SAR)</td>
      </tr>
      {body}
    </table>
    <div style="font-family:{SANS}; font-size:11px; color:#5B6B7A; padding:8px 2px 0 2px;">{totals}</div>
  </td></tr>"""


def _pdf_txt(v: Any, limit: int = 64) -> str:
    """Latin-1-safe text for reportlab core fonts (truncated, replaced)."""
    s = _s(v)
    if len(s) > limit:
        s = s[: limit - 3] + "..."
    return s.encode("latin-1", "replace").decode("latin-1")


def _val_ccy(val_txt: str, ccy: str) -> str:
    """v1.6.0 (B2): append the native currency code to a Position Value cell.

    Position Value is stored in the ROW's currency (FER 198 / RCI 1,624 /
    BBD 849 were USD) but sat unlabeled next to a SAR book total — RCI read
    as 5.7% of the book when it is 21.2%. SAR (and blank) stay unadorned."""
    c = _s(ccy).upper()
    if not val_txt or val_txt == "-" or not c or c == "SAR":
        return val_txt
    return f"{val_txt} {c}"


def render_pdf(model: Dict[str, Any], owner: str, when: _dt.datetime) -> Optional[bytes]:
    """v1.5.0: redesigned PDF copy of the recommendations, with portfolio
    current-ROI/ratio and income-statement-snapshot tables. Returns bytes, or
    None on any failure — the email must never be blocked by the attachment."""
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.units import mm
        from reportlab.platypus import (BaseDocTemplate, Frame, PageTemplate,
                                        Paragraph, Spacer, Table, TableStyle)
    except Exception as exc:  # pragma: no cover - environment dependent
        sys.stderr.write(f"[brief] PDF skipped (reportlab unavailable: {exc}); "
                         f"email will carry no attachment\n")
        return None
    try:
        d = model["decision"]; t = model["top10"]
        mx: Dict[str, Dict[str, Any]] = model.get("metrics", {}) or {}
        holdings = d["sell"] + d["trim"] + d["add"] + d["hold"]

        ink = colors.HexColor(INK); brass = colors.HexColor(BRASS)
        grid = colors.HexColor("#DAD5CA"); zebra = colors.HexColor("#F6F4EF")
        gray6 = colors.HexColor("#5B6B7A"); paper = colors.HexColor("#FBFAF7")
        red = colors.HexColor(SELL_C); green = colors.HexColor("#0E7C5A")

        st_h2 = ParagraphStyle("h2", fontName="Helvetica-Bold", fontSize=11,
                               textColor=ink, spaceBefore=11, spaceAfter=2)
        st_note = ParagraphStyle("note", fontName="Helvetica", fontSize=7.2,
                                 textColor=colors.HexColor("#666666"),
                                 spaceAfter=4, leading=9.5)
        st_foot = ParagraphStyle("foot", fontName="Helvetica", fontSize=6.8,
                                 textColor=colors.HexColor("#8A857A"),
                                 spaceBefore=12, leading=9)

        def _r(v: Optional[float], digits: int = 1) -> str:
            return "-" if v is None else f"{v:,.{digits}f}"

        def _mc(v: Optional[float]) -> str:
            s = _mcap_compact(v)
            return _pdf_txt(s if s and s != "\u2014" else "-", 12)

        def _p(v: Optional[float], digits: int = 1) -> str:
            return "-" if v is None else f"{v:+.{digits}f}%"

        def _base(extra=None) -> TableStyle:
            cmds = [
                ("BACKGROUND", (0, 0), (-1, 0), ink),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 7),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 1), (-1, -1), 7),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LINEBELOW", (0, 0), (-1, 0), 0.9, brass),
                ("GRID", (0, 0), (-1, -1), 0.35, grid),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, zebra]),
                ("LEFTPADDING", (0, 0), (-1, -1), 3.5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 3.5),
                ("TOPPADDING", (0, 0), (-1, -1), 2.6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 2.6),
            ]
            cmds += (extra or [])
            return TableStyle(cmds)

        story: List[Any] = []

        # ---- masthead band (matches the email's ink/brass identity) ----
        mast = Table(
            [[Paragraph(f'<font color="#F4F1EA" size="15"><b>{_pdf_txt(owner, 40)}</b></font>'
                        f'<br/><font color="{BRASS}" size="8">D A I L Y&nbsp;&nbsp;I N V E S T M E N T'
                        f'&nbsp;&nbsp;B R I E F</font>',
                        ParagraphStyle("m1", leading=13)),
              Paragraph(f'<font color="#FFFFFF" size="9"><b>{when:%A}</b></font>'
                        f'<br/><font color="#A9B4C4" size="8">{when:%d %B %Y} - {when:%H:%M} Riyadh'
                        f'<br/>recommendations copy - v{__version__}</font>',
                        ParagraphStyle("m2", alignment=2, leading=11))]],
            colWidths=[118 * mm, 62 * mm],
            style=TableStyle([("BACKGROUND", (0, 0), (-1, -1), ink),
                              ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                              ("LEFTPADDING", (0, 0), (-1, -1), 10),
                              ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                              ("TOPPADDING", (0, 0), (-1, -1), 8),
                              ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                              ("LINEBELOW", (0, 0), (-1, -1), 1.4, brass)]))
        story.append(mast)
        story.append(Spacer(0, 5))

        # ---- KPI strip ----
        pl = d.get("pl_pct")
        kpis = [("BOOK VALUE", f"{d['mv']:,.0f} SAR", gray6),
                ("CURRENT ROI", _p(pl), (green if (pl or 0) >= 0 else red)),
                ("FREED CASH", f"~{d['freed_cash']:,.0f} SAR", gray6),
                ("HOLDINGS", str(len(holdings)), gray6),
                ("NEW BUYS", str(len(t["top"])), gray6)]
        cells = [Paragraph(f'<font color="#7E8AA0" size="6">{k}</font><br/>'
                           f'<font color="{v_c.hexval() if hasattr(v_c, "hexval") else INK}" size="10">'
                           f'<b>{_pdf_txt(v, 20)}</b></font>',
                           ParagraphStyle("kpi", leading=13, alignment=1))
                 for k, v, v_c in kpis]
        story.append(Table([cells], colWidths=[36 * mm] * 5,
                           style=TableStyle([("BACKGROUND", (0, 0), (-1, -1), paper),
                                             ("BOX", (0, 0), (-1, -1), 0.5, grid),
                                             ("INNERGRID", (0, 0), (-1, -1), 0.5, grid),
                                             ("TOPPADDING", (0, 0), (-1, -1), 5),
                                             ("BOTTOMPADDING", (0, 0), (-1, -1), 5)])))

        # ---- 1) Portfolio actions ----
        story.append(Paragraph("1 - Portfolio actions (decision layer)", st_h2))
        story.append(Paragraph("Actions come from the confidence-gated Portfolio_Decision layer. "
                               "Daily-horizon decision support - not advice; verify before acting.",
                               st_note))
        rows: List[List[str]] = [["Action", "Symbol", "Name", "P&L %", "Wt %",
                                  "ROI 12M", "Rel", "Note"]]
        extra = []
        for label, recs, hexc in (("EXIT/SELL", d["sell"], SELL_C), ("TRIM", d["trim"], TRIM_C),
                                  ("ADD", d["add"], ADD_C), ("HOLD", d["hold"], HOLD_C)):
            for r in recs:
                rows.append([label, _pdf_txt(r["symbol"], 12), _pdf_txt(r["name"], 28),
                             _pct(r.get("pl")), _pct(r.get("weight"), 1).lstrip("+"),
                             _pct(r.get("eroi")), _num_str(r.get("rel")), _pdf_txt(r.get("note"), 52)])
                ri = len(rows) - 1
                extra.append(("TEXTCOLOR", (0, ri), (0, ri), colors.HexColor(hexc)))
                extra.append(("FONTNAME", (0, ri), (0, ri), "Helvetica-Bold"))
        if len(rows) == 1:
            rows.append(["-", "-", "no holdings parsed", "-", "-", "-", "-", "-"])
        extra.append(("ALIGN", (3, 1), (6, -1), "RIGHT"))
        story.append(Table(rows, colWidths=[17*mm, 17*mm, 36*mm, 12*mm, 10*mm, 13*mm, 9*mm, 66*mm],
                           repeatRows=1, style=_base(extra)))

        # ---- 2) Portfolio — current ROI & key ratios ----
        story.append(Paragraph("2 - Portfolio - current ROI &amp; key ratios", st_h2))
        story.append(Paragraph("Current ROI is your ACTUAL unrealized P&amp;L on the position "
                               "(from My_Portfolio); Exp ROI 12M is the engine's modelled outlook "
                               "(low-to-moderate reliability). Ratios from the 115-column schema; "
                               "dashes mean the sheet has no value for that name.", st_note))
        rows2: List[List[str]] = [["Symbol", "Qty", "Avg cost", "Value", "Cur ROI",
                                   "Exp 12M", "P/E", "P/B", "D/E", "Div %", "Beta"]]
        extra2 = [("ALIGN", (1, 1), (-1, -1), "RIGHT")]
        for r in holdings:
            m = mx.get(r["symbol"].upper(), {}) or {}
            cur = m.get("uplpct", r.get("pl"))
            rows2.append([_pdf_txt(r["symbol"], 12), _r(m.get("qty"), 0), _r(m.get("avgcost"), 2),
                          _val_ccy(_r(m.get("pval"), 0), m.get("ccy") or ""),
                          _p(cur), _p(r.get("eroi")),
                          _r(m.get("pe")), _r(m.get("pb")), _r(m.get("de"), 2),
                          _r(m.get("divy")), _r(m.get("beta"), 2)])
            ri = len(rows2) - 1
            extra2.append(("TEXTCOLOR", (4, ri), (4, ri), green if (cur or 0) >= 0 else red))
            extra2.append(("FONTNAME", (4, ri), (4, ri), "Helvetica-Bold"))
        if len(rows2) == 1:
            rows2.append(["-"] * 11)
        story.append(Table(rows2, colWidths=[22*mm, 13*mm, 17*mm, 21*mm, 16*mm, 16*mm,
                                             13*mm, 13*mm, 13*mm, 13*mm, 13*mm],
                           repeatRows=1, style=_base(extra2)))

        # ---- 3) Income-statement snapshot (TTM) ----
        story.append(Paragraph("3 - Income-statement snapshot (TTM) - portfolio", st_h2))
        story.append(Paragraph("The income-statement LINE ITEMS available on the sheets today. "
                               "A full revenue-to-net-income statement is not on the 115-column "
                               "schema; adding one would be backend work.", st_note))
        rows3: List[List[str]] = [["Symbol", "Revenue", "Rev YoY", "Gross %", "Oper %",
                                   "Net %", "EPS", "FCF"]]
        for r in holdings:
            m = mx.get(r["symbol"].upper(), {}) or {}
            rows3.append([_pdf_txt(r["symbol"], 12), _mc(m.get("rev")), _p(m.get("revg")),
                          _r(m.get("gm")), _r(m.get("om")), _r(m.get("margin")),
                          _r(m.get("eps"), 2), _mc(m.get("fcf"))])
        if len(rows3) == 1:
            rows3.append(["-"] * 8)
        story.append(Table(rows3, colWidths=[24*mm, 26*mm, 20*mm, 20*mm, 20*mm, 20*mm, 20*mm, 30*mm],
                           repeatRows=1,
                           style=_base([("ALIGN", (1, 1), (-1, -1), "RIGHT")])))

        # ---- 4) Best new buys ----
        story.append(Paragraph("4 - Best new buys (all markets)", st_h2))
        story.append(Paragraph("Ranked entry candidates you do not hold. The figure is the gap to "
                               "estimated fair value - a valuation target, not a prediction; "
                               "reliability and confidence show how much weight to give each.", st_note))
        rows4: List[List[str]] = [["#", "Symbol", "Name", "Market", "Sector", "To FV %", "Rel", "Conf"]]
        n = 0
        for p in t["top"]:
            n += 1
            rows4.append([str(n), _pdf_txt(p["symbol"], 12), _pdf_txt(p["name"], 28),
                          _pdf_txt(p["market"], 14), _pdf_txt(p["sector"], 16),
                          _pct(p.get("roi"), 0), _num_str(p.get("rel")), _pdf_txt(p.get("conf"), 10)])
        for market, names in (t.get("rest") or {}).items():
            for p in names:
                n += 1
                rows4.append([str(n), _pdf_txt(p["symbol"], 12), _pdf_txt(p["name"], 28),
                              _pdf_txt(market, 14), _pdf_txt(p.get("sector"), 16),
                              _pct(p.get("roi"), 0), _num_str(p.get("rel")), _pdf_txt(p.get("conf"), 10)])
        if len(rows4) == 1:
            rows4.append(["-", "-", "no funded candidates today", "-", "-", "-", "-", "-"])
        story.append(Table(rows4, colWidths=[7*mm, 17*mm, 45*mm, 21*mm, 28*mm, 14*mm, 9*mm, 39*mm],
                           repeatRows=1,
                           style=_base([("ALIGN", (5, 1), (6, -1), "RIGHT")])))

        # ---- 5) Action summary (END) ----
        story.append(Paragraph("5 - Action summary", st_h2))
        rows5: List[List[str]] = [["ACTION", "#", "SYMBOLS", "CASH (SAR)"]]
        extra5 = [("ALIGN", (3, 1), (3, -1), "RIGHT")]
        for label, recs, hexc, cash in (("EXIT / SELL", d["sell"], SELL_C, True),
                                        ("TRIM", d["trim"], TRIM_C, True),
                                        ("ADD / BUY", d["add"], ADD_C, True),
                                        ("HOLD", d["hold"], HOLD_C, False)):
            sar = _grp_cash(recs)
            rows5.append([label, str(len(recs)),
                          _pdf_txt(", ".join(r["symbol"] for r in recs) or "-", 88),
                          (f"~{sar:,.0f}" if (cash and sar) else "-")])
            ri = len(rows5) - 1
            extra5.append(("TEXTCOLOR", (0, ri), (0, ri), colors.HexColor(hexc)))
            extra5.append(("FONTNAME", (0, ri), (0, ri), "Helvetica-Bold"))
        rows5.append(["NEW BUY CANDIDATES", str(len(t["top"])),
                      _pdf_txt(", ".join(p["symbol"] for p in t["top"]) or "-", 88), "-"])
        story.append(Table(rows5, colWidths=[34*mm, 8*mm, 106*mm, 32*mm], repeatRows=1,
                           style=_base(extra5)))
        story.append(Paragraph(_pdf_txt(f"Book {d['mv']:,.0f} SAR  |  Current ROI {_p(pl)}  |  "
                                        f"Freed cash ~{d['freed_cash']:,.0f} SAR", 120), st_note))

        story.append(Paragraph("Prepared from your own screening engine for monitoring and decision "
                               "support - not personalised financial advice or an instruction to "
                               "trade. Recommendations are daily-horizon; modelled 12-month outlooks "
                               "carry low-to-moderate reliability. You remain the decision-maker; "
                               "verify before acting.", st_foot))

        # ---- document with numbered page footer ----
        buf = io.BytesIO()
        owner_footer = _pdf_txt(owner, 40)
        stamp = f"{when:%d %b %Y %H:%M} Riyadh"

        def _page_footer(canv, doc_):
            canv.saveState()
            canv.setStrokeColor(brass); canv.setLineWidth(0.6)
            canv.line(15 * mm, 11 * mm, 195 * mm, 11 * mm)
            canv.setFont("Helvetica", 6.5); canv.setFillColor(colors.HexColor("#8A857A"))
            canv.drawString(15 * mm, 7.5 * mm, f"{owner_footer} - Daily Investment Brief - {stamp}")
            canv.drawRightString(195 * mm, 7.5 * mm, f"Page {canv.getPageNumber()}")
            canv.restoreState()

        doc = BaseDocTemplate(buf, pagesize=A4,
                              leftMargin=15 * mm, rightMargin=15 * mm,
                              topMargin=12 * mm, bottomMargin=16 * mm,
                              title=f"Daily Investment Brief {when:%Y-%m-%d}",
                              author=owner_footer)
        frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="body")
        doc.addPageTemplates([PageTemplate(id="page", frames=[frame], onPage=_page_footer)])
        doc.build(story)
        return buf.getvalue()
    except Exception as exc:
        sys.stderr.write(f"[brief] PDF build failed ({exc}); email will carry no attachment\n")
        return None


def render_text(model: Dict[str, Any], owner: str, when: _dt.datetime) -> str:
    """A concise plaintext alternative part (deliverability + non-HTML clients)."""
    d = model["decision"]; t = model["top10"]
    add = d["add"][0] if d["add"] else None
    freed = d["freed_cash"]
    _adds_total = sum((r.get("sar") or 0.0) for r in d["add"])
    dry = max(freed - _adds_total, 0.0)  # v1.6.0 (B4): remainder after ALL adds
    lines: List[str] = [
        f"{owner} - Daily Investment Brief - {when:%A, %d %B %Y}",
        "Local & global markets. Daily-horizon, decision support - not advice; verify before acting.",
        "",
        "TODAY'S ONE MOVE",
    ]
    if add:
        _adds_txt = ", ".join(f"{r['symbol']} ~{(r.get('sar') or 0.0):,.0f}" for r in d["add"])
        _tail = (f"; keep ~{dry:,.0f} SAR as dry powder for new buys."
                 if dry >= 1 else "; freed cash fully deployed.")
        lines.append(f"  Free ~{freed:,.0f} SAR (exit/trim below); fund {_adds_txt} SAR"
                     f" (~{_adds_total:,.0f} total){_tail}")
    else:
        lines.append("  No high-confidence portfolio action today.")
    lines += ["", f"BOOK: {d['pl_pct']:+.1f}% overall ({d['mv']:,.0f} vs {d['cost']:,.0f} SAR)", ""]
    # v1.7.0 (A): one-line data-vintage summary (dominant dates per page).
    _vin_t = model.get("vintage", {}) or {}
    _vparts: List[str] = []
    for _pg in MARKET_PAGES:
        _vd = (_vin_t.get(_pg) or {}).get("dates") or []
        if _vd:
            _vparts.append(_PAGE_ABBREV.get(_pg, _pg) + " " +
                           "/".join(f"{d[5:]}x{c}" for d, c in _vd[:2]))
    if _vparts:
        lines.append("DATA VINTAGE: " + "; ".join(_vparts))
        lines.append("")
    if d["sell"]:
        lines.append("EXIT:  " + "; ".join(f"{r['symbol']} ({_pct(r['pl'])})" for r in d["sell"]))
    if d["trim"]:
        lines.append("TRIM:  " + "; ".join(f"{r['symbol']} (~{r['sar']:,.0f} SAR)" for r in d["trim"]))
    if d["add"]:
        lines.append("ADD:   " + "; ".join(f"{r['symbol']} (~{r['sar']:,.0f} SAR)" for r in d["add"]))
        _mx = model.get("metrics", {}) or {}
        for r in d["add"]:
            _m = _mx.get(r["symbol"].upper())
            if _m:
                lines.append(f"       {r['symbol']} outlook: 1M {_pct(_m.get('roi1m'))} / "
                             f"3M {_pct(_m.get('roi3m'))} / 12M {_pct(_m.get('roi12m'))} "
                             f"(valuation path, reliability {_num_str(_m.get('rel'))})")
    if d["hold"]:
        lines.append("HOLD (data too weak to act): " + ", ".join(r["symbol"] for r in d["hold"]))
    if t["top"]:
        lines += ["", "BEST NEW BUYS (to fair value; a target, not a forecast):"]
        _mx2 = model.get("metrics", {}) or {}
        for i, p in enumerate(t["top"], 1):
            _m2 = _mx2.get(p["symbol"].upper())
            _ol = (f" | 1M {_pct(_m2.get('roi1m'))} / 3M {_pct(_m2.get('roi3m'))}" if _m2 else "")
            _sb = f" [same issuer as held {p['sibling']}]" if p.get("sibling") else ""
            lines.append(f"  {i}. {p['symbol']} {p['name'][:34]} - {_pct(p['roi'],0)} "
                         f"[{p['market']}, reliability {_num_str(p['rel'])}, {p['conf']}]{_ol}{_sb}")
    # v1.4.0: end-of-brief action summary (owner request 2026-07-05)
    lines += ["", "ACTION SUMMARY"]
    for label, grp, cash in (("EXIT/SELL", d["sell"], True), ("TRIM", d["trim"], True),
                             ("ADD", d["add"], True), ("HOLD", d["hold"], False)):
        if not grp:
            continue
        sar = _grp_cash(grp)
        tail = f" (~{sar:,.0f} SAR)" if (cash and sar) else ""
        lines.append(f"  {label}: {len(grp)} - " + ", ".join(r["symbol"] for r in grp) + tail)
    if t["top"]:
        lines.append(f"  NEW BUYS: {len(t['top'])} - " + ", ".join(p["symbol"] for p in t["top"]))
    _pl = f"{d['pl_pct']:+.1f}%" if d.get("pl_pct") is not None else "-"
    lines.append(f"  BOOK: {d['mv']:,.0f} SAR | P&L {_pl} | freed cash ~{d['freed_cash']:,.0f} SAR")
    lines += ["", "When you can act (Riyadh): Tadawul 10:00-15:00 Sun-Thu - US 16:30-23:00 - "
              "Europe ~10:00-19:30 - Tokyo/HK early morning.", ""]
    return "\n".join(lines)


def build_subject(model: Dict[str, Any], when: _dt.datetime) -> str:
    d = model["decision"]
    # v1.6.0 (B4): the subject named only the first add; name them all (cap 3).
    _adds = [r["symbol"] for r in d["add"]]
    add = ", ".join(_adds[:3]) + ("…" if len(_adds) > 3 else "") if _adds else None
    teaser = (f"free ~{d['freed_cash']:,.0f} SAR, add {add}"
              if add and d["freed_cash"] else "portfolio + market update")
    return f"Daily Investment Brief — {when:%d %b %Y} · {teaser}"


def _mail_config() -> Dict[str, Any]:
    return {
        "host": (os.getenv("TFB_MAIL_SMTP_HOST") or "smtp.gmail.com").strip(),
        "port": int((os.getenv("TFB_MAIL_SMTP_PORT") or "587").strip()),
        "user": (os.getenv("TFB_MAIL_FROM") or "").strip(),
        "password": (os.getenv("TFB_MAIL_PASSWORD") or "").strip(),
        "to": (os.getenv("TFB_MAIL_TO") or "").strip(),
        "from_name": os.getenv("TFB_BRIEF_OWNER_NAME", "Emad Bahbah"),
    }


def send_email(html_body: str, text_body: str, subject: str,
               cfg: Optional[Dict[str, Any]] = None,
               pdf_bytes: Optional[bytes] = None,
               pdf_filename: str = "daily_brief.pdf") -> None:
    """Send the brief over SMTP (STARTTLS on 587 by default, implicit SSL on 465)."""
    import smtplib
    import ssl
    from email.message import EmailMessage
    from email.utils import formataddr, formatdate, make_msgid

    cfg = cfg or _mail_config()
    missing = [k for k in ("user", "password", "to") if not cfg.get(k)]
    if missing:
        raise RuntimeError("missing mail secrets: "
                           + ", ".join({"user": "TFB_MAIL_FROM", "password": "TFB_MAIL_PASSWORD",
                                        "to": "TFB_MAIL_TO"}[k] for k in missing))

    recipients = [a.strip() for a in cfg["to"].replace(";", ",").split(",") if a.strip()]
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = formataddr((f'{cfg["from_name"]} — Investment Brief', cfg["user"]))
    msg["To"] = ", ".join(recipients)
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()
    msg.set_content(text_body or "Daily Investment Brief")
    msg.add_alternative(html_body, subtype="html")
    if pdf_bytes:  # v1.4.0: PDF copy of the recommendations
        msg.add_attachment(pdf_bytes, maintype="application", subtype="pdf",
                           filename=pdf_filename)

    ctx = ssl.create_default_context()
    if cfg["port"] == 465:
        with smtplib.SMTP_SSL(cfg["host"], cfg["port"], context=ctx, timeout=45) as s:
            s.login(cfg["user"], cfg["password"])
            s.send_message(msg, from_addr=cfg["user"], to_addrs=recipients)
    else:
        with smtplib.SMTP(cfg["host"], cfg["port"], timeout=45) as s:
            s.ehlo(); s.starttls(context=ctx); s.ehlo()
            s.login(cfg["user"], cfg["password"])
            s.send_message(msg, from_addr=cfg["user"], to_addrs=recipients)
    sys.stderr.write(f"[brief] emailed -> {', '.join(recipients)}"
                     + (f" (+ {pdf_filename})" if pdf_bytes else "") + "\n")


# ----------------------------------------------------------------------------- #
# Entry point
# ----------------------------------------------------------------------------- #
def generate(pages_data: Dict[str, List[List[Any]]], owner: str,
             when: Optional[_dt.datetime] = None) -> str:
    model = build_model(pages_data)
    return render_html(model, owner, when or _dt.datetime.now())


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Generate (and optionally email) the Daily Investment Brief.")
    ap.add_argument("--xlsx", default=os.getenv("TFB_BRIEF_LOCAL_XLSX", ""),
                    help="read pages from a local xlsx instead of the live sheet")
    ap.add_argument("--out", default=os.getenv("TFB_BRIEF_OUTPUT_PATH", "daily_brief.html"))
    ap.add_argument("--send", action="store_true",
                    default=(os.getenv("TFB_BRIEF_SEND", "").strip() == "1"),
                    help="email the brief after rendering (needs TFB_MAIL_* secrets)")
    args = ap.parse_args(argv)

    owner = os.getenv("TFB_BRIEF_OWNER_NAME", "Emad Bahbah")

    if args.xlsx:
        pages = read_pages_xlsx(args.xlsx, ALL_PAGES)
    else:
        sid = (os.getenv("DEFAULT_SPREADSHEET_ID") or os.getenv("SPREADSHEET_ID") or "").strip()
        if not sid:
            sys.stderr.write("[brief] ERROR: DEFAULT_SPREADSHEET_ID / SPREADSHEET_ID not set\n")
            return 2
        pages = read_pages_live(sid, ALL_PAGES)

    when = _dt.datetime.now()
    model = build_model(pages)
    html_out = render_html(model, owner, when)

    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write(html_out)
    sys.stderr.write(f"[brief] wrote {len(html_out):,} bytes -> {args.out}\n")

    # v1.4.0: PDF copy of the recommendations (owner request 2026-07-05).
    # TFB_BRIEF_PDF=0 is the kill-switch; any failure degrades to no attachment.
    pdf_bytes: Optional[bytes] = None
    if (os.getenv("TFB_BRIEF_PDF", "1").strip() or "1") != "0":
        pdf_bytes = render_pdf(model, owner, when)
        if pdf_bytes:
            pdf_path = (os.path.splitext(args.out)[0] or "daily_brief") + ".pdf"
            try:
                with open(pdf_path, "wb") as fh:
                    fh.write(pdf_bytes)
                sys.stderr.write(f"[brief] wrote {len(pdf_bytes):,} bytes -> {pdf_path}\n")
            except OSError as exc:
                sys.stderr.write(f"[brief] WARN: could not write {pdf_path}: {exc}\n")

    if args.send:
        text_out = render_text(model, owner, when)
        subject = build_subject(model, when)
        try:
            send_email(html_out, text_out, subject,
                       pdf_bytes=pdf_bytes,
                       pdf_filename=f"Investment_Brief_{when:%Y-%m-%d_%H%M}.pdf")
        except Exception as exc:
            sys.stderr.write(f"[brief] ERROR sending email: {exc}\n")
            return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
