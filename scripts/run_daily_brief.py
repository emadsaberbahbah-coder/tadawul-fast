#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_daily_brief.py — "Emad Bahbah — Daily Investment Brief" generator.

WHAT THIS DOES
    Reads the production workbook's decision + opportunity + market pages and renders
    a polished, color-coded HTML brief (the design approved 2026-06-30). It is
    READ-ONLY: it never writes to the sheet. Delivery (email / WhatsApp) is a
    SEPARATE step and is intentionally NOT in this file.

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
    TFB_BRIEF_OWNER_NAME                      masthead name (default: "Emad Bahbah")

USAGE
    python run_daily_brief.py                         # live (CI): reads sheet via read_range
    python run_daily_brief.py --xlsx file.xlsx        # local verification against a snapshot
    python run_daily_brief.py --xlsx file.xlsx --out brief.html
"""
from __future__ import annotations

__version__ = "1.0.0"

import argparse
import datetime as _dt
import html as _html
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
MARKET_PAGES = ["Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds"]
ALL_PAGES = [PAGE_DECISION, PAGE_TOP10] + MARKET_PAGES

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
    """Render a fraction-or-percent value as a percent string.

    Sheet values are stored inconsistently: P&L % as whole numbers (-20.0),
    forecast ROI as fractions (0.568). We treat |x|<=1.5 as a fraction.
    """
    if x is None:
        return "—"
    v = x * 100.0 if abs(x) <= 1.5 else x
    return f"{v:+.{digits}f}%"


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


def read_pages_xlsx(path: str, pages: List[str]) -> Dict[str, List[List[Any]]]:
    import pandas as pd  # local import: verification path only

    xl = pd.ExcelFile(path)
    out: Dict[str, List[List[Any]]] = {}
    for p in pages:
        if p in xl.sheet_names:
            df = pd.read_excel(path, sheet_name=p, header=None, dtype=object)
            out[p] = df.where(df.notna(), None).values.tolist()
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
            out["add"].append(rec)
        else:
            out["hold"].append(rec)
    out["pl_pct"] = ((out["mv"] - out["cost"]) / out["cost"] * 100.0) if out["cost"] else None
    return out


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
    return {"total": total, "invest": len(invest), "top": invest[:top_n]}


# ----------------------------------------------------------------------------- #
# Build model (parse all pages)
# ----------------------------------------------------------------------------- #
def build_model(pages_data: Dict[str, List[List[Any]]]) -> Dict[str, Any]:
    decision = extract_decision(pages_data.get(PAGE_DECISION, []))
    held = {r["symbol"].upper() for grp in ("sell", "trim", "add", "hold") for r in decision[grp]}
    top10 = extract_top10(pages_data.get(PAGE_TOP10, []), exclude=held)
    market = {p: extract_market_page(pages_data.get(p, [])) for p in MARKET_PAGES}
    return {"decision": decision, "top10": top10, "market": market}


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


def _opp_row(rank: int, p: Dict[str, Any]) -> str:
    conf = p["conf"].title()
    cc = ADD_C if conf.lower() == "high" else (TRIM_C if conf.lower() in ("medium", "moderate") else "#888")
    return f"""<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:7px; background:#E9F5EF; border:1px solid #C7E4D5;"><tr>
      <td style="padding:12px 12px; width:34px;"><div style="width:24px; height:24px; background:{ADD_C}; color:#fff; border-radius:50%; text-align:center; line-height:24px; font-family:{SANS}; font-size:12px; font-weight:bold;">{rank}</div></td>
      <td style="padding:11px 6px; font-family:{SANS};"><div style="font-size:14px; color:#1A1A1A;"><strong>{_esc(p['symbol'])}</strong> <span style="color:#777; font-size:12px;">{_esc(p['name'])[:30]}</span></div><div style="font-size:11px; color:#8A8A8A; margin-top:2px;">{_esc(p['sector'])} · {_esc(p['market'])}</div></td>
      <td align="right" style="padding:11px 14px; font-family:{SANS}; white-space:nowrap;"><div style="font-size:13px; color:#0E7C5A;"><strong>{_pct(p['roi'],0)} to fair value</strong></div><div style="font-size:11px; color:#888;">Reliability {_num_str(p['rel'])} · <span style="color:{cc};">{conf}</span></div></td></tr></table>"""


def _num_str(x: Optional[float]) -> str:
    return "—" if x is None else f"{x:.0f}"


def render_html(model: Dict[str, Any], owner: str, when: _dt.datetime) -> str:
    d = model["decision"]; t = model["top10"]; mk = model["market"]
    date_long = when.strftime("%A").rstrip()
    date_short = when.strftime("%d %B %Y")

    # ---- hero numbers ----
    freed = d["freed_cash"]
    add = d["add"][0] if d["add"] else None
    add_sar = add["sar"] if add else 0.0
    dry = max(freed - add_sar, 0.0)
    sell_syms = ", ".join(r["symbol"].split(".")[0] for r in d["sell"][:3]) or "—"
    trim_sym = d["trim"][0]["symbol"].split(".")[0] if d["trim"] else "—"
    add_sym = add["symbol"] if add else "—"

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
        for r in d["add"])

    hold_rows = "".join(_hold_row(r) for r in d["hold"])

    # ---- opportunities ----
    opp_rows = "".join(_opp_row(i + 1, p) for i, p in enumerate(t["top"]))
    rest_rows = "".join(_rest_row(market, names) for market, names in t["rest"].items()) if t["rest"] else ""
    rest_block = f"""
    <div style="font-family:{SERIF}; font-size:13px; color:#0E7C5A; margin:14px 2px 8px 2px;">Across the rest of your markets</div>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#F2F8F5; border:1px solid #D5E8DE;">{rest_rows}</table>""" if rest_rows else ""

    # ---- per-page strip ----
    page_rows = "".join(_page_row(name, mk[name]) for name in MARKET_PAGES)

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{_esc(owner)} — Daily Investment Brief</title></head>
<body style="margin:0; padding:0; background:#E7E4DC; -webkit-text-size-adjust:100%;">
<div style="display:none; max-height:0; overflow:hidden; opacity:0; color:#E7E4DC; font-size:1px; line-height:1px;">Today's move: free ~{_money(freed)} SAR, add {add_sym}, keep the rest as dry powder.</div>
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

  <tr><td style="padding:26px 32px 6px 32px;">
    <div style="font-family:{SANS}; font-size:11px; letter-spacing:2px; color:{INK}; font-weight:bold;">TODAY'S ONE MOVE</div>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-top:10px; border:1px solid #C9D2C4; border-left:4px solid {ADD_C}; background:#F2F8F4;"><tr><td style="padding:18px 20px;">
      <div style="font-family:{SERIF}; font-size:20px; color:#1A1A1A; line-height:1.4;">Free up <strong style="color:{INK};">~{_money(freed)}&nbsp;SAR</strong>, then add <strong style="color:{ADD_C};">{_esc(add_sym)}</strong></div>
      <div style="font-family:{SANS}; font-size:14px; color:#4A4A4A; line-height:1.65; margin-top:8px;">Exit the weak names (<strong>{_esc(sell_syms)}</strong>) and trim your overweight <strong>{_esc(trim_sym)}</strong> back to cap — about <strong>{_money(freed)}&nbsp;SAR</strong> freed. Put <strong>~{_money(add_sar)}&nbsp;SAR</strong> into {_esc(add_sym)}, the one buy the engine backs at high confidence. The remaining <strong style="color:{INK};">~{_money(dry)}&nbsp;SAR is dry powder</strong> for the new opportunities below.</div>
      <div style="font-family:{SANS}; font-size:12px; color:#888; line-height:1.6; margin-top:10px; padding-top:9px; border-top:1px dashed #CDD8CC;">All high-confidence calls. A plan for your decision, not an instruction.</div>
    </td></tr></table>
  </td></tr>

  <tr><td style="padding:18px 32px 4px 32px;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#F6F4EF; border:1px solid #E6E1D6;"><tr><td style="padding:14px 18px; font-family:{SANS}; font-size:13px; color:#3A3A3A; line-height:1.7;">
      <strong style="font-family:{SERIF}; font-size:14px;">Book snapshot.</strong> {len(d['sell'])+len(d['trim'])+len(d['add'])+len(d['hold'])} holdings · <strong style="color:{SELL_C};">{_pct(d['pl_pct'])}</strong> overall ({_money(d['mv'])} vs {_money(d['cost'])}&nbsp;SAR). {len(d['hold'])} positions are <em>held only because the data is too weak to act</em> — not comfort holds. {_concentration(d)}
    </td></tr></table>
  </td></tr>

  <tr><td style="padding:22px 32px 0 32px;"><div style="font-family:{SERIF}; font-size:16px; color:{INK}; border-bottom:2px solid {INK}; padding-bottom:6px;">Act now <span style="font-family:{SANS}; font-size:11px; color:#7E8AA0; letter-spacing:0.5px;">HIGH-CONFIDENCE CALLS</span></div></td></tr>
  <tr><td style="padding:10px 32px 0 32px;">{sell_rows}{trim_rows}{add_rows}</td></tr>

  <tr><td style="padding:18px 32px 0 32px;"><div style="font-family:{SERIF}; font-size:16px; color:#46535F; border-bottom:2px solid {HOLD_C}; padding-bottom:6px;">Hold &mdash; no action <span style="font-family:{SANS}; font-size:11px; color:#8C97A3; letter-spacing:0.5px;">DATA TOO WEAK TO ACT</span></div></td></tr>
  <tr><td style="padding:10px 32px 0 32px;">
    <div style="font-family:{SANS}; font-size:12px; color:#555; line-height:1.6; margin-bottom:9px;">The engine leans bearish on most of these, but their data reliability is too low to act on — so they're capped to hold. Verify before any move.</div>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#F0F2F4; border:1px solid #DDE2E7;">{hold_rows}</table>
  </td></tr>

  <tr><td style="padding:20px 32px 0 32px;"><div style="font-family:{SERIF}; font-size:16px; color:#0E7C5A; border-bottom:2px solid {ADD_C}; padding-bottom:6px;">Best new buys <span style="font-family:{SANS}; font-size:11px; color:#8A958E; letter-spacing:0.5px;">ALL MARKETS · WHERE YOUR DRY POWDER GOES</span></div></td></tr>
  <tr><td style="padding:10px 32px 0 32px;">
    <div style="font-family:{SANS}; font-size:12px; color:#555; line-height:1.6; margin-bottom:10px;">Ranked entry candidates you <strong>don't hold</strong>, screened across <strong>all your markets</strong>. The figure is the gap to estimated <strong>fair value — a valuation target, not a prediction</strong>; reliability and confidence show how much weight to give each. Full entry/stop/target levels are in your Top&nbsp;10 sheet.</div>
    {opp_rows}{rest_block}
    <div style="font-family:{SANS}; font-size:11px; color:#A09B90; line-height:1.6; padding:8px 2px 0 2px; font-style:italic;">Live brief refreshes the full cross-market list each morning and always excludes names you hold.</div>
  </td></tr>

  <tr><td style="padding:20px 32px 0 32px;"><div style="font-family:{SERIF}; font-size:16px; color:#46535F; border-bottom:2px solid {HOLD_C}; padding-bottom:6px;">Across your market pages</div></td></tr>
  <tr><td style="padding:10px 32px 0 32px;"><table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#F5F4F0; border:1px solid #E2DDD2;">{page_rows}</table></td></tr>

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
    label = ", ".join(f"<strong>{_esc(n['symbol'])}</strong>" for n in names[:4])
    return f"""<tr style="font-family:{SANS};"><td style="padding:9px 14px; font-size:12px; color:#0E7C5A; font-weight:bold; width:110px; vertical-align:top; border-bottom:1px solid #E2EFE8;">{_esc(market)}</td><td style="padding:9px 14px; font-size:12px; color:#444; line-height:1.5; border-bottom:1px solid #E2EFE8;">{label}</td></tr>"""


def _page_row(name: str, info: Dict[str, Any]) -> str:
    pretty = name.replace("_", " ")
    inv = info["invest"]
    if inv == 0:
        note = "No signals clear the bar — <em>monitoring only</em>."
        ncol = "#777"
    elif inv <= 2:
        nm = info["top"][0]["name"][:34] if info["top"] else ""
        note = f"Only {_esc(nm)} clears — effectively <em>monitoring only</em>."
        ncol = "#777"
    else:
        names = ", ".join(_esc(t["name"].split()[0]) for t in info["top"][:5])
        note = f"Top names: {names}."
        ncol = "#555"
    return f"""<tr style="font-family:{SANS};"><td style="padding:10px 14px; font-size:13px; color:#1A1A1A; vertical-align:top; border-bottom:1px solid #E8E3D8; width:150px;"><strong>{_esc(pretty)}</strong><br><span style="font-size:11px; color:#999;">{info['total']:,} names · {inv} investable</span></td><td style="padding:10px 14px; font-size:12px; color:{ncol}; line-height:1.5; border-bottom:1px solid #E8E3D8;">{note}</td></tr>"""


# ----------------------------------------------------------------------------- #
# Entry point
# ----------------------------------------------------------------------------- #
def generate(pages_data: Dict[str, List[List[Any]]], owner: str,
             when: Optional[_dt.datetime] = None) -> str:
    model = build_model(pages_data)
    return render_html(model, owner, when or _dt.datetime.now())


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Generate the Daily Investment Brief HTML.")
    ap.add_argument("--xlsx", default=os.getenv("TFB_BRIEF_LOCAL_XLSX", ""),
                    help="read pages from a local xlsx instead of the live sheet")
    ap.add_argument("--out", default=os.getenv("TFB_BRIEF_OUTPUT_PATH", "daily_brief.html"))
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

    html_out = generate(pages, owner)
    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write(html_out)
    sys.stderr.write(f"[brief] wrote {len(html_out):,} bytes -> {args.out}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
