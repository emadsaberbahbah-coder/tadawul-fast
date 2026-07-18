"""
core/compliance_gate.py — TFB Generation-2 Layer-0 Compliance & Eligibility Gate
================================================================================
VERSION 1.0.0  (2026-07-18)  — NEW MODULE (first deliverable of Wave A0)

WHY (root-cause evidence, per TFB_GenII_Master_Plan_v2.1 §4, §19 and Decision_Log):
  * Gen-1 had no compliance layer: Nomu names (e.g. 9628.SR) could reach INVEST
    boards despite the operator's TASI-only regulation; conventional banks sat
    un-flagged; 5023.SR (a sukuk) was scored as an equity for weeks.
  * D-2 (2026-07-18): GENERAL ACCEPTANCE — MODEL_SCREEN_PASS_NOT_FATWA is
    INVEST-eligible on all broker-tradable markets; label stays visible forever;
    authority statuses override wherever official coverage exists.
  * §19 (2026-07-18): broker-constrained universe — Derayah market matrix,
    ticket floors vs the position cap (floor-vs-cap law), ADR preference.

DESIGN RULES:
  * Pure library: no network, no Sheets I/O. Callers supply rows/authority data;
    the quarterly refresh script owns fetching. Deterministic + replayable.
  * Environment-gated, backward compatible: with TFB_COMPLIANCE_GATE_ENABLED
    unset/0 the gate stamps informationally but blocks nothing (Champion
    behavior unchanged until Shadow wiring turns it on).
  * Safe direction: UNKNOWN / DATA_STALE / CONFLICT are never INVEST-eligible.

ENV (all optional):
  TFB_COMPLIANCE_GATE_ENABLED          "0" (default) | "1"
  TFB_COMPLIANCE_MODEL_SCREEN_ENABLED  "1" (default; D-2 general acceptance) | "0"
  TFB_COMPLIANCE_AUTHORITY_MAX_AGE_DAYS "120"
  TFB_COMPLIANCE_DEBT_RATIO_MAX        "0.30"   (Rajhi-family threshold)
  TFB_COMPLIANCE_IMPURE_INCOME_MAX     "0.05"   (Rajhi-family threshold)
  TFB_COMPLIANCE_MAX_POSITION_PCT      "0.15"   (floor-vs-cap law input)
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

__version__ = "1.0.0"
COMPLIANCE_GATE_VERSION = __version__

# --------------------------------------------------------------------------- #
# Status vocabulary (Master Plan v2.1 §4.2)                                    #
# --------------------------------------------------------------------------- #
AUTHORITY_PASS = "AUTHORITY_PASS"
AUTHORITY_FAIL = "AUTHORITY_FAIL"
MODEL_SCREEN_PASS = "MODEL_SCREEN_PASS_NOT_FATWA"
MODEL_SCREEN_FAIL = "MODEL_SCREEN_FAIL"
UNKNOWN = "UNKNOWN"
DATA_STALE = "DATA_STALE"
CONFLICT = "CONFLICT"
VENUE_BLOCK = "VENUE_BLOCK"
INSTRUMENT_BLOCK = "INSTRUMENT_BLOCK"
BROKER_TRADABLE = "BROKER_TRADABLE"
BROKER_UNTRADABLE = "BROKER_UNTRADABLE"
FLOOR_LOCKED = "FLOOR_LOCKED"
GATE_DISABLED = "GATE_DISABLED"

INVEST_OK_STATUSES = {AUTHORITY_PASS, MODEL_SCREEN_PASS}

# Asset classes (§5)
EQUITY = "EQUITY"
ETF = "ETF"
REIT = "REIT"
SUKUK = "SUKUK"
COMMODITY_ETP = "COMMODITY_ETP"
FX_INSTRUMENT = "FX_INSTRUMENT"
FUND_OTHER = "FUND_OTHER"

# --------------------------------------------------------------------------- #
# ENV helpers                                                                  #
# --------------------------------------------------------------------------- #
def _env_flag(name: str, default: str) -> bool:
    return (os.getenv(name) or default).strip().lower() in ("1", "true", "yes", "on")

def _env_float(name: str, default: str) -> float:
    try:
        return float((os.getenv(name) or default).strip())
    except Exception:
        return float(default)

def gate_enabled() -> bool:
    return _env_flag("TFB_COMPLIANCE_GATE_ENABLED", "0")

def model_screen_enabled() -> bool:
    return _env_flag("TFB_COMPLIANCE_MODEL_SCREEN_ENABLED", "1")

def authority_max_age_days() -> int:
    return int(_env_float("TFB_COMPLIANCE_AUTHORITY_MAX_AGE_DAYS", "120"))

def debt_ratio_max() -> float:
    return _env_float("TFB_COMPLIANCE_DEBT_RATIO_MAX", "0.30")

def impure_income_max() -> float:
    return _env_float("TFB_COMPLIANCE_IMPURE_INCOME_MAX", "0.05")

def max_position_pct() -> float:
    return _env_float("TFB_COMPLIANCE_MAX_POSITION_PCT", "0.15")

# --------------------------------------------------------------------------- #
# Broker tradability matrix + venue ticket floors (§18.5, §19.1, §19.3)        #
# Floors are SAR, derived from the OFFICIAL Derayah Global minimums so that    #
# round-trip cost <= 0.8% incl. ~0.1% spread. FX constants of 2026-07-18       #
# (documented; the slippage ledger reconciles actuals).                        #
# --------------------------------------------------------------------------- #
DERAYAH_MARKETS: Dict[str, Tuple[str, int]] = {
    "":     ("USA",        5000),    # no suffix  -> US line via Global Lite
    ".US":  ("USA",        5000),
    ".SR":  ("Saudi",      4000),
    ".T":   ("Japan",     13200),
    ".HK":  ("HongKong",  27200),
    ".L":   ("UK",        27700),
    ".PA":  ("Eurozone",  24500),
    ".AS":  ("Eurozone",  24500),
    ".BR":  ("Eurozone",  24500),
    ".DE":  ("Eurozone",  24500),
    ".MI":  ("Eurozone",  24500),
    ".MC":  ("Eurozone",  24500),
    ".LS":  ("Eurozone",  24500),
    ".VI":  ("Eurozone",  24500),
    ".SW":  ("Switzerland", 24200),
    ".TO":  ("Canada",    15500),
    ".AX":  ("Australia", 14100),
    ".OL":  ("Norway",    20100),
    ".SI":  ("Singapore", 16700),
    ".MX":  ("Mexico",    11000),
}

_SUFFIX_RE = re.compile(r"\.([A-Za-z]{1,3})$")

def symbol_suffix(symbol: str) -> str:
    m = _SUFFIX_RE.search(str(symbol).strip())
    return ("." + m.group(1).upper()) if m else ""

def tradability(symbol: str) -> Tuple[str, Optional[str], Optional[int]]:
    """-> (BROKER_TRADABLE|BROKER_UNTRADABLE, market_name|None, floor_sar|None)"""
    suf = symbol_suffix(symbol)
    hit = DERAYAH_MARKETS.get(suf)
    if hit is None:
        return BROKER_UNTRADABLE, None, None
    return BROKER_TRADABLE, hit[0], hit[1]

def floor_vs_cap(symbol: str, equity_sar: float,
                 cap_pct: Optional[float] = None) -> Tuple[bool, Optional[int], Optional[float]]:
    """§19.3 — (unlocked?, floor, unlock_equity). Untradable -> (False, None, None)."""
    st, _mk, floor = tradability(symbol)
    if st != BROKER_TRADABLE or floor is None:
        return False, None, None
    cap = cap_pct if cap_pct is not None else max_position_pct()
    unlock_at = floor / cap if cap > 0 else float("inf")
    return (floor <= cap * float(equity_sar)), floor, unlock_at

# --------------------------------------------------------------------------- #
# Venue (Tadawul main vs Nomu). Numeric 9000-9999 -> Nomu (override table in   #
# _Lists_Config is authoritative once loaded; heuristic documented in §4.4).   #
# --------------------------------------------------------------------------- #
def saudi_venue(symbol: str, override: Optional[Dict[str, str]] = None) -> Optional[str]:
    s = str(symbol).strip()
    if not s.endswith(".SR"):
        return None
    if override and s in override:
        return override[s].upper()
    code = s[:-3]
    if code.isdigit() and 9000 <= int(code) <= 9999:
        return "NOMU"
    return "TASI"

# --------------------------------------------------------------------------- #
# Asset-class taxonomy (§5). Explicit map wins; then quote-type; then name.    #
# --------------------------------------------------------------------------- #
EXPLICIT_ASSET_CLASS: Dict[str, str] = {
    "5023.SR": SUKUK,   # Arabian Centres Sukuk — the founding example (D-5)
}

_ETF_PAT = re.compile(r"\b(ETF|UCITS|INDEX FUND|ISHARES|SPDR|VANGUARD|INVESCO|WISDOMTREE|FIRST TRUST)\b", re.I)
_REIT_PAT = re.compile(r"\bREIT\b|\bريت\b", re.I)
_SUKUK_PAT = re.compile(r"\bSUKUK\b|\bصكوك\b|\bBOND\b", re.I)
_CMDTY_PAT = re.compile(r"\b(GOLD|SILVER|OIL|COMMODITY|BITCOIN|CRYPTO|FUTURES?)\b", re.I)

def classify_asset(symbol: str, name: str = "", quote_type: str = "") -> str:
    s = str(symbol).strip()
    if s in EXPLICIT_ASSET_CLASS:
        return EXPLICIT_ASSET_CLASS[s]
    qt = (quote_type or "").upper()
    if qt in ("ETF", "MUTUALFUND"):
        base = ETF
    elif qt in ("EQUITY", "STOCK", ""):
        base = EQUITY
    else:
        base = FUND_OTHER
    nm = name or ""
    if _SUKUK_PAT.search(nm):
        return SUKUK
    if _REIT_PAT.search(nm):
        return REIT
    if _CMDTY_PAT.search(nm) and base != EQUITY:
        return COMMODITY_ETP
    if _ETF_PAT.search(nm):
        return ETF
    if "=" in s or s.endswith("=X"):
        return FX_INSTRUMENT
    return base

# Instrument permissions (§4.4): operator matrix; safe defaults.
DEFAULT_PERMISSIONS: Dict[str, bool] = {
    EQUITY: True, ETF: True, REIT: True, SUKUK: True,
    COMMODITY_ETP: False, FX_INSTRUMENT: False, FUND_OTHER: False,
}

# --------------------------------------------------------------------------- #
# Activity screen (Tier-1). Conventional finance & haram activities.           #
# Takaful / Islamic bank names are exempted before the block scan.             #
# --------------------------------------------------------------------------- #
_ACTIVITY_EXEMPT = re.compile(r"\b(ISLAMIC|TAKAFUL|SHARIA|SHARIAH|الراجحي|الإنماء|البلاد|الجزيرة)\b", re.I)
_ACTIVITY_BLOCK = re.compile(
    r"\b(BANK|BANKS|BANKING|INSURANCE|ASSURANCE|REINSURANCE|CASINO|GAMBLING|"
    r"BETTING|LOTTERY|ALCOHOL|BREWER|DISTILLER|WINER|TOBACCO|CIGARETTE|"
    r"ADULT|PORK)\b", re.I)

def activity_screen(name: str, sector: str = "", industry: str = "") -> Tuple[bool, str]:
    """-> (clean?, reason). Conservative: block-hit without exemption = not clean."""
    blob = " ".join(x for x in (name, sector, industry) if x)
    if not blob.strip():
        return False, "activity_undisclosed"
    if _ACTIVITY_EXEMPT.search(blob):
        return True, "activity_islamic_exempt"
    if _ACTIVITY_BLOCK.search(blob):
        return False, "activity_blocked:" + _ACTIVITY_BLOCK.search(blob).group(1).lower()
    return True, "activity_clean"

# --------------------------------------------------------------------------- #
# Authority table (§4.1). Rows: {symbol, status: PASS|FAIL, as_of: date/ISO,   #
# source}. Monitor rows (Argaam) optional -> CONFLICT on disagreement.         #
# --------------------------------------------------------------------------- #
def _as_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def build_authority_index(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for r in rows or []:
        sym = str(r.get("symbol", "")).strip()
        if sym:
            idx[sym] = {
                "status": str(r.get("status", "")).strip().upper(),
                "as_of": _as_date(r.get("as_of")),
                "source": str(r.get("source", "AL_RAJHI_OFFICIAL")),
            }
    return idx

def authority_lookup(symbol: str, index: Dict[str, Dict[str, Any]],
                     monitor: Optional[Dict[str, str]] = None,
                     today: Optional[date] = None) -> Tuple[Optional[str], List[str]]:
    """-> (AUTHORITY_PASS|AUTHORITY_FAIL|DATA_STALE|CONFLICT|None, reasons)."""
    rec = index.get(str(symbol).strip())
    if not rec:
        return None, []
    reasons = [f"authority_source:{rec['source']}", f"authority_as_of:{rec['as_of']}"]
    t = today or date.today()
    if rec["as_of"] is None or (t - rec["as_of"]).days > authority_max_age_days():
        return DATA_STALE, reasons + ["authority_stale"]
    if monitor:
        m = str(monitor.get(str(symbol).strip(), "")).strip().upper()
        if m and m != rec["status"]:
            return CONFLICT, reasons + [f"monitor_disagrees:{m}"]
    if rec["status"] == "PASS":
        return AUTHORITY_PASS, reasons
    if rec["status"] == "FAIL":
        return AUTHORITY_FAIL, reasons
    return None, reasons + ["authority_status_unreadable"]

# --------------------------------------------------------------------------- #
# Model screen (§4.2): Rajhi-family ratios applied mechanically. NOT a fatwa.  #
# --------------------------------------------------------------------------- #
def model_screen(name: str, sector: str, industry: str,
                 interest_debt: Optional[float], market_cap: Optional[float],
                 impure_income_ratio: Optional[float]) -> Tuple[str, List[str]]:
    reasons: List[str] = []
    clean, why = activity_screen(name, sector, industry)
    reasons.append(why)
    if not clean:
        return (MODEL_SCREEN_FAIL, reasons) if why.startswith("activity_blocked") \
               else (UNKNOWN, reasons)
    if interest_debt is None or not market_cap:
        return UNKNOWN, reasons + ["debt_ratio_inputs_missing"]
    ratio = float(interest_debt) / float(market_cap)
    reasons.append(f"debt_to_mcap:{ratio:.3f}")
    if ratio > debt_ratio_max():
        return MODEL_SCREEN_FAIL, reasons + ["debt_ratio_exceeds_max"]
    if impure_income_ratio is None:
        # Board's conservative-estimation principle: activity clean + debt pass,
        # undisclosed impure income -> pass WITH flag (purification advised).
        return MODEL_SCREEN_PASS, reasons + ["impure_income_undisclosed_conservative"]
    reasons.append(f"impure_income:{float(impure_income_ratio):.3f}")
    if float(impure_income_ratio) > impure_income_max():
        return MODEL_SCREEN_FAIL, reasons + ["impure_income_exceeds_max"]
    return MODEL_SCREEN_PASS, reasons

# --------------------------------------------------------------------------- #
# Purification (§4.3): authority per-share figure overrides; else ratio x      #
# dividends received (practical retail basis; documented).                     #
# --------------------------------------------------------------------------- #
def purification_amount(dividends_received: float,
                        impure_income_ratio: Optional[float],
                        authority_per_share: Optional[float] = None,
                        shares: Optional[float] = None) -> Tuple[Optional[float], str]:
    if authority_per_share is not None and shares:
        return round(float(authority_per_share) * float(shares), 2), "authority_per_share"
    if impure_income_ratio is None:
        return None, "unpayable_unknown_flag"
    return round(float(dividends_received) * float(impure_income_ratio), 2), "ratio_x_dividends"

# --------------------------------------------------------------------------- #
# Master verdict                                                               #
# --------------------------------------------------------------------------- #
@dataclass
class ComplianceVerdict:
    symbol: str
    asset_class: str
    tradability: str
    market: Optional[str]
    venue: Optional[str]
    shariah_status: str
    shariah_source: str
    invest_eligible: bool
    floor_sar: Optional[int]
    floor_unlocked: Optional[bool]
    unlock_equity_sar: Optional[float]
    reasons: List[str] = field(default_factory=list)

def evaluate(symbol: str,
             row: Optional[Dict[str, Any]] = None,
             authority_index: Optional[Dict[str, Dict[str, Any]]] = None,
             monitor: Optional[Dict[str, str]] = None,
             equity_sar: float = 130000.0,
             venue_override: Optional[Dict[str, str]] = None,
             permissions: Optional[Dict[str, bool]] = None,
             today: Optional[date] = None) -> ComplianceVerdict:
    """Gate order (§4/§19): AssetClass -> Tradability -> Venue -> Permissions
    -> Shariah (authority first, model screen per D-2) -> Floor-vs-cap."""
    row = row or {}
    reasons: List[str] = []
    ac = classify_asset(symbol, row.get("name", ""), row.get("quote_type", ""))
    trd, market, _floor = tradability(symbol)
    ven = saudi_venue(symbol, venue_override)

    if not gate_enabled():
        return ComplianceVerdict(symbol, ac, trd, market, ven, GATE_DISABLED,
                                 "gate_disabled", True, _floor, None, None,
                                 ["gate_disabled_passthrough"])

    perms = permissions or DEFAULT_PERMISSIONS
    blocked_status: Optional[str] = None
    if trd == BROKER_UNTRADABLE:
        blocked_status = BROKER_UNTRADABLE; reasons.append("broker_untradable_watch_only")
    elif ven == "NOMU":
        blocked_status = VENUE_BLOCK; reasons.append("venue_block_nomu_policy")
    elif not perms.get(ac, False):
        blocked_status = INSTRUMENT_BLOCK; reasons.append(f"instrument_block:{ac}")

    src = "none"
    if ac == SUKUK:
        st = AUTHORITY_PASS if blocked_status is None else blocked_status
        reasons.append("sukuk_pass_by_construction")
        src = "asset_class"
    else:
        a_status, a_reasons = authority_lookup(symbol, authority_index or {}, monitor, today)
        reasons += a_reasons
        if a_status is not None:
            st, src = a_status, "AL_RAJHI_OFFICIAL"
        elif model_screen_enabled():
            st, m_reasons = model_screen(row.get("name",""), row.get("sector",""),
                                         row.get("industry",""),
                                         row.get("interest_debt"), row.get("market_cap"),
                                         row.get("impure_income_ratio"))
            reasons += m_reasons
            src = "MODEL_SCREEN_RAJHI_STYLE"
        else:
            st, src = UNKNOWN, "model_screen_disabled"
        if blocked_status is not None:
            reasons.append(f"shariah_informational:{st}")
            st = blocked_status

    unlocked, floor, unlock_at = floor_vs_cap(symbol, equity_sar)
    eligible = (blocked_status is None and st in INVEST_OK_STATUSES)
    if eligible and not unlocked:
        eligible = False
        reasons.append(f"{FLOOR_LOCKED}:floor={floor}:unlock_equity={int(unlock_at or 0)}")
    return ComplianceVerdict(symbol, ac, trd, market, ven, st, src, eligible,
                             floor, unlocked, unlock_at, reasons)

# --------------------------------------------------------------------------- #
# SELFTEST                                                                     #
# --------------------------------------------------------------------------- #
def _selftest() -> None:
    os.environ["TFB_COMPLIANCE_GATE_ENABLED"] = "1"
    today = date(2026, 7, 18)
    auth = build_authority_index([
        {"symbol": "1120.SR", "status": "FAIL", "as_of": "2026-07-01"},
        {"symbol": "7010.SR", "status": "PASS", "as_of": "2026-07-01"},
        {"symbol": "2222.SR", "status": "PASS", "as_of": "2026-01-01"},  # stale
    ])
    checks: List[Tuple[str, bool]] = []

    v = evaluate("5023.SR", {"name": "Arabian Centres Sukuk 5023"}, auth, today=today)
    checks.append(("sukuk pass-by-construction + eligible",
                   v.asset_class == SUKUK and v.shariah_status == AUTHORITY_PASS and v.invest_eligible))
    v = evaluate("9628.SR", {"name": "Lamasat Co."}, auth, today=today)
    checks.append(("Nomu -> VENUE_BLOCK not SHARIAH_FAIL",
                   v.venue == "NOMU" and v.shariah_status == VENUE_BLOCK and not v.invest_eligible))
    v = evaluate("1120.SR", {"name": "Al Rajhi Bank"}, auth, today=today)
    checks.append(("authority FAIL blocks", v.shariah_status == AUTHORITY_FAIL and not v.invest_eligible))
    v = evaluate("7010.SR", {"name": "Saudi Telecom Company"}, auth, today=today)
    checks.append(("authority PASS eligible", v.shariah_status == AUTHORITY_PASS and v.invest_eligible))
    v = evaluate("2222.SR", {"name": "Saudi Aramco"}, auth, today=today)
    checks.append(("stale authority -> DATA_STALE blocked",
                   v.shariah_status == DATA_STALE and not v.invest_eligible))
    v = evaluate("AAPL", {"name": "Apple Inc.", "sector": "Technology",
                          "interest_debt": 95e9, "market_cap": 3.4e12,
                          "impure_income_ratio": 0.01}, auth, today=today)
    checks.append(("US model-screen PASS_NOT_FATWA eligible (D-2)",
                   v.shariah_status == MODEL_SCREEN_PASS and v.invest_eligible))
    v = evaluate("BBD.US", {"name": "Banco Bradesco S.A.", "sector": "Financials",
                            "industry": "Banks"}, auth, today=today)
    checks.append(("conventional bank -> MODEL_SCREEN_FAIL",
                   v.shariah_status == MODEL_SCREEN_FAIL and not v.invest_eligible))
    v = evaluate("2269.T", {"name": "Meiji Holdings", "sector": "Consumer Staples",
                            "interest_debt": 2.0e11, "market_cap": 1.1e12}, auth,
                 equity_sar=130000, today=today)
    checks.append(("Japan unlocked at 130K + undisclosed-impure conservative pass",
                   v.invest_eligible and v.floor_unlocked is True and
                   "impure_income_undisclosed_conservative" in " ".join(v.reasons)))
    v = evaluate("0083.HK", {"name": "Sino Land", "sector": "Real Estate",
                             "interest_debt": 1e9, "market_cap": 9e10}, auth,
                 equity_sar=130000, today=today)
    checks.append(("HK floor-locked at 130K", not v.invest_eligible and v.floor_unlocked is False))
    v = evaluate("0083.HK", {"name": "Sino Land", "sector": "Real Estate",
                             "interest_debt": 1e9, "market_cap": 9e10}, auth,
                 equity_sar=200000, today=today)
    checks.append(("HK unlocks at 200K", v.invest_eligible and v.floor_unlocked is True))
    v = evaluate("RELIANCE.NS", {"name": "Reliance Industries"}, auth, today=today)
    checks.append(("untradable .NS watch-only",
                   v.tradability == BROKER_UNTRADABLE and not v.invest_eligible))
    v = evaluate("7010.SR", {}, auth, monitor={"7010.SR": "FAIL"}, today=today)
    checks.append(("monitor disagreement -> CONFLICT blocked",
                   v.shariah_status == CONFLICT and not v.invest_eligible))
    amt, basis = purification_amount(500.0, 0.03)
    checks.append(("purification ratio basis", amt == 15.0 and basis == "ratio_x_dividends"))
    os.environ["TFB_COMPLIANCE_GATE_ENABLED"] = "0"
    v = evaluate("9628.SR", {}, auth, today=today)
    checks.append(("gate disabled -> passthrough eligible",
                   v.shariah_status == GATE_DISABLED and v.invest_eligible))
    os.environ["TFB_COMPLIANCE_GATE_ENABLED"] = "1"

    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[compliance_gate v{__version__}] SELFTEST {passed}/{len(checks)}")
    if passed != len(checks):
        raise SystemExit(1)

if __name__ == "__main__":
    _selftest()
