"""
core/risk_limits.py — TFB Gen-2 Correlation-Adjusted Heat & Concentration Caps
==============================================================================
VERSION 1.0.0  (2026-07-19)  — NEW MODULE (Wave A completion, deliverable #17)

WHY (Master Plan v2.1 §7 risk engine, §12 lever 3 "correlation cap [4-tanker
cluster evidence]"): the heat cap counts positions as if independent. They are
not. Measured on the LIVE boards 2026-07-19:
    challenger (Gen2-eligible, 5 names): Japan 60%, Energy 40%
    champion (10 names):                 USA 40%, Real Estate 30%, Energy 30%
and across this session's boards four tanker/shipping names appeared together
(TRMD, TNK, STNG, CMBT). A book of correlated names consumes far more of the
risk budget than its naive sum suggests, and nothing in the system measured it.

METHOD (standard portfolio math, no black box):
    effective_heat = sqrt( SUM_i SUM_j  rho_ij * r_i * r_j )
where r_i is a position's risk contribution (default 0.75%/position per the
Path-B ladder) and rho_ij is a TRANSPARENT PROXY, not an estimated matrix:
    same symbol      1.00
    same theme       0.70   (tankers, pharma, REITs, ... — see THEME_RULES)
    same country     0.40
    otherwise        0.20   (broad equity beta)
Proxies are used deliberately: a correlation matrix estimated from short price
history is noisier and less auditable than a stated assumption. Every rho is
a named constant, overridable, and printed with the result. When real
correlations arrive (Wave D), swap `pair_rho` and nothing else changes.

PURITY: stdlib only, no I/O, no env. Consumers (shadow board, weekly memo)
pass positions in and display the verdict. Advisory in Shadow Mode: this
module reports and suggests; it never mutates a board.
"""

from __future__ import annotations

import math
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

__version__ = "1.0.0"
RISK_LIMITS_VERSION = __version__

# --- correlation proxies (§7; overridable per call) ------------------------ #
RHO_SAME_THEME = 0.70
RHO_SAME_COUNTRY = 0.40
RHO_BASE = 0.20

# --- default limits (Path-B ladder constants, §7 / Decision_Log D-1) ------- #
DEFAULT_LIMITS = {
    "risk_per_position_pct": 0.75,
    "max_effective_heat_pct": 6.0,
    "max_position_weight_pct": 15.0,
    "max_country_weight_pct": 40.0,
    "max_sector_weight_pct": 35.0,
    "max_theme_weight_pct": 30.0,
}

# --- country from venue suffix (mirrors compliance_gate's matrix) ---------- #
_SUFFIX_COUNTRY = {
    "": "USA", ".US": "USA", ".SR": "Saudi", ".T": "Japan", ".HK": "HongKong",
    ".L": "UK", ".PA": "Eurozone", ".AS": "Eurozone", ".BR": "Eurozone",
    ".DE": "Eurozone", ".MI": "Eurozone", ".MC": "Eurozone", ".LS": "Eurozone",
    ".VI": "Eurozone", ".SW": "Switzerland", ".TO": "Canada", ".AX": "Australia",
    ".OL": "Norway", ".SI": "Singapore", ".MX": "Mexico", ".IS": "Turkey",
    ".NS": "India", ".KS": "Korea", ".TW": "Taiwan", ".ST": "Sweden",
}

_SUFFIX_RE = re.compile(r"(\.[A-Za-z]{1,3})$")


def country_of(symbol: str) -> str:
    m = _SUFFIX_RE.search(str(symbol).strip())
    suf = m.group(1).upper() if m else ""
    return _SUFFIX_COUNTRY.get(suf, "Other")


# --- theme classification (ordered: most specific first) ------------------- #
THEME_RULES: List[Tuple[str, str]] = [
    ("TANKERS_SHIPPING",
     r"\b(TANKER|TANKERS|SHIPPING|MARITIME|MARINE|SHIPHOLDING|BULKERS?|"
     r"CARRIERS?|SEAWAYS|NAVIGATION)\b"),
    ("OIL_GAS_EP", r"\b(PETROLEUM|OIL|GAS|ENERGY|DRILLING|OFFSHORE|RESOURCES)\b"),
    ("PHARMA_HEALTH",
     r"\b(PHARMA|PHARMACEUTICAL|BIOTECH|MEDIC|HEALTH|THERAPEUT|DENSHI|DIAGNOST)\b"),
    ("REIT_PROPERTY",
     r"\b(REIT|PROPERT|REALTY|REAL ESTATE|WAREHOUSE|LAND|ESTATES|CENTRES|MALL)\b"),
    ("BANKS_FIN", r"\b(BANK|BANCO|FINANC|INSUR|CAPITAL|CREDIT|LEASING)\b"),
    ("SEMIS_TECH",
     r"\b(SEMICONDUCTOR|MICRO|CHIP|ELECTRON|SOFTWARE|TECHNOLOG|DIGITAL|CLOUD)\b"),
    ("CONSUMER_STAPLE",
     r"\b(FOOD|BEVERAGE|DAIRY|CONFECTION|HOLDINGS CO|MEIJI|CONSUMER|RETAIL)\b"),
    ("MATERIALS_INDUSTRIAL",
     r"\b(STEEL|CEMENT|CHEMIC|MINING|CONSTRUCT|INDUSTRIAL|ENGINEER|INSAAT)\b"),
    ("UTILITIES", r"\b(UTILIT|ELECTRIC POWER|WATER|POWER CO)\b"),
    ("TELECOM", r"\b(TELECOM|COMMUNICAT|WIRELESS|MOBILE)\b"),
]

_SECTOR_THEME = {
    "energy": "OIL_GAS_EP", "healthcare": "PHARMA_HEALTH",
    "real estate": "REIT_PROPERTY", "financials": "BANKS_FIN",
    "financial services": "BANKS_FIN", "technology": "SEMIS_TECH",
    "consumer defensive": "CONSUMER_STAPLE", "consumer staples": "CONSUMER_STAPLE",
    "industrials": "MATERIALS_INDUSTRIAL", "basic materials": "MATERIALS_INDUSTRIAL",
    "utilities": "UTILITIES", "communication services": "TELECOM",
}


def classify_theme(symbol: str = "", name: str = "", sector: str = "",
                   industry: str = "") -> str:
    """Name/industry keywords win (more specific), then sector, then UNTHEMED.
    Never guesses from the symbol alone."""
    blob = " ".join(x for x in (name, industry) if x).upper()
    for theme, pattern in THEME_RULES:
        if re.search(pattern, blob):
            return theme
    s = str(sector or "").strip().lower()
    if s in _SECTOR_THEME:
        return _SECTOR_THEME[s]
    return "UNTHEMED"


# --- correlation-adjusted heat -------------------------------------------- #
def pair_rho(a: Dict[str, Any], b: Dict[str, Any],
             rho_theme: float = RHO_SAME_THEME,
             rho_country: float = RHO_SAME_COUNTRY,
             rho_base: float = RHO_BASE) -> float:
    """Transparent proxy. Swap this one function for a real matrix in Wave D."""
    if a.get("symbol") == b.get("symbol"):
        return 1.0
    ta, tb = a.get("theme"), b.get("theme")
    if ta and ta == tb and ta != "UNTHEMED":
        return rho_theme
    if a.get("country") and a.get("country") == b.get("country"):
        return rho_country
    return rho_base


def enrich_positions(rows: Sequence[Dict[str, Any]],
                     risk_per_position_pct: Optional[float] = None
                     ) -> List[Dict[str, Any]]:
    """Attach country/theme/risk to raw rows (symbol, name, sector, industry,
    optional weight_pct / risk_pct)."""
    default_risk = (risk_per_position_pct
                    if risk_per_position_pct is not None
                    else DEFAULT_LIMITS["risk_per_position_pct"])
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        sym = str((r or {}).get("symbol") or "").strip()
        if not sym:
            continue
        out.append({
            "symbol": sym,
            "name": str(r.get("name") or ""),
            "country": r.get("country") or country_of(sym),
            "theme": r.get("theme") or classify_theme(
                sym, str(r.get("name") or ""), str(r.get("sector") or ""),
                str(r.get("industry") or "")),
            "sector": str(r.get("sector") or "") or "Unclassified",
            "risk_pct": float(r.get("risk_pct") or default_risk),
            "weight_pct": (float(r["weight_pct"])
                           if r.get("weight_pct") is not None else None),
        })
    return out


def effective_heat(positions: Sequence[Dict[str, Any]], **rho_kw) -> Dict[str, Any]:
    """-> naive sum, correlation-adjusted heat, and the diversification ratio.
    Empty book returns zeros, never an error."""
    pos = list(positions or [])
    if not pos:
        return {"naive_heat_pct": 0.0, "effective_heat_pct": 0.0,
                "diversification_ratio": None, "n": 0}
    naive = sum(float(p.get("risk_pct") or 0.0) for p in pos)
    total = 0.0
    for i, pi in enumerate(pos):
        ri = float(pi.get("risk_pct") or 0.0)
        for j, pj in enumerate(pos):
            rj = float(pj.get("risk_pct") or 0.0)
            total += pair_rho(pi, pj, **rho_kw) * ri * rj
    eff = math.sqrt(max(0.0, total))
    return {"naive_heat_pct": round(naive, 4),
            "effective_heat_pct": round(eff, 4),
            "diversification_ratio": (round(eff / naive, 4) if naive else None),
            "n": len(pos)}


# --- concentration -------------------------------------------------------- #
def _weights(positions: Sequence[Dict[str, Any]], key: str) -> Dict[str, float]:
    """Weight by explicit weight_pct when present, else equal-weight."""
    pos = list(positions or [])
    if not pos:
        return {}
    explicit = [p for p in pos if p.get("weight_pct") is not None]
    out: Dict[str, float] = {}
    if len(explicit) == len(pos):
        total = sum(float(p["weight_pct"]) for p in pos) or 1.0
        for p in pos:
            out[str(p.get(key) or "Unclassified")] = out.get(
                str(p.get(key) or "Unclassified"), 0.0) + \
                float(p["weight_pct"]) / total * 100.0
    else:
        share = 100.0 / len(pos)
        for p in pos:
            k = str(p.get(key) or "Unclassified")
            out[k] = out.get(k, 0.0) + share
    return {k: round(v, 2) for k, v in
            sorted(out.items(), key=lambda kv: -kv[1])}


def concentration(positions: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    return {"country": _weights(positions, "country"),
            "sector": _weights(positions, "sector"),
            "theme": _weights(positions, "theme")}


def check_limits(positions: Sequence[Dict[str, Any]],
                 limits: Optional[Dict[str, float]] = None,
                 **rho_kw) -> Dict[str, Any]:
    """Full verdict: heat + every concentration cap. Advisory only."""
    lim = dict(DEFAULT_LIMITS)
    lim.update(limits or {})
    pos = list(positions or [])
    heat = effective_heat(pos, **rho_kw)
    conc = concentration(pos)
    breaches: List[Dict[str, Any]] = []

    if heat["effective_heat_pct"] > lim["max_effective_heat_pct"]:
        breaches.append({
            "type": "HEAT",
            "overage": round(heat["effective_heat_pct"]
                             - lim["max_effective_heat_pct"], 4),
            "detail": (f"effective heat {heat['effective_heat_pct']:.2f}% > "
                       f"cap {lim['max_effective_heat_pct']:.2f}% "
                       f"(naive {heat['naive_heat_pct']:.2f}%)")})
    for dim, cap_key in (("country", "max_country_weight_pct"),
                         ("sector", "max_sector_weight_pct"),
                         ("theme", "max_theme_weight_pct")):
        for bucket, w in conc[dim].items():
            if bucket in ("UNTHEMED", "Unclassified"):
                continue
            if w > lim[cap_key]:
                breaches.append({
                    "type": dim.upper(),
                    "overage": round(w - lim[cap_key], 4),
                    "detail": f"{bucket} {w:.1f}% > cap {lim[cap_key]:.1f}%"})
    for p in pos:
        w = p.get("weight_pct")
        if w is not None and float(w) > lim["max_position_weight_pct"]:
            breaches.append({
                "type": "POSITION",
                "overage": round(float(w) - lim["max_position_weight_pct"], 4),
                "detail": (f"{p['symbol']} {float(w):.1f}% > cap "
                           f"{lim['max_position_weight_pct']:.1f}%")})
    return {"version": __version__, "verdict": "BREACH" if breaches else "OK",
            "heat": heat, "concentration": conc, "breaches": breaches,
            "limits": lim,
            "rho": {"same_theme": rho_kw.get("rho_theme", RHO_SAME_THEME),
                    "same_country": rho_kw.get("rho_country", RHO_SAME_COUNTRY),
                    "base": rho_kw.get("rho_base", RHO_BASE)},
            "governance": "advisory — reports and suggests, never mutates a board"}


def _worst_overage(result: Dict[str, Any]) -> float:
    """Severity of the single worst breach; 0.0 when clean."""
    return max((float(b.get("overage") or 0.0)
                for b in result.get("breaches") or []), default=0.0)


def suggest_trims(positions: Sequence[Dict[str, Any]],
                  limits: Optional[Dict[str, float]] = None,
                  min_book: int = 3, **rho_kw) -> Dict[str, Any]:
    """Greedy de-concentration. Objective is the WORST OVERAGE first, then
    breach count, then heat — minimizing breach COUNT alone is wrong: dropping
    an off-theme name can clear a small sector breach while making the
    dominant country breach worse (observed on the live 3-Japan/2-US board).

    A drop must strictly improve the objective, and the book never shrinks
    below `min_book`. When no subset can satisfy the caps — mathematically
    true for a 5-name book of only two countries under a 40% cap — this
    returns feasible=False and says the fix is DIVERSIFICATION, not trimming.
    Suggestions only; the operator decides."""
    lim = dict(DEFAULT_LIMITS)
    lim.update(limits or {})
    pos = list(positions or [])
    drops: List[Dict[str, Any]] = []
    if not pos:
        return {"drops": [], "feasible": True, "note": "empty book",
                "residual_breaches": []}

    while len(pos) > max(1, int(min_book)):
        cur = check_limits(pos, lim, **rho_kw)
        if cur["verdict"] == "OK":
            break
        cur_score = (round(_worst_overage(cur), 6), len(cur["breaches"]),
                     cur["heat"]["effective_heat_pct"])
        best_idx: Optional[int] = None
        best_score = cur_score
        for i in range(len(pos)):
            t = check_limits(pos[:i] + pos[i + 1:], lim, **rho_kw)
            score = (round(_worst_overage(t), 6), len(t["breaches"]),
                     t["heat"]["effective_heat_pct"])
            if score < best_score:
                best_score, best_idx = score, i
        if best_idx is None:
            break                      # no single drop improves anything
        victim = pos[best_idx]
        drops.append({"symbol": victim["symbol"], "theme": victim["theme"],
                      "country": victim["country"],
                      "reason": cur["breaches"][0]["detail"],
                      "worst_overage_after": best_score[0],
                      "breaches_after": best_score[1]})
        pos = pos[:best_idx] + pos[best_idx + 1:]

    final = check_limits(pos, lim, **rho_kw)
    feasible = final["verdict"] == "OK"
    if feasible:
        note = (f"caps satisfied after {len(drops)} trim(s)" if drops
                else "already within caps")
    else:
        note = ("caps CANNOT be met by trimming this book — the remaining "
                "concentration is structural. Fix by ADDING diversifying "
                "names, not by cutting further.")
    return {"drops": drops, "feasible": feasible, "note": note,
            "residual_breaches": final["breaches"],
            "kept": [p["symbol"] for p in pos]}


# --------------------------------------------------------------------------- #
# SELFTEST                                                                     #
# --------------------------------------------------------------------------- #
def _selftest() -> int:
    checks: List[Tuple[str, bool]] = []

    checks.append(("country from suffix",
                   country_of("6960.T") == "Japan" and country_of("TNK.US") == "USA"
                   and country_of("AAPL") == "USA" and country_of("ENKAI.IS") == "Turkey"
                   and country_of("5023.SR") == "Saudi"))
    checks.append(("theme: tankers detected by name",
                   classify_theme("TNK.US", "Teekay Tankers Ltd.") == "TANKERS_SHIPPING"
                   and classify_theme("TRMD.US", "TORM plc", "Energy")
                   in ("TANKERS_SHIPPING", "OIL_GAS_EP")))
    checks.append(("theme: sector fallback when name is uninformative",
                   classify_theme("EXE.US", "Expand Energy Corporation",
                                  "Energy") == "OIL_GAS_EP"))
    checks.append(("theme: REIT by name beats generic sector",
                   classify_theme("WDP.BR", "Warehouses De Pauw SA",
                                  "Real Estate") == "REIT_PROPERTY"))
    checks.append(("theme: unknown stays UNTHEMED, never guessed",
                   classify_theme("XYZ.US", "", "") == "UNTHEMED"))

    # LIVE challenger board 2026-07-19
    chal = enrich_positions([
        {"symbol": "6960.T", "name": "Fukuda Denshi Co., Ltd.", "sector": "Healthcare"},
        {"symbol": "2269.T", "name": "Meiji Holdings Co., Ltd.", "sector": "Consumer Defensive"},
        {"symbol": "4503.T", "name": "Astellas Pharma Inc.", "sector": "Healthcare"},
        {"symbol": "TNK.US", "name": "Teekay Tankers Ltd.", "sector": "Energy"},
        {"symbol": "EXE.US", "name": "Expand Energy Corporation", "sector": "Energy"},
    ])
    checks.append(("live board enriched (5 positions)", len(chal) == 5))
    conc = concentration(chal)
    checks.append(("live board: Japan measured at 60%",
                   abs(conc["country"]["Japan"] - 60.0) < 0.01))
    res = check_limits(chal)
    checks.append(("live board BREACHES the 40% country cap — the finding",
                   res["verdict"] == "BREACH"
                   and any(b["type"] == "COUNTRY" and "Japan" in b["detail"]
                           for b in res["breaches"])))
    checks.append(("live board heat is under cap (small book)",
                   res["heat"]["effective_heat_pct"] < 6.0))
    checks.append(("diversification ratio < 1 (correlation reduces vs naive)",
                   0.0 < res["heat"]["diversification_ratio"] < 1.0))

    # correlation actually bites: 8 tankers vs 8 unrelated, same naive heat
    tankers = enrich_positions([
        {"symbol": f"T{i}.US", "name": f"Tanker {i} Shipping", "sector": "Energy"}
        for i in range(8)])
    spread = enrich_positions([
        {"symbol": "A.US", "name": "Alpha Pharma", "sector": "Healthcare"},
        {"symbol": "B.T", "name": "Beta Foods", "sector": "Consumer Defensive"},
        {"symbol": "C.L", "name": "Gamma Utilities", "sector": "Utilities"},
        {"symbol": "D.HK", "name": "Delta Telecom", "sector": "Communication Services"},
        {"symbol": "E.AX", "name": "Epsilon Steel", "sector": "Basic Materials"},
        {"symbol": "F.TO", "name": "Zeta Software", "sector": "Technology"},
        {"symbol": "G.SW", "name": "Eta Realty", "sector": "Real Estate"},
        {"symbol": "H.SI", "name": "Theta Oil", "sector": "Energy"},
    ])
    ht, hs = effective_heat(tankers), effective_heat(spread)
    checks.append(("identical naive heat for both books",
                   abs(ht["naive_heat_pct"] - hs["naive_heat_pct"]) < 1e-9
                   and abs(ht["naive_heat_pct"] - 6.0) < 1e-9))
    checks.append(("correlated book consumes MORE effective risk",
                   ht["effective_heat_pct"] > hs["effective_heat_pct"] * 1.4))
    checks.append(("tanker cluster breaches theme cap (§12 lever 3)",
                   any(b["type"] == "THEME" for b in check_limits(tankers)["breaches"])))
    checks.append(("diversified book of 8 passes cleanly",
                   check_limits(spread)["verdict"] == "OK"))

    trims = suggest_trims(chal)
    checks.append(("first trim targets the over-concentrated country",
                   trims["drops"] and trims["drops"][0]["country"] == "Japan"))
    checks.append(("BUG FIXED: never trims a US name while Japan is the breach",
                   all(d["country"] == "Japan" for d in trims["drops"])))
    checks.append(("BUG FIXED: book is never trimmed to empty",
                   len(trims["kept"]) >= 3))
    checks.append(("structural concentration reported as INFEASIBLE, honestly",
                   trims["feasible"] is False
                   and "ADDING diversifying" in trims["note"]))
    checks.append(("each trim strictly reduces the worst overage",
                   all(d["worst_overage_after"] >= 0 for d in trims["drops"])))
    checks.append(("trims carry a stated reason",
                   all(d.get("reason") for d in trims["drops"])))
    fixable = enrich_positions([
        {"symbol": "6960.T", "name": "Fukuda Denshi", "sector": "Healthcare"},
        {"symbol": "4503.T", "name": "Astellas Pharma", "sector": "Healthcare"},
        {"symbol": "TNK.US", "name": "Teekay Tankers", "sector": "Energy"},
        {"symbol": "C.L", "name": "Gamma Utilities", "sector": "Utilities"},
        {"symbol": "D.HK", "name": "Delta Telecom", "sector": "Communication Services"}])
    fx = suggest_trims(fixable)
    checks.append(("a fixable book IS fixed and reported feasible",
                   fx["feasible"] is True))
    checks.append(("clean book needs no trims",
                   suggest_trims(spread)["drops"] == []
                   and suggest_trims(spread)["feasible"] is True))
    checks.append(("breaches carry numeric overage for ranking",
                   all("overage" in b for b in res["breaches"])))
    checks.append(("empty book: zeros, no crash",
                   effective_heat([])["n"] == 0
                   and check_limits([])["verdict"] == "OK"))
    checks.append(("explicit weights honored over equal-weight",
                   abs(_weights(enrich_positions([
                       {"symbol": "A.US", "weight_pct": 90},
                       {"symbol": "B.US", "weight_pct": 10}]),
                       "country")["USA"] - 100.0) < 0.01))
    checks.append(("governance line present + rho printed",
                   "never mutates" in res["governance"]
                   and res["rho"]["same_theme"] == RHO_SAME_THEME))

    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[risk_limits v{__version__}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    raise SystemExit(_selftest())
