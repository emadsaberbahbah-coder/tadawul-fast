# -*- coding: utf-8 -*-
"""Behavioral suite for core/analysis/trend_signals.py v1.0.0."""
import os
import sys
sys.path.insert(0, '.')
FAILS = []


def ok(name, cond, extra=""):
    if not cond:
        FAILS.append("%s %s" % (name, extra))


def R(sector, roi, reco="BUY", **kw):
    row = {"Symbol": "X", "Sector": sector, "Expected ROI 12M": roi,
           "Recommendation Detail": reco}
    row.update(kw)
    return row


# --- default OFF: rows untouched ---------------------------------------------
os.environ.pop("TFB_TREND_SIGNALS", None)
import importlib
import core.analysis.trend_signals as ts
importlib.reload(ts)
rows = [R("Energy", 15), R("Energy", 12), R("Energy", 11)]
out, meta = ts.enrich_rows_with_trends(rows)
ok("off-enabled", meta["enabled"] is False)
ok("off-untouched", all("Sector Trend" not in r for r in out))
ok("off-version", meta["version"] == "1.0.0")

# --- enabled ---------------------------------------------------------------------
os.environ["TFB_TREND_SIGNALS"] = "1"

# Positive: median ROI >= 10, no sells
rows = [R("Energy", 15), R("Energy", 12), R("Energy", 11)]
out, meta = ts.enrich_rows_with_trends(rows)
ok("pos-trend", meta["sectors"]["Energy"]["trend"] == "Positive",
   meta["sectors"])
ok("pos-stamped", meta["stamped"] == 3)
ok("pos-rowkey", out[0]["Sector Trend"] == "Positive")

# Negative by ROI
rows = [R("Tech", -5), R("Tech", -2), R("Tech", 1)]
_, meta = ts.enrich_rows_with_trends(rows)
ok("neg-roi", meta["sectors"]["Tech"]["trend"] == "Negative")

# Negative by sell breadth (median ROI fine, 2/3 sell-tier)
rows = [R("Banks", 12, "SELL"), R("Banks", 14, "STRONG SELL"),
        R("Banks", 13, "BUY")]
_, meta = ts.enrich_rows_with_trends(rows)
s = meta["sectors"]["Banks"]
ok("neg-breadth", s["trend"] == "Negative", s)
ok("breadth-val", abs(s["sell_breadth"] - 0.6667) < 0.001)

# Neutral: median between thresholds
rows = [R("Retail", 5), R("Retail", 6), R("Retail", 4)]
_, meta = ts.enrich_rows_with_trends(rows)
ok("neutral", meta["sectors"]["Retail"]["trend"] == "Neutral")

# Positive blocked by breadth > 20%
rows = [R("Pharma", 15), R("Pharma", 14, "REDUCE"), R("Pharma", 16),
        R("Pharma", 13)]
_, meta = ts.enrich_rows_with_trends(rows)
ok("pos-blocked", meta["sectors"]["Pharma"]["trend"] == "Neutral",
   meta["sectors"]["Pharma"])

# Cohort < 3 -> Unknown, not stamped
rows = [R("Tiny", 20), R("Tiny", 25)]
out, meta = ts.enrich_rows_with_trends(rows)
ok("small-unknown", meta["sectors"]["Tiny"]["trend"] == "Unknown")
ok("small-nostamp", meta["stamped"] == 0 and
   "Sector Trend" not in out[0])

# Blank/Unknown sector -> excluded + counted
rows = [R("", 10), R("Unknown", 10), R("Energy", 12), R("Energy", 13),
        R("Energy", 14)]
out, meta = ts.enrich_rows_with_trends(rows)
ok("nosector-count", meta["no_sector"] == 2, meta["no_sector"])
ok("nosector-unstamped", "Sector Trend" not in out[0])

# Provider-supplied value never overwritten
rows = [R("Energy", 12, **{"Sector Trend": "Negative"}),
        R("Energy", 13), R("Energy", 14)]
out, meta = ts.enrich_rows_with_trends(rows)
ok("supplied-kept", out[0]["Sector Trend"] == "Negative")
ok("supplied-count", meta["already_supplied"] == 1)
ok("others-stamped", meta["stamped"] == 2)

# News counted, never fabricated
rows = [R("Energy", 12, **{"News Trend": "Positive"}), R("Energy", 13),
        R("Energy", 14)]
out, meta = ts.enrich_rows_with_trends(rows)
ok("news-counted", meta["news_supplied"] == 1)
ok("news-not-fabricated", all("News Trend" not in r or
   r.get("News Trend") == "Positive" for r in out))

# ROI parses % strings and commas
rows = [R("FX", "12%"), R("FX", "1,400"), R("FX", 11)]
_, meta = ts.enrich_rows_with_trends(rows)
ok("roi-parse", meta["sectors"]["FX"]["cohort"] == 3)

# Env tuning respected
os.environ["TFB_TREND_MIN_COHORT"] = "5"
rows = [R("Energy", 12), R("Energy", 13), R("Energy", 14)]
_, meta = ts.enrich_rows_with_trends(rows)
ok("env-cohort", meta["sectors"]["Energy"]["trend"] == "Unknown")
os.environ.pop("TFB_TREND_MIN_COHORT")

# Builder pickup: stamped key resolves through opportunity_builder aliases
from core.analysis.opportunity_builder import normalize_candidate
cand_rows = [R("Energy", 15, **{"Current Price": 30, "Currency": "SAR",
              "Forecast Reliability Score": 80,
              "Data Quality Score": 90}) for _ in range(3)]
enriched, meta = ts.enrich_rows_with_trends(cand_rows)
from core.analysis.opportunity_builder import make_criteria
cand = normalize_candidate(enriched[0], {"SAR": 1.0}, make_criteria())
ok("builder-pickup", cand["sector_trend"] == "Positive",
   cand.get("sector_trend"))

# Determinism
_, m1 = ts.enrich_rows_with_trends([R("Energy", 12), R("Energy", 13),
                                    R("Energy", 14)])
_, m2 = ts.enrich_rows_with_trends([R("Energy", 12), R("Energy", 13),
                                    R("Energy", 14)])
ok("deterministic", m1["sectors"] == m2["sectors"])

os.environ.pop("TFB_TREND_SIGNALS", None)
if FAILS:
    print("FAILURES (%d):" % len(FAILS))
    for f in FAILS:
        print("  -", f)
    sys.exit(1)
print("ALL TREND SIGNALS TESTS PASS")
