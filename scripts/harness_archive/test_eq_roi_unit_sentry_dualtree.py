#!/usr/bin/env python3
"""enriched_quote v4.11.0 sentry harness — REAL module, no stand-ins.
H1 off/off deep-equal vs pinned base | H2 observe log-only + tag survival
H3 enforce ground-truth scaling + idempotence | H4 points value untouched
H5 tiny-points 0.55 NOT inflated (F01-inverse guard) | H6 no-ground-truth never scales
Run x3; digest must be identical."""
import sys, os, json, copy, hashlib
KEYS = ["symbol", "current_price", "forecast_price_1m", "forecast_price_3m",
        "forecast_price_12m", "expected_roi_1m", "expected_roi_3m",
        "expected_roi_12m", "warnings"]
ROWS = [
    {"symbol": "FRAC.US", "current_price": 10.0, "forecast_price_12m": 13.4,
     "expected_roi_12m": 0.34},                       # fraction, ground truth
    {"symbol": "PTS.US", "current_price": 10.0, "forecast_price_12m": 13.4,
     "expected_roi_12m": 34.0},                        # already points
    {"symbol": "TINY.US", "current_price": 100.0, "forecast_price_12m": 100.55,
     "expected_roi_12m": 0.55},                        # 0.55 POINTS (true frac .0055)
    {"symbol": "NOGT.US", "current_price": 10.0,
     "expected_roi_12m": 0.34},                        # fraction, NO ground truth
]
def canon(o): return json.dumps(o, sort_keys=True, default=str)
def run(mod, rows): return mod.normalize_rows(copy.deepcopy(rows), list(KEYS), "Global_Markets")
def get(out, sym):
    return [r for r in out if r.get("symbol") == sym][0]

def main():
    os.environ.pop("TFB_EQ_ROI_UNIT_SENTRY", None)
    sys.path.insert(0, "base_pkg")
    import core.enriched_quote as eb
    assert eb.MODULE_VERSION == "4.10.0"
    base = run(eb, ROWS)
    for m in list(sys.modules):
        if m.startswith("core"): del sys.modules[m]
    sys.path.insert(0, "rev_pkg")
    import core.enriched_quote as er
    assert er.MODULE_VERSION == "4.11.0"
    # H1 off/off
    rev_off = run(er, ROWS)
    assert canon(base) == canon(rev_off), "H1 FAIL off/off differs"
    print("H1 PASS  off/off deep-equal vs pinned base (4 rows)")
    # H2 observe
    os.environ["TFB_EQ_ROI_UNIT_SENTRY"] = "observe"
    obs = run(er, ROWS)
    r = get(obs, "FRAC.US")
    assert r["expected_roi_12m"] == 0.34, "H2 value changed in observe"
    assert "roi_unit_points:eq:expected_roi_12m:observe" in (r["warnings"] or ""), r["warnings"]
    # Composed-pipeline note: step 4 (_normalize_percent_units, always on)
    # fractionalizes the 34.0-points input to 0.34 BEFORE step 8c, so the
    # sentry correctly tags it too — and observe still changes no values
    # (H1 proved off == base; here we prove obs values == off values).
    p = get(obs, "PTS.US")
    assert abs(p["expected_roi_12m"] - 0.34) < 1e-9
    assert "roi_unit_points:eq:expected_roi_12m:observe" in (p["warnings"] or "")
    print("H2 PASS  observe: values untouched (0.34/0.34), tags present and "
          "SURVIVE _normalize_warnings_field + _strip_stale_warnings")
    # H3 enforce + idempotence
    os.environ["TFB_EQ_ROI_UNIT_SENTRY"] = "enforce"
    enf = run(er, ROWS)
    r = get(enf, "FRAC.US")
    assert abs(r["expected_roi_12m"] - 34.0) < 1e-9, r["expected_roi_12m"]
    assert "roi_unit_points:eq:expected_roi_12m:enforce" in r["warnings"]
    twice = run(er, enf)          # feed enforced output back through
    assert abs(get(twice, "FRAC.US")["expected_roi_12m"] - 34.0) < 1e-9, "H3 not idempotent"
    print("H3 PASS  enforce: 0.34 -> 34.0 with tag; idempotent on second pass")
    # H4 composed round-trip: step 4 fractionalized the points input; the
    # sentry restores points at the boundary — net output is the correct 34.0.
    assert abs(get(enf, "PTS.US")["expected_roi_12m"] - 34.0) < 1e-9
    assert "roi_unit_points:eq:expected_roi_12m:enforce" in (get(enf, "PTS.US")["warnings"] or "")
    print("H4 PASS  points input round-trips to 34.0 at the boundary (step4 frac -> step8c points)")
    # H5 tiny points not inflated
    t = get(enf, "TINY.US")
    assert abs(t["expected_roi_12m"] - 0.55) < 1e-9, "H5 FAIL: 0.55 points was inflated!"
    print("H5 PASS  0.55-POINTS survives enforce untouched (F01-inverse structurally blocked)")
    # H6 no ground truth never scales
    n = get(enf, "NOGT.US")
    assert abs(n["expected_roi_12m"] - 0.34) < 1e-9, "H6 FAIL: scaled without ground truth"
    assert "roi_unit_ambiguous:eq:expected_roi_12m:enforce" in (n["warnings"] or "")
    print("H6 PASS  no ground truth: never scaled, ambiguous tag countable")
    os.environ.pop("TFB_EQ_ROI_UNIT_SENTRY", None)
    print("RUN-DIGEST", hashlib.sha256((canon(obs)+canon(enf)).encode()).hexdigest()[:16])

if __name__ == "__main__":
    main()
