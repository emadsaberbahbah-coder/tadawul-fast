"""F-7 (data_engine_v2 v5.147.0) — SCORING SETTLE PASS: pass-dependent scoring.

The orchestrator scores a row BEFORE Phase-II enriches it, so pass 1 lacks
intrinsic/upside and (synthetic or late-restored target) any forecast at
scoring time; core.scoring then labels the row both_present_fallback and the
gate's -15 "fallback" reliability leg fires on a label that describes pass
ORDER, not data. v5.147.0 adds one seam after the pair — _f7_settle_pass —
that re-runs the SAME pair on a deep copy until the decision fields stop
moving (cap TFB_SCORING_SETTLE_MAX_PASSES, default 4, clamped 2..5).

  off (unset)  -> row untouched (byte-identical to v5.145.0)
  observe      -> values untouched; ONE countable substring-safe tag on rows
                  whose pass-2 output would differ
  enforce      -> the settled row replaces the pass-1 row (tagged); rows stable
                  after pass 2 are returned untouched

Fixtures: six REAL Global_Markets input rows from the 2026-09-22 export
(engine keys, derived fields stripped) — two early-target rows (provider
target present at scoring), two late-target rows (target restored from the
R-6 keep-last-good store inside Phase-II, i.e. AFTER pass-1 scoring) and two
synthetic rows. Each changes its recommendation once settled. The golden is an
independent settle loop in this file over the same field set and tolerances.

Run: python -m pytest -q tests/test_de_f7_scoring_settle.py   (or python tests/...)
"""
from __future__ import annotations

import copy
import inspect
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.data_engine_v2 as de  # noqa: E402

ENV = "TFB_SCORING_SETTLE"
MAX_ENV = "TFB_SCORING_SETTLE_MAX_PASSES"
FORBIDDEN = ("cap", "forecast", "target", "roi", "drop", "reject", "provider_target",
             "price_bar_stale", "xprovider_price_conflict")
TAG_RE = re.compile(r"f7_settle:(observe|enforce):st[0-9x]+:p\d+(?::[a-z0-9]+=[^:;]*>[^:;]*)*(?::chg\d+)?$")
VOLATILE = ("scoring_updated_utc", "scoring_updated_riyadh")
FIELDS = (("overall_score", "num", 0.05), ("opportunity_score", "num", 0.05), ("valuation_score", "num", 0.05),
          ("forecast_confidence", "num", 0.001), ("expected_roi_12m", "num", 0.0001),
          ("recommendation", "str", 0.0), ("opportunity_source", "str", 0.0), ("forecast_source", "str", 0.0))

FIXTURES = [{'cohort': 'early',
  'lkg': None,
  'row': {'asset_class': 'Equity',
          'avg_volume_10d': 3991287.0,
          'avg_volume_30d': 2975156.0,
          'beta_5y': 1.05,
          'country': 'USA',
          'currency': 'USD',
          'current_price': 153.89,
          'data_provider': 'eodhd',
          'day_high': 155.48,
          'day_low': 153.24,
          'debt_to_equity': 1.381380978,
          'dividend_yield': 0.000144,
          'eps_ttm': 8.57,
          'ev_ebitda': 0.0,
          'exchange': 'NYSE/NASDAQ',
          'float_shares': 676794312.0,
          'forecast_price_12m': 167.79,
          'forecast_source': 'provider_target',
          'free_cash_flow_ttm': 1592000000.0,
          'gross_margin': 52.5234,
          'industry': 'Banks - Diversified',
          'last_updated_riyadh': '2026-09-22T07:24:36.142002+03:00',
          'last_updated_utc': '2026-09-22T04:24:36.141979+00:00',
          'market_cap': 104347148288.0,
          'max_drawdown_1y': -0.1015,
          'name': 'The Bank of New York Mellon Corporation',
          'open_price': 155.19,
          'operating_margin': 19.7154,
          'payout_ratio': 0.002,
          'pb_ratio': 2.61,
          'pe_forward': 15.34,
          'pe_ttm': 17.95,
          'percent_change': 0.0007000000000000001,
          'previous_close': 153.79,
          'price_change': 0.1,
          'ps_ratio': 4.87,
          'revenue_growth_yoy': -0.0166,
          'revenue_ttm': 40481000000.0,
          'risk_bucket': 'LOW',
          'risk_score': 13.82,
          'rsi_14': 29.57,
          'sector': 'Financial Services',
          'sharpe_1y': 1.83,
          'symbol': 'BNY.US',
          'target_mean_price': 167.79,
          'var_95_1d': -0.0213,
          'volatility_30d': 0.18780000000000002,
          'volatility_90d': 0.2163,
          'volume': 3439403.0,
          'week_52_high': 165.84,
          'week_52_low': 103.11,
          'week_52_position_pct': 0.8095}},
 {'cohort': 'early',
  'lkg': None,
  'row': {'asset_class': 'Equity',
          'avg_volume_10d': 44387204.0,
          'avg_volume_30d': 33355382.0,
          'beta_5y': 0.12,
          'country': 'Hong Kong',
          'currency': 'HKD',
          'current_price': 8.24,
          'data_provider': 'eodhd',
          'day_high': 8.265,
          'day_low': 8.05,
          'debt_to_equity': 3.866379396,
          'dividend_yield': 0.0451,
          'eps_ttm': 1.18,
          'ev_ebitda': 0.0,
          'exchange': 'HKEX',
          'float_shares': 29145026950.0,
          'forecast_price_12m': 8.410812,
          'forecast_source': 'provider_target',
          'free_cash_flow_ttm': 454690544000.0,
          'gross_margin': 0.0,
          'industry': 'Banks - Diversified',
          'last_updated_riyadh': '2026-09-21T23:24:10.440674+03:00',
          'last_updated_utc': '2026-09-21T20:24:10.440658+00:00',
          'market_cap': 729884877907.0,
          'max_drawdown_1y': -0.1516,
          'name': 'Bank of Communications Co., Ltd.',
          'open_price': 8.05,
          'operating_margin': 0.46715,
          'payout_ratio': 0.3208,
          'pb_ratio': 0.53,
          'pe_forward': 6.43,
          'pe_ttm': 7.0,
          'peg_ratio': 1.53,
          'percent_change': 0.01228501,
          'previous_close': 8.14,
          'price_change': 0.1,
          'profit_margin': 44.725,
          'ps_ratio': 3.35,
          'revenue_growth_yoy': 0.11800000000000001,
          'revenue_ttm': 217951993856.0,
          'risk_bucket': 'LOW',
          'risk_score': 19.66,
          'rsi_14': 66.96,
          'sector': 'Financial Services',
          'sharpe_1y': 0.68,
          'symbol': '3328.HK',
          'target_mean_price': 8.410812,
          'var_95_1d': -0.0204,
          'volatility_30d': 0.174204,
          'volatility_90d': 0.2208,
          'volume': 26970507.0,
          'week_52_high': 8.27,
          'week_52_low': 6.33,
          'week_52_position_pct': 98.70801}},
 {'cohort': 'late',
  'lkg': {'fp12': 353.41196, 'name': 'Subsea 7 S.A.'},
  'row': {'asset_class': 'Equity',
          'avg_volume_10d': 528966.0,
          'avg_volume_30d': 326204.0,
          'beta_5y': 0.6,
          'country': 'Norway',
          'currency': 'NOK',
          'current_price': 323.4,
          'data_provider': 'eodhd',
          'day_high': 325.2,
          'day_low': 318.0,
          'debt_to_equity': 0.2,
          'dividend_yield': 0.0607,
          'eps_ttm': 19.42,
          'ev_ebitda': 67.036,
          'exchange': 'Oslo Bors',
          'float_shares': 197551289.0,
          'free_cash_flow_ttm': 1507262464.0,
          'gross_margin': 0.1838,
          'industry': 'Oil & Gas Equipment & Services',
          'last_updated_riyadh': '2026-09-22T07:35:38.196509+03:00',
          'last_updated_utc': '2026-09-22T04:35:38.196497+00:00',
          'market_cap': 95774186878.0,
          'max_drawdown_1y': -0.1435,
          'name': 'Subsea 7 S.A.',
          'open_price': 324.2,
          'operating_margin': 0.1592,
          'payout_ratio': 0.9956,
          'pb_ratio': 2.17,
          'pe_forward': 13.031997,
          'pe_ttm': 16.652935,
          'peg_ratio': 1.28,
          'percent_change': 0.0075,
          'previous_close': 321.0,
          'price_change': 2.4,
          'ps_ratio': 12.74,
          'revenue_growth_yoy': 0.098,
          'revenue_ttm': 7517799936.0,
          'risk_bucket': 'LOW',
          'risk_score': 28.11,
          'rsi_14': 40.5,
          'sector': 'Energy',
          'sharpe_1y': 1.47,
          'symbol': 'SUBC.OL',
          'var_95_1d': -0.028300000000000002,
          'volatility_30d': 0.2673,
          'volatility_90d': 0.3414,
          'volume': 113221.0,
          'week_52_high': 358.2,
          'week_52_low': 180.1,
          'week_52_position_pct': 0.8046}},
 {'cohort': 'late',
  'lkg': {'fp12': 175.5, 'name': 'FTI Consulting, Inc.'},
  'row': {'asset_class': 'Equity',
          'avg_volume_10d': 465487.0,
          'avg_volume_30d': 399200.0,
          'beta_5y': -0.06,
          'country': 'USA',
          'currency': 'USD',
          'current_price': 140.74,
          'data_provider': 'eodhd',
          'day_high': 142.06,
          'day_low': 139.98,
          'debt_to_equity': 0.95,
          'eps_ttm': 8.26,
          'ev_ebitda': 11.15,
          'exchange': 'NYSE/NASDAQ',
          'float_shares': 26371910.0,
          'free_cash_flow_ttm': 289741120.0,
          'gross_margin': 0.3182,
          'industry': 'Consulting Services',
          'last_updated_riyadh': '2026-09-22T07:35:38.535493+03:00',
          'last_updated_utc': '2026-09-22T04:35:38.535478+00:00',
          'market_cap': 3886053217.0,
          'max_drawdown_1y': -0.2493,
          'name': 'FTI Consulting, Inc.',
          'open_price': 141.67,
          'operating_margin': 0.09230000000000001,
          'payout_ratio': 0.0,
          'pb_ratio': 2.86,
          'pe_forward': 12.872566,
          'pe_ttm': 17.04,
          'peg_ratio': 0.96,
          'percent_change': -0.0031,
          'previous_close': 141.18,
          'price_change': -0.44,
          'profit_margin': 6.443,
          'ps_ratio': 0.99,
          'revenue_growth_yoy': 0.053,
          'revenue_ttm': 3923721984.0,
          'risk_bucket': 'MODERATE',
          'risk_score': 38.13,
          'rsi_14': 29.6,
          'sector': 'Industrials',
          'sharpe_1y': -0.45,
          'symbol': 'FCN.US',
          'var_95_1d': -0.030299999999999997,
          'volatility_30d': 0.2097,
          'volatility_90d': 0.3152,
          'volume': 364106.0,
          'week_52_high': 189.3,
          'week_52_low': 137.65,
          'week_52_position_pct': 0.059800000000000006}},
 {'cohort': 'synth',
  'lkg': None,
  'row': {'asset_class': 'Equity',
          'avg_volume_10d': 5140937.0,
          'avg_volume_30d': 4531930.0,
          'beta_5y': 0.6,
          'country': 'USA',
          'currency': 'USD',
          'current_price': 502.01,
          'data_provider': 'eodhd',
          'day_high': 510.5,
          'day_low': 501.98,
          'debt_to_equity': 0.17,
          'dividend_yield': 0.0,
          'eps_ttm': 39.76,
          'ev_ebitda': 0.0,
          'exchange': 'NYSE/NASDAQ',
          'float_shares': 1233781000.0,
          'free_cash_flow_ttm': 24215000000.0,
          'gross_margin': 23.517,
          'industry': 'Insurance - Diversified',
          'last_updated_riyadh': '2026-09-22T07:24:32.727614+03:00',
          'last_updated_utc': '2026-09-22T04:24:32.727593+00:00',
          'market_cap': 1091269558272.0,
          'max_drawdown_1y': -0.0942,
          'name': 'Berkshire Hathaway Inc.',
          'open_price': 507.93,
          'operating_margin': 16.251099999999997,
          'payout_ratio': 0.0,
          'pb_ratio': 1.46,
          'pe_forward': 22.88,
          'pe_ttm': 12.82,
          'percent_change': -0.0152,
          'previous_close': 509.77,
          'price_change': -7.76,
          'profit_margin': 22.3,
          'ps_ratio': 2.84,
          'revenue_growth_yoy': 0.10039999999999999,
          'revenue_ttm': 384687000000.0,
          'risk_bucket': 'LOW',
          'risk_score': 9.48,
          'rsi_14': 47.63,
          'sector': 'Financial Services',
          'sharpe_1y': 0.38,
          'symbol': 'BRK-B.US',
          'var_95_1d': -0.0149,
          'volatility_30d': 0.1482,
          'volatility_90d': 0.14550000000000002,
          'volume': 4691097.0,
          'week_52_high': 537.74,
          'week_52_low': 464.01,
          'week_52_position_pct': 0.5154}},
 {'cohort': 'synth',
  'lkg': None,
  'row': {'asset_class': 'Equity',
          'beta_5y': 0.96,
          'country': 'United Kingdom',
          'currency': 'GBX',
          'current_price': 3758.0,
          'data_provider': 'eodhd',
          'day_high': 0.0,
          'day_low': 0.0,
          'debt_to_equity': 0.67,
          'dividend_yield': 0.000367,
          'eps_ttm': 2.46,
          'ev_ebitda': 6.25,
          'exchange': 'LSE',
          'float_shares': 256893945.0,
          'free_cash_flow_ttm': 676000000.0,
          'gross_margin': 33.5965,
          'industry': 'Packaging & Containers',
          'last_updated_riyadh': '2026-09-19T07:44:11.689065+03:00',
          'last_updated_utc': '2026-09-19T04:44:11.689042+00:00',
          'market_cap': 19173599232.0,
          'name': 'Smurfit Westrock Plc',
          'operating_margin': 12.2782,
          'payout_ratio': 0.0002,
          'pb_ratio': 1.95,
          'pe_forward': 13.587,
          'pe_ttm': 0.0,
          'profit_margin': 0.0673,
          'ps_ratio': 0.82,
          'revenue_growth_yoy': -0.1547433904,
          'revenue_ttm': 11272000000.0,
          'risk_bucket': 'LOW',
          'risk_score': 25.71,
          'sector': 'Consumer Cyclical',
          'symbol': 'SKG.L',
          'volume': 2519293.0,
          'week_52_high': 3910.0,
          'week_52_low': 0.0,
          'week_52_position_pct': 0.9611}}]


def _env(mode, max_passes=None):
    if mode is None:
        os.environ.pop(ENV, None)
    else:
        os.environ[ENV] = mode
    if max_passes is None:
        os.environ.pop(MAX_ENV, None)
    else:
        os.environ[MAX_ENV] = str(max_passes)


def _pair(row):
    de._compute_scores_canonical_first(row)
    de._apply_phase_dd_enhancements(row)


def _prep(fx):
    """Orchestrator prep identical to production: seed the target LKG for a
    late row, phase-BB sanity, then pass 1."""
    os.environ["TFB_ENGINE_TARGET_KLG"] = "1"
    row = dict(fx["row"])
    sym = row["symbol"].upper()
    de._TGT_LKG_STORE.pop(sym, None)
    if fx.get("lkg"):
        de._TGT_LKG_STORE[sym] = {"ts": time.time(), "name": fx["lkg"]["name"], "fp12": float(fx["lkg"]["fp12"])}
    row = de._apply_phase_bb_sanity(row)
    _pair(row)
    return row


def _norm(row):
    r = dict(row)
    for k in VOLATILE:
        r.pop(k, None)
    w = r.get("warnings")
    if w in (None, ""):
        r["warnings"] = ""
    elif isinstance(w, str):
        r["warnings"] = "; ".join(p.strip() for p in w.split(";") if p.strip() and not p.strip().startswith("f7_settle:"))
    return r


def _tags(row):
    w = row.get("warnings") or ""
    return [p.strip() for p in str(w).split(";") if p.strip().startswith("f7_settle:")]


def _differs(a, b):
    for k, kind, tol in FIELDS:
        x, y = a.get(k), b.get(k)
        if kind == "num":
            fx, fy = de._as_float(x), de._as_float(y)
            if fx is None and fy is None:
                continue
            if fx is None or fy is None or abs(fx - fy) > tol:
                return True
        elif de._safe_str(x).strip().upper() != de._safe_str(y).strip().upper():
            return True
    return False


def _golden(row, max_passes):
    """Independent settle loop: same fields, same tolerances, same return rule."""
    prev, last, first = row, row, None
    for k in range(2, max_passes + 1):
        cand = copy.deepcopy(prev)
        _pair(cand)
        d = _differs(prev, cand)
        if first is None:
            first = d
        if not d:
            if k > 2:
                last = cand
            break
        last = cand
        prev = cand
    return row if not first else last


def test_t1_gate_explicit_words_only():
    for raw, want in ((None, "off"), ("", "off"), ("1", "off"), ("true", "off"), ("on", "off"), ("yes", "off"),
                      ("off", "off"), ("observe", "observe"), ("OBSERVE ", "observe"), ("enforce", "enforce"), ("garbage", "off")):
        _env(raw)
        assert de._f7_settle_mode() == want, (raw, want)
        assert de.surface_gate_states().get("scoring_settle") == want
    _env(None)


def test_t2_max_passes_clamped():
    for raw, want in ((None, 4), ("", 4), ("x", 4), ("1", 2), ("2", 2), ("3", 3), ("4", 4), ("9", 5), ("-3", 2)):
        _env("observe", raw)
        assert de._f7_settle_max_passes() == want, (raw, want)
    _env(None)


def test_t3_off_is_byte_identical():
    _env(None)
    for fx in FIXTURES:
        row = _prep(fx)
        before = copy.deepcopy(row)
        out = de._f7_settle_pass(row, row["symbol"], "Global_Markets")
        assert out is row
        assert _norm(out) == _norm(before) and out.get("warnings") == before.get("warnings")
    _env("off")
    for fx in FIXTURES:
        row = _prep(fx)
        before = copy.deepcopy(row)
        assert de._f7_settle_pass(row) is row and row == before
    _env(None)


def test_t4_observe_values_untouched_one_safe_tag():
    _env("observe")
    tagged = 0
    for fx in FIXTURES:
        row = _prep(fx)
        before = copy.deepcopy(row)
        out = de._f7_settle_pass(row, row["symbol"], "Global_Markets")
        assert out is row
        assert _norm(out) == _norm(before), fx["row"]["symbol"]
        tags = _tags(out)
        assert len(tags) <= 1
        if tags:
            tagged += 1
            t = tags[0]
            assert TAG_RE.match(t), t
            assert not any(f in t.lower() for f in FORBIDDEN), t
            assert ":observe:" in t
    assert tagged == len(FIXTURES), "every fixture moves on pass 2 by construction"
    _env(None)


def test_t5_enforce_equals_independent_golden():
    _env("enforce", 4)
    for fx in FIXTURES:
        row = _prep(fx)
        base = copy.deepcopy(row)
        golden = _golden(copy.deepcopy(base), 4)
        out = de._f7_settle_pass(row, row["symbol"], "Global_Markets")
        assert _norm(out) == _norm(golden), fx["row"]["symbol"]
        tags = _tags(out)
        if _norm(golden) != _norm(base):
            assert len(tags) == 1 and ":enforce:" in tags[0] and TAG_RE.match(tags[0])
            # the pass-1 label was the pass-order artifact; settled rows publish the
            # data-based label
            assert out.get("recommendation") != base.get("recommendation") or out.get("opportunity_source") != base.get("opportunity_source")
        else:
            assert out is row and not tags
    _env(None)


def test_t5b_late_and_synthetic_rows_lose_the_fallback_label():
    _env("enforce", 4)
    for fx in FIXTURES:
        row = _prep(fx)
        p1_src = row.get("opportunity_source")
        out = de._f7_settle_pass(row, row["symbol"], "Global_Markets")
        if fx["cohort"] in ("late", "synth"):
            assert p1_src == "both_present_fallback", (fx["cohort"], p1_src)
            assert out.get("opportunity_source") == "roi_based", fx["row"]["symbol"]
        else:
            assert p1_src == "roi_based" and out.get("opportunity_source") == "roi_based"
    _env(None)


def test_t6_fail_open_on_exception():
    _env("enforce")
    row = _prep(FIXTURES[0])
    before = copy.deepcopy(row)
    saved = de._compute_scores_canonical_first

    def _boom(r):
        raise RuntimeError("settle boom")

    de._compute_scores_canonical_first = _boom
    try:
        out = de._f7_settle_pass(row, row["symbol"], "Global_Markets")
    finally:
        de._compute_scores_canonical_first = saved
    assert out is row and row == before
    _env(None)


def test_t7_tag_composer_degrades_on_forbidden_substring():
    tag = de._f7_settle_tag("observe", 3, 2, [("ov", "1", "2"), ("rc", "roi_x", "b")])
    assert tag == "f7_settle:observe:st3:p2:chg2"
    tag = de._f7_settle_tag("enforce", None, 3, [("ov", "76.78", "65.71"), ("os", "bpf", "rb")])
    assert tag == "f7_settle:enforce:stx:p3:ov=76.78>65.71:os=bpf>rb"
    assert de._f7_settle_token("both_present_fallback") == "bpf"
    assert de._f7_settle_token("roi_based") == "rb"
    assert de._f7_settle_token("provider_target") == "pt"
    assert de._f7_settle_token("phase_ii_synthetic") == "sy"
    for tok in ("momentum_only_fallback", "insufficient", "provider_target", "forecast_capped"):
        assert not any(f in de._f7_settle_token(tok) for f in FORBIDDEN)


def test_t8_wiring_and_version():
    src = inspect.getsource(de)
    assert src.count("merged = _f7_settle_pass(merged, sym, page_ctx)") == 1
    assert src.count('"scoring_settle": _f7_settle_mode(),') == 1
    # v5.148.0: the boot-line literal grows a leg per gate; check the leg, not the line
    assert src.count("scoring_settle=%s") == 1
    assert src.count("_f7_settle_mode(),          # v5.147.0") == 1
    assert tuple(int(x) for x in de.__version__.split(".")[:3]) >= (5, 147, 0)


TESTS = [test_t1_gate_explicit_words_only, test_t2_max_passes_clamped, test_t3_off_is_byte_identical,
         test_t4_observe_values_untouched_one_safe_tag, test_t5_enforce_equals_independent_golden,
         test_t5b_late_and_synthetic_rows_lose_the_fallback_label, test_t6_fail_open_on_exception,
         test_t7_tag_composer_degrades_on_forbidden_substring, test_t8_wiring_and_version]

if __name__ == "__main__":
    import traceback
    fails = 0
    for t in TESTS:
        try:
            t()
            print("PASS", t.__name__)
        except Exception:
            fails += 1
            print("FAIL", t.__name__)
            traceback.print_exc()
    print("RESULT", "PASS" if not fails else "FAIL", "%d/%d" % (len(TESTS) - fails, len(TESTS)))
    sys.exit(1 if fails else 0)
