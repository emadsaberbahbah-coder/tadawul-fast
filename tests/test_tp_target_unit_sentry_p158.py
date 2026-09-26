# -*- coding: utf-8 -*-
"""tests/test_tp_target_unit_sentry_p158.py

P-158 - track_performance v6.39.0 TARGET-UNIT SENTRY.

Executes the REAL module: PerformanceRecord, HorizonType, PerformanceStatus,
PerformanceTrackerApp._derive_target / _derive_checkpoint_target /
_publish_s1_calibration / _track_selftest_ and s1_checkpoint_calibration.
No stand-in for any code under test; the only double is the Google Sheets
I/O boundary of the publisher (a recorder), which cannot exist offline.

Run:  python tests/test_tp_target_unit_sentry_p158.py
      pytest -q tests/test_tp_target_unit_sentry_p158.py
Dual-tree (optional): TFB_P158_BASE=/path/to/track_performance_v6.38.0.py
Target override:      TFB_P158_TARGET=/path/to/track_performance.py
"""
import hashlib
import importlib.util
import json
import os
import random
import sys
from datetime import datetime, timedelta, timezone

GATE = "TFB_PERF_TARGET_UNIT_SENTRY"
_HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.environ.get("TFB_P158_TARGET") or os.path.join(
    os.path.dirname(_HERE), "scripts", "track_performance.py")
BASE = os.environ.get("TFB_P158_BASE") or ""
RIYADH = timezone(timedelta(hours=3))


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _gate(value):
    if value is None:
        os.environ.pop(GATE, None)
    else:
        os.environ[GATE] = value


def _app(mod):
    # REAL class, REAL methods; __init__ needs argv/network, so it is bypassed.
    return object.__new__(mod.PerformanceTrackerApp)


def _reset_counters(mod):
    c = getattr(mod, "_PERF_UNIT_CREATION", None)
    if isinstance(c, dict):
        for k in list(c):
            c[k] = 0


# ---------------------------------------------------------------- corpus
ETO = {"symbol": "ETO.US", "current_price": 30.16,
       "forecast_price_1m": 30.8654, "expected_roi_1m": 0.023388,
       "forecast_price_3m": 32.10, "expected_roi_3m": 0.064324}


def build_corpus(mod, seed=158, days=24, nsym=30):
    """Records are built by the module's OWN derive methods from engine-shaped
    rows (expected_roi_* = FRACTION, the engine contract). Eras / defects:
      era 'fraction' : engine fraction ROI + real forecast price (live shape)
      era 'percent'  : engine percent ROI + consistent forecast price
      'nosib'        : the 1M sibling is dropped      -> unresolved/no_sibling
      'mismatch'     : ROI contradicts the price      -> unresolved/mismatch
      'noforecast'   : no 1M forecast at all          -> target 0, excluded
    Returns (records, truth) where truth[record_id] = (kind, true_target_pp).
    """
    rnd = random.Random(seed)
    app = _app(mod)
    H = mod.HorizonType
    hz = [H.WEEK_1, H.WEEK_2, H.MONTH_1, H.MONTH_3]
    day0 = datetime(2026, 8, 20, 9, 0, 0, tzinfo=RIYADH)
    records, truth = [], {}
    syms = ["S%02d.US" % i for i in range(nsym)] + ["ETO.US"]
    rid = 0
    for d in range(days):
        now = day0 + timedelta(days=d)
        for si, sym in enumerate(syms):
            cp = 30.16 if sym == "ETO.US" else round(rnd.uniform(4, 400), 2)
            f1 = 0.023388 if sym == "ETO.US" else round(rnd.uniform(-0.04, 0.09), 6)
            f3 = round(f1 * 2.75, 6)
            kind = "fraction"
            k = (d * 131 + si * 17) % 23
            if sym != "ETO.US":
                if k == 0:
                    kind = "percent"
                elif k == 1:
                    kind = "nosib"
                elif k == 2:
                    kind = "mismatch"
                elif k == 3:
                    kind = "noforecast"
            row = {"symbol": sym, "current_price": cp,
                   "forecast_price_1m": round(cp * (1 + f1), 4),
                   "expected_roi_1m": f1,
                   "forecast_price_3m": round(cp * (1 + f3), 4),
                   "expected_roi_3m": f3}
            if kind == "percent":
                row["expected_roi_1m"] = f1 * 100.0
                row["expected_roi_3m"] = f3 * 100.0
            elif kind == "mismatch":
                row["expected_roi_1m"] = f1 * 1.9 + 0.013
            elif kind == "noforecast":
                row["expected_roi_1m"] = 0.0
                row["forecast_price_1m"] = 0.0
            if sym == "ETO.US":
                row = dict(ETO)
            implied_1m_pp = ((row["forecast_price_1m"] / cp - 1.0) * 100.0
                             if row["forecast_price_1m"] > 0 else 0.0)
            for h in hz:
                if kind == "nosib" and h.value == "1M":
                    continue
                tp, troi = app._derive_target(row, h, cp)
                if tp <= 0.0:
                    tp, troi = cp, 0.0
                rid += 1
                age = (days - d)
                matured = h.value in ("1W", "2W") and age >= h.days
                r = mod.PerformanceRecord(
                    record_id="r%06d" % rid, symbol=sym, horizon=h,
                    date_recorded=now, entry_price=cp,
                    entry_recommendation=mod.RecommendationType.HOLD,
                    entry_score=70.0, entry_risk_bucket="LOW",
                    entry_confidence="HIGH", origin_tab="Top_10_Investments",
                    target_price=tp, target_roi=troi,
                    target_date=now + timedelta(days=h.days),
                    status=(mod.PerformanceStatus.MATURED if matured
                            else mod.PerformanceStatus.ACTIVE),
                    current_price=cp)
                if matured:
                    r.realized_roi = round(rnd.gauss(0.1, 2.6 if h.value == "1W" else 3.7), 6)
                records.append(r)
                if h.value in ("1W", "2W"):
                    truth[r.record_id] = (kind, implied_1m_pp * h.days / 30.0)
    return records, truth


def _expected(records, truth, mod):
    """Independent recomputation of what the sentry must report."""
    cnt = {"fraction": 0, "percent": 0, "unresolved": 0}
    why = {"no_sibling": 0, "mismatch": 0}
    errs = []
    for r in records:
        if r.horizon.value not in ("1W", "2W"):
            continue
        if r.status != mod.PerformanceStatus.MATURED or r.realized_roi is None:
            continue
        if r.target_roi == 0.0:
            continue
        kind, true_pp = truth[r.record_id]
        if kind == "nosib":
            cnt["unresolved"] += 1
            why["no_sibling"] += 1
        elif kind == "mismatch":
            # A contradiction is only a contradiction beyond the tolerance:
            # when the truth is ~0 both readings sit inside 0.05pp and the
            # closer one is kept (error impact < 0.05pp, disclosed as tiny).
            tol = max(0.05, 0.10 * abs(true_pp))
            d_f = abs(r.target_roi * 100.0 - true_pp)
            d_p = abs(r.target_roi - true_pp)
            if min(d_f, d_p) > tol:
                cnt["unresolved"] += 1
                why["mismatch"] += 1
            elif d_f < d_p:
                cnt["fraction"] += 1
                errs.append(r.realized_roi - r.target_roi * 100.0)
            else:
                assert abs(true_pp) < 0.6, true_pp
                cnt["percent"] += 1
                errs.append(r.realized_roi - r.target_roi)
        elif kind == "percent":
            cnt["percent"] += 1
            errs.append(r.realized_roi - r.target_roi)
        else:
            cnt["fraction"] += 1
            errs.append(r.realized_roi - r.target_roi * 100.0)
    n = len(errs)
    mae = round(sum(abs(e) for e in errs) / n, 2) if n else None
    sgn = round(sum(errs) / n, 2) if n else None
    return cnt, why, n, mae, sgn


# ---------------------------------------------------------------- recorder
class _Rec(object):
    """Sheets I/O boundary recorder (NOT code under test)."""

    def __init__(self):
        self.calls = []

    def update(self, *a, **k):
        self.calls.append(("update", a, k))

    def append_row(self, *a, **k):
        self.calls.append(("append_row", a, k))


class _Sheet(object):
    def __init__(self):
        self.tabs = {}

    def worksheet(self, name):
        return self.tabs.setdefault(name, _Rec())


class _Backoff(object):
    def execute_sync(self, fn, *a, **k):
        return fn(*a, **k)


class _Store(object):
    def __init__(self):
        self.sheet = _Sheet()
        self.backoff = _Backoff()

    def is_available(self):
        return True


# ---------------------------------------------------------------- tests
def run_all():
    res = {}
    new = _load(TARGET, "tp_p158_new")
    assert new.SCRIPT_VERSION >= "6.39.0", new.SCRIPT_VERSION
    base = _load(BASE, "tp_p158_base") if BASE else None

    # T1 - golden negative on the live specimen (defect reproduced through
    # the real derive methods with the gate OFF; identical on the base tree).
    _gate(None)
    app = _app(new)
    tp1m, r1m = app._derive_target(dict(ETO), new.HorizonType.MONTH_1, 30.16)
    tp1w, r1w = app._derive_target(dict(ETO), new.HorizonType.WEEK_1, 30.16)
    assert (tp1m, r1m) == (30.8654, 0.023388), (tp1m, r1m)
    assert abs(r1w - 0.023388 * 7 / 30) < 1e-12
    assert abs(tp1w - 30.16) < 0.01, "checkpoint price ~= entry IS the defect"
    res["T1_defect_off"] = [tp1m, r1m, round(tp1w, 6), round(r1w, 9)]
    if base is not None:
        b = _app(base)
        assert b._derive_target(dict(ETO), base.HorizonType.MONTH_1, 30.16) == (tp1m, r1m)
        assert b._derive_target(dict(ETO), base.HorizonType.WEEK_1, 30.16) == (tp1w, r1w)

    # T2 - gate OFF: no report key, counters untouched, dual-tree identity.
    _reset_counters(new)
    recs, truth = build_corpus(new)
    off = new.s1_checkpoint_calibration(recs)
    assert "unit_sentry" not in off
    assert all(v == 0 for v in new._PERF_UNIT_CREATION.values())
    res["T2_off"] = {k: off[k] for k in ("state", "n", "mean_abs_error_pp",
                                         "mean_signed_error_pp", "detail")}
    if base is not None:
        brecs, _ = build_corpus(base)
        assert len(brecs) == len(recs)
        for x, y in zip(brecs, recs):
            assert (x.key, x.target_price, x.target_roi, x.realized_roi) == \
                   (y.key, y.target_price, y.target_roi, y.realized_roi)
        assert base.s1_checkpoint_calibration(brecs) == off
        res["T2_dualtree_records"] = len(recs)

    # T3 - OBSERVE: headline byte-identical; report equals the independent
    # recomputation; creation values unchanged but counted.
    _gate("observe")
    _reset_counters(new)
    orecs, otruth = build_corpus(new)
    for x, y in zip(recs, orecs):
        assert (x.target_price, x.target_roi) == (y.target_price, y.target_roi)
    obs = new.s1_checkpoint_calibration(orecs)
    for k in off:
        assert obs[k] == off[k], k
    us = obs["unit_sentry"]
    cnt, why, n, mae, sgn = _expected(orecs, otruth, new)
    assert us["counts"] == cnt, (us["counts"], cnt)
    assert us["unresolved_why"] == why
    assert sum(cnt.values()) == off["n"], "labels must partition the legacy sample"
    assert (us["n"], us["mean_abs_error_pp"], us["mean_signed_error_pp"]) == (n, mae, sgn)
    assert us["creation"]["scaled"] == 0 and us["creation"]["fraction"] > 0
    assert cnt["fraction"] > 0 and cnt["percent"] > 0 and cnt["unresolved"] > 0
    res["T3_observe"] = {"counts": cnt, "why": why, "n": n, "mae": mae,
                         "signed": sgn, "legacy_mae": off["mean_abs_error_pp"],
                         "creation": us["creation"]}

    # T4 - ENFORCE measurement on the SAME stored (legacy) rows.
    _gate("enforce")
    enf = new.s1_checkpoint_calibration(orecs)
    assert (enf["n"], enf["mean_abs_error_pp"], enf["mean_signed_error_pp"]) == (n, mae, sgn)
    assert "[unit-sentry enforce:" in enf["detail"]
    assert enf["unit_sentry"]["legacy"]["mean_abs_error_pp"] == off["mean_abs_error_pp"]
    assert enf["state"] == ("PASS" if mae <= enf["band_pp"] else "FAIL")
    res["T4_enforce"] = {"state": enf["state"], "detail": enf["detail"]}

    # T5 - ENFORCE creation: pp targets, correct checkpoint price, and NO
    # double scaling when those rows are measured again (idempotence).
    _reset_counters(new)
    e1m = _app(new)._derive_target(dict(ETO), new.HorizonType.MONTH_1, 30.16)
    e1w = _app(new)._derive_target(dict(ETO), new.HorizonType.WEEK_1, 30.16)
    assert e1m[0] == 30.8654 and abs(e1m[1] - 2.3388) < 1e-9
    assert abs(e1w[1] - 2.3388 * 7 / 30) < 1e-9
    assert abs(e1w[0] - 30.16 * (1 + e1w[1] / 100.0)) < 1e-9 and e1w[0] > 30.30
    erecs, etruth = build_corpus(new)
    again = new.s1_checkpoint_calibration(erecs)
    c2 = again["unit_sentry"]["counts"]
    assert c2["fraction"] == 0, "enforced rows must read as pp - never x100 twice"
    _gate("observe")
    chk = new.s1_checkpoint_calibration(erecs)
    assert chk["unit_sentry"]["mean_abs_error_pp"] is not None
    # on an all-pp corpus the legacy error over the resolved rows IS the truth
    res["T5_enforce_creation"] = {"eto_1m": list(e1m), "eto_1w": [round(e1w[0], 6), round(e1w[1], 9)],
                                  "counts_after": c2}

    # T6 - fail-open: nothing resolvable -> legacy basis kept + note; junk
    # records never raise.
    _gate("enforce")
    nosib = [r for r in orecs if r.horizon.value != "1M"]
    keep = new.s1_checkpoint_calibration(nosib)
    leg = None
    _gate(None)
    leg = new.s1_checkpoint_calibration(nosib)
    _gate("enforce")
    for k in leg:
        assert keep[k] == leg[k], k
    assert "legacy basis kept" in keep["unit_sentry"]["enforce_note"]
    junk = new.s1_checkpoint_calibration([object(), None, 7, "x"] + orecs[:50])
    assert isinstance(junk, dict) and "state" in junk
    res["T6_failopen"] = keep["unit_sentry"]["enforce_note"]

    # T7 - block + log line shapes.
    rows = new._perf_unit_block_rows(us, "2026-09-21 12:00:00")
    assert len(rows) == 6 and all(len(r) == 4 for r in rows)
    line = new._perf_unit_log_line(us)
    assert line.startswith("[PERF-UNIT v6.39.0] mode=observe") and "\n" not in line
    res["T7_block"] = [rows[0][0], rows[3][0], line[:60]]

    # T8 - publisher wiring through the REAL _publish_s1_calibration.
    def _publish(mode):
        _gate(mode)
        a = _app(new)
        a.store = _Store()
        ok = a._publish_s1_calibration(orecs)
        cal = a.store.sheet.tabs.get(new.S1_CAL_TAB)
        log = a.store.sheet.tabs.get("_Run_Log")
        return ok, cal.calls, (log.calls if log else [])
    ok0, cal0, log0 = _publish(None)
    ok1, cal1, log1 = _publish("observe")
    assert ok0 and ok1
    assert len(cal0) == 1 and not log0, "gate off: exactly the legacy A1 write"
    assert cal0[0][1][0] == "A1"
    row_off, row_obs = cal0[0][1][1][1], cal1[0][1][1][1]
    assert row_off[1:] == row_obs[1:], "criterion-4 row identical under observe"
    assert len(cal1) == 2 and cal1[1][2]["range_name"] == "A4:D9"
    assert len(cal1[1][2]["values"]) == 6
    assert len(log1) == 1 and log1[0][1][0][4] == "UNIT_SENTRY"
    res["T8_publisher"] = {"off_writes": len(cal0), "observe_writes": len(cal1),
                           "runlog_lines": len(log1)}

    # T9 - the embedded self-test, real method.
    _gate(None)
    a = _app(new)
    assert a._track_selftest_() is True
    assert new._TRACK_SELFTEST_MSG == "PASS 14/14", new._TRACK_SELFTEST_MSG
    assert GATE not in os.environ
    res["T9_selftest"] = new._TRACK_SELFTEST_MSG
    if base is not None:
        b = _app(base)
        assert b._track_selftest_() is True
        res["T9_selftest_base"] = base._TRACK_SELFTEST_MSG
    _gate(None)
    return res


def test_p158_target_unit_sentry():
    run_all()


if __name__ == "__main__":
    out = run_all()
    blob = json.dumps(out, sort_keys=True, default=str)
    for k in sorted(out):
        print(k, "->", json.dumps(out[k], sort_keys=True, default=str)[:230])
    print("PASS T1-T9 | digest", hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16])
