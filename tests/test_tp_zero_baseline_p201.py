# -*- coding: utf-8 -*-
"""tests/test_tp_zero_baseline_p201.py

P-201 - track_performance v6.42.0 ZERO-FORECAST BASELINE MAE.

Executes the REAL module: _s1_checkpoint_calibration_legacy,
_s1_unit_sentry_measure, _s1_unit_sentry_apply, s1_checkpoint_calibration,
_perf_unit_block_rows, _s1_zero_baseline_enabled, _s1_zero_mae and
PerformanceTrackerApp._publish_s1_calibration / _track_selftest_.
The only double is the Google Sheets I/O boundary of the publisher (an
in-memory recorder, the same pattern as tests/test_tp_target_unit_sentry_p158.py),
which cannot exist offline. ZERO network: nothing here touches a provider,
a Google API or the internet.

Proves:
  T1  the pure measurement carries zero_mae_pp regardless of the env, and
      it equals mean |realized| over the qualifying cohort (per horizon too)
  T2  GOLDEN NEGATIVE - gate OFF: 10-column header, the row and the Detail
      cell byte-identical to the pre-change behaviour, no token anywhere,
      and the sentry block still writes A4:D9 with six rows
  T3  PUBLISH: 11-column header ending 'Zero MAE (pp)', the value equal to
      mean |realized|, the Detail token PARSEABLE by a local copy of
      run_shadow_scorer v1.9.2's parse_zero_mae regex, the stdout line
      carrying the token, and the sentry block at A4:D10 with seven rows
  T4  the enforce-branch basis coupling: row 2's zero baseline is on the
      SAME (unit-corrected) cohort as row 2's model MAE
  T5  the embedded self-test still passes k/k

Run:  /home/user/tfb-venv/bin/python tests/test_tp_zero_baseline_p201.py
      /home/user/tfb-venv/bin/python -m pytest -q tests/test_tp_zero_baseline_p201.py
Target override: TFB_P201_TARGET=/path/to/track_performance.py
"""
import importlib.util
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

GATE = "TFB_S1_ZERO_BASELINE"
UNIT_GATE = "TFB_PERF_TARGET_UNIT_SENTRY"
_HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.environ.get("TFB_P201_TARGET") or os.path.join(
    os.path.dirname(_HERE), "scripts", "track_performance.py")
RIYADH = timezone(timedelta(hours=3))

# The pre-change (v6.41.0) header, pinned here so a drift in S1_CAL_HEADER
# cannot quietly redefine what "byte-identical OFF path" means.
V641_HEADER = [
    "As Of (Riyadh)", "State", "N Checkpoints", "Mean Abs Error (pp)",
    "Mean Signed Error (pp)", "Band (pp)", "Min Sample", "By Horizon",
    "Detail", "Writer Version",
]

# A LOCAL COPY of run_shadow_scorer.py v1.9.2 _ZERO_MAE_RE (copied on
# purpose - the scorer is never imported here).
SCORER_ZERO_MAE_RE = re.compile(
    r"zero[_\s-]*(?:baseline)?[_\s-]*mae\s*[=:]?\s*"
    r"([0-9]+(?:\.[0-9]+)?)\s*pp", re.IGNORECASE)


def scorer_parse_zero_mae(header, row):
    """A LOCAL re-implementation of the scorer's parse_zero_mae (v1.9.2):
    the 'Zero MAE (pp)' column first, then the Detail token."""
    hdr = [str(h).strip().lower() for h in header]
    try:
        if "zero mae (pp)" in hdr:
            v = str(row[hdr.index("zero mae (pp)")]).strip()
            if v:
                return float(v)
    except Exception:
        pass
    try:
        detail = str(row[hdr.index("detail")]) if "detail" in hdr else ""
    except Exception:
        detail = ""
    m = SCORER_ZERO_MAE_RE.search(detail or "")
    return float(m.group(1)) if m else None


# track_performance registers prometheus Counters at import time, so the
# module is loaded ONCE per process and shared by every test function.
_MOD = None


def _mod():
    global _MOD
    if _MOD is None:
        spec = importlib.util.spec_from_file_location("tp_p201", TARGET)
        m = importlib.util.module_from_spec(spec)
        sys.modules["tp_p201"] = m
        spec.loader.exec_module(m)
        _MOD = m
    return _MOD


def _env(name, value):
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


def _app(mod):
    # REAL class, REAL methods; __init__ needs argv/network, so it is bypassed.
    return object.__new__(mod.PerformanceTrackerApp)


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


# ---------------------------------------------------------------- corpus
def build_corpus(mod):
    """A hand-built cohort whose zero MAE and model MAE are both known by
    hand. Checkpoint rows are 1W/2W, MATURED, realized present, target != 0;
    every other shape must be EXCLUDED from both measurements.

    Qualifying rows (realized, target):
      1W (+2.00, +1.00) -> err +1.00  zero 2.00
      1W (-4.00, -1.00) -> err -3.00  zero 4.00
      2W (+5.00, +1.00) -> err +4.00  zero 5.00
      2W (-1.00, +1.00) -> err -2.00  zero 1.00
    model MAE = (1 + 3 + 4 + 2) / 4 = 2.50 pp
    zero  MAE = (2 + 4 + 5 + 1) / 4 = 3.00 pp   (model BEATS zero here:
      lower MAE is better, and 2.50 < 3.00. The model-LOSES direction --
      the only one that can flip criterion 4 -- is build_losing_corpus /
      t6 below. Corrected 2026-10-06: this line used to claim the
      opposite of its own arithmetic.)
      1W: model 2.00 / zero 3.00     2W: model 3.00 / zero 3.00
    Each qualifying row also carries a same-day 1M sibling whose target
    price makes the stored checkpoint target read as already-pp, so the
    unit sentry resolves it as 'percent' and enforce measures the SAME
    four rows (basis coupling, item (b)).
    """
    H = mod.HorizonType
    S = mod.PerformanceStatus
    day0 = datetime(2026, 9, 1, 9, 0, 0, tzinfo=RIYADH)
    spec = [
        ("A1.US", H.WEEK_1, 2.0, 1.0),
        ("A2.US", H.WEEK_1, -4.0, -1.0),
        ("A3.US", H.WEEK_2, 5.0, 1.0),
        ("A4.US", H.WEEK_2, -1.0, 1.0),
    ]
    records = []
    for i, (sym, hz, realized, target) in enumerate(spec):
        now = day0 + timedelta(days=i)
        entry = 100.0
        rec = mod.PerformanceRecord(
            record_id="z%03d" % i, symbol=sym, horizon=hz,
            date_recorded=now, entry_price=entry,
            entry_recommendation=mod.RecommendationType.HOLD,
            entry_score=70.0, entry_risk_bucket="LOW",
            entry_confidence="HIGH", origin_tab="Top_10_Investments",
            target_price=entry * (1.0 + target / 100.0), target_roi=target,
            target_date=now + timedelta(days=hz.days),
            status=S.MATURED, current_price=entry)
        rec.realized_roi = realized
        records.append(rec)
        # same-day 1M sibling: price-implied 1M thesis in pp such that the
        # checkpoint slice (target * 30 / hz.days) matches the stored value.
        thesis_pp = target * 30.0 / float(hz.days)
        sib = mod.PerformanceRecord(
            record_id="z%03dm" % i, symbol=sym, horizon=H.MONTH_1,
            date_recorded=now, entry_price=entry,
            entry_recommendation=mod.RecommendationType.HOLD,
            entry_score=70.0, entry_risk_bucket="LOW",
            entry_confidence="HIGH", origin_tab="Top_10_Investments",
            target_price=entry * (1.0 + thesis_pp / 100.0),
            target_roi=thesis_pp,
            target_date=now + timedelta(days=30),
            status=S.ACTIVE, current_price=entry)
        records.append(sib)
    # --- rows that must be excluded from BOTH measurements --------------
    def _extra(sym, hz, status, realized, target, days):
        r = mod.PerformanceRecord(
            record_id="x" + sym, symbol=sym, horizon=hz,
            date_recorded=day0, entry_price=100.0,
            entry_recommendation=mod.RecommendationType.HOLD,
            entry_score=70.0, entry_risk_bucket="LOW",
            entry_confidence="HIGH", origin_tab="Top_10_Investments",
            target_price=100.0 * (1.0 + target / 100.0), target_roi=target,
            target_date=day0 + timedelta(days=days),
            status=status, current_price=100.0)
        r.realized_roi = realized
        return r
    records.append(_extra("B1.US", H.WEEK_1, mod.PerformanceStatus.MATURED,
                          99.0, 0.0, 7))          # target 0 -> no forecast
    records.append(_extra("B2.US", H.WEEK_1, mod.PerformanceStatus.ACTIVE,
                          None, 1.0, 7))          # not matured
    records.append(_extra("B3.US", H.WEEK_2, mod.PerformanceStatus.MATURED,
                          None, 1.0, 14))         # no realized ROI
    records.append(_extra("B4.US", H.MONTH_3, mod.PerformanceStatus.MATURED,
                          77.0, 1.0, 90))         # wrong horizon
    expected = {
        "n": 4, "model_mae": 2.5, "zero_mae": 3.0,
        "by_horizon": {"1W": {"n": 2, "model": 2.0, "zero": 3.0},
                       "2W": {"n": 2, "model": 3.0, "zero": 3.0}},
    }
    return records, expected


def _mean_abs_realized(mod, records):
    """Independent recomputation of the zero-forecast MAE."""
    vals = []
    for r in records:
        if getattr(getattr(r, "horizon", None), "value", None) not in ("1W", "2W"):
            continue
        if r.status != mod.PerformanceStatus.MATURED:
            continue
        if r.realized_roi is None or float(r.target_roi) == 0.0:
            continue
        vals.append(abs(float(r.realized_roi)))
    return (round(sum(vals) / float(len(vals)), 2) if vals else None), len(vals)


def _publish(mod, recs, zero_gate, unit_gate=None):
    """Drive the REAL _publish_s1_calibration through the recorder."""
    sv_z = os.environ.get(GATE)
    sv_u = os.environ.get(UNIT_GATE)
    out_lines = []
    _real_out = mod._out
    mod._out = lambda s: out_lines.append(s)
    try:
        _env(GATE, zero_gate)
        _env(UNIT_GATE, unit_gate)
        a = _app(mod)
        a.store = _Store()
        ok = a._publish_s1_calibration(recs)
        cal = a.store.sheet.tabs.get(mod.S1_CAL_TAB)
        return ok, (cal.calls if cal else []), out_lines
    finally:
        mod._out = _real_out
        _env(GATE, sv_z)
        _env(UNIT_GATE, sv_u)


# ---------------------------------------------------------------- tests
def t1_pure_measurement_is_ungated(res):
    mod = _mod()
    recs, exp = build_corpus(mod)
    indep, indep_n = _mean_abs_realized(mod, recs)
    assert (indep, indep_n) == (exp["zero_mae"], exp["n"]), (indep, indep_n)
    seen = {}
    for gate in (None, "0", "off", "1", "publish"):
        sv = os.environ.get(GATE)
        try:
            _env(GATE, gate)
            rep = mod._s1_checkpoint_calibration_legacy(recs)
        finally:
            _env(GATE, sv)
        assert rep["n"] == exp["n"], (gate, rep["n"])
        assert rep["mean_abs_error_pp"] == exp["model_mae"], (gate, rep)
        assert rep["zero_mae_pp"] == exp["zero_mae"], (gate, rep)
        assert rep["zero_mae_pp"] == indep
        for hz, e in exp["by_horizon"].items():
            bh = rep["by_horizon"][hz]
            assert bh["n"] == e["n"] and bh["mean_abs_pp"] == e["model"], (hz, bh)
            assert bh["zero_mae_pp"] == e["zero"], (hz, bh)
        seen[str(gate)] = rep["zero_mae_pp"]
    assert len(set(seen.values())) == 1, seen
    # (2026-10-06) THE DIRECTION IS THE WHOLE POINT, SO ASSERT IT.
    # This line used to read `model < zero or model > zero`, which is just
    # "not equal" and passes whichever way round the numbers are -- while the
    # comment above it claimed the fixture showed the model LOSING. It did
    # not: this fixture's model MAE (2.50 pp) is LOWER, i.e. BETTER, than the
    # zero-forecast baseline (3.00 pp), so it is the model-WINS direction.
    # Lower MAE is better, and the consumer's rule is one line in
    # scripts/run_shadow_scorer.py: `beats = model_mae_pp < zero_mae_pp`.
    # Both directions are now pinned explicitly, here and in
    # test_c6_model_worse_than_zero_is_publishable below -- the second is the
    # only direction that can flip criterion 4, which is what P-201 exists
    # for, and it was previously never driven through the publisher at all.
    assert exp["model_mae"] == 2.50 and exp["zero_mae"] == 3.00, exp
    assert exp["model_mae"] < exp["zero_mae"], (
        "this fixture is the model-WINS direction", exp)
    assert mod._s1_zero_mae([]) is None and mod._s1_zero_mae(None) is None
    assert mod._s1_zero_mae([1.0, 2.0]) == 1.5
    empty = mod._s1_checkpoint_calibration_legacy([])
    assert empty["n"] == 0 and empty["zero_mae_pp"] is None
    # the unit-sentry cohort carries it too, gate or no gate
    sv = os.environ.get(GATE)
    try:
        _env(GATE, None)
        m_off = mod._s1_unit_sentry_measure(recs)
        _env(GATE, "publish")
        m_on = mod._s1_unit_sentry_measure(recs)
    finally:
        _env(GATE, sv)
    assert m_off["zero_mae_pp"] == m_on["zero_mae_pp"] == exp["zero_mae"], m_off
    assert m_off["n"] == exp["n"], m_off["n"]
    res["T1_pure"] = {"n": exp["n"], "model_mae": exp["model_mae"],
                      "zero_mae": exp["zero_mae"],
                      "by_horizon": exp["by_horizon"],
                      "zero_mae_by_gate": seen}
    return recs, exp


def t2_golden_negative_off(res, recs, exp):
    """Gate OFF -> every published surface is the pre-change one."""
    mod = _mod()
    ok, calls, lines = _publish(mod, recs, None)
    assert ok is True
    assert len(calls) == 1, calls            # exactly the legacy A1 write
    args = calls[0][1]
    assert args[0] == "A1", args[0]
    hdr, row = args[1][0], args[1][1]
    assert hdr == V641_HEADER, hdr
    assert len(hdr) == 10 and len(row) == 10, (len(hdr), len(row))
    assert list(mod.S1_CAL_HEADER) == V641_HEADER, "S1_CAL_HEADER was mutated"
    detail = str(row[V641_HEADER.index("Detail")])
    assert "zero_mae" not in detail, detail
    assert scorer_parse_zero_mae(hdr, row) is None, "OFF must publish nothing"
    assert len(lines) == 1 and "zero_mae" not in lines[0], lines
    # also OFF for the "0"/"off" words
    for word in ("0", "off", "false", "no", "junk"):
        ok2, calls2, lines2 = _publish(mod, recs, word)
        assert ok2 and len(calls2) == 1 and calls2[0][1][1][0] == V641_HEADER
        assert "zero_mae" not in str(calls2[0][1][1][1]), word
    # sentry block under observe: still six rows at A4:D9
    ok3, calls3, _ = _publish(mod, recs, None, unit_gate="observe")
    assert ok3 and len(calls3) == 2, calls3
    assert calls3[1][2]["range_name"] == "A4:D9", calls3[1][2]["range_name"]
    assert len(calls3[1][2]["values"]) == 6
    assert all(len(r) == 4 for r in calls3[1][2]["values"])
    res["T2_off_golden_negative"] = {
        "header_len": len(hdr), "row_len": len(row),
        "detail": detail[:90], "stdout": lines[0][:90],
        "sentry_range": calls3[1][2]["range_name"],
        "sentry_rows": len(calls3[1][2]["values"]),
        "scorer_reads": scorer_parse_zero_mae(hdr, row)}
    return hdr, row, detail, lines[0]


def t3_publish_on(res, recs, exp, off_hdr, off_row, off_detail, off_line):
    mod = _mod()
    ok, calls, lines = _publish(mod, recs, "publish")
    assert ok is True and len(calls) == 1, calls
    hdr, row = calls[0][1][1][0], calls[0][1][1][1]
    assert len(hdr) == 11 and hdr[-1] == "Zero MAE (pp)", hdr
    assert hdr[:10] == V641_HEADER, hdr
    assert list(mod.S1_CAL_HEADER) == V641_HEADER, "S1_CAL_HEADER was mutated"
    assert len(row) == 11, len(row)
    assert row[-1] == exp["zero_mae"], row[-1]
    # every pre-existing cell except Detail is untouched
    di = V641_HEADER.index("Detail")
    for i in range(10):
        if i == di or i == 0:            # 0 is the as-of timestamp
            continue
        assert row[i] == off_row[i], (i, row[i], off_row[i])
    detail = str(row[di])
    assert detail.startswith(off_detail), (detail, off_detail)
    token = " | zero_mae=%.2fpp" % exp["zero_mae"]
    assert detail.endswith(token), detail
    # PARSEABLE by the scorer's own regex, both ways
    assert scorer_parse_zero_mae(hdr, row) == exp["zero_mae"]
    assert scorer_parse_zero_mae(V641_HEADER, row[:10]) == exp["zero_mae"], \
        "the Detail token alone must carry the baseline"
    m = SCORER_ZERO_MAE_RE.search(detail)
    assert m is not None and abs(float(m.group(1)) - exp["zero_mae"]) < 1e-12
    # the stdout line carries the token too
    assert len(lines) == 1 and lines[0] == off_line + token, lines[0]
    # sentry block: seven rows at A4:D10, the last one the baseline
    ok2, calls2, _ = _publish(mod, recs, "publish", unit_gate="observe")
    assert ok2 and len(calls2) == 2, calls2
    blk = calls2[1][2]
    assert blk["range_name"] == "A4:D10", blk["range_name"]
    assert len(blk["values"]) == 7 and all(len(r) == 4 for r in blk["values"])
    extra = blk["values"][6]
    assert extra[0] == "zero-forecast baseline (|realized|)", extra
    assert extra[2] == exp["zero_mae"], extra      # legacy cohort
    assert extra[3] == exp["zero_mae"], extra      # unit-corrected cohort
    res["T3_publish"] = {"header_tail": hdr[-1], "header_len": len(hdr),
                         "row_tail": row[-1], "detail": detail[-40:],
                         "stdout_tail": lines[0][-40:],
                         "scorer_column": scorer_parse_zero_mae(hdr, row),
                         "scorer_detail_only":
                             scorer_parse_zero_mae(V641_HEADER, row[:10]),
                         "sentry_range": blk["range_name"],
                         "sentry_rows": len(blk["values"]),
                         "sentry_extra": extra}


def t4_enforce_basis_coupling(res, recs, exp):
    """Row 2's zero baseline must be on the SAME basis as row 2's model
    MAE: under enforce both come from the unit-corrected cohort."""
    mod = _mod()
    sv_z, sv_u = os.environ.get(GATE), os.environ.get(UNIT_GATE)
    try:
        _env(GATE, "publish")
        _env(UNIT_GATE, None)
        legacy = mod.s1_checkpoint_calibration(recs)
        _env(UNIT_GATE, "enforce")
        enf = mod.s1_checkpoint_calibration(recs)
    finally:
        _env(GATE, sv_z)
        _env(UNIT_GATE, sv_u)
    rep = enf["unit_sentry"]
    assert rep["n"] > 0, rep
    assert enf["n"] == rep["n"], (enf["n"], rep["n"])
    assert enf["mean_abs_error_pp"] == rep["mean_abs_error_pp"]
    assert enf["zero_mae_pp"] == rep["zero_mae_pp"], \
        "the published zero baseline must follow the enforced basis"
    assert rep["legacy"]["zero_mae_pp"] == legacy["zero_mae_pp"]
    assert "zero_mae_pp" in rep["by_horizon"][sorted(rep["by_horizon"])[0]]
    # and the publisher then prints the enforced value
    ok, calls, lines = _publish(mod, recs, "publish", unit_gate="enforce")
    assert ok and calls[0][1][1][1][-1] == rep["zero_mae_pp"]
    assert (" | zero_mae=%.2fpp" % float(rep["zero_mae_pp"])) in lines[0]
    res["T4_enforce_coupling"] = {
        "enforced_n": rep["n"],
        "enforced_model_mae": rep["mean_abs_error_pp"],
        "enforced_zero_mae": rep["zero_mae_pp"],
        "legacy_zero_mae": legacy["zero_mae_pp"],
        "published_cell": calls[0][1][1][1][-1]}


def t5_selftest(res):
    mod = _mod()
    sv = os.environ.get(GATE)
    try:
        _env(GATE, None)
        a = _app(mod)
        assert a._track_selftest_() is True, mod._TRACK_SELFTEST_MSG
    finally:
        _env(GATE, sv)
    msg = mod._TRACK_SELFTEST_MSG
    word, nums = msg.split()[0], msg.split()[1].split("/")
    p, q = int(nums[0]), int(nums[1])
    assert word == "PASS" and p == q and p > 0, msg
    assert GATE not in os.environ or os.environ.get(GATE) == sv
    res["T5_selftest"] = msg


def run_all():
    res = {}
    mod = _mod()
    assert mod.SCRIPT_VERSION >= "6.42.0", mod.SCRIPT_VERSION
    res["version"] = mod.SCRIPT_VERSION
    recs, exp = t1_pure_measurement_is_ungated(res)
    hdr, row, detail, line = t2_golden_negative_off(res, recs, exp)
    t3_publish_on(res, recs, exp, hdr, row, detail, line)
    t4_enforce_basis_coupling(res, recs, exp)
    t5_selftest(res)
    return res


# --------------------------------------------------- pytest entry points

def build_losing_corpus(mod):
    """(2026-10-06) THE MODEL-LOSES COHORT -- the direction P-201 exists for.

    Every qualifying row's forecast error is LARGER than the realized move,
    so predicting a flat zero would have been more accurate than the model:

      1W (realized +1.00, target +5.00) -> err -4.00  zero 1.00
      1W (realized -1.00, target +4.00) -> err -5.00  zero 1.00
      2W (realized +2.00, target -4.00) -> err +6.00  zero 2.00
      2W (realized -2.00, target +3.00) -> err -5.00  zero 2.00
    model MAE = (4 + 5 + 6 + 5) / 4 = 5.00 pp
    zero  MAE = (1 + 1 + 2 + 2) / 4 = 1.50 pp
    -> 5.00 > 1.50, so the consumer's rule `model_mae_pp < zero_mae_pp` is
       FALSE and criterion 4 must not be certified on this cohort, even
       though 5.00 pp sits comfortably inside the 10 pp band. That gap --
       in-band but worse than no forecast at all -- IS the P-201 defect.
    """
    H = mod.HorizonType
    S = mod.PerformanceStatus
    day0 = datetime(2026, 9, 1, 9, 0, 0, tzinfo=RIYADH)
    spec = [
        ("L1.US", H.WEEK_1, 1.0, 5.0),
        ("L2.US", H.WEEK_1, -1.0, 4.0),
        ("L3.US", H.WEEK_2, 2.0, -4.0),
        ("L4.US", H.WEEK_2, -2.0, 3.0),
    ]
    records = []
    for i, (sym, hz, realized, target) in enumerate(spec):
        now = day0 + timedelta(days=i)
        entry = 100.0
        rec = mod.PerformanceRecord(
            record_id="w%03d" % i, symbol=sym, horizon=hz,
            date_recorded=now, entry_price=entry,
            entry_recommendation=mod.RecommendationType.HOLD,
            entry_score=70.0, entry_risk_bucket="LOW",
            entry_confidence="HIGH", origin_tab="Top_10_Investments",
            target_price=entry * (1.0 + target / 100.0), target_roi=target,
            target_date=now + timedelta(days=hz.days),
            status=S.MATURED, current_price=entry)
        rec.realized_roi = realized
        records.append(rec)
    return records


def t6_model_worse_than_zero_is_published_and_not_certified(res):
    """The in-band-but-skill-less cohort, driven through the REAL publisher.

    Audit finding P2 (2026-10-06): no case in this harness or in the embedded
    self-test ever drove model-worse-than-zero through _publish_s1_calibration
    -- the only direction whose published numbers change an S-1 verdict. This
    case does, and it asserts the consumer's own comparison on the values it
    actually reads back off the sheet.
    """
    mod = _mod()
    recs = build_losing_corpus(mod)
    rep = mod._s1_checkpoint_calibration_legacy(recs)
    assert rep["n"] == 4, rep
    assert rep["mean_abs_error_pp"] == 5.00, rep
    assert rep["zero_mae_pp"] == 1.50, rep
    # the defect in one line: inside the 10 pp band, yet beaten by no forecast
    band = float(rep.get("band_pp") or 10.0)
    assert rep["mean_abs_error_pp"] <= band, (rep, band)
    assert rep["mean_abs_error_pp"] > rep["zero_mae_pp"], rep

    ok, calls, lines = _publish(mod, recs, "publish")
    assert ok is True and len(calls) == 1, calls
    hdr, row = calls[0][1][1][0], calls[0][1][1][1]
    assert len(hdr) == 11 and hdr[-1] == "Zero MAE (pp)", hdr
    published_zero = scorer_parse_zero_mae(hdr, row)
    assert published_zero == 1.50, (published_zero, row)

    # the consumer's rule, verbatim from run_shadow_scorer v1.9.2:
    #     beats = model_mae_pp < zero_mae_pp
    published_model = float(row[hdr.index("Mean Abs Error (pp)")])
    beats = published_model < published_zero
    assert beats is False, (published_model, published_zero)

    # and the Detail token alone carries the same number (column-free path)
    detail = str(row[hdr.index("Detail")])
    assert scorer_parse_zero_mae(["detail"], [detail]) == 1.50, detail
    assert "zero_mae=1.50pp" in detail, detail

    res["T6_model_loses"] = {
        "n": rep["n"], "model_mae": published_model,
        "zero_mae": published_zero, "band_pp": band,
        "in_band": published_model <= band,
        "consumer_beats_zero": beats,
        "detail": detail[-40:], "stdout": lines[0][-60:]}


def test_p201_model_worse_than_zero_is_not_certified():
    t6_model_worse_than_zero_is_published_and_not_certified({})

def test_p201_pure_measurement_is_ungated():
    t1_pure_measurement_is_ungated({})


def test_p201_golden_negative_gate_off():
    res = {}
    recs, exp = t1_pure_measurement_is_ungated(res)
    t2_golden_negative_off(res, recs, exp)


def test_p201_publish_header_token_and_block():
    res = {}
    recs, exp = t1_pure_measurement_is_ungated(res)
    hdr, row, detail, line = t2_golden_negative_off(res, recs, exp)
    t3_publish_on(res, recs, exp, hdr, row, detail, line)


def test_p201_enforce_basis_coupling():
    res = {}
    recs, exp = t1_pure_measurement_is_ungated(res)
    t4_enforce_basis_coupling(res, recs, exp)


def test_p201_selftest_still_green():
    t5_selftest({})


if __name__ == "__main__":
    _res = {}
    _res["version"] = _mod().SCRIPT_VERSION
    _ctx = {}

    def _c1():
        recs, exp = t1_pure_measurement_is_ungated(_res)
        _ctx["recs"], _ctx["exp"] = recs, exp

    def _c2():
        h, r, d, l = t2_golden_negative_off(_res, _ctx["recs"], _ctx["exp"])
        _ctx["off"] = (h, r, d, l)

    def _c3():
        t3_publish_on(_res, _ctx["recs"], _ctx["exp"], *_ctx["off"])

    def _c4():
        t4_enforce_basis_coupling(_res, _ctx["recs"], _ctx["exp"])

    def _c5():
        t5_selftest(_res)

    def _c6():
        t6_model_worse_than_zero_is_published_and_not_certified(_res)

    CASES = [("T1 pure ungated measurement", _c1),
             ("T2 golden negative (gate off)", _c2),
             ("T3 publish (column + token + block)", _c3),
             ("T4 enforce basis coupling", _c4),
             ("T5 embedded self-test", _c5),
             ("T6 model loses to zero (publisher + consumer rule)", _c6)]
    _passed = 0
    for _name, _fn in CASES:
        try:
            _fn()
            _passed += 1
        except Exception as _exc:
            print("FAIL %s -> %s: %s" % (_name, type(_exc).__name__, _exc))
    print("version ->", _res.get("version"))
    for _k in sorted(_res):
        if _k == "version":
            continue
        print(_k, "->", json.dumps(_res[_k], sort_keys=True, default=str)[:260])
    print("PASS %d/%d" % (_passed, len(CASES)))
    sys.exit(0 if _passed == len(CASES) else 1)
