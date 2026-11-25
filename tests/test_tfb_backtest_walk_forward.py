"""tfb_backtest v1.2.0: the verdict's out-of-sample Brier is scored in date order.

External audit of 25 Nov 2026 (P1 validation): the random K-fold let cohorts from
the same day, and overlapping horizon windows, sit on both sides of a fold. These
known-answer cases pin the purged walk-forward that replaces it.
"""
import datetime as dt
import random

import pytest

from scripts import tfb_backtest as backtest

D0 = dt.date(2026, 7, 1)


def _cohort(i, day, win, horizon="1W", **extra):
    row = {"Record ID": str(i), "Key": f"K{i}", "Status": "matured", "Outcome": "WIN" if win else "LOSS",
           "Realized ROI %": "1" if win else "-1", "Horizon": horizon,
           "Date Recorded (Riyadh)": day.isoformat() + " 09:00:00"}
    row.update(extra)
    return row


def _day_shock_rows(n_days=60, per_day=50, seed=3):
    rng = random.Random(seed)
    rows = []
    for i in range(n_days * per_day):
        day = D0 + dt.timedelta(days=i // per_day)
        p = 0.8 if (i // per_day) % 2 == 0 else 0.3
        rows.append(_cohort(i, day, rng.random() < p, Day=day.isoformat()))
    return rows


def test_entry_day_label_gains_only_under_the_legacy_random_kfold():
    rows = _day_shock_rows()
    wf = backtest.evaluate_signal(rows, "Day")
    legacy = backtest.evaluate_signal(rows, "Day", cv="legacy")
    assert legacy["cv_gain_vs_base"] > 0.02          # the leak: same-day cohorts on both sides
    assert wf["wf_status"] == "ok" and wf["wf_folds"] >= 4   # block 1 has no outcome known yet (7 + 3 days)
    assert wf["wf_gain_vs_base"] <= 0.0005            # a day not yet seen carries nothing
    assert wf["verdict"] == "NONE" and legacy["verdict"] == "WEAK"


def test_legacy_mode_reproduces_the_v111_verdict_rule(monkeypatch):
    rows = _day_shock_rows()
    monkeypatch.setenv("TFB_BACKTEST_CV", "legacy")
    via_env = backtest.evaluate_signal(rows, "Day")
    assert via_env["cv_mode"] == "legacy" and via_env["verdict"] == "WEAK"
    assert backtest.evaluate_signal(rows, "Day", cv="wf")["verdict"] == "NONE"   # the flag wins over env


def test_training_set_holds_only_outcomes_known_before_the_test_block():
    ys = [1.0] * 40 + [0.0] * 40
    entry = [D0] * 40 + [D0 + dt.timedelta(days=3)] * 40
    known = [D0 + dt.timedelta(days=30)] * 40 + [D0 + dt.timedelta(days=33)] * 40   # 1M horizon
    seen = []

    def labeler_for(train):
        seen.append(list(train))
        return lambda i: "g"
    out = backtest.wf_brier(ys, entry, known, labeler_for, k=1, min_train=1)
    # the first block's outcomes are known on day 30, after the second block starts on day 3: purged
    assert seen == [] and out["wf_status"] == "insufficient_training_history"


def test_embargo_excludes_an_outcome_known_on_the_test_start_day():
    ys = [1.0, 0.0, 1.0, 0.0]
    entry = [D0, D0, D0 + dt.timedelta(days=7), D0 + dt.timedelta(days=7)]
    known = [D0 + dt.timedelta(days=7)] * 2 + [D0 + dt.timedelta(days=14)] * 2
    calls = []

    def labeler_for(train):
        calls.append(sorted(train))
        return lambda i: "g"
    backtest.wf_brier(ys, entry, known, labeler_for, k=1, embargo_days=0, min_train=1)
    backtest.wf_brier(ys, entry, known, labeler_for, k=1, embargo_days=1, min_train=1)
    assert calls == [[0, 1]]          # embargo 0 trains on day-7 outcomes; embargo 1 does not


def test_quintile_cuts_are_refitted_on_training_rows_only(monkeypatch):
    rows = [_cohort(i, D0 + dt.timedelta(days=i // 10), i % 2 == 0, Sig=str(i)) for i in range(100)]
    captured = {}
    real = backtest.wf_brier

    def spy(ys, entry, known, labeler_for, **kw):
        captured["labeler_for"] = labeler_for
        return real(ys, entry, known, labeler_for, **kw)
    monkeypatch.setattr(backtest, "wf_brier", spy)
    backtest.evaluate_signal(rows, "Sig")
    label = captured["labeler_for"](list(range(10)))      # train = the ten lowest values (0..9)
    # cuts from 0..9 are 2,4,6,8: value 50 sits above them all (Q5); cuts from all rows would put it in Q3
    assert label(50) == 5 and label(1) == 1 and label(5) == 3


def test_stationary_quintile_signal_keeps_its_out_of_sample_gain():
    rng = random.Random(5)
    rows = []
    for i in range(3000):
        sig = rng.uniform(0, 100)
        rows.append(_cohort(i, D0 + dt.timedelta(days=i // 50), rng.random() < 0.3 + 0.5 * sig / 100, Sig=f"{sig:.2f}"))
    res = backtest.evaluate_signal(rows, "Sig")
    assert res["wf_status"] == "ok" and res["wf_gain_vs_base"] > 0.01 and res["verdict"] == "SEPARATES"


def test_known_day_is_the_maturity_date_else_the_padded_target():
    rows = [_cohort(1, D0, True, horizon="1W", **{"Target Date (Riyadh)": "2026-07-20 16:00:00"}),
            _cohort(2, D0, False, horizon="1M"),
            _cohort(3, D0, False, horizon=""),
            _cohort(4, D0, True, **{"Target Date (Riyadh)": "2026-06-01"}),
            # a Saturday target settled at Monday's close: the recorded Maturity Date governs
            _cohort(5, D0, True, **{"Target Date (Riyadh)": "2026-07-11", "Maturity Date": "2026-07-13 23:10:00"}),
            # a Maturity Date before the target is not evidence of availability: pad the target instead
            _cohort(6, D0, True, **{"Target Date (Riyadh)": "2026-07-11", "Maturity Date": "2026-07-09"})]
    entry, known = backtest.label_days(rows)
    assert known == [dt.date(2026, 7, 23), dt.date(2026, 8, 3), None, None,
                     dt.date(2026, 7, 13), dt.date(2026, 7, 14)]
    assert entry == [D0] * 6


def test_weekend_target_does_not_train_the_next_days_block():
    # Codex review: a Saturday target must not enter training for a Sunday test block (outcome needs Monday).
    ys = [1.0, 0.0, 1.0, 0.0]
    sat, sun = dt.date(2026, 7, 11), dt.date(2026, 7, 12)
    rows = [_cohort(i, dt.date(2026, 7, 4), bool(y), **{"Target Date (Riyadh)": sat.isoformat()}) for i, y in enumerate(ys[:2])]
    rows += [_cohort(2 + i, sun, bool(y)) for i, y in enumerate(ys[2:])]
    entry, known = backtest.label_days(rows)
    calls = []

    def labeler_for(train):
        calls.append(sorted(train))
        return lambda i: "g"
    backtest.wf_brier(ys, entry, [k or sun for k in known], labeler_for, k=1, embargo_days=0, min_train=1)
    assert calls == []          # padded to Tuesday: nothing is known before Sunday's block


def test_selftest_passes():
    assert backtest.main(["--selftest"]) == 0
