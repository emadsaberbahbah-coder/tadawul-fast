"""tfb_backtest v1.2.0: the verdict's out-of-sample Brier is scored in date order.

External audit of 25 Nov 2026 (P1 validation): the random K-fold let cohorts from
the same day, and overlapping horizon windows, sit on both sides of a fold. These
known-answer cases pin the purged walk-forward that replaces it.

v1.3.0 (26 Nov 2026, week plan package 4): quintiles are cut per formation date, the
train-to-test gap is reported, and every output is named descriptive or out-of-sample.
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
    backtest.evaluate_signal(rows, "Sig", quintiles="pooled")   # v1.3.0: the pooled cut is the rollback path
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


def test_date_quintiles_rank_each_value_inside_its_own_day():
    d1 = D0 + dt.timedelta(days=1)
    xs = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] + [500, 100, 300, 200, 400] + [7, 7, 7] + [1.0]
    days = [D0] * 10 + [d1] * 5 + [D0 + dt.timedelta(days=2)] * 3 + [None]
    q = backtest.date_quintiles(xs, days)
    assert q[:10] == [1, 1, 2, 2, 3, 3, 4, 4, 5, 5]
    assert q[10:15] == [5, 1, 3, 2, 4]            # the rank inside the day, whatever the level of the values
    assert q[15:] == [None, None, None, None]     # a day with fewer than five values, and a row with no day
    # equal values share one quintile, and the order of the rows does not matter
    assert backtest.date_quintiles([5, 5, 5, 5, 5, 5], [D0] * 6) == [3] * 6
    tied = backtest.date_quintiles([1, 2, 2, 2, 2, 9], [D0] * 6)
    assert tied[1:5] == [tied[1]] * 4 and tied[0] == 1 and tied[5] == 5
    assert backtest.date_quintiles(xs[:10][::-1], days[:10]) == q[:10][::-1]


def _two_day_rows(later_values):
    # day 0: ten winners with values 1..10; day 1: ten losers whose values are the planted future
    rows = [_cohort(i, D0, True, Sig=str(i + 1)) for i in range(10)]
    rows += [_cohort(10 + i, D0 + dt.timedelta(days=1), False, Sig=str(v)) for i, v in enumerate(later_values)]
    return rows


def test_a_later_day_cannot_move_an_earlier_rows_quintile():
    above, below = list(range(11, 21)), list(range(-20, -10))

    def table(rows, **kw):
        return {g["group"]: g["win_pct"] for g in backtest.evaluate_signal(rows, "Sig", min_n=1, **kw)["groups"]}
    # the planted look-ahead: under the pooled cut the SAME day-0 rows are the bottom quintiles when the later
    # day's values are higher and the top quintiles when they are lower - their label is made of the future
    assert table(_two_day_rows(above), quintiles="pooled")["Q1"] == 100.0
    assert table(_two_day_rows(below), quintiles="pooled")["Q1"] == 0.0
    # cut per formation date, the later day changes nothing: each quintile holds two rows of each day
    by_date = table(_two_day_rows(above))
    assert by_date == table(_two_day_rows(below)) == {"Q%d" % k: 50.0 for k in range(1, 6)}


def test_market_level_score_separates_only_under_pooled_quintiles():
    rng = random.Random(11)
    rows = []
    for i in range(3000):
        up = (i // 50) % 2 == 0
        rows.append(_cohort(i, D0 + dt.timedelta(days=i // 50), rng.random() < (0.8 if up else 0.3),
                            Sig=f"{(70 if up else 30) + rng.uniform(-10, 10):.2f}"))
    pooled = backtest.evaluate_signal(rows, "Sig", quintiles="pooled")
    by_date = backtest.evaluate_signal(rows, "Sig")
    assert pooled["bands"] == "pooled_quintiles" and pooled["win_spread_pp"] >= 30 and pooled["verdict"] != "NONE"
    assert by_date["bands"] == "date_quintiles" and by_date["rows_without_date_quintile"] == 0
    assert by_date["win_spread_pp"] < 8 and by_date["verdict"] == "NONE"   # the days differ, the choice inside a day does not


def test_quintile_mode_flag_wins_over_env_and_thin_days_fall_back(monkeypatch):
    rows = [_cohort(i, D0 + dt.timedelta(days=i // 10), i % 2 == 0, Sig=str(i)) for i in range(100)]
    monkeypatch.setenv("TFB_BACKTEST_QUINTILES", "pooled")
    assert backtest.evaluate_signal(rows, "Sig")["bands"] == "pooled_quintiles"
    assert backtest.evaluate_signal(rows, "Sig", quintiles="date")["bands"] == "date_quintiles"
    monkeypatch.delenv("TFB_BACKTEST_QUINTILES")
    thin = [_cohort(i, D0 + dt.timedelta(days=i), i % 2 == 0, Sig=str(i)) for i in range(40)]   # one row a day
    res = backtest.evaluate_signal(thin, "Sig")
    assert res["bands"] == "pooled_quintiles (no formation day with 5 values)" and res["n"] == 40
    mixed = rows + [_cohort(500 + i, D0 + dt.timedelta(days=30 + i), True, Sig="1") for i in range(3)]
    res = backtest.evaluate_signal(mixed, "Sig")
    assert res["bands"] == "date_quintiles" and res["rows_without_date_quintile"] == 3 and res["n"] == 100


def test_gap_between_training_and_test_covers_the_horizon():
    rng = random.Random(5)
    for horizon, days in (("1W", 7), ("1M", 30)):
        rows = [_cohort(i, D0 + dt.timedelta(days=i // 20), rng.random() < 0.5, horizon=horizon, G="a" if i % 2 else "b")
                for i in range(3000)]
        res = backtest.evaluate_signal(rows, "G")
        assert res["wf_status"] == "ok" and res["wf_embargo_days"] == 1
        # a training row is usable once its own horizon (+ the availability pad) has passed, plus the embargo
        assert res["wf_min_gap_days"] >= days + backtest.AVAILABILITY_PAD_DAYS + 1
    wider = backtest.evaluate_signal(rows, "G", embargo_days=10)
    assert wider["wf_embargo_days"] == 10 and wider["wf_min_gap_days"] >= 30 + backtest.AVAILABILITY_PAD_DAYS + 10


def test_every_output_is_named_descriptive_or_out_of_sample(tmp_path, capsys):
    rng = random.Random(5)
    rows = [_cohort(i, D0 + dt.timedelta(days=i // 50), rng.random() < 0.3 + 0.5 * (i % 50) / 50, Sig=str(i % 50))
            for i in range(3000)]
    res = backtest.evaluate_signal(rows, "Sig")
    assert res["evidence"]["out_of_sample"] == ["wf_brier", "wf_base_brier", "wf_gain_vs_base"]
    assert set(res["evidence"]["descriptive"]) == {"base_win_pct", "base_brier", "groups", "raw_brier_as_probability",
                                                   "spearman_vs_roi", "win_spread_pp", "spread_z",
                                                   "cv_brier_group_calibrated", "cv_gain_vs_base"}
    # every number of the result is in one list or the other, or is a count / a status / a label
    plain = {"signal", "type", "n", "spearman_status", "wf_scored", "wf_folds", "wf_status", "wf_min_gap_days",
             "wf_embargo_days", "cv_mode", "verdict", "bands", "rows_without_date_quintile", "verdict_basis", "evidence"}
    assert set(res) == plain | set(res["evidence"]["descriptive"]) | set(res["evidence"]["out_of_sample"])
    assert res["verdict_basis"] == "walk-forward gain (out-of-sample) and win spread (descriptive)"
    assert backtest.evaluate_signal(rows, "Sig", cv="legacy")["verdict_basis"] == \
        "legacy random K-fold gain and win spread - both descriptive"
    text = backtest.render([res], "synthetic")
    body = [ln for ln in text.splitlines()[2:] if ln.startswith("    ") and not ln.lstrip().startswith(("group", "Q"))]
    assert body and all(ln.lstrip().startswith(("[descriptive]", "[out-of-sample]")) for ln in body)
    assert sum(ln.lstrip().startswith("[out-of-sample]") for ln in body) == 1
    assert "legacy random K-fold" in next(ln for ln in body if "K-fold" in ln) and "[descriptive]" in text.splitlines()[1]


def test_selftest_passes():
    assert backtest.main(["--selftest"]) == 0
