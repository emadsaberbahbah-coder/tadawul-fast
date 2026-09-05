"""
tests/test_top10_selector.py
============================================================================
PY-10 — STABILITY HISTORY vs RANKING SCORE (top10_selector v4.25.0)

The defect this suite pins down: board ADMISSION ranks on _selector_score()
(the golden composite), but membership CHURN — capacity eviction,
displacement and the published top10_rank — ranked on the smoothed RAW
`overall_score`. Two different numbers deciding one board.

These tests are deliberately narrow. They assert the CONTRACT, not the
composite's weights (those belong to the golden-composite suite):

  1. the two scores genuinely disagree on a realistic snapshot — without
     this the whole finding would be theoretical;
  2. the basis flag resolves correctly and defaults to v4.24.0 behaviour;
  3. under the engine basis the stability history is byte-identical to
     v4.24.0 (the backward-safe default actually holds);
  4. under the selector basis the history stores the ranking score;
  5. flipping the basis CLEARS the persisted history instead of averaging
     two incompatible scales inside one smoothing window;
  6. a v1 state blob (no `hs` key) is read as the engine scale.
"""

import importlib
import os

import pytest

MOD = "core.analysis.top10_selector"


def _fresh(monkeypatch, basis=None):
    if basis is None:
        monkeypatch.delenv("TFB_TOP10_STABILITY_SCORE_BASIS", raising=False)
    else:
        monkeypatch.setenv("TFB_TOP10_STABILITY_SCORE_BASIS", basis)
    mod = importlib.import_module(MOD)
    return importlib.reload(mod)


def _row(symbol, overall, reliability, conviction, roi, risk=50.0):
    """A snapshot row carrying every golden-composite input."""
    return {
        "symbol": symbol,
        "overall_score": overall,
        "forecast_reliability_score": reliability,
        "conviction_score": conviction,
        "expected_roi_pct": roi,
        "risk_score": risk,
    }


# --------------------------------------------------------------------------- #
# 1. the two scores disagree — the finding is real, not theoretical
# --------------------------------------------------------------------------- #
def test_selector_score_and_overall_score_can_rank_a_snapshot_differently(monkeypatch):
    mod = _fresh(monkeypatch)
    criteria = {}

    # STRONG has the weaker raw engine score but wins the composite on
    # reliability + conviction + ROI (75% of the weights).
    strong = _row("STRONG.SR", overall=60.0, reliability=95.0,
                  conviction=95.0, roi=30.0)
    # WEAK leads on the raw engine score alone.
    weak = _row("WEAK.SR", overall=80.0, reliability=20.0,
                conviction=20.0, roi=0.0)

    s_strong = mod._selector_score(strong, criteria)
    s_weak = mod._selector_score(weak, criteria)

    # Engine basis prefers WEAK; composite prefers STRONG. If this ever stops
    # holding, the PY-10 finding needs re-deriving before the flag is trusted.
    assert weak["overall_score"] > strong["overall_score"]
    assert s_strong > s_weak, (
        "golden composite should favour the high-reliability/conviction row; "
        "got strong=%r weak=%r" % (s_strong, s_weak)
    )


# --------------------------------------------------------------------------- #
# 2. flag resolution
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("value,expected", [
    (None, "engine"),
    ("", "engine"),
    ("engine", "engine"),
    ("nonsense", "engine"),
    ("selector", "selector"),
    ("composite", "selector"),
    ("1", "selector"),
    ("true", "selector"),
    ("ON", "selector"),
])
def test_stability_score_basis_resolution(monkeypatch, value, expected):
    mod = _fresh(monkeypatch, value)
    assert mod._stability_score_basis() == expected


def test_default_is_the_v4_24_0_engine_basis(monkeypatch):
    """The backward-safe default must hold: unset env == old behaviour."""
    mod = _fresh(monkeypatch)
    assert mod._stability_score_basis() == mod.STABILITY_HIST_SCALE_ENGINE


# --------------------------------------------------------------------------- #
# 3 + 4. what actually lands in `hist`
# --------------------------------------------------------------------------- #
def _run_stability(mod, rows, state):
    pools = [[(0.0, r) for r in rows], [], [], []]
    return mod._apply_selection_stability(
        raw_selected=list(rows),
        pools=pools,
        criteria={"stability_state": state},
        knobs={"confirm_days": 1, "exit_days": 3, "smooth_days": 5,
               "rank_buffer": 0},
        limit=10,
    )


def test_engine_basis_stores_the_raw_overall_score(monkeypatch):
    mod = _fresh(monkeypatch, "engine")
    row = _row("AAA.SR", overall=60.0, reliability=95.0, conviction=95.0, roi=30.0)
    _rows, meta = _run_stability(mod, [row], {})
    hist = meta["state"]["symbols"]["AAA.SR"]["hist"]
    assert hist == [60.0], "engine basis must keep storing overall_score"


def test_selector_basis_stores_the_ranking_score(monkeypatch):
    mod = _fresh(monkeypatch, "selector")
    row = _row("AAA.SR", overall=60.0, reliability=95.0, conviction=95.0, roi=30.0)
    expected = round(mod._selector_score(row, {}), 4)
    _rows, meta = _run_stability(mod, [row], {})
    st = meta["state"]["symbols"]["AAA.SR"]
    assert st["hist"] == [expected], (
        "selector basis must store the same score that ranked the row"
    )
    assert st["hist"] != [60.0], "must no longer be the raw engine score"
    assert st["hs"] == "selector"


# --------------------------------------------------------------------------- #
# 5. scale migration must clear, never blend
# --------------------------------------------------------------------------- #
def test_flipping_the_basis_clears_history_instead_of_mixing_scales(monkeypatch):
    mod = _fresh(monkeypatch, "selector")
    row = _row("AAA.SR", overall=60.0, reliability=95.0, conviction=95.0, roi=30.0)

    # A persisted v4.24.0 blob: five engine-scale points, no `hs` key.
    legacy_state = {
        "v": 1,
        "date": "1970-01-01",
        "symbols": {
            "AAA.SR": {"ci": 3, "co": 0, "member": True, "since": "1970-01-01",
                       "ls": "1970-01-01",
                       "hist": [60.0, 60.0, 60.0, 60.0, 60.0]},
        },
    }
    _rows, meta = _run_stability(mod, [row], legacy_state)
    st = meta["state"]["symbols"]["AAA.SR"]

    assert len(st["hist"]) == 1, (
        "stale engine-scale points must be dropped, not averaged with "
        "composite-scale points; got %r" % (st["hist"],)
    )
    assert 60.0 not in st["hist"]
    assert st["hs"] == "selector"
    # Clocks and membership are NOT reset by a scale migration.
    assert st["member"] is True
    assert st["ci"] >= 3


def test_v1_state_blob_without_hs_reads_as_engine_scale(monkeypatch):
    mod = _fresh(monkeypatch, "engine")
    parsed = mod._stability_parse_state({
        "v": 1, "date": "1970-01-01",
        "symbols": {"AAA.SR": {"ci": 1, "co": 0, "member": True,
                               "since": "1970-01-01", "ls": "1970-01-01",
                               "hist": [60.0]}},
    })
    assert parsed["symbols"]["AAA.SR"]["hs"] == "engine"
    # ...and an engine-basis run therefore does NOT clear it.
    row = _row("AAA.SR", overall=60.0, reliability=95.0, conviction=95.0, roi=30.0)
    _rows, meta = _run_stability(mod, [row], parsed)
    assert len(meta["state"]["symbols"]["AAA.SR"]["hist"]) == 2


# --------------------------------------------------------------------------- #
# 7. v4.31.0 [BC-6] — a fast-track seat stays sizing-withheld until confirmed
# --------------------------------------------------------------------------- #
def _run_stab_on(mod, monkeypatch, rows, state, day, confirm_days=3):
    """Run the stability layer as if today were `day` (ISO date)."""
    monkeypatch.setattr(mod, "_stability_today_key", lambda: day)
    pools = [[(0.0, r) for r in rows], [], [], []]
    return mod._apply_selection_stability(
        raw_selected=list(rows),
        pools=pools,
        criteria={"stability_state": state},
        knobs={"confirm_days": confirm_days, "exit_days": 3,
               "smooth_days": 5, "rank_buffer": 0},
        limit=10,
    )


def test_fast_track_seat_stays_withheld_on_a_same_day_rerun(monkeypatch):
    """The live 2026-09-05 defect: 03:40 FAST-TRACK (day 1) -> 07:46 ACTIVE
    (day 1) -> 08:08 EXECUTABLE with Confirm Days = 3 and ci = 1."""
    mod = _fresh(monkeypatch)
    monkeypatch.delenv("TFB_T10_FASTTRACK_LEGACY", raising=False)
    row = _row("AEFES.IS", overall=70.0, reliability=75.7, conviction=70.0,
               roi=35.0)
    rows1, meta1 = _run_stab_on(mod, monkeypatch, [row], {}, "2026-09-05")
    assert rows1[0]["stability_status"] == "FAST-TRACK (day 1)"
    assert meta1["state"]["symbols"]["AEFES.IS"]["ft"] is True
    assert meta1["audit"]["fast_track_unconfirmed"] == ["AEFES.IS"]
    # same-day re-run with the round-tripped blob: must NOT read ACTIVE
    rows2, meta2 = _run_stab_on(mod, monkeypatch, [row], meta1["state"],
                                "2026-09-05")
    assert rows2[0]["stability_status"] == "FAST-TRACK (day 1, 1/3 confirmed)"
    assert meta2["audit"]["fast_track_unconfirmed"] == ["AEFES.IS"]
    assert meta2["audit"]["final_order"] == meta1["audit"]["final_order"]
    assert meta2["fast_track_legacy"] is False


def test_fast_track_seat_graduates_to_active_after_confirm_days(monkeypatch):
    mod = _fresh(monkeypatch)
    monkeypatch.delenv("TFB_T10_FASTTRACK_LEGACY", raising=False)
    row = _row("AEFES.IS", overall=70.0, reliability=75.7, conviction=70.0,
               roi=35.0)
    state = {}
    seen = []
    for day in ("2026-09-05", "2026-09-06", "2026-09-07"):
        rows, meta = _run_stab_on(mod, monkeypatch, [row], state, day)
        state = meta["state"]
        seen.append(rows[0]["stability_status"])
    assert seen == ["FAST-TRACK (day 1)",
                    "FAST-TRACK (day 2, 2/3 confirmed)",
                    "ACTIVE (day 3)"], seen
    assert state["symbols"]["AEFES.IS"]["ft"] is False
    assert meta["audit"]["fast_track_unconfirmed"] == []


def test_fast_track_kill_switch_restores_v4_30_labels(monkeypatch):
    mod = _fresh(monkeypatch)
    row = _row("AEFES.IS", overall=70.0, reliability=75.7, conviction=70.0,
               roi=35.0)
    monkeypatch.delenv("TFB_T10_FASTTRACK_LEGACY", raising=False)
    _rows1, meta1 = _run_stab_on(mod, monkeypatch, [row], {}, "2026-09-05")
    monkeypatch.setenv("TFB_T10_FASTTRACK_LEGACY", "1")
    rows2, meta2 = _run_stab_on(mod, monkeypatch, [row], meta1["state"],
                                "2026-09-05")
    assert rows2[0]["stability_status"] == "ACTIVE (day 1)"   # v4.30.0 label
    assert meta2["fast_track_legacy"] is True
    assert meta2["audit"]["final_order"] == meta1["audit"]["final_order"]


def test_legacy_blob_without_ft_keeps_pre_existing_members_active(monkeypatch):
    mod = _fresh(monkeypatch)
    monkeypatch.delenv("TFB_T10_FASTTRACK_LEGACY", raising=False)
    legacy = {"v": 2, "date": "2026-09-04",
              "symbols": {"AAA.SR": {"ci": 1, "co": 0, "member": True,
                                     "since": "2026-09-04",
                                     "ls": "2026-09-04", "hist": [60.0]}}}
    parsed = mod._stability_parse_state(legacy)
    assert parsed["symbols"]["AAA.SR"]["ft"] is False
    row = _row("AAA.SR", overall=60.0, reliability=95.0, conviction=95.0,
               roi=30.0)
    rows, _meta = _run_stab_on(mod, monkeypatch, [row], legacy, "2026-09-05")
    assert rows[0]["stability_status"] == "ACTIVE (day 2)"


def test_confirmed_entry_is_never_flagged_fast_track(monkeypatch):
    """A challenger that enters by confirmation (ci >= confirm_days, seats
    full while it accrued) carries ft=False and reads NEW (confirmed 3/3)
    exactly as in v4.30.0 — BC-6 touches fast-track seats only."""
    mod = _fresh(monkeypatch)
    monkeypatch.delenv("TFB_T10_FASTTRACK_LEGACY", raising=False)
    inc = _row("INC.SR", overall=80.0, reliability=95.0, conviction=95.0,
               roi=30.0)
    cha = _row("CHA.SR", overall=70.0, reliability=90.0, conviction=90.0,
               roi=25.0)
    knobs = {"confirm_days": 3, "exit_days": 3, "smooth_days": 5,
             "rank_buffer": 0}
    state = {}
    # Day 1: the single seat goes to INC (fast-track fill of an empty board).
    monkeypatch.setattr(mod, "_stability_today_key", lambda: "2026-09-01")
    _r, meta = mod._apply_selection_stability(
        raw_selected=[inc], pools=[[(0.0, inc)], [], [], []],
        criteria={"stability_state": {}}, knobs=knobs, limit=1)
    state = meta["state"]
    # Days 2-3: seats FULL (limit=1); CHA ranks first but must wait (pending).
    for day in ("2026-09-02", "2026-09-03"):
        monkeypatch.setattr(mod, "_stability_today_key", lambda d=day: d)
        rows, meta = mod._apply_selection_stability(
            raw_selected=[cha, inc], pools=[[(0.0, cha), (0.0, inc)], [], [], []],
            criteria={"stability_state": state}, knobs=knobs, limit=1)
        state = meta["state"]
        assert [r["symbol"] for r in rows] == ["INC.SR"]
        assert [p["symbol"] for p in meta["audit"]["pending"]] == ["CHA.SR"]
    assert state["symbols"]["CHA.SR"]["ci"] == 2
    # Day 4: a second seat opens; CHA has ci=3 -> enters CONFIRMED, not fast-track.
    monkeypatch.setattr(mod, "_stability_today_key", lambda: "2026-09-04")
    rows, meta = mod._apply_selection_stability(
        raw_selected=[cha, inc], pools=[[(0.0, cha), (0.0, inc)], [], [], []],
        criteria={"stability_state": state}, knobs=knobs, limit=2)
    st = {r["symbol"]: r["stability_status"] for r in rows}
    assert st["CHA.SR"] == "NEW (confirmed 3/3)", st
    assert meta["audit"]["entered"] == ["CHA.SR"]
    assert meta["audit"]["fast_tracked"] == []
    assert meta["state"]["symbols"]["CHA.SR"]["ft"] is False
    assert meta["audit"]["fast_track_unconfirmed"] == []


def teardown_module(_module):
    """Leave the imported module on the process-default basis."""
    os.environ.pop("TFB_TOP10_STABILITY_SCORE_BASIS", None)
    importlib.reload(importlib.import_module(MOD))
