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


def teardown_module(_module):
    """Leave the imported module on the process-default basis."""
    os.environ.pop("TFB_TOP10_STABILITY_SCORE_BASIS", None)
    importlib.reload(importlib.import_module(MOD))
