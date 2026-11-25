"""data_engine_v2 5.165.0 [HOLD-PROSE]: the post-gate HOLD owns the reason and sizing hint.

External audit of 25 Nov 2026 (P1 analysis): My_Portfolio GLPI / AX / IBOC read HOLD / WATCH
while their reason said "Scale in / accumulate ... supports a gradual position" and the hint said
"Scale in gradually / partial position".
"""
import pytest

from core import data_engine_v2 as de

SCALE_IN_REASON = ("ACCUMULATE: Scale in / accumulate — moderate 1M return (score 63.3) supports a gradual "
                   "position, but is below the BUY threshold for a full entry. | overall=63.3 | risk=41.0")


def _row(**kw):
    row = {"recommendation": "HOLD", "recommendation_detailed": "HOLD",
           "recommendation_reason": SCALE_IN_REASON,
           "position_size_hint": "Scale in gradually / partial position"}
    row.update(kw)
    return row


@pytest.fixture(autouse=True)
def _default_env(monkeypatch):
    monkeypatch.delenv("TFB_HOLD_PROSE_POST_GATE", raising=False)


def test_demoted_hold_drops_add_capital_prose_and_labels_the_pre_gate_view():
    row = _row()
    de._reconcile_recommendation_family(row)
    assert row["position_size_hint"] == "Maintain or trim"
    assert row["recommendation_reason"].startswith("HOLD: no new capital now; pre-gate view: Scale in / accumulate")
    assert row["recommendation_reason"].endswith("| overall=63.3 | risk=41.0")


def test_rewrite_is_idempotent():
    row = _row()
    de._reconcile_recommendation_family(row)
    once = dict(row)
    de._reconcile_recommendation_family(row)
    assert row == once


def test_coherence_pass_path_from_accumulate_to_hold():
    row = _row(recommendation="ACCUMULATE", recommendation_detailed="ACCUMULATE",
               block_reason="Strict gate: forecast reliability below floor")
    de._apply_reco_coherence(row)
    assert row["recommendation"] == "HOLD"
    assert "Scale in gradually" not in row["position_size_hint"]
    assert "pre-gate view:" in row["recommendation_reason"]


@pytest.mark.parametrize("reco", ["ACCUMULATE", "BUY", "STRONG_BUY"])
def test_buy_family_keeps_its_scale_in_prose(reco):
    row = _row(recommendation=reco, recommendation_detailed=reco)
    de._reconcile_recommendation_family(row)
    assert row["position_size_hint"] == "Scale in gradually / partial position"
    assert "pre-gate view" not in row["recommendation_reason"]


@pytest.mark.parametrize("hint", ["Maintain / watch for better entry", "Hold existing position; no new capital",
                                  "Maintain or trim", "Trim into strength"])
def test_hold_keeps_hold_language_hints(hint):
    row = _row(position_size_hint=hint,
               recommendation_reason="HOLD: Strong profile, but expected return does not justify adding exposure now.")
    de._reconcile_recommendation_family(row)
    assert row["position_size_hint"] == hint
    assert row["recommendation_reason"] == ("HOLD: Strong profile, but expected return does not justify "
                                            "adding exposure now.")


def test_off_is_the_v5164_output(monkeypatch):
    monkeypatch.setenv("TFB_HOLD_PROSE_POST_GATE", "off")
    row = _row()
    de._reconcile_recommendation_family(row)
    assert row["position_size_hint"] == "Scale in gradually / partial position"
    assert row["recommendation_reason"] == "HOLD:" + SCALE_IN_REASON.partition(":")[2]


def test_sell_family_rules_unchanged():
    row = _row(recommendation="REDUCE", recommendation_detailed="REDUCE")
    de._reconcile_recommendation_family(row)
    assert row["position_size_hint"] == "Avoid / reduce"
    assert "pre-gate view" not in row["recommendation_reason"]


def test_version_pins():
    assert de.__version__ == "5.165.0"
