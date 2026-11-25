"""repair_corporate_actions 1.2.0 [WITNESS-BASIS]: a finished outcome whose Target Date and Maturity stamp straddle a
split is no longer held when its own [OUTCOME-WITNESS v1] receipt names the quote session of the exit price (full audit
of 25 Nov 2026: one held Granite REIT row kept S-1 criterion 5 at "CA UNREPAIRED").

The fixture copies the shape of the live row (GRT-UN.TO 1W, recorded 14 Nov, target 21 Nov, witnessed session 23 Nov,
matured 24 Nov, split 3-for-1 effective 24 Nov) with invented record ids; no sheet, no network.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core import corporate_actions as ca  # noqa: E402
from scripts import repair_corporate_actions as rca  # noqa: E402
from tests.test_repair_corporate_actions_seed import HDR, _book, rec  # noqa: E402

IDX = ca.build_adjustment_index(ca.parse_actions([ca.ACTIONS_HEADER,
                                                  ["GRT-UN.TO", "SPLIT", "2026-11-24", "3", "", "", "issuer", "", ""]]))


def witness(price, session="2026-11-23", extra=""):
    body = {"acquired_at": "2026-11-24T02:39:45+00:00", "price": price, "quote_session": session,
            "provider": "eodhd", "state": "MATURED"}
    return "[TFB_TARGET_SCHEMA {\"version\":2}] | [OUTCOME-WITNESS v1] " + json.dumps(body) + extra


def row(notes, cur="88.92", tdate="2026-11-21", maturity="2026-11-24 05:43:14"):
    return rec("w1", "GRT-UN.TO", "2026-11-14 00:39:27", "87.49", "88.11622", "matured", cur, "1.634472511", "WIN",
               maturity, notes=notes, tdate=tdate)


def plan_of(r):
    plan, _, _ = rca.plan_repairs([HDR, r], IDX)
    assert len(plan) == 1
    return plan[0]


def test_w1_the_live_shape_is_repaired_on_the_witnessed_session(monkeypatch):
    monkeypatch.delenv("TFB_CA_WITNESS_BASIS", raising=False)
    p = plan_of(row(witness(88.92)))
    assert "held" not in p and p["exit_basis"] == "witness 2026-11-23"
    assert p["entry_new"] == pytest.approx(87.49 / 3) and p["exit_new"] == pytest.approx(88.92 / 3)
    assert p["roi_new"] == pytest.approx(1.6345, abs=1e-3) and p["outcome_new"] == "WIN"   # the ROI stands


def test_w2_a_session_on_the_new_basis_keeps_the_exit_price(monkeypatch):
    monkeypatch.delenv("TFB_CA_WITNESS_BASIS", raising=False)
    p = plan_of(row(witness(30.58, "2026-11-24"), cur="30.58"))
    assert p["exit_basis"] == "witness 2026-11-24" and p["exit_new"] == pytest.approx(30.58)
    assert p["roi_new"] == pytest.approx((30.58 / (87.49 / 3) - 1) * 100, abs=1e-3)


@pytest.mark.parametrize("notes,why", [
    ("", "no receipt"),
    ("[OUTCOME-WITNESS v1] not json", "unreadable receipt"),
    (None, "receipt for another price"),
    ("WITNESS_OUTSIDE", "session outside the two ends"),
    ("[OUTCOME-WITNESS v1] {\"price\": 88.92}", "no session"),
])
def test_w3_anything_else_is_held_as_before(monkeypatch, notes, why):
    monkeypatch.delenv("TFB_CA_WITNESS_BASIS", raising=False)
    if notes is None:
        notes = witness(88.0)
    elif notes == "WITNESS_OUTSIDE":
        notes = witness(88.92, "2026-11-30")
    p = plan_of(row(notes))
    assert "ambiguous" in p["held"] and "roi_new" not in p and "exit_basis" not in p, why


@pytest.mark.parametrize("off", ["off", "0", "false", "no", " OFF "])
def test_w4_the_rollback_restores_the_hold(monkeypatch, off):
    monkeypatch.setenv("TFB_CA_WITNESS_BASIS", off)
    assert "ambiguous" in plan_of(row(witness(88.92)))["held"]


def test_w5_rows_with_one_basis_are_unchanged_by_the_receipt(monkeypatch):
    monkeypatch.delenv("TFB_CA_WITNESS_BASIS", raising=False)
    both_before = plan_of(row(witness(77.0, "2026-11-10"), cur="88.92", tdate="2026-11-20",
                              maturity="2026-11-21 05:00:00"))
    assert "exit_basis" not in both_before and both_before["exit_new"] == pytest.approx(88.92 / 3)


def test_w6_apply_writes_the_repair_and_the_scorer_check_reads_clean(monkeypatch, tmp_path, capsys):
    monkeypatch.delenv("TFB_CA_WITNESS_BASIS", raising=False)
    seed = tmp_path / "seed.json"
    seed.write_text(json.dumps({"actions": []}), encoding="utf-8")
    register = [ca.ACTIONS_HEADER, ["GRT-UN.TO", "SPLIT", "2026-11-24", "3", "", "", "issuer", "", ""]]
    b = _book([HDR, row(witness(88.92))], register)
    monkeypatch.setattr(rca, "_open_sheet", lambda cli: b)
    assert rca.main(["--apply", "--seed", str(seed)]) == 0
    out = capsys.readouterr().out
    assert "held=0" in out and "HELD row" not in out
    assert b.tabs["Performance_Log"].batches, "the repair is written"
    written = {u["range"]: u["values"] for u in b.tabs["Performance_Log"].batches}
    flat = json.dumps(written)
    assert "ca_adjusted" in flat and "29.64" in flat


def test_w7_version_and_ci():
    assert rca.SCRIPT_VERSION == "1.2.0"
    src = (ROOT / "scripts/repair_corporate_actions.py").read_text(encoding="utf-8")
    assert "VERSION 1.2.0" in src and "TFB_CA_WITNESS_BASIS" in src
    wf = (ROOT / ".github/workflows/corporate_actions.yml").read_text(encoding="utf-8")
    assert "TFB_CA_WITNESS_BASIS: ${{ vars.TFB_CA_WITNESS_BASIS }}" in wf
