"""Operational publication failures must not masquerade as learning success.

All stores and prices are in-memory fixtures; no network/Sheet/mail writes.
S-1 FAIL and insufficient backtest evidence remain successful executions.
"""
import asyncio
import copy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripts import track_performance as track
from tests.test_shadow_scorer_regret_read_failure import (
    PRIVATE_ERROR, _sheet, runner, scorer,  # noqa: F401 -- reuse actual-main fixtures
)


@pytest.mark.parametrize("failed", [
    "S1_Gate", "Shadow_History", "Regret_Ledger", "Regret_Summary", "_Run_Log",
])
def test_scorer_attempts_every_publication_but_exits_nonzero_on_write_failure(
    scorer, runner, monkeypatch, capsys, failed,
):
    sheet = _sheet(scorer, ledger=[])
    attempted = []
    for title in (scorer.TAB_GATE, scorer.TAB_HISTORY, scorer.TAB_REGRET,
                  scorer.TAB_REGRET_SUMMARY, "_Run_Log"):
        worksheet = sheet.tabs[title]
        method = "update" if title in (scorer.TAB_GATE, scorer.TAB_REGRET_SUMMARY) else "append_rows"
        original = getattr(worksheet, method)

        def write(*args, _title=title, _original=original, **kwargs):
            attempted.append(_title)
            if _title == failed:
                raise PermissionError(PRIVATE_ERROR)
            return _original(*args, **kwargs)

        monkeypatch.setattr(worksheet, method, write)

    run, observations = runner
    assert run(sheet) == 1
    assert observations["gates"][0]["verdict"] == "PASS"  # execution failed, policy did not change
    assert attempted == [scorer.TAB_GATE, scorer.TAB_HISTORY, scorer.TAB_REGRET,
                         scorer.TAB_REGRET_SUMMARY, "_Run_Log"]
    output = capsys.readouterr().out
    assert f"WRITE FAILED {failed}:PermissionError" in output
    assert "partial writes possible" in output
    assert PRIVATE_ERROR not in output and "api_token=" not in output
    if failed != "_Run_Log":
        receipt = sheet.tabs["_Run_Log"].rows[-1]
        assert receipt[1] == "ERROR" and receipt[4] == "WRITE_FAILED"
        assert json.loads(receipt[9])["publication_errors"] == [failed + ":PermissionError"]
    else:
        # The four actual artifacts were published before the receipt failed.
        assert any(write[:2] == (scorer.TAB_HISTORY, "append") for write in sheet.writes)
    if failed in (scorer.TAB_GATE, scorer.TAB_REGRET_SUMMARY):
        # clear succeeded, update failed: the test proves genuine partial I/O.
        assert (failed, "clear") in sheet.writes
        assert sheet.tabs[failed].rows == []


def test_scorer_domain_fail_is_still_a_successful_publication(scorer, runner, monkeypatch):
    sheet = _sheet(scorer, ledger=[])
    evaluate = scorer.evaluate_s1

    def failed_gate(*args, **kwargs):
        result = evaluate(*args, **kwargs)
        result["verdict"], result["why"] = "FAIL", "synthetic policy rejection"
        return result

    monkeypatch.setattr(scorer, "evaluate_s1", failed_gate)
    run, observations = runner
    assert run(sheet) == 0
    assert observations["gates"][0]["verdict"] == "PASS"  # original evaluation fixture remains intact
    receipt = sheet.tabs["_Run_Log"].rows[-1]
    assert receipt[1] == "INFO" and receipt[4] == "GATE_FAIL"
    assert "publication_errors" not in json.loads(receipt[9])


def test_scorer_dry_run_never_exercises_publication(scorer, runner, monkeypatch):
    sheet = _sheet(scorer, ledger=[])

    def forbidden(*_args, **_kwargs):
        raise AssertionError("dry-run attempted publication")

    monkeypatch.setattr(scorer, "write_gate", forbidden)
    run, _ = runner
    assert run(sheet, ["--dry-run"]) == 0
    assert sheet.writes == []


@pytest.fixture
def backtest(monkeypatch):
    state = {"calls": [], "responses": [True, True]}

    class Registry:
        sheet_name = "Hypothesis_Registry"

        def __init__(self, *_args):
            pass

        def is_available(self):
            return True

        def load(self):
            return track.default_hypotheses()

        register_defaults = track.HypothesisRegistry.register_defaults

        def save(self, hypotheses):
            state["calls"].append(copy.deepcopy(hypotheses))
            value = state["responses"].pop(0)
            if isinstance(value, Exception):
                raise value
            return value

    monkeypatch.setattr(track, "HypothesisRegistry", Registry)
    # Isolate publication, while returning real result objects in the defined
    # INSUFFICIENT_DATA domain state. No trigger/gate policy is reimplemented.
    monkeypatch.setattr(track, "run_backtest", lambda hypothesis, *_args, **_kwargs: copy.copy(hypothesis))
    app = object.__new__(track.PerformanceTrackerApp)
    app.spreadsheet_id = "synthetic-sheet"
    app.args = SimpleNamespace(register_hypotheses=True, list_hypotheses=False,
                               backtest=True, hypothesis="", backtest_days=730)
    app._backtest_universe = lambda: ["SYNTHETIC.US"]
    app._fetch_backtest_history = AsyncMock(return_value={"SYNTHETIC.US": [{"close": 100}]})
    yield app, state
    track._shutdown_executor()


@pytest.mark.parametrize("failure", [False, None, PermissionError(PRIVATE_ERROR)])
@pytest.mark.parametrize("stage", ["registration", "results"])
def test_backtest_never_claims_failed_registry_publication(backtest, capsys, caplog, failure, stage):
    app, state = backtest
    state["responses"] = [failure] if stage == "registration" else [True, failure]
    assert asyncio.run(app.run_backtest_mode()) == 1
    assert len(state["calls"]) == (1 if stage == "registration" else 2)
    output = capsys.readouterr().out
    assert "results written" not in output
    if stage == "registration":
        assert "registered" not in output
        app._fetch_backtest_history.assert_not_awaited()
    else:
        app._fetch_backtest_history.assert_awaited_once()
    assert "registry " + stage + " write failed" in caplog.text
    assert "partial writes possible" in caplog.text
    assert PRIVATE_ERROR not in caplog.text + output


def test_acknowledged_insufficient_data_backtest_is_successful(backtest, capsys):
    app, state = backtest
    assert asyncio.run(app.run_backtest_mode()) == 0
    assert len(state["calls"]) == 2
    output = capsys.readouterr().out
    assert "results written to Hypothesis_Registry" in output
    assert "ACCEPTED 0/6" in output
    assert "history depth min=1 max=1 bars; requested=730 calendar days" in output


def test_actual_registry_partial_save_failure_is_sanitized(caplog):
    registry = object.__new__(track.HypothesisRegistry)
    mutations = []

    class Worksheet:
        def batch_clear(self, ranges):
            mutations.append(("clear", ranges))

        def update(self, **kwargs):
            mutations.append(("update", kwargs["range_name"]))
            raise PermissionError(PRIVATE_ERROR)

    registry.ws = Worksheet()
    registry.backoff = SimpleNamespace(execute_sync=lambda fn: fn())
    assert registry.save(track.default_hypotheses()) is False
    assert [kind for kind, _ in mutations] == ["clear", "update"]
    assert "PermissionError" in caplog.text and "partial writes possible" in caplog.text
    assert PRIVATE_ERROR not in caplog.text
