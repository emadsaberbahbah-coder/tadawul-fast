"""tests/test_grid_capacity_report_queue.py - a grid capacity REPORT never joins the production-write queue.

On 27 Nov 2026 at 11:07 UTC a report of .github/workflows/grid_capacity_repair.yml (apply=false) joined the
tadawul-production-write group while a Daily Sync dispatch waited there; GitHub cancelled the waiting sync before
it started. A report writes nothing, so it runs in its own group; an armed repair keeps the production-write group.
"""
from __future__ import annotations

from pathlib import Path

import yaml

WF = Path(".github/workflows/grid_capacity_repair.yml")
SYNC = Path(".github/workflows/daily_sync.yml")


def _load(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _group(expr: str, apply: str, ref: str = "refs/heads/main") -> str:
    """The workflow's own group expression evaluated for one value of inputs.apply (the two literal forms only)."""
    head = "${{ inputs.apply == 'true' && format('tadawul-production-write-{0}', github.ref) || "
    tail = "format('tfb-grid-capacity-report-{0}', github.ref) }}"
    assert expr == head + tail, expr
    return ("tadawul-production-write-" if apply == "true" else "tfb-grid-capacity-report-") + ref


def test_a_report_runs_in_its_own_group_and_an_armed_repair_keeps_the_production_queue():
    wf = _load(WF)
    conc = wf["concurrency"]
    assert conc["cancel-in-progress"] is False
    assert _group(conc["group"], "false") == "tfb-grid-capacity-report-refs/heads/main"
    assert _group(conc["group"], "true") == "tadawul-production-write-refs/heads/main"
    on = wf[True] if True in wf else wf["on"]
    assert list(on) == ["workflow_dispatch"]                                  # manual only, as before
    assert on["workflow_dispatch"]["inputs"]["apply"]["options"] == ["false", "true"]
    assert on["workflow_dispatch"]["inputs"]["apply"]["default"] == "false"


def test_the_armed_group_is_the_daily_syncs_own():
    sync = _load(SYNC)["concurrency"]["group"]
    assert "format('tadawul-production-write-{0}', github.ref)" in sync      # the queue a real repair must share
    assert "tfb-grid-capacity-report" not in sync


def test_the_header_says_why():
    text = WF.read_text(encoding="utf-8")
    assert "2026-11-28 -- A REPORT NO LONGER JOINS THE PRODUCTION-WRITE QUEUE" in text
    assert "run 45987841282" in text and "run 45980484268" in text
