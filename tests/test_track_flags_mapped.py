#!/usr/bin/env python3
"""tests/test_track_flags_mapped.py - the tracker's env-gated flags are MAPPED
in BOTH tracker steps, with identical values (ENV PARITY), and the workflow
fallbacks equal the script defaults.

WHY (2026-10-11, WP-7 / findings 'track-flags-unmapped')
--------------------------------------------------------
track_performance v6.41.0 shipped TRACK_DEDUP_MATURED and TRACK_RISK_BLANK on
2026-10-01 and named the Performance Clock run step as their env lane - but
no workflow ever mapped them, so a repo Variable could never reach
os.getenv(): the 2026-10-10 export still carries the 27 MATURED/MATURED
duplicate Keys and 0.0 in the three risk cells on every matured row, and none
of the 284 [PERF-VERDICT] _Run_Log lines carries 'dedup_matured'. This is the
kill-switch-without-mapping class. v6.44.0 adds TRACK_DEDUP_SIGNAL_HISTORY and
the same delivery maps all three in both tracker steps.

The parity rule (track_performance.yml's own ENV PARITY note) matters most for
TRACK_RISK_BLANK: PerformanceStore.save_records rewrites every row, so a flag
armed in one lane only is undone by the other lane's next save.

YAML text only (pattern: tests/test_scheduled_portfolio_refresh.py's
`re.findall(r"^\\s+NAME:\\s*(.+)$", text, re.M) == [...]`); no workflow is
executed, no module imported, no network. Runs under pytest or directly.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLOCK = ROOT / ".github/workflows/track_performance.yml"
DAILY = ROOT / ".github/workflows/daily_sync.yml"
TRACKER = ROOT / "scripts/track_performance.py"

# (env name, workflow fallback) - the fallback MUST equal the script default
FLAGS = [("TRACK_DEDUP_MATURED", "off"),
         ("TRACK_RISK_BLANK", "0"),
         ("TRACK_DEDUP_SIGNAL_HISTORY", "off")]

# the reader function and the literal it falls back to in the script
READERS = {"TRACK_DEDUP_MATURED": ("_dedup_matured_mode", "off"),
           "TRACK_RISK_BLANK": ("_risk_blank_enabled", "0"),
           "TRACK_DEDUP_SIGNAL_HISTORY": ("_dedup_signal_mode", "off")}

STEP_NAMES = {CLOCK: "Run performance clock",
              DAILY: "Track Performance (record + audit)"}


def _expected(name, default):
    return "${{ vars.%s || '%s' }}" % (name, default)


def _declarations(text, name):
    return re.findall(r"^\s+%s:\s*(.+)$" % re.escape(name), text, re.M)


def _tracker_step(path):
    """The text of the tracker step only: both files indent steps by six
    spaces (`      - name: `), and daily_sync.yml nests other `- name:` lines
    deeper, so split on the six-space form and pick the slice whose header
    carries the tracker step's name."""
    text = path.read_text(encoding="utf-8")
    parts = re.split(r"^\s{6}- name: ", text, flags=re.M)
    hits = [p for p in parts if p.splitlines() and STEP_NAMES[path] in p.splitlines()[0]]
    assert len(hits) == 1, (path.name, len(hits))
    return hits[0]


def _three_lines(path):
    """The three mapping lines of the tracker step, in file order, stripped."""
    step = _tracker_step(path)
    lines = [ln.strip() for ln in step.splitlines()
             if re.match(r"^\s+(TRACK_DEDUP_MATURED|TRACK_RISK_BLANK|TRACK_DEDUP_SIGNAL_HISTORY):", ln)]
    return lines


def test_tracker_flags_mapped_in_performance_clock():
    text = CLOCK.read_text(encoding="utf-8")
    step = _tracker_step(CLOCK)
    for name, default in FLAGS:
        assert _declarations(text, name) == [_expected(name, default)], (name, _declarations(text, name))
        assert _declarations(step, name) == [_expected(name, default)], (name, "not inside the run step")


def test_tracker_flags_mapped_in_daily_sync_tracker_step():
    text = DAILY.read_text(encoding="utf-8")
    step = _tracker_step(DAILY)
    for name, default in FLAGS:
        assert _declarations(text, name) == [_expected(name, default)], (name, _declarations(text, name))
        assert _declarations(step, name) == [_expected(name, default)], (name, "not inside the tracker step")


def test_tracker_env_parity_for_dedup_flags():
    clock = _three_lines(CLOCK)
    daily = _three_lines(DAILY)
    assert clock == daily, (clock, daily)
    assert clock == ["%s: %s" % (n, _expected(n, d)) for n, d in FLAGS], clock


def test_workflow_defaults_match_script_defaults():
    src = TRACKER.read_text(encoding="utf-8")
    for name, default in FLAGS:
        fn, literal = READERS[name]
        assert literal == default, (name, literal, default)
        m = re.search(r"^def %s\(\)[^\n]*:\n(.*?)(?=^def |\Z)" % re.escape(fn), src, re.M | re.S)
        assert m, "reader %s missing from scripts/track_performance.py" % fn
        body = m.group(1)
        pat = r'os\.getenv\("%s"\)\s*or\s*"%s"' % (re.escape(name), re.escape(default))
        assert re.search(pat, body), (fn, "fallback literal is not %r" % default)


def test_mapping_comments_carry_date_and_rollback():
    """The house style: a dated WHY and a rollback line next to every mapping."""
    for path in (CLOCK, DAILY):
        step = _tracker_step(path)
        assert "(2026-10-11)" in step, path.name
        assert re.search(r"ROLLBACK: delete the three lines", step), path.name


if __name__ == "__main__":
    import sys
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print("PASS", name)
            except AssertionError as e:
                fails += 1
                print("FAIL", name, e)
    print("RESULT %d FAIL" % fails)
    sys.exit(1 if fails else 0)
