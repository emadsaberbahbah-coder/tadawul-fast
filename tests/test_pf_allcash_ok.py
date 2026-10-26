"""10_My_Portfolio v1.5.15 [PD-ALLCASH], checked against the real final audit.

Runs tests/js/pf_allcash_ok.test.js (the REAL Apps Script source in a vm on the
synthetic 24 Oct shape: one row outside the execution scope, an IBKR capture
with 0 positions, the backend answering 'empty') and feeds every status line it
produced to scripts/audit_decision_surface_freshness:
- enforce (default) + the broker-proven all-cash book -> status OK, exactly
  the My_Portfolio run consumed, and the strict final audit passes;
- the v1.5.14 line (golden, and the PD_ALLCASH_OK=off line) -> PF_STATUS_NOT_OK
  and PF_SOURCE_CONSUMPTION_UNPROVEN, the 23/24 Oct failure;
- every fail-closed variant -> the v1.5.14 verdict.
It also lints the added lines (ES5, ASCII) and the function inventory against
the v1.5.14 base. The whole-suite CI job collects this file, so the native
harness runs there too.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from scripts.audit_decision_surface_freshness import (
    _consumed_source_run, audit_surfaces, parse_surface_status)
from tests.test_decision_surface_freshness import FLOORS, publication_grids

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "js" / "pf_allcash_ok.test.js"
GAS = ROOT / "apps_script" / "10_My_Portfolio.gs"
GOLDEN = ROOT / "tests" / "fixtures" / "pf_allcash_v1_5_14_golden.json"
RUN = "6363636363"
TOKEN = "book all-cash (broker 0 positions, decisions 0)"
NOW = dt.datetime(2026, 10, 24, 14, 50, tzinfo=dt.timezone.utc)   # 17:50 Riyadh
GATE = {"PF_STATUS_NOT_OK", "PF_SOURCE_CONSUMPTION_UNPROVEN"}


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def _run_harness(tmp, extra_env=None):
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    out = tmp / "dump.json"
    env = dict(os.environ, PF_DUMP=str(out), PF_GAS=str(GAS), **(extra_env or {}))
    env.pop("PF_WRITE_GOLDEN", None)
    proc = subprocess.run([node, str(HARNESS)], env=env, capture_output=True, text=True,
                          timeout=180, cwd=str(tmp))
    assert proc.returncode == 0, (proc.stdout[-2000:], proc.stderr[-2000:])
    match = re.search(r"PASS 7/7 digest ([0-9a-f]{16})", proc.stdout)
    assert match, proc.stdout[-2000:]
    return json.loads(out.read_text(encoding="utf-8")), match.group(1)


@pytest.fixture(scope="module")
def golden():
    return json.loads(GOLDEN.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def base_source(tmp_path_factory, golden):
    """The v1.5.14 text, when git can supply it; its sha256 is pinned by the golden."""
    for ref in (os.environ.get("TFB_PF_BASE_REF", ""), "origin/main", "main"):
        if not ref:
            continue
        try:
            proc = subprocess.run(["git", "show", f"{ref}:apps_script/10_My_Portfolio.gs"],
                                  cwd=str(ROOT), capture_output=True, text=True, timeout=60)
        except Exception:
            continue
        if proc.returncode == 0 and hashlib.sha256(
                proc.stdout.encode("utf-8")).hexdigest() == golden["source_sha256"]:
            path = tmp_path_factory.mktemp("pf_base") / "10_My_Portfolio_v1_5_14.gs"
            path.write_text(proc.stdout, encoding="utf-8")
            return path
    pytest.skip("the v1.5.14 base (golden sha256) is not reachable through git here")


@pytest.fixture(scope="module")
def dump(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("pf_allcash")
    data, digest = _run_harness(tmp)
    data["_digest"] = digest
    return data


def _grids(line):
    sources, portfolio, top10 = publication_grids(run=RUN)
    for row in sources[1:]:
        row[1] = "2026-10-24 17:40:00"
    top10[1][1] = top10[1][1].replace("Last run 2026-07-31 20:10:00", "Last run 2026-10-24 17:45:00")
    portfolio[1][1] = line
    return sources, portfolio, top10


def _strict(line):
    return audit_surfaces(*_grids(line), now_utc=NOW, min_rows=FLOORS, expected_source_run=RUN)


def test_enforce_line_is_ok_and_consumed_by_the_real_audit_parsers(dump):
    line = dump["allcash_enforce"]
    assert " | status: ok | " + TOKEN + " | " in line
    stamp, status = parse_surface_status(line)
    assert stamp == dt.datetime(2026, 10, 24, 17, 43, 37) and status == "OK"
    assert _consumed_source_run(line) == RUN


def test_enforce_line_passes_the_strict_final_audit(dump):
    report = _strict(dump["allcash_enforce"])
    assert report.exit_code == 0 and report.executable, [f.code for f in report.findings]
    assert report.findings == []
    # the token is visible to a reader and inert to the parsers
    assert "book all-cash" in dump["allcash_enforce"]
    assert _consumed_source_run(dump["allcash_enforce"].replace(" | " + TOKEN, "")) == RUN


def test_v1_5_14_line_and_the_kill_switch_fail_the_gate_the_24_oct_way(dump, golden):
    base_line = golden["scenarios"]["allcash"]["b2"]
    assert dump["allcash_off"] == base_line
    for label, line in (("golden", base_line), ("off", dump["allcash_off"])):
        stamp, status = parse_surface_status(line)
        assert stamp is not None and status == "EMPTY", label
        assert _consumed_source_run(line) is None, label
        assert "source readiness WITHHELD: decision status empty" in line, label
        codes = {f.code for f in _strict(line).findings}
        assert GATE <= codes, (label, codes)


def test_every_fail_closed_variant_keeps_the_v1_5_14_verdict(dump, golden):
    closed = dump["closed"]
    assert len(closed) >= 9
    for label, line in closed.items():
        assert line == golden["scenarios"][label]["b2"], label
        assert TOKEN not in line, label
        codes = {f.code for f in _strict(line).findings}
        if label == "status ok with 0 rows":
            # v1.5.14 already publishes this one; nothing changes
            assert parse_surface_status(line)[1] == "OK" and _consumed_source_run(line) == RUN
            assert not (GATE & codes), codes
        else:
            assert _consumed_source_run(line) is None, label
            assert GATE <= codes, (label, codes)


def test_without_the_receipt_the_status_token_alone_is_not_consumption(dump):
    line = dump["allcash_no_receipt"]
    assert parse_surface_status(line)[1] == "OK"
    assert _consumed_source_run(line) is None
    assert "PF_SOURCE_CONSUMPTION_UNPROVEN" in {f.code for f in _strict(line).findings}


def test_harness_is_deterministic_across_three_runs(dump, tmp_path):
    digests = {dump["_digest"]}
    for i in range(2):
        sub = tmp_path / f"r{i}"
        sub.mkdir()
        _data, digest = _run_harness(sub)
        digests.add(digest)
    assert len(digests) == 1, digests


def test_live_base_matches_the_golden_and_fails_as_v1_5_14(base_source, tmp_path):
    _run_harness(tmp_path, {"PF_BASE_GAS": str(base_source)})
    node = node_binary()
    env = dict(os.environ, PF_GAS=str(base_source))
    env.pop("PF_WRITE_GOLDEN", None)
    env.pop("PF_DUMP", None)
    proc = subprocess.run([node, str(HARNESS)], env=env, capture_output=True, text=True,
                          timeout=180, cwd=str(tmp_path))
    assert proc.returncode != 0, "the harness must fail on v1.5.14 (golden negative)"
    assert "PASS T1 " in proc.stdout and "PASS T2 " not in proc.stdout, proc.stdout[-1500:]


ES6_RE = re.compile(r"(^|[^A-Za-z0-9_$.'\"])(let|const|class|import|export|yield|async|await)\s|=>|`|\.\.\.")


def _top_level_functions(text):
    return sorted(re.findall(r"^function\s+([A-Za-z0-9_$]+)\s*\(", text, re.M))


def test_version_bumped_in_lockstep_and_nothing_removed(golden):
    text = GAS.read_text(encoding="utf-8")
    # 2026-10-26: the delivered file is v1.5.16 (evidence publication stacked on this change); pin moved
    assert " * Version: 1.5.16\n" in text and "var PD_VERSION = '1.5.16';" in text
    assert "var PD_VERSION = '1.5.14';" not in text and "var PD_VERSION = '1.5.15';" not in text
    assert "\r" not in text
    now = set(_top_level_functions(text))
    base = set(golden["functions"])
    assert len(base) == 118
    assert base <= now, sorted(base - now)
    # 2026-10-26: v1.5.16 (evidence publication) adds eight pdIbkr* functions on top of this change
    assert sorted(now - base) == ["pdAllCashMode_", "pdAllCashProof_", "pdIbkrArmFollowUp_", "pdIbkrEvidenceFollowUp",
                                  "pdIbkrFollowUpCore_", "pdIbkrFollowUpState_", "pdIbkrPublishMode_", "pdIbkrPublish_",
                                  "pdIbkrRemoveFollowUp_", "pdIbkrRunLog_"]
    non_ascii = sum(1 for line in text.split("\n") if re.search(r"[^\x00-\x7f]", line))
    assert non_ascii == golden["non_ascii_lines"] == 97
    assert len(text.split("\n")) - golden["source_lines"] == 92 + 224


def test_added_lines_are_es5_and_ascii(base_source):
    import difflib
    base = base_source.read_text(encoding="utf-8").split("\n")
    now = GAS.read_text(encoding="utf-8").split("\n")
    added = [line[1:] for line in difflib.unified_diff(base, now, n=0, lineterm="")
             if line.startswith("+") and not line.startswith("+++")]
    removed = [line[1:] for line in difflib.unified_diff(base, now, n=0, lineterm="")
               if line.startswith("-") and not line.startswith("---")]
    assert 95 <= len(added) <= 110 and len(removed) <= 10, (len(added), len(removed))
    code = [line for line in added if not re.match(r"\s*(\*|/\*|//)", line)]
    assert len(code) >= 40, len(code)
    for line in added:
        assert all(ord(ch) < 128 for ch in line), repr(line)
    for line in code:
        assert not ES6_RE.search(line), repr(line)
    # nothing but the known anchors left the file
    for line in removed:
        assert any(key in line for key in (
            "Version: 1.5.14", "PD_VERSION = '1.5.14'",
            "function pdReceiptNote_(mode, ss, ctx, consumedRows, payload, binding) {",
            "if (src.ready && st !== 'ok') {",
            "var res = {note: '', excluded: [], controls: null, sent: false};",
            "Returns {note, excluded, controls, sent}. Never throws. */",
            "var receiptNote = pdReceiptNote_(receiptMode, ss, sourceCtx,",
            "pool.rows.length, payload, binding);",
            "var statusLine = poolNote + receiptNote + (ibkr ? ibkr.note : '') + ' | ' +",
            "statusCell.setValue(pdStatusLine_(String(payload.status || '?'),")), repr(line)
