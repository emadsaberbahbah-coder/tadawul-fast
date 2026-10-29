#!/usr/bin/env python3
# tests/test_bq_views.py
"""
Harness for scripts/apply_bq_views.py v1.0.0 and the SQL in sql/bigquery/
(C8 Phase 2: typed BigQuery views for Looker Studio). Offline: the BigQuery
session is a fake, no google library is imported.

  V1  the 13 objects load in apply order (3 functions, then 10 views) and the
      static checks pass
  V2  every header a view reads with f_cell exists in that tab's archived
      header row (fixture: the 2026-10-29 snapshot)
  V3  render: the placeholder is fully replaced; bad project / dataset ids
      are refused
  V4  the body each object keeps in BigQuery hashes to the definitions read
      back from INFORMATION_SCHEMA after the 29 Oct creation (installed pins)
  V5  the static checks refuse a data-changing statement, a hard-coded
      project, a forward reference, two statements, a function after a view
      and a misnamed file
  V6  main(): dry run creates nothing; --write without credentials exits 2;
      --write applies every file in order then verifies; --verify reports a
      changed or missing object with exit 1
  V7  bq_views.yml is dispatch-only, read-only for the repository, defaults to
      verify, and runs the script in the chosen mode
  V8  nothing here can write to the workbook: no Sheets API in the script or
      the SQL, and every SQL object is a view or a function
  V9  v_performance_log.entry_stance mirrors track_performance's
      _reco_trade_sign (read from its source): STRONG SELL is bearish
  V10 dry run: offline without a key; with a key every statement is sent as
      a BigQuery dry run in order; a broken key exits 2
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import apply_bq_views as bv  # noqa: E402

FIXTURE = ROOT / "tests" / "fixtures" / "bq_archive_headers_2026-10-29.json"
WORKFLOW = ROOT / ".github" / "workflows" / "bq_views.yml"
_KEY_ENV = ("GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS")
PROJECT = bv.DEFAULT_PROJECT
DATASET = bv.DEFAULT_DATASET

# Read back on 2026-10-29 from INFORMATION_SCHEMA.VIEWS / ROUTINES of
# project-2e3530e6-2311-4aa3-b36.tfb_archive right after the objects were
# created: TO_HEX(SHA256(view_definition | routine_definition));
# v_performance_log re-applied after the Codex review (STRONG SELL bearish).
INSTALLED = {
    "f_cell": "25c3a65640088f2a000cd3c0a9f8c792a7129a546e5189f6a39ec08cf2315e7d",
    "f_num": "135ec9de4bf03c52499f4edf6e2a1d9caaf7cf6df1f1b6989a426968c663189a",
    "f_ts": "a03bb5075755336cde6ba070413984e3bd43cb842cb835f9eea64751f10c7fb6",
    "v_archive_health": "9932e59c56f917b3691ff63eae0b24581049d1a25d0bed91f9eecd71d37dcb72",
    "v_board_episodes": "76a6a240891eca669a574fd50b6d61b3d41439e502d9107aaefeb3c43d386048",
    "v_my_portfolio": "3fade0a8611cd65e9106643e753c454bb01c13a4c1743caa2a98403e7ef7c5dd",
    "v_performance_log": "164916ee9201f26f865e84b846ca9770151c9bf0e206bbe7b5efacf7102f8d98",
    "v_portfolio_decision": "a44c112595437f6b0a7a3ece94b142dd34fbb6d20459139a031c73fd43f3bfb7",
    "v_run_log": "4b06dfa258072044b7a8a7fd162dc9b0407879e9238f5d6b9eada60a2e5a824c",
    "v_selection_log": "3a9400566a6bdbeab9087a8950ea18dbf22150b98efc80d7a27de9af7846a234",
    "v_signal_history": "b0f74c3f849376e969ea5637993f33fa24a06e799537b8f112aed35d04a43ff7",
    "v_top10_board": "dde854ba2022d754bb9398ca4405133ae8a9c7dedce618cca8746ee5aaa30144",
    "v_top10_status": "ec55e82d26bfeae7e043cdce6a66a6c0fa5fe86fa6f2668ad5a4758cc8b36945",
}
EXPECTED_ORDER = [
    "f_cell", "f_num", "f_ts",
    "v_board_episodes", "v_performance_log", "v_selection_log", "v_signal_history",
    "v_top10_board", "v_portfolio_decision", "v_my_portfolio", "v_top10_status",
    "v_run_log", "v_archive_health",
]
# view -> the archive table whose header row its f_cell calls read
VIEW_SOURCE = {
    "v_board_episodes": "board_scorecard",
    "v_performance_log": "performance_log",
    "v_selection_log": "selection_log",
    "v_signal_history": "signal_history",
    "v_top10_board": "top_10_investments",
    "v_portfolio_decision": "portfolio_decision",
    "v_my_portfolio": "my_portfolio",
    "v_run_log": "run_log",
}


class FakeResp:
    def __init__(self, status: int, body: dict):
        self.status_code = status
        self._body = body

    def json(self):
        return self._body


class FakeSession:
    """Records every statement; answers the INFORMATION_SCHEMA read with the
    hashes of `live` (name -> body text) or of what --write sent."""

    def __init__(self, live=None, fail_on=None):
        self.sent = []
        self.live = dict(live or {})
        self.fail_on = fail_on

    def post(self, url, json=None, timeout=None):  # noqa: A002 - requests API
        q = (json or {}).get("query", "")
        if "INFORMATION_SCHEMA" in q:
            rows = [{"f": [{"v": n}, {"v": bv.sha256(b)}]} for n, b in self.live.items()]
            return FakeResp(200, {"rows": rows})
        self.sent.append((q, bool((json or {}).get("dryRun"))))
        if self.fail_on and self.fail_on in q:
            return FakeResp(400, {"error": {"message": "boom"}})
        for o in bv.load_objects():
            if o.render(PROJECT, DATASET) == q:
                self.live[o.name] = o.body(PROJECT, DATASET)
        return FakeResp(200, {"jobComplete": True})


def _bodies():
    return {o.name: o.body(PROJECT, DATASET) for o in bv.load_objects()}


def case_v1():
    objs = bv.load_objects()
    assert [o.name for o in objs] == EXPECTED_ORDER, [o.name for o in objs]
    kinds = [o.kind for o in objs]
    assert kinds == ["FUNCTION"] * 3 + ["VIEW"] * 10, kinds
    assert bv.check_objects(objs) == [], bv.check_objects(objs)


def case_v2():
    headers = json.loads(FIXTURE.read_text(encoding="utf-8"))
    checked = 0
    for o in bv.load_objects():
        if o.name not in VIEW_SOURCE:
            continue
        cols = set(headers[VIEW_SOURCE[o.name]]["cells"])
        used = bv.header_names_used(o.template)
        assert used, f"{o.name}: no f_cell call found"
        missing = sorted(set(used) - cols)
        assert not missing, f"{o.name}: headers not in {VIEW_SOURCE[o.name]}: {missing}"
        checked += len(set(used))
    assert checked >= 150, checked


def case_v3():
    for o in bv.load_objects():
        r = o.render(PROJECT, DATASET)
        assert bv.PLACEHOLDER not in r and "{{" not in r, o.name
        assert f"`{PROJECT}.{DATASET}." in r, o.name
    for bad_project, bad_dataset in (("Bad Project", DATASET), (PROJECT, "a-b"),
                                     ("", DATASET), (PROJECT, "x`; DROP")):
        try:
            bv.render("`{{DS}}.x`", bad_project, bad_dataset)
        except ValueError:
            continue
        raise AssertionError(f"accepted {bad_project!r} / {bad_dataset!r}")


def case_v4():
    got = {n: bv.sha256(b) for n, b in _bodies().items()}
    assert got == INSTALLED, {n: (got.get(n), INSTALLED.get(n)) for n in set(got) | set(INSTALLED)
                              if got.get(n) != INSTALLED.get(n)}
    view = "x\nCREATE OR REPLACE VIEW `p.d.v`\nOPTIONS (description = \"d\")\nAS\nSELECT 1 AS a\nFROM t;\n"
    assert bv.object_body(view, "VIEW") == "SELECT 1 AS a\nFROM t"
    fn = "CREATE OR REPLACE FUNCTION `p.d.f`(s STRING)\nRETURNS STRING\nAS (\n  TRIM(s)\n)\nOPTIONS (description = \"d\");\n"
    assert bv.object_body(fn, "FUNCTION") == "TRIM(s)"


def _write(tmp: Path, files: dict) -> Path:
    for name, text in files.items():
        (tmp / name).write_text(text, encoding="utf-8")
    return tmp


GOOD_FN = ("CREATE OR REPLACE FUNCTION `{{DS}}.f_a`(s STRING)\nRETURNS STRING\nAS (\n  TRIM(s)\n)\n"
           "OPTIONS (description = \"a\");\n")
GOOD_VIEW = ("CREATE OR REPLACE VIEW `{{DS}}.v_a`\nOPTIONS (description = \"a\")\nAS\n"
             "SELECT `{{DS}}.f_a`(tab) AS t FROM `{{DS}}.run_log`;\n")


def case_v5():
    with tempfile.TemporaryDirectory() as d:
        ok = bv.check_objects(bv.load_objects(_write(Path(d), {"00_f_a.sql": GOOD_FN, "10_v_a.sql": GOOD_VIEW})))
        assert ok == [], ok
    bad_sets = {
        "forbidden": {"00_f_a.sql": GOOD_FN,
                      "10_v_a.sql": GOOD_VIEW.replace("SELECT", "DELETE FROM `{{DS}}.run_log` WHERE TRUE; SELECT")},
        "hard-coded": {"00_f_a.sql": GOOD_FN,
                       "10_v_a.sql": GOOD_VIEW.replace("`{{DS}}.run_log`", "`project-2e3530e6-2311-4aa3-b36.tfb_archive.run_log`")},
        "before it is defined": {"00_f_a.sql": GOOD_FN,
                                 "10_v_a.sql": GOOD_VIEW.replace("`{{DS}}.run_log`", "`{{DS}}.v_later`")},
        "exactly one statement": {"00_f_a.sql": GOOD_FN, "10_v_a.sql": GOOD_VIEW + "SELECT 2;\n"},
        "functions must come before": {"00_v_a.sql": GOOD_VIEW.replace("`{{DS}}.f_a`(tab)", "tab"),
                                       "10_f_a.sql": GOOD_FN},
        "file name says": {"00_f_a.sql": GOOD_FN, "10_v_b.sql": GOOD_VIEW},
    }
    for needle, files in bad_sets.items():
        with tempfile.TemporaryDirectory() as d:
            problems = bv.check_objects(bv.load_objects(_write(Path(d), files)))
            assert any(needle in p for p in problems), (needle, problems)


def _no_key_env():
    saved = {k: os.environ.pop(k) for k in _KEY_ENV if k in os.environ}
    return saved


def _restore_env(saved):
    for k in _KEY_ENV:
        os.environ.pop(k, None)
    os.environ.update(saved)


def case_v6():
    import io
    import contextlib

    saved = _no_key_env()
    try:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(io.StringIO()):
            rc = bv.main([])
    finally:
        _restore_env(saved)
    assert rc == 0 and "nothing created" in buf.getvalue(), (rc, buf.getvalue())
    assert "static checks only" in buf.getvalue(), buf.getvalue()

    real = bv.make_session
    try:
        def no_creds():
            raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS missing or unreadable")
        bv.make_session = no_creds
        with contextlib.redirect_stderr(io.StringIO()):
            assert bv.main(["--write"]) == 2
            assert bv.main(["--verify"]) == 2
    finally:
        bv.make_session = real

    s = FakeSession()
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        rc = bv.main(["--write"], session=s)
    assert rc == 0, rc
    names_sent = []
    for q, dry in s.sent:
        assert not dry
        for o in bv.load_objects():
            if o.render(PROJECT, DATASET) == q:
                names_sent.append(o.name)
    assert names_sent == EXPECTED_ORDER, names_sent

    s = FakeSession(fail_on="v_selection_log")
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        assert bv.main(["--write"], session=s) == 1
    assert len(s.sent) == EXPECTED_ORDER.index("v_selection_log") + 1

    bodies = _bodies()
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        assert bv.main(["--verify"], session=FakeSession(live=bodies)) == 0
    changed = dict(bodies, v_run_log=bodies["v_run_log"] + " ")
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
        assert bv.main(["--verify"], session=FakeSession(live=changed)) == 1
    assert "mismatch: v_run_log" in out.getvalue(), out.getvalue()
    missing = {k: v for k, v in bodies.items() if k != "f_ts"}
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
        assert bv.main(["--verify"], session=FakeSession(live=missing)) == 1
    assert "f_ts" in out.getvalue()


def case_v7():
    wf = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    on = wf.get(True) or wf.get("on")
    assert set(on.keys()) == {"workflow_dispatch"}, on
    mode = on["workflow_dispatch"]["inputs"]["mode"]
    assert mode["default"] == "verify" and mode["options"] == ["verify", "write"], mode
    assert wf["permissions"] == {"contents": "read"}, wf["permissions"]
    job = wf["jobs"]["views"]
    assert int(job["timeout-minutes"]) <= 20
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "scripts/apply_bq_views.py" in text
    assert '--${MODE}' in text or "--$MODE" in text
    assert "actions/checkout@v6" in text and "actions/setup-python@v6" in text
    assert "schedule" not in text.split("on:", 1)[1].split("concurrency:", 1)[0]


def case_v8():
    script = (ROOT / "scripts" / "apply_bq_views.py").read_text(encoding="utf-8")
    assert "sheets.googleapis" not in script and "spreadsheets" not in script.replace("GOOGLE_SHEETS_CREDENTIALS", "")
    for o in bv.load_objects():
        assert o.kind in ("VIEW", "FUNCTION")
        assert "sheets.googleapis" not in o.template


def _stance_lists(template):
    import re
    out = {}
    for label in ("bullish", "bearish"):
        m = re.search(r"WHEN entry_recommendation IN \(([^)]*)\) THEN '" + label + "'", template)
        assert m, label
        out[label] = set(re.findall(r"'([^']+)'", m.group(1)))
    m = re.search(r"WHEN entry_recommendation = '([^']+)' THEN 'neutral'", template)
    out["neutral"] = {m.group(1)} if m else set()
    return out


def case_v9():
    import re
    src = (ROOT / "scripts" / "track_performance.py").read_text(encoding="utf-8")
    block = src.split("_RECO_TRADE_SIGN: Dict[str, int] = {", 1)[1].split("}", 1)[0]
    sign = {name.replace("_", " "): int(v) for name, v in
            re.findall(r"RecommendationType\.([A-Z_]+)\.value:\s*(-?\d+)", block)}
    aliases = src.split("_RECO_TRADE_SIGN_ALIASES: Dict[str, int] = {", 1)[1].split("}", 1)[0]
    sign.update({k: int(v) for k, v in re.findall(r'"([A-Z ]+)":\s*(-?\d+)', aliases)})
    assert sign.get("STRONG SELL") == -1 and sign.get("HOLD") == 0, sign
    tpl = [o for o in bv.load_objects() if o.name == "v_performance_log"][0].template
    lists = _stance_lists(tpl)
    assert lists["bullish"] == {k for k, v in sign.items() if v == 1}, (lists, sign)
    assert lists["bearish"] == {k for k, v in sign.items() if v == -1}, (lists, sign)
    assert lists["neutral"] == {k for k, v in sign.items() if v == 0}, (lists, sign)


def case_v10():
    import io
    import contextlib

    saved = _no_key_env()
    real = bv.make_session
    try:
        os.environ["GOOGLE_SHEETS_CREDENTIALS"] = "{}"
        s = FakeSession()
        bv.make_session = lambda: s
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(io.StringIO()):
            assert bv.main(["--dry-run"]) == 0
        assert "BigQuery dry-run" in buf.getvalue(), buf.getvalue()
        assert [d for _, d in s.sent] == [True] * len(EXPECTED_ORDER), s.sent
        names = [o.name for q, _ in s.sent for o in bv.load_objects() if o.render(PROJECT, DATASET) == q]
        assert names == EXPECTED_ORDER, names

        def broken():
            raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS missing or unreadable")
        bv.make_session = broken
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            assert bv.main([]) == 2
    finally:
        bv.make_session = real
        _restore_env(saved)


CASES = [case_v1, case_v2, case_v3, case_v4, case_v5, case_v6, case_v7, case_v8, case_v9, case_v10]


def test_v1(): case_v1()
def test_v2(): case_v2()
def test_v3(): case_v3()
def test_v4(): case_v4()
def test_v5(): case_v5()
def test_v6(): case_v6()
def test_v7(): case_v7()
def test_v8(): case_v8()
def test_v9(): case_v9()
def test_v10(): case_v10()


if __name__ == "__main__":
    passed = 0
    for fn in CASES:
        try:
            fn()
            passed += 1
            print(f"PASS {fn.__name__}")
        except AssertionError as exc:
            print(f"FAIL {fn.__name__}: {exc}")
    print(f"{'PASS' if passed == len(CASES) else 'FAIL'} {passed}/{len(CASES)}")
    sys.exit(0 if passed == len(CASES) else 1)
