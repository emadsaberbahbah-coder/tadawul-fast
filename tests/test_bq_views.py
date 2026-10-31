#!/usr/bin/env python3
# tests/test_bq_views.py
"""
Harness for scripts/apply_bq_views.py v1.2.0 and the SQL in sql/bigquery/
(C8 Phase 2: typed BigQuery views for Looker Studio; 2b: the global markets
page; 2c: the World Top-25 ROI, My Portfolio / Top-10 evidence and symbol
pages). Offline: the BigQuery session is a fake, no google library is
imported.

  V1  the 29 objects load in apply order (6 functions, then the views and
      table functions in file order) and the static checks pass
  V2  every header a view reads with f_cell exists in that tab's archived
      header row (fixture: the 2026-10-29 snapshot)
  V3  render: the placeholder is fully replaced; bad project / dataset ids
      are refused
  V4  the body each object keeps in BigQuery hashes to the definitions read
      back from INFORMATION_SCHEMA after the 29 Oct creation (installed pins);
      the 13 objects of v1.2.0 hash to their reviewed files (pending pins,
      confirmed by bq_views --verify after the first --write); a table
      function keeps the query after its 'AS' line
  V5  the static checks refuse a data-changing statement, a hard-coded
      project, a forward reference, two statements, a function after a view,
      a misnamed file, a CREATE TABLE and an unknown source table; they accept
      a table function after the view it reads
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
  V11 the market objects: v_market_daily reads only columns that
      scripts/archive_market_series.py SCHEMA declares, from that table only;
      the table functions take (start_date DATE, end_date DATE) and read only
      the objects before them
  V12 v_performance_log scores the CALL as track_performance's summary does
      (_directional_outcome on records loaded with a HOLD default): the
      8 tiers' signs read from its source, HOLD and aliases neutral,
      is_scored = decided, win_flag only for decided calls, one record per
      Key; fails on the first cut's price-direction flags
  V13 v_portfolio_decision.pd_ibkr_evidence recognizes every note
      pdIbkrNote_ (apps_script/10_My_Portfolio.gs) writes - SENT (...) too;
      fails on the first cut's pattern
  V14 every header a v1.2.0 view looks up is one its writer declares:
      v_universe on all four market pages (core/sheets/schema_registry.py),
      the news views on scripts/news_macro_gate.py's three headers,
      v_calendar_events on scripts/run_calendar_sync.py's; and the tables
      they read are exactly what archive_to_bigquery v1.1.0 adds (header row 1)
  V15 v_universe units and regions: a registry pct column (a fraction) is
      read x 100 or recomputed from prices, a points column never is; every
      listing suffix on the live pages (31 Oct) has exactly one region and a
      listing country; the market-series regions are reused by name
  V16 the new table functions: f_universe_return(start_date DATE, end_date
      DATE) and f_symbol_pick(q STRING, market STRING) read only earlier
      objects and document their Looker custom queries
  V17 the evidence views read typed views only (no raw archive table), the
      portfolio's bearish calls are v_performance_log's, f_col / f_at pair
      exactly as f_cell, and every caution flag has a plain-word label
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
# v_performance_log re-applied after the Codex review (STRONG SELL bearish);
# v_market_daily / f_market_window / f_market_summary created the same day
# (apply_bq_views 1.1.0) and read back the same way; v_performance_log and
# v_portfolio_decision re-applied after the Codex review of #879 (call-scored
# learning flags; the SENT evidence note) and read back again.
INSTALLED = {
    "f_cell": "25c3a65640088f2a000cd3c0a9f8c792a7129a546e5189f6a39ec08cf2315e7d",
    "f_num": "135ec9de4bf03c52499f4edf6e2a1d9caaf7cf6df1f1b6989a426968c663189a",
    "f_ts": "a03bb5075755336cde6ba070413984e3bd43cb842cb835f9eea64751f10c7fb6",
    "v_archive_health": "9932e59c56f917b3691ff63eae0b24581049d1a25d0bed91f9eecd71d37dcb72",
    "v_board_episodes": "76a6a240891eca669a574fd50b6d61b3d41439e502d9107aaefeb3c43d386048",
    "v_my_portfolio": "3fade0a8611cd65e9106643e753c454bb01c13a4c1743caa2a98403e7ef7c5dd",
    "v_performance_log": "c8f644b51d60100f062824d4a7edd19898fedd89005e122ecce8d515d02635dc",
    "v_portfolio_decision": "98b6090df20eb1244dc9dd6d0a0054ace85ed9036eedc4679fc86e85fd87b4ce",
    "v_run_log": "4b06dfa258072044b7a8a7fd162dc9b0407879e9238f5d6b9eada60a2e5a824c",
    "v_selection_log": "3a9400566a6bdbeab9087a8950ea18dbf22150b98efc80d7a27de9af7846a234",
    "v_signal_history": "b0f74c3f849376e969ea5637993f33fa24a06e799537b8f112aed35d04a43ff7",
    "v_top10_board": "dde854ba2022d754bb9398ca4405133ae8a9c7dedce618cca8746ee5aaa30144",
    "v_top10_status": "ec55e82d26bfeae7e043cdce6a66a6c0fa5fe86fa6f2668ad5a4758cc8b36945",
    "v_market_daily": "9c541c587ec6b06a2abd5c11542a60c6356fc7e98e7d5cdf77c99f6571e34a04",
    "f_market_window": "64f1a45c397a3f8f0c66fd1495ed54be4447cd26193aab56f21363bf8e428750",
    "f_market_summary": "06c5ec61c824be4019428bc36212230f94b0fea6a3394b63c8b007cd01cda808",
}
# The 13 objects apply_bq_views 1.2.0 adds (31 Oct 2026), hashed from the
# reviewed files: bq_views --verify after the first --write must find these
# exact bodies in INFORMATION_SCHEMA (then they move to INSTALLED).
PENDING = {
    "f_col": "bdc2111efe102498cb71de7287bbd54f04060720c72a12055ba169850c9d0e13",
    "f_at": "0778d85d64b36c5c40183dbd19b6ef16edeb539e246443bc083bdaca71671108",
    "f_ts_utc": "2a97c7875166cc6548e2d5443bc831405d56b9069ceb91bbee6ebadd120173be",
    "v_universe": "eeb532b232eb1269e5246139004ef9eb68feb622e91ef6948ff319d3529b140a",
    "v_universe_latest": "0705c098a94352de9e7a2ecdcdee1669be1834cc0d2c7bcc95a0431b00c76ebe",
    "f_universe_return": "f3a7153aa93cb74570c523ab549db903274ac9290c68be8a1c2d048ca0e7e09d",
    "v_news_items": "98d76e754284969f91c457d84c4ea59df01402bc3ce593053d0425e444aade47",
    "v_econ_events": "88f79df71ae3a0d2be5973b8d6e99af77d7d6cbed3c816443d3ad93d1a0842d6",
    "v_news_gate": "ba73984a770ff9e343fbc477ecae62cd5317dbea9d24880559ca33c3ad7a6c6f",
    "v_calendar_events": "982b2d62ea7254759c320d3af87220543ec01405b72c2e2bf99938c639f4e6f0",
    "v_top10_insight": "a1638f8d5f93167bb13b537205d4a0e362714952332e527f8871daf4df148755",
    "v_portfolio_insight": "6dda2f09c116d73584a1938385d90f68e07a335a99e7f5c2ea2f92baa1b16ccb",
    "f_symbol_pick": "32777aa976c7220b789a103de61a027892173c98c731cfb3a7dedc467affb877",
}
EXPECTED_ORDER = [
    "f_cell", "f_num", "f_ts", "f_col", "f_at", "f_ts_utc",
    "v_board_episodes", "v_performance_log", "v_selection_log", "v_signal_history",
    "v_top10_board", "v_portfolio_decision", "v_my_portfolio", "v_top10_status",
    "v_run_log", "v_archive_health",
    "v_market_daily", "f_market_window", "f_market_summary",
    "v_universe", "v_universe_latest", "f_universe_return",
    "v_news_items", "v_econ_events", "v_news_gate", "v_calendar_events",
    "v_top10_insight", "v_portfolio_insight", "f_symbol_pick",
]
EXPECTED_KINDS = (["FUNCTION"] * 6 + ["VIEW"] * 11 + ["TABLE FUNCTION"] * 2 + ["VIEW"] * 2
                  + ["TABLE FUNCTION"] + ["VIEW"] * 6 + ["TABLE FUNCTION"])
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
    assert kinds == EXPECTED_KINDS, kinds
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
    pins = {**INSTALLED, **PENDING}
    assert not set(INSTALLED) & set(PENDING)
    assert got == pins, {n: (got.get(n), pins.get(n)) for n in set(got) | set(pins)
                         if got.get(n) != pins.get(n)}
    view = "x\nCREATE OR REPLACE VIEW `p.d.v`\nOPTIONS (description = \"d\")\nAS\nSELECT 1 AS a\nFROM t;\n"
    assert bv.object_body(view, "VIEW") == "SELECT 1 AS a\nFROM t"
    fn = "CREATE OR REPLACE FUNCTION `p.d.f`(s STRING)\nRETURNS STRING\nAS (\n  TRIM(s)\n)\nOPTIONS (description = \"d\");\n"
    assert bv.object_body(fn, "FUNCTION") == "TRIM(s)"
    tvf = ("CREATE OR REPLACE TABLE FUNCTION `p.d.t`(a DATE, b DATE)\nOPTIONS (description = \"d\")\nAS\n"
           "SELECT a AS x\nFROM u;\n")
    assert bv.object_body(tvf, "TABLE FUNCTION") == "SELECT a AS x\nFROM u"


def _write(tmp: Path, files: dict) -> Path:
    for name, text in files.items():
        (tmp / name).write_text(text, encoding="utf-8")
    return tmp


GOOD_FN = ("CREATE OR REPLACE FUNCTION `{{DS}}.f_a`(s STRING)\nRETURNS STRING\nAS (\n  TRIM(s)\n)\n"
           "OPTIONS (description = \"a\");\n")
GOOD_VIEW = ("CREATE OR REPLACE VIEW `{{DS}}.v_a`\nOPTIONS (description = \"a\")\nAS\n"
             "SELECT `{{DS}}.f_a`(tab) AS t FROM `{{DS}}.run_log`;\n")


GOOD_TVF = ("CREATE OR REPLACE TABLE FUNCTION `{{DS}}.t_a`(start_date DATE, end_date DATE)\n"
            "OPTIONS (description = \"a\")\nAS\nSELECT t FROM `{{DS}}.v_a` WHERE start_date <= end_date;\n")


def case_v5():
    with tempfile.TemporaryDirectory() as d:
        ok = bv.check_objects(bv.load_objects(_write(Path(d), {"00_f_a.sql": GOOD_FN, "10_v_a.sql": GOOD_VIEW})))
        assert ok == [], ok
    with tempfile.TemporaryDirectory() as d:
        objs = bv.load_objects(_write(Path(d), {"00_f_a.sql": GOOD_FN, "10_v_a.sql": GOOD_VIEW,
                                                "20_t_a.sql": GOOD_TVF}))
        assert [o.kind for o in objs] == ["FUNCTION", "VIEW", "TABLE FUNCTION"]
        assert bv.check_objects(objs) == [], bv.check_objects(objs)
    with tempfile.TemporaryDirectory() as d:
        src = GOOD_VIEW.replace("`{{DS}}.run_log`", "`{{DS}}.market_series_daily`")
        assert bv.check_objects(bv.load_objects(_write(Path(d), {"00_f_a.sql": GOOD_FN, "10_v_a.sql": src}))) == []
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
        "forbidden statement 'CREATE OR REPLACE TABLE'": {
            "00_f_a.sql": GOOD_FN,
            "10_v_a.sql": GOOD_VIEW.replace("CREATE OR REPLACE VIEW `{{DS}}.v_a`",
                                            "CREATE OR REPLACE VIEW `{{DS}}.v_a`\n-- x\nCREATE OR REPLACE TABLE `{{DS}}.t` AS SELECT 1;")},
        "references nope_table before it is defined": {
            "00_f_a.sql": GOOD_FN, "10_v_a.sql": GOOD_VIEW.replace("`{{DS}}.run_log`", "`{{DS}}.nope_table`")},
        "functions must come before every view and table function": {
            "00_v_a.sql": GOOD_VIEW.replace("`{{DS}}.f_a`(tab)", "tab"), "10_t_a.sql": GOOD_TVF, "20_f_a.sql": GOOD_FN},
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
        assert o.kind in ("VIEW", "FUNCTION", "TABLE FUNCTION")
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


def case_v11():
    import re
    import archive_market_series as ms
    objs = {o.name: o for o in bv.load_objects()}
    schema = [f["name"] for f in ms.SCHEMA]
    daily = objs["v_market_daily"].template
    first = daily.split("WITH s AS (", 1)[1].split("LOGICAL_AND", 1)[0]
    cols = [c.strip() for c in first.replace("SELECT", "").split(",") if c.strip()]
    assert cols and set(cols) <= set(schema), sorted(set(cols) - set(schema))
    assert set(bv._REF.findall(bv._code(daily))) == {"market_series_daily", "v_market_daily"}
    assert ms.TABLE == "market_series_daily" and ms.TABLE in bv.SOURCE_TABLES
    order = [o.name for o in bv.load_objects()]
    for name in ("f_market_window", "f_market_summary"):
        tpl = objs[name].template
        assert objs[name].kind == "TABLE FUNCTION"
        assert re.search(r"`\{\{DS\}\}\." + name + r"`\(start_date DATE, end_date DATE\)", tpl), name
        refs = set(bv._REF.findall(bv._code(tpl))) - {name}
        assert refs and all(order.index(r) < order.index(name) for r in refs), (name, refs)
        assert "@DS_START_DATE" in tpl and "@DS_END_DATE" in tpl   # the Looker custom query, documented


def case_v12():
    import re
    src = (ROOT / "scripts" / "track_performance.py").read_text(encoding="utf-8")
    # the summary path loads Entry Recommendation with a HOLD default, so the
    # EXIT / TRIM aliases of _reco_trade_sign never reach its scoring
    assert "rec = _parse_enum_value(RecommendationType, rec_raw, RecommendationType.HOLD)" in src
    assert "return _directional_outcome(getattr(r, \"entry_recommendation\", None)," in src
    block = src.split("_RECO_TRADE_SIGN: Dict[str, int] = {", 1)[1].split("}", 1)[0]
    sign = {name.replace("_", ""): int(v) for name, v in
            re.findall(r"RecommendationType\.([A-Z_]+)\.value:\s*(-?\d+)", block)}
    assert sign["STRONGSELL"] == -1 and sign["HOLD"] == 0 and len(sign) == 8, sign
    tpl = [o for o in bv.load_objects() if o.name == "v_performance_log"][0].template
    code = bv._code(tpl)
    lists = {}
    for won, label in (("> 0", "long"), ("< 0", "short")):
        m = re.search(r"IN \(([^)]*)\)\s*THEN CASE WHEN realized_roi_pp " + re.escape(won) + r" THEN 'WIN'", code)
        assert m, label
        lists[label] = set(re.findall(r"'([^']+)'", m.group(1)))
    assert lists["long"] == {k for k, v in sign.items() if v == 1}, lists
    assert lists["short"] == {k for k, v in sign.items() if v == -1}, lists
    assert not ({"HOLD", "EXIT", "TRIM"} & (lists["long"] | lists["short"]))
    assert "WHEN IFNULL(status, '') != 'matured' OR realized_roi_pp IS NULL THEN NULL" in code
    assert "IFNULL(call_outcome IN ('WIN', 'LOSS'), FALSE) AS is_scored" in code
    assert "CASE WHEN call_outcome = 'WIN' THEN 1 WHEN call_outcome = 'LOSS' THEN 0 END AS win_flag" in code
    assert "PARTITION BY record_key, matured_valid" in code
    # golden negative: the first cut scored the sheet's PRICE label
    assert "outcome IN ('WIN', 'LOSS', 'BREAKEVEN') AS is_scored" not in code
    assert "WHEN outcome = 'WIN' THEN 1" not in code


def case_v13():
    import re
    gs = (ROOT / "apps_script" / "10_My_Portfolio.gs").read_text(encoding="utf-8")
    fn = gs.split("function pdIbkrNote_(", 1)[1].split("\nfunction ", 1)[0]
    notes = {
        "SENT": "' | ibkr evidence SENT ('",
        "NOT SENT": "' | ibkr evidence NOT SENT: '",
        "WOULD SEND (observe)": "' | [ibkr-observe] would send ('",
        "WOULD NOT SEND (observe)": "' | [ibkr-observe] would not send: '",
    }
    for literal in notes.values():
        assert literal in fn, literal
    tpl = [o for o in bv.load_objects() if o.name == "v_portfolio_decision"][0].template
    rules = re.findall(r"WHEN REGEXP_CONTAINS\(st\.status_text, r'([^']+)'\) THEN '([^']+)'", tpl)
    assert len(rules) == 4, rules

    def label(text):
        for pattern, value in rules:
            if re.search(pattern, text):
                return value
        return None

    head = "Portfolio Decision | status: ok | Last run 2026-10-29 10:00:00"
    for want, literal in notes.items():
        text = head + literal.strip("'") + "capture 2026-10-29 09:58 age 2m; linked 9/9; cash 1,234.00 SAR; USD 3.75)"
        assert label(text) == want, (want, label(text))
    assert label(head) is None
    # golden negative: the first cut's pattern never matched the SENT note
    sent = head + notes["SENT"].strip("'") + "capture 2026-10-29 09:58 age 2m)"
    assert re.search(r"ibkr evidence ([A-Z ]+?)[:|]", sent) is None


def _py_list(path, name):
    """A module-level list literal (NAME = [...]) read with ast - no import."""
    import ast
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == name for t in node.targets):
            return list(ast.literal_eval(node.value))
    raise AssertionError(f"{name} not in {path}")


MARKET_PAGES = ("Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX")
HELPERS = {"f_cell", "f_num", "f_ts", "f_col", "f_at", "f_ts_utc"}
NEW_SOURCES = {
    "v_universe": {"market_leaders", "global_markets", "mutual_funds", "commodities_fx"},
    "v_news_items": {"news_items"}, "v_econ_events": {"econ_events"},
    "v_news_gate": {"news_gate"}, "v_calendar_events": {"calendar_events"},
}
# listing suffixes on the four market pages of the live workbook, 31 Oct 2026
LIVE_SUFFIXES_20261031 = (
    "US", "T", "HK", "L", "TO", "AX", "DE", "NS", "PA", "KS", "ST", "TW", "IS", "BK", "JK", "SW", "MI",
    "MC", "MX", "AS", "OL", "VN", "SI", "JO", "CO", "KL", "SN", "AT", "BR", "WA", "HE", "QA", "NZ",
    "BA", "VI", "LS", "AE", "BH", "SR")


def _registry():
    sys.path.insert(0, str(ROOT))
    from core.sheets import schema_registry as reg
    return reg


def case_v14():
    import archive_to_bigquery as arc
    reg = _registry()
    objs = {o.name: o for o in bv.load_objects()}
    gate = "scripts/news_macro_gate.py"
    writers = {
        "v_news_items": _py_list(gate, "ITEMS_HEADER"),
        "v_news_gate": _py_list(gate, "GATE_HEADER"),
        "v_econ_events": _py_list(gate, "ECON_HEADER"),
        "v_calendar_events": (_py_list("scripts/run_calendar_sync.py", "HEADERS")
                              + _py_list("scripts/run_calendar_sync.py", "LAST_REPORT_HEADERS")),
    }
    pages = [reg.get_sheet_headers(p) for p in MARKET_PAGES]
    assert all(h == pages[0] for h in pages) and len(pages[0]) == 115
    used = bv.header_names_used(objs["v_universe"].template)
    assert len(used) == len(set(used)) == 115, len(used)        # every column, looked up once
    assert sorted(used) == sorted(pages[0])
    for name, header in writers.items():
        got = bv.header_names_used(objs[name].template)
        assert got and not set(got) - set(header), (name, sorted(set(got) - set(header)))
    # the tables they read: exactly the tabs archive v1.1.0 adds, header row 1
    added = {arc.table_name(t): h for t, h in arc.TABS[10:]}
    assert set(added) == {"market_leaders", "global_markets", "mutual_funds", "commodities_fx",
                          "news_items", "news_gate", "econ_events"} and set(added.values()) == {1}
    assert {arc.table_name(t) for t, _ in arc.TABS} == bv.ARCHIVE_TABLES
    for name, tables in NEW_SOURCES.items():
        refs = set(bv._REF.findall(bv._code(objs[name].template))) - {name}
        assert refs - HELPERS == tables, (name, refs)


def case_v15():
    import re
    import archive_market_series as ms
    reg = _registry()
    code = bv._code({o.name: o for o in bv.load_objects()}["v_universe"].template)
    alias_of = dict(re.findall(r"\.f_col`\(h, '([^']+)'\) AS i_([a-z0-9_]+)", code))
    times100 = set(re.findall(
        r"ROUND\(`\{\{DS\}\}\.f_num`\(`\{\{DS\}\}\.f_at`\(s\.cells, i_([a-z0-9_]+)\)\) \* 100, 4\)", code))
    dtype = {c.header: c.dtype for c in reg.get_sheet_columns("Global_Markets")}
    derived = {"Percent Change": "change_pct", "52W Position %": "position_52w_pct",
               "Expected ROI 1M": "expected_roi_1m_pct", "Expected ROI 3M": "expected_roi_3m_pct",
               "Expected ROI 12M": "expected_roi_12m_pct", "Upside %": "intrinsic_upside_pct",
               "Upside/Downside %": "target_upside_pct"}
    pct = {h for h, d in dtype.items() if d == "pct"}
    assert len(pct) == 17, sorted(pct)
    for h in pct:               # a fraction column is read x 100 or recomputed from prices
        assert alias_of[h] in times100 or h in derived, h
    for h, col in derived.items():
        assert f" AS {col}" in code, col
    for h, a in alias_of.items():
        if a in times100:       # golden negative: a 0-100 points column is never x 100
            assert dtype[h] == "pct", (h, dtype[h])
    assert alias_of["Data Quality Score"] == "data_quality" and "data_quality" not in times100
    region_of = {}
    for body, region in re.findall(r"WHEN k\.market_code IN \(([^)]*)\) THEN '([^']+)'", code):
        for sfx in re.findall(r"'([^']+)'", body):
            assert sfx not in region_of, sfx
            region_of[sfx] = region
    countries = dict(re.findall(r"WHEN '([A-Z]+)' THEN '([^']+)'", code))
    for sfx in LIVE_SUFFIXES_20261031:
        assert sfx in region_of and sfx in countries, sfx
    assert set(region_of) == set(countries)
    assert (region_of["SR"], region_of["US"], region_of["T"], region_of["HK"], region_of["L"]) == \
        ("Gulf", "US", "Japan", "China", "Europe")
    named = set(region_of.values()) | set(re.findall(r"THEN '([A-Za-z]+)'\n", code))
    assert set(ms.REGIONS) - {"World"} <= named, set(ms.REGIONS) - named   # same names as the market series


def case_v16():
    import re
    objs = {o.name: o for o in bv.load_objects()}
    order = [o.name for o in bv.load_objects()]
    for name, sig, docs in (
            ("f_universe_return", "(start_date DATE, end_date DATE)", ("@DS_START_DATE", "@DS_END_DATE")),
            ("f_symbol_pick", "(q STRING, market STRING)", ("(@q, @market)", "WHERE pick_order = 1)"))):
        tpl = objs[name].template
        assert objs[name].kind == "TABLE FUNCTION"
        assert f"`{{{{DS}}}}.{name}`{sig}" in tpl, name
        refs = set(bv._REF.findall(bv._code(tpl))) - {name}
        assert refs and all(order.index(r) < order.index(name) for r in refs), (name, refs)
        for doc in docs:
            assert doc in tpl, (name, doc)
    pick = bv._code(objs["f_symbol_pick"].template)
    assert [int(x) for x in re.findall(r"THEN (\d)\n", pick)] == [1, 2, 3, 4, 5, 6, 7]
    assert re.search(r"QUALIFY pick_order <= 20;?$", pick.strip()) and "a.um IN ('', 'ALL', 'ANY')" in pick
    ret = bv._code(objs["f_universe_return"].template)
    assert "snapshot_date < start_date" in ret and "snapshot_date <= end_date" in ret
    assert "before_start IS NOT NULL AS covers_window" in ret


def case_v17():
    import re
    objs = {o.name: o for o in bv.load_objects()}
    for name in ("v_universe_latest", "f_universe_return", "v_top10_insight", "v_portfolio_insight",
                 "f_symbol_pick"):
        refs = set(bv._REF.findall(bv._code(objs[name].template))) - {name}
        assert refs and not refs & bv.SOURCE_TABLES, (name, refs & bv.SOURCE_TABLES)   # typed views only
    perf = bv._code(objs["v_performance_log"].template)
    bearish = set(re.findall(r"'([^']+)'", re.search(
        r"WHEN entry_recommendation IN \(([^)]*)\) THEN 'bearish'", perf).group(1)))
    pf = bv._code(objs["v_portfolio_insight"].template)
    said = set(re.findall(r"'([^']+)'", re.search(r"IF\(UPPER\(u\.recommendation\) IN \(([^)]*)\)", pf).group(1)))
    assert said == bearish, (said, bearish)
    cell, col, at = (bv._code(objs[n].template) for n in ("f_cell", "f_col", "f_at"))
    for frag in ("FROM UNNEST(h) AS x WITH OFFSET AS o", "WHERE TRIM(x) = name", "ORDER BY o", "LIMIT 1"):
        assert frag in cell and frag in col, frag
    assert "NULLIF(NULLIF(TRIM(c[SAFE_OFFSET(o)]), ''), '—')" in cell
    assert "NULLIF(NULLIF(TRIM(c[SAFE_OFFSET(i)]), ''), '—')" in at
    for name in ("v_top10_insight", "v_portfolio_insight"):
        code = bv._code(objs[name].template)
        labels = re.findall(r", '([^']+)', NULL\)", code)
        assert len(labels) >= 8 and all(re.fullmatch(r"[a-z0-9 /.-]+", x) for x in labels), labels
        assert "ARRAY_TO_STRING(j.flags, ' · ') AS caution_flags" in code
        assert "ARRAY_LENGTH(j.flags) AS caution_count" in code


CASES = [case_v1, case_v2, case_v3, case_v4, case_v5, case_v6, case_v7, case_v8, case_v9, case_v10, case_v11,
         case_v12, case_v13, case_v14, case_v15, case_v16, case_v17]


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
def test_v11(): case_v11()
def test_v12(): case_v12()
def test_v13(): case_v13()
def test_v14(): case_v14()
def test_v15(): case_v15()
def test_v16(): case_v16()
def test_v17(): case_v17()


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
