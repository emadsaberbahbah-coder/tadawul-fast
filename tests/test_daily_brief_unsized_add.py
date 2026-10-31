#!/usr/bin/env python3
"""tests/test_daily_brief_unsized_add.py - run_daily_brief v1.21.0 [UNSIZED-ADD]
(gate TFB_BRIEF_UNSIZED_ADD = off | observe | enforce, default off = v1.20.0).

WHY (2026-10-31): extract_decision read an ADD's amount with _note_sar, the
first "<n> SAR" anywhere in the Advisor Note. An unsized ADD ("ADD (unsized -
no deployable capital); ...; stop S / TP1 T1 / TP2 T2 SAR; ...") has no
amount, so its TP2 or stop LEVEL was read as cash: it folded into another
ADD ticket, became a phantom ticket at 200 SAR or more, or was dropped with
the held line missing from the holdings count while its MV and cost stayed
in the book totals. The v1.8.0 floor also removed folded or under-floor
sized ADD rows from the holdings.

Every note below is written by the REAL portfolio_actions._advisor_sentence
(or, for the sukuk ladder, with the real SUKUK_LADDER_NOTE), so the note
contract the brief parses is pinned. Offline: no network, no credentials.

Cases
WORKS   W1-W7   enforce: a sized ticket keeps its own amount; a sukuk with an
                unsized ADD stays in the count and tables; no phantom ticket;
                under-floor and folded rows stay held; the "no TP ladder"
                note; observe prints one stderr line per affected row.
NEVER   N1-N6   off (11 spellings) == v1.20.0 on every fixture (decision dict,
                HTML, text), checked against OFF_GOLDEN digests everywhere and
                against the v1.20.0 base when available; observe == off;
                sized tickets at or above the floor unchanged; the floor
                disabled; never mutates its input; the parser table.
GOLD    G1      the WORKS checks fail on the untouched v1.20.0 base (main
                05b51ebe) with the gate set - the golden negative.
V       V1      version floor, verifier pin, workflow mapping, CI lane,
                ASCII added lines.

Base copy: TFB_TEST_BRIEF_BASE=<path>, else `git show
05b51ebe:scripts/run_daily_brief.py`; without either the base cases SKIP.
Golden negative of the whole file: TFB_TEST_BRIEF_DELIVERED=<base copy> runs
every case against v1.20.0 (the WORKS cases and V1 fail).

Run: pytest -q tests/test_daily_brief_unsized_add.py
  or python3 tests/test_daily_brief_unsized_add.py   (prints PASS k/k)
"""
from __future__ import annotations

import copy
import datetime as dt
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.analysis import portfolio_actions as pa  # noqa: E402

SCRIPT = ROOT / "scripts" / "run_daily_brief.py"
DELIVERED = Path(os.getenv("TFB_TEST_BRIEF_DELIVERED", "").strip() or SCRIPT)
BASE_SHA = "05b51ebe"   # main 2026-10-31; run_daily_brief.py is v1.20.0 there
GATE = "TFB_BRIEF_UNSIZED_ADD"
WHEN = dt.datetime(2026, 10, 31, 10, 30)
HEADER = ["Action", "Symbol", "Name", "P&L %", "Weight %", "Engine ROI %",
          "Rel", "Conf", "Advisor Note", "MV SAR", "Cost SAR"]


def _load(name, path):
    saved = list(sys.path)
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        spec.loader.exec_module(mod)
    finally:
        sys.path[:] = saved
    return mod


B = _load("brief_unsized_delivered", DELIVERED)
_BASE = {}


def _base_or_none():
    if "mod" not in _BASE:
        path = os.getenv("TFB_TEST_BRIEF_BASE", "").strip()
        if not path:
            try:
                text = subprocess.run(["git", "show", BASE_SHA + ":scripts/run_daily_brief.py"],
                                      cwd=ROOT, capture_output=True, text=True, check=True).stdout
            except Exception:
                text = ""
            if text:
                tmp = Path(tempfile.mkdtemp()) / "brief_v1200_base.py"
                tmp.write_text(text, encoding="utf-8")
                path = str(tmp)
        _BASE["mod"] = _load("brief_unsized_base", path) if path else None
    return _BASE["mod"]


def base():
    if _base_or_none() is None:
        pytest.skip("base copy unavailable (set TFB_TEST_BRIEF_BASE)")
    return _BASE["mod"]


# ------------------------------------------------------------------ notes --
def _entry(sh, sar, stop=52.1, tp1=60.0, tp2=66.4, funds="cash"):
    return {"cand": {"fx_to_sar": 1.0, "stop": stop, "tp1": tp1, "tp2": tp2},
            "action": pa.ACTION_ADD, "confidence_band": "MEDIUM",
            "suggested_delta_sar": sar, "suggested_delta_shares": sh, "funds_from": funds,
            "action_reason": "engine BUY, room in the book", "proceeds_sar": 0.0}


def sized(sh, sar, **kw):
    return pa._advisor_sentence(_entry(sh, sar, **kw), {}, "2026-11-07")


def unsized(**kw):
    return pa._advisor_sentence(_entry(0, 0, **kw), {}, "2026-11-07")


SUKUK = ("ADD (unsized \u2014 no deployable capital); engine BUY, room in the book; "
         + pa.SUKUK_LADDER_NOTE + "; confidence MEDIUM; review by 2026-11-07.")


def row(action, sym, note, mv=1_000.0, cost=950.0, pl=5.26):
    return [action, sym, "Synthetic " + sym, pl, 5.0, 10.0, 60, "MEDIUM", note, mv, cost]


FIXTURES = {
    "w1_sized_plus_unsized": [row("ADD", "1120.SR", sized(10, 1250)),
                              row("ADD", "2222.SR", unsized())],
    "w2_sukuk_unsized": [row("ADD", "5023.SR", SUKUK, mv=10_000.0, cost=9_950.0, pl=0.5)],
    "w3_phantom_ticket": [row("ADD", "9999.SR", unsized(stop=250.0, tp1=255.0, tp2=262.5))],
    "w4_two_under_floor": [row("ADD", "1111.SR", sized(3, 120)),
                           row("ADD", "2020.SR", sized(1, 50))],
    "w5_sunday_fold": [row("ADD", "3030.SR", sized(1, 14)),
                       row("ADD", "4040.SR", sized(2, 98)),
                       row("ADD", "1180.SR", sized(40, 1992))],
    "w6_no_tp_ladder": [row("ADD", "1150.SR", sized(8, 900)),
                        row("ADD", "7020.SR", unsized(tp1=None, tp2=None))],
    "n_mixed_book": [row("EXIT", "2380.SR", "EXIT full position (~3,100 SAR); valuation ROI -4%."),
                     row("TRIM", "1010.SR", "TRIM ~800 SAR; over the position cap."),
                     row("BLOCK", "4002.SR", "BLOCKED; RECONCILIATION_REQUIRED ~9,999 SAR"),
                     row("HOLD", "4280.SR", "HOLD; capped low confidence."),
                     row("ADD", "1120.SR", sized(10, 1250) + " | [ticketcap-observe] ok (1 open, cap 1)"),
                     row("ADD", "2222.SR", unsized())],
    "n_sized_only": [row("ADD", "1120.SR", sized(10, 1250)), row("ADD", "1150.SR", sized(4, 480))],
}


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    for name in (GATE, "TFB_MIN_TICKET_SAR", "TFB_BRIEF_HOLD_NARRATIVE_V2", "TFB_BRIEF_LEVELS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_BRIEF_NEWS_CONTEXT", "0")


def _decision(mod, rows):
    return mod.extract_decision([list(HEADER)] + copy.deepcopy(rows))


def _model(mod, rows):
    return mod.build_model({mod.PAGE_DECISION: [list(HEADER)] + copy.deepcopy(rows)})


def _surfaces(mod, rows):
    m = _model(mod, rows)
    return (m["decision"], mod.render_html(m, "Synthetic owner", WHEN),
            mod.render_text(m, "Synthetic owner", WHEN))


def _digest(mod, rows):
    d, html, text = _surfaces(mod, rows)
    blob = json.dumps(d, sort_keys=True, default=str) + "\n" + html + "\n" + text
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _syms(recs):
    return [r["symbol"] for r in recs]


def _count(html):
    import re
    m = re.search(r"Book snapshot\.</strong>\s*(\d+) holdings", html)
    return int(m.group(1)) if m else None


# ------------------------------------------------------- WORKS checks ------
# Each check raises AssertionError on a module that does not implement the
# gate; G1 runs them on the v1.20.0 base.
def check_w1(mod):
    d = _decision(mod, FIXTURES["w1_sized_plus_unsized"])
    assert [(r["symbol"], r["sar"], r.get("shares")) for r in d["add"]] == [("1120.SR", 1250.0, 10)]
    assert d["add"][0]["note"] == sized(10, 1250) and d["folded"] == []
    held = {r["symbol"]: r for r in d["hold"]}
    assert held["2222.SR"]["unplaced_add"] == "unsized" and held["2222.SR"]["sar"] == 0.0


def check_w2(mod):
    d, html, text = _surfaces(mod, FIXTURES["w2_sukuk_unsized"])
    assert d["mv"] == 10_000.0 and d["cost"] == 9_950.0 and d["add"] == []
    assert _syms(d["hold"]) == ["5023.SR"] and d["dropped_small"] == []
    assert _count(html) == 1 and "ADD not placed: unsized" in html
    assert "ADD NOT PLACED TODAY (still held): 5023.SR (unsized - no deployable capital)" in text
    assert "HOLD: 1 - 5023.SR" in text


def check_w3(mod):
    d, html, text = _surfaces(mod, FIXTURES["w3_phantom_ticket"])
    assert d["add"] == [] and _syms(d["hold"]) == ["9999.SR"]
    assert "262 SAR" not in text and "263 SAR" not in text and "~262" not in html


def check_w4(mod):
    d, html, _text = _surfaces(mod, FIXTURES["w4_two_under_floor"])
    assert d["add"] == [] and d["dropped_small"] == []
    assert [(r["symbol"], r["unplaced_add"], r["sar"]) for r in d["hold"]] == [
        ("1111.SR", "below_floor", 0.0), ("2020.SR", "below_floor", 0.0)]
    assert all("under the 200 SAR minimum ticket - not in today's plan" in r["note"] for r in d["hold"])
    assert "+" not in d["hold"][0]["note"].split("|")[-1]       # no fold text leaked from the copy
    assert _count(html) == 2


def check_w5(mod):
    d, html, _text = _surfaces(mod, FIXTURES["w5_sunday_fold"])
    assert [(r["symbol"], r["sar"]) for r in d["add"]] == [("1180.SR", 2104.0)]
    assert d["add"][0]["shares"] == 42                          # 2104 / (1992 / 40)
    assert d["folded"] == [("3030.SR", 14.0, "1180.SR"), ("4040.SR", 98.0, "1180.SR")]
    assert [(r["symbol"], r["unplaced_add"], r["sar"]) for r in d["hold"]] == [
        ("3030.SR", "folded", 0.0), ("4040.SR", "folded", 0.0)]
    assert "its ~14 SAR joins the 1180.SR ticket" in d["hold"][0]["note"]
    assert _count(html) == 3


def check_w6(mod):
    d = _decision(mod, FIXTURES["w6_no_tp_ladder"])
    assert [(r["symbol"], r["sar"], r.get("shares")) for r in d["add"]] == [("1150.SR", 900.0, 8)]
    assert d["folded"] == [] and _syms(d["hold"]) == ["7020.SR"]


def check_w7(mod, monkeypatch, capsys):
    monkeypatch.setenv(GATE, "observe")
    obs = _surfaces(mod, FIXTURES["w1_sized_plus_unsized"])
    err = capsys.readouterr().err
    monkeypatch.delenv(GATE)
    off = _surfaces(mod, FIXTURES["w1_sized_plus_unsized"])
    assert obs[1:] == off[1:] and json.dumps(obs[0], sort_keys=True, default=str) == \
        json.dumps(off[0], sort_keys=True, default=str)
    lines = [ln for ln in err.splitlines() if ln.startswith("[UNSIZED-ADD v")]
    assert lines == [
        "[UNSIZED-ADD v%s observe] 1120.SR: ticket ~1,316 SAR / 11 sh would be ~1,250 SAR / 10 sh"
        % mod.__version__,
        "[UNSIZED-ADD v%s observe] 2222.SR: would stay in the book (ADD not placed: unsized - no "
        "deployable capital) - v1.20.0 drops it from the holdings" % mod.__version__], err


def _enforce(monkeypatch):
    monkeypatch.setenv(GATE, "enforce")


def test_w1_sized_ticket_keeps_its_own_amount(monkeypatch):
    _enforce(monkeypatch)
    check_w1(B)


def test_w2_sukuk_with_unsized_add_stays_in_the_book(monkeypatch):
    _enforce(monkeypatch)
    check_w2(B)


def test_w3_no_phantom_ticket_from_a_large_level(monkeypatch):
    _enforce(monkeypatch)
    check_w3(B)


def test_w4_under_floor_sized_adds_stay_held(monkeypatch):
    _enforce(monkeypatch)
    check_w4(B)


def test_w5_folded_rows_stay_held_and_cash_is_conserved(monkeypatch):
    _enforce(monkeypatch)
    check_w5(B)


def test_w6_no_tp_ladder_note_is_unsized(monkeypatch):
    _enforce(monkeypatch)
    check_w6(B)


def test_w7_observe_changes_nothing_and_names_each_row(monkeypatch, capsys):
    check_w7(B, monkeypatch, capsys)


# ------------------------------------------------------------- NEVER -------
OFF_SPELLINGS = (None, "", "off", "OFF", " off ", "1", "on", "true", "yes", "enforced", "observe,enforce")
# v1.20.0 (main 05b51ebe) per fixture, gate unset: sha256[:16] of the decision
# dict, the HTML and the text at WHEN. N1 proves them against the base copy
# when it is available and checks the delivered module everywhere.
OFF_GOLDEN = {
    "w1_sized_plus_unsized": "24e3b24ee37456a1",
    "w2_sukuk_unsized": "000332b09c430158",
    "w3_phantom_ticket": "32c1079fe87acbfd",
    "w4_two_under_floor": "a4bc61cb10907819",
    "w5_sunday_fold": "437e2feab0418682",
    "w6_no_tp_ladder": "688dd2d8aa55f006",
    "n_mixed_book": "8de6e73b6d3a534a",
    "n_sized_only": "83383584a214cb10",
}


def _version_neutral(mod, digest_rows):
    """The HTML/text carry no version string; the decision dict neither."""
    return _digest(mod, digest_rows)


def test_n1_off_is_v1_20_0_on_every_fixture(monkeypatch):
    for spelling in OFF_SPELLINGS:
        if spelling is None:
            monkeypatch.delenv(GATE, raising=False)
        else:
            monkeypatch.setenv(GATE, spelling)
        assert B._unsized_add_mode() == "off", spelling
        for name, rows in FIXTURES.items():
            assert _version_neutral(B, rows) == OFF_GOLDEN[name], (spelling, name)
    b = _base_or_none()
    if b is not None:
        monkeypatch.delenv(GATE, raising=False)
        for name, rows in FIXTURES.items():
            assert _version_neutral(b, rows) == OFF_GOLDEN[name], name


def test_n2_observe_outputs_equal_off(monkeypatch, capsys):
    for name, rows in FIXTURES.items():
        monkeypatch.delenv(GATE, raising=False)
        off = _digest(B, rows)
        monkeypatch.setenv(GATE, "observe")
        assert _digest(B, rows) == off, name
    capsys.readouterr()


def test_n3_sized_tickets_at_or_above_the_floor_are_unchanged(monkeypatch):
    off = _decision(B, FIXTURES["n_sized_only"])
    mixed_off = _decision(B, FIXTURES["n_mixed_book"])
    _enforce(monkeypatch)
    on = _decision(B, FIXTURES["n_sized_only"])
    mixed_on = _decision(B, FIXTURES["n_mixed_book"])
    assert json.dumps(on, sort_keys=True, default=str) == json.dumps(off, sort_keys=True, default=str)
    for key in ("sell", "trim", "block", "freed_cash", "mv", "cost", "pl_pct", "min_ticket"):
        assert mixed_on[key] == mixed_off[key], key
    assert [(r["symbol"], r["sar"], r["shares"], r["note"]) for r in mixed_on["add"]] == [
        ("1120.SR", 1250.0, 10, sized(10, 1250) + " | [ticketcap-observe] ok (1 open, cap 1)")]
    assert _syms(mixed_on["hold"]) == ["4280.SR", "2222.SR"]


def test_n4_floor_disabled_keeps_sized_tickets_and_unsized_held(monkeypatch):
    monkeypatch.setenv("TFB_MIN_TICKET_SAR", "0")
    _enforce(monkeypatch)
    d = _decision(B, FIXTURES["w5_sunday_fold"] + FIXTURES["w2_sukuk_unsized"])
    assert [(r["symbol"], r["sar"]) for r in d["add"]] == [("3030.SR", 14.0), ("4040.SR", 98.0),
                                                           ("1180.SR", 1992.0)]
    assert d["folded"] == [] and [(r["symbol"], r["unplaced_add"]) for r in d["hold"]] == [
        ("5023.SR", "unsized")]


def test_n5_plan_never_mutates_its_input(monkeypatch):
    for name, rows in FIXTURES.items():
        adds = [{"symbol": r[1], "note": r[8], "sar": B._note_sar(r[8]),
                 "shares": B._note_shares(r[8])} for r in rows if r[0] == "ADD"]
        before = copy.deepcopy(adds)
        for floor in ("", "0"):
            monkeypatch.setenv("TFB_MIN_TICKET_SAR", floor)
            B._plan_unsized_adds(adds)
            assert adds == before, (name, floor)


@pytest.mark.parametrize("note,want", [
    ("ADD 10 sh (~1250 SAR, cash); x", (10, 1250.0)),
    ("ADD 1,200 sh (~3,418 SAR, freed cash); x", (1200, 3418.0)),
    ("ADD 3 sh (~400.50 SAR, cash)", (3, 400.5)),
    ("add 3 sh (~400 sar, cash)", (3, 400.0)),
    ("ADD 10 sh (~1250 SAR, cash); ... | [ticketcap-observe] ok (1 open, cap 1)", (10, 1250.0)),
    ("ADD 0 sh (~0 SAR, cash)", None),
    ("ADD (unsized \u2014 no deployable capital); r; stop 52.1 / TP1 60 / TP2 66.4 SAR; x", None),
    ("ADD (unsized \u2014 no deployable capital); r; stop 52.1 SAR; no TP ladder", None),
    (SUKUK, None),
    ("", None),
    ("ADD 10 sh cash 1250 SAR", None),
])
def test_n6_sized_clause_parser(note, want):
    assert B._sized_add(note) == want


@pytest.mark.parametrize("raw,want", [
    (None, "off"), ("", "off"), ("off", "off"), ("observe", "observe"), (" Observe ", "observe"),
    ("enforce", "enforce"), ("ENFORCE", "enforce"), ("1", "off"), ("on", "off"), ("enforced", "off"),
])
def test_n7_gate_words(monkeypatch, raw, want):
    if raw is None:
        monkeypatch.delenv(GATE, raising=False)
    else:
        monkeypatch.setenv(GATE, raw)
    assert B._unsized_add_mode() == want


# ---------------------------------------------------------------- GOLD -----
def test_g1_golden_negative_base_fails_every_works_check(monkeypatch, capsys):
    b = base()
    _enforce(monkeypatch)
    failed = []
    for check in (check_w1, check_w2, check_w3, check_w4, check_w5, check_w6):
        try:
            check(b)
        except (AssertionError, KeyError):
            failed.append(check.__name__)
    try:
        check_w7(b, monkeypatch, capsys)
    except (AssertionError, KeyError):
        failed.append("check_w7")
    assert failed == ["check_w1", "check_w2", "check_w3", "check_w4", "check_w5", "check_w6",
                      "check_w7"], failed
    # and the base reproduces the defects themselves
    d = _decision(b, FIXTURES["w1_sized_plus_unsized"])
    assert [(r["symbol"], r["sar"], r.get("shares")) for r in d["add"]] == [("1120.SR", 1316.4, 11)]
    m = _model(b, FIXTURES["w2_sukuk_unsized"])
    assert _count(b.render_html(m, "Synthetic owner", WHEN)) == 0 and m["decision"]["mv"] == 10_000.0


# ------------------------------------------------------------------- V -----
def test_v1_version_pin_workflow_ci_ascii():
    import yaml
    assert tuple(int(x) for x in B.__version__.split(".")) >= (1, 21, 0)
    vd = (ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("run_daily_brief", "%s", "daily brief")' % B.__version__ in vd
    wf = yaml.safe_load((ROOT / ".github" / "workflows" / "daily_brief.yml").read_text(encoding="utf-8"))
    steps = {s.get("name"): s for s in wf["jobs"]["brief"]["steps"]}
    env = steps["\U0001f4e8 Render & send brief"]["env"]
    assert env[GATE] == "${{ vars.TFB_BRIEF_UNSIZED_ADD || 'off' }}"
    assert GATE not in (wf.get("env") or {}) and GATE not in (wf["jobs"]["brief"].get("env") or {})
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "tests/test_daily_brief_unsized_add.py" in ci
    src = DELIVERED.read_text(encoding="utf-8")
    assert src.index("# v1.21.0 (2026-10-31) -- [UNSIZED-ADD]") < src.index('__version__ = "1.21.0"')
    assert all(ord(c) < 128 for c in Path(__file__).read_text(encoding="utf-8"))
    b = _base_or_none()
    if b is not None:
        import difflib
        old = Path(b.__file__).read_text(encoding="utf-8").splitlines()
        new = src.splitlines()
        diff = list(difflib.unified_diff(old, new, lineterm="", n=0))
        added = [ln[1:] for ln in diff if ln.startswith("+") and not ln.startswith("+++")]
        removed = [ln[1:] for ln in diff if ln.startswith("-") and not ln.startswith("---")]
        assert added and all(ln.isascii() for ln in added), [ln for ln in added if not ln.isascii()][:3]
        import ast

        def defs(text):
            return {n.name for n in ast.walk(ast.parse(text))
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
        assert not defs("\n".join(old)) - defs("\n".join(new))
        assert defs("\n".join(new)) - defs("\n".join(old)) == {
            "_unsized_add_mode", "_sized_add", "_plan_unsized_adds", "_log_unsized_add_observe",
            "_unplaced_label"}
        assert len(removed) <= 12, removed


if __name__ == "__main__":
    class _Tally:
        def __init__(self):
            self.passed = self.failed = self.skipped = 0

        def pytest_runtest_logreport(self, report):
            if report.when == "call" and report.passed:
                self.passed += 1
            elif report.skipped:
                self.skipped += 1
            elif report.failed:
                self.failed += 1

    tally = _Tally()
    rc = pytest.main([__file__, "-q", "-p", "no:cacheprovider"], plugins=[tally])
    run = tally.passed + tally.failed
    print("PASS %d/%d" % (tally.passed, run) + (" (%d skipped)" % tally.skipped if tally.skipped else ""))
    sys.exit(rc)
