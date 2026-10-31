#!/usr/bin/env python3
"""tests/test_daily_brief_unsized_add.py - run_daily_brief v1.21.0 [UNSIZED-ADD]
(gate TFB_BRIEF_UNSIZED_ADD = off | observe | enforce, default off = v1.20.0).

WHY (2026-10-31): extract_decision read an ADD's amount with _note_sar, the
first "<n> SAR" anywhere in the Advisor Note. An unsized ADD ("ADD (unsized -
no deployable capital); ...; stop S / TP1 T1 / TP2 T2 SAR; ...") has no
amount, so its TP2 or stop LEVEL was read as cash: it folded into another
ADD ticket, became a phantom ticket (alone at 200 SAR or more, or several
sub-floor levels summed past the floor), or was dropped with the held line
missing from the holdings count while its MV and cost stayed in the book
totals. The v1.8.0 floor also removed folded or under-floor sized ADD rows
from the holdings.

The ADD notes of the WORKS fixtures are written by the REAL
portfolio_actions._advisor_sentence (the sukuk one with the real
SUKUK_LADDER_NOTE), so the note contract the brief parses is pinned; the
other actions' notes, the BUY / blank rows and the N6 parser strings are
hand-written. Offline: no network, no credentials.

Cases
WORKS   W1-W12  enforce: a sized ticket keeps its own amount; a sukuk with an
                unsized ADD stays in the count and on every surface (HTML,
                text, hold intro, Book snapshot, hero, PDF); no phantom
                ticket (one level, or two summed); under-floor and folded rows
                stay held with their reasons; the "no TP ladder" note; observe
                prints one line per affected input row on every fixture; a
                folded row whose target is withheld says so; the narrative
                kill-switch keeps the true copy; BUY / blank rows read "no
                ticket size in its note".
NEVER   N1-N7   off (11 spellings) == v1.20.0 on every fixture (decision dict,
                HTML, text: OFF_GOLDEN digests) and logs nothing; observe ==
                off; sized tickets at or above the floor unchanged; the floor
                disabled; never mutates its input; the parser table; the gate
                words.
BASE    N1b N1c off == the v1.20.0 base copy (digests; PDF text when reportlab
                and pypdf are installed).
GOLD    G1      every WORKS check fails on the untouched v1.20.0 base (main
                05b51ebe) with the gate set, which also reproduces the defects.
V       V1 V1b  version floor, verifier pin, step-env mapping, the pytest line
                in ci.yml; against the base: ASCII added lines, no function
                removed, exactly the seven added.

Base copy: TFB_TEST_BRIEF_BASE=<path>, else `git show
05b51ebe:scripts/run_daily_brief.py`; without either the BASE cases, G1 and
V1b SKIP (visibly). Golden negative of the whole file:
TFB_TEST_BRIEF_DELIVERED=<base copy> runs every case against v1.20.0: the
WORKS cases fail on behaviour, N1 and N3-N7 and V1 fail because the helpers
and the version do not exist there.

Run: pytest -q tests/test_daily_brief_unsized_add.py
  or python3 tests/test_daily_brief_unsized_add.py   (prints PASS k/k)
"""
from __future__ import annotations

import copy
import datetime as dt
import hashlib
import importlib.util
import io
import json
import os
import re
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
DASH = "\u2014"
HEADER = ["Action", "Symbol", "Name", "P&L %", "Weight %", "Engine ROI %",
          "Rel", "Conf", "Advisor Note", "MV SAR", "Cost SAR"]
ADDED = {"_unsized_add_mode", "_sized_add", "_plan_unsized_adds", "_log_unsized_add_observe",
         "_unplaced_label", "_settle_unplaced", "_unplaced_phrase"}


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
    "w3b_levels_sum_past_floor": [row("ADD", "8010.SR", unsized(stop=100.0, tp1=110.0, tp2=120.0)),
                                  row("ADD", "8020.SR", unsized(stop=95.0, tp1=100.0, tp2=110.0))],
    "w4_two_under_floor": [row("ADD", "1111.SR", sized(3, 120)),
                           row("ADD", "2020.SR", sized(1, 50))],
    "w5_sunday_fold": [row("ADD", "3030.SR", sized(1, 14)),
                       row("ADD", "4040.SR", sized(2, 98)),
                       row("ADD", "1180.SR", sized(40, 1992))],
    "w6_no_tp_ladder": [row("ADD", "1150.SR", sized(8, 900)),
                        row("ADD", "7020.SR", unsized(tp1=None, tp2=None))],
    # all under the floor; the unsized level (150) outranks the sized rows in
    # v1.20.0, so enforce moves the ticket to 1111.SR (Codex review, PR #882)
    "w3c_ticket_moves_to_a_sized_row": [row("ADD", "1111.SR", sized(1, 120)),
                                        row("ADD", "2020.SR", sized(1, 100)),
                                        row("ADD", "9090.SR", unsized(stop=130.0, tp1=140.0, tp2=150.0))],
    "n_mixed_book": [row("EXIT", "2380.SR", "EXIT full position (~3,100 SAR); valuation ROI -4%."),
                     row("TRIM", "1010.SR", "TRIM ~800 SAR; over the position cap."),
                     row("BLOCK", "4002.SR", "BLOCKED; RECONCILIATION_REQUIRED ~9,999 SAR"),
                     row("HOLD", "4280.SR", "HOLD; capped low confidence."),
                     row("ADD", "1120.SR", sized(10, 1250) + " | [ticketcap-observe] ok (1 open, cap 1)"),
                     row("ADD", "2222.SR", unsized())],
    "n_sized_only": [row("ADD", "1120.SR", sized(10, 1250)), row("ADD", "1150.SR", sized(4, 480))],
    "n_buy_and_blank": [row("BUY", "6001.SR", "BUY on breakout; stop 20 SAR"), row("ADD", "6002.SR", "")],
    "n_dup_symbol": [row("ADD", "2222.SR", sized(3, 300)), row("ADD", "2222.SR", unsized())],
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
    m = re.search(r"Book snapshot\.</strong>\s*(\d+) holdings", html)
    return int(m.group(1)) if m else None


def _line(text, prefix):
    hits = [ln for ln in text.splitlines() if ln.startswith(prefix)]
    return hits[0] if len(hits) == 1 else hits


def _pdf_text(mod, model):
    pytest.importorskip("reportlab")
    reader = pytest.importorskip("pypdf").PdfReader
    pdf = mod.render_pdf(model, "Synthetic owner", WHEN)
    assert pdf is not None and pdf.startswith(b"%PDF-")
    return "\n".join(p.extract_text() for p in reader(io.BytesIO(pdf)).pages)


def _observe_lines(mod, rows, capsys):
    capsys.readouterr()
    _decision(mod, rows)
    return [ln for ln in capsys.readouterr().err.splitlines() if ln.startswith("[UNSIZED-ADD")]


# ------------------------------------------------------- WORKS checks ------
# Each check raises AssertionError (or KeyError / AttributeError) on a module
# that does not implement the gate; G1 runs them on the v1.20.0 base.
def check_w1(mod):
    d = _decision(mod, FIXTURES["w1_sized_plus_unsized"])
    assert [(r["symbol"], r["sar"], r.get("shares")) for r in d["add"]] == [("1120.SR", 1250.0, 10)]
    assert d["add"][0]["note"] == sized(10, 1250) and d["folded"] == []
    held = {r["symbol"]: r for r in d["hold"]}
    assert held["2222.SR"]["unplaced_add"] == "unsized" and held["2222.SR"]["sar"] == 0.0
    assert held["2222.SR"]["note"] == unsized()           # unsized rows keep their own note


def check_w2(mod):
    d, html, text = _surfaces(mod, FIXTURES["w2_sukuk_unsized"])
    assert d["mv"] == 10_000.0 and d["cost"] == 9_950.0 and d["add"] == []
    assert _syms(d["hold"]) == ["5023.SR"] and d["dropped_small"] == []
    assert _count(html) == 1 and "ADD not placed: unsized - no deployable capital" in html
    assert ("1 positions are on hold for stated reasons %s 1 ADD not placed today %s not comfort holds."
            % (DASH, DASH)) in html
    assert "ADD NOT PLACED TODAY %s SEE NOTES" % DASH in html
    assert "1 ADD not placed today (1 no deployable capital)" in html
    assert "No ADD can be placed today" in html and "no buy clears it either" not in html
    assert _line(text, "ADD NOT PLACED TODAY") == \
        "ADD NOT PLACED TODAY (still held): 5023.SR (unsized - no deployable capital)"
    assert "HOLD (data too weak" not in text and "HOLD: 1 - 5023.SR" in text
    assert "  No ADD can be placed today: 1 ADD signal(s) not placed" in text


def check_w3(mod):
    for name, syms in (("w3_phantom_ticket", ["9999.SR"]),
                       ("w3b_levels_sum_past_floor", ["8010.SR", "8020.SR"])):
        d, html, text = _surfaces(mod, FIXTURES[name])
        assert d["add"] == [] and _syms(d["hold"]) == syms, name
        assert "ADD:" not in text and "~262" not in html and "~230" not in html, name
        assert _count(html) == len(syms), name
    d = _decision(mod, FIXTURES["w3c_ticket_moves_to_a_sized_row"])
    assert [(r["symbol"], r["sar"], r.get("shares")) for r in d["add"]] == [("1111.SR", 220.0, 2)]
    assert d["folded"] == [("2020.SR", 100.0, "1111.SR")]
    assert [(r["symbol"], r["unplaced_add"]) for r in d["hold"]] == [("2020.SR", "folded"), ("9090.SR", "unsized")]


def check_w4(mod):
    d, html, text = _surfaces(mod, FIXTURES["w4_two_under_floor"])
    assert d["add"] == [] and d["dropped_small"] == []
    assert [(r["symbol"], r["unplaced_add"], r["sar"], r["shares"]) for r in d["hold"]] == [
        ("1111.SR", "below_floor", 0.0, 3), ("2020.SR", "below_floor", 0.0, 1)]
    assert all(r["note"].endswith("| v1.21.0: under the 200 SAR minimum ticket - not in today's plan")
               for r in d["hold"])
    assert all("min-ticket fold" not in r["note"] for r in d["hold"])   # no fold from the copy
    assert _count(html) == 2 and "2 ADD not placed today (2 under the 200 SAR minimum ticket)" in html
    assert _line(text, "ADD NOT PLACED TODAY") == (
        "ADD NOT PLACED TODAY (still held): 1111.SR (under the 200 SAR minimum ticket); "
        "2020.SR (under the 200 SAR minimum ticket)")


def check_w5(mod):
    d, html, text = _surfaces(mod, FIXTURES["w5_sunday_fold"])
    assert [(r["symbol"], r["sar"]) for r in d["add"]] == [("1180.SR", 2104.0)]
    assert d["add"][0]["shares"] == 42                          # 2104 / (1992 / 40)
    assert d["folded"] == [("3030.SR", 14.0, "1180.SR"), ("4040.SR", 98.0, "1180.SR")]
    assert [(r["symbol"], r["unplaced_add"], r["sar"], r.get("fold_into")) for r in d["hold"]] == [
        ("3030.SR", "folded", 0.0, "1180.SR"), ("4040.SR", "folded", 0.0, "1180.SR")]
    assert "its ~14 SAR joins the 1180.SR ticket" in d["hold"][0]["note"]
    assert _count(html) == 3 and "2 ADD not placed today (2 cash folded into another ADD ticket)" in html
    assert "ADD not placed: its cash joins the 1180.SR ticket" in html
    assert _line(text, "ADD:").startswith("ADD:   1180.SR (~2,104 SAR)")


def check_w6(mod):
    d = _decision(mod, FIXTURES["w6_no_tp_ladder"])
    assert [(r["symbol"], r["sar"], r.get("shares")) for r in d["add"]] == [("1150.SR", 900.0, 8)]
    assert d["folded"] == [] and _syms(d["hold"]) == ["7020.SR"]


OBS = "[UNSIZED-ADD v1.21.0 observe] "
OBSERVE_LINES = {
    "w1_sized_plus_unsized": [
        OBS + "1120.SR: ticket ~1,316 SAR / 11 sh would be ~1,250 SAR / 10 sh",
        OBS + "2222.SR: would stay in the book (ADD not placed: unsized - no deployable capital)"
              " - v1.20.0 drops it from the holdings"],
    "w2_sukuk_unsized": [
        OBS + "5023.SR: would stay in the book (ADD not placed: unsized - no deployable capital)"
              " - v1.20.0 drops it from the holdings"],
    "w3_phantom_ticket": [
        OBS + "9999.SR: would stay in the book (ADD not placed: unsized - no deployable capital)"
              " - v1.20.0 shows it as an ADD ticket of ~262 SAR"],
    "w3b_levels_sum_past_floor": [
        OBS + "8010.SR: would stay in the book (ADD not placed: unsized - no deployable capital)"
              " - v1.20.0 shows it as an ADD ticket of ~230 SAR",
        OBS + "8020.SR: would stay in the book (ADD not placed: unsized - no deployable capital)"
              " - v1.20.0 drops it from the holdings"],
    "w4_two_under_floor": [
        OBS + "1111.SR: would stay in the book (ADD not placed: under the 200 SAR minimum ticket)"
              " - v1.20.0 drops it from the holdings",
        OBS + "2020.SR: would stay in the book (ADD not placed: under the 200 SAR minimum ticket)"
              " - v1.20.0 drops it from the holdings"],
    "w5_sunday_fold": [
        OBS + "3030.SR: would stay in the book (ADD not placed: its cash joins the 1180.SR ticket)"
              " - v1.20.0 drops it from the holdings",
        OBS + "4040.SR: would stay in the book (ADD not placed: its cash joins the 1180.SR ticket)"
              " - v1.20.0 drops it from the holdings"],
    "w3c_ticket_moves_to_a_sized_row": [
        OBS + "1111.SR: would be an ADD ticket of ~220 SAR / 2 sh - v1.20.0 drops it from the holdings",
        OBS + "2020.SR: would stay in the book (ADD not placed: its cash joins the 1111.SR ticket)"
              " - v1.20.0 drops it from the holdings",
        OBS + "9090.SR: would stay in the book (ADD not placed: unsized - no deployable capital)"
              " - v1.20.0 shows it as an ADD ticket of ~370 SAR"],
    "w6_no_tp_ladder": [
        OBS + "1150.SR: ticket ~952 SAR / 8 sh would be ~900 SAR / 8 sh",
        OBS + "7020.SR: would stay in the book (ADD not placed: unsized - no deployable capital)"
              " - v1.20.0 drops it from the holdings"],
    "n_mixed_book": [
        OBS + "1120.SR: ticket ~1,316 SAR / 11 sh would be ~1,250 SAR / 10 sh",
        OBS + "2222.SR: would stay in the book (ADD not placed: unsized - no deployable capital)"
              " - v1.20.0 drops it from the holdings"],
    "n_sized_only": [],
    "n_buy_and_blank": [
        OBS + "6001.SR: would stay in the book (ADD not placed: no ticket size in its note)"
              " - v1.20.0 drops it from the holdings",
        OBS + "6002.SR: would stay in the book (ADD not placed: no ticket size in its note)"
              " - v1.20.0 drops it from the holdings"],
    "n_dup_symbol": [
        OBS + "2222.SR: ticket ~366 SAR / 4 sh would be ~300 SAR / 3 sh",
        OBS + "2222.SR: would stay in the book (ADD not placed: unsized - no deployable capital)"
              " - v1.20.0 drops it from the holdings"],
}


def check_w7(mod, monkeypatch, capsys):
    monkeypatch.setenv(GATE, "observe")
    for name, rows in FIXTURES.items():
        assert _observe_lines(mod, rows, capsys) == OBSERVE_LINES[name], name


def check_w8(mod, monkeypatch):
    """The fold target is withheld by the final-feed veto after extract_decision."""
    monkeypatch.setattr(mod, "_extract_final_feed",
                        lambda rows: "NOT_ACTIONABLE(final_publication) | run=1 | x | coverage_failed")
    d, html, text = _surfaces(mod, FIXTURES["w5_sunday_fold"])
    assert d["add"] == [] and _syms(d["block"]) == ["1180.SR"]
    assert [(r["symbol"], r["unplaced_add"]) for r in d["hold"]] == [
        ("3030.SR", "folded_withheld"), ("4040.SR", "folded_withheld")]
    assert all("was to join the 1180.SR ticket, which is not placed today" in r["note"]
               and "joins the 1180.SR ticket (" not in r["note"] for r in d["hold"])
    assert "its cash was to join the 1180.SR ticket, which is not placed today" in text
    assert "cash joins" not in text and "cash joins" not in html


def check_w9(mod, monkeypatch):
    """The narrative kill-switch never turns a held ADD into 'data too weak' copy."""
    monkeypatch.setenv("TFB_BRIEF_HOLD_NARRATIVE_V2", "0")
    rows = [row("HOLD", "4280.SR", "HOLD; capped low confidence.")] + FIXTURES["w2_sukuk_unsized"]
    d, html, text = _surfaces(mod, rows)
    assert "DATA TOO WEAK TO ACT" not in html and "held only because the data is too weak" not in html
    assert ("2 positions are on hold for stated reasons %s 1 reliability-capped, 1 ADD not placed today"
            % DASH) in html
    assert _line(text, "HOLD (data too weak to act):") == "HOLD (data too weak to act): 4280.SR"


def check_w10(mod):
    d = _decision(mod, FIXTURES["n_buy_and_blank"])
    assert d["add"] == [] and [(r["symbol"], r["unplaced_add"]) for r in d["hold"]] == [
        ("6001.SR", "unparsed"), ("6002.SR", "unparsed")]
    assert mod._unplaced_label(d["hold"][0]) == "no ticket size in its note"


def check_w11(mod):
    d, html, text = _surfaces(mod, FIXTURES["n_mixed_book"])
    assert _line(text, "HOLD (data too weak to act):") == "HOLD (data too weak to act): 4280.SR"
    assert _line(text, "ADD NOT PLACED TODAY") == \
        "ADD NOT PLACED TODAY (still held): 2222.SR (unsized - no deployable capital)"
    assert [(r["symbol"], r["sar"]) for r in d["add"]] == [("1120.SR", 1250.0)]
    assert _count(html) == 6 and d["freed_cash"] == 3_900.0


def check_w12(mod):
    m = _model(mod, FIXTURES["w2_sukuk_unsized"])
    text = _pdf_text(mod, m)
    assert "5023.SR" in text and "unsized" in text and "HOLD" in text
    assert re.search(r"HOLDINGS\s*1\b", text) or re.search(r"\b1\s*HOLDINGS", text), text[:600]


def _enforce(monkeypatch):
    monkeypatch.setenv(GATE, "enforce")


def test_w1_sized_ticket_keeps_its_own_amount(monkeypatch):
    _enforce(monkeypatch)
    check_w1(B)


def test_w2_sukuk_with_unsized_add_stays_on_every_surface(monkeypatch):
    _enforce(monkeypatch)
    check_w2(B)


def test_w3_no_phantom_ticket_from_one_or_two_levels(monkeypatch):
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


def test_w7_observe_prints_one_line_per_affected_row(monkeypatch, capsys):
    check_w7(B, monkeypatch, capsys)


def test_w8_withheld_fold_target_is_named(monkeypatch):
    _enforce(monkeypatch)
    check_w8(B, monkeypatch)


def test_w9_narrative_kill_switch_keeps_the_true_copy(monkeypatch):
    _enforce(monkeypatch)
    check_w9(B, monkeypatch)


def test_w10_rows_without_a_size_read_as_unparsed(monkeypatch):
    _enforce(monkeypatch)
    check_w10(B)


def test_w11_mixed_book_text_lines(monkeypatch):
    _enforce(monkeypatch)
    check_w11(B)


def test_w12_pdf_lists_the_held_add(monkeypatch):
    _enforce(monkeypatch)
    check_w12(B)


# ------------------------------------------------------------- NEVER -------
OFF_SPELLINGS = (None, "", "off", "OFF", " off ", "1", "on", "true", "yes", "enforced", "observe,enforce")
# v1.20.0 (main 05b51ebe) per fixture, gate unset: sha256[:16] of the decision
# dict, the HTML and the text at WHEN. N1b proves them against the base copy.
OFF_GOLDEN = {
    "w1_sized_plus_unsized": "24e3b24ee37456a1",
    "w2_sukuk_unsized": "000332b09c430158",
    "w3_phantom_ticket": "32c1079fe87acbfd",
    "w3b_levels_sum_past_floor": "cb0e96f619deee6a",
    "w4_two_under_floor": "a4bc61cb10907819",
    "w5_sunday_fold": "437e2feab0418682",
    "w6_no_tp_ladder": "688dd2d8aa55f006",
    "w3c_ticket_moves_to_a_sized_row": "03ef48a8fc74eaa1",
    "n_mixed_book": "8de6e73b6d3a534a",
    "n_sized_only": "83383584a214cb10",
    "n_buy_and_blank": "95861ea3e1eec63c",
    "n_dup_symbol": "0fd08e9620d24ecd",
}


def test_n1_off_is_v1_20_0_on_every_fixture_and_logs_nothing(monkeypatch, capsys):
    for spelling in OFF_SPELLINGS:
        if spelling is None:
            monkeypatch.delenv(GATE, raising=False)
        else:
            monkeypatch.setenv(GATE, spelling)
        assert B._unsized_add_mode() == "off", spelling
        for name, rows in FIXTURES.items():
            capsys.readouterr()
            assert _digest(B, rows) == OFF_GOLDEN[name], (spelling, name)
            assert "[UNSIZED-ADD" not in capsys.readouterr().err, (spelling, name)


def test_n1b_off_golden_digests_are_the_v1_20_0_base():
    b = base()
    for name, rows in FIXTURES.items():
        assert _digest(b, rows) == OFF_GOLDEN[name], name


def test_n1c_off_pdf_text_equals_the_v1_20_0_base():
    """Byte-equal PDF text except the footer, which prints each copy's own version."""
    b = base()
    for name, rows in FIXTURES.items():
        mine = _pdf_text(B, _model(B, rows)).replace("v" + B.__version__, "v<version>")
        theirs = _pdf_text(b, _model(b, rows)).replace("v" + b.__version__, "v<version>")
        assert mine == theirs, name


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
                 "shares": B._note_shares(r[8])} for r in rows if r[0] in ("ADD", "BUY")]
        before = copy.deepcopy(adds)
        for floor in ("", "0", "500"):
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
    checks = [(check_w1, ()), (check_w2, ()), (check_w3, ()), (check_w4, ()), (check_w5, ()),
              (check_w6, ()), (check_w8, (monkeypatch,)), (check_w9, (monkeypatch,)), (check_w10, ()),
              (check_w11, ())]
    for check, extra in checks:
        try:
            check(b, *extra)
        except (AssertionError, KeyError, AttributeError):
            failed.append(check.__name__)
    try:
        check_w7(b, monkeypatch, capsys)
    except (AssertionError, KeyError, AttributeError):
        failed.append("check_w7")
    assert sorted(failed) == sorted([c.__name__ for c, _e in checks] + ["check_w7"]), failed
    monkeypatch.undo()
    monkeypatch.setenv("TFB_BRIEF_NEWS_CONTEXT", "0")
    monkeypatch.delenv(GATE, raising=False)
    d = _decision(b, FIXTURES["w1_sized_plus_unsized"])
    assert [(r["symbol"], r["sar"], r.get("shares")) for r in d["add"]] == [("1120.SR", 1316.4, 11)]
    assert [(r["symbol"], r["sar"]) for r in _decision(b, FIXTURES["w3b_levels_sum_past_floor"])["add"]] == [
        ("8010.SR", 230.0)]
    m = _model(b, FIXTURES["w2_sukuk_unsized"])
    assert _count(b.render_html(m, "Synthetic owner", WHEN)) == 0 and m["decision"]["mv"] == 10_000.0


# ------------------------------------------------------------------- V -----
def test_v1_version_pin_workflow_ci():
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
    assert re.search(r"^\s*python -m pytest [^\n#]*tests/test_daily_brief_unsized_add\.py", ci, re.M)
    src = DELIVERED.read_text(encoding="utf-8")
    assert src.index("# v1.21.0 (2026-10-31) -- [UNSIZED-ADD]") < src.index("__version__ = ")
    assert all(ord(c) < 128 for c in Path(__file__).read_text(encoding="utf-8"))


def test_v1b_against_the_base_ascii_and_functions():
    import ast
    import difflib
    b = base()
    if B.__version__ != "1.21.0":
        pytest.skip("the exact 05b51ebe -> 1.21.0 diff checks apply to 1.21.0 only")
    old = Path(b.__file__).read_text(encoding="utf-8").splitlines()
    new = DELIVERED.read_text(encoding="utf-8").splitlines()
    diff = list(difflib.unified_diff(old, new, lineterm="", n=0))
    added = [ln[1:] for ln in diff if ln.startswith("+") and not ln.startswith("+++")]
    assert added and all(ln.isascii() for ln in added), [ln for ln in added if not ln.isascii()][:3]

    kinds = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)

    def defs(text):        # every def, nested ones included: none may disappear
        return {n.name for n in ast.walk(ast.parse(text)) if isinstance(n, kinds)}

    def top(text):         # module-level defs: exactly the seven are new
        return {n.name for n in ast.parse(text).body if isinstance(n, kinds)}
    assert not defs("\n".join(old)) - defs("\n".join(new))
    assert top("\n".join(new)) - top("\n".join(old)) == ADDED


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
