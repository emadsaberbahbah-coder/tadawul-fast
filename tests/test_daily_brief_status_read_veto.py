#!/usr/bin/env python3
"""tests/test_daily_brief_status_read_veto.py - run_daily_brief v1.22.0
[STATUS-READ-VETO] (TFB_BRIEF_STATUS_READ_VETO, default off = v1.20.0 byte
for byte).

WHY (2026-11-02): the final TFB Decision Feed receipt on _Status is the
brief's only guard on the Portfolio_Decision SELL / TRIM / ADD tickets and
its first guard on the Top-10 picks. read_pages_live turns a failed page
read into [], so one Sheets error on _Status dropped the veto and the email
offered executable tickets while the feed was NOT_ACTIONABLE. With the gate
on, an unreadable _Status stands in a WITHHELD(status_unreadable) receipt;
the existing veto withholds every ticket and pick and the brief still sends.

REAL module; the only stub is the Sheets seam (integrations
.google_sheets_service.read_range, imported inside read_pages_live). No
network.

S1  off (unset / off / 0 / blank / onn / enforce): read_pages_live output,
    its stderr, the model's decision and top10, the text, the HTML and the
    subject equal the untouched v1.20.0 (git show 3bbf3c80), with the _Status
    read failing and with it readable
S1b the six on-spellings; S1c junk stays off
S2  on, _Status read raises: one receipt row WITHHELD(status_unreadable)
    with the exception's TYPE only (never its text); every SELL / TRIM / ADD
    withheld (0 SAR, no shares / stop / targets, freed cash 0); Top-10 picks
    withheld; text, HTML and PDF carry the withheld banner and no ticket;
    the subject offers nothing; two stderr lines
S3  on, _Status readable (EXECUTABLE / NOT_ACTIONABLE / no key / empty):
    identical to off - the gate acts only on a failed read
S4  on, another page fails (Top_10, Portfolio_Decision, Calendar_Events):
    identical to off
S5  on, any exception class: the receipt names the class only
G1  golden negative: on the untouched v1.20.0 the same failed read emails
    'add ADD.US', TRIM and EXIT tickets and the SELECTED.US buy (S2 fails)
V1  version floor, verify_deployment pin, daily_brief.yml step env maps
    the gate with default 'off' (exactly one line), ci.yml runs this file,
    WHY block order, ASCII file
V1b against the base: ASCII added lines; exactly one function added
    (_status_read_veto_enabled), none removed

Base copy: TFB_TEST_BRIEF_BASE=<path>, else `git show 3bbf3c80:scripts/
run_daily_brief.py`; without either, S1, G1 and V1b SKIP visibly. Golden
negative of the whole file: TFB_TEST_BRIEF_DELIVERED=<base copy>.

Run: python3 tests/test_daily_brief_status_read_veto.py   (prints PASS k/k)
  or python3 -m pytest -q tests/test_daily_brief_status_read_veto.py
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import io
import os
import re
import subprocess
import sys
import tempfile
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BASE_SHA = "3bbf3c80"   # main 2026-11-02; run_daily_brief v1.20.0 there
GATE = "TFB_BRIEF_STATUS_READ_VETO"
WHEN = dt.datetime(2026, 11, 2, 10, 30)
_MODS = {}


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def delivered():
    if "delivered" not in _MODS:
        path = os.getenv("TFB_TEST_BRIEF_DELIVERED", "").strip() or str(
            ROOT / "scripts" / "run_daily_brief.py")
        _MODS["delivered"] = _load("run_daily_brief_delivered", path)
    return _MODS["delivered"]


def _base_or_none():
    if "base" not in _MODS:
        path = os.getenv("TFB_TEST_BRIEF_BASE", "").strip()
        if not path:
            try:
                blob = subprocess.run(["git", "-C", str(ROOT), "show",
                                       BASE_SHA + ":scripts/run_daily_brief.py"],
                                      capture_output=True, timeout=60)
            except Exception:
                blob = None
            if blob is not None and blob.returncode == 0 and b'__version__ = "1.20.0"' in blob.stdout:
                tmp = Path(tempfile.mkdtemp()) / "run_daily_brief_v1200_base.py"
                tmp.write_bytes(blob.stdout)
                path = str(tmp)
        _MODS["base"] = _load("run_daily_brief_v1200_base", path) if path else None
    return _MODS["base"]


def base():
    if _base_or_none() is None:
        pytest.skip("base copy unavailable (set TFB_TEST_BRIEF_BASE)")
    return _MODS["base"]


# ------------------------------------------------------------ fixtures -----
HEADER = ["Action", "Symbol", "Name", "P&L %", "Weight %", "Engine ROI %",
          "Rel", "Conf", "Advisor Note", "MV SAR", "Cost SAR"]


def row(action, symbol, mv=1_100, cost=1_000, note=""):
    return [action, symbol, "Synthetic holding", None, None, None, None, "", note, mv, cost]


DECISION = [HEADER, row("EXIT", "EXIT.US", note="Full exit ~500 SAR"),
            row("TRIM", "TRIM.US", note="Trim ~200 SAR"),
            row("ADD", "ADD.US", note="ADD 10 sh ~700 SAR"),
            row("HOLD", "HOLD.US")]
TOP10 = [["SELECTED \u2014 EXECUTABLE TICKETS (1)"],
         ["Rank", "Symbol", "Name", "Advisor Note", "Shares", "Stop SAR", "TP1 SAR"],
         [1, "SELECTED.US", "Synthetic", "INVEST \u2014 synthetic ticket", 10, 90, 120]]
FEED_NA = "NOT_ACTIONABLE(final_publication) | run=1 | synthetic"
FEED_EX = "EXECUTABLE | run=1 | synthetic"


def status(feed):
    return [["Page", "Last Updated", "Status", "Message", "", "", "", "", "", "", "",
             "TFB Decision Feed", feed]]


class HttpError(Exception):
    pass


class Sheets:
    """read_range stand-in: per-page grids or exceptions; records reads."""

    def __init__(self, grids=None, errors=None):
        self.grids = dict({"Portfolio_Decision": DECISION, "Top_10_Investments": TOP10,
                           "_Status": status(FEED_NA)}, **(grids or {}))
        self.errors = errors or {}
        self.reads = []

    def read_range(self, sid, rng):
        page = rng.split("!", 1)[0].strip("'")
        self.reads.append(page)
        if page in self.errors:
            raise self.errors[page]
        return self.grids.get(page, [])


def _stub(monkeypatch, sheets):
    mod = types.ModuleType("integrations.google_sheets_service")
    mod.read_range = sheets.read_range
    pkg = types.ModuleType("integrations")
    pkg.__path__ = []
    pkg.google_sheets_service = mod
    monkeypatch.setitem(sys.modules, "integrations", pkg)
    monkeypatch.setitem(sys.modules, "integrations.google_sheets_service", mod)


def _env(monkeypatch, gate):
    monkeypatch.setenv("TFB_BRIEF_NEWS_CONTEXT", "0")
    monkeypatch.setenv("TFB_MIN_TICKET_SAR", "0")
    if gate is None:
        monkeypatch.delenv(GATE, raising=False)
    else:
        monkeypatch.setenv(GATE, gate)


def run(mod, monkeypatch, capsys, gate, sheets=None):
    """The brief's live path from the read to every rendered surface."""
    sheets = sheets or Sheets(errors={"_Status": RuntimeError("HTTP 503 synthetic Sheets outage")})
    _stub(monkeypatch, sheets)
    _env(monkeypatch, gate)
    capsys.readouterr()
    pages = mod.read_pages_live("SID", mod.ALL_PAGES)
    err = capsys.readouterr().err
    model = mod.build_model(pages)
    v = mod.__version__
    out = {
        "pages": pages,
        "stderr": err,
        "decision": model["decision"],
        "top10": model["top10"],
        "text": mod.render_text(model, "Owner", WHEN).replace(v, "V"),
        "html": mod.render_html(model, "Owner", WHEN).replace(v, "V"),
        "subject": mod.build_subject(model, WHEN),
    }
    return out, model


def _pdf_text(mod, model):
    pytest.importorskip("reportlab")
    pypdf = pytest.importorskip("pypdf")
    pdf = mod.render_pdf(model, "Owner", WHEN)
    assert pdf is not None and pdf.startswith(b"%PDF-")
    return "\n".join(p.extract_text() for p in pypdf.PdfReader(io.BytesIO(pdf)).pages)


WITHHELD = "WITHHELD(status_unreadable) | _Status read failed: RuntimeError"


# ------------------------------------------------------- WORKS checks ------
def check_s2(mod, monkeypatch, capsys):
    out, model = run(mod, monkeypatch, capsys, "on")
    assert out["pages"]["_Status"] == [["TFB Decision Feed", WITHHELD]], out["pages"]["_Status"]
    d = out["decision"]
    assert d.get("execution_veto") == WITHHELD, d.get("execution_veto")
    assert d["sell"] == [] and d["trim"] == [] and d["add"] == [], (d["sell"], d["trim"], d["add"])
    held = {r["symbol"]: r for r in d["block"]}
    assert {"EXIT.US", "TRIM.US", "ADD.US"} <= set(held), sorted(held)
    for sym in ("EXIT.US", "TRIM.US", "ADD.US"):
        rec = held[sym]
        assert rec["sar"] == 0.0 and all(rec.get(k) is None for k in ("shares", "stop", "tp1", "tp2"))
        assert rec["note"].startswith("Final decision feed withheld"), rec["note"]
    assert d["freed_cash"] == 0.0
    assert out["top10"]["top"] == [] and out["top10"].get("withheld_reason"), out["top10"]
    for surface in ("text", "html"):
        s = out[surface]
        assert "Final decision feed withheld" in s, surface
        assert "status_unreadable" in s and "HTTP 503" not in s, surface
        assert "SELECTED.US" not in s, surface
    assert "BEST NEW BUYS" not in out["text"] and "ADD:" not in out["text"] and "TRIM:" not in out["text"]
    assert "ADD.US" not in out["subject"] and "SELECTED.US" not in out["subject"], out["subject"]
    assert out["stderr"].splitlines() == [
        "[brief] WARN could not read _Status: HTTP 503 synthetic Sheets outage",
        "[brief] _Status unreadable - final decision feed treated as WITHHELD(status_unreadable)"], out["stderr"]
    return model


def test_s2_on_unreadable_status_withholds_every_ticket(monkeypatch, capsys):
    model = check_s2(delivered(), monkeypatch, capsys)
    pdf = _pdf_text(delivered(), model)
    assert "Final decision feed withheld" in pdf and "status_unreadable" in pdf
    assert "SELECTED.US" not in pdf and "~700 SAR" not in pdf


@pytest.mark.parametrize("feed", [FEED_EX, FEED_NA, None, "empty"])
def test_s3_on_readable_status_is_unchanged(monkeypatch, capsys, feed):
    grid = [] if feed == "empty" else ([["Page", "Status"], ["Market_Leaders", "OK"]] if feed is None
                                       else status(feed))
    mod = delivered()
    off, _m = run(mod, monkeypatch, capsys, None, Sheets(grids={"_Status": grid}))
    on, _m = run(mod, monkeypatch, capsys, "on", Sheets(grids={"_Status": grid}))
    assert on == off, feed


@pytest.mark.parametrize("page", ["Top_10_Investments", "Portfolio_Decision", "Calendar_Events"])
def test_s4_on_other_failed_pages_are_unchanged(monkeypatch, capsys, page):
    mod = delivered()
    errs = {page: RuntimeError("HTTP 500 synthetic")}
    off, _m = run(mod, monkeypatch, capsys, None, Sheets(errors=errs))
    on, _m = run(mod, monkeypatch, capsys, "on", Sheets(errors=errs))
    assert on == off, page


@pytest.mark.parametrize("exc,name", [
    (HttpError("<HttpError 500 ... key=AIzaSECRET>"), "HttpError"),
    (TimeoutError("read timed out"), "TimeoutError"),
    (ValueError("Unable to parse range: _Status!A1:DZ5000"), "ValueError"),
])
def test_s5_receipt_names_the_exception_type_only(monkeypatch, capsys, exc, name):
    out, _m = run(delivered(), monkeypatch, capsys, "on", Sheets(errors={"_Status": exc}))
    rcpt = out["pages"]["_Status"]
    assert rcpt == [["TFB Decision Feed", "WITHHELD(status_unreadable) | _Status read failed: " + name]]
    assert str(exc) not in rcpt[0][1] and str(exc) not in out["text"] and str(exc) not in out["html"]
    assert out["decision"]["add"] == [] and out["top10"]["top"] == []


# ------------------------------------------------------------- NEVER -------
@pytest.mark.parametrize("gate", [None, "off", "0", "", "  ", "onn", "enforce", "observe"])
def test_s1_off_is_v1200(monkeypatch, capsys, gate):
    b = base()
    for sheets_kw in ({"errors": {"_Status": RuntimeError("HTTP 503 synthetic Sheets outage")}},
                      {}, {"grids": {"_Status": status(FEED_EX)}}):
        mine, _m = run(delivered(), monkeypatch, capsys, gate, Sheets(**sheets_kw))
        theirs, _m = run(b, monkeypatch, capsys, gate, Sheets(**sheets_kw))
        assert mine == theirs, (gate, sheets_kw)
    assert delivered()._status_read_veto_enabled() is False


@pytest.mark.parametrize("gate", ["on", "ON", " on ", "1", "true", "Yes"])
def test_s1b_gate_words(monkeypatch, gate):
    monkeypatch.setenv(GATE, gate)
    assert delivered()._status_read_veto_enabled() is True


@pytest.mark.parametrize("gate", ["off", "0", "", "enforce", "observe", "onn", "y"])
def test_s1c_junk_is_off(monkeypatch, gate):
    monkeypatch.setenv(GATE, gate)
    assert delivered()._status_read_veto_enabled() is False


# ---------------------------------------------------------------- GOLD -----
def test_g1_golden_negative_base_emails_tickets_on_a_failed_read(monkeypatch, capsys):
    b = base()
    out, _m = run(b, monkeypatch, capsys, "on")
    assert out["pages"]["_Status"] == []
    d = out["decision"]
    assert d.get("execution_veto") in (None, "")
    assert [r["symbol"] for r in d["add"]] == ["ADD.US"] and [r["symbol"] for r in d["trim"]] == ["TRIM.US"]
    assert [r["symbol"] for r in d["sell"]] == ["EXIT.US"]
    assert [r["symbol"] for r in out["top10"]["top"]] == ["SELECTED.US"]
    assert "ADD.US" in out["subject"], out["subject"]
    assert "BEST NEW BUYS" in out["text"]
    with pytest.raises(AssertionError):
        check_s2(b, monkeypatch, capsys)


# ------------------------------------------------------------------- V -----
MAPPING = "${{ vars.TFB_BRIEF_STATUS_READ_VETO || 'off' }}"


def test_v1_version_pin_workflow_ci_why():
    mod = delivered()
    assert tuple(int(x) for x in mod.__version__.split(".")) >= (1, 22, 0)
    vd = (ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("run_daily_brief", "%s", "daily brief")' % mod.__version__ in vd
    wf = (ROOT / ".github" / "workflows" / "daily_brief.yml").read_text(encoding="utf-8")
    lines = [ln for ln in wf.splitlines() if GATE + ":" in ln and not ln.lstrip().startswith("#")]
    assert lines == ["          %s: %s" % (GATE, MAPPING)], lines
    env_top = wf.split("\njobs:", 1)[0]
    assert GATE not in env_top                              # step env only
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert re.search(r"python -m pytest -q tests/test_daily_brief_status_read_veto\.py", ci)
    src = Path(mod.__file__).read_text(encoding="utf-8")
    i22 = src.index("# v1.22.0 (2026-11-02) -- [STATUS-READ-VETO]")
    assert src.index("# v1.20.0 (2026-10-28) -- [SEND-WINDOW]") < i22 < src.index("__version__ = ")
    assert all(ord(c) < 128 for c in Path(__file__).read_text(encoding="utf-8"))


def test_v1b_against_the_base_ascii_and_functions():
    import ast
    import difflib
    b = base()
    mod = delivered()
    if mod.__version__ != "1.22.0":
        pytest.skip("the exact 3bbf3c80 -> 1.22.0 diff checks apply to 1.22.0 only")
    old = Path(b.__file__).read_text(encoding="utf-8").splitlines()
    new = Path(mod.__file__).read_text(encoding="utf-8").splitlines()
    diff = list(difflib.unified_diff(old, new, lineterm="", n=0))
    added = [ln[1:] for ln in diff if ln.startswith("+") and not ln.startswith("+++")]
    removed = [ln[1:] for ln in diff if ln.startswith("-") and not ln.startswith("---")]
    assert added and all(ln.isascii() for ln in added), [ln for ln in added if not ln.isascii()][:3]
    assert removed == ['__version__ = "1.20.0"'], removed
    kinds = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)

    def defs(text):
        return {n.name for n in ast.walk(ast.parse(text)) if isinstance(n, kinds)}
    assert defs("\n".join(new)) - defs("\n".join(old)) == {"_status_read_veto_enabled"}
    assert not defs("\n".join(old)) - defs("\n".join(new))


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
    run_n = tally.passed + tally.failed
    print("PASS %d/%d" % (tally.passed, run_n) + (" (%d skipped)" % tally.skipped if tally.skipped else ""))
    sys.exit(rc)
