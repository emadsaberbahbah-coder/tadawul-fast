"""run_daily_brief v1.19.0 [POST-EARNINGS-HELD] and 16_Decision_Top10 v1.18.0.

The cockpit puts '\u26a0 post-earnings S/L \u00b7 EPS x% \u00b7 ' on the Advisor
Note of a SELECTED seat inside its post-earnings quarantine (interim trading
policy rule 3) while DT10_POST_EARN is armed - after the forward earnings tag
when the note carries one. The brief keeps such a seat out of its executable
picks (it never was a pick: the note does not begin with INVEST) and, since
v1.19.0, names it in one line on all three surfaces instead of dropping it
silently (independent review of PR #855). With no tag on the sheet every
output is the untouched v1.18.1 (git show e8337a16) byte for byte.
Offline; no network.
"""
import datetime as dt
import importlib.util
import io
from pathlib import Path
import subprocess

import pytest

from scripts import run_daily_brief as brief

ROOT = Path(__file__).resolve().parents[1]
BASE_REV = "e8337a16"   # main with run_daily_brief v1.18.1
WHEN = dt.datetime(2026, 10, 27, 10, 30)
POST = "\u26a0 post-earnings 3/10 \u00b7 EPS -22.2% \u00b7 "
FWD = "\u26a0 earnings \u22647d \u00b7 "
INVEST = "INVEST \u2014 6,000 SAR (400 sh)"
HDR = ["Rank", "Symbol", "Name", "Market", "Sector", "Ccy", "Shares", "ROI %", "Rel", "Conf",
       "Stability", "Advisor Note"]


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("TFB_BRIEF_NEWS_CONTEXT", "0")
    monkeypatch.setenv("TFB_MIN_TICKET_SAR", "0")


def _base_brief():
    try:
        blob = subprocess.run(["git", "-C", str(ROOT), "show", BASE_REV + ":scripts/run_daily_brief.py"],
                              capture_output=True, timeout=60)
    except Exception:
        return None
    if blob.returncode != 0 or b'__version__ = "1.18.1"' not in blob.stdout:
        return None
    tmp = ROOT / "scripts" / "_pytest_brief_v1181_base.py"
    tmp.write_bytes(blob.stdout)
    try:
        spec = importlib.util.spec_from_file_location("scripts._pytest_brief_v1181_base", tmp)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    finally:
        tmp.unlink()


def seat(rank, sym, note, shares=400, stability="ACTIVE 6d"):
    return [rank, sym, sym + " Inc", "US", "Financials", "USD", shares, 18.0 + rank, 76, "High", stability, note]


def grid(*rows):
    return [["SELECTED (%d)" % len(rows)], HDR] + list(rows)


TAGGED = grid(seat(1, "FRST.US", POST + INVEST, shares=174),
              seat(2, "OPBK.US", INVEST),
              seat(3, "WPC.US", FWD + "\u26a0 post-earnings 1/5 \u00b7 EPS n/a \u00b7 " + INVEST),
              seat(4, "GRC.US", POST + INVEST, stability="GRACE (1/2 missed)"),
              seat(5, "ZER.US", POST + INVEST, shares=0),
              seat(6, "WHD.US", POST + "SIZING WITHHELD \u2014 feed NOT_ACTIONABLE (x)"),
              seat(7, "HLD.US", POST + INVEST))
PLAIN = grid(seat(1, "FRST.US", INVEST, shares=174), seat(2, "OPBK.US", INVEST),
             seat(3, "WPC.US", FWD + INVEST), seat(4, "GRC.US", INVEST, stability="GRACE (1/2 missed)"))


def test_untagged_and_forward_tagged_invest_notes_are_picks():
    assert brief._positive_invest_note(INVEST)
    assert brief._positive_invest_note(FWD + INVEST)
    assert brief._post_earnings_note(INVEST) is None and brief._post_earnings_note(FWD + INVEST) is None


def test_post_earnings_notes_parse_and_are_not_picks_by_themselves():
    for note in (POST + INVEST, FWD + POST + INVEST, "\u26a0 post-earnings 5/5 \u00b7 EPS n/a \u00b7 INVEST"):
        assert not brief._positive_invest_note(note), note
    assert brief._post_earnings_note(POST + INVEST) == {"sessions": 3, "limit": 10, "eps": "-22.2%", "rest": INVEST}
    assert brief._post_earnings_note(FWD + POST + INVEST) == {"sessions": 3, "limit": 10, "eps": "-22.2%",
                                                              "rest": FWD + INVEST}


def test_extract_top10_holds_the_tagged_seats_back_and_names_them():
    out = brief.extract_top10(TAGGED, exclude={"HLD.US"})
    assert [p["symbol"] for p in out["top"]] == ["OPBK.US"]
    # GRACE, zero shares, a withheld note and a held name were never picks -> not named
    assert out["post_earnings_held"] == [
        {"symbol": "FRST.US", "sessions": 3, "limit": 10, "eps": "-22.2%"},
        {"symbol": "WPC.US", "sessions": 1, "limit": 5, "eps": "n/a"}]
    line = brief._post_earnings_held_line({"top10": out})
    assert line == ("2 candidate(s) held back \u2014 reported earnings recently (post-earnings quarantine, "
                    "interim rule 3): FRST.US (session 3 of 10, EPS -22.2%), WPC.US (session 1 of 5, EPS n/a). "
                    "They re-enter automatically when the quarantine ends.")
    assert "post_earnings_held" not in brief.extract_top10(PLAIN) and brief._post_earnings_held_line({"top10": {}}) == ""


def _norm(text, mod):
    return text.replace(mod.__version__, "V")


def test_golden_untagged_board_is_v1181_byte_for_byte():
    base = _base_brief()
    if base is None:
        pytest.skip("git show %s unavailable (shallow clone)" % BASE_REV)
    for rows in (PLAIN, TAGGED[:2] + TAGGED[3:4], []):
        assert brief.extract_top10(rows) == base.extract_top10(rows)
        pages = {brief.PAGE_TOP10: rows}
        new_m, old_m = brief.build_model(pages), base.build_model(pages)
        assert _norm(brief.render_text(new_m, "Synthetic owner", WHEN), brief) == \
            _norm(base.render_text(old_m, "Synthetic owner", WHEN), base)
        assert _norm(brief.render_html(new_m, "Synthetic owner", WHEN), brief) == \
            _norm(base.render_html(old_m, "Synthetic owner", WHEN), base)


def test_tagged_board_named_on_text_and_html_not_in_v1181():
    pages = {brief.PAGE_TOP10: TAGGED}
    model = brief.build_model(pages)
    text = brief.render_text(model, "Synthetic owner", WHEN)
    html = brief.render_html(model, "Synthetic owner", WHEN)
    assert "held back \u2014 reported earnings recently" in text and "FRST.US (session 3 of 10, EPS -22.2%)" in text
    assert "reported earnings recently" in html and "FRST.US (session 3 of 10, EPS -22.2%)" in html
    base = _base_brief()
    if base is not None:   # golden negative: v1.18.1 drops the seats without a word
        old = base.render_text(base.build_model(pages), "Synthetic owner", WHEN)
        assert "FRST.US" not in old and "reported earnings recently" not in old


def test_tagged_board_named_in_the_pdf():
    pytest.importorskip("reportlab")
    PdfReader = pytest.importorskip("pypdf").PdfReader
    pdf = brief.render_pdf(brief.build_model({brief.PAGE_TOP10: TAGGED}), "Synthetic owner", WHEN)
    assert pdf
    text = " ".join(" ".join(page.extract_text() for page in PdfReader(io.BytesIO(pdf)).pages).split())
    assert "reported earnings recently" in text and "FRST.US (session 3 of 10, EPS -22.2%)" in text
