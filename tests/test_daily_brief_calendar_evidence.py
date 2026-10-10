"""Real brief calendar parsing, date guard, displays and bounded live reads.

All sheet transports are fixtures. No email, Google or provider calls occur.
"""
from __future__ import annotations

from datetime import datetime, timezone
import sys
from types import ModuleType, SimpleNamespace

import pytest

from core.calendar_evidence import CALENDAR_HEADERS
from scripts import run_daily_brief as brief


WHEN = datetime(2026, 10, 10, 9, 30, tzinfo=timezone.utc)
OBSERVED = "2026-10-09T08:15:02.123456Z"


def event_row(symbol="SYNTH.US", event="2026-10-14", source="yahoo", status="estimated", observed=OBSERVED):
    return [symbol, event, 999, "2026-10-21", 999, "2026-10-10 12:00", "issuer confirmed row summary",
            source, observed, status, "eodhd", OBSERVED, "reported"]


def calendar(*rows, headers=CALENDAR_HEADERS):
    return brief._extract_calendar([list(headers), *rows])


def candidate(symbol="SYNTH.US", note="⚠ earnings ≤0d · INVEST"):
    return {"symbol": symbol, "note": note, "suggested_shares": 20, "suggested_sar": 7500}


@pytest.fixture(autouse=True)
def isolation(monkeypatch):
    monkeypatch.setenv("TFB_BRIEF_EARNINGS_FLAG_DAYS", "7")
    monkeypatch.setenv("TFB_BRIEF_EARNINGS_ON_ACTIONS", "1")
    monkeypatch.setenv("TFB_BRIEF_CANDIDATE_EARNINGS_GUARD", "1")
    monkeypatch.setenv("TFB_BRIEF_NEWS_CONTEXT", "0")


def test_real_parser_preserves_independent_vendor_metadata_and_legacy_date_keys():
    info = calendar(event_row())["SYNTH.US"]
    assert info["earn"] == "2026-10-14" and info["days"] is None
    assert (info["earnings_source"], info["earnings_observed_at"], info["earnings_status"]) == \
        ("yahoo", OBSERVED, "estimated")
    assert (info["exdiv_source"], info["exdiv_observed_at"], info["exdiv_status"]) == \
        ("eodhd", OBSERVED, "reported")


@pytest.mark.parametrize("source,status,label", [
    ("yahoo", "estimated", "estimated by yahoo"),
    ("eodhd", "reported", "reported by eodhd"),
])
def test_warnings_and_existing_presentation_guard_retain_truthful_evidence(source, status, label):
    ctx = calendar(event_row(source=source, status=status))
    flag = brief._earnings_flag_for("SYNTH.US", ctx, WHEN)
    assert flag == f"Earnings 2026-10-14 (in 4d; {label})"
    original = {"top": [candidate()], "rest": {"US": [candidate()]}}
    output, held = brief._filter_candidates_earnings(original, ctx, WHEN)
    assert output["top"] == [] and output["rest"] == {}
    assert [row["evidence"] for row in held] == [label, label]
    assert original["top"][0]["suggested_sar"] == 7500
    assert original["top"][0]["suggested_shares"] == 20
    held_line = brief._earnings_held_line({"top10": output})
    assert label in held_line and "date passes or is revised" in held_line
    assert "after reporting" not in held_line
    model = {"calendar": ctx, "news_ctx": {}}
    assert label in brief._ticket_flags_html("SYNTH.US", model, WHEN)
    chip = brief._action_earn_chip("SYNTH.US", model, WHEN)
    assert label in chip and "calendar context, not a signal" in chip
    assert "confirmed" not in flag + chip


def test_legacy_date_remains_conservative_with_unknown_observation_not_row_publication():
    info = calendar(event_row()[:7], headers=CALENDAR_HEADERS[:7])["SYNTH.US"]
    assert info["earnings_source"] == "unknown" and info["earnings_observed_at"] == ""
    assert "evidence unknown" in brief._earnings_flag_for("SYNTH.US", {"SYNTH.US": info}, WHEN)
    assert "issuer" not in brief._calendar_earnings_label(info)


@pytest.mark.parametrize("event", ["", None, "unknown", "2026-02-30", "2026-10-14junk",
                                    "2026-10-14T12:00:00Z", "2026-10-09", "2026-10-22"])
def test_unknown_invalid_expired_or_far_date_never_uses_positive_sheet_or_note_countdown(event):
    row = event_row(event=event); row[2] = 1
    ctx = calendar(row)
    assert brief._earnings_flag_for("SYNTH.US", ctx, WHEN) is None
    original = {"top": [candidate()], "rest": {}}
    output, held = brief._filter_candidates_earnings(original, ctx, WHEN)
    assert output["top"] == original["top"] and not held


def test_aware_review_clock_uses_riyadh_midnight_and_not_utc_day():
    ctx = calendar(event_row(event="2026-10-11"))
    before = datetime(2026, 10, 10, 20, 59, tzinfo=timezone.utc)
    after = datetime(2026, 10, 10, 21, 0, tzinfo=timezone.utc)
    assert "in 1d" in brief._earnings_flag_for("SYNTH.US", ctx, before)
    assert "in 0d" in brief._earnings_flag_for("SYNTH.US", ctx, after)


@pytest.mark.parametrize("source,status,observed", [
    ("issuer", "reported", OBSERVED), ("eodhd", "confirmed", OBSERVED),
    ("eodhd", "estimated", OBSERVED), ("yahoo", "estimated", ""),
    ("yahoo", "estimated", "2026-10-09"), ("eodhd", "reported", "2026-10-09T08:15:00"),
])
def test_unsupported_or_incomplete_claim_is_unknown_without_suppressing_known_date(source, status, observed):
    ctx = calendar(event_row(source=source, status=status, observed=observed))
    assert "in 4d; evidence unknown" in brief._earnings_flag_for("SYNTH.US", ctx, WHEN)


def test_conflicting_duplicate_dates_or_headers_never_choose_a_warning():
    assert "SYNTH.US" not in calendar(event_row(), event_row(event="2026-10-15"))
    assert calendar(event_row(), headers=[*CALENDAR_HEADERS, "Earnings Source"]) == {}


def test_finite_native_metadata_prefix_does_not_displace_invest_or_change_note_day_parser():
    note = "⚠ earnings ≤4d · [estimated: yahoo] · " + "review " * 4 + "INVEST"
    headers = ["Symbol", "Name", "Advisor Note"]
    top10 = brief.extract_top10([headers, ["SYNTH.US", "Synthetic", note]])
    assert top10["top"][0]["symbol"] == "SYNTH.US"
    assert brief._note_earn_days(note) == 4


def test_actual_html_and_text_displays_label_estimate_and_do_not_assert_confirmation():
    model = brief.build_model({brief.PAGE_CALENDAR: [CALENDAR_HEADERS, event_row()],
        brief.PAGE_DECISION: [["Action", "Symbol", "Name", "Advisor Note", "MV SAR", "Cost SAR"],
                             ["ADD", "SYNTH.US", "Synthetic", "Add 20 shares (~7500 SAR)", 10000, 9000]]})
    # Coherence may demote an action without a full market fixture; both paths
    # render the actual shared calendar through the production surfaces.
    html = brief.render_html(model, "Synthetic operator", WHEN)
    text = brief.render_text(model, "Synthetic operator", WHEN)
    assert "estimated by yahoo" in html and "estimated by yahoo" in text
    assert "calendar context, not a signal" in html + text
    assert "issuer confirmed" not in html + text


def test_actual_text_retains_evidence_when_every_candidate_is_held():
    model = brief.build_model({brief.PAGE_CALENDAR: [CALENDAR_HEADERS, event_row()],
        brief.PAGE_DECISION: [["Action", "Symbol", "MV SAR", "Cost SAR"], ["HOLD", "OTHER.US", 10000, 9000]]})
    top10, _ = brief._filter_candidates_earnings({"top": [candidate()], "rest": {}}, model["calendar"], WHEN)
    model["top10"] = top10
    assert "estimated by yahoo" in brief.render_text(model, "Synthetic operator", WHEN)


def install_sheet_transport(monkeypatch, rows=5001, cols=13, *, metadata_error=None):
    calls = []
    service = ModuleType("integrations.google_sheets_service")
    def get(**kwargs):
        calls.append(("metadata", kwargs))
        def execute():
            if metadata_error:
                raise metadata_error
            return {"sheets": [{"properties": {"title": "Calendar_Events", "gridProperties":
                                                {"rowCount": rows, "columnCount": cols}}}]}
        return SimpleNamespace(execute=execute)
    service.get_sheets_service = lambda: SimpleNamespace(spreadsheets=lambda: SimpleNamespace(get=get))
    def read(sheet_id, range_name):
        calls.append(("read", sheet_id, range_name))
        return [CALENDAR_HEADERS, event_row()]
    service.read_range = read
    monkeypatch.setitem(sys.modules, "integrations.google_sheets_service", service)
    return calls


@pytest.mark.parametrize("legacy_cap", ["0", "1"])
def test_actual_live_calendar_read_uses_complete_thirteen_column_extent_independent_of_market_cap(monkeypatch, legacy_cap):
    calls = install_sheet_transport(monkeypatch)
    monkeypatch.setenv("TFB_SYNC_PAGE_READ_MAX_ROW", "1000")
    monkeypatch.setenv("TFB_SYNC_UNIVERSE_CAP_V2", legacy_cap)
    pages = brief.read_pages_live("synthetic-sheet", [brief.PAGE_CALENDAR, "Market_Leaders"])
    reads = [call[2] for call in calls if call[0] == "read"]
    assert reads[0] == "Calendar_Events!A1:M5001"
    assert reads[1] == ("Market_Leaders!A1:DZ5000" if legacy_cap == "0" else "Market_Leaders!A1:DZ1000")
    assert pages[brief.PAGE_CALENDAR] and brief._extract_calendar(pages[brief.PAGE_CALENDAR])


def test_actual_live_legacy_calendar_read_never_requests_nonexistent_metadata_columns(monkeypatch):
    calls = install_sheet_transport(monkeypatch, rows=1000, cols=7)
    brief.read_pages_live("synthetic-sheet", [brief.PAGE_CALENDAR])
    assert [call[2] for call in calls if call[0] == "read"] == ["Calendar_Events!A1:G1000"]


@pytest.mark.parametrize("rows,cols", [(5002, 13), (True, 13), (None, 13), (1000, 6), (1000, None)])
def test_unknown_or_oversized_live_extent_is_unknown_without_reading_a_prefix(monkeypatch, rows, cols):
    calls = install_sheet_transport(monkeypatch, rows=rows, cols=cols)
    assert brief.read_pages_live("synthetic-sheet", [brief.PAGE_CALENDAR])[brief.PAGE_CALENDAR] == []
    assert not [call for call in calls if call[0] == "read"]


def test_metadata_service_failure_is_unknown_without_a_truncated_calendar_fallback(monkeypatch):
    calls = install_sheet_transport(monkeypatch, metadata_error=RuntimeError("synthetic unavailable"))
    assert brief.read_pages_live("synthetic-sheet", [brief.PAGE_CALENDAR])[brief.PAGE_CALENDAR] == []
    assert not [call for call in calls if call[0] == "read"]
