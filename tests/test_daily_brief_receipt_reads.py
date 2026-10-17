"""Receipt I/O failure cannot erase a final veto before preview or delivery.

Use the real page reader, model, renderers and CLI with a memory-only Sheets
adapter and captured SMTP. A successful legacy blank remains valid input;
an exception reading the receipt must stop before creating a new artifact.
"""
import copy
import datetime as dt
import socket
import sys
from types import ModuleType, SimpleNamespace

import pytest

from scripts import run_daily_brief as brief

HEADER = ["Action", "Symbol", "Name", "P&L %", "Weight %", "Engine ROI %",
          "Rel", "Conf", "Advisor Note", "MV SAR", "Cost SAR"]
DECISION = [HEADER, ["EXIT", "SYNTH.US", "Synthetic holding", 10, None,
                     None, None, "", "Full exit ~500 SAR", 1_100, 1_000]]
PRIVATE_FAILURE = "synthetic private response api_token=must-not-appear"
WHEN = dt.datetime(2026, 10, 17, 8, 0, tzinfo=dt.timezone.utc)


@pytest.fixture
def live_brief(monkeypatch):
    state = SimpleNamespace(
        pages={brief.PAGE_DECISION: copy.deepcopy(DECISION),
               brief.PAGE_STATUS: [["TFB Decision Feed", "EXECUTABLE | run=90001"]]},
        read_errors={}, reads=[], sent=[],
    )

    def read_range(sheet_id, range_name):
        assert sheet_id == "synthetic-sheet"
        page = range_name.split("!", 1)[0]
        state.reads.append(page)
        if page in state.read_errors:
            raise state.read_errors[page]
        return copy.deepcopy(state.pages.get(page, []))

    sheets = ModuleType("integrations.google_sheets_service")
    sheets.read_range = read_range
    monkeypatch.setitem(sys.modules, sheets.__name__, sheets)

    def send_email(html_body, text_body, subject, **kwargs):
        state.sent.append({"html": html_body, "text": text_body,
                           "subject": subject, **kwargs})

    def unexpected_network(*_args, **_kwargs):
        raise AssertionError("brief regression attempted network I/O")

    monkeypatch.setattr(brief, "send_email", send_email)
    monkeypatch.setattr(socket, "create_connection", unexpected_network)
    monkeypatch.setattr(socket.socket, "connect", unexpected_network)
    monkeypatch.setattr(socket.socket, "connect_ex", unexpected_network)
    monkeypatch.setenv("DEFAULT_SPREADSHEET_ID", "synthetic-sheet")
    monkeypatch.setenv("TFB_BRIEF_OWNER_NAME", "Synthetic owner")
    monkeypatch.setenv("TFB_BRIEF_NEWS_CONTEXT", "0")
    monkeypatch.setenv("TFB_BRIEF_PDF", "0")
    monkeypatch.setenv("TFB_MIN_TICKET_SAR", "0")
    monkeypatch.delenv("TFB_BRIEF_LOCAL_XLSX", raising=False)
    monkeypatch.delenv("TFB_BRIEF_SEND", raising=False)
    return state


@pytest.mark.parametrize("feed", (
    "NOT_ACTIONABLE(final_publication) | run=90001 | synthetic audit failure",
    "WITHHELD | synthetic final receipt",
))
def test_readable_veto_still_overrides_the_same_executable_decision(
    live_brief, tmp_path, feed,
):
    live_brief.pages[brief.PAGE_STATUS] = [["TFB Decision Feed", feed]]
    output = tmp_path / "brief.html"
    assert brief.main(["--send", "--out", str(output)]) == 0
    assert len(live_brief.sent) == 1
    sent = live_brief.sent[0]
    assert "Final decision feed withheld execution" in sent["text"]
    assert feed in sent["text"]
    assert not any(line.startswith("EXIT:") for line in sent["text"].splitlines())
    assert ">Act now " not in sent["html"]
    assert output.read_text() == sent["html"]


@pytest.mark.parametrize("delivery", ("send-flag", "send-env", "preview"))
def test_unreadable_receipt_stops_before_artifacts_or_smtp(
    live_brief, monkeypatch, tmp_path, capsys, delivery,
):
    # This same decision is executable when the receipt read succeeds. A read
    # failure must not manufacture a legacy blank or a replacement HOLD/EXIT.
    live_brief.read_errors[brief.PAGE_STATUS] = OSError(PRIVATE_FAILURE)
    args = ["--out", str(tmp_path / "unavailable.html")]
    if delivery == "send-flag":
        args.append("--send")
    elif delivery == "send-env":
        monkeypatch.setenv("TFB_BRIEF_SEND", "1")

    assert brief.main(args) == 2
    assert live_brief.sent == []
    assert brief.PAGE_DECISION in live_brief.reads
    assert brief.PAGE_STATUS in live_brief.reads
    assert not (tmp_path / "unavailable.html").exists()
    assert not (tmp_path / "unavailable.pdf").exists()
    captured = capsys.readouterr()
    assert "_Status final decision receipt could not be read (OSError)" in captured.err
    assert "no brief generated or sent" in captured.err
    assert PRIVATE_FAILURE not in captured.err + captured.out
    assert "SYNTH.US" not in captured.err + captured.out


def test_page_reader_preserves_read_error_identity_without_private_details(live_brief):
    live_brief.read_errors[brief.PAGE_STATUS] = PermissionError(PRIVATE_FAILURE)
    with pytest.raises(brief.DecisionReceiptReadError) as exc:
        brief.read_pages_live("synthetic-sheet", brief.ALL_PAGES)
    assert str(exc.value) == "PermissionError"
    assert PRIVATE_FAILURE not in str(exc.value)


@pytest.mark.parametrize("legacy_status", (
    [],
    [["Legacy page", "OK"]],
    [["TFB Decision Feed", ""]],
    None,
))
def test_successful_legacy_receipt_absence_preserves_model_and_normal_send(
    live_brief, tmp_path, legacy_status,
):
    live_brief.pages[brief.PAGE_STATUS] = legacy_status
    pages = brief.read_pages_live("synthetic-sheet", brief.ALL_PAGES)
    actual = brief.build_model(pages)
    legacy = brief.build_model({brief.PAGE_DECISION: copy.deepcopy(DECISION)})
    assert actual == legacy
    assert brief.render_text(actual, "Synthetic owner", WHEN) == brief.render_text(
        legacy, "Synthetic owner", WHEN)
    assert brief.render_html(actual, "Synthetic owner", WHEN) == brief.render_html(
        legacy, "Synthetic owner", WHEN)
    assert [r["symbol"] for r in actual["decision"]["sell"]] == ["SYNTH.US"]
    assert "execution_veto" not in actual["decision"]

    assert brief.main(["--send", "--out", str(tmp_path / "legacy.html")]) == 0
    assert len(live_brief.sent) == 1
    assert "EXIT:  SYNTH.US (+10.0%)" in live_brief.sent[0]["text"]


def test_readable_executable_receipt_preserves_normal_send(live_brief, tmp_path):
    output = tmp_path / "normal.html"
    assert brief.main(["--send", "--out", str(output)]) == 0
    assert len(live_brief.sent) == 1
    assert "EXIT:  SYNTH.US (+10.0%)" in live_brief.sent[0]["text"]
    assert "Final decision feed withheld" not in live_brief.sent[0]["text"]
    assert output.read_text() == live_brief.sent[0]["html"]


def test_readable_dry_preview_renders_without_smtp(live_brief, tmp_path):
    output = tmp_path / "preview.html"
    assert brief.main(["--out", str(output)]) == 0
    assert output.is_file()
    assert "SYNTH.US" in output.read_text()
    assert live_brief.sent == []


def test_known_absent_legacy_xlsx_receipt_keeps_its_existing_path(
    live_brief, monkeypatch, tmp_path,
):
    # read_pages_xlsx already establishes genuine worksheet absence using the
    # workbook's sheet list; the new live I/O error must not change that path.
    monkeypatch.setattr(brief, "read_pages_xlsx", lambda *_args: {
        brief.PAGE_DECISION: copy.deepcopy(DECISION),
    })
    assert brief.main(["--xlsx", "synthetic.xlsx", "--send",
                       "--out", str(tmp_path / "legacy-xlsx.html")]) == 0
    assert live_brief.reads == []
    assert len(live_brief.sent) == 1
    assert "EXIT:  SYNTH.US (+10.0%)" in live_brief.sent[0]["text"]


def test_optional_page_read_failure_keeps_existing_degraded_send(live_brief, tmp_path):
    live_brief.read_errors[brief.PAGE_CALENDAR] = OSError("synthetic optional page unavailable")
    assert brief.main(["--send", "--out", str(tmp_path / "optional.html")]) == 0
    assert len(live_brief.sent) == 1
    assert "EXIT:  SYNTH.US (+10.0%)" in live_brief.sent[0]["text"]
