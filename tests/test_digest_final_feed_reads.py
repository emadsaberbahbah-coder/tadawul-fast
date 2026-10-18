"""A failed final-feed read cannot become a legacy blank or authorize picks.

Run the real manifest, page readers, selection and rendering against memory-only
Sheets data. Capture the send boundary and deny network access in every case.
"""
import copy
import datetime as dt
import socket
from types import SimpleNamespace

import pytest

from scripts import send_digest as digest


MANIFEST_RANGE = "'_Status'!A1:J400"
FEED_RANGE = "'_Status'!L1:M60"
PRIVATE_FAILURE = "synthetic private response api_token=must-not-appear"
HEADER = ["Symbol", "Name", "Final Action", "Investable", "Block Reason",
          "Overall Score", "Current Price", "Expected ROI 12m",
          "Last Updated (Riyadh)"]


class FrozenDatetime(dt.datetime):
    @classmethod
    def now(cls, tz=None):
        fixed = cls(2026, 10, 18, 10, 0, tzinfo=dt.timezone.utc)
        return fixed.astimezone(tz) if tz else fixed.replace(tzinfo=None)


@pytest.fixture
def live_digest(monkeypatch):
    for key, value in {
        "DIGEST_ENABLE": "1", "DIGEST_REQUIRE_MANIFEST": "1",
        "DIGEST_STATE_BRIEF": "1", "DIGEST_STATE_EXIT": "0",
        "DIGEST_CANDIDATE_PAGES": "Market_Leaders",
        "DIGEST_HOLDINGS_PAGE": "My_Portfolio",
        "DIGEST_MANIFEST_PAGES": "Market_Leaders,Global_Markets,Commodities_FX,Mutual_Funds",
        "DIGEST_MANIFEST_TTL_H": "6", "DIGEST_SKIP_IF_EMPTY": "0",
        "DIGEST_LEDGER_FILTER": "1", "DIGEST_LEDGER_PAGE": "_Portfolio_CostBasis",
        "DIGEST_EXCLUDE_HELD": "1", "DIGEST_STALE_HOURS": "24",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(digest, "datetime", FrozenDatetime)
    monkeypatch.setitem(digest._MANIFEST_NOTE, "line", "")
    stamp = FrozenDatetime.now(digest.RIYADH_TZ).strftime("%Y-%m-%d %H:%M:%S")
    state = SimpleNamespace(
        globals=[["TFB Decision Feed", "EXECUTABLE | run=synthetic-run"]],
        error=None, reads=[], sent=[],
    )
    manifest = [[stamp, page, "sync data=COMPLETE run=synthetic-run"]
                for page in digest._manifest_pages()]
    candidates = [HEADER, ["SYNTH.US", "Synthetic candidate", "INVEST", "YES", "",
                           90, 25, 12, stamp]]

    def read_range(sheet_id, range_name):
        assert sheet_id == "synthetic-sheet"
        state.reads.append(range_name)
        if range_name == MANIFEST_RANGE:
            return copy.deepcopy(manifest)
        if range_name == FEED_RANGE:
            if state.error is not None:
                raise state.error
            return copy.deepcopy(state.globals)
        if range_name.startswith("'Market_Leaders'!"):
            return copy.deepcopy(candidates)
        if range_name.startswith("'My_Portfolio'!"):
            return [HEADER]
        return []

    def send_email(subject, text, html, dry_run):
        state.sent.append(dict(subject=subject, text=text, html=html, dry_run=dry_run))
        return 0

    def unexpected_network(*_args, **_kwargs):
        raise AssertionError("digest regression attempted network I/O")

    monkeypatch.setattr(digest, "_get_read_range", lambda: read_range)
    monkeypatch.setattr(digest, "_send_email", send_email)
    monkeypatch.setattr(socket, "create_connection", unexpected_network)
    monkeypatch.setattr(socket.socket, "connect", unexpected_network)
    monkeypatch.setattr(socket.socket, "connect_ex", unexpected_network)
    return state


def run_digest(*extra_args):
    return digest.main(["--sheet-id", "synthetic-sheet", "--session", "synthetic session",
                        *extra_args])


@pytest.mark.parametrize("extra_args", [[], ["--dry-run"]])
@pytest.mark.parametrize("error_type", [TimeoutError, PermissionError])
def test_failed_final_feed_read_stops_before_picks_rendering_or_smtp(
    live_digest, capsys, extra_args, error_type,
):
    live_digest.error = error_type(PRIVATE_FAILURE)

    assert run_digest(*extra_args) == 2

    assert live_digest.sent == []
    assert live_digest.reads == [MANIFEST_RANGE, FEED_RANGE]
    output = capsys.readouterr().out
    assert "final decision feed could not be read" in output
    assert error_type.__name__ in output
    assert "no digest generated or sent" in output
    assert "ACTIONABLE:" not in output
    assert PRIVATE_FAILURE not in output and "SYNTH.US" not in output


@pytest.mark.parametrize("legacy_globals", [
    [], None, [["Legacy key", "OK"]], [["TFB Decision Feed", ""]],
])
def test_successful_legacy_absence_preserves_manifest_only_picks(
    live_digest, legacy_globals,
):
    live_digest.globals = []
    assert run_digest() == 0
    baseline = live_digest.sent.pop()
    assert "SYNTH.US" in baseline["text"]
    assert "DECISION SURFACE: EXECUTABLE" in baseline["text"]
    assert "feed: UNKNOWN" in baseline["text"]

    live_digest.globals = legacy_globals
    assert run_digest() == 0
    assert live_digest.sent == [baseline]


def test_readable_final_veto_still_stops_before_picks(live_digest):
    live_digest.globals = [["TFB Decision Feed", "NOT_ACTIONABLE(final_publication) | run=synthetic-run"]]

    assert run_digest() == 0

    assert len(live_digest.sent) == 1
    sent = live_digest.sent[0]
    assert "NOT ACTIONABLE (final_publication)" in sent["text"]
    assert "SYNTH.US" not in sent["text"] + sent["html"]
    assert live_digest.reads == [MANIFEST_RANGE, FEED_RANGE, "'Top_10_Investments'!A1:L60"]


def test_readable_executable_feed_preserves_normal_digest(live_digest):
    assert run_digest() == 0
    assert len(live_digest.sent) == 1
    assert "SYNTH.US" in live_digest.sent[0]["text"]
    assert "feed: EXECUTABLE" in live_digest.sent[0]["text"]
