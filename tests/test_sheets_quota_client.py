"""sheets_backoff 1.0.0 + run_calendar_sync 1.3.1: every request of a page job waits out the Sheets per-minute quota
(full audit of 25 Nov 2026: five hourly page runs ended red on HTTP 429 in 36 hours, one of them on the workbook open,
which no job retried). No network: gspread's HTTPClient.request is replaced by a scripted fake."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# The lean decision-layer suite installs neither requests nor gspread: skip there, run in the whole suite.
requests = pytest.importorskip("requests")
gspread = pytest.importorskip("gspread")
from gspread.exceptions import APIError  # noqa: E402
from gspread.http_client import HTTPClient  # noqa: E402

from scripts import run_calendar_sync as cal  # noqa: E402
from scripts import sheets_backoff as sq  # noqa: E402


def api_error(code, text="Quota exceeded for quota metric 'Read requests'"):
    r = requests.models.Response()
    r.status_code = code
    r._content = ('{"error": {"code": %d, "message": "%s", "status": "X"}}' % (code, text)).encode()
    return APIError(r)


class Script:
    def __init__(self, answers):
        self.answers, self.calls = list(answers), 0

    def __call__(self, _self, *a, **k):
        self.calls += 1
        ans = self.answers.pop(0)
        if isinstance(ans, BaseException):
            raise ans
        return ans


@pytest.fixture
def waits(monkeypatch):
    seen = []
    monkeypatch.setattr(sq, "_SLEEP", seen.append)
    monkeypatch.setattr(sq, "_RANDOM", lambda: 0.5)
    return seen


def client():
    cls = sq.quota_http_client()
    assert issubclass(cls, HTTPClient)
    return cls.__new__(cls)          # no session or auth is needed for request(): the parent is scripted


def test_q1_a_quota_answer_is_waited_out_and_tried_again(monkeypatch, waits):
    fake = Script([api_error(429), api_error(429), "ok"])
    monkeypatch.setattr(HTTPClient, "request", fake)
    assert client().request("get", "u") == "ok"
    assert fake.calls == 3 and waits == [75.0, 75.0]          # 65 s + 0.5 x 20 s, twice


def test_q2_at_most_three_more_tries_then_the_quota_answer_is_raised(monkeypatch, waits):
    fake = Script([api_error(429)] * 4)
    monkeypatch.setattr(HTTPClient, "request", fake)
    with pytest.raises(APIError):
        client().request("get", "u")
    assert fake.calls == 4 and len(waits) == 3


@pytest.mark.parametrize("code", [400, 403, 404, 500, 503])
def test_q3_every_other_answer_is_raised_at_once(monkeypatch, waits, code):
    fake = Script([api_error(code, "something else")])
    monkeypatch.setattr(HTTPClient, "request", fake)
    with pytest.raises(APIError):
        client().request("post", "u")
    assert fake.calls == 1 and waits == []                      # a write is never sent twice after a 5xx


def test_q4_the_wait_spreads_with_the_jitter(monkeypatch):
    seen = []
    monkeypatch.setattr(sq, "_SLEEP", seen.append)
    vals = iter([0.0, 1.0])
    monkeypatch.setattr(sq, "_RANDOM", lambda: next(vals))
    monkeypatch.setattr(HTTPClient, "request", Script([api_error(429), api_error(429), "ok"]))
    client().request("get", "u")
    assert seen == [65.0, 85.0]


@pytest.mark.parametrize("val,on", [("", True), ("on", True), ("off", False), ("0", False), (" No ", False),
                                    ("false", False)])
def test_q5_the_rollback_spellings(monkeypatch, val, on):
    monkeypatch.setenv("TFB_SHEETS_BACKOFF", val)
    assert sq.backoff_on() is on


def test_q6_the_shared_opener_passes_the_client(monkeypatch):
    got = {}

    class GC:
        def open_by_key(self, sid):
            return ("book", sid)

    def authorize(creds, **kw):
        got.update(kw, creds=creds)
        return GC()

    monkeypatch.setattr(gspread, "authorize", authorize)
    monkeypatch.setattr(cal, "_credentials", lambda: "CREDS")
    monkeypatch.setenv("DEFAULT_SPREADSHEET_ID", "sid123")
    monkeypatch.delenv("TFB_SHEETS_BACKOFF", raising=False)
    assert cal._open_book() == ("book", "sid123")
    assert got["creds"] == "CREDS" and issubclass(got["http_client"], HTTPClient)
    got.clear()
    monkeypatch.setenv("TFB_SHEETS_BACKOFF", "off")
    cal._open_book()
    assert "http_client" not in got                             # the 1.3.0 call exactly


def test_q7_versions_manifest_and_the_unchanged_pure_helper():
    from scripts import sheets_quota as pure
    assert sq.__version__ == "1.0.0" and cal.__version__ == "1.3.1" and pure.__version__ == "1.1.0"
    assert pure.WAITS == (65.0, 65.0) and pure.RETRIES == 2       # call() is unchanged
    assert sq.CLIENT_WAITS == (65.0, 65.0, 65.0) and sq.CLIENT_JITTER == 20.0
    man = (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("sheets_backoff", "1.0.0", "sheets backoff")' in man and '("run_calendar_sync", "1.3.1", "calendar sync")' in man
    assert "tests/test_sheets_quota_client.py" in (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
