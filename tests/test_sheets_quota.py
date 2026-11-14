"""scripts/sheets_quota.py 1.0.0 - wait out the Sheets per-minute read quota (owner, 13 Nov 2026 19:12: full check and
another improvement; three scheduled runs had ended red on HTTP 429 that afternoon).

  Q1  what counts as the quota answer
  Q2  call(): waits 65 s into a new quota minute, tries again, gives up after the retries; other errors at once
  Q3  the Exit Plan waits 65 s after a quota answer and 20 s after any other read error
  Q4  the Evidence Board retries the workbook read alone - the BigQuery queries are not repeated
  Q5  the manifest pins and the CI lane
"""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import evidence_board as eb  # noqa: E402
from scripts import exit_plan as ep  # noqa: E402
from scripts import sheets_quota as sq  # noqa: E402
from tests import test_evidence_board as teb  # noqa: E402
from tests import test_exit_plan as tep  # noqa: E402


class APIError(Exception):
    pass


QUOTA = "APIError: [429]: Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute per user'"


class Resp:
    status_code = 429


def test_q1_what_counts_as_the_quota_answer():
    assert sq.is_quota(APIError(QUOTA)) and sq.is_quota(RuntimeError("RESOURCE_EXHAUSTED"))
    e = APIError("busy")
    e.response = Resp()
    assert sq.is_quota(e)
    assert not sq.is_quota(APIError("[503]: The service is currently unavailable"))
    assert not sq.is_quota(KeyError("Symbol")) and not sq.is_quota(RuntimeError("quota"))   # the word alone is not it
    assert sq.wait_for(APIError(QUOTA), 1) == 65.0 and sq.wait_for(APIError(QUOTA), 2) == 65.0
    assert sq.wait_for(APIError(QUOTA), 9) == 65.0 and sq.wait_for(APIError("[503]"), 1) is None
    assert min(sq.WAITS) > 60                                         # each wait crosses into a new quota minute


def test_q2_call_waits_retries_and_gives_up():
    slept, said, n = [], [], {"i": 0}

    def flaky(fail):
        def fn():
            n["i"] += 1
            if n["i"] <= fail:
                raise APIError(QUOTA)
            return "rows"
        return fn

    assert sq.call(flaky(0), slept.append, said.append, "reading X") == "rows" and slept == [] and said == []
    n["i"] = 0
    assert sq.call(flaky(2), slept.append, said.append, "reading X") == "rows"
    assert slept == [65.0, 65.0] and said == ["Sheets quota reached while reading X: waiting 65 s, then try 1 of 2",
                                               "Sheets quota reached while reading X: waiting 65 s, then try 2 of 2"]
    n["i"], slept[:] = 0, []
    with pytest.raises(APIError):
        sq.call(flaky(3), slept.append, said.append, "reading X")
    assert slept == [65.0, 65.0] and n["i"] == 3                      # two retries, then the error stands
    slept[:] = []

    def other():
        raise KeyError("Symbol")
    with pytest.raises(KeyError):
        sq.call(other, slept.append, said.append, "reading X")
    assert slept == []                                                # not the quota: no wait, no second try
    n["i"] = 0
    with pytest.raises(APIError):
        sq.call(flaky(1), slept.append, said.append, "reading X", retries=0)
    assert slept == []


def test_q3_exit_plan_waits_a_quota_minute(monkeypatch, capsys):
    slept = []
    monkeypatch.setattr(ep, "_SLEEP", slept.append)
    monkeypatch.setenv("TFB_EXIT_PLAN", "on")

    def bad(err):
        class Bad(tep.Book):
            def worksheet(self, name):
                raise err
        return Bad({})

    assert ep.run(False, True, book=bad(APIError(QUOTA)), now=tep.NOW, opener=tep.opener) == 2
    out = capsys.readouterr().out
    assert slept == [65.0, 65.0] and "attempt 1 of 3, retrying in 65 s" in out and "nothing written" in out
    slept[:] = []
    assert ep.run(False, True, book=bad(RuntimeError("timeout")), now=tep.NOW, opener=tep.opener) == 2
    assert slept == [20, 20] and "retrying in 20 s" in capsys.readouterr().out       # every other error: as before


def test_q4_evidence_board_retries_the_read_not_the_queries(monkeypatch, capsys):
    slept = []
    monkeypatch.setattr(eb, "_SLEEP", slept.append)
    monkeypatch.setenv("TFB_BQ_PROJECT", "proj-1")
    monkeypatch.delenv("TFB_EVIDENCE_MIN_GAP_H", raising=False)

    class Limited(teb.Sheet):
        fails = 0

        def get(self, rng):
            if Limited.fails:
                Limited.fails -= 1
                raise APIError(QUOTA)
            return teb.Sheet.get(self, rng)

    def book():
        return teb.Book({"_Daily_Plan": teb.Sheet(teb.LEDGER), "S1_Gate": Limited(teb.GATE),
                         "_Evidence_Log": teb.Sheet([list(eb.LOG_HEADERS)])})

    Limited.fails, b, sqls = 2, book(), []
    assert eb.run_once(False, book=b, session=sqls, now=teb.NOW, query=teb.fake_query) == 0
    assert slept == [65.0, 65.0] and len(sqls) == 5 and "Evidence_Board" in b.tabs   # the five queries ran once
    assert "Sheets quota reached while reading S1_Gate: waiting 65 s, then try 1 of 2" in capsys.readouterr().out
    Limited.fails, b, sqls, slept[:] = 3, book(), [], []
    assert eb.run_once(False, book=b, session=sqls, now=teb.NOW, query=teb.fake_query) == 2
    assert slept == [65.0, 65.0] and "Evidence_Board" not in b.tabs and "nothing written" in capsys.readouterr().out

    class Broken(teb.Sheet):
        def get(self, rng):
            raise RuntimeError("[503]: unavailable")
    b, slept[:] = teb.Book({"_Daily_Plan": Broken(teb.LEDGER)}), []
    assert eb.run_once(False, book=b, session=[], now=teb.NOW, query=teb.fake_query) == 2 and slept == []


def test_q5_manifest_and_ci():
    man = (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("sheets_quota", "%s", "sheets quota")' % sq.__version__ in man
    assert '("exit_plan", "%s", "exit plan")' % ep.__version__ in man and ep.__version__ >= "1.3.1"
    assert '("evidence_board", "%s", "evidence board")' % eb.__version__ in man and eb.__version__ == "1.2.1"
    assert "tests/test_sheets_quota.py" in (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    src = (ROOT / "scripts/sheets_quota.py").read_text(encoding="utf-8")
    assert "gspread" not in src and "requests" not in src.split('"""')[2]         # pure: no client, no network
