"""audit_provider_target_coverage v1.3.0: a page read that hits the Sheets
per-minute quota (HTTP 429) is waited out and tried again before it counts as
READ_FAIL; any other read error is still a READ_FAIL at once."""
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import audit_provider_target_coverage as ptc  # noqa: E402

QUOTA = ("<HttpError 429 when requesting https://sheets.googleapis.com/v4/... returned "
         "\"Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute per user'\">")
GRID = [["Symbol", "Forecast Source"]] + [[f"S{i}", "provider_target"] for i in range(60)]


class HttpError(Exception):
    pass


def _reader(fail_plan):
    calls = {"n": 0}

    def read(sid, a1):
        calls["n"] += 1
        page = a1.split("!")[0]
        plan = fail_plan.get(page, [])
        if plan:
            raise plan.pop(0)
        return GRID
    return read, calls


def _run(reader, sleeps):
    async def fake_sleep(sec):
        sleeps.append(sec)
    return asyncio.run(ptc.run_live("1QaOSNHqKlCaqlnaB45O4RlajWeMNe5cAE", ["Market_Leaders"], None,
                                    ptc.Config(), bootstrap=True, accept=False, reader=reader,
                                    sleep=fake_sleep))


def test_version():
    assert ptc.VERSION == "1.3.0"


def test_quota_once_then_read():
    reader, calls = _reader({"Market_Leaders": [HttpError(QUOTA)]})
    sleeps = []
    rep = _run(reader, sleeps)
    assert sleeps == [65.0] and calls["n"] == 2
    assert rep.pages["Market_Leaders"]["verdict"] != "READ_FAIL"
    assert rep.pages["Market_Leaders"]["rows"] == 60


def test_quota_three_times_is_still_read_fail():
    reader, calls = _reader({"Market_Leaders": [HttpError(QUOTA)] * 3})
    sleeps = []
    rep = _run(reader, sleeps)
    assert sleeps == [65.0, 65.0] and calls["n"] == 3
    assert rep.pages["Market_Leaders"]["verdict"] == "READ_FAIL"
    assert rep.exit_code == 2


def test_other_error_not_retried():
    reader, calls = _reader({"Market_Leaders": [HttpError("<HttpError 403 forbidden>")]})
    sleeps = []
    rep = _run(reader, sleeps)
    assert sleeps == [] and calls["n"] == 1
    assert rep.pages["Market_Leaders"]["verdict"] == "READ_FAIL"


def test_selftest_still_passes():
    assert ptc.selftest() == 0
