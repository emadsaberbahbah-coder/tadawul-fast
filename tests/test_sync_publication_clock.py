"""Offline publication-clock contracts on the real helpers and Sheets publisher."""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
import math
import os
import re
import time
from types import SimpleNamespace

import pytest

from scripts import run_dashboard_sync as sync


NOW = datetime(2026, 10, 8, 12, tzinfo=timezone.utc).timestamp()
PAGES = ("Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds")
STAMP = "2026-10-08 15:00:00+03:00"


@pytest.fixture(autouse=True)
def isolated_policy(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_UPSTREAM_VERDICT", "1")
    monkeypatch.delenv("TFB_SYNC_VERDICT_PAGES", raising=False)
    monkeypatch.delenv("TFB_SYNC_VERDICT_MAX_AGE_MIN", raising=False)
    monkeypatch.setenv("TFB_SYNC_CAPACITY_STATUS", "0")
    monkeypatch.setenv("TFB_SYNC_STATUS_TRUTH", "0")
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "off")
    monkeypatch.setenv("GITHUB_RUN_ID", "synthetic-current")


@contextmanager
def process_zone(zone):
    """Change only this test process's libc zone and restore it even on failure."""
    original = os.environ.get("TZ")
    try:
        os.environ["TZ"] = zone
        time.tzset()
        yield
    finally:
        if original is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = original
        time.tzset()


def healthy():
    return {page: ("OK", NOW - 60) for page in PAGES}


def feed_value(stamp, *, state="OK", run="synthetic-previous"):
    return f"{state} | cov=100 | run={run} | {stamp}"


@pytest.mark.parametrize("zone", ["UTC", "Asia/Riyadh", "America/New_York"])
@pytest.mark.parametrize("stamp", [
    "2026-10-08 12:00:00+00:00", "2026-10-08 15:00:00+03:00",
    "2026-10-08 07:00:00-05:00", "2026-10-08 17:30:00+05:30",
])
def test_explicit_wire_offsets_name_the_same_instant_in_every_runner_zone(zone, stamp):
    with process_zone(zone):
        assert sync._uv_parse_value(feed_value(stamp)) == ("OK", NOW)


@pytest.mark.parametrize("stamp", [
    "", "2026-10-08", "2026-10-08 12:00", "2026-10-08 12:00:00",
    "2026-10-08T12:00:00+00:00", "2026-10-08 12:00:00Z",
    "2026-10-08 12:00:00+0000", "2026-10-08 12:00:00+00",
    "2026-10-08 12:00:00+00:00 trailing", "2026-10-08 12:00:00+garbage",
    "2026-10-08 12:00:00+03:99", "2026-10-08 12:00:00+24:00",
    "2026-10-08 12:00:00.123+00:00", "2026-02-30 12:00:00+00:00",
    "2026-10-08 25:00:00+00:00", "2026-10-08 12:00:60+00:00",
    "２０２６-10-08 12:00:00+00:00", "not a timestamp",
])
def test_unverified_wire_timestamps_keep_state_but_cannot_certify_freshness(stamp):
    assert sync._uv_parse_value(feed_value(stamp)) == ("OK", None)
    rows = healthy()
    rows["Global_Markets"] = sync._uv_parse_value(feed_value(stamp))
    verdict, summary = sync._uv_compose(rows, NOW)
    assert verdict == "NOT_ACTIONABLE(unverified_ts:GM)"
    assert "GM:UNVERIFIED_TS" in summary


@pytest.mark.parametrize("other", [STAMP, "2026-10-08 12:00:00", "2026-10-08 12:00:00+garbage"])
def test_multiple_timestamp_fields_do_not_hide_ambiguous_or_bad_evidence(other):
    assert sync._uv_parse_value(feed_value(STAMP) + " | " + other) == ("OK", None)


@pytest.mark.parametrize("instant,label", [
    (None, "UNVERIFIED_TS"), (float("nan"), "INVALID_TS"),
    (float("inf"), "INVALID_TS"), (float("-inf"), "INVALID_TS"),
    (True, "INVALID_TS"), (False, "INVALID_TS"), ("2026-10-08", "INVALID_TS"),
    (10 ** 1000, "INVALID_TS"), (NOW + 0.001, "FUTURE_TS"),
    (NOW + 900, "FUTURE_TS"),
])
def test_ok_pages_require_finite_nonboolean_nonfuture_instants(instant, label):
    rows = healthy()
    rows["Global_Markets"] = ("OK", instant)
    assert sync._uv_compose(rows, NOW) == (
        f"NOT_ACTIONABLE({label.lower()}:GM)", f"ML:OK GM:{label} CFX:OK MF:OK")


@pytest.mark.parametrize("anchor", [None, True, False, "now", float("nan"), float("inf"),
                                    float("-inf"), 10 ** 1000])
def test_invalid_current_clock_cannot_certify_any_pages(anchor):
    assert sync._uv_compose(healthy(), anchor) == ("NOT_ACTIONABLE(clock_invalid)", "CLOCK_INVALID")


@pytest.mark.parametrize("configured,minutes", [(None, 240), ("bad", 240), ("1", 30),
                                               ("60", 60), ("1440", 1440), ("9999", 1440)])
def test_trailing_window_keeps_defaults_clamps_and_inclusive_boundary(monkeypatch, configured, minutes):
    if configured is not None:
        monkeypatch.setenv("TFB_SYNC_VERDICT_MAX_AGE_MIN", configured)
    assert sync._upstream_verdict_max_age_min() == minutes
    rows = healthy()
    rows["Global_Markets"] = ("OK", NOW - minutes * 60)
    assert sync._uv_compose(rows, NOW)[0] == "EXECUTABLE"
    rows["Global_Markets"] = ("OK", NOW - minutes * 60 - 0.001)
    assert sync._uv_compose(rows, NOW)[0] == "NOT_ACTIONABLE(aged:GM)"
    rows["Global_Markets"] = ("OK", NOW)
    assert sync._uv_compose(rows, NOW)[0] == "EXECUTABLE"


@pytest.mark.parametrize("state", ["FAILED", "PARTIAL", "STALE_COV", "SKIPPED", "POLICY_ERROR"])
@pytest.mark.parametrize("instant", [None, NOW + 60, float("nan")])
def test_unhealthy_states_keep_the_existing_visible_state_reason(state, instant):
    rows = healthy()
    rows["Global_Markets"] = (state, instant)
    assert sync._uv_compose(rows, NOW)[0] == f"NOT_ACTIONABLE({state.lower()}:GM)"


def test_aged_override_required_order_and_configured_pages_remain_unchanged(monkeypatch):
    rows = healthy()
    rows["Global_Markets"] = ("FAILED", NOW - 241 * 60)
    assert sync._uv_compose(rows, NOW)[0] == "NOT_ACTIONABLE(aged:GM)"
    rows["Market_Leaders"] = ("OK", None)
    assert sync._uv_compose(rows, NOW)[0] == "NOT_ACTIONABLE(unverified_ts:ML)"
    monkeypatch.setenv("TFB_SYNC_VERDICT_PAGES", "Global_Markets, Market_Leaders")
    assert sync._uv_compose(rows, NOW)[0] == "NOT_ACTIONABLE(aged:GM)"
    monkeypatch.setenv("TFB_SYNC_VERDICT_PAGES", "Global_Markets")
    assert sync._uv_compose({"Global_Markets": ("OK", NOW)}, NOW) == ("EXECUTABLE", "GM:OK")
    assert sync._uv_compose({}, NOW) == ("NOT_ACTIONABLE(missing:GM)", "GM:MISSING")


@pytest.mark.parametrize("zone", ["UTC", "Asia/Riyadh"])
def test_offset_aware_age_checks_fix_both_false_fresh_and_false_aged_readings(zone):
    with process_zone(zone):
        rows = healthy()
        rows["Global_Markets"] = sync._uv_parse_value(feed_value("2026-10-08 10:00:00+03:00"))
        assert NOW - rows["Global_Markets"][1] == 300 * 60
        assert sync._uv_compose(rows, NOW)[0] == "NOT_ACTIONABLE(aged:GM)"
        rows["Global_Markets"] = sync._uv_parse_value(feed_value("2026-10-08 10:59:59+00:00"))
        assert NOW - rows["Global_Markets"][1] == 60 * 60 + 1
        assert sync._uv_compose(rows, NOW)[0] == "EXECUTABLE"


@pytest.mark.parametrize("zone", ["UTC", "Asia/Riyadh", "America/New_York"])
def test_current_producer_keeps_consumer_compatible_wire_format(zone):
    with process_zone(zone):
        before = time.time()
        stamp = sync._status_ts_str()
        after = time.time()
        assert re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}", stamp)
        state, instant = sync._uv_parse_value(feed_value(stamp))
        assert state == "OK" and math.floor(before) <= instant <= after


class FakeSheets:
    """Fake only the Google resource boundary; successful updates alter its grid."""
    def __init__(self, grid, *, failures=0):
        self.grid = deepcopy(grid)
        self.calls = []
        self.failures = failures

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def get(self, **kwargs):
        self.calls.append(("get", deepcopy(kwargs)))
        return SimpleNamespace(execute=lambda: {"values": deepcopy(self.grid)})

    def update(self, **kwargs):
        self.calls.append(("update", deepcopy(kwargs)))
        def execute():
            key, value = kwargs["body"]["values"][0]
            if key == "TFB Decision Feed" and self.failures:
                self.failures -= 1
                raise RuntimeError("synthetic Sheets retry failure")
            slot = int(re.fullmatch(r"'_Status'!L([0-9]+):M\1", kwargs["range"])[1]) - 1
            while len(self.grid) <= slot:
                self.grid.append([])
            self.grid[slot] = [key, value]
            return {"updatedRows": 1}
        return SimpleNamespace(execute=execute)

    def append(self, **_kwargs):
        raise AssertionError("publication must never append")

    def value(self, key):
        return next((row[1] for row in self.grid if row and row[0] == key), None)


def publisher(monkeypatch, *, bad_stamp=None, now=NOW, failures=0, result_page="Market_Leaders"):
    grid = [["Backend URL", "https://synthetic.invalid"]]
    for index, page in enumerate(PAGES):
        stamp = bad_stamp if page == "Global_Markets" and bad_stamp is not None else STAMP
        grid.append(["TFB Feed " + page, feed_value(stamp, run=f"synthetic-old-{index}")])
    grid.append(["TFB Decision Feed", "previous composite sentinel"])
    fake = FakeSheets(grid, failures=failures)
    writer = sync.SheetsWriter()
    monkeypatch.setattr(writer, "_get_service", lambda: fake)
    sleeps = []
    monkeypatch.setattr(sync, "time", SimpleNamespace(time=lambda: now, sleep=sleeps.append))
    monkeypatch.setattr(sync, "_status_ts_str", lambda: STAMP)
    result = sync.TaskResult(key="synthetic", sheet_name=result_page, status="success",
                             start_utc="2026-10-08T11:59:00+00:00", symbols_requested=100)
    result._stamp_meta = {"requested": 100, "pre_persist_rows": 100, "klg_kept": 0}
    sync._write_upstream_verdict(writer, "synthetic-sheet", [result])
    return fake, sleeps


@pytest.mark.parametrize("stamp,reason", [
    ("", "unverified_ts"), ("2026-10-08 12:00:00", "unverified_ts"),
    ("2026-10-08 15:00:00+garbage", "unverified_ts"),
    ("2026-10-08 15:00:01+03:00", "future_ts"),
    ("2026-10-08 10:00:00+03:00", "aged"),
])
def test_actual_publisher_withholds_unproved_retained_page(monkeypatch, stamp, reason):
    fake, sleeps = publisher(monkeypatch, bad_stamp=stamp)
    assert fake.value("TFB Decision Feed").startswith(f"NOT_ACTIONABLE({reason}:GM) |")
    assert f"GM:{reason.upper()}" in fake.value("TFB Decision Feed")
    assert sleeps == []
    updates = [kwargs for kind, kwargs in fake.calls if kind == "update"]
    assert len(updates) == 2 and all(kwargs["valueInputOption"] == "RAW" for kwargs in updates)
    assert all(re.fullmatch(r"'_Status'!L([0-9]+):M\1", kwargs["range"]) for kwargs in updates)


def test_actual_publisher_retains_valid_trailing_pages_from_distinct_runs(monkeypatch):
    fake, sleeps = publisher(monkeypatch)
    assert fake.value("TFB Decision Feed") == (
        f"EXECUTABLE | run=synthetic-current | {STAMP} | ML:OK GM:OK CFX:OK MF:OK")
    assert "run=synthetic-current" in fake.value("TFB Feed Market_Leaders")
    assert "run=synthetic-old-1" in fake.value("TFB Feed Global_Markets")
    assert sleeps == []


def test_actual_publisher_current_page_can_replace_its_unverified_previous_stamp(monkeypatch):
    fake, _ = publisher(monkeypatch, bad_stamp="offsetless old value", result_page="Global_Markets")
    assert fake.value("TFB Feed Global_Markets") == f"OK | cov=100.0 | run=synthetic-current | {STAMP}"
    assert fake.value("TFB Decision Feed").startswith("EXECUTABLE |")


@pytest.mark.parametrize("anchor", [float("nan"), float("inf"), True])
def test_actual_publisher_reports_invalid_current_clock(monkeypatch, anchor):
    fake, _ = publisher(monkeypatch, now=anchor)
    assert fake.value("TFB Decision Feed") == (
        f"NOT_ACTIONABLE(clock_invalid) | run=synthetic-current | {STAMP} | CLOCK_INVALID")


def test_actual_publisher_retry_preserves_clock_verdict_and_bounded_update(monkeypatch):
    fake, sleeps = publisher(monkeypatch, bad_stamp="", failures=1)
    assert fake.value("TFB Decision Feed").startswith("NOT_ACTIONABLE(unverified_ts:GM) |")
    retries = [kwargs for kind, kwargs in fake.calls if kind == "update"
               and kwargs["body"]["values"][0][0] == "TFB Decision Feed"]
    assert len(retries) == 2 and retries[0] == retries[1] and sleeps == [1.0]


def test_failed_publication_retry_keeps_previous_composite_and_reports_failure(monkeypatch, capsys):
    fake, sleeps = publisher(monkeypatch, bad_stamp="", failures=2)
    assert fake.value("TFB Decision Feed") == "previous composite sentinel"
    assert sleeps == [1.0, 1.0]
    assert "write FAILED" in capsys.readouterr().out


def test_disabled_publication_gate_performs_no_api_calls(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_UPSTREAM_VERDICT", "0")
    writer = sync.SheetsWriter()
    def prohibited_service():
        raise AssertionError("disabled gate must not construct a service")
    monkeypatch.setattr(writer, "_get_service", prohibited_service)
    sync._write_upstream_verdict(writer, "synthetic-sheet", [])
