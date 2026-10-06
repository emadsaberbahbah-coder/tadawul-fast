"""Exercise producer/feed/audit agreement using identical frozen source facts."""
from datetime import datetime, timedelta, timezone
import pytest

from core.data_validity import coverage_validity, symbol_validity, timestamp_freshness
from scripts import run_dashboard_sync as sync
from scripts.audit_full_refresh_coverage import Rule, audit_grid
from scripts.audit_sync_outcome import audit_artifacts


@pytest.mark.parametrize("fresh,minimum,accepted", [
    (9496, "95", False), (9500, "95", True), (9501, "95", True),
    (9504, "95.01", True), (9500, "95.01", False),
])
@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
def test_exact_boundary_shared_by_all_refresh_surfaces(fresh, minimum, accepted, mode, monkeypatch, tmp_path):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", mode)
    monkeypatch.setenv("TFB_SYNC_STATUS_TRUTH", "0")
    monkeypatch.setenv("TFB_SYNC_STATUS_FRESH_MIN", minimum)
    monkeypatch.setenv("TFB_AUDIT_REQUIRE_FULL_FETCH", "1")
    monkeypatch.setenv("TFB_AUDIT_MIN_FRESH_PCT", minimum)
    result = sync.TaskResult(
        key="global_markets", sheet_name="Global_Markets", status="success",
        start_utc="2026-10-06T00:00:00+00:00", rows_written=10000,
        symbols_requested=10000, _stamp_meta={
            "requested": 10000, "fresh_lineage_known": True,
            "fetched_origin": fresh, "noncurrent_fetched": 0,
            "fetchfail_lineage_known": True, "ff_new_fetched": 0,
        },
    )
    stamp = sync._status_stamp_row("Global_Markets", result, 10)
    feed, _ = sync._uv_page_state(result)
    numerator, denominator, _ = sync._page_fresh_fetch_metrics(result)
    (tmp_path / "sync_frozen.log").write_text(
        f"[PAGE-VERDICT v{sync.SCRIPT_VERSION}] page=Global_Markets "
        f"status=success rows_written=10000 fresh_rows={numerator} "
        f"requested_rows={denominator} fresh_pct={100*fresh/10000:.4f}\n"
    )
    audit = audit_artifacts(tmp_path, required_pages=("Global_Markets",))
    assert ("data=COMPLETE" in stamp[3]) == accepted
    assert (stamp[2] == "SUCCESS") == accepted
    assert (feed == "OK") == accepted
    assert (audit.status == "ok") == accepted
    assert coverage_validity(10000, fresh, minimum).valid == accepted


@pytest.mark.parametrize("mode", ["off", "observe", "enforce"])
def test_failed_origins_cannot_be_fresh_in_any_rollout_mode(mode, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", mode)
    result = sync.TaskResult(
        key="global_markets", sheet_name="Global_Markets", status="success",
        start_utc="2026-10-06T00:00:00+00:00", symbols_requested=100,
        _stamp_meta={"requested": 100, "fresh_lineage_known": True,
                     "fetched_origin": 100, "noncurrent_fetched": 3,
                     "fetchfail_lineage_known": True, "ff_new_fetched": 3},
    )
    assert sync._page_fresh_fetch_metrics(result) == (94, 100, 94.0)
    assert sync._uv_page_state(result) == ("STALE_COV", 94.0)
    assert "fresh=94" in sync._status_stamp_row("Global_Markets", result, 10)[3]
    # Two failures overlap preserved rows; set arithmetic must count them once.
    validity = symbol_validity(
        map(str, range(100)), map(str, range(100)),
        preserved=("0", "1", "2"), failed=("1", "2", "3", "4", "5"),
    )
    assert validity.fresh == 94 and not validity.valid


def test_unknown_modern_lineage_and_empty_request_cannot_certify_source():
    result = sync.TaskResult(
        key="global_markets", sheet_name="Global_Markets", status="success",
        start_utc="2026-10-06T00:00:00+00:00", symbols_requested=100,
        _stamp_meta={"requested": 100, "pre_persist_rows": 100,
                     "fresh_lineage_known": False},
    )
    assert sync._uv_page_state(result) == ("STALE_COV", None)
    assert "data=PARTIAL" in sync._status_stamp_row("Global_Markets", result, 10)[3]
    assert not coverage_validity(0, 0).valid


@pytest.mark.parametrize("requested,fresh,minimum", [
    (None, 1, 95), ("4", 1, 95), (True, 1, 95), (4, True, 95),
    (4, 5, 95), (4, -1, 95), (4, 4, "NaN"), (4, 4, "Infinity"),
])
def test_invalid_coverage_facts_fail_closed(requested, fresh, minimum):
    verdict = coverage_validity(requested, fresh, minimum)
    assert not verdict.valid
    assert verdict.percent is None or 0 <= verdict.percent <= 100


def test_timestamp_basis_and_policy_errors_fail_closed():
    now = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
    assert not timestamp_freshness(now.replace(tzinfo=None), now, max_age_seconds=3600)[0]
    assert not timestamp_freshness(now, now, max_age_seconds=float("nan"))[0]


@pytest.mark.parametrize("offset,accepted", [(299, True), (300, True), (301, False)])
def test_future_skew_boundaries(offset, accepted):
    now = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
    future = now + timedelta(seconds=offset)
    valid, age, reason = timestamp_freshness(future, now, max_age_seconds=3600)
    assert valid == accepted and age == -offset
    assert reason == ("" if accepted else "timestamp_future")


@pytest.mark.parametrize("stamp,accepted", [
    ("2026-10-06T12:00:00Z", True),
    ("2026-10-06T15:00:00+03:00", True),
    ("2026-10-06T12:00:00+00:00", True),
    ("2099-01-01T00:00:00Z", False),
    ("2026-10-06", False),
])
def test_full_row_timestamp_contract(stamp, accepted):
    headers = ["Symbol", "Name", "Current Price", "Last Updated (UTC)"]
    rule = Rule("Global_Markets", 1, 1, 95, 100, 100)
    result = audit_grid([headers, ["A.US", "A", 100, stamp]], rule, headers,
                        datetime(2026, 10, 6, 12, tzinfo=timezone.utc))
    assert result.fresh == int(accepted)
    assert (result.status == "PASS") == accepted


def test_feed_offsets_preserve_same_instant_and_future_token_blocks(monkeypatch):
    states = [sync._uv_parse_value(f"OK | {stamp}") for stamp in (
        "2026-10-06 12:00:00+00:00", "2026-10-06 15:00:00+03:00",
        "2026-10-06T12:00:00Z",
    )]
    assert states[0] == states[1] == states[2]
    monkeypatch.setenv("TFB_SYNC_VERDICT_PAGES", "Global_Markets")
    epoch = states[0][1]
    assert sync._uv_compose({"Global_Markets": ("OK", epoch + 301)}, epoch)[0].startswith("NOT_ACTIONABLE")
    assert sync._uv_compose({"Global_Markets": ("OK", None)}, epoch)[0].startswith("NOT_ACTIONABLE")
