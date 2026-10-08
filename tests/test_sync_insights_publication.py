"""Offline actual-runner regressions for failed Insights publication."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from unittest.mock import patch

import pytest

from scripts import run_dashboard_sync as sync


HEADERS = ["Section", "Item", "Symbol", "Metric", "Value", "Notes", "Last Updated (Riyadh)"]
VALID_ROW = ["Overview", "Sources", "", "count", 3, "Source summary", "2026-10-08T08:00:00+03:00"]
FALLBACK_ROW = ["Status", "Engine availability", "", "warning", "Engine returned no usable rows",
                "Live engine and upstream proxies returned empty/error payloads", "2026-10-08T08:00:00+03:00"]


def run_sync(payload, *, error=None, code=200, page="Insights_Analysis", expects_rows=True,
             env=None):
    """Replace only external clients; execute real main_async and task logic."""
    class Backend:
        async def get_json(self, _path):
            return {"status": "ready"}, None, 200

        async def post_json(self, _path, _payload):
            return deepcopy(payload), error, code

        async def close(self):
            pass

    class Writer:
        def __init__(self):
            self.prior = [list(HEADERS), list(VALID_ROW)]
            self.table = deepcopy(self.prior)
            self.writes = []
            self.clears = []

        def _get_service(self):
            # The real task checks readiness; this sentinel has no API methods.
            return object()

        def read_values(self, *_args, **_kwargs):
            return deepcopy(self.table)

        def write_table(self, _sheet_id, _page, _start, headers, rows):
            self.writes.append((deepcopy(headers), deepcopy(rows)))
            self.table = [list(headers), *deepcopy(rows)]
            return len(rows)

        def clear_from(self, *_args):
            self.clears.append(_args)

    writer = Writer()
    results = []
    real_run = sync._run_one_task

    async def capture(*args, **kwargs):
        result = await real_run(*args, **kwargs)
        results.append(result)
        return result

    key = "INSIGHTS_ANALYSIS" if page == "Insights_Analysis" else "TOP_10_INVESTMENTS"
    task = sync.TaskSpec(key, page, "analysis", max_symbols=0, expects_rows=expects_rows)
    with patch.dict("os.environ", {"TFB_SYNC_DECISION_GUARD": "0", **(env or {})}, clear=True), \
            patch.object(sync, "BackendClient", return_value=Backend()), \
            patch.object(sync, "SheetsWriter", return_value=writer), \
            patch.object(sync, "_default_tasks", return_value=[task]), \
            patch.object(sync, "_run_one_task", side_effect=capture):
        exit_code = asyncio.run(sync.main_async([
            "--sheet-id", "synthetic", "--backend", "https://synthetic.invalid",
            "--keys", key, "--no-lock", "--start-cell", "A1",
        ]))
    return exit_code, results[0], writer


def assert_retained_failure(exit_code, result, writer):
    assert exit_code == 2
    assert result.status == "failed"
    assert result.rows_written == 0
    assert result.error
    assert writer.writes == writer.clears == []
    assert writer.table == writer.prior


def test_transport_502_remains_a_failed_run_and_retains_prior_insights():
    exit_code, result, writer = run_sync(None, error="HTTP 502: synthetic gateway unavailable", code=502)
    assert_retained_failure(exit_code, result, writer)
    assert "HTTP 502" in result.error


@pytest.mark.parametrize("status,error,rows,dispatch", [
    ("error", "No usable rows returned; schema-shaped fallback emitted", [], "analysis_wrapper_fail_soft"),
    ("partial", "analysis_sheet_rows runtime fallback: synthetic timeout", [], "analysis_sheet_rows_emergency_fallback"),
    ("partial", "Local non-empty fallback emitted after upstream degradation", [FALLBACK_ROW], "analysis_wrapper_fail_soft_nonempty"),
    ("error", "Synthetic analysis failed", [FALLBACK_ROW], "analysis_wrapper_fail_soft"),
])
@pytest.mark.parametrize("write_then_trim", ["0", "1"])
def test_http200_route_failure_envelope_is_not_success_or_a_benign_skip(
        status, error, rows, dispatch, write_then_trim):
    payload = {"status": status, "error": error, "headers": HEADERS,
               "rows_matrix": rows, "meta": {"dispatch": dispatch}}
    exit_code, result, writer = run_sync(payload, env={"TFB_SYNC_WRITE_THEN_TRIM": write_then_trim})
    assert_retained_failure(exit_code, result, writer)
    assert error in result.error


@pytest.mark.parametrize("payload", [
    {"status": "fail", "headers": HEADERS, "rows_matrix": [VALID_ROW]},
    {"status": "degraded", "headers": HEADERS, "rows_matrix": [VALID_ROW]},
    {"status": "unavailable", "headers": HEADERS, "rows_matrix": [FALLBACK_ROW]},
    {"status": "success", "error": "upstream analysis failed", "headers": HEADERS, "rows_matrix": [VALID_ROW]},
    {"status": "success", "data": {"status": "error", "error": "inner analysis failed", "headers": HEADERS, "rows_matrix": [FALLBACK_ROW]}},
    {"status": "error", "error": "outer analysis failed", "data": {"status": "success", "headers": HEADERS, "rows_matrix": [VALID_ROW]}},
])
def test_explicit_failure_at_either_supported_envelope_level_retains_prior(payload):
    assert_retained_failure(*run_sync(payload))


@pytest.mark.parametrize("rows", [[], [["", None, " ", "", "", "", ""]], [{"unexpected": "not a matrix row"}]])
@pytest.mark.parametrize("status", ["success", "ok"])
@pytest.mark.parametrize("empty_guard", ["0", "1"])
def test_required_unusable_insights_fail_independently_of_the_generic_empty_guard(rows, status, empty_guard):
    payload = {"status": status, "headers": HEADERS, "rows_matrix": rows}
    assert_retained_failure(*run_sync(payload, env={"TFB_SYNC_EMPTY_GUARD": empty_guard}))


@pytest.mark.parametrize("status", ["success", None])
def test_successful_and_legacy_statusless_usable_insights_still_publish(status):
    payload = {"headers": HEADERS, "rows_matrix": [VALID_ROW]}
    if status:
        payload.update(status=status, error=None)
    exit_code, result, writer = run_sync(payload)
    assert exit_code == 0
    assert result.status == "success"
    assert result.rows_written == 1
    assert writer.writes == [(HEADERS, [VALID_ROW])]


@pytest.mark.parametrize("nested", [False, True])
def test_genuine_usable_partial_insights_publish_with_partial_status(nested):
    payload = {"status": "partial", "error": None, "headers": HEADERS, "rows_matrix": [VALID_ROW]}
    if nested:
        payload = {"status": "success", "data": payload}
    exit_code, result, writer = run_sync(payload)
    assert exit_code == 1
    assert result.status == "partial"
    assert result.rows_written == 1
    assert writer.writes == [(HEADERS, [VALID_ROW])]


@pytest.mark.parametrize("matrix_key,status", [("rows", None), ("rows_matrix", "success")])
def test_legacy_permitted_zero_opportunity_board_schema_still_succeeds(matrix_key, status):
    payload = {"headers": ["Symbol"], matrix_key: [], "count": 0}
    if status:
        payload["status"] = status
    exit_code, result, writer = run_sync(payload, page="Top_10_Investments", expects_rows=False)
    assert exit_code == 0
    assert result.status == "success"
    assert result.rows_written == 0
    assert len(writer.writes) == 1
    headers, rows = writer.writes[0]
    assert "Symbol" in headers  # Existing Top10 schema repair may expand headers.
    assert rows == []


def test_upstream_failure_diagnostic_is_sanitized_before_reporting():
    secret = "synthetic-secret-do-not-disclose"
    payload = {"status": "error", "error": "analysis failed: Authorization: Bearer " + secret,
               "headers": HEADERS, "rows_matrix": []}
    exit_code, result, writer = run_sync(payload)
    assert_retained_failure(exit_code, result, writer)
    assert "analysis failed" in result.error
    assert secret not in result.error
    assert secret not in str(result.to_dict())
