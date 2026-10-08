"""Offline actual-runner regressions for failed Insights publication."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from scripts import run_dashboard_sync as sync


HEADERS = ["Section", "Item", "Symbol", "Metric", "Value", "Notes", "Last Updated (Riyadh)"]
CURRENT_HEADERS = ["Section", "Item", "Metric", "Value", "Notes", "Source", "Sort Order"]
CURRENT_KEYS = ["section", "item", "metric", "value", "notes", "source", "sort_order"]
CURRENT_ROW = ["Overview", "Sources", "count", 0, "", "", 1]
VALID_ROW = ["Overview", "Sources", "", "count", 3, "Source summary", "2026-10-08T08:00:00+03:00"]
FALLBACK_ROW = ["Status", "Engine availability", "", "warning", "Engine returned no usable rows",
                "Live engine and upstream proxies returned empty/error payloads", "2026-10-08T08:00:00+03:00"]


def run_sync(payload, *, error=None, code=200, page="Insights_Analysis", expects_rows=True,
             env=None, responses=None):
    """Replace only external clients; execute real main_async and task logic."""
    class Backend:
        def __init__(self):
            self.calls = []

        async def get_json(self, _path):
            return {"status": "ready"}, None, 200

        async def post_json(self, _path, _payload):
            self.calls.append(_path)
            if responses:
                data, response_error, response_code = responses[min(len(self.calls) - 1, len(responses) - 1)]
                return deepcopy(data), response_error, response_code
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
    backend = Backend()
    writer.backend_calls = backend.calls
    results = []
    real_run = sync._run_one_task

    async def capture(*args, **kwargs):
        result = await real_run(*args, **kwargs)
        results.append(result)
        return result

    key = "INSIGHTS_ANALYSIS" if page == "Insights_Analysis" else "TOP_10_INVESTMENTS"
    task = sync.TaskSpec(key, page, "analysis", max_symbols=0, expects_rows=expects_rows)
    with patch.dict("os.environ", {"TFB_SYNC_DECISION_GUARD": "0", **(env or {})}, clear=True), \
            patch.object(sync, "BackendClient", return_value=backend), \
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


@pytest.mark.parametrize("headers", [
    [""] * 7,
    [" "] * 7,
    ["Section", "Section", *CURRENT_HEADERS[2:]],
    ["Group", *CURRENT_HEADERS[1:]],
    [*CURRENT_HEADERS[:5], "", "Sort Order"],
    ["A", "B", "C", "D", "E", "F", "G"],
    CURRENT_HEADERS[:-1],
    [None, *CURRENT_HEADERS[1:]],
])
@pytest.mark.parametrize("trim", ["0", "1"])
def test_malformed_insights_headers_fail_before_any_publication(headers, trim):
    payload = {"status": "success", "headers": headers, "rows_matrix": [CURRENT_ROW]}
    assert_retained_failure(*run_sync(payload, env={"TFB_SYNC_WRITE_THEN_TRIM": trim}))


@pytest.mark.parametrize("rows", [
    [CURRENT_ROW[:2]],
    [[*CURRENT_ROW, "unrequested extra cell"]],
    [CURRENT_ROW, "not a matrix row"],
    [CURRENT_ROW, {"section": "not a matrix row"}],
    [["", "", "", "", "", "provider", 1]],
    [["", "Count", "count", 0, "", "", 1]],
    [["Overview", "", "count", 0, "", "", 1]],
    [[{"section": "Overview"}, "Count", "count", 0, "", "", 1]],
])
def test_raw_matrix_shape_and_required_row_labels_cannot_be_repaired_to_success(rows):
    payload = {"status": "success", "headers": CURRENT_HEADERS, "rows_matrix": rows}
    assert_retained_failure(*run_sync(payload))


def test_legacy_timestamp_only_content_is_not_analysis():
    payload = {"headers": HEADERS,
               "rows_matrix": [["", "", "", "", "", "", "2026-10-08T08:00:00+03:00"]]}
    assert_retained_failure(*run_sync(payload))


@pytest.mark.parametrize("keys", [
    ["section", "section", *CURRENT_KEYS[2:]],
    ["item", "section", *CURRENT_KEYS[2:]],
    CURRENT_KEYS[:-1],
])
def test_dictionary_keys_must_match_the_declared_headers_without_inventing_labels(keys):
    payload = {"headers": CURRENT_HEADERS, "keys": keys,
               "rows": [{"section": "Overview", "item": "Count", "value": 0}]}
    assert_retained_failure(*run_sync(payload))


@pytest.mark.parametrize("rows", [
    [{"section": "Overview", "item": "Count", "value": 0}, "bad row"],
    [{"section": "Overview", "item": "Count", "value": 0}, CURRENT_ROW],
    [{"source": "provider", "sort_order": 1}],
])
def test_dictionary_payload_does_not_hide_invalid_or_provenance_only_rows(rows):
    payload = {"headers": CURRENT_HEADERS, "keys": CURRENT_KEYS, "rows": rows}
    assert_retained_failure(*run_sync(payload))


@pytest.mark.parametrize("status,expected_exit", [(None, 0), ("success", 0), ("partial", 1)])
@pytest.mark.parametrize("representation", ["matrix", "rows", "dict", "nested"])
def test_current_producer_schema_keeps_zero_values_and_optional_blanks(status, expected_exit, representation):
    from core.sheets.schema_registry import get_sheet_headers, get_sheet_keys

    headers = get_sheet_headers("Insights_Analysis")
    keys = get_sheet_keys("Insights_Analysis")
    assert headers == CURRENT_HEADERS and keys == CURRENT_KEYS
    payload = {"headers": headers, "keys": keys}
    if status is not None:
        payload["status"] = status
    if representation == "dict":
        payload["rows"] = [{"section": "Overview", "item": "Sources", "metric": "count", "value": 0,
                            "sort_order": 1}]
    else:
        payload["rows" if representation == "rows" else "rows_matrix"] = [CURRENT_ROW]
    if representation == "nested":
        payload = {"status": "success", "data": payload}
    exit_code, result, writer = run_sync(payload)
    assert exit_code == expected_exit
    assert result.status == ("partial" if expected_exit else "success")
    assert result.rows_written == 1
    assert writer.writes[0][0] == CURRENT_HEADERS
    written = writer.writes[0][1][0]
    assert written[0:4] == CURRENT_ROW[0:4]
    assert written[5] in (None, "")


def test_reordered_current_headers_and_aligned_keys_keep_their_original_order():
    payload = {"headers": list(reversed(CURRENT_HEADERS)), "keys": list(reversed(CURRENT_KEYS)),
               "rows": [dict(zip(CURRENT_KEYS, CURRENT_ROW))]}
    exit_code, result, writer = run_sync(payload)
    assert exit_code == 0 and result.status == "success"
    assert writer.writes == [(list(reversed(CURRENT_HEADERS)), [list(reversed(CURRENT_ROW))])]


@pytest.mark.parametrize("headers,row", [(CURRENT_HEADERS, CURRENT_ROW), (HEADERS, VALID_ROW)])
def test_dictionary_rows_keyed_by_display_headers_need_no_new_key_metadata(headers, row):
    payload = {"headers": headers, "rows": [dict(zip(headers, row))]}
    exit_code, result, writer = run_sync(payload)
    assert exit_code == 0 and result.status == "success"
    assert writer.writes == [(headers, [row])]


@pytest.mark.parametrize("label", [0, False, ["Overview"], {"label": "Overview"},
                                   datetime(2026, 10, 8, tzinfo=timezone.utc)])
@pytest.mark.parametrize("representation", ["matrix", "dict"])
def test_required_identity_types_cannot_be_stringified_into_analysis(label, representation):
    payload = {"headers": CURRENT_HEADERS, "keys": CURRENT_KEYS}
    if representation == "dict":
        payload["rows"] = [{"section": label, "item": "Count", "value": 0}]
    else:
        payload["rows_matrix"] = [[label, *CURRENT_ROW[1:]]]
    assert_retained_failure(*run_sync(payload))


def test_explicit_matrix_of_dictionaries_is_not_a_permitted_empty_schema():
    payload = {"headers": CURRENT_HEADERS, "rows_matrix": [{"section": "Overview", "item": "Count"}]}
    assert_retained_failure(*run_sync(payload, expects_rows=False))


@pytest.mark.parametrize("bad_key", [{"token": "synthetic-secret"}, ["section"], None, 0, ""])
def test_malformed_explicit_keys_cannot_skip_a_healthy_endpoint_fallback(bad_key):
    malformed = {"headers": CURRENT_HEADERS, "keys": [bad_key, *CURRENT_KEYS[1:]],
                 "rows": [{"section": "Overview", "item": "Count", "value": 0}]}
    healthy = {"headers": CURRENT_HEADERS, "rows_matrix": [CURRENT_ROW]}
    exit_code, result, writer = run_sync(None, env={"TFB_SYNC_SAFE_GATEWAYS": "0"},
                                        responses=[(malformed, None, 200), (healthy, None, 200)])
    assert exit_code == 0 and result.status == "success"
    assert len(writer.backend_calls) == 2
    assert writer.writes == [(CURRENT_HEADERS, [CURRENT_ROW])]
    assert "synthetic-secret" not in str(result.to_dict())


def test_all_malformed_key_candidates_retain_prior_page_without_raw_diagnostics():
    malformed = {"headers": CURRENT_HEADERS, "keys": [{"token": "synthetic-secret"}, *CURRENT_KEYS[1:]],
                 "rows": [{"section": "Overview", "item": "Count", "value": 0}]}
    exit_code, result, writer = run_sync(malformed, env={"TFB_SYNC_SAFE_GATEWAYS": "0"})
    assert_retained_failure(exit_code, result, writer)
    assert len(writer.backend_calls) > 1
    assert writer.backend_calls[-1] == "/enriched/sheet-rows"
    assert "nonblank strings" in result.error
    assert "synthetic-secret" not in str(result.to_dict())
