"""Final acquisition telemetry exposes only existing counters and reason codes."""
import copy
import json
import logging

import pytest

from scripts import run_dashboard_sync as sync
from tests.test_portfolio_acquisition_census import SYMBOLS, provider_row, run_portfolio


def _result(meta=None, *, page="Global_Markets"):
    result = sync.TaskResult("GLOBAL_MARKETS", page, "success", "synthetic-clock")
    result.symbols_requested = 6609
    result._stamp_meta = {
        "requested": 6609, "acquisition_known": True, "acquired_fresh": 6160,
        "timestamp_fresh": 6609, "quote_asof_known": 6160,
        "acquisition_unknown": 0, "acquisition_invalid": 449, "noncurrent_fetched": 381,
        "acquisition_reasons": {"failed_or_quarantined_or_preserved": 341,
                                "price_missing_or_invalid": 108},
    } if meta is None else meta
    return result


def test_actual_failure_counts_are_reported_without_promoting_success_or_timestamp_freshness(caplog):
    result = _result()
    original = copy.deepcopy(result._stamp_meta)
    caplog.set_level(logging.INFO, logger=sync.logger.name)
    sync._emit_acquisition_diagnostics(result)
    public = result.to_dict()["acquisition"]
    assert public == {
        "schema_version": 1, "known": True, "requested": 6609, "successful": 6160,
        "timestamp_fresh": 6609, "quote_asof_known": 6160, "unknown": 0, "invalid": 449,
        "noncurrent_fetched": 381,
        "reason_counts": {"failed_or_quarantined_or_preserved": 341, "price_missing_or_invalid": 108},
    }
    payload = caplog.messages[-1].split(" evidence=", 1)[1]
    assert json.loads(payload) == public
    assert result._stamp_meta == original and result.status == "success"
    assert sync._page_fresh_fetch_metrics(result) == (6160, 6609, 93.20623392343774)


def test_raw_metadata_and_arbitrary_reason_text_cannot_escape_json_or_console(caplog):
    result = _result(page="PRIVATE-PAGE-SENTINEL")
    result._stamp_meta.update({"account": "PRIVATE-SENTINEL", "names": ["PRIVATE-SENTINEL"],
                              "price": 123.456, "provider_response": {"secret": "PRIVATE-SENTINEL"}})
    result._stamp_meta["acquisition_reasons"].update({"PRIVATE-SENTINEL": 2, "arbitrary-provider-error": 3})
    caplog.set_level(logging.INFO, logger=sync.logger.name)
    sync._emit_acquisition_diagnostics(result)
    public = result.to_dict()["acquisition"]
    assert public["reason_counts"]["other_reason"] == 5
    assert "PRIVATE" not in json.dumps(public) + caplog.messages[-1]
    assert "123.456" not in json.dumps(public) + caplog.messages[-1]
    assert "page=unrecognized" in caplog.messages[-1]


@pytest.mark.parametrize("value", [None, "5", True, -1, 1_000_001, float("inf"), {"secret": "PRIVATE-SENTINEL"}])
def test_malformed_counter_values_remain_unknown(value):
    result = _result()
    result._stamp_meta["acquired_fresh"] = value
    result._stamp_meta["acquisition_reasons"] = {"price_missing_or_invalid": value}
    public = result.to_dict()["acquisition"]
    assert public["successful"] is None and public["reason_counts"] == {}


@pytest.mark.parametrize("known", [False, None, "true", 1])
def test_unknown_census_never_becomes_zero_or_success(known):
    result = _result()
    result._stamp_meta["acquisition_known"] = known
    public = result.to_dict()["acquisition"]
    assert public["known"] is False and public["requested"] == 6609
    assert all(public[field] is None for field in ("successful", "timestamp_fresh", "quote_asof_known",
                                                 "unknown", "invalid", "noncurrent_fetched"))
    assert public["reason_counts"] == {}


@pytest.mark.parametrize("failure,expected", [("", 5), ("failed", 4), ("preserved", 4), ("missing", 4), ("unknown", 4)])
def test_real_task_emits_once_after_restoration_and_final_census(monkeypatch, caplog, failure, expected):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "off")
    rows = [provider_row(symbol, failure if index == 0 else "", index)
            for index, symbol in enumerate(SYMBOLS)]
    if failure == "missing":
        rows = rows[1:]
    caplog.set_level(logging.INFO, logger=sync.logger.name)
    result, writer = run_portfolio(monkeypatch, rows)
    receipts = [message for message in caplog.messages if message.startswith("[ACQUISITION-DIAGNOSTICS ")]
    assert len(receipts) == 1
    public = result.to_dict()["acquisition"]
    assert json.loads(receipts[0].split(" evidence=", 1)[1]) == public
    assert public["successful"] == expected and public["requested"] == 5
    assert result.status == "success" and len(writer.published) == 5
    assert sync._page_fresh_fetch_metrics(result) == (expected, 5, expected * 20.0)
    if failure == "missing":
        assert public["reason_counts"]["acquisition_preserved"] == 1


def test_diagnostic_logging_failure_cannot_change_a_real_task(monkeypatch):
    def fail(_result):
        raise ValueError("synthetic logger failure")
    monkeypatch.setattr(sync, "_emit_acquisition_diagnostics", fail)
    rows = [provider_row(symbol, index=index) for index, symbol in enumerate(SYMBOLS)]
    result, writer = run_portfolio(monkeypatch, rows)
    assert result.status == "success" and len(writer.published) == 5
    assert result.to_dict()["acquisition"]["successful"] == 5
