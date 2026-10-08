"""Real sync task failure outputs using synthetic errors and offline writers."""
from __future__ import annotations

import asyncio
import importlib.util
import json
import os
from pathlib import Path
import sys
import uuid

import pytest

from core import secret_redaction
from core.sheets.schema_registry import get_sheet_headers
from scripts import run_dashboard_sync as sync

if os.getenv("TFB_REDACTION_SOURCE_ROOT"):
    path = Path(os.environ["TFB_REDACTION_SOURCE_ROOT"]) / "scripts/run_dashboard_sync.py"
    spec = importlib.util.spec_from_file_location("redaction_baseline_sync", path)
    sync = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = sync
    spec.loader.exec_module(sync)


@pytest.mark.parametrize("failure", ["symbol_read", "write", "guard"])
def test_real_task_errors_redact_without_changing_failed_outcome_or_write_counts(monkeypatch, failure):
    secret = "synthetic_" + uuid.uuid4().hex
    monkeypatch.setattr(secret_redaction, "configured_secret_values", lambda: (secret,))
    for name, value in {
        "TFB_MARKET_SYMBOL_READBACK": "0", "TFB_SYNC_SYMBOL_BATCH_SIZE": "0",
        "TFB_SYNC_PERSISTENCE_HARD": "0", "TFB_SYNC_ROW_ID_FIREWALL": "0",
        "TFB_SYNC_NAME_DEDUP_MODE": "off", "TFB_SYNC_OHLC_LAKE": "0",
        "TFB_SYNC_FALSE_GREEN_SCREEN": "0", "TFB_SYNC_STATUS_STAMP": "0",
        "TFB_SYNC_FETCHFAIL_TRUTH": "off", "TFB_SYNC_IDENTITY_TRIPWIRE": "0",
        "TFB_SYNC_COHERENCE_TRIPWIRE": "0", "TFB_SYNC_OHLC_PREWRITE": "0",
        "TFB_SYNC_OHLC_READBACK": "0", "TFB_SYNC_WRITE_SENTINEL": "0",
    }.items():
        monkeypatch.setenv(name, value)
    symbols = ["GC=F", "SI=F", "HG=F", "CL=F"]
    # Reach the intended writer failure through the production market schema;
    # malformed partial headers now correctly fail before any write attempt.
    headers = list(get_sheet_headers("Commodities_FX"))
    now = sync._utc_now().isoformat()
    warning = "acquisition_status:success; acquisition_provider:yahoo_chart; acquisition_acquired_at:" + now
    facts = [{"Symbol": symbol, "Name": "Synthetic instrument", "Current Price": 12.5,
              "Data Provider": "yahoo_chart", "Last Updated (UTC)": now, "Warnings": warning}
             for symbol in symbols]
    rows = [[row.get(header, "") for header in headers] for row in facts]
    observed = {"backend": 0, "write": 0, "clear": 0}

    class Backend:
        async def post_json(self, endpoint, payload):
            observed["backend"] += 1
            return {"headers": headers, "rows_matrix": rows}, None, 200

    class Writer:
        def _get_service(self):
            return object()
        def read_values(self, *args, **kwargs):
            return [headers] + rows
        def write_table(self, *args, **kwargs):
            observed["write"] += 1
            raise RuntimeError("HTTP 503 upstream echo " + secret)
        def clear_from(self, *args, **kwargs):
            observed["clear"] += 1

    def read_symbols(*args, **kwargs):
        if failure == "symbol_read":
            raise RuntimeError("HTTP 503 upstream echo " + secret)
        return symbols
    monkeypatch.setattr(sync, "_read_symbols", read_symbols)
    if failure == "guard":
        monkeypatch.setattr(sync, "_ohlc_prewrite_enabled", lambda: True)
        monkeypatch.setattr(sync, "_ohlc_prewrite_mode", lambda: "enforce")
        def fail_guard(*args, **kwargs):
            raise RuntimeError("HTTP 503 upstream echo " + secret)
        monkeypatch.setattr(sync, "_apply_ohlc_prewrite_guard", fail_guard)
    result = asyncio.run(sync._run_one_task(
        sync.TaskSpec("COMMODITIES_FX", "Commodities_FX", "analysis"),
        "offline", "A1", -1, False, False, Backend(), Writer(),
    ))
    assert result.status == ("skipped" if failure == "guard" else "failed")
    assert result.rows_written == 0 and result.rows_failed == 0
    assert observed["clear"] == 0
    assert observed["backend"] == (0 if failure == "symbol_read" else 1)
    assert observed["write"] == (1 if failure == "write" else 0)
    if failure == "guard":
        assert result.error is None
        assert any("RuntimeError" in warning and "HTTP 503" in warning for warning in result.warnings)
    else:
        assert result.error is not None and "HTTP 503" in result.error
    before_warnings = list(result.warnings)
    before_error = result.error
    output = result.to_dict()
    assert result.warnings == before_warnings and result.error == before_error
    assert output["status"] == result.status and output["rows_written"] == 0
    leaked = secret in json.dumps(output)
    assert leaked is False, "synthetic credential survived TaskResult diagnostic output"
    if failure != "guard":
        assert "RuntimeError" in result.error
