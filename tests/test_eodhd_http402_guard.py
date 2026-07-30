"""Regression tests for EODHD HTTP 402 and staged concurrency safety."""

from core.providers import eodhd_provider as eodhd
from scripts import concurrent_batch_fetch as batch_fetch


def test_http_402_is_provider_unhealthy() -> None:
    assert eodhd._err_indicates_provider_unhealthy("HTTP 402") is True
    assert eodhd._err_indicates_provider_unhealthy(
        "HTTP 402 plan_restricted endpoint unavailable"
    ) is True
    assert eodhd._err_indicates_provider_unhealthy("payment_required") is True


def test_http_402_error_patch_emits_existing_health_marker() -> None:
    patch = eodhd._build_error_patch_with_geo(
        "AAPL.US", "AAPL.US", "HTTP 402 plan_restricted"
    )
    warnings = patch.get("warnings") or []
    assert "fetch_failed:HTTP 402 plan_restricted" in warnings
    assert "provider_unhealthy:eodhd" in warnings


def test_non_systemic_errors_do_not_trip_provider_health() -> None:
    assert eodhd._err_indicates_provider_unhealthy("HTTP 404 not_found") is False
    assert eodhd._err_indicates_provider_unhealthy("HTTP 429") is False
    assert eodhd._err_indicates_provider_unhealthy("network_error:Timeout") is False


def test_production_concurrency_defaults_to_one(monkeypatch) -> None:
    monkeypatch.delenv("TFB_SYNC_BATCH_CONCURRENCY", raising=False)
    assert batch_fetch.concurrency() == 1


def test_benchmark_can_explicitly_request_three(monkeypatch) -> None:
    monkeypatch.setenv("TFB_SYNC_BATCH_CONCURRENCY", "3")
    assert batch_fetch.concurrency() == 3
