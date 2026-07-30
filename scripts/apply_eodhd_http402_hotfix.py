#!/usr/bin/env python3
"""Apply two staged-rollout safety fixes exactly once.

1. Signal EODHD HTTP 402 as provider-unhealthy so the existing provider registry
   stops repeating a known plan/entitlement failure for every symbol.
2. Lock the new batch adapter's default concurrency to 1. Benchmarks may still
   explicitly set TFB_SYNC_BATCH_CONCURRENCY=3, but production remains sequential
   until the no-write deployment gate passes.

The transformer is assertion-based. It refuses to modify source unless every
expected block matches exactly. Re-running after application is a verified no-op.
"""

from __future__ import annotations

from pathlib import Path

PROVIDER = Path("core/providers/eodhd_provider.py")
CONCURRENCY_MODULE = Path("scripts/concurrent_batch_fetch.py")
TEST_FILE = Path("tests/test_eodhd_http402_guard.py")

DOC_OLD = """EODHD Provider — v4.15.0 (UNIT-AWARE PERCENT CONVERSION: the magnitude guess
                          that silently inflated every sub-1.5% value by 100x
                          is retired)
"""

DOC_NEW = """EODHD Provider — v4.15.1 (HTTP 402 PROVIDER-UNHEALTHY SIGNAL)

v4.15.1 — WHY
--------------------------------------------------------------------------------
The production workbook carried `fetch_failed:HTTP 402` on thousands of rows.
HTTP 402 is a provider-wide plan or endpoint-entitlement failure, but v4.15.0
classified it as an ordinary one-symbol FetchError. The existing engine health
registry therefore never saw `provider_unhealthy:eodhd` and retried EODHD for
every symbol in the universe.

FIX: treat an HTTP 402 error string as a provider-unhealthy trigger. The existing
error patch then emits `provider_unhealthy:eodhd`; data_engine_v2's existing
provider health registry demotes EODHD for its bounded TTL and proceeds to the
next provider instead of repeating a known failing request thousands of times.
No scoring, recommendation, schema, price-selection or Sheet-writer logic changes.

EODHD Provider — v4.15.0 (UNIT-AWARE PERCENT CONVERSION: the magnitude guess
                          that silently inflated every sub-1.5% value by 100x
                          is retired)
"""

TOKENS_OLD = '''_PROVIDER_UNHEALTHY_TRIGGER_TOKENS: Tuple[str, ...] = (
    "auth_error",
    "ip_blocked",
)
'''

TOKENS_NEW = '''_PROVIDER_UNHEALTHY_TRIGGER_TOKENS: Tuple[str, ...] = (
    "auth_error",
    "ip_blocked",
    # v4.15.1: EODHD uses HTTP 402 for plan/endpoint entitlement rejection.
    # This is systemic, not symbol-specific, so let the existing engine health
    # registry demote the provider and use the next provider in the chain.
    "http 402",
    "plan_restricted",
    "payment_required",
)
'''

VERSION_OLD = 'PROVIDER_VERSION = "4.15.0"\n'
VERSION_NEW = 'PROVIDER_VERSION = "4.15.1"\n'

CONCURRENCY_OLD = '''def concurrency() -> int:
    """Configured concurrent provider requests; 1 is the safe rollback mode."""
    return _int("TFB_SYNC_BATCH_CONCURRENCY", 3, 1, 6)
'''

CONCURRENCY_NEW = '''def concurrency() -> int:
    """Configured provider requests; default 1 until the staged gate passes."""
    return _int("TFB_SYNC_BATCH_CONCURRENCY", 1, 1, 6)
'''

TEST_CONTENT = '''"""Regression tests for EODHD HTTP 402 and staged concurrency safety."""

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
'''


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one source block, found {count}")
    return text.replace(old, new, 1)


def verify_provider(text: str) -> None:
    required = (
        'PROVIDER_VERSION = "4.15.1"',
        '"http 402"',
        '"plan_restricted"',
        '"payment_required"',
    )
    missing = [item for item in required if item not in text]
    if missing:
        raise RuntimeError(f"v4.15.1 verification failed; missing: {missing}")


def verify_concurrency(text: str) -> None:
    required = 'return _int("TFB_SYNC_BATCH_CONCURRENCY", 1, 1, 6)'
    if required not in text:
        raise RuntimeError("staged concurrency verification failed")


def main() -> int:
    provider_text = PROVIDER.read_text(encoding="utf-8")
    concurrency_text = CONCURRENCY_MODULE.read_text(encoding="utf-8")
    changed: list[str] = []

    if VERSION_NEW.strip() in provider_text:
        verify_provider(provider_text)
    else:
        provider_text = replace_once(
            provider_text, DOC_OLD, DOC_NEW, "module documentation"
        )
        provider_text = replace_once(
            provider_text, TOKENS_OLD, TOKENS_NEW, "provider unhealthy tokens"
        )
        provider_text = replace_once(
            provider_text, VERSION_OLD, VERSION_NEW, "provider version"
        )
        verify_provider(provider_text)
        PROVIDER.write_text(provider_text, encoding="utf-8")
        changed.append("EODHD HTTP 402 health signal")

    if CONCURRENCY_NEW in concurrency_text:
        verify_concurrency(concurrency_text)
    else:
        concurrency_text = replace_once(
            concurrency_text,
            CONCURRENCY_OLD,
            CONCURRENCY_NEW,
            "batch concurrency default",
        )
        verify_concurrency(concurrency_text)
        CONCURRENCY_MODULE.write_text(concurrency_text, encoding="utf-8")
        changed.append("production concurrency default=1")

    TEST_FILE.write_text(TEST_CONTENT, encoding="utf-8")
    if changed:
        print("Applied: " + "; ".join(changed))
    else:
        print("Hotfixes already applied; verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
