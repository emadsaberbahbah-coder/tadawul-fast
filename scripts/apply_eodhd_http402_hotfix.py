#!/usr/bin/env python3
"""Apply the EODHD HTTP 402 provider-unhealthy hotfix exactly once.

This transformer is intentionally assertion-based. It refuses to modify the
provider unless the expected v4.15.0 source blocks match exactly. Re-running it
on v4.15.1 is a no-op after verification.
"""

from __future__ import annotations

from pathlib import Path

PROVIDER = Path("core/providers/eodhd_provider.py")
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

TEST_CONTENT = '''"""Regression tests for EODHD HTTP 402 provider-health signaling."""

from core.providers import eodhd_provider as eodhd


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
'''


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one source block, found {count}")
    return text.replace(old, new, 1)


def verify_applied(text: str) -> None:
    required = (
        'PROVIDER_VERSION = "4.15.1"',
        '"http 402"',
        '"plan_restricted"',
        '"payment_required"',
    )
    missing = [item for item in required if item not in text]
    if missing:
        raise RuntimeError(f"v4.15.1 verification failed; missing: {missing}")


def main() -> int:
    text = PROVIDER.read_text(encoding="utf-8")

    if VERSION_NEW.strip() in text:
        verify_applied(text)
        changed = False
    else:
        text = replace_once(text, DOC_OLD, DOC_NEW, "module documentation")
        text = replace_once(text, TOKENS_OLD, TOKENS_NEW, "provider unhealthy tokens")
        text = replace_once(text, VERSION_OLD, VERSION_NEW, "provider version")
        verify_applied(text)
        PROVIDER.write_text(text, encoding="utf-8")
        changed = True

    TEST_FILE.write_text(TEST_CONTENT, encoding="utf-8")
    print(
        "EODHD HTTP 402 hotfix: "
        + ("applied" if changed else "already applied; verified")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
