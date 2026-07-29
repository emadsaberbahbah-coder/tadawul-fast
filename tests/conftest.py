#!/usr/bin/env python3
"""
tests/conftest.py
================================================================================
Pytest shared configuration and fixtures for Tadawul Fast Bridge.
================================================================================
Emad Bahbah -- Tadawul Fast Bridge

Auto-loaded by pytest before any test in tests/ runs. Single source of truth for:
  - sys.path bootstrap (tests can import core/, routes/, integrations/, etc.)
  - environment variable defaults for the test process
  - shared fixtures used across multiple test files

Why this file exists
--------------------
- FIX: tests previously scattered between repo root and tests/ caused
       inconsistent pytest discovery. Centralizing here ensures pytest finds
       and configures every test the same way regardless of invocation cwd.
- FIX: tests calling FastAPI routes used to 401 because REQUIRE_AUTH defaults
       to true. We force open-mode + auth-off here so route tests don't need
       real tokens.
- FIX: providers used to occasionally try real network I/O during unit tests
       depending on import order. We set DISABLE_NETWORK_AT_IMPORT so any
       provider checking that flag stays offline.
- FIX: schema contract tests can inspect routes before FastAPI's lifespan has
       mounted startup-owned routers. The lifecycle-safe hook below invokes the
       same controlled production mounting implementation and then retries
       discovery; genuine route import or mount failures remain visible.

Notes
-----
- Production env vars on Render are NOT affected -- os.environ.setdefault only
  sets a value if it isn't already present.
- Add new fixtures below as the test suite grows. The _StubEngine pattern
  currently inlined inside test_schema_alignment.py belongs here long-term.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

# =============================================================================
# Path bootstrap
# =============================================================================
# Ensures `from core import ...`, `from routes import ...`, `from integrations
# import ...` etc. resolve correctly when pytest is invoked from any directory
# (repo root, tests/, CI runner, IDE test runners).
_REPO_ROOT = Path(__file__).resolve().parent.parent
_REPO_ROOT_STR = str(_REPO_ROOT)
if _REPO_ROOT_STR not in sys.path:
    sys.path.insert(0, _REPO_ROOT_STR)


# =============================================================================
# Test environment defaults
# =============================================================================
# Auth / open mode -- route tests should not need real X-APP-TOKEN values.
os.environ.setdefault("TFB_OPEN_MODE", "true")
os.environ.setdefault("OPEN_MODE", "true")
os.environ.setdefault("REQUIRE_AUTH", "false")
os.environ.setdefault("TFB_REQUIRE_AUTH", "false")

# Environment markers
os.environ.setdefault("APP_ENV", "testing")
os.environ.setdefault("TFB_APP_ENV", "testing")

# Logging -- keep test output clean
os.environ.setdefault("LOG_LEVEL", "WARNING")
os.environ.setdefault("LOG_JSON", "false")

# Network safety -- providers that respect this flag will not hit external
# APIs (Yahoo, EODHD, Finnhub, Argaam, Tadawul) during unit tests.
os.environ.setdefault("DISABLE_NETWORK_AT_IMPORT", "true")
os.environ.setdefault("PROVIDERS_OFFLINE_MODE", "true")

# Engine init -- defer heavy bootstrapping during tests, but keep route mounting
# available because contract tests intentionally validate the mounted API.
os.environ.setdefault("INIT_ENGINE_ON_BOOT", "false")
os.environ.setdefault("DEFER_ROUTER_MOUNT", "false")
os.environ.setdefault("PRESTART_MOUNT_ROUTES", "true")

# Performance budgets -- relax for slower CI machines
os.environ.setdefault("TFB_TEST_SCORE_BUDGET_MS", "200")


# =============================================================================
# Schema-route lifecycle compatibility
# =============================================================================
def pytest_runtest_setup(item: Any) -> None:
    """Make schema endpoint discovery lifecycle-safe without masking failures.

    `test_schema_alignment` historically discovers `/sheet-rows` routes before
    entering `TestClient`, while the application may mount routers during its
    lifespan. If the first lookup is empty, run the same controlled mounting
    implementation used by production and retry. `_mount_routes_controlled` is
    deliberately used rather than `_mount_routes_once`: a partially completed
    prestart attempt may already have set `routes_mounted=True`, and the once
    guard would then return without retrying missing route families.
    """
    module = getattr(item, "module", None)
    if module is None or not str(getattr(module, "__name__", "")).endswith(
        "test_schema_alignment"
    ):
        return

    original = getattr(module, "_find_sheet_rows_endpoints", None)
    if not callable(original) or getattr(original, "_tfb_lifecycle_safe", False):
        return

    def lifecycle_safe_find(app: Any):
        try:
            return original(app)
        except AssertionError as exc:
            if "No GET/POST */sheet-rows endpoint found" not in str(exc):
                raise

        # Importing main is safe in the test environment: network and engine
        # boot are disabled above. Re-run the actual controlled mount directly
        # so a stale/partial `routes_mounted` flag cannot suppress the retry.
        import main as main_mod

        controlled_mount = getattr(main_mod, "_mount_routes_controlled", None)
        if not callable(controlled_mount):
            raise AssertionError(
                "main._mount_routes_controlled is unavailable for contract setup"
            )

        snapshot = controlled_mount(app)
        try:
            app.state.routes_snapshot = snapshot
            app.state.routes_mounted = True
            app.state.routes_mount_phase = "pytest-contract-discovery"
        except Exception:
            pass

        try:
            return original(app)
        except AssertionError as exc:
            diagnostics = {
                "import_errors": dict(snapshot.get("import_errors", {}) or {}),
                "mount_errors": dict(snapshot.get("mount_errors", {}) or {}),
                "no_router": dict(snapshot.get("no_router", {}) or {}),
                "filtered_out_routes": dict(
                    snapshot.get("filtered_out_routes", {}) or {}
                ),
                "mounted": list(snapshot.get("mounted", []) or []),
                "route_family_presence": dict(
                    snapshot.get("route_family_presence", {}) or {}
                ),
            }
            raise AssertionError(
                f"{exc} Controlled-mount diagnostics: "
                f"{json.dumps(diagnostics, ensure_ascii=False, default=str)}"
            ) from exc

    lifecycle_safe_find._tfb_lifecycle_safe = True  # type: ignore[attr-defined]
    setattr(module, "_find_sheet_rows_endpoints", lifecycle_safe_find)


# =============================================================================
# Shared fixtures (add as needed)
# =============================================================================
# Example placeholder -- uncomment and expand when shared fixtures are needed:
#
# import pytest
#
# @pytest.fixture(scope="session")
# def repo_root() -> Path:
#     """Absolute path to the repository root."""
#     return _REPO_ROOT
#
# @pytest.fixture
# def stub_engine():
#     """Lightweight engine stub for route tests that don't need live data."""
#     from tests._stubs import StubEngine  # extract from test_schema_alignment
#     return StubEngine()
