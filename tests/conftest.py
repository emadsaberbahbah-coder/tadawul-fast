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

Notes
-----
- Production env vars on Render are NOT affected -- os.environ.setdefault only
  sets a value if it isn't already present.
- Add new fixtures below as the test suite grows. The _StubEngine pattern
  currently inlined inside test_schema_alignment.py belongs here long-term.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

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

# Engine init -- defer heavy bootstrapping during tests
os.environ.setdefault("INIT_ENGINE_ON_BOOT", "false")
os.environ.setdefault("DEFER_ROUTER_MOUNT", "false")

# Performance budgets -- relax for slower CI machines
os.environ.setdefault("TFB_TEST_SCORE_BUDGET_MS", "200")


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
