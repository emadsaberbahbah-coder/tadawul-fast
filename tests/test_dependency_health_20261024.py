"""Retire unused NLTK without changing the real news analysis or its evidence.

The presence probe remains a truthful diagnostic. These offline tests execute
the public RSS-to-result path with that probe both available and unavailable,
while rejecting actual NLTK imports. Strict production validation also checks
the resolved environment, rather than only the direct requirements file.
"""
from __future__ import annotations

import asyncio
import builtins
import importlib
import importlib.util
import os
import re
import sys
from dataclasses import replace
from datetime import datetime, timezone
from importlib import metadata
from importlib.machinery import ModuleSpec
from pathlib import Path
from unittest.mock import AsyncMock
from xml.sax.saxutils import escape

import pytest


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 24, 8, tzinfo=timezone.utc)


def _news(monkeypatch, nltk_available):
    """Load independent production state; simulate only the NLTK discovery."""
    name = "_dependency_health_news_" + str(nltk_available)
    spec = importlib.util.spec_from_file_location(name, ROOT / "core/news_intelligence.py")
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    original_find = importlib.util.find_spec

    def find(name, *args, **kwargs):
        if name == "nltk":
            return ModuleSpec("nltk", loader=None) if nltk_available else None
        return original_find(name, *args, **kwargs)

    with monkeypatch.context() as probe:
        probe.setattr(importlib.util, "find_spec", find)
        assert spec.loader is not None
        spec.loader.exec_module(module)
    monkeypatch.setattr(module, "_utc_now", lambda: NOW)
    monkeypatch.setattr(module, "_CONFIG", replace(
        module._CONFIG, allow_network=False, enable_cache=False,
        enable_deep_learning=False, enable_translation=False,
        enable_fuzzy_matching=False, enable_redis=False,
        rss_sources=[], query_mode="google"))
    monkeypatch.setattr(module, "_get_httpx_client", AsyncMock(
        side_effect=AssertionError("dependency parity attempted network")))
    return module


@pytest.fixture
def no_nltk_import(monkeypatch):
    """Catch normal and dynamic imports even when NLTK is already installed."""
    original_import = builtins.__import__
    original_dynamic_import = importlib.import_module
    attempted = []

    def reject(name):
        if name == "nltk" or name.startswith("nltk."):
            attempted.append(name)
            raise AssertionError("production imported retired NLTK")

    def guarded_import(name, *args, **kwargs):
        reject(name)
        return original_import(name, *args, **kwargs)

    def guarded_dynamic_import(name, *args, **kwargs):
        reject(name)
        return original_dynamic_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    monkeypatch.setattr(importlib, "import_module", guarded_dynamic_import)
    yield
    # Production intentionally catches optional-dependency errors; do not let
    # such a fallback hide an attempted import of the retired dependency.
    assert attempted == []


def _run(monkeypatch, module, title, *, ml=False):
    if title is None:
        response = "<rss><channel/></rss>"
    elif title == "FAILED_SOURCE":
        response = "not an RSS document"
    else:
        response = ("<rss><channel><item><title>" + escape(title) + "</title>"
                    "<link>https://www.reuters.com/acme-update</link>"
                    "<pubDate>Sat, 24 Oct 2026 06:00:00 GMT</pubDate>"
                    "<source url='https://www.reuters.com'>Reuters</source>"
                    "</item></channel></rss>")
    fetch = AsyncMock(return_value=response)
    monkeypatch.setattr(module._BACKOFF, "execute_async", fetch)
    if ml:
        # Exercise production optional-model blending without downloading a model.
        model = type("Model", (), {"analyze": AsyncMock(return_value=[0.6])})()
        monkeypatch.setattr(module.DeepLearningSentiment, "get_instance", AsyncMock(return_value=model))
        monkeypatch.setattr(module, "_CONFIG", replace(module._CONFIG, enable_deep_learning=True))
    result = asyncio.run(module.get_news_intelligence({"symbol": "ACME.US", "company_name": "Acme"}))
    fetch.assert_awaited_once()
    # Wall-clock duration is unrelated to predictions or installed capability.
    result.pop("processing_time_ms")
    return result


@pytest.mark.parametrize("title,expected_sign,ml", [
    ("Acme ACME.US strong profit growth upgrade", 1, False),
    ("Acme ACME.US loss decline downgrade", -1, False),
    ("Acme ACME.US fraud investigation scandal", -1, False),
    # Current lexicon is English; disabling translation preserves neutral Arabic.
    ("Acme ACME.US أرباح نمو ارتفاع", 0, False),
    ("Acme ACME.US quarterly company update", 0, False),
    ("Acme ACME.US strong profit growth upgrade", 1, True),
    (None, 0, False),
    ("FAILED_SOURCE", 0, False),
])
def test_real_news_results_are_identical_without_nltk(
        monkeypatch, no_nltk_import, title, expected_sign, ml):
    with_nltk = _news(monkeypatch, True)
    without_nltk = _news(monkeypatch, False)
    assert with_nltk.HAS_NLTK is True and without_nltk.HAS_NLTK is False
    before = _run(monkeypatch, with_nltk, title, ml=ml)
    after = _run(monkeypatch, without_nltk, title, ml=ml)
    assert after == before
    score = after["sentiment"]
    assert (score > 0) - (score < 0) == expected_sign
    expected_status = "EMPTY" if title is None else "ERROR" if title == "FAILED_SOURCE" else "OK"
    assert after["collection_evidence"]["status"] == expected_status
    assert after["articles_analyzed"] == (1 if expected_status == "OK" else 0)
    if ml:
        assert after["breakdown"]["ml_score"] == 0.6


def test_health_truthfully_discloses_nltk_retirement(monkeypatch, no_nltk_import):
    before = asyncio.run(_news(monkeypatch, True).health_check())
    after = asyncio.run(_news(monkeypatch, False).health_check())
    assert before["dependencies"].pop("nltk") is True
    assert after["dependencies"].pop("nltk") is False
    assert after == before
    assert after["status"] == "healthy"


def test_nltk_is_not_a_direct_production_requirement():
    text = (ROOT / "requirements.txt").read_text(encoding="utf-8")
    assert not re.search(r"^\s*nltk\s*(?:\[|[<=>!~@]|$)", text, re.MULTILINE | re.IGNORECASE)


def test_resolved_production_environment_does_not_retain_nltk():
    if os.environ.get("TFB_LIBSMOKE_REQUIRE_PINNED") != "1":
        pytest.skip("set TFB_LIBSMOKE_REQUIRE_PINNED=1 in the resolved production environment")
    from packaging.requirements import Requirement

    installed = {dist.metadata["Name"].lower().replace("_", "-"): dist
                 for dist in metadata.distributions() if dist.metadata["Name"]}
    assert "nltk" not in installed, "retired vulnerable NLTK is still installed"
    consumers = []
    for name, dist in installed.items():
        for raw in dist.requires or ():
            req = Requirement(raw)
            if req.name.lower().replace("_", "-") == "nltk" and (
                    req.marker is None or req.marker.evaluate({"extra": ""})):
                consumers.append((name, raw))
    assert consumers == [], f"production dependencies still require NLTK: {consumers}"
