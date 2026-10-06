"""Margin publication from the explicit-symbol quote path, with offline inputs."""

from __future__ import annotations

import asyncio
import copy
import socket

import pytest

from core import enriched_quote as enriched


FIELDS = ("gross_margin", "operating_margin", "profit_margin")


@pytest.fixture(autouse=True)
def _deny_external_io(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("margin publication fixtures must not access the network or build an engine")

    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect_ex", forbidden)
    # Keep the real resolver's explicit-engine path; any missing fixture
    # engine must fail before consulting app state or provider factories.
    monkeypatch.setattr(enriched, "resolve_app_state_engine", forbidden)


def _row():
    return {
        "symbol": "DDI.US", "name": "Offline margin fixture",
        "current_price": 13.42, "previous_close": 13.4,
        "market_cap": 656_087_534, "revenue_ttm": 380_043_008,
        "gross_margin": 0.73363996, "operating_margin": 0.38715,
        "profit_margin": 32.908, "overall_score": 71.5,
        "warnings": "yahoo_enrichment_applied; fund_coherence_repaired:profit_margin:x100",
    }


class _FixtureEngine:
    def __init__(self, rows):
        self.rows = rows
        self.calls = 0

    async def get_enriched_quotes(self, **kwargs):
        self.calls += 1
        return copy.deepcopy(self.rows)


def _payload(row):
    engine = _FixtureEngine([row])
    payload = asyncio.run(enriched.build_enriched_sheet_rows_payload(
        engine=engine, page="My_Portfolio", symbols=[row["symbol"]],
        include_matrix=True,
    ))
    assert engine.calls == 1
    return payload


def _normalize(row):
    keys = list(row)
    return enriched.normalize_rows([row], keys, "My_Portfolio")[0]


def test_explicit_symbol_builder_publishes_witnessed_margin_as_fraction(monkeypatch):
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "enforce")
    row = _row()
    original = copy.deepcopy(row)
    payload = _payload(row)
    projected = payload["rows"][0]
    assert projected["profit_margin"] == pytest.approx(0.32908)
    assert projected["gross_margin"] == original["gross_margin"]
    assert projected["operating_margin"] == original["operating_margin"]
    assert projected["overall_score"] == original["overall_score"]
    index = payload["keys"].index("profit_margin")
    assert payload["rows_matrix"][0][index] == pytest.approx(0.32908)
    assert "margin_publish:profit_margin:pts" in projected["warnings"]
    assert row == original, "publication must not mutate the internal engine row"


@pytest.mark.parametrize("mode", [None, "off", "1", "on", "observe"])
def test_disabled_or_observe_mode_preserves_values(monkeypatch, mode):
    if mode is None:
        monkeypatch.delenv("TFB_MARGIN_PUBLISH", raising=False)
    else:
        monkeypatch.setenv("TFB_MARGIN_PUBLISH", mode)
    row = _row()
    result = _payload(row)["rows"][0]
    assert tuple(result[f] for f in FIELDS) == tuple(row[f] for f in FIELDS)
    if mode == "observe":
        assert "margin_publish:profit_margin:pts:observe" in result["warnings"]
        twice = _normalize(result)
        assert twice["warnings"] == result["warnings"]
    else:
        assert result["warnings"] == row["warnings"]


def test_observe_then_enforce_and_repeated_projection_are_idempotent(monkeypatch):
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "observe")
    observed = _normalize(_row())
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "enforce")
    enforced = _normalize(observed)
    assert enforced["profit_margin"] == pytest.approx(0.32908)
    assert _normalize(enforced) == enforced
    already_published = _row()
    already_published["profit_margin"] = 0.32908
    already_published["warnings"] += "; margin_publish:profit_margin:pts"
    assert _normalize(already_published)["profit_margin"] == pytest.approx(0.32908)


@pytest.mark.parametrize("witness", [
    "fund_unit_contract:eodhd:gross_margin",
    "fund_coherence_repaired:gross_margin:x100",
    "fund_coherence_repaired:gross_margin:d100",
])
def test_points_witness_is_field_specific_and_handles_thin_points(monkeypatch, witness):
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "enforce")
    row = _row()
    row.update(gross_margin=0.9, operating_margin=2.25, profit_margin=-2.25, warnings=witness)
    projected = _normalize(row)
    assert projected["gross_margin"] == pytest.approx(0.009)
    assert projected["operating_margin"] == 2.25
    assert projected["profit_margin"] == -2.25
    assert "margin_publish:gross_margin:pts_thin" in projected["warnings"]


@pytest.mark.parametrize("warnings", [
    "", "eodhd_fundamentals_fallback_applied", "yahoo_enrichment_applied",
    "fund_unit_contract:eodhd:profit_margin:observe",
    "fund_coherence_repaired:profit_margin:x100:observe",
    "fund_unit_contract:eodhd:profit_margin_extra",
    "fund_coherence_repaired:profit_margin:unknown",
])
def test_magnitude_and_unproven_tags_cannot_change_margin_units(monkeypatch, warnings):
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "enforce")
    row = _row()
    row.update(gross_margin=2.25, operating_margin=-2.25, profit_margin=32.908, warnings=warnings)
    result = _normalize(row)
    assert tuple(result[f] for f in FIELDS) == tuple(row[f] for f in FIELDS)
    assert "margin_publish:" not in (result["warnings"] or "")


def test_missing_null_and_nonfinite_values_do_not_create_margin_data(monkeypatch):
    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "enforce")
    row = _row()
    row.pop("gross_margin")
    row.update(operating_margin=None, profit_margin=float("nan"))
    result = _normalize(row)
    assert "gross_margin" not in result
    assert result["operating_margin"] is None and result["profit_margin"] is None
    assert "margin_publish:" not in result["warnings"]


@pytest.mark.parametrize("mode,expected", [("off", 32.908), ("observe", 32.908), ("enforce", 0.32908)])
def test_route_engine_direct_fallback_honors_same_witness_and_mode(monkeypatch, mode, expected):
    from routes import enriched_quote as route

    monkeypatch.setenv("TFB_MARGIN_PUBLISH", mode)
    row = _row()
    original = copy.deepcopy(row)
    keys = list(row)
    result = route._normalize_row(keys, keys, row)
    assert result["profit_margin"] == pytest.approx(expected)
    assert result["gross_margin"] == original["gross_margin"]
    assert row == original
    assert route._normalize_row(keys, keys, result) == result


def test_route_fallback_preserves_fraction_above_one_and_missing_bridge(monkeypatch):
    from routes import enriched_quote as route

    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "enforce")
    row = {"symbol": "VALID.US", "profit_margin": 2.25, "warnings": ""}
    assert route._normalize_row(list(row), list(row), row) == row
    # The established fallback remains usable if the bridge module is absent.
    import sys
    monkeypatch.setitem(sys.modules, "core.enriched_quote", None)
    assert route._normalize_row(list(row), list(row), row) == row


def test_route_fallback_converts_witness_after_header_alias_projection(monkeypatch):
    from routes import enriched_quote as route

    monkeypatch.setenv("TFB_MARGIN_PUBLISH", "enforce")
    keys = ["symbol", "profit_margin", "warnings"]
    headers = ["Symbol", "Profit Margin", "Warnings"]
    row = {"Symbol": "DDI.US", "Profit Margin": 32.908,
           "Warnings": "fund_coherence_repaired:profit_margin:x100"}
    projected = route._normalize_row(keys, headers, row)
    assert projected["profit_margin"] == pytest.approx(0.32908)
    assert projected["symbol"] == "DDI.US"
