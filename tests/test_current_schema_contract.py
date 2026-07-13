#!/usr/bin/env python3
"""Regression tests for the *current* deployed sheet contract.

Why this file exists
--------------------
Several older smoke/compatibility tests still carry historical 80/83-column
baselines.  The production registry is now v2.15.x and is the only authority.
This suite deliberately locks the accepted v2.15 physical-sheet contract so a
truncated response cannot pass merely because it still exceeds an old minimum.

These tests are network-free and read only ``core.sheets.schema_registry``.
"""
from __future__ import annotations

from collections import Counter
from typing import Dict, Iterable, List

from core.sheets import schema_registry as sr


EXPECTED_WIDTHS: Dict[str, int] = {
    "Market_Leaders": 115,
    "Global_Markets": 115,
    "Commodities_FX": 115,
    "Mutual_Funds": 115,
    "My_Portfolio": 122,
    "Top_10_Investments": 118,
    "Insights_Analysis": 7,
    "Data_Dictionary": 9,
}

MARKET_PAGES = (
    "Market_Leaders",
    "Global_Markets",
    "Commodities_FX",
    "Mutual_Funds",
)

INSTRUMENT_PAGES = MARKET_PAGES + ("My_Portfolio", "Top_10_Investments")

INVESTABILITY_GATE_KEYS = {
    "data_quality_score",
    "forecast_reliability_score",
    "provider_engine_conflict",
    "conflict_type",
    "final_decision_basis",
    "investability_status",
    "final_action",
    "block_reason",
}

MARKET_V215_KEYS = {
    "analyst_rating",
    "target_price",
    "upside_downside_pct",
    "signal",
    "trend_1m",
    "trend_3m",
    "trend_12m",
    "st_signal",
    "provider_secondary",
    "row_source",
}

TOP10_KEYS = {"top10_rank", "selection_reason", "criteria_snapshot"}

MY_PORTFOLIO_DECISION_KEYS = {
    "buy_date",
    "target_weight",
    "actual_weight",
    "weight_gap",
    "action_flag",
    "decision",
    "user_notes",
}


def _headers(page: str) -> List[str]:
    return list(sr.get_sheet_headers(page))


def _keys(page: str) -> List[str]:
    return list(sr.get_sheet_keys(page))


def _duplicates(values: Iterable[str]) -> List[str]:
    counts = Counter(str(v).strip().casefold() for v in values)
    return sorted(k for k, n in counts.items() if k and n > 1)


def test_schema_registry_matches_accepted_v215_widths() -> None:
    actual = {page: len(_keys(page)) for page in EXPECTED_WIDTHS}
    assert actual == EXPECTED_WIDTHS, (
        "Sheet contract changed. Review every route/writer/auditor before "
        f"accepting the new widths. expected={EXPECTED_WIDTHS}, actual={actual}"
    )


def test_headers_and_keys_are_exactly_aligned_and_unique() -> None:
    for page, expected_width in EXPECTED_WIDTHS.items():
        headers = _headers(page)
        keys = _keys(page)
        assert len(headers) == expected_width, (page, len(headers), expected_width)
        assert len(keys) == expected_width, (page, len(keys), expected_width)
        assert len(headers) == len(keys), (page, len(headers), len(keys))
        assert not _duplicates(headers), f"{page}: duplicate headers {_duplicates(headers)}"
        assert not _duplicates(keys), f"{page}: duplicate keys {_duplicates(keys)}"
        assert all(str(h).strip() for h in headers), f"{page}: blank header"
        assert all(str(k).strip() for k in keys), f"{page}: blank key"


def test_every_instrument_page_carries_the_full_investability_gate() -> None:
    for page in INSTRUMENT_PAGES:
        missing = INVESTABILITY_GATE_KEYS.difference(_keys(page))
        assert not missing, f"{page}: missing investability keys {sorted(missing)}"


def test_market_pages_carry_the_v215_physical_analyst_trend_layout() -> None:
    for page in MARKET_PAGES:
        keys = set(_keys(page))
        missing = MARKET_V215_KEYS.difference(keys)
        assert not missing, f"{page}: missing v2.15 physical-sheet keys {sorted(missing)}"


def test_top10_special_fields_are_present_once() -> None:
    keys = _keys("Top_10_Investments")
    for key in TOP10_KEYS:
        assert keys.count(key) == 1, f"Top_10_Investments: {key!r} count={keys.count(key)}"


def test_my_portfolio_decision_layer_is_not_truncated() -> None:
    keys = set(_keys("My_Portfolio"))
    missing = MY_PORTFOLIO_DECISION_KEYS.difference(keys)
    assert not missing, f"My_Portfolio: missing decision keys {sorted(missing)}"


MARKET_TAIL_KEYS = [
    "block_reason",
    "data_provider",
    "provider_secondary",
    "last_updated_utc",
    "last_updated_riyadh",
    "row_source",
    "warnings",
]

MARKET_TAIL_HEADERS = [
    "Block Reason",
    "Data Provider",
    "Provider Secondary",
    "Last Updated (UTC)",
    "Last Updated (Riyadh)",
    "Row Source",
    "Warnings",
]


def test_market_and_top10_tail_is_the_governed_decision_block() -> None:
    # Accepted v2.15 physical layout (verified against the live registry,
    # 2026-07-13): the governance block ENDS at block_reason and is followed
    # by the provenance tail (data_provider .. warnings); `warnings` is the
    # last market contract field. The original draft asserted block_reason
    # last and was red against production from day one — this is the exact
    # tail lock instead, in the file's own lock-the-contract style. Top_10
    # appends its three selector fields after the full market tail.
    for page in MARKET_PAGES:
        assert _keys(page)[-7:] == MARKET_TAIL_KEYS, (page, _keys(page)[-7:])
        assert _headers(page)[-7:] == MARKET_TAIL_HEADERS, (page, _headers(page)[-7:])

    top10_keys = _keys("Top_10_Investments")
    top10_headers = _headers("Top_10_Investments")
    assert top10_keys[-10:] == MARKET_TAIL_KEYS + [
        "top10_rank", "selection_reason", "criteria_snapshot",
    ], top10_keys[-10:]
    assert top10_headers[-10:] == MARKET_TAIL_HEADERS + [
        "Top 10 Rank", "Selection Reason", "Criteria Snapshot",
    ], top10_headers[-10:]
