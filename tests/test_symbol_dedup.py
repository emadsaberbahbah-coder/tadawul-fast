#!/usr/bin/env python3
# tests/test_symbol_dedup.py
"""
Tests for core/analysis/symbol_dedup.py

The critical property under test is asymmetric: removing a real duplicate is a
convenience, but deleting a distinct issuer is data loss. So the "must survive"
cases matter more than the "must collapse" cases, and each one is drawn from a
real collision observed in the live universe.

Run:  python3 -m pytest tests/test_symbol_dedup.py -v
      python3 tests/test_symbol_dedup.py          (no pytest needed)
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.analysis.symbol_dedup import (  # noqa: E402
    DedupVerdict,
    base_symbol_of,
    dedupe_symbol_rows,
    drop_quarantined_shells,
    identity_key,
    normalise_company_name,
    resolve_identity,
)


def _row(symbol, name="", currency="", price=None, updated="", warnings=""):
    return {
        "Symbol": symbol,
        "Name": name,
        "Currency": currency,
        "Current Price": price,
        "Last Updated (UTC)": updated,
        "Warnings": warnings,
    }


def _symbols(result):
    return {r["Symbol"] for r in result.rows}


# ---------------------------------------------------------------------------
# identity primitives
# ---------------------------------------------------------------------------


def test_base_symbol_strips_only_real_exchange_suffixes():
    assert base_symbol_of("AAPL.US") == "AAPL"
    assert base_symbol_of("AAPL") == "AAPL"
    assert base_symbol_of("1211.HK") == "1211"
    # share class is not an exchange
    assert base_symbol_of("BRK.B") == "BRK.B"
    assert base_symbol_of("BRK.B.US") == "BRK.B"
    # KSA stays canonical so 2222.SR never collides with a bare 2222
    assert base_symbol_of("2222.SR") == "2222.SR"
    # specials untouched
    assert base_symbol_of("^N225") == "^N225"
    assert base_symbol_of("BTC-USD") == "BTC-USD"


def test_company_name_normalisation_ignores_legal_form():
    assert normalise_company_name("Apple Inc.") == normalise_company_name("Apple Inc")
    assert normalise_company_name("Airbnb, Inc.") == normalise_company_name("Airbnb Inc")
    assert normalise_company_name("Autoliv, Inc.") != normalise_company_name("Allianz SE")
    assert normalise_company_name("") == ""
    assert normalise_company_name(None) == ""


def test_identity_key_is_base_name_currency():
    assert identity_key(_row("NTES", "NetEase, Inc.", "USD", 119.55)) == identity_key(
        _row("NTES.US", "NetEase Inc", "USD", 129.55)
    )


def test_row_field_aliases_accept_snake_case_engine_rows():
    identity = resolve_identity(
        {"symbol": "aapl.us", "name": "Apple Inc.", "currency": "usd", "current_price": "333.02"}
    )
    assert identity.symbol == "AAPL.US"
    assert identity.base == "AAPL"
    assert identity.currency == "USD"
    assert identity.price == 333.02


# ---------------------------------------------------------------------------
# the property that matters: distinct issuers must never be merged
# ---------------------------------------------------------------------------


def test_distinct_issuers_sharing_a_ticker_root_all_survive():
    rows = [
        _row("1211.SR", "Saudi Arabian Mining Company (Maaden)", "SAR", 56.15),
        _row("1211.HK", "BYD Company Limited", "HKD", 90.95),
        _row("AAL.L", "Anglo American plc", "GBX", 3400.0),
        _row("AAL.US", "American Airlines Group Inc.", "USD", 15.60),
        _row("ALV.US", "Autoliv, Inc.", "USD", 117.51),
        _row("ALV.DE", "Allianz SE", "EUR", 419.60),
        _row("7203.T", "Toyota Motor Corporation", "JPY", 2909.0),
        _row("7203.SR", "Elm Company", "SAR", 643.0),
    ]
    result = dedupe_symbol_rows(rows)
    assert result.removed_count == 0, "distinct issuers must never be removed"
    assert len(result.rows) == 8
    # Only AAL.* and ALV.* actually collide on the base ticker. The two Saudi
    # pairs never reach the collision path at all, because base_symbol_of()
    # preserves the canonical .SR suffix -- see the test below.
    assert len(result.by_verdict(DedupVerdict.DISTINCT_ISSUER)) == 2


def test_ksa_codes_cannot_collide_with_foreign_numeric_tickers():
    """
    1211.SR keeps its suffix while 1211.HK reduces to 1211, so Tadawul numeric
    codes are structurally incapable of colliding with HK/JP/TW numeric
    tickers. This is why Maaden/BYD and Toyota/Elm are safe by construction
    rather than by name comparison.
    """
    assert base_symbol_of("1211.SR") != base_symbol_of("1211.HK")
    assert base_symbol_of("7203.SR") != base_symbol_of("7203.T")
    rows = [
        _row("2222.SR", "Saudi Arabian Oil Company", "SAR", 26.84),
        _row("2222.HK", "Some Hong Kong Issuer", "HKD", 12.0),
    ]
    result = dedupe_symbol_rows(rows)
    assert result.removed_count == 0
    assert result.counts() == {}, "no collision should even be reported"


def test_three_way_distinct_issuer_collision_survives():
    rows = [
        _row("AC.TO", "Air Canada", "CAD", 23.245),
        _row("AC.PA", "Accor SA", "EUR", 47.25),
        _row("AC.MX", "Arca Continental, S.A.B. de C.V.", "MXN", 196.29),
    ]
    result = dedupe_symbol_rows(rows)
    assert result.removed_count == 0
    assert len(result.rows) == 3


# ---------------------------------------------------------------------------
# real duplicates collapse, keeping the freshest row
# ---------------------------------------------------------------------------


def test_suffix_twin_collapses_to_one_row():
    rows = [
        _row("AAPL", "Apple Inc.", "USD", 333.02, "2026-07-25T09:00:00+00:00"),
        _row("AAPL.US", "Apple Inc.", "USD", 333.02, "2026-07-25T09:00:00+00:00"),
    ]
    result = dedupe_symbol_rows(rows)
    assert result.output_count == 1
    assert len(result.by_verdict(DedupVerdict.DUPLICATE)) == 1


def test_keeps_the_freshest_row_not_the_first():
    stale = _row("NTES.US", "NetEase, Inc.", "USD", 129.55, "2026-07-22T10:38:34+00:00")
    fresh = _row("NTES", "NetEase, Inc.", "USD", 119.55, "2026-07-25T10:27:25+00:00")
    result = dedupe_symbol_rows([stale, fresh])
    assert _symbols(result) == {"NTES"}
    assert result.rows[0]["Current Price"] == 119.55


def test_stale_twin_is_flagged_even_though_prices_differ():
    """The regression this module exists for: price equality would miss this."""
    rows = [
        _row("DECK", "Deckers Outdoor Corporation", "USD", 102.47, "2026-07-16T00:00:00+00:00"),
        _row("DECK.US", "Deckers Outdoor Corporation", "USD", 108.95, "2026-07-25T00:00:00+00:00"),
    ]
    result = dedupe_symbol_rows(rows)
    assert result.output_count == 1
    finding = result.by_verdict(DedupVerdict.DUPLICATE)[0]
    assert finding.price_spread is not None and finding.price_spread > 0.02
    assert "stale twin" in finding.note
    assert _symbols(result) == {"DECK.US"}  # the fresher one


# ---------------------------------------------------------------------------
# cross-listings are kept but tagged
# ---------------------------------------------------------------------------


def test_cross_listing_keeps_both_and_tags_single_exposure():
    rows = [
        _row("AEM.TO", "Agnico Eagle Mines Limited", "CAD", 206.89),
        _row("AEM", "Agnico Eagle Mines Limited", "USD", 147.05),
    ]
    result = dedupe_symbol_rows(rows)
    assert result.output_count == 2, "different currencies are separate listings"
    finding = result.by_verdict(DedupVerdict.CROSS_LISTING)[0]
    assert "ONE exposure" in finding.note


# ---------------------------------------------------------------------------
# quarantined shells — the empty-row bug
# ---------------------------------------------------------------------------


def test_shell_with_live_twin_is_dropped_as_duplicate():
    rows = [
        _row("YUMC", "Yum China Holdings, Inc.", "USD", 43.40, "2026-07-25T00:00:00+00:00"),
        _row("YUMC.US", warnings="identity_quarantined:name_dedup"),
    ]
    result = dedupe_symbol_rows(rows)
    assert _symbols(result) == {"YUMC"}
    assert len(result.by_verdict(DedupVerdict.SHELL_WITH_TWIN)) == 1


def test_orphan_shell_is_reported_for_refetch():
    """90% of live quarantined rows are orphans — data destroyed for nothing."""
    rows = [
        _row("V.US", warnings="identity_quarantined:v6.24.0"),
        _row("JPM", warnings="identity_quarantined:v6.24.0"),
    ]
    result = dedupe_symbol_rows(rows)
    assert result.output_count == 0
    assert len(result.by_verdict(DedupVerdict.SHELL_ORPHAN)) == 2
    assert set(result.orphan_symbols()) == {"V.US", "JPM"}


def test_both_twins_blanked_is_reported_as_orphan_not_silently_dropped():
    rows = [
        _row("BABA", warnings="identity_quarantined:v6.24.0"),
        _row("BABA.US", warnings="identity_quarantined:v6.24.0"),
    ]
    result = dedupe_symbol_rows(rows)
    assert result.output_count == 0
    assert "BABA" in result.orphan_symbols() and "BABA.US" in result.orphan_symbols()


def test_drop_shells_false_preserves_rows_for_debugging():
    rows = [_row("V.US", warnings="identity_quarantined:v6.24.0")]
    result = dedupe_symbol_rows(rows, drop_shells=False)
    assert result.output_count == 1


# ---------------------------------------------------------------------------
# fallback quotes are flagged, never removed
# ---------------------------------------------------------------------------


def test_fallback_quote_is_flagged_but_row_kept():
    """BK -> 'Hanwha Aerospace @ 979,000 USD' reached the sheet looking normal."""
    rows = [
        _row("BK", "Hanwha Aerospace Co., Ltd.", "USD", 979000.0,
             warnings="quote_current_price_missing; low_data_trust")
    ]
    result = dedupe_symbol_rows(rows)
    assert result.output_count == 1, "flagging must not delete data"
    assert len(result.by_verdict(DedupVerdict.SUSPECT_QUOTE)) == 1


# ---------------------------------------------------------------------------
# contract / robustness
# ---------------------------------------------------------------------------


def test_input_order_is_preserved():
    rows = [_row(s, f"Company {s}", "USD", 1.0) for s in ("CCC", "AAA", "BBB")]
    result = dedupe_symbol_rows(rows)
    assert [r["Symbol"] for r in result.rows] == ["CCC", "AAA", "BBB"]


def test_does_not_mutate_caller_rows():
    rows = [_row("AAPL", "Apple Inc.", "USD", 333.02)]
    snapshot = [dict(r) for r in rows]
    dedupe_symbol_rows(rows)
    assert rows == snapshot


def test_empty_and_degenerate_input():
    assert dedupe_symbol_rows([]).output_count == 0
    assert dedupe_symbol_rows([{}]).output_count in (0, 1)  # no symbol -> shell
    assert drop_quarantined_shells([]).output_count == 0


def test_missing_timestamps_fall_back_to_completeness():
    sparse = _row("MSFT.US", "Microsoft Corporation", "USD", 500.0)
    rich = dict(_row("MSFT", "Microsoft Corporation", "USD", 500.0))
    rich.update({"Sector": "Information Technology", "P/E (TTM)": 35.0, "RSI (14)": 55.0})
    result = dedupe_symbol_rows([sparse, rich])
    assert _symbols(result) == {"MSFT"}, "richer row wins when timestamps are absent"


def test_warning_summary_is_a_single_line():
    rows = [
        _row("AAPL", "Apple Inc.", "USD", 333.02),
        _row("AAPL.US", "Apple Inc.", "USD", 333.02),
    ]
    summary = dedupe_symbol_rows(rows).warning_summary()
    assert "\n" not in summary and "dedup v" in summary


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS  {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL  {name}: {exc}")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"ERROR {name}: {type(exc).__name__}: {exc}")
    print(f"\n{'FAILED' if failures else 'ALL PASSED'} ({failures} failure(s))")
    sys.exit(1 if failures else 0)
