#!/usr/bin/env python3
# tests/test_identity_guard.py
"""
Regression tests for core/analysis/identity_guard.py

Every case here is drawn from the 2026-07-20 incident, so a failure means the
defect that blanked 608 rows has come back. The asymmetry matters: clearing a
contaminated row is a nuisance, clearing a healthy one is data loss, so the
"must survive" tests are the important ones.

Run:  python3 -m pytest tests/test_identity_guard.py -v
      python3 tests/test_identity_guard.py          (no pytest needed)
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.analysis.identity_guard import (  # noqa: E402
    Action,
    Reason,
    currency_is_consistent,
    expected_currency_for,
    guard_sheet_rows,
    price_band_applies,
    price_is_plausible,
)

FAILED = "quote_current_price_missing; low_data_trust"


def _row(symbol, name="", currency="USD", price=None, warnings="",
         asset_class="Equity", updated="2026-07-25T09:00:00+00:00"):
    return {
        "Symbol": symbol,
        "Name": name,
        "Asset Class": asset_class,
        "Currency": currency,
        "Current Price": price,
        "Last Updated (UTC)": updated,
        "Warnings": warnings,
        "Block Reason": "",
        "Investability Status": "INVESTABLE",
    }


def _intact(plan):
    return {r["Symbol"] for r in plan.rows if r.get("Name")}


def _cleared(plan):
    return {r["Symbol"] for r in plan.rows if not r.get("Name")}


def _dropped(plan):
    return {f.symbol for f in plan.by_action(Action.DROP_ROW)}


# ---------------------------------------------------------------------------
# venue / currency
# ---------------------------------------------------------------------------


def test_expected_currency_from_suffix():
    assert expected_currency_for("7203.T") == "JPY"
    assert expected_currency_for("2222.SR") == "SAR"
    assert expected_currency_for("HSBA.L") == "GBX"
    assert expected_currency_for("AAPL.US") == "USD"
    assert expected_currency_for("AAPL") is None  # bare ticker: unverifiable


def test_gbx_and_gbp_are_the_same_venue():
    assert currency_is_consistent("HSBA.L", "GBX") is True
    assert currency_is_consistent("HSBA.L", "GBP") is True
    assert currency_is_consistent("7203.T", "USD") is False
    assert currency_is_consistent("AAPL", "USD") is None


# ---------------------------------------------------------------------------
# price magnitude
# ---------------------------------------------------------------------------


def test_price_band_skips_non_equities():
    """SHIB at 4.1e-06 and JPYCHF at 0.005 are correct prices, not corruption."""
    assert price_band_applies("SHIB-USD", "Crypto") is False
    assert price_band_applies("JPYCHF=X", "FX_INSTRUMENT") is False
    assert price_band_applies("^N225", "Index") is False
    assert price_band_applies("AAPL.US", "Equity") is True


def test_berkshire_class_a_is_not_implausible():
    """BRK-A really does trade near 738,000 USD -- the band must not flag it."""
    assert price_is_plausible("USD", 738_500.0, symbol="BRK-A.US",
                              asset_class="Equity") is True


def test_usd_magnitude_on_a_jpy_line_is_implausible():
    """A Tokyo listing at 2.71 JPY is a USD number wearing a JPY label."""
    assert price_is_plausible("JPY", 2.71, symbol="2899.T",
                              asset_class="Equity") is False
    assert price_is_plausible("JPY", 8.21, symbol="4485.T",
                              asset_class="Equity") is False
    assert price_is_plausible("JPY", 2909.0, symbol="7203.T",
                              asset_class="Equity") is True


def test_usd_ceiling_is_deliberately_above_bk_to_protect_brk_a():
    """
    Documented trade-off, not an oversight.

    BK's borrowed 979,000 "USD" (a KRW number) sits BELOW the USD ceiling,
    because the ceiling has to clear BRK-A at ~738,000. Magnitude therefore
    cannot catch BK -- and does not need to: BK carries a failed quote plus a
    borrowed name, so the BORROWED_IDENTITY signature catches it instead.
    Narrowing the ceiling to catch BK would destroy Berkshire Class A, which is
    the strictly worse error.
    """
    assert price_is_plausible("USD", 979_000.0, symbol="BK.US",
                              asset_class="Equity") is True

    # In the live sheet the true owner of the borrowed name is present, so the
    # BORROWED_IDENTITY signature fires and BK is cleared.
    rows = [
        _row("012450.KS", "Hanwha Aerospace Co., Ltd.", "KRW", 979_000.0),
        _row("BK.US", "Bank of New York Mellon Corp", "USD", 137.16),
        _row("BK", "Hanwha Aerospace Co., Ltd.", "USD", 979_000.0, FAILED),
    ]
    plan = guard_sheet_rows(rows, sheet="Market_Leaders")
    assert _cleared(plan) == {"BK"}, "caught by identity, not by magnitude"
    assert _intact(plan) == {"012450.KS", "BK.US"}


def test_known_limitation_borrow_is_invisible_without_the_owner_row():
    """
    Documents a residual gap rather than asserting it away.

    If the row that legitimately owns the borrowed name is NOT in the same
    sheet, name matching has nothing to compare against, and a borrowed price
    inside the currency band slips through. BK at 979,000 "USD" is the worked
    example: it clears the USD ceiling (which must stay above BRK-A's 738,000)
    and its true owner 012450.KS lives on a different page.

    This is not fixable from the sheet alone -- the provider layer must reject
    any record whose returned symbol differs from the requested symbol. The
    guard reduces the blast radius; it does not replace that fix.
    """
    rows = [
        _row("BK.US", "Bank of New York Mellon Corp", "USD", 137.16),
        _row("BK", "Hanwha Aerospace Co., Ltd.", "USD", 979_000.0, FAILED),
    ]
    plan = guard_sheet_rows(rows, sheet="Market_Leaders")
    assert plan.counts() == {}, "known gap: no owner row, so nothing to match"
    assert "BK" in _intact(plan)


def test_magnitude_alone_never_clears_a_row():
    """
    Requires a failed quote as corroboration. BRK-A.US had a healthy quote and
    a 738k price; the borrowed rows all carried quote_current_price_missing.
    """
    healthy = _row("BRK-A.US", "Berkshire Hathaway Inc.", "USD", 738_500.0)
    plan = guard_sheet_rows([healthy], sheet="t")
    assert plan.counts() == {}
    assert _intact(plan) == {"BRK-A.US"}


# ---------------------------------------------------------------------------
# the leak: borrowed identity
# ---------------------------------------------------------------------------


def test_borrowed_name_is_caught_and_owner_is_untouched():
    """DTI.US owns the name; FI.US borrowed it after its quote failed."""
    rows = [
        _row("DTI.US", "Drilling Tools International Corporation", "USD", 2.31),
        _row("FI.US", "Drilling Tools International Corporation", "USD", 88.8, FAILED),
    ]
    plan = guard_sheet_rows(rows, sheet="Market_Leaders")
    assert _intact(plan) == {"DTI.US"}, "the healthy owner must keep its data"
    assert _cleared(plan) == {"FI.US"}
    finding = plan.by_action(Action.QUARANTINE_FIELDS)[0]
    assert finding.reason == Reason.BORROWED_IDENTITY
    assert finding.borrowed_from == "DTI.US"


def test_cross_currency_borrow_is_caught():
    """7205.T carrying Acme United's USD price under a JPY label."""
    rows = [
        _row("ACU.US", "Acme United Corporation", "USD", 45.90),
        _row("7205.T", "Acme United Corporation", "JPY", 47.00, FAILED),
    ]
    plan = guard_sheet_rows(rows, sheet="Global_Markets")
    assert _intact(plan) == {"ACU.US"}
    assert _cleared(plan) == {"7205.T"}


def test_contaminated_row_survives_as_a_refetchable_stub():
    """
    It must NOT vanish. The symbol stays in the sheet with an explicit reason so
    it can be re-fetched -- unlike the old firewall, which left an unexplained
    blank indistinguishable from a provider outage.
    """
    rows = [
        _row("GT.US", "The Goodyear Tire & Rubber Company", "USD", 11.20),
        _row("BRK-B", "The Goodyear Tire & Rubber Company", "USD", 398.37, FAILED),
    ]
    plan = guard_sheet_rows(rows, sheet="Market_Leaders")
    stub = next(r for r in plan.rows if r["Symbol"] == "BRK-B")
    assert stub["Name"] is None and stub["Current Price"] is None
    assert stub["Symbol"] == "BRK-B", "symbol must be preserved"
    assert "re-fetch" in stub["Block Reason"].lower()
    assert stub["Investability Status"] == "BLOCKED"
    assert "identity_guard" in stub["Warnings"]
    assert plan.refetch_symbols() == ["BRK-B"]


# ---------------------------------------------------------------------------
# the amplifier: name-only grouping must never fire
# ---------------------------------------------------------------------------


def test_share_classes_all_survive():
    """Liberty Global A/B/K: one issuer name, three distinct securities."""
    rows = [
        _row("LBTYA.US", "Liberty Global Ltd.", "USD", 9.50),
        _row("LBTYB.US", "Liberty Global Ltd.", "USD", 10.10),
        _row("LBTYK.US", "Liberty Global Ltd.", "USD", 9.20),
    ]
    plan = guard_sheet_rows(rows, sheet="Global_Markets")
    assert len(_intact(plan)) == 3
    assert plan.counts() == {}


def test_preferred_series_all_survive():
    rows = [
        _row("AFGB.US", "American Financial Group, Inc.", "USD", 24.2),
        _row("AFGC.US", "American Financial Group, Inc.", "USD", 23.8),
        _row("AFGD.US", "American Financial Group, Inc.", "USD", 22.5),
        _row("AFGE.US", "American Financial Group, Inc.", "USD", 21.9),
    ]
    plan = guard_sheet_rows(rows, sheet="Global_Markets")
    assert len(_intact(plan)) == 4


def test_three_way_cross_listing_all_survive():
    rows = [
        _row("0005.HK", "HSBC Holdings plc", "HKD", 98.5),
        _row("HSBA.L", "HSBC Holdings plc", "GBX", 985.0),
        _row("HSBC", "HSBC Holdings plc", "USD", 62.3),
    ]
    plan = guard_sheet_rows(rows, sheet="Global_Markets")
    assert len(_intact(plan)) == 3


def test_healthy_blue_chips_are_never_touched():
    """These were all blanked on 2026-07-25. None may be touched again."""
    rows = [
        _row("V.US", "Visa Inc.", "USD", 329.21),
        _row("JPM", "JPMorgan Chase & Co.", "USD", 341.10),
        _row("PEP.US", "PepsiCo, Inc.", "USD", 148.90),
        _row("IDXX", "IDEXX Laboratories, Inc.", "USD", 512.40),
        _row("WFC.US", "Wells Fargo & Company", "USD", 84.15),
    ]
    plan = guard_sheet_rows(rows, sheet="Market_Leaders")
    assert len(_intact(plan)) == 5
    assert plan.counts() == {}


# ---------------------------------------------------------------------------
# genuine duplicates still collapse
# ---------------------------------------------------------------------------


def test_genuine_suffix_duplicate_still_collapses():
    rows = [
        _row("AAPL", "Apple Inc.", "USD", 333.02),
        _row("AAPL.US", "Apple Inc.", "USD", 333.02),
    ]
    plan = guard_sheet_rows(rows, sheet="Market_Leaders")
    assert len(plan.rows) == 1
    assert len(_dropped(plan)) == 1


def test_pre_existing_blank_shell_is_removed_and_reported():
    rows = [
        _row("AAPL", "Apple Inc.", "USD", 333.02),
        {"Symbol": "V.US", "Warnings": "identity_quarantined:v6.24.0"},
    ]
    plan = guard_sheet_rows(rows, sheet="Market_Leaders")
    assert {r["Symbol"] for r in plan.rows} == {"AAPL"}
    reasons = {f.reason for f in plan.by_action(Action.DROP_ROW)}
    assert "pre_existing_blank_shell" in reasons


# ---------------------------------------------------------------------------
# the guardrail the old firewall lacked
# ---------------------------------------------------------------------------


def test_guard_refuses_to_alter_more_than_a_quarter_of_a_sheet():
    """
    The 2026-07-20 run blanked 9.5% of Market_Leaders with no alarm. Anything
    touching >25% is a defect in this module and must fail loudly rather than
    write.
    """
    rows = [_row("OWNER.US", "Shared Name Inc.", "USD", 10.0)]
    rows += [
        _row(f"BAD{i}.US", "Shared Name Inc.", "USD", 10.0 + i * 0.01, FAILED)
        for i in range(40)
    ]
    try:
        guard_sheet_rows(rows, sheet="Market_Leaders")
    except RuntimeError as exc:
        assert "refused to write" in str(exc)
    else:
        raise AssertionError("guard should have refused this run")


def test_small_sheets_are_exempt_from_the_ratio_guard():
    rows = [
        _row("OWNER.US", "Shared Name Inc.", "USD", 10.0),
        _row("BAD.US", "Shared Name Inc.", "USD", 10.1, FAILED),
    ]
    plan = guard_sheet_rows(rows, sheet="tiny")
    assert len(plan.rows) == 2


# ---------------------------------------------------------------------------
# contract
# ---------------------------------------------------------------------------


def test_does_not_mutate_caller_rows():
    rows = [
        _row("GT.US", "The Goodyear Tire & Rubber Company", "USD", 11.20),
        _row("BRK-B", "The Goodyear Tire & Rubber Company", "USD", 398.37, FAILED),
    ]
    snapshot = [dict(r) for r in rows]
    guard_sheet_rows(rows, sheet="t")
    assert rows == snapshot


def test_empty_input():
    plan = guard_sheet_rows([], sheet="t")
    assert plan.rows == [] and plan.findings == []


def test_summary_is_one_line():
    rows = [
        _row("GT.US", "Goodyear", "USD", 11.20),
        _row("BRK-B", "Goodyear", "USD", 398.37, FAILED),
    ]
    summary = guard_sheet_rows(rows, sheet="t").summary()
    assert "\n" not in summary and "identity_guard" in summary


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
