"""Actual Insights builder: snapshot membership and bounded research receipts."""
import asyncio
from copy import deepcopy
from unittest.mock import patch

from core.analysis import insights_builder as builder


class Engine:
    def __init__(self, holdings=None, quotes=None):
        self.holdings = holdings if holdings is not None else [{"symbol": "DDI.US"}]
        self.quotes = quotes or {}
        self.calls = []

    async def get_cached_sheet_snapshot(self, page):
        assert page == "My_Portfolio"
        return deepcopy(self.holdings)

    async def list_symbols_for_page(self, page):
        raise AssertionError("Emergency membership must not be used")

    async def get_enriched_quotes_batch(self, symbols, **kwargs):
        self.calls.append(list(symbols))
        return {sym: deepcopy(self.quotes.get(sym, {"symbol": sym, "current_price": 10}))
                for sym in symbols}


def build(engine, **kwargs):
    criteria = {"include_top_opportunities": False, "include_risk_scenarios": False,
                "include_short_term": False, **kwargs.pop("criteria", {})}
    return asyncio.run(builder.build_insights_analysis_rows(
        engine=engine, symbols=["AAPL", "NVDA"], criteria=criteria,
        include_top10_section=False, **kwargs,
    ))


def test_explicit_symbols_include_snapshot_holdings_first_without_claiming_custody():
    engine = Engine(holdings=[{"symbol": "DDI.US"}, {"symbol": "DDI.US"}])
    payload = build(engine)
    assert engine.calls[0] == ["DDI.US"]
    receipt = payload["meta"]["coverage_receipt"]
    assert receipt["portfolio_membership"] == "cached_snapshot_research_only"
    assert receipt["cohorts"]["My_Portfolio"]["requested"] == ["DDI.US"]
    assert receipt["cohorts"]["Selected Symbols"]["requested"] == ["AAPL", "NVDA"]
    assert receipt["scope"] == "sampled_research_only"
    assert any(r.get("value") == "Position Evidence Unavailable" for r in payload["rows"])
    assert not any(r.get("metric") == "total_value" for r in payload["rows"])


def test_missing_snapshot_stays_unknown_and_does_not_use_emergency_symbols():
    payload = build(Engine(holdings=[]))
    assert payload["status"] == "partial"
    receipt = payload["meta"]["coverage_receipt"]
    assert receipt["portfolio_membership"] == "unknown"
    assert "My_Portfolio" not in receipt["cohorts"]
    assert any(r.get("value") == "Membership Evidence Unavailable" for r in payload["rows"])


def test_portfolio_opt_out_does_not_fetch_or_inject_holdings():
    engine = Engine()
    payload = build(engine, criteria={"include_portfolio_health": False})
    assert engine.calls == [["AAPL", "NVDA"]]
    assert payload["meta"]["coverage_receipt"]["portfolio_membership"] == "not_requested"


def test_counts_separate_truncation_placeholders_and_identity_conflicts():
    engine = Engine(quotes={"AAPL": {"symbol": "MSFT", "current_price": 500},
                            "NVDA": {"symbol": "NVDA"}})
    payload = build(engine)
    cohort = payload["meta"]["coverage_receipt"]["cohorts"]["Selected Symbols"]
    assert cohort["sampled"] == ["AAPL", "NVDA"]
    assert cohort["returned"] == []
    assert cohort["rejected"] == ["AAPL", "NVDA"]
    truncated = build(Engine(), max_symbols_per_universe=1)
    cohort = truncated["meta"]["coverage_receipt"]["cohorts"]["Selected Symbols"]
    assert cohort["requested"] == ["AAPL", "NVDA"]
    assert cohort["sampled"] == cohort["returned"] == ["AAPL"]
    assert cohort["rejected"] == []
    assert any("unsampled=1" in r.get("notes", "") for r in truncated["rows"])


def test_hash_is_stable_and_binds_membership_criteria_and_results():
    def digest(payload):
        return payload["meta"]["coverage_receipt"]["cohort_hash"]
    first = digest(build(Engine()))
    assert first == digest(build(Engine()))
    assert first != digest(build(Engine(holdings=[{"symbol": "AER.US"}])))
    assert first != digest(build(Engine(), criteria={"include_macro_signals": False}))
    assert first != digest(build(Engine(quotes={"NVDA": {"symbol": "NVDA"}})))


def test_timeout_cannot_synthesize_holdings():
    class SlowEngine(Engine):
        async def get_cached_sheet_snapshot(self, page):
            await asyncio.sleep(1)
    payload = build(SlowEngine(), quotes_timeout_sec=0.1)
    assert payload["meta"]["coverage_receipt"]["portfolio_membership"] == "unknown"


def test_decision_scope_uses_snapshot_not_emergency_portfolio_membership():
    class DecisionEngine(Engine):
        async def list_symbols_for_page(self, page):
            assert page != "My_Portfolio"
            return ["AAPL"]
    async def run():
        with patch.object(builder, "_fetch_top10_symbols", return_value=[]):
            return await builder._resolve_decision_universe(DecisionEngine(holdings=[]))
    result = asyncio.run(run())
    assert result == {"Decision Set": ["AAPL"]}
