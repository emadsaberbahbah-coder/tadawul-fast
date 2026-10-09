"""The real derived-page route binds request criteria to the cohort receipt."""
import asyncio
from unittest.mock import patch

from core.analysis import insights_builder as builder
from routes import advanced_analysis as route


def test_special_route_preserves_criteria_opt_out_and_builder_receipt():
    class Engine:
        async def get_cached_sheet_snapshot(self, page):
            raise AssertionError("Portfolio opt-out must reach the real builder")

        async def get_enriched_quotes_batch(self, symbols, **kwargs):
            return {sym: {"symbol": sym, "current_price": 100} for sym in symbols}

    async def run():
        with patch.object(route, "_provider_engine_for_builder", return_value=Engine()), \
                patch.object(route, "_build_insights_rows", builder.build_insights_analysis_rows):
            return await route._build_special_page_payload(
                page="Insights_Analysis", merged_body={"criteria": {
                    "Include Portfolio Health": False, "Include Top Opportunities": False,
                    "Include Risk Scenarios": False,
                }}, mode="", limit=100, offset=0, top_n=10,
                requested_symbols=["AAPL"], timeout_s=2,
            )

    payload = asyncio.run(run())
    assert payload is not None
    receipt = payload["meta"]["coverage_receipt"]
    assert receipt["portfolio_membership"] == "not_requested"
    assert receipt["cohorts"]["Selected Symbols"]["returned"] == ["AAPL"]
    normalized = route._normalize_external_payload(
        external_payload=payload, page="Insights_Analysis", headers=payload["headers"],
        keys=payload["keys"], include_matrix=True, request_id="offline-test",
        started_at=0, mode="", limit=100,
    )
    assert normalized["meta"]["coverage_receipt"] == receipt
