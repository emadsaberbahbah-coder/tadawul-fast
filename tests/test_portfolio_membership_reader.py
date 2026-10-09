"""Actual engine and reader reject emergency/partial portfolio universes."""
import asyncio
from unittest.mock import patch

from core import data_engine_v2 as de
from core.sheets import rows_reader as reader


def read_membership(grid):
    class GridReader:
        def read_range(self, sid, rng):
            assert sid == "synthetic"
            assert rng == "My_Portfolio!A:ZZ"
            return grid
    with patch.object(reader, "_resolve_spreadsheet_id", return_value="synthetic"), \
            patch.object(reader, "_reader_enabled", return_value=True), \
            patch.object(reader, "_new_grid_reader", return_value=GridReader()):
        return reader.RowsReader().get_membership_for_page("My_Portfolio")


def test_reader_returns_complete_membership_beyond_old_row_caps_and_reordered_header():
    grid = [["Name", "Symbol"], *[["Holding", f"TEST{i}.US"] for i in range(2100)]]
    result = read_membership(grid)
    assert result["complete"] is True
    assert result["source"] == "workbook_readonly"
    assert len(result["symbols"]) == 2100
    assert result["symbols"][-1] == "TEST2099.US"


def test_missing_or_ambiguous_header_does_not_claim_complete_membership():
    for grid in ([], [["Name"], ["DDI.US"]], [["Symbol", "Symbol"], ["DDI.US"]]):
        result = read_membership(grid)
        assert result["complete"] is False
        assert result["symbols"] == []


def test_real_engine_uses_full_reader_even_when_quote_cache_contains_one_holding():
    class MembershipReader:
        def get_membership_for_page(self, page):
            return read_membership([["Symbol"], ["AER.US"], ["DDI.US"], ["5023.SR"]])
    async def run():
        engine = de.DataEngineV5(rows_reader=MembershipReader())
        engine._page_snapshots["My_Portfolio"] = [{"symbol": "DDI.US"}]
        return await engine.get_sheet_membership("My_Portfolio")
    assert asyncio.run(run())["symbols"] == ["AER.US", "DDI.US", "5023.SR"]


def test_real_engine_cold_or_partial_reader_never_falls_back_to_emergency_holdings():
    class PartialReader:
        async def get_membership_for_page(self, page):
            return {"status": "success", "complete": False,
                    "source": "workbook_readonly", "symbols": ["DDI.US"]}
    async def run():
        engine = de.DataEngineV5(rows_reader=PartialReader())
        engine._page_snapshots["My_Portfolio"] = [{"symbol": "DDI.US"}]
        return await engine.get_sheet_membership("My_Portfolio")
    result = asyncio.run(run())
    assert result["complete"] is False
    assert result["symbols"] == []
