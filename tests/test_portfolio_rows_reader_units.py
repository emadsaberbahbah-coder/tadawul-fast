"""Real portfolio schema preserves monetary and percentage units independently."""
import pytest

from core import data_engine_v2 as engine
from core.sheets import rows_reader as reader


def map_live_grid(monkeypatch, grid):
    class GridReader:
        def read_range(self, sid, rng):
            assert sid == "synthetic"
            assert rng.startswith("My_Portfolio!A1:")
            return grid

    monkeypatch.setattr(reader, "_resolve_spreadsheet_id", lambda: "synthetic")
    monkeypatch.setattr(reader, "_reader_enabled", lambda: True)
    monkeypatch.setattr(reader, "_new_grid_reader", lambda: GridReader())
    return reader.RowsReader().get_rows_for_page("My_Portfolio")


@pytest.mark.parametrize("reversed_columns", [False, True])
def test_real_registry_preserves_money_and_percent_even_without_recomputable_inputs(monkeypatch, reversed_columns):
    headers, keys = engine.get_sheet_spec("My_Portfolio")
    observed = {"Symbol": "SYNTH.US", "Unrealized P/L": -125.75,
                "Unrealized P/L %": -2.5, "Currency": "USD"}
    grid_headers = list(reversed(headers)) if reversed_columns else headers
    grid = [grid_headers, [observed.get(header, "") for header in grid_headers]]
    rows = map_live_grid(monkeypatch, grid)
    assert len(rows) == 1
    assert rows[0]["unrealized_pl"] == -125.75
    assert rows[0]["unrealized_pl_pct"] == -2.5
    assert "position_qty" not in rows[0] and "avg_cost" not in rows[0]


def test_blank_percentage_never_erases_money(monkeypatch):
    rows = map_live_grid(monkeypatch, [["Symbol", "Unrealized P/L", "Unrealized P/L %"],
                                      ["SYNTH.US", -125.75, ""]])
    assert rows == [{"symbol": "SYNTH.US", "unrealized_pl": -125.75}]


def test_header_case_spacing_and_punctuation_aliases_keep_distinct_units(monkeypatch):
    rows = map_live_grid(monkeypatch, [["SYMBOL", "unrealized p / l", "UNREALIZED_P/L_%",
                                       "POSITION_QTY", "avg_cost"],
                                      ["SYNTH.US", -125.75, -2.5, 10, 100]])
    assert rows == [{"symbol": "SYNTH.US", "unrealized_pl": -125.75,
                     "unrealized_pl_pct": -2.5, "position_qty": 10, "avg_cost": 100}]


def test_preexisting_unit_omitting_aliases_remain_when_schema_meaning_is_unambiguous(monkeypatch):
    rows = map_live_grid(monkeypatch,
        [["Symbol", "Upside", "52W Position", "Target Weight", "Actual Weight"],
         ["SYNTH.US", 0.15, 60, 25, 30]])
    assert rows == [{"symbol": "SYNTH.US", "upside_pct": 0.15,
                     "week_52_position_pct": 60, "target_weight": 25, "actual_weight": 30}]


@pytest.mark.parametrize("header", ["Symbol", "Position Qty", "Avg Cost"])
def test_ambiguous_manual_input_columns_do_not_choose_last_value(monkeypatch, header):
    headers = ["Symbol", "Position Qty", "Avg Cost", header.upper().replace(" ", "_")]
    values = ["SYNTH.US", 10, 100, "SECOND.US" if header == "Symbol" else 999]
    assert map_live_grid(monkeypatch, [headers, values]) == []


def test_duplicate_optional_observation_is_unknown_without_erasing_manual_holdings(monkeypatch):
    rows = map_live_grid(monkeypatch,
        [["Symbol", "Position Qty", "Unrealized P/L", "unrealized_p/l", "Unrealized P/L %"],
         ["SYNTH.US", 10, -125.75, -100, -2.5]])
    assert rows == [{"symbol": "SYNTH.US", "position_qty": 10, "unrealized_pl_pct": -2.5}]
