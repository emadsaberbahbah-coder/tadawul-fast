"""G1 prepare (2026-10-17): the Global_Markets / Commodities_FX row floors the
final publication gate, the full-refresh coverage audit and the decision-surface
freshness audit hard-coded (4 literals) now read config/market_floors_v1.json
through scripts.approved_universe_scope.market_floor(). The file holds the
pre-D3 values (6512 / 453), so every gate is byte-identical today; the D3
retirement changes the file, not the code. Unreadable / malformed file -> the
literal default (never raises).
"""
from __future__ import annotations

import json
from pathlib import Path

from scripts import approved_universe_scope as aus
from scripts import audit_decision_surface_freshness as fresh
from scripts import audit_full_refresh_coverage as cov
from scripts import finalize_decision_publication as final

ROOT = Path(__file__).resolve().parents[1]


def test_config_file_holds_the_pre_d3_floors():
    raw = json.loads((ROOT / "config/market_floors_v1.json").read_text(encoding="utf-8"))
    assert raw["schema"] == "tfb.market-floors/v1"
    assert raw["floors"] == {"Global_Markets": 6512, "Commodities_FX": 453}
    assert "D3" in raw["note"] and "6476" in raw["note"]


def test_market_floor_reads_the_file_and_falls_back(tmp_path):
    assert aus.market_floor("Global_Markets", 1) == 6512
    assert aus.market_floor("Commodities_FX", 1) == 453
    assert aus.market_floor("Mutual_Funds", 2474) == 2474, "pages outside the file keep the caller default"
    assert aus.market_floor("Global_Markets", 6512, path=tmp_path / "missing.json") == 6512
    bad = tmp_path / "bad.json"; bad.write_text("{not json", encoding="utf-8")
    assert aus.market_floor("Global_Markets", 6512, path=bad) == 6512
    wrong = tmp_path / "wrong.json"; wrong.write_text(json.dumps({"schema": "other", "floors": {"Global_Markets": 1}}), encoding="utf-8")
    assert aus.market_floor("Global_Markets", 6512, path=wrong) == 6512
    junk = tmp_path / "junk.json"; junk.write_text(json.dumps({"schema": "tfb.market-floors/v1", "floors": {"Global_Markets": "6476"}}), encoding="utf-8")
    assert aus.market_floor("Global_Markets", 6512, path=junk) == 6512, "a string is not an integer floor"
    post = tmp_path / "post.json"; post.write_text(json.dumps({"schema": "tfb.market-floors/v1", "floors": {"Global_Markets": 6476}}), encoding="utf-8")
    assert aus.market_floor("Global_Markets", 6512, path=post) == 6476


def test_the_four_literal_sites_read_the_single_source():
    assert final.MARKET_FLOORS == {"Market_Leaders": 255, "Global_Markets": 6512, "Commodities_FX": 453, "Mutual_Funds": 2474}
    src_final = (ROOT / "scripts/finalize_decision_publication.py").read_text(encoding="utf-8")
    assert '"Global_Markets": market_floor("Global_Markets", 6512)' in src_final
    src_cov = (ROOT / "scripts/audit_full_refresh_coverage.py").read_text(encoding="utf-8")
    assert 'R("Global_Markets",market_floor("Global_Markets",6512))' in src_cov
    assert 'R("Commodities_FX",market_floor("Commodities_FX",453),95,95)' in src_cov
    src_fresh = (ROOT / "scripts/audit_decision_surface_freshness.py").read_text(encoding="utf-8")
    assert '_env_int("TFB_EXPECTED_MIN_ROWS_GLOBAL_MARKETS", _market_floor("Global_Markets", 6512))' in src_fresh
    assert 'max(floors.get("Global_Markets", 0), _market_floor("Global_Markets", 6512))' in src_fresh
    # no bare 6512 floor literal remains outside the defaults handed to market_floor
    for src in (src_final, src_cov, src_fresh):
        for line in src.splitlines():
            if "6512" in line and "market_floor" not in line and not line.strip().startswith("#"):
                raise AssertionError("bare 6512 literal: " + line.strip())


def test_post_d3_file_moves_every_gate_together(monkeypatch, tmp_path):
    post = tmp_path / "post.json"
    post.write_text(json.dumps({"schema": "tfb.market-floors/v1", "floors": {"Global_Markets": 6476, "Commodities_FX": 453}}), encoding="utf-8")
    monkeypatch.setattr(aus, "DEFAULT_MARKET_FLOORS_PATH", post)
    rules = {r.page: r for r in cov.rules(approved_scope=aus.load_scope())}
    assert rules["Global_Markets"].min_rows == 6476 and rules["Commodities_FX"].min_rows == 453
    assert fresh._configured_floors(None)["Global_Markets"] == 6476
    assert fresh._market_floor("Global_Markets", 6512) == 6476
    # the pre-D3 file: byte-identical gates
    monkeypatch.setattr(aus, "DEFAULT_MARKET_FLOORS_PATH", ROOT / "config/market_floors_v1.json")
    rules = {r.page: r for r in cov.rules(approved_scope=aus.load_scope())}
    assert rules["Global_Markets"].min_rows == 6512
    assert fresh._configured_floors(None)["Global_Markets"] == 6512
