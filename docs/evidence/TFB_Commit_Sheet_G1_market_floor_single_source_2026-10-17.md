# TFB Commit Sheet — G1 prepare: one source for the market-page row floors (D3 ready)

**Date:** 2026-10-17 · **Brief item:** Codex Brief v2 §5 G1 ("prepare; merge only on Emad's GO D3; 133 nameless today; post-retirement count computed from the merged list") · **Branch:** `claude/new-session-twoifp` · **Base:** main `11ef134` · **Previous commit on branch:** `4066d8b`

## 1. What ships (prepare only — nothing retired, every gate byte-identical)

| File | Change |
|---|---|
| `config/market_floors_v1.json` (new) | `{"schema": "tfb.market-floors/v1", "floors": {"Global_Markets": 6512, "Commodities_FX": 453}}` with the D3 note: the retirement of the 133 nameless GM rows changes this file to the merged post-retirement count (6,609 − 133 = **6,476**) in one PR on Emad's GO. Market_Leaders / Mutual_Funds floors stay in `config/approved_universe_scope_v1.json`. |
| `scripts/approved_universe_scope.py` v1.0.0 → v1.1.0 | `market_floor(page, default, path=None)`: the configured floor, else the caller's literal default; never raises (missing / malformed / wrong-schema / non-integer file → default). |
| `scripts/finalize_decision_publication.py` | `MARKET_FLOORS` GM / CFX entries → `market_floor(...)` (literal 1 of 4). |
| `scripts/audit_full_refresh_coverage.py` | `rules()` GM / CFX → `market_floor(...)` (literal 2 of 4). |
| `scripts/audit_decision_surface_freshness.py` | `_configured_floors` default and the clamp `max(…, 6512)` → `_market_floor(...)` (lazy import, same style as the file) (literals 3 and 4 of 4). |

The literal defaults handed to `market_floor` are the pre-D3 values, so the four gates compute exactly what they computed yesterday (test: `final.MARKET_FLOORS == {ML 255, GM 6512, CFX 453, MF 2474}`, `rules()["Global_Markets"].min_rows == 6512`, `_configured_floors(None)["Global_Markets"] == 6512`).

## 2. What D3 then needs (one PR, on GO)

1. Emad's retire list of the 133 nameless GM rows (D3) → `TFB_SYNC_RETIRE_TAB` lists them; the merged list gives the post-retirement count.
2. Edit `config/market_floors_v1.json` → `"Global_Markets": <merged count>` (6,476 if all 133 go) and `revision`.
3. The workflow Actions-variable fallbacks (`vars.EXPECTED_MIN_ROWS_GLOBAL_MARKETS || '6512'` in daily_sync, decision_surface_freshness, full_refresh_coverage, release_decision_feed, workflow_repair_tests) are operator-overridable and clamp **upward** only (`minimum=baseline`), so they must move in the same PR or the gate stays at 6,512; listed here so the GO PR is complete.
4. The cockpit's embedded `DT10_SOURCE_APPROVED_SCOPE` floors (16_Decision_Top10, `policy.floors.Global_Markets 6512`) follow in the GAS layer of that PR.

## 3. Protocol evidence

- **Anchored edits:** 7 (every anchor `count == 1`); removed lines = the two `MARKET_FLOORS` lines, one `rules()` line and three `_configured_floors` / clamp lines, each rewritten in place with the same default; functions added 2 (`market_floor`, `_market_floor`); removed 0.
- **Byte-identical:** `test_post_d3_file_moves_every_gate_together` proves the pre-D3 file reproduces 6,512 and a post-D3 file moves the coverage rules and the freshness floors together.
- **Harness:** `tests/test_market_floor_source_g1.py` + the final-publication, full-refresh-coverage, decision-surface-freshness, approved-universe and release-decision-feed suites; ruff clean; `ci.yml` lean list updated. No ENV, Script Property or cell changed.
