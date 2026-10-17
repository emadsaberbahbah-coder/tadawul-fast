# TFB Commit Sheet — B1: Top_10_Investments v2 layout (`16_Decision_Top10.gs` v1.18.0 → v1.19.0, behind `T10_V2_LAYOUT`)

**Date:** 2026-10-17 · **Plan item:** Decision Pages v2 Execution Plan §2 B1 / §5 (Codex) · **Branch:** `claude/new-session-twoifp` · **Base:** main `11ef134` · **Previous commit on branch:** `bf035c6` (B2)

## 1. Version reconciliation (read first)

The plan's §5 brief says "16_Decision_Top10.gs v1.13.9 → v1.14.0". On this branch v1.14.0 … v1.18.0 are already taken by the Codex-brief items T1/T5/T6, T2, T7, L3 and L4 (commits `9f46633` … `8d5417b`). The v2 layout is therefore **v1.19.0**, built on v1.18.0, and the paste order for the IDE is simply "paste v1.19.0" (one file carries every layer). The same applies to 10_My_Portfolio: the plan's A3 "v1.6.0 on top of v1.5.13" must be built on **v1.5.17** (this branch) or it will drop C1/C3, C4, B3 and the B2 hook.

## 2. Base pin

| Item | Value |
|---|---|
| File | `apps_script/16_Decision_Top10.gs` |
| Before | v1.18.0 · 9,275 lines (branch `bf035c6`) |
| After | **v1.19.0** · 10,062 lines · sha256 `31880234964b…` · functions 243 (+24) · vars 120 (+14) · removed 0 |
| Gate | Script Property `T10_V2_LAYOUT = 1` (or `on`); absent / any other value = the v1.18.0 render byte-for-byte (the seven layered harnesses prove it on the reverted source) |
| Rollback | set the property off (next refresh rebuilds the v1 fixed block; operator panel values survive through the new label-scan fallback in `dt10PanelPreserve_`) or re-paste v1.18.0 |

## 3. What the v2 layout renders (property on)

| Zone | Rows | Content |
|---|---|---|
| Status block | 1–3 (frozen) | A1 title; **B1 state chip** `NOT ACTIONABLE / RESEARCH / ACTIONABLE` (coloured); **B2 blocker** in plain words (feed verdict, source readiness, held seats, unfunded, empty); **B3 run tokens** `epoch <key>/<src> · HH:mm Riyadh · v1.19.0 · req <id>` |
| Status line | 4 | the full `Last run … | status … | output …` line (was B2) |
| KPI strip | 5–6 | the 11 KPIs (withheld-truth pass and seat-truth cell unchanged); input-truth gate cells (enforce) at columns 12–13 |
| MODEL EVIDENCE | 8+ | the L3 strip when `_Forecast_Scorecard` exists |
| **Seats** | | 10 columns: `#`, Symbol · Name · Sector, Seat (EXECUTABLE / RESEARCH / FAST-TRACK (suspended) · stability label), Price, Research plan (`TP1 = price × (1 + plan ROI)`; stop numeric only when executable, else `[withheld]`), ROI plan / 12M eng., R/R (TP2), Ref. size (5 % of NAV = holdings + entered cash; executable seats show their SAR and shares), Rel · DQ · Score, Flags (earnings ≤ Nd, `[w52-…]` tag, risk bucket, same-industry cluster, trend). Grace seats collapse into one row. **No cell ever contains an em dash.** |
| WHY CANDIDATES DIDN'T QUALIFY | | the DATA GAPS table directly under the seats, then one **alert chip line** (`ALERTS (2): capital_call ×3 · missing_fx ×1`) |
| SECTOR CAP deferrals (T2) | | unchanged |
| ALL QUALIFIED | | unchanged (incl. the Cluster column) |
| **CONTROL PANEL** | | moved below the opportunity set; same 3-per-row grid and blue inputs; values are read back by label scan before the zone is cleared, blanks re-seed from `_Lists_Config` then the built-ins |
| **FUNNEL** | | title `FUNNEL — 9,786 → 238 → 24 → 24 → 4 → 0 (pool → eligible → kept → passed → seated → executable)`; 20 rows reserved for **four EmbeddedChartBuilder charts**: funnel (column, `#2a78d6`), universe (Investability Status counts: Global_Markets vs all pages), concentration (bar: qualified vs seats by sector), hit rate by horizon (line, `#2a78d6`, 2 px) — data on the hidden tab `_Charts_Data_T10` (A:C, E:G, I:K, M:O, stamp Q1:Q2), rewritten each run |
| NEAR MISS, INVESTABILITY reasons (T1), CANDIDATES (cand-tab logic unchanged), footer | | unchanged |

Failure paths (network, HTTP, degraded, sync-in-flight HELD) write the chip `NOT ACTIONABLE` + "Refresh failed: … (board kept from the last render)" so yesterday's word never stands over a failed run. The status line gains ` | v2 layout: 4 charts`.

**Not in v2 (stated):** the WP-8 vintage columns (they extend the 30-column v1 table; the status token still prints) and the T7 research-levels row restore (the plan column carries TP1; the stop stays `[withheld]` until executable).

## 4. Protocol evidence

- **Anchored edits:** 14 (`scratchpad/plan_v2/edit_b1_gas.py`), every anchor `count == 1`, recorded in `tests/js/dt10_v1190_history.js` (reversal layer; chain v1190 → v1180 → … → the pinned v1.13.8 digest verified).
- **Zero-removal proof:** removed lines = `Version:` header, `DT10_VERSION`, the v1 layout-guard `if (` → `if (!v2 && (` (two lines), `var statusCell = …` → ternary, `var panel = …` → ternary, and `poolNote` line unchanged; every other change is an insertion. Functions removed 0.
- **ES5:** every added line passes the guard (harness V2; comments reworded where they tripped it).
- **Harness:** `node tests/js/dt10_v1190_v2_layout.test.js` 11/11 (version/census, reversal layer, gate memo, state/blocker/tokens, seat rows without em dash, funnel/universe/concentration/hit-rate/chips, panel scan + defaults, fixed block + fail chip, full v2 render on a stub workbook with four charts and the hidden data tab, refresh wiring, self-test line); the layered harnesses `dt10_v1180` 5/5, `v1170` 7/7, `v1160` 9/9, `v1150` 11/11, `v1140` 12/12 now load `revertV1190(…)`; `decision_input_truth` 20 passed (pins 1.19.0, census 243/120, `need` += `v2 layout core: ok`); `dt10_feed_cohort_binding` 21/21; `decision_source_readiness` 73; `decision_board_funding` 64; calendar completeness; `apps_script_syntax` 5 compiled; `ci.yml` JS step += `dt10_v1190_v2_layout`.
- **No ENV, Script Property or cell changed by this commit.**

## 5. Acceptance (plan §2 B1) and read-back after paste (Emad)

1. Paste v1.19.0; `dt10SelfTest` → `v2 layout core: ok | layout (T10_V2_LAYOUT): v1 (default)`; the board is unchanged.
2. Set Script Property `T10_V2_LAYOUT = 1`; run Refresh Decision Top 10 once: B1 reads one of the three states, B2 names the blocker, B3 carries the epoch; the seat table has no `—`; the FUNNEL title shows the six counts (9,786 → 238 → 24 → 10 → 4 → 0 on a 10-10-shaped run); four charts sit under it; `_Charts_Data_T10` is hidden. Status line ends with `| v2 layout: 4 charts`.
3. Edit a blue panel cell (now below ALL QUALIFIED), refresh again: the value is read back (label scan) and re-rendered.
4. Observe the 04:52 / 08:07 runs through Wed 22 Oct (plan C5), then leave the property on or set it off to return to v1.
