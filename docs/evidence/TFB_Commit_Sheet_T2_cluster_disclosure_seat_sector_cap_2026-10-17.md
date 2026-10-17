# TFB Commit Sheet — T2 Concentration: disclose the cluster + sector cap on carried seats

**Date:** 2026-10-17 · **Brief item:** Codex Brief v2 §3 T2 (P1) · **Branch:** `claude/new-session-twoifp` · **Base:** main `11ef134` · **Previous commit on branch:** `55a1574`

## 1. What the brief measured and what ships

| Measured 17-Oct | Delivered |
|---|---|
| 19 of 24 qualified names are US Financials; the page says nothing | **Builder (opportunity_builder v1.28.0 → v1.29.0):** every `candidates_rows` record gains `cluster` ("`<Industry> ×N`" on qualified rows, "" elsewhere; Industry falls back to Sector; ''/Unknown never cluster), `meta["concentration"]` ranks the clusters (count, share, 3 examples), ONE `{"type": "concentration"}` alert when the top cluster holds > 50 % of a qualified set of ≥ 4 names. Certified and withheld paths. |
| 5 Real Estate seats of 10 against panel `T10: Max Per Sector = 2`; carried GRACE/FAST-TRACK seats re-enter from `dt10StabCore_` without the cap | **Cockpit (16_Decision_Top10 v1.14.0 → v1.15.0):** `dt10SeatSectorCapApply_` runs after the stability core: raw seats count first (never deferred — the builder capped them), carried seats fill what the cap leaves in rank order, the rest are **deferred**: removed from `payload.selected`, listed in a new "SECTOR CAP — CARRIED SEATS DEFERRED (n)" table directly under SELECTED with the note `sector cap — carried seat deferred (<sector> n/cap)`, named in the stab token `seat-cap deferred n (syms)` and in `meta.stability.audit.sector_cap_deferred`. Membership and clocks untouched. ALL QUALIFIED gains a `Cluster` column (before "Why Not Selected"). |

## 2. Acceptance on the 17-Oct shape (harness, not the live sheet)

- `tests/test_ob_cluster_concentration_t2.py::test_certified_path_carries_cluster_field_meta_and_one_alert`: 19 regional banks + 5 others qualified → `cluster = "Banks - Regional ×19"`, `meta.concentration.top = {count 19, share 79.2 %}`, exactly one concentration alert; WATCHLIST row blank; the deferral set at selection is unchanged.
- `tests/js/dt10_v1150_cluster_seat_cap.test.js` C2/C6: board with 5 Real Estate of 10 (2 raw + 3 carried) and cap 2 → **Real Estate seats 5 → 2**, the deferred three listed (GLPI, NNN, O in the fixture), kept board re-ranked 1..7, state blob byte-identical before/after the pass, SelLog signature carries only the kept seats.

## 3. Decisions taken (flag for Emad)

- **"Defer" = listed, not seated.** A removed seat keeps its membership and clock (the ghost's miss clock already runs while it is absent from today's raw selection; it soft-exits on `Exit Days` as before). Suspending sizing instead would have left 5 Real Estate rows on the board, which is the defect the brief names.
- **No backfill.** The freed seat stays empty today; a fast-track fill would be a selection change inside the pure core — panel decision, not this PR's.
- **Alert floor of 4 qualified names.** Below four a majority is arithmetic (1 of 1 = 100 %), so the alert would fire on every thin board.
- **CANDIDATES grid unchanged.** `DT10_CAND_HEADERS` is the WP-8 hidden-tab checksum/receipt contract (`dt10CandTabWrite_`); the cluster is rendered in ALL QUALIFIED only. The data is on every `candidates_rows` record regardless.
- **Industry availability.** GM/ML pool rows carry `Industry` (schema registry "Identity/Industry, provider"); where blank the cluster falls back to Sector ("Financials ×19"). The live label will read the EODHD industry string (e.g. "Banks - Regional"), not the brief's prose "US regional banks".

## 4. Protocol evidence

- **Anchored edits:** builder 9 (`scratchpad/edit_t2_builder.py`), GAS 10 (`scratchpad/edit_t2_gas.py`), every anchor `count == 1`; the GAS reversal layer `tests/js/dt10_v1150_history.js` is generated from the same edit list and takes the file back to v1.14.0, which `dt10_v1140_history.js` takes to v1.13.9 and the pinned v1.13.8 digest (V2 test).
- **Zero-removal proof:** builder removed lines = 2 (`Version:` header, `OPPORTUNITY_BUILDER_VERSION`), both rewritten in place. GAS removed lines = 6: header `Version:` line, `DT10_VERSION`, the `dt10QualToRow_` return tail (rewritten with the inserted cell), and three selftest lines indexing `hsQual[14]` → `[15]` because "Why Not Selected" moved one column right. No function, var, branch or message removed. Census 199 → 205 functions, 104 → 105 vars (6 + 1 added, 0 removed).
- **ES5:** every added GAS line passes the V3 guard (no let/const/arrow/template/class/spread/includes, no smart quotes/CR).
- **Default on / kill switches:** builder `TFB_T10_CLUSTER_LEGACY=1` → payload byte-identical to v1.28.0 save the version stamp (test `test_legacy_switch_is_byte_identical_save_the_additions`); GAS Script Property `DT10_SEAT_SECTOR_CAP_LEGACY='1'` → board exactly the core output, no table, no token (C5). Default on is the brief's instruction ("the off state is the defect, P-127/P-130 precedent"). No ENV, Script Property or cell changed by this PR.
- **Harness:** `node tests/js/dt10_v1150_cluster_seat_cap.test.js` 11/11; the seven other GAS harnesses pass against the v1.15.0 file (v1140 harness now runs on the v1.15.0-reverted source; version/census pins in `decision_input_truth`, `dt10_feed_cohort_binding`, `decision_source_readiness` moved to 1.15.0 / 205 / 105); `dt10SelfTest` gains `seat sector cap core: ok`; pytest 197 passed over the builder + decision suites; ruff clean; `ci.yml` lean list += `tests/test_ob_cluster_concentration_t2.py`, JS step += the new harness.

## 5. Read-back after the paste (Claude/Emad)

1. Paste v1.15.0 (after v1.13.9 → v1.14.0 in order), run `dt10SelfTest` → `seat sector cap core: ok | ON`.
2. Next refresh status line: `stab: … | seat-cap deferred 3 (…)`; a "SECTOR CAP — CARRIED SEATS DEFERRED (3)" table under SELECTED; ALL QUALIFIED `Cluster` column populated once the Render deploy carries builder v1.29.0 (blank until then).
3. ALERTS table shows one `concentration` row with "= 19 of 24 qualified names (79.2% > 50%)" (or the day's figures).
4. `_Selection_Log`: the three deferred seats are absent from the day's rows; everything else as before.
