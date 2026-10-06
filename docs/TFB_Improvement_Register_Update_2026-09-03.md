# TFB Improvement Register — update 2026-09-03 (Day 3 of the 10-Day Finalization Program)

Provisional IDs P-34…P-44 (next free IR number still unconfirmed by operator — assign on confirmation).
HEAD at close of morning session: `199327c` — run_dashboard_sync 6.57.0 `a64602671dc52e06`, daily_sync.yml `b2af96ba23d35d96`, tfb_acceptance 1.0.5, shadow scorer 1.7.0, builder 1.19.2 (Render live), engine 5.135.0 (Render live, `engine_target_klg=true`).

## Acceptance trajectory (measured, tfb_acceptance v1.0.5)
| When | PASS | WARN | FAIL | NA | What moved |
|---|---|---|---|---|---|
| 2026-09-01 ~13:30 | 18 | 1 | 6 | 1 | — |
| 2026-09-03 10:08 (#8) | 20 | 8 | 1 | 1 | D10-1 Passed 0→80 (Render OPP flags active); D10-3b duplicate_key=0; D10-5-CFX FAIL (survived 8 = 1.77%) |
| 2026-09-03 12:54 (#9) | **24** | 5 | **0** | 1 | D10-5 all four pages survived=0 on **scheduled** run 33735652738; D10-4a/4c EXECUTABLE; Passed=90 |
Remaining WARN: G4 glyph-share info ×4 (layout), D10-7 S-1 3/28. NA: D10-6 digest (read in Actions).

## Armings today (2 of ≤3; each with read-back proof)
1. `TFB_SYNC_NULL_CLEAR_SCOPE=all` (GitHub Variable, 10:05) — proof: run #3887 readback delta 0 on all pages + `[UPSTREAM-VERDICT] EXECUTABLE`; held on scheduled run 33735652738 (DISTRIBUTION). Kill = `ohlc`.
2. `TFB_SHADOW_EODHD=1` (GitHub Variable, ~10:20) — dry-run #49 executed; rescue count not yet read (job log); the 17:40 scheduled run writes `eodhd_rescued:N/M` into S1_Gate. Kill = unset. Secret `EODHD_API_KEY` present (age 3 weeks).

## Entries

### P-34 — Identity grafting on non-OHLC columns for stub rows (Name / Current Price / 52W) — **CURED, verification pending**
- Mechanism (proven): a row whose backend payload carries nulls inherits the previous occupant's cells at that sheet position when the write skips nulls; OLDEST-FIRST reordering moves rows every run. Evidence: JHG.US quarantined 02:21 → "Imunon, Inc." / 1.6 / own 52W after the 03:27 write; identical 5-name sequence at GM rows 2–7, 16–20, 103–107; 69 GM rows priced outside their own 52W band, all KLG-certified (klg_kept=111).
- Cure at HEAD v6.57.0 (`TFB_SYNC_NULL_CLEAR_SCOPE=all` over the armed fill guard), armed 2026-09-03 10:05. Result: write-survival 10/8/1 → 0/0/0; GM name_dup 12 groups → 1 legitimate family; klg_kept 111 → 0.
- Measured one-time cost (honest state): provider_target cohort GM 2,187 → 1,748 (−439), ML 134 → 111 (−23); expect `missing_valuation` up on the board.
- Open: (a) confirm from the evening GM export that rows 2–7 are nameless/blank (fossils cleared) — if chimeras persist, one-run `TFB_SYNC_FORCE_REFETCH_SYMBOLS` purge (137-symbol list delivered; mapping at HEAD); (b) after 3 clean scheduled runs, make `all` the workflow default (`|| 'all'`) so a deleted Variable cannot regress the cure (W1A-6h, workflow-only).
- Superseded: STUB-EXPLICIT build proposal (withdrawn 2026-09-03).

### P-35 — Cockpit `output:` label independent of the feed verdict — OPEN (latent)
- 16_Decision_Top10 v1.11.0 printed `output: EXECUTABLE` while every row read "SIZING WITHHELD — feed NOT_ACTIONABLE". Coincident today (both EXECUTABLE) but the label must inherit the UPSTREAM-VERDICT key. Owner: GAS. Small.

### P-36 — Board venue substitution + venue gate arming — OPERATOR DECISION
- 2026-09-03 board: BYMA.BA (no IBKR contract; BCBA not offered) and HDFCBANK.NS (NSE line; tradable form is the ADR HDB, NYSE contract 12796138; HDB.US already in the GM pool, WATCH 61<68, ROI12 31.3%, Rel 71.5). Shadow board blocked exactly 2 BROKER_UNTRADABLE of 5. Live board has no venue gate (IR-020: `TFB_T10_VENUE_LOTS="defult"` never on).
- Proposal: (1) arm the venue gate (recommendation-touching → explicit operator decision); (2) same-issuer substitution rule when the pick is untradable and a family listing on a tradable venue exists (FW-4 family logic already groups them).

### P-37 — Ledger FX 3.7787 vs 3.75 — OPEN
- `_Portfolio_CostBasis` converts USD at 3.7787 (outside the SAMA band); Commodities_FX SAR=X 3.75 and Portfolio_Decision use 3.75. Ledger overstates USD MV ≈0.77% (~+590 SAR on ~76k). Owner: GAS 21_Portfolio_Ledger (source of the rate to identify; pin to the page's SAR=X or the peg).

### P-38 — Performance_Log deletion was two-step — VERIFY
- 38,578 → 15,000 (09-01 17:01) → 11,211 (09-01 19:10); plan of record was one archived tail-drop of ~23.8k. Second step (−3,981) needs its archive artifact. Side effect: calibration BLOCKED cohort now n=9 (n/a). duplicate_key=0 confirmed by acceptance.

### P-39 — Second out-of-repo Open writer on Market_Leaders at market open — OBSERVE
- `[OHLC-LAKE] Market_Leaders foreign_open_fill=117` at 10:12 Riyadh (07:12Z) — outside the eodhd-screener window (22:00–00:50Z). Acceptance `open_outside_range` on ML 3 (07:01) → 51 (11:56, in session): the foreign Opens are not coherent with the day range. Candidate: GAS intraday refresh. Extends the writer census; no action until identified.

### P-40 — GRACE retains engine-SELL names — maps to R-4 (queue #6)
- ELPC.US and VINP.US held at ranks 2–3 via GRACE(1/3) while the page verdict is DO_NOT_INVEST / "Engine recommends SELL" (Rel 31.3). A sell-tier current verdict should demote regardless of grace budget.

### P-41 — Top_10 fires before the GM leg lands — maps to queue #4 (ordering)
- 06:55 Top_10 vs 07:51 GM stamp: the board scored the previous run's GM. Either gate Top_10 on the GM STATUS-STAMP of the current run or order decision symbols first.

### P-42 — `TFB_SYNC_IDENTITY_REFETCH` and `TFB_SYNC_FORCE_REFETCH_SYMBOLS` now mapped (`199327c`) — UNARMED
- Both were kill-switch-without-mapping. Arm IDENTITY_REFETCH only if name_dup groups reappear after P-34 verification.

### P-43 — `EODHD_API_KEY` repo secret predates the D-6 rotation date — VERIFY
- Secret age 3 weeks; D-6 rotation pinned 2026-09-01. If D-6 was executed on Render, the GitHub secret must be the rotated key or the shadow rescue fails (visible as `eodhd:` errors in the 17:40 note). If D-6 not executed, no action.

### P-44 — S-1 criterion 1 exclusion mechanism — ARMED FIX, RESULT PENDING
- 3/28 scored, 30 excluded-infra; scorer prices from Yahoo on the GitHub runner (19 price errors 09-02). EODHD rescue armed 09-03 (boundary note row required in S1_Gate before arming). Read the 17:40 run: `eodhd_rescued:N/M` and day class. Criterion 6 (rollback drill) remains the only blocker with no engineering dependency — operator.

## Closed today
- EPRT GO-ticket provenance: executed 2026-08-27, 100 @ 30.6129 (IBKR), ledger and broker agree; cash 14,700 → 3,217.50 SAR reconciles.
- IRM chase: no order at the broker; engine SELL, Rel 31.3, 111.85 (−2.1%). Closed by evidence.
- `TFB_ENGINE_TARGET_KLG=1`: confirmed active (Render startup `engine_target_klg: true`).
- Render OPP flags: confirmed indirectly (D10-1 Passed 0 → 80 → 90).
- `TRACK_DROP_DUPLICATE_ROWS`: executed (see P-38 for the verification residue).
- Fill guard enforce + single-writer guard: both live with read-back proof.
