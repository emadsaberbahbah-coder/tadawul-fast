# Commit Sheet - `scripts/run_shadow_board.py` v1.6.0 (read a settled Top-10) + `shadow_board.yml` v1.3.0 - 2026-10-28

**Scope:** weekly-review defect D7. The evening Shadow_Board run intermittently evaluates 0 candidates because it reads `Top_10_Investments` while the cockpit is re-rendering it. Board-input reliability only: no gate, verdict, ranking, sizing, freeze, regime or ROI path changes.

**Files:**
- `scripts/run_shadow_board.py` 1.5.0 -> 1.6.0: WHY block added above `SCRIPT_VERSION` (earlier blocks verbatim); 4 functions added (`_t10_read_retries`, `_t10_read_wait_s`, `top10_rendering`, `read_top10_settled`), 0 removed; one call site in `main()` (the v1.5.0 statement is kept verbatim in the off branch).
- `.github/workflows/shadow_board.yml` v1.3.0: `TFB_SHADOW_T10_READ_RETRIES: "6"` (armed; rollback `"0"`).
- `scripts/verify_deployment.py`: pin 1.6.0.
- `tests/test_shadow_board_t10_settle.py` (new, 7 cases) and `tests/test_sb_freeze_nontrading_p175.py` (its F2-F4 literals `"1.5.0"` now read the delivered `SCRIPT_VERSION`, the file's own version-masking intent).
- This sheet.

## S1 - Evidence
`[SHADOW-BOARD] cands=` per run (shadow_board.yml logs):

| Day | Morning run | Evening run |
|---|---|---|
| 15 Oct | - | 6 |
| 18 Oct | - | 10 |
| 19 Oct | - | 6 |
| 20 Oct | 10 (09:58Z) | **0** (18:26Z) |
| 21 Oct | 25 | **0** |
| 22 Oct | 10 | 5 |
| 23 Oct | - | **0** |
| 24 Oct | 10 | 10 |
| 25 Oct | - | 10 |
| 26 Oct | 10 | 10 |
| 27 Oct | 10 | **0** (19:24:47Z) |

27 Oct: the job started 22:24:47 Riyadh (board stamp "as of 2026-10-27 22:25"). `_Run_Log` shows `tfbDecisionChainTick` closing Top-10 renders at 22:14:35, 22:19:58 and 22:27:32 Riyadh (about 2 min each, every ~5 min - weekly-review D5). A render writes `status: running...` into B2 (`16_Decision_Top10`, `statusCell.setValue(dt10StatusLine_('running…', ''))`), then `dt10RenderPayload_` clears every zone from row 18 down before repainting. A read inside that window finds no `Symbol` header and `rows_to_records` returns `[]`. Today's settled board parses 7 rows with the same logic (checked against the live sheet).

So the weekly review's "0 rows every day since ~20 Oct" was too strong: 4 of the 8 evening runs from 20 to 27 Oct read 0; all 6 morning runs checked read rows.

## S2 - WHAT
- `TFB_SHADOW_T10_READ_RETRIES` 0 (default, junk, negative) .. 20; `TFB_SHADOW_T10_READ_WAIT_S` 30 (5..120).
- Off: one read, the v1.5.0 statement - byte-identical (no log line).
- Armed: if the read has no candidate rows or B2 says `status: running`, re-read after the wait, up to N times; the first settled read (rows present, not rendering) is used. A failed re-read keeps the last values. A read that never settles keeps v1.5.0's result. One line: `[SHADOW-BOARD-READ v1.6.0] retries=<n> first=<empty|rendering|ok> final=<...> cands=<k>`.
- Workflow arms 6 re-reads (3 min max, inside `timeout-minutes: 15`); a settled first read costs nothing.
- Alternative considered: reading `_T10_Candidates` (the cockpit's staged, checksummed candidate tab). Rejected for now: it is the qualified pool, not the board's first table, so the shadow board's verdict inputs (Ticket SAR, Conf, ROI %) would change meaning.
- #866 (26_Decision_Chain v1.0.3) removes most of the repeated renders; this change makes the read safe against any render (chain, 4-hourly wrapper, manual).

## S3 - Tests (`tests/test_shadow_board_t10_settle.py`, 7 passed; offline, real module, faked Sheets seams)
| Case | Proves |
|---|---|
| S1 | Env parsing: default 0, junk 0, clamps; wait default 30, clamp 5..120. |
| S2 | `top10_rendering` on real cockpit status lines; cleared board parses to [], settled board to 3 rows. |
| S3 | cleared -> rendering with old rows -> settled: settled rows used, 2 re-reads, 2 waits of 30 s. |
| S4 | Never settles: bounded, last read kept; settled first read: no wait, one read. |
| S5 | A failing re-read keeps the last values; the loop continues. |
| S6 | `main()` off: one Top-10 read, no READ line, `cands=0` on a render window; armed: three reads, `retries=2 first=rendering final=ok cands=3`, board evaluates 3. |
| S7 | Version, manifest pin, workflow arming line, the v1.5.0 statement kept in the off branch. |

Golden negative on `cc450da7`: all 7 cases fail (the functions do not exist). Neighbouring suites pass: freeze harness 31/31, learning guards, manifest pins. Workflow audit: errors 0, warnings 0. The script's own `--selftest` reads 32/34 on this branch and on `cc450da7` alike (two pre-existing `fundamentals ->` checks).

## S4 - After merge
Nothing to arm: the workflow carries the setting. Read-back on the next evening run: the `[SHADOW-BOARD-READ ...]` line and `cands > 0`; the S-1 scorer's `[S1-SHAPE-GUARD] board_rows > 0`.

## S5 - Rollback
Set `TFB_SHADOW_T10_READ_RETRIES: "0"` in the workflow (or delete the line), or revert.

Not a licensed financial adviser; the owner decides and executes every trade.
