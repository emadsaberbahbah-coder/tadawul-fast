# TFB Commit Sheet - run_weekly_brief v1.1.0 [BOARD-PARITY]

| Item | Value |
| --- | --- |
| Date | 2026-11-03 (daily improvement routine) |
| Branch | `claude/daily-2026-11-03` |
| Base | origin/main `3e326e26` (run_weekly_brief v1.0.3, run_shadow_board v1.6.0) |
| Lane | GitHub Actions only (`weekly_brief.yml`, Sunday 04:10 UTC); no Render deploy, no Apps Script paste |
| Gate | repository Variable `TFB_WEEKLY_BOARD_PARITY` = on / 1 / true / yes; default off = v1.0.3 byte for byte (the version string aside) |
| Priority | (a) a published recommendation is wrong: the Sunday advisor memo mails NO ACTION while the Shadow_Board proposes switches, and renders every board row BLOCKED |
| Version | 1.1.0; draft #795 claims 1.0.4 for an ungated fix of the column half (credited below) |

## S1 - Defect and evidence

The Weekly Decision Brief is the Sunday advisor memo (Master Plan v2.1 section 18.8). Its own DESIGN line says it imports the shadow-board computations "so the memo and the Shadow_Board tab can never disagree". On Sunday 1 Nov 2026 they disagreed:

| Run | Time (UTC) | Log |
| --- | --- | --- |
| weekly_brief 41300907168 | 11:13:43 | `[shariah_authority v1.1.0] load failed: RuntimeError:sheet_id_missing (TFB_SHARIAH_SHEET_ID / DEFAULT_SPREADSHEET_ID)` then `[WEEKLY-BRIEF v1.0.3] cands=10 switch=NO_ACTION entered=9 exited=9 flips=0 mail=SENT:1` |
| shadow_board 41303426265 | 11:47:45 | `[SHADOW-BOARD-READ v1.6.0] retries=0 first=ok final=ok cands=10` then `[SHADOW-BOARD v1.6.0] cands=10 eligible=8 blocked={'FLOOR_LOCKED': 2} switch=SWITCH_CANDIDATES auth_rows=408 fnd=10/10 ...` |

Same 10 Top_10 candidates, 34 minutes apart. The owner was mailed "NO ACTION this week ... Keeping everything is the recommendation" while the board had switch candidates.

Two causes, both reproduced offline with the real modules (`tests/test_weekly_brief_board_parity.py`, P3 and G1):

1. **Position.** run_shadow_board v1.4.0 (#500, 2026-09-13) inserted `ROI Src` at index 16 of `OUT_HEADER` and moved `Gen2 Eligible` to 17. run_weekly_brief v1.0.3 still reads `r[16]` as Gen2 Eligible in three places:
   - `_hard_gate_rows`: every row, TRADE rows included, renders `BLOCKED - <status>` with ROI / RT / Net Edge suppressed;
   - `render_memo` `key_cols`: the memo's last column is ROI Src, under that header;
   - `snapshot`: `_Brief_State` stores ROI Src (usually blank) as `gen2`.

   The switch verdict itself uses `shadow_board.eligible_symbols` (`r[-1]`), which is correct, so this half garbles the board table but not the headline.
2. **Lane.** `weekly_brief.yml` runs the imported board without the `shadow_board.yml` job env:
   - no `TFB_COMPLIANCE_SCREEN_RETIRED=1` (the screen was retired 2026-08-13; the board lane hardcodes it), so the model screen still blocks names;
   - no `TFB_BOARD_ENGINE_ROI=enforce`, and the brief never calls `fetch_engine_roi_map` / `apply_engine_roi`, so a blank Top_10 ROI stays `NO_ROI`;
   - no `TFB_SHADOW_T10_READ_RETRIES` (the brief never calls `read_top10_settled`);
   - no `DEFAULT_SPREADSHEET_ID`, so the authority read fails as logged.

   This half changes the headline: NO_ACTION instead of SWITCH_CANDIDATES.

Offline replay on one fake workbook (4 candidates, two with blank ROI that the engine pages fill, one at 50 % debt, one bank; holding 7010.SR at 1 %):

| | switch | compliance-eligible | memo board rows | authority |
| --- | --- | --- | --- | --- |
| REAL `shadow_board.main --dry-run`, shadow_board.yml env | SWITCH_CANDIDATES | 4 | - | read (3 rows) |
| brief v1.0.3 / gate off | NO_ACTION | 2 (MODEL_SCREEN_FAIL 2) | all four `BLOCKED - ...`, ROI / RT / Net Edge suppressed, last column ROI Src | `sheet_id_missing` |
| brief v1.1.0, gate on | SWITCH_CANDIDATES (SELL 7010.SR -> BUY AAPL.US / MSFT.US / 2222.SR) | 4 | AAPL.US 35.0 TRADE YES, MSFT.US 30.0 TRADE YES, 2222.SR 18.0 TRADE YES, 1120.SR 2.0 EDGE_BELOW_COST NO | read from this sheet |

No capital moves on this memo (SHADOW MODE banner), but it is the owner's Sunday decision document, and the next send is Sunday 8 Nov.

Overlap checked:

- Draft #795 (2026-10-18, 125 files, conflicted; its own body says its PR-event checks never started) carries an ungated header-name binding for cause 1 as v1.0.4. It does not touch cause 2 or the seam rule. This build credits it and takes 1.1.0.
- No other open PR touches `run_weekly_brief.py` or `weekly_brief.yml`.

## S2 - Change

| Site | Change |
| --- | --- |
| `scripts/run_weekly_brief.py` | v1.1.0. A dated WHY block below the v1.0.3 block and above `SCRIPT_VERSION` (this file stacks oldest first); prior blocks verbatim. |
| `_board_parity_enabled` | added: `TFB_WEEKLY_BOARD_PARITY` in on / 1 / true / yes, any case and padding. |
| `BOARD_LANE_ENV` | constant: `TFB_COMPLIANCE_SCREEN_RETIRED=1`, `TFB_BOARD_ENGINE_ROI=enforce`, `TFB_SHADOW_T10_READ_RETRIES=6`, the literal knobs of `shadow_board.yml`'s job env. |
| `_apply_board_lane_env` | added (gate on, first thing in `main`). Each knob that is unset or blank takes the board value; one already set in this lane is kept. `DEFAULT_SPREADSHEET_ID` takes the sheet this run opens (`--sheet-id`, `TARGET_SHEET_ID`, `TRACK_SHEET_ID`, the order of `shadow_board._open_sheet`) when unset. Returns where the id came from, never the id. |
| `main` (gate on) | the Top_10 read goes through `shadow_board.read_top10_settled` when retries > 0; blank ROI is filled through `fetch_engine_roi_map` / `apply_engine_roi` when the mode is not off. These are the calls and the order of `shadow_board.main`. |
| `_board_cols`, `snapshot_by_header`, `MEMO_COLS` | added: columns bound by `shadow_board.OUT_HEADER` name. A missing name raises `ValueError` before any mail, state or _Run_Log write. |
| `_hard_gate_rows_by_header` | added (gate on). A row that is not Gen2-eligible and still reads TRADE (the 1015.KL class; reachable only with `TFB_SB_COST_LEGACY=1`) is terminalized as v1.0.3 does. A row the board already BLOCKED keeps its verdict and loses ROI / RT / Net Edge. Every other row (EDGE_BELOW_COST, NO_ROI, NO_COST_MODEL, TRADE + YES) reads as the tab reads it. v1.0.3's rule (any Gen2 != YES) also relabelled compliant EDGE_BELOW_COST / NO_ROI rows; under the retired screen that reads "BLOCKED - SCREEN_RETIRED" on a row the board calls EDGE_BELOW_COST. |
| `render_memo` | new keyword `lane=None`. None is v1.0.3's path. With a lane: the seam and columns above, plus one `BOARD LANE | ...` line under PROVENANCE (text) and one footer line (HTML). |
| `_board_lane_line` | added: the shared evidence text. |
| evidence (gate on) | one stdout line `[WEEKLY-BRIEF-PARITY v1.1.0] lane=shadow_board screen_retired=1 engine_roi=enforce applied=a/b unresolved=u roi_errors=e t10_read=first->final/retries authority_sheet=this_sheet eligible=n switch=S`; `_Run_Log` details gain `board_parity`. |
| `_selftest` | three v1.1.0 checks on the 18-column board: 18/18. |
| `.github/workflows/weekly_brief.yml` | `TFB_WEEKLY_BOARD_PARITY: ${{ vars.TFB_WEEKLY_BOARD_PARITY \|\| 'off' }}` in the job env, after `TFB_MAIL_TO`. **Not armed.** |
| `scripts/verify_deployment.py` | pin run_weekly_brief 1.1.0 (pin only). |
| `.github/workflows/ci.yml` | 1.1.67 (1.1.62 / 1.1.63 are claimed by open #778, 1.1.64 by #882, 1.1.65 by #889, 1.1.66 by #897). A separate step in the blocking heavy job runs the harness, after fetching `3e326e26` for its golden legs. |

Functions: 6 added, 0 removed.

Unchanged in every mode:
- `run_shadow_board.py`, the Shadow_Board tab, `eligible_symbols`, `evaluate_board`, the cost model and `advisor_switch_scan`;
- the mail chain and `_Brief_State`'s shape;
- `compute_diff`, which still compares status and verdict only;
- the cron.

## S3 - Off identity

`TFB_WEEKLY_BOARD_PARITY` unset, blank, off, 0, false, no, enforce, observe or onn, crossed with the default run, `--no-mail` and `--dry-run`. Against the untouched v1.0.3 (`git show 3e326e26`), each run compares:
- the return code and stdout;
- every mail (subject, text, HTML);
- every sheet read (in order) and every write, including the `_Brief_State` update and the `_Run_Log` row minus its timestamp;
- the authority fetch and the sleeps;
- `os.environ` afterwards.

All equal, with only the version string normalized (P1).

## S4 - Measured

`tests/test_weekly_brief_board_parity.py` passes **PASS 30/30** as a script and under pytest.
- In the contract-lane emulation (PyYAML, pandas, yfinance, bs4 and lxml blocked): 30 passed.
- Without git history: 25 passed and 5 skipped (P1 x3, G1, V1b need the base).

| Case | What it checks |
| --- | --- |
| P1 / P1b / P1c | off = v1.0.3 on every surface (S3); the six on-spellings; junk stays off |
| P2 | on: the REAL `shadow_board.main` (dry run, the shadow_board.yml job env parsed from the file) and the memo, on one fake book, agree on switch verdict, compliance-eligible count, blocked counts and every board row (after the documented seam); the authority read uses this sheet |
| P3 | off on the same book is the 1 Nov defect: NO_ACTION against SWITCH_CANDIDATES, every row BLOCKED, `sheet_id_missing` |
| P4 | on: the header ends with Gen2 Eligible, TRADE rows keep their economics, AAPL.US shows the engine-filled 35.0, `_Brief_State` gen2 is YES / NO (off: blank, the defect) |
| P5 | the seam table: TRADE + NO, BLOCKED + NO, EDGE_BELOW_COST / NO_ROI / NO_COST_MODEL / TRADE + YES / lower-case yes, short and non-list rows; pure; v1.0.3's `_hard_gate_rows` byte-equal to the base on the 17-column board |
| P6 | lane env: unset and blank knobs filled, preset kept; sheet id precedence; preset id kept; the id never in the evidence |
| P7 | on: a rendering Top_10 is re-read twice (two 30 s sleeps, stubbed) and the memo carries the settled 4 candidates; off reads once (0 candidates) |
| P8 | on: engine ROI applied 2/2 from Global_Markets / Market_Leaders; off never opens those tabs; a preset `observe` is kept and AAPL.US stays NO_ROI |
| P9 | on: a header without Gen2 Eligible raises before any write; off on the same header still mails |
| P10 | the parity line exactly, the BOARD LANE memo line after PROVENANCE, the HTML footer line, `_Run_Log` details; none of them when off |
| P11 | drift: every literal knob of shadow_board.yml's job env is in `BOARD_LANE_ENV` with the same value or set identically by weekly_brief.yml; `BOARD_LANE_ENV` has nothing extra; every `vars` knob there is mapped in weekly_brief.yml too or is `DEFAULT_SPREADSHEET_ID`; cross-checked with PyYAML when present |
| G1 | golden negative: v1.0.3 with the gate set mails NO_ACTION against the board's SWITCH_CANDIDATES, and P2's check fails on it |
| V1 / V1b | version floor, verifier pin, the single job-env mapping line, the ci.yml step, WHY order, ASCII test file; against the base: ASCII added lines, the exact removed lines, exactly six functions added and none removed |

Golden negative of the whole file (`TFB_TEST_WEEKLY_DELIVERED=` the v1.0.3 copy): 27 failed, 2 passed (P3 and G1, which assert the base's defect), 1 skipped (V1b, 1.1.0 only).

Mutants, each run against the harness: **19 / 19 killed**:
1. state `gen2` bound to ROI Src;
2. seam reverted to v1.0.3's predicate;
3. positional memo columns when on;
4. lane without the retired screen;
5. no authority sheet id;
6. engine ROI not applied;
7. no settled read;
8. a preset knob overwritten;
9. the gate reading any word;
10. no `_Run_Log` evidence;
11. BLOCKED rows keeping their economics;
12. TRADE + NO not terminalized;
13. a case-sensitive Gen2 check;
14. a blank knob not filled;
15. the lane env applied when off;
16. a missing header falling back to a position;
17. no BOARD LANE line;
18. positional state when on;
19. a sheet id overriding a preset one.

Other checks:
- `run_weekly_brief.py --selftest`: 18/18;
- `tests/test_verify_manifest_pins.py`: 3 passed. The flag registry, render env contract, repository workflow audit, track flags and workflow flag mapping suites: 50 passed, 1 skipped;
- `audit_repository_workflows.py`: errors=0 warnings=0. `render_env_contract.py --check`: OK.
- `run_shadow_board.py --selftest`: 32/34 here and on untouched main 3e326e26. The two "fundamentals ->" fixtures fail the same way on main; this PR does not touch that file.
- `compileall` (main.py core routes scripts tests): rc 0. ruff E9/F63/F7/F82 on the changed .py files: clean.

## S5 - Owner steps (proposed; nothing is armed here)

1. Merge. This is GitHub Actions only: no deploy, no paste.
2. Optional check before arming: dispatch "TFB Weekly Decision Brief" with `dry_run=true` once with the Variable unset and once with `TFB_WEEKLY_BOARD_PARITY=on`. The second log shows the `[WEEKLY-BRIEF-PARITY v1.1.0] ...` line, and its `eligible=` / `switch=` match the latest Shadow_Board run's line.
3. Set the repository Variable `TFB_WEEKLY_BOARD_PARITY=on`.
4. On Sunday 8 Nov 07:10 Riyadh, expect three things:
   - the memo's verdict and board table read like the Shadow_Board tab;
   - its PROVENANCE line shows `authority rows=408`;
   - the CHANGES section lists one FLIP per row whose status moves from the model-screen codes to SCREEN_RETIRED. That is a one-time diff against the last v1.0.3 state.

Rollback: unset the Variable (= v1.0.3).

## S6 - Known limits and not done

- The memo still computes the board itself, at 04:10 UTC on Sunday; it does not read the Shadow_Board tab. Parity means the same inputs and code on the same workbook, not the same instant. A Top_10 render between the two runs can still differ, and the settled read only protects against a render in progress.
- `TFB_BOARD_FREEZE_NONTRADING` is not mirrored; neither lane sets it. `TFB_SB_COST_LEGACY` and `TFB_SB_ATOMIC_WRITE` stay whatever each lane sets (both unset).
- A board-only kill switch added later to shadow_board.yml as a literal knob fails P11 until it is mirrored here. That is intended: the memo should not drift silently.
- Upstream `evaluate_board` labels a FLOOR_LOCKED row `BLOCKED - <invest-ok status>` (for example `BLOCKED - SCREEN_RETIRED`); the memo inherits the tab's wording. That label is the board's, not changed here.
- No change to the cron. The brief is scheduled Sunday 04:10 UTC, the board daily at 05:10 and 14:10 UTC (GitHub ran both late on 1 Nov: 11:13Z and 11:47Z). Arming the gate does not change when either runs.
- No live workbook read or write, no provider call, no mail: everything is offline with stubs.
