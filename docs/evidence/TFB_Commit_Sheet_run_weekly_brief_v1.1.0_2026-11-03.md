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
| `_apply_board_lane_env` | added (gate on, first thing in `main`). Each knob that is unset or blank takes the board value; one already set in this lane is kept (P11 fails if weekly_brief.yml presets one). `DEFAULT_SPREADSHEET_ID` takes the sheet this run opens (`--sheet-id`, `TARGET_SHEET_ID`, `TRACK_SHEET_ID`, the order of `shadow_board._open_sheet`) when unset. Returns where the id came from, never the id; `authority_sheet` says `shariah_sheet_id` when `TFB_SHARIAH_SHEET_ID` (which the authority reader takes first) is set. |
| `main` (gate on) | the Top_10 read goes through `shadow_board.read_top10_settled` when retries > 0; blank ROI is filled through `fetch_engine_roi_map` / `apply_engine_roi` when the mode is not off. These are the calls and the order of `shadow_board.main`. |
| `_board_cols`, `snapshot_by_header`, `MEMO_COLS` | added: columns bound by `shadow_board.OUT_HEADER` name. A missing name raises `ValueError` before any mail, state or _Run_Log write. `snapshot_by_header` saves the verdict after the seam, so a CHANGES line never says a blocked name turned TRADE. |
| `_hard_gate_rows_by_header` | added (gate on). A row that is not Gen2-eligible is terminalized as v1.0.3 does (`BLOCKED - <status>`, ROI / RT / Net Edge suppressed) when it reads TRADE, when the board already BLOCKED it (its verdict is kept word for word), or when its own gate evidence says hard block: a status outside `compliance_gate.INVEST_OK_STATUSES` (GATE_DISABLED aside) or Floor OK not YES. That covers every row `TFB_SB_COST_LEGACY=1` scores. A compliant EDGE_BELOW_COST / NO_ROI / NO_COST_MODEL row and TRADE + YES read as the tab reads them. v1.0.3's rule (any Gen2 != YES) also relabelled those compliant rows; under the retired screen that reads "BLOCKED - SCREEN_RETIRED" on a row the board calls EDGE_BELOW_COST. |
| `render_memo` | new keyword `lane=None`. None is v1.0.3's path. With a lane: the seam and columns above, plus one `BOARD LANE | ...` line under PROVENANCE (text) and one footer line (HTML). |
| `_board_lane_line` | added: the shared evidence text. |
| evidence (gate on) | one stdout line `[WEEKLY-BRIEF-PARITY v1.1.0] lane=shadow_board screen_retired=1 engine_roi=enforce applied=a/b unresolved=u roi_errors=e t10_read=first->final/retries authority_sheet=this_sheet eligible=n switch=S`; `_Run_Log` details gain `board_parity`. |
| `_selftest` | four v1.1.0 checks on the 18-column board (TRADE + YES, a compliant EDGE_BELOW_COST row, TRADE + NO, the production BLOCKED shape, a legacy-scored hard block, the saved verdict): 19/19. |
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

`TFB_WEEKLY_BOARD_PARITY` unset, blank, off, 0, false, no, enforce, observe or onn, crossed with the default run, `--no-mail`, `--dry-run` and `--sheet-id`, crossed with six book / env variants (default, no `_Brief_State`, empty Top_10, lane knobs preset, authority readable, no `TARGET_SHEET_ID`). Against the untouched v1.0.3 (`git show 3e326e26`), each run compares:
- the return code, stdout and stderr (the authority loader's warnings included);
- every mail (subject, text, HTML);
- every sheet read (in order) and every write, including the `_Brief_State` update and the `_Run_Log` row minus its timestamp;
- the authority fetch and the sleeps;
- `os.environ` afterwards.

All equal, with only the brief's own version tokens normalized (P1).

## S4 - Measured

`tests/test_weekly_brief_board_parity.py` passes **PASS 55/55** as a script and under pytest (30/30 at the first push; the review round below added the rest).
- In the contract-lane emulation (PyYAML, pandas, yfinance, bs4 and lxml blocked): 55 passed.
- Without git history: 29 passed and 26 skipped (P1 x24, G1, V1b need the base). CI fetches the base, and its first run shows 30 passed, 0 skipped.

| Case | What it checks |
| --- | --- |
| P1 / P1b / P1c | off = v1.0.3 on every surface (S3); the six on-spellings; junk stays off |
| P2 | on: the REAL `shadow_board.main` (dry run, the shadow_board.yml job env parsed from the file) and the memo (weekly_brief.yml's own job env), on one fake book, agree on switch verdict, compliance-eligible count, blocked counts and every board row (after the documented seam); the authority read uses this sheet |
| P2b | the same with four hard blocks (two BROKER_UNTRADABLE, two floor-locked), with and without `TFB_SB_COST_LEGACY=1`: under legacy the board scores them TRADE / NO_ROI / EDGE_BELOW_COST, and the memo still shows every one BLOCKED with ROI / RT / Net Edge suppressed |
| P3 | off on the same book is the 1 Nov defect: NO_ACTION against SWITCH_CANDIDATES, every row BLOCKED, `sheet_id_missing` |
| P4 | on: the header ends with Gen2 Eligible, TRADE rows keep their economics, AAPL.US shows the engine-filled 35.0, `_Brief_State` gen2 is YES / NO (off: blank, the defect) |
| P5 | the seam table: TRADE + NO, BLOCKED + NO (worded verdict kept), hard blocks by evidence (BROKER_UNTRADABLE, MODEL_SCREEN_FAIL, Floor OK NO or blank) whatever their verdict, compliant EDGE_BELOW_COST / NO_ROI / NO_COST_MODEL / TRADE + YES / lower-case yes / GATE_DISABLED, short and non-list rows; pure; v1.0.3's `_hard_gate_rows` byte-equal to the base on the 17-column board |
| P6 / P6b | lane env: unset and blank knobs filled, preset kept; sheet id precedence; preset id kept; `authority_sheet=shariah_sheet_id` when that id is set; no id in the evidence; `main` passes `--sheet-id` through |
| P7 | on: a rendering Top_10 is re-read twice (two 30 s sleeps, stubbed) and the memo carries the settled 4 candidates; off reads once (0 candidates) |
| P8 | on: engine ROI applied 2/2 from Global_Markets / Market_Leaders; off never opens those tabs; a preset `observe` is kept and AAPL.US stays NO_ROI |
| P9 | on: a header without Gen2 Eligible raises before any write; off on the same header still mails |
| P10 | the parity line exactly, the BOARD LANE memo line after PROVENANCE, the HTML footer line, `_Run_Log` details; none of them when off |
| P11 | drift: every literal knob of shadow_board.yml's job env is in `BOARD_LANE_ENV` with the same value or set identically by weekly_brief.yml; `BOARD_LANE_ENV` has nothing extra; every `vars` knob there is mapped in weekly_brief.yml too or is `DEFAULT_SPREADSHEET_ID`; weekly_brief.yml presets no lane knob; no `TFB_` / `DEFAULT_` knob sits in a step env of either workflow; a `TFB_BOARD_FREEZE_*` knob in shadow_board.yml fails (the memo would need `freeze_verdict` in code); cross-checked with PyYAML when present |
| P12 | legacy cost on: `_Brief_State` saves the memo's verdict (BLOCKED) for every hard block, and no non-eligible row is saved as TRADE |
| G1 | golden negative: v1.0.3 with the gate set mails NO_ACTION against the board's SWITCH_CANDIDATES, and P2's check fails on exactly that assertion |
| V1 / V1b | version floor, verifier pin, the single job-env mapping line, the ci.yml step inside the blocking heavy job with its base fetch, WHY order, ASCII test file; against the base: ASCII added lines, the exact removed lines, exactly six functions added and none removed |

Golden negative of the whole file (`TFB_TEST_WEEKLY_DELIVERED=` the v1.0.3 copy): 52 failed, 2 passed (P3 and G1, which assert the base's defect), 1 skipped (V1b, 1.1.0 only).

Mutants, each run against the harness: **26 / 26 killed** (19 at the first push, 7 added in the review round):
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
19. a sheet id overriding a preset one;
20. no hard-block evidence;
21. the floor not checked;
22. GATE_DISABLED treated as a hard block;
23. the state verdict taken before the seam;
24. `authority_sheet` ignoring `TFB_SHARIAH_SHEET_ID`;
25. a worded BLOCKED verdict relabelled;
26. `--sheet-id` not passed through.

Other checks:
- `run_weekly_brief.py --selftest`: 19/19;
- `tests/test_verify_manifest_pins.py`: 3 passed. The flag registry, render env contract, repository workflow audit, track flags and workflow flag mapping suites: 50 passed, 1 skipped;
- `audit_repository_workflows.py`: errors=0 warnings=0. `render_env_contract.py --check`: OK.
- `run_shadow_board.py --selftest`: 32/34 here and on untouched main 3e326e26. The two "fundamentals ->" fixtures fail the same way on main; this PR does not touch that file.
- `compileall` (main.py core routes scripts tests): rc 0. ruff E9/F63/F7/F82 on the changed .py files: clean.

## S5 - Owner steps (proposed; nothing is armed here)

1. Merge. This is GitHub Actions only: no deploy, no paste.
2. Set the repository Variable `TFB_WEEKLY_BOARD_PARITY=on` (the workflow has no dispatch input for it).
3. Optional check: dispatch "TFB Weekly Decision Brief" with `dry_run=true` (no mail, no state save). The log shows the `[WEEKLY-BRIEF-PARITY v1.1.0] ...` line, and its `eligible=` / `switch=` match the latest Shadow_Board run's line.
4. On Sunday 8 Nov 07:10 Riyadh, expect three things:
   - the memo's verdict and board table read like the Shadow_Board tab;
   - its PROVENANCE line shows `authority rows=408`;
   - the CHANGES section diffs against the last v1.0.3 state once: a status FLIP per row (model-screen code -> SCREEN_RETIRED) and, for most rows, a verdict FLIP too (NO_ROI / BLOCKED - MODEL_SCREEN_* -> TRADE / EDGE_BELOW_COST), so `flips=` reads about twice the row count that Sunday.

Rollback: unset the Variable. The gate-off code is v1.0.3's; its first memo after an armed one shows the reverse diff once, because the saved state is the armed one.

## S6 - Known limits and not done

- The memo still computes the board itself, at 04:10 UTC on Sunday; it does not read the Shadow_Board tab. Parity means the same inputs and code on the same workbook, not the same instant. A Top_10 render between the two runs can still differ, and the settled read only protects against a render in progress.
- `TFB_BOARD_FREEZE_*` is not mirrored, and cannot be by env: `shadow_board.main` calls `freeze_verdict`, this script does not. Neither lane sets it; P11 fails if shadow_board.yml ever does, so the code follows. `TFB_SB_COST_LEGACY` and `TFB_SB_ATOMIC_WRITE` stay whatever each lane sets (both unset).
- A board-only kill switch added later to shadow_board.yml as a literal knob fails P11 until it is mirrored here. That is intended: the memo should not drift silently.
- `os.environ` keeps the lane knobs after `main` returns. The workflow runs one process per memo, so nothing reads them afterwards.
- Upstream `evaluate_board` labels a FLOOR_LOCKED row `BLOCKED - <invest-ok status>` (for example `BLOCKED - SCREEN_RETIRED`); the memo inherits the tab's wording. That label is the board's, not changed here.
- No change to the cron. The brief is scheduled Sunday 04:10 UTC, the board daily at 05:10 and 14:10 UTC (GitHub ran both late on 1 Nov: 11:13Z and 11:47Z). Arming the gate does not change when either runs.
- No live workbook read or write, no provider call, no mail: everything is offline with stubs.

## S7 - Review (2026-11-03, after the first push `1fd35775`)

Codex: no findings on `1fd35775`.

An independent four-lens review had an adversarial verifier per finding. The lenses were off identity, parity correctness, seam and downstream, and tests / CI / claims. 7 findings were confirmed (all minor or nit) and 2 rejected. All confirmed findings are fixed in the follow-up commit:

| Finding | Fix |
| --- | --- |
| P11 missed a lane knob preset in weekly_brief.yml (kept by `_apply_board_lane_env`) and step-level env; P2 used a hardcoded memo env | P11 checks both; P2 builds the memo env from weekly_brief.yml's job env |
| A freeze armed in shadow_board.yml would pass P11 once mirrored, yet do nothing (no `freeze_verdict` here) | P11 fails on any `TFB_BOARD_FREEZE_*` knob there, with a message that code must follow |
| Under `TFB_SB_COST_LEGACY=1` the seam left hard-blocked EDGE_BELOW_COST / NO_ROI rows with their economics (only TRADE was terminalized) | hard block read from the row's own evidence (status, Floor OK); P2b end to end with and without legacy, P5 rows |
| Under legacy, CHANGES could print `FLIP X: ... -> TRADE` for a name the table shows BLOCKED (carried over from v1.0.3) | `snapshot_by_header` saves the verdict after the seam; P12 |
| The selftest never exercised the BLOCKED branch | four checks on production-shaped rows: 19/19 |
| The KNOWN note and S5 understated the first-armed FLIPs (verdict flips too) | reworded, with the reverse diff after a rollback |

The cheap nits are also fixed:
- P1 compares stderr, normalizes only the brief's own version tokens, and covers six book / env variants and `--sheet-id`;
- G1 accepts only the exact NO_ACTION mismatch;
- V1 pins the step inside the blocking heavy job;
- the evidence names `TFB_SHARIAH_SHEET_ID` when it decides the authority sheet;
- the owner steps are in order (the Variable is set before the dry-run dispatch).

Rejected:
- "Rollback is not v1.0.3": its verifier showed the gate-off code equals v1.0.3 byte for byte on the same workbook; the flips come from the saved state. Documented in S5.
- "The authority exception text can leak the sheet id into the memo": real in mechanism. A network error from the real loader carries the request path, sheet id included, into `sa.last_error()`, which the PROVENANCE line prints (unchanged v1.0.3 lines). Not a defect, for four reasons:
  - the id is the repo Variable `DEFAULT_SPREADSHEET_ID`, printed in every run log;
  - the memo goes to the workbook owner only;
  - the board already writes the same `authority_error=` text to its tab;
  - `_apply_board_lane_env` itself returns only where the id came from.

Left as is (nits):
- the missing-header `ValueError` fires after the reads and leaves no `_Run_Log` row (a red run is the signal);
- the lane env outliving `main` (one process per run);
- FLOOR_LOCKED rows read `BLOCKED - SCREEN_RETIRED`, which is the board's own wording.
