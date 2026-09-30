# TFB Commit Sheet — scripts/run_shadow_scorer.py v1.9.0 [P-176 EVIDENCE-DAY KEY + VISIBLE DUPLICATE REFUSAL]

Date: 2026-09-30 (Wednesday) · Lane: GitHub/Python (shadow_scorer.yml) · Build #1 of the day · Protocol: One-Pass

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | `scripts/run_shadow_scorer.py` v1.8.0 at HEAD `6bcf7df` (#629) |
| Base SHA-256 | `8a7b4476fbc6a3ff…` (2,073 lines) — identical to the 09-12 v1.8.0 commit record; zero drift |
| Workflow at HEAD | `.github/workflows/shadow_scorer.yml` (cron `20 15 * * *` = 18:20 Riyadh; `TFB_S1_BOARD_FRESH_GUARD: "observe"` L58; `dry_run` / `rollback_drill_passed` dispatch inputs) — **not changed by this delivery** |
| Environment golden | base `--selftest` = 89/89 in this workspace |

## S2 — Root (pinned on the live tab, the Actions run pages and the source)
`S1_Gate` on the 09-30 export is byte-identical to the 09-29 export (11/28, 41 excluded-infra, alpha +3.78 %) although scorer run **#77** ([36623303107](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/36623303107)) completed green in 23 s. Mechanism: `main()` keys every day by `_today_riyadh()` = the **wall-clock** Riyadh date at run time. Run **#76** ([36484835769](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/36484835769), the 09-28 slot) fired after 21:00 UTC → `today = 2026-09-29` → it appended the **09-28 board's** evaluation (v1.4.0, 5 rows, `chal fresh 1/5`) under 2026-09-29. Run #77 (the real 09-29 slot, 20:00 UTC) then hit `if any(h["date"] == str(today) …): print("already recorded — refusing duplicate"); return 0` — stdout only, no sheet row. One jitter event consumed two calendar days, the second unrecoverably; the board-fresh guard compounds it (its stale test compares the board's as-of with the same wrong `today`). The last six shadow-board crons fired 4.2–6.5 h late; the scorer's own slot needs only a 2 h 40 m slip to cross midnight Riyadh.

## S3 — Change (8 anchored edits, each `count == 1`)
| # | Site | Edit |
|---|---|---|
| E1 | header | v1.9.0 WHY block + `SCRIPT_VERSION = "1.9.0"` |
| E2 | after `_today_riyadh` | 4 pure helpers: `_day_key_mode()` (`TFB_S1_DAY_KEY`: **slot** default · `observe` · `wallclock`/`legacy`/`0`/`off` = kill; anything else → slot), `_slot_utc_hm(raw)` (`TFB_S1_SLOT_UTC` "HH:MM", default 15:20 = the yml cron; junk/out-of-range → default), `evidence_day(now_utc, slot_hm)` = `(now − slot_offset).date()` — the date of the most recent slot boundary ≤ now, `resolve_evidence_day(now_utc) → (date, details)` (details: mode, key, wallclock, slot, slot_utc, drift, now_utc) |
| E3 | `main()` | `today, _dk = resolve_evidence_day()` replaces `today = _today_riyadh()`; `[S1-DAY-KEY v1.9.0] mode=… key=… wallclock=… slot=…@HH:MMZ[ DRIFT]` printed on every path (incl. dry-run) |
| E4 | duplicate-refusal branch | the refusal now appends ONE `_Run_Log` row: `WARNING · shadow_scorer · S1_Gate · DUPLICATE_REFUSED · <message> \| <day-key line>` with JSON `{version, duplicate_of, day_key}`; `--dry-run` still writes nothing |
| E5 | verdict line | the day-key token appended **only when key ≠ wall-clock** (drift) |
| E6 | S1_Gate meta cell | same token, same condition |
| E7 | final `_Run_Log` JSON | `"day_key": {…}` added (always — the deploy/arming read-back) |
| E8 | `_selftest` | +10 DAYKEY checks incl. the #76 / #77 replays, the boundary second, the slot parser, kill/observe/default/junk modes, tz-aware input |

**Default ON (slot) — a stated deviation from default-OFF, per the file's own v1.5.0 B-2 precedent: the OFF state IS the defect.** Justification: at the slot (15:20 UTC = 18:20 Riyadh) `evidence_day` equals the Riyadh date, so every on-time run is byte-identical to v1.8.0 (harness D7 proves history rows, S1_Gate body and verdict identical under both modes); the two keys differ only on a run delayed past midnight Riyadh, where the wall-clock key is provably wrong (#76). Emad can veto with one yml line (`TFB_S1_DAY_KEY: "wallclock"`) or choose `"observe"` (wall-clock kept, drift logged — at the cost of another lost day if tonight slips).

Semantics worth stating: a `workflow_dispatch` **before** 15:20 UTC keys to the previous evidence day — that is the recovery path for a failed slot (dispatch next morning records yesterday); a genuine duplicate is refused visibly. `read_s1_calibration`'s staleness clock stays wall-clock (a tracker-freshness test, not an evidence key).

UNTOUCHED (S-1 integrity contract): `count_scored_days`, `evaluate_s1`, `basket_return_fresh`, `blended_benchmark_return_fresh`, `check_point_in_time`, `count_compliance_violations`, `chain_index`, `turnover_pct`, `cost_drag_pct`, day classes, `HISTORY_HEADER`, all constants. Rows never edited; the 2026-09-29 row written by #76 stands as it is (append-only law) — its content is the 09-28 board's evaluation and is already an excluded day; the true 09-29 session is unrecorded and is not reconstructed.

## S4 — Audits (×3 identical)
| Check | Result |
|---|---|
| Delivered SHA-256 | `0fc887f6eb40deae39b7a2b570af4d044103df4e05e6090a101cc262ab163ec8` (2,243 lines) |
| `py_compile` | PASS |
| AST | functions 59 → 63 (+4 listed above, **0 removed**) |
| Line audit | 6 base lines not verbatim = version constant · `today =` assignment · the two-line refusal print (now a variable) · the meta-cell line extension · the JSON closing line; every WHY block carried |
| Non-ASCII | 0 new characters vs base; 0 smart quotes |
| Delivered `--selftest` | **99/99** (89 base + 10 DAYKEY) |
| Harness `tests/test_s1_day_key_p176.py` (REAL module, REAL `main()` on an in-memory sheet stub; injected: fixed clock via a `datetime` subclass, `fetch_spot`, `sb._open_sheet`) | **D1–D7 PASS ×3, digest `59062a94e1ba3549` ×3** |
| D1 | full battery 99/99 via subprocess |
| D2 | replay table: 15:20Z→same day · **#76 21:40Z 09-28 → 2026-09-28** · **#77 20:00Z 09-29 → 2026-09-29** · 01:00Z 09-30 → 09-29 · 15:19:59Z → prior day, 15:20:00Z → new day |
| D3 | end-to-end default: #76 appends 2026-09-28 with `DRIFT` in verdict + JSON; the 09-29 board then makes #77 append **2026-09-29** (not refused); S1_Gate as-of follows the key |
| D4 | kill switch `wallclock`: v1.8.0 dates reproduced (late run writes 09-29; real 09-29 refused) — and the refusal writes `WARNING/DUPLICATE_REFUSED` with `duplicate_of=2026-09-29`; history untouched |
| D5 | `--dry-run` on the duplicate path: zero writes |
| D6 | past-midnight run with a 09-29 board under `TFB_S1_BOARD_FRESH_GUARD=observe`: slot key reads it **fresh** and keys 09-29; wall-clock flags `STALE` and keys 09-30 |
| D7 | on-time run: history rows, S1_Gate body, `_Run_Log` verdict **byte-identical** under slot vs wallclock (only the JSON `mode` field differs) |
| Existing batteries | `tests/test_s1_board_fresh_guard.py` K1–K5 ×3 digest `d318c49402b2e35c` (two literal pins updated: version 1.8.0→1.9.0, count 89/89→99/99); `tests/test_shadow_scorer_shape_guard.py` PASS ×3 |

## S5 — Delivery
| File | Destination |
|---|---|
| `scripts/run_shadow_scorer.py` | repo (full file) |
| `tests/test_s1_day_key_p176.py` | repo `tests/` (new) |
| `tests/test_s1_board_fresh_guard.py` | repo `tests/` (two pin literals; otherwise byte-identical) |
| `docs/evidence/TFB_Commit_Sheet_run_shadow_scorer_v1.9.0_2026-09-30.md` | repo `docs/evidence/` |

## S6 — Arming / read-back (GitHub lane)
1. Commit the four files **before 15:20 UTC today (18:20 Riyadh)** so tonight's slot runs v1.9.0. No yml change needed (default ON). Read-back = tonight's `_Run_Log` scorer row carries `{"version":"1.9.0", …, "day_key":{"mode":"slot", …}}`; the verdict line shows `DRIFT` only if the run slips past 21:00 UTC — in which case the history row still carries **2026-09-30** (the proof).
2. Veto / rollback: `TFB_S1_DAY_KEY: "wallclock"` beside `TFB_S1_BOARD_FRESH_GUARD` in `shadow_scorer.yml` (or `"observe"` to log drift without changing the key).
3. If the cron is ever moved, change `TFB_S1_SLOT_UTC` in the same commit (default 15:20 is hard-wired to today's cron).
4. Companion (not this build): the P-170 dispatcher extension (24_Sync_Dispatch v1.1.0) removes the jitter itself by firing board + scorer at fixed Riyadh times.
