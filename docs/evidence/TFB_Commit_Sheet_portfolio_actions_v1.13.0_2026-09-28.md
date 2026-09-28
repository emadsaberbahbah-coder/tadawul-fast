# TFB commit sheet — portfolio_actions v1.13.0 [P-168b SESSION-KEYED ADD CONFIRMATION] — 2026-09-28

**Build #1 of 2026-09-28 (backend lane).** Owner Claude · approver/executor Emad · read-only session: nothing committed, no ENV touched, no order placed.

## 1. What and why (one mechanism)
`_apply_add_confirmation` requires an ADD to qualify on `add_confirm_days` (2) "distinct consecutive days" — keyed on the **UTC calendar date**. The portfolio page refreshes every 4 h, seven days a week, so a signal that qualifies on Friday's close is counted on Sunday and again on Monday **before the US open**. Live specimen: DDI.US "ADD pending (day 1/2)" on Sun 2026-09-27 → **"ADD confirmed (day 2/2)" on Mon 2026-09-28 03:40Z** with the price 13.00 unchanged since the 09-23 close — two confirmation days, zero completed sessions (the external 28-Sep audit read the same rows). The v1.6.0 persistence fix made the calendar clock durable; it did not change what a "day" is. This is the ADD-counter half of the P-144/P-168 day-key class already fixed on the cockpit side.

v1.13.0 makes the clock **session-keyed** behind one gate:

| ENV (read at call time, no restart) | Values | Effect |
|---|---|---|
| `TFB_PF_CONFIRM_SESSION` | `off` (default) · `observe` · `enforce` | off = v1.12.2 byte-identical; observe = legacy verdict + one countable tag per raw-ADD row from a SHADOW session chain; enforce = the confirmation clock keys on the holding venue's last **completed** session |
| `TFB_PF_SESSION_HOLIDAYS` | csv of `YYYY-MM-DD` (all venues), optional | adds Eid / ad-hoc closures to the built-in fixed-date sets (NYSE 2026, Tadawul fixed dates). A missing holiday fails OPEN (the date is counted — the legacy direction), never CLOSED |

Mechanics: venues from the symbol suffix — US (default, incl. bare symbols), AMER, EU, ASIA Mon–Fri; KSA (`.SR`), GULF Sun–Thu — each with a deliberately **late** UTC close (US 21:00Z year-round) so a confirmation is delayed by at most hours, never granted early. Session key = last completed session at run time; "consecutive" = the session immediately before it; same-session reruns stay frozen; a skipped session restarts (the contract). Under **enforce** the same store / Redis keys are used with session dates (a later flip back to off finds a date ≤ today and restarts conservatively) and the rendered reason gains `; session YYYY-MM-DD` inside the day counter. Under **observe** the shadow chain lives in its own dict and under the `~S~` namespace of the same Redis prefix (same TTL); the tag reads `[confirm-session-observe] venue=US session=2026-09-25 legacy 2/2 vs session 1/2 - FLIP; legacy clock kept`. A calendar fault under enforce falls back to the legacy UTC pair for that call (one WARNING); the P-165 fail-closed contract on the store path is untouched. `meta.confirm_session` is emitted only when the gate is armed, so the off payload is byte-identical.

## 2. Pinned source (S1)
| Item | Value |
|---|---|
| Repo / branch | `emadsaberbahbah-coder/tadawul-fast` · `main` |
| HEAD at fetch | `4e15a127c971c98b574c15906bcde3d553f8a798` (#611, 2026-09-27 19:26 Riyadh) — the commit the 28-Sep external audit inspected |
| Base file | `core/analysis/portfolio_actions.py` v1.12.2 · 3,357 lines · sha256 `ac6e32ac80a500ff8005d9c0f0e28b5d2a55949385fbe090f6b216f4a882ba6e` |
| Live versions (Portfolio_Decision 07:40:27) | route v4.16.0 · actions v1.12.2 · builder v1.22.0 (HEAD already carries `opportunity_builder` **v1.22.1** — Render has not redeployed) |
| Runtime | python-3.11 |

## 3. Delivered files (S5)
| File | Lines | sha256 |
|---|---|---|
| `core/analysis/portfolio_actions.py` **v1.13.0** | 3,633 | `211f8c4c409cc976e58a54dcf83e7189b2214baf5d65b2fc36013df492702f6a` |
| `tests/test_pf_confirm_session_p168b.py` (new, 9 tests) | 262 | `250babc751aa1253c442f0f03ba8f27b3b1c6a4c227d390051e5cbdd770ab82a` |
| `portfolio_actions_v1.12.2_to_v1.13.0.diff` (review aid, not for commit) | 306 | — |

## 4. Build proof (S3/S4)
- **Anchored edits: 8, each `count == 1` asserted** — version constant + WHY block; helpers block inserted before `_apply_add_confirmation`; clock line (`_confirm_clock`) and yesterday-key line inside the gate; the two rendered reason strings gain the `%s` session suffix (empty under legacy); call-site witness + observe seam; armed-only meta echo.
- `py_compile` OK · **AST defs 88 → 98: +10 (`_env_confirm_session_mode`, `_confirm_venue`, `_confirm_session_holidays`, `_confirm_is_session_day`, `_confirm_session_key`, `_confirm_prev_session`, `_confirm_clock`, `_confirm_session_shadow_count`, `_apply_confirm_session_observe`, `_confirm_session_meta`), removed 0** · diff +283 / −7 lines · smart quotes on added lines 0 · net-new non-ASCII 0 (the two em dashes are carried inside the existing reason strings) · CR characters 0.
- **Real-module dual-tree harness ×3** (base v1.12.2 vs delivered v1.13.0 loaded as real modules; real dependency `opportunity_builder` v1.22.1 at HEAD; real rows = the 2026-09-28 `My_Portfolio` export, 5 holdings; live panel 38,047.50 / 10 / 20 / 30 / 70 / 80 / Advisory; FX 3.7560): digest `b5da227e7b3f0d23` identical on all three runs.

| Gate | Result |
|---|---|
| G1 off = byte-identical | integration payloads equal under persist 1 and 0, with and without a seeded legacy chain; unit matrix (day 1 / same day / yesterday chain / gap / non-ADD) equal; no `meta.confirm_session` key |
| G2 observe | verdicts, capped_from and adds_funded identical to base; exactly one tag per raw-ADD row rendered at BOTH payload sites (action_reason + advisor_note); DDI tag `venue=US session=2026-09-25 legacy 2/2 vs session 1/2 - FLIP`; raw non-ADD rows clear the shadow and print nothing; an injected calendar fault is a pass-through |
| G3 enforce clock | Sun 06:00Z / Mon 03:40Z / Mon 20:40Z → all `(day 1/2; session 2026-09-25)` (frozen — no completed session); Tue 00:40Z → `ADD confirmed (day 2/2; session 2026-09-28)`; Wed → 3/2; skipped session → restart 1/2; non-ADD clears; calendar: Labor Day 09-07 skipped, KSA Friday → Thursday, National Day 09-23 skipped, env holidays honoured; calendar fault → legacy pair + WARNING; P-165 fail-closed preserved (sentinel store untouched) |
| G4 golden negative | BASE reproduces the defect (Sun 06:00Z pending → Mon 03:40Z confirmed 2/2); delivered tree in off mode reproduces it byte-identically |
| G5 enforce on the real book | Mon 03:40Z: base = today's page (DDI ADD, adds_funded 7,422); enforce = DDI HOLD pending `session 2026-09-25`, capped_from ADD, adds_funded 0, alert `add_confirmation_pending` 1, meta mode enforce; Tue 00:40Z: DDI ADD confirmed `session 2026-09-28`, funded 7,422 |

- **Tests:** new file 9/9 ×3 on v1.13.0; on v1.12.2 it fails 8/9 (T2 golden negative passes on both by design) — it discriminates. Existing batteries on the delivered tree: `test_portfolio_actions.py` + `test_pf_add_confirm_failclosed_p165.py` (incl. T6 integration on today's export) + `test_pf_dd_guard_sukuk_exempt.py` → 28/28 with the new file.
- Harness caveat (disclosed): Redis is absent in the harness process, so the persist-ON paths ran with the client marked dead (memory-only, strict consecutiveness) — exactly the documented v1.6.0 behaviour when Redis is unreachable; the Redis write/read of the `~S~` shadow keys is exercised only in production.

## 5. Deliberate scope cuts (register)
- **P-153** (sukuk row still receives equity-style stop/TP levels and the `[f1-observe]` tag) — display class, next v1.13.x; not bundled to keep one mechanism per build.
- **P-169** thesis-exit rule — waits for the `backtest_pf_thesis_exit` dispatch (prerequisite: SBAC Sell Date 2026-08-24 → 2026-09-24 + `plRefinalizeRow`).
- Eid / ad-hoc Tadawul closures are not built in (announced yearly) — supply via `TFB_PF_SESSION_HOLIDAYS`; a missing date counts as a session (fail-open, legacy direction).
- The cockpit's own stability clocks are unchanged (P-144 epoch key, P-168 outage pause); this build is the PF ADD-counter half only.

## 6. Operator steps (one action each; GitHub web UI)
1. Upload the delivered `portfolio_actions.py` over `core/analysis/portfolio_actions.py`: https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/core/analysis
2. Upload `test_pf_confirm_session_p168b.py` into `tests/`: https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/tests
3. Upload this sheet into `docs/evidence/`: https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/docs/evidence
4. Commit message: `portfolio_actions v1.13.0 [P-168b] session-keyed ADD confirmation (default off = byte-identical) + tests`
5. Render: if the push does not auto-deploy, **Manual Deploy** from `main` — this deploy also carries `opportunity_builder` v1.22.1 (already at HEAD). Read-back: next Portfolio_Decision status line `actions v1.13.0`; next cockpit status line `builder v1.22.1`; page unchanged (gate off).
6. Rollback: revert the commit (no ENV to unset).

## 7. Arming — a separate act, flagged for approval
- **Observe (recommended today as the day's one Render ENV change, after step 5's read-back):** `TFB_PF_CONFIRM_SESSION=observe`. Read-back on the next Portfolio_Decision run: DDI's row (while it still qualifies for ADD) carries `[confirm-session-observe] venue=US session=… legacy N/2 vs session M/2 [- FLIP]; legacy clock kept` at both sites, `meta.confirm_session.mode = "observe"`, verdicts unchanged. Zero tags on a run where no holding qualifies raw-ADD is the expected quiet state, not a failure.
- **Enforce:** recommendation-timing change → Program v2 discipline #3 (no parameter change outside the Saturday review) — decide at the 3-Oct review after ≥1 clean observe read-back. Expected effect on the current book: DDI's ADD would have read pending on Sunday and Monday and confirmed on Tuesday 00:40Z (after Monday's session), i.e. one completed session later than today's page.
