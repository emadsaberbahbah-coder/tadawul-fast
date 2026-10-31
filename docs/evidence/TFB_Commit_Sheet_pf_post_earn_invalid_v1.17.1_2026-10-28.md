# Commit Sheet - Portfolio_Decision post-earnings guard: a malformed entry is unknown evidence (portfolio_actions 1.17.1) - 2026-10-28

**Scope:** owner readiness review, 28 Oct 14:47 Riyadh, priority 3 ("close the earnings-input defect"). Backend only: `core/analysis/portfolio_actions.py` 1.17.0 -> 1.17.1. The guard stays default off (`TFB_PF_ADD_POST_EARN` unset on Render) and off is unchanged. No sizing rule, funding pass, confirmation clock, gate threshold, Apps Script file or other page is touched. The route (`routes/advanced_analysis.py` 4.19.0) already forwards `body.post_earnings` as a mapping, unchanged; it needs no edit.

**Files:**
| File | Version | Change |
|---|---|---|
| `core/analysis/portfolio_actions.py` | 1.17.0 -> 1.17.1 | malformed entries kept as unknown evidence; unknown EPS surprise takes the longer window; optional `uncovered` list; two meta counters; WHY block above the constant |
| `scripts/verify_deployment.py` | pin | portfolio_actions 1.17.1 (manifest pin only) |
| `.github/workflows/ci.yml` | 1.1.54 -> 1.1.55 | the isolated protective step also fetches the merged v1.17.0 module (`0c3d3e21`) by SHA for the new identity cases |
| `tests/test_pf_add_guards_d1.py` | 36 -> 107 cases | the v1.17.1 matrix (isolated protective lane; numpy + pytest only) |
| `tests/test_pf_add_guards_d1_route.py` | 1 -> 2 cases | one complete input-to-action case: real route + real builder (contract lane; FastAPI) |

## S1 - Evidence (the reproduction)
- Owner's reviewer, against v1.17.0 (merged #869, `0c3d3e21`): `{"FRST.US": {"date": "2026-10-22", "sessions": 4, "surprise": -22.2}}` holds FRST's ADD under `TFB_PF_ADD_POST_EARN=enforce`; the same entry with `"sessions": "4"` is dropped by `_post_earn_inputs` ("Malformed entries are dropped"), FRST is then absent from the map, `_post_earn_eval` reads "no report", and the ADD passes.
- Reproduced here: `test_reproduction_string_sessions_holds_under_enforce` and the route case `test_full_path_string_sessions_holds_and_absent_symbol_adds` both fail on `0c3d3e21` (the ADD stays ADD).
- The same drop applied to a bad date, a negative / fractional / boolean / missing session count and a non-object entry. A blank EPS surprise was read as a beat (5-session window) although it cannot rule out a miss.

## S2 - What (only while the guard is observe or enforce)
- **Malformed entry -> unknown, never dropped, never coerced.** A held symbol whose entry has a bad structure, date or session count goes to `pe["unknown"]` with the rejected field. Rules: the entry is an object; `date` is a `YYYY-MM-DD` string (ASCII digits) not after `asof`; `sessions` is an `int` (not bool) `>= 0` and not above the calendar days between the report date and `asof`. `"4"`, `True`, `4.0`, `4.5`, `None`, NaN, inf, `-1`, `""`, `[4]` and a missing key all fail. A duplicate symbol (after trim/upper-case), or a symbol also listed as uncovered, is unknown too. An entry whose key names no symbol makes the whole input `invalid` (it cannot be attributed, so it cannot clear any ADD).
  - enforce: `ADD held [POST-EARN]: earnings entry for FRST.US invalid (sessions '4') - the post-earnings window cannot be verified (was ADD: ...)`, proceeds 0, `capped_from=ADD`.
  - observe: ` | [postearn-observe] earnings entry for FRST.US invalid (sessions '4') - would HOLD under enforce`; actions, sizing and KPIs as off.
- **Unknown EPS surprise -> the longer window.** A blank, missing or malformed surprise cannot rule out a miss, so the limit is the longer of `TFB_PF_POST_EARN_SESSIONS` (5) and `TFB_PF_POST_EARN_SESSIONS_NEG` (10). The trigger reads `(EPS unknown, the longer window applies)` or `(EPS invalid (surprise 'n/a'), the longer window applies)`. A known surprise keeps the v1.17.0 windows; a malformed surprise beyond every window still passes (it cannot decide the outcome). Report day (0 sessions) holds as before.
- **Incomplete calendar coverage (optional field).** `uncovered`: a list of held symbols that have no `Calendar_Events` row. Absent -> exactly v1.17.0. A valid list (at most 200 non-blank strings) -> a listed ADD is unknown: `ADD held [POST-EARN]: no earnings calendar row for FRST.US - the post-earnings window cannot be verified (was ADD: ...)`. Anything else (a string, null, a dict, a non-string or blank item, more than 200) -> input `invalid`, which holds every ADD under enforce like the other unverifiable inputs. The live 10_My_Portfolio 1.5.17 does not send the field; the sender is a later GAS change (10_My_Portfolio 1.5.18), not this PR.
  - **KSA consequence (by design):** the EODHD calendar does not cover KSA names, so once the GAS sends `uncovered`, an ADD on a KSA holding holds under enforce until a KSA calendar source exists. Today (field absent) a KSA ADD passes, as in v1.17.0.
- **Read-back:** `meta.post_earn_guard` gains `invalid_entries` (entries with any rejected field, a malformed surprise included) and `uncovered` (count; `null` when the field was not sent). Alerts reuse `add_post_earn_hold` / `add_post_earn_observe` (texts unchanged).
- **Preserved:** a held symbol absent from the map (clean calendar, no recent report) passes; statuses absent / unavailable / stale / invalid hold under enforce; the sukuk exemption (D-9); a post-earnings hold does not use a ticket-cap slot; off reads nothing (`_post_earn_inputs` is not called).
- **Ticket-cap limitations (unchanged, restated):** open tickets are held non-sukuk positions with quantity > 0 in this run; a working entry order that has not filled is not counted (the evidence packet carries positions only); the Top-10 board does not apply the cap.

## S3 - Tests
| Test | Proves |
|---|---|
| `test_malformed_entry_is_unknown_never_dropped` (25 cases) | every invalid sessions / date / structure value becomes unknown with an ASCII reason naming the field; the other symbol's valid entry is kept |
| `test_well_formed_entries_keep_their_values` (10) | valid entries keep their values; blank or missing surprise = unknown; a malformed surprise keeps the entry with a note; report day; an old but consistent report |
| `test_unattributed_or_duplicate_entries` | a key that names no symbol -> input invalid; a duplicate symbol -> unknown |
| `test_uncovered_list_parsing`, `test_malformed_uncovered_makes_the_input_invalid` (8), `test_symbol_both_reported_and_uncovered_is_unknown` | the optional field: absent = None, valid list = set, malformed = invalid, contradiction = unknown |
| `test_reproduction_string_sessions_holds_under_enforce` | the reported defect: numeric 4 and string "4" both HOLD under enforce; the absent symbol stays ADD; observe tags only; off identical with or without the field; meta counters |
| `test_unknown_surprise_takes_the_longer_window` (7) | unknown / malformed surprise holds inside 10 sessions, passes after; a known beat at 6 sessions passes |
| `test_uncovered_holding_holds_under_enforce_only` | listed ADD holds (enforce) / tags (observe); an empty list changes nothing; a malformed list holds every ADD |
| `test_sukuk_exempt_even_when_its_entry_is_invalid`, `test_invalid_entry_hold_frees_the_ticket_cap_slot` | D-9 exemption and the ticket-cap interplay unchanged |
| `test_off_is_byte_identical_to_v1170_base` | guard off (ticket cap off and enforce) = the merged v1.17.0 module, malformed inputs included (versions masked) |
| `test_valid_inputs_decide_exactly_as_v1170` (9) | under observe and enforce, valid integer sessions with a known surprise, report day, empty map and the non-ok statuses decide byte-identically to v1.17.0 (only the two new meta counters differ, asserted 0 / null) |
| `test_off_is_byte_identical_to_v1161_base` (kept) | off = v1.16.1 at `22658e0f`; the version mask now reads the live constant (stacked pin) |
| Existing cases moved on purpose | `test_inputs_validation` (malformed entries now in `unknown`; a float 6.0 is not coerced), `test_window_eval` and `test_report_day_is_inside_every_window` (a blank surprise now takes the 10-session window), `test_post_earn_observe_tags_without_changing_anything` (+2 meta keys). Every other assertion is unchanged. |
| `tests/test_pf_add_guards_d1_route.py::test_full_path_string_sessions_holds_and_absent_symbol_adds` | real route + real builder + certified synthetic evidence: off -> ADD; enforce + `"sessions": "4"` -> HOLD naming the field; enforce + numeric 4 -> HOLD `4/10 sessions`; FRST absent -> ADD; observe -> tag only |

**Golden negative** (both new files run against the `0c3d3e21` module): 71 of 109 fail. 62 are behaviour cases (61 builder cases + the full-path route case), including the reproduction itself; the other 9 are the v1.17.0-identity cases, which only make sense against 1.17.1. The 38 that pass on the base are the off-identity, unchanged-behaviour and ticket-cap cases.

**Local runs:** both guard files (109 passed, venv with FastAPI; the protective file also in a numpy + pytest + PyYAML venv), the version-pin and portfolio route tests, `scripts/render_env_contract.py --check` (current; no env added), and the whole suite: 6339 passed, 48 skipped, 10 failed - the 10 known environment-only failures (`test_framework_security_20261024` x9, `test_security_pins_libsmoke_20261013` x1).

## S4 - Owner steps
Installation order (unchanged from the D1b commit sheet): GAS first, backend second, arming last.
1. GAS `10_My_Portfolio.gs` 1.5.17 + Script Property `PD_POST_EARN=on` - **done** (owner, 28 Oct).
2. Merge this PR.
3. Render -> Manual Deploy -> latest main, **outside a sync**: no `daily_sync` run in progress or queued in GitHub Actions (the scheduled slots are 07:17, 15:17 and 23:17 Riyadh, and GitHub can start them hours late). One deploy also brings #865/#867/#869 live if they are not yet. No environment change in this step.
4. Read-back: `/health` -> `pf_gates.portfolio_actions_version` = `1.17.1`; the next Portfolio_Decision status line reads `actions v1.17.1`. With the guard still off, the page is unchanged.
5. Later, in its own evidence run (one ENV arming per evidence run): `TFB_PF_ADD_POST_EARN=enforce` (and `TFB_PF_MAX_OPEN_TICKETS_MODE=enforce` per the D1b/D1c sheet) -> Save, rebuild, and deploy outside a sync. Expected: the status line ends ` | post-earn enforce (ok; held 1)`, FRST's ADD reads `ADD held [POST-EARN]: reported 2026-10-22, k/10 sessions since the report (EPS -22.2%)` until the tenth US session after the report (5 Nov), and `meta.post_earn_guard.invalid_entries` = 0.

## S5 - Rollback
Leave or set `TFB_PF_ADD_POST_EARN` unset/off (off is the v1.17.0 output, byte for byte, apart from the version string), or revert the PR. The GAS side needs no rollback (it never sends `uncovered` today).

## Owner table
| Defect | Root cause | Change | Tests | Deployed/installed version | Effective mode | Live acceptance | Remaining dependency |
|---|---|---|---|---|---|---|---|
| D1b input boundary: a present but malformed post-earnings entry (`"sessions": "4"`) was dropped, so the held name read as report-free and its ADD passed under enforce | `_post_earn_inputs` (v1.17.0) discarded malformed entries and read a blank EPS surprise as a beat | portfolio_actions 1.17.1: malformed entry = unknown evidence (HOLD under enforce, tag under observe, never coerced); unknown surprise takes the longer window; optional `uncovered` list; meta `invalid_entries` / `uncovered` | 107 protective + 2 route cases; 62 behaviour cases fail on `0c3d3e21`; off and valid inputs byte-identical to 1.17.0; full suite green apart from the 10 known env-only failures | Repo: this PR (not merged). Render: 1.16.1 (`18ab6e38`, 1.17.0/1.17.1 not deployed). GAS: 10_My_Portfolio 1.5.17 installed, `PD_POST_EARN=on` (owner) | off (`TFB_PF_ADD_POST_EARN` unset) | Not yet: needs merge + deploy, then arming in a later evidence run (expected FRST ADD -> HOLD [POST-EARN]) | Owner merge -> Render deploy outside a sync -> arm `enforce`; GAS sender for `uncovered` (10_My_Portfolio 1.5.18) |

Not a licensed financial adviser; the owner decides and executes every trade.
