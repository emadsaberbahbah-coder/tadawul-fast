# TFB Commit Sheet - universe maintenance v1.0.0 [Global_Markets: weekly US-first additions, red-row watch, confirmed-delisting retirement]

Date: 2026-10-20 (Tuesday, Riyadh). Base: `origin/main` aa1f440 (#802).
Delivered: `scripts/universe_maintenance.py` 1.0.0 (new), `.github/workflows/universe_maintenance.yml` 1.0.0 (new),
`config/universe_exclusions.json` 1.0.0 (new), `tests/test_universe_maintenance.py` (63 cases, new),
`ci.yml` 1.1.26 -> 1.1.27 (enrols the suite in the blocking lean lane), this sheet.
Not edited: `run_dashboard_sync.py` (its v6.64.0 retirement seam is used as is), `daily_sync.yml`, every gate, floor,
threshold, ticket, trigger and `_Lists_Config`. No Render or Apps Script change.

## Why (owner request, 20 Oct)

Global_Markets is maintained by hand: the 18 Oct Top-200 additions and the 196-row retirement list were both pasted
by hand. Dead tickers stay on the page, fail every provider leg and pull the coverage gates down; important new
companies arrive only when someone remembers. The owner asked for automatic additions with the US market first, and
for unread rows to be coloured red (27_Decision_Pages_v2 v1.2.24, PR #803) and removed once confirmed dead.

## What

One job, three parts, default **off** (`TFB_UNIVERSE_MAINT` unset = the workflow stops in its first step and reads
nothing).

1. **Watch (every run).** A Global_Markets row is red when the v1.2.24 sheet rule says so: Warnings carries
   `acquisition_status:unavailable|preserved`, `identity_quarantined` or `quote_current_price_missing`, or
   `Last Updated (UTC)` is blank or more than 2 days old. `_Universe_Watch` keeps one streak per red symbol: +1 per
   weekday seen red; the first good read drops it (the count restarts, like the colour clears).
2. **Retire (every run).** A `.US` row red for 10+ trading days that is **absent from EODHD's active US exchange
   list** is appended to `_Retired_Symbols` (Page = Global_Markets, Reason prefixed `universe_maintenance v1.0.0`).
   The sync's existing seam removes it when `TFB_SYNC_RETIRE_TAB=enforce`. Never retired: a My_Portfolio holding
   (also as a bare ticker), and nothing at all when My_Portfolio cannot be found; a symbol still on the active
   list (a data problem, reported); a non-US line (no confirmable list in v1.0.0, reported); more than 25 per run;
   anything that would take the page below 6,512 rows (the publication floor) once the rows `_Retired_Symbols`
   already lists for the page are counted as gone.
3. **Add (once a week, from Sunday Riyadh).** The EODHD screener's top 1,000 US names by market cap, missing from
   the page, appended to column A: at most 25 per week and never above 6,750 rows on the page now (the
   name-identity roster bound; a planned retirement frees no seat until the sync removes it).
   Not added, each with its reason in the report: already on the page (also bare) or retired; the same company
   under another line, ADR or share class (normalised name against all rows); not a Common Stock on NYSE / Nasdaq /
   NYSE American / Arca / Cboe BZX (no OTC); a foreign issuer unless `TFB_UNIVERSE_ADD_FOREIGN=1`;
   Israeli-domiciled or Israeli-founded (owner policy 28 Jul: ISIN IL, EODHD country or address Israel, or
   `config/universe_exclusions.json`, 42 codes; entries marked *conservative* can be removed by the owner).

**observe** writes only `_Universe_Watch`, `_Universe_Report` and one `_Run_Log` row (`[UNIVERSE-MAINT v1.0.0]`,
status OBSERVE). **enforce** also writes Global_Markets column A and `_Retired_Symbols`, and degrades to observe if
the pure self-test fails. A manual run can choose observe or off; enforce comes only from the Variable.

Safety: the run is skipped while a scheduled or dispatched Daily Sync is queued or running (fail-closed if the
Actions API cannot be read), and enforce repeats the check right before its writes (deferred to the next run if a
sync appeared or its state is unknown). The weekly marker is written before the page append, so additions stay at
most once per week even if a later write fails (a switch from observe to enforce runs them on the next run). Every
read precedes the first write: an unreadable tab stops the run with nothing written. A Global_Markets read below the
floor writes nothing. Tabs are overwritten from A1 and padded, never cleared first. The EODHD key never reaches a log
line. Budget per week: about 10 screener calls and at most 120
fundamentals calls; the US list once per run.

## 20 Oct export, offline dry run of the watch

99 Global_Markets rows are red today (91 `identity_quarantined`, 8 `quote_current_price_missing`); 76 are `.US`.
Most are live companies (WHR, WDC, TWLO, RUN, SNDK, WRBY) quarantined by the identity guard. They stay on the active
list, so after 10 days they appear under "red 10+ trading days, kept - a data problem", not under retirements. The
job removes only confirmed delistings. The identity backlog remains a separate item.

## Tests

| Battery | Result |
| --- | --- |
| `tests/test_universe_maintenance.py` (U1-U13: modes, red rule = the GAS regex, streaks, retirement rules, policy, addition filters, cadence, observe/enforce end-to-end, fail-safes, key masking, workflow contract, review round 1) | 63 passed |
| `test_runner_pin_ubuntu2404`, `test_workflow_flag_mappings`, `test_ci_required_verdict`, `test_security_pins_20261013` | 40 passed, 4 skipped |
| `test_workflow_activation_observe`, `test_approved_universe_workflow`, `test_release_decision_feed` | 140 passed, 10 skipped |
| `test_flag_registry`, `test_repository_workflow_audit`, `test_verify_optional_rollout_flags`, `test_render_env_contract` | 28 passed, 1 skipped |
| `scripts/audit_repository_workflows.py --fail-on-warnings` | errors 0; the 2 pre-existing `daily_sync.yml` warnings only |
| ruff E9/F63/F7/F82/F401/F841, YAML load, py_compile | clean |

## Arming (owner, GitHub repository Variables, after merge)

1. `TFB_UNIVERSE_MAINT=observe`. Read `_Universe_Report` daily for one week: WOULD ADD (names and ranks), WOULD
   RETIRE, the kept list and NOT ADDED with reasons.
2. `TFB_UNIVERSE_MAINT=enforce`. Additions run on the next run, then weekly.
3. For retirements to leave the page: `TFB_SYNC_RETIRE_TAB=enforce` (the sync's existing Variable; it also applies
   every other line already on `_Retired_Symbols`).

Optional Variables (empty = default): `TFB_UNIVERSE_ADD_MAX` (25), `TFB_UNIVERSE_ADD_WEEKDAY` (SUN),
`TFB_UNIVERSE_ADD_FOREIGN` (0), `TFB_UNIVERSE_RED_DAYS` (10), `TFB_UNIVERSE_RETIRE_MAX` (25). The 6,512 floor and the
6,750 cap are not Variables.

Rollback: `TFB_UNIVERSE_MAINT=off`. An added row is an ordinary row (delete it or list it on `_Retired_Symbols`); a
retired row returns by deleting its `_Retired_Symbols` line and pasting the symbol back into column A.

## Review round 1 (Codex, 6c0f8dd)

| Finding | Disposition |
| --- | --- |
| P1 share the `tadawul-production-write-<ref>` concurrency group | Declined: GitHub keeps one pending run per group and a newly queued member cancels it, so this job could evict a queued Daily Sync (recorded in `track_performance.yml`, 2026-10-06). The race is narrowed instead: enforce re-checks the Actions API right before its writes and defers them when a sync is queued or running, or when the state is unknown. |
| P1 fail closed when My_Portfolio cannot be opened | Fixed: only "worksheet not found" reads as absent; any other error propagates before the first write. A missing My_Portfolio retires nothing. |
| P1 additions idempotent across report failures | Fixed: the weekly marker is written before the page append (at most once per week). |
| P1 existing retirements in the floor calculation | Fixed: rows already listed for Global_Markets (or for every page) count against the 6,512 floor first. |
| P2 pending retirements are not free seats | Fixed: the 6,750 cap counts the rows on the page now. |

## Known limits

- Retirement is US-only in v1.0.0. Non-US red rows are reported for a manual check.
- The duplicate check is by normalised company name; two different companies with the same name (First Bancorp NC
  and First BanCorp PR) keep the second out, and it is listed under NOT ADDED.
- `_Retired_Symbols` gains a second writer (this job, append-only). The sync still never writes it.
- Foreign ADRs are not added by default (`TFB_UNIVERSE_ADD_FOREIGN=1` allows them; the name check is the only
  duplicate guard against their home lines).
