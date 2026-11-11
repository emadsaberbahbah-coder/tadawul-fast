# TFB Commit Sheet - daily_plan 1.0.2 [SETTLE-PREV]: a pick bought after its plan day's last refresh reads EXECUTED, and the next day's Stability line stops counting it as dropped

| Item | Value |
| --- | --- |
| Date | 2026-11-10 (daily improvement routine) |
| Branch | `claude/daily-2026-11-10` |
| Base | origin/main `50f55740` (daily_plan 1.0.1, #953; ci.yml 1.1.77) |
| Lane | GitHub Actions only (`daily_plan.yml`). No Render deploy, no Apps Script paste |
| Gate | Repository Variable `TFB_PLAN_SETTLE_PREV` = on / 1 / true / yes; unset or anything else = off = 1.0.1 byte for byte (ledger, page, log). `daily_plan.yml` maps it with default `'off'`: not armed |
| Priority | (a) a published number on the owner's Daily_Plan page is wrong: the Stability line counts a pick the owner bought as "dropped", and the plan's ledger never records the execution |
| Versions | daily_plan 1.0.2 (no open PR or branch touches `scripts/daily_plan.py`). daily_plan.yml 1.0.1. ci.yml 1.1.85: main holds every number up to 1.1.77 (its 1.1.75 is a line inside its 1.1.76 block); 1.1.78-1.1.84 are claimed by open #938, #915, #946, #922, #923, #909, #926 |

## S1 - Evidence

**The defect.** `run_once` re-statuses only the rows whose Plan Date is today (`today_rows`, then `refresh_status`); every older row is written back as it was (`all_rows = older + today_rows`). A pick is marked EXECUTED only by a run on its own plan day that sees the lot (`pick_status`: buy date on or after the plan date).

**Why US picks miss it.** The schedule is `cron: "40 5,9,13 * * *"`: 08:40, 12:40 and 16:40 Riyadh. US markets open at 17:30 Riyadh since 1 Nov. So every scheduled refresh of a plan day runs before its US session. The only other runs are the `workflow_run` runs after a dispatched sync. Such a run settles the pick only if it starts after the purchase, after the lot is in `_Portfolio_CostBasis`, and before Riyadh midnight. A lot typed in the next morning can never be seen on its plan day.

**The live plan.** The first plan was cut at 15:24 Riyadh on 10 Nov (run 42815393625, "CUT 2026-11-10 from the render of 2026-11-10 15:22 (UNVERIFIED): 3 pick(s), 18 in the pipeline"). Its three picks are all US names: TEN.US, ZTO.US and NMM.US. The refresh at 16:24 Riyadh (run 42823721152) reads TEN.US and NMM.US as CHECK and ZTO.US as INVALIDATED ("earnings in 7 day(s)").

**What 1.0.1 publishes.** An offline replay of that shape (harness S1, real `run_once` on an in-memory workbook):
- NMM.US is cut on 10 Nov and bought in the 10 Nov US session.
- At the next run (11 Nov 08:40) the 10 Nov row still reads OPEN, "price 60.00 is inside the entry zone".
- The page reads "Stability: ... versus the previous plan: 2 kept, 1 new, 1 dropped". The one dropped name is the one the owner bought.
- The row stays OPEN for good: no later run touches a past day.

**Why this number matters.** daily_plan 1.0.0 was built on the owner's 10 Nov review of recommendation stability ("change of recommdadtion one hour there is recommdadtions and after next hour it's removed"). The Stability line is the page's answer to that concern, and this defect inflates it with every US pick he acts on. The `_Daily_Plan` ledger is also the plan's only record of what was executed. Nothing else reads `_Daily_Plan` or `Daily_Plan` (grep over .py, .gs and .yml).

**Dedupe.** No open PR touches `scripts/daily_plan.py` (all open PRs checked, codex/* included). The remote branches `daily-plan-v1` and `daily-plan-1.0.1` are the heads of the merged #951 and #953.

**Picked over** (from today's survey; each candidate checked by an independent refuter):
- daily_plan re-issues a previous pick that went through its own stop as pick 1, labelled "kept". This is real, but rare, and it is a judgement call on the spec sentence "Yesterday's unexecuted picks keep their place". It is the next candidate.
- run_shadow_scorer 1.9.5: with lastscored armed today, an empty Top_10 day can freeze the CHAMPION series. This is real but latent: no empty day is recorded, and S-1 stands at 26/28.
- The Yahoo price fallback prices 0 symbols (HTTP 429). This is real, but the fix is in the provider transport, which cannot be tested offline.
- 28_Plan_Of_Record and US holidays: still conditional on `POR_MODE=on`.
- Intraday Quote Refresh is red. Refuted as a number defect: the refuter traced that the rows it writes are marked preserved and fail the Top_10 quote-freshness gate anyway, so no ticket depends on them.

## S2 - Change

**`scripts/daily_plan.py` 1.0.1 -> 1.0.2.** Dated WHY block prepended; every prior WHY line verbatim. 3 functions added, 0 removed; the only removed line is `__version__`. Every added line is ASCII.
- `settle_prev_on()`: reads the gate.
- `settle_prev(older, prev_date, held, today_picks, today, stamp)`:
  - It looks at each PICK row of the newest previous plan date P whose Status is not EXECUTED or INVALIDATED.
  - The row becomes EXECUTED "bought <date> (settled <today>)", Status At = this run, when its symbol is an active lot bought on or after the first day of the name's unbroken run of PICK days ending at P. That day is usually P itself; a pick carried forward because its lot came after the next cut still settles.
  - Skipped: a name bought today or later that today's plan also picks. Today's own row records that purchase.
  - It writes those three cells and nothing else. There is no WAIT, INVALIDATED or CHECK on a past day, and rows older than P are untouched.
- `stability_counts(older, prev_date, today_rows)`: returns (kept, new, dropped, executed) against every PICK of P. kept + dropped + executed = P's picks.
- `run_once`: when on and a previous plan exists, it settles before `prev_picks` is built. After the cut it puts the counts into `info` and logs one line: "SETTLE-PREV previous plan P: settled SYM (bought D), ... | kept K, new N, dropped D, executed E".
- `page_rows`: when `info` carries the counts, the Stability line reads "... K kept, N new, D dropped, E executed". The original line is built first and left untouched, so the off path is the same code.

**`.github/workflows/daily_plan.yml` 1.0.0 -> 1.0.1.** Maps `TFB_PLAN_SETTLE_PREV: ${{ vars.TFB_PLAN_SETTLE_PREV || 'off' }}`. No trigger, schedule or concurrency change.

**`scripts/verify_deployment.py`.** Pins daily_plan 1.0.2.

**`.github/workflows/ci.yml` 1.1.85.** A new blocking lean-unit step, before the board scorecard step:
- `git fetch --no-tags --depth=1 origin 50f55740...`;
- `python -m pytest -q tests/test_daily_plan_settle_prev.py` with `TFB_TEST_REQUIRE_BASE=1`, so a failed fetch fails the golden legs.

**Rollback.** Unset the Variable or set it to off.

## S3 - Measured

**`tests/test_daily_plan_settle_prev.py`:** PASS 32/32 as a script (three runs, identical), and 51 passed under pytest together with `tests/test_daily_plan.py` (19), with `TFB_TEST_REQUIRE_BASE=1`. Zero network: the real `main()` / `run_once` run on an in-memory workbook that reads back text as Sheets does. The legs:
- **S1:** the 10 Nov shape. When on, the 10 Nov NMM row reads EXECUTED "bought 2026-11-10 (settled 2026-11-11)", the page reads "2 kept, 1 new, 0 dropped, 1 executed", and one SETTLE-PREV line is logged. Today's picks, levels and pipeline equal the off run. A later run settles nothing more.
- **S2 off identity:** 14 scenarios against the untouched main copy, with the gate unset, off, OFF, no, 0, enforce and empty. Ledger, page, return codes and every log line are equal, with no SETTLE-PREV line.
- **S3:** an unbought previous pick keeps its status, reason, stamp and last price even through its stop. A lot bought before the plan, and a sold lot, are not settled.
- **S4:** INVALIDATED is not relabelled. A same-day EXECUTED row is unchanged and counted as executed.
- **S5:** rows older than P are byte-equal, P's pipeline rows included.
- **S6 / S7 / S7b / S8 / S9:** bought today and picked again; a lot entered after today's cut (settled, stays kept), and the next day the carried pick settles too; bought today and not picked; a run after Riyadh midnight.
- **S10:** observe and dry-run settle in memory, print the line and write nothing.
- **S11 / S11b:** the counts add up in every on scenario; the settle writes only the three status cells of a settled PICK row; the counts read only P's rows.
- **S12:** with no previous plan, on equals off.
- **S13:** workflow mapping, manifest pin and the CI step.
- **S14:** two names settle in one run, in ledger order, on the exact log line.
- **S15:** a CHECK pick from an unverified cut (as on 10 Nov) and a WAIT pick settle.
- **S16:** the run of PICK days, tested directly: a day without the pick, a day with the name only in the pipeline, or a plan day with no pick at all ends the run, and the walk goes newest first.

**Golden negative on the untouched main copy** (`TFB_TEST_SETTLE_TARGET` = `git show 50f55740:scripts/daily_plan.py`): PASS 15/32, 17 failed. The 15 that pass are the 14 off-identity scenarios and S12, which describe main. S1 fails on its first assertion: the row reads OPEN where EXECUTED is expected.

**Against the first 1.0.2 commit `4bdf0b12`:** 30/32. S7b and S16 fail; they encode the review finding in S5 below.

**Mutation** (`settle_prev_on`, `settle_prev`, `stability_counts`, the run_once block, the page suffix): 51 of 56 mutants killed. The 5 survivors are equivalent on any ledger the script writes:
- m11: `>=` vs `==` on today; it differs only for a lot with a future buy date.
- m20: `.upper()` on today's picks.
- m36: settle after `prev_picks`; settled names are held, and `decide` already sends held names to the pipeline.
- m37: settle after the cut; same reason.
- m57: a `min()` that cannot change the threshold.

The first review round's 52 mutants left 8 non-equivalent survivors (only OPEN settles, PIPELINE rows settle, Last Price written in two variants, the plan date logged as the buy date, executed read from every date, two log-join variants). Each now has a killing leg.

**Lanes** on an export of the final tree (`cf8f73d1` plus this line): the lean emulation gives LEAN FAILS 0 with all 14 steps exiting 0. The decision-layer suites give 4,548 passed / 36 skipped, and the new step gives 32 passed with the base fetched by SHA. The contract lane gives rc 0 (426 passed, 13 skipped, as on main). compileall and ruff (CI rules) are clean. No added line is non-ASCII.

## S4 - Off identity

- **Structure.** With the gate off, `settle` is False, so `settle_prev`, `stability_counts` and the log line never run, and `info` never carries the counts. The extra work is one `os.environ` read that cannot raise.
- **AST check.** With the five gated statements removed, `run_once` and `page_rows` are AST-identical to 1.0.1, and every other function is unchanged.
- **Fuzz.** A differential fuzz against main through `main()` compared return codes, exceptions, stdout, stderr and every workbook call in order with its arguments. It found no difference with the gate off. It ran on the first 1.0.2 commit; the later changes are inside `settle_prev`, which the off path never calls.
- **Harness.** S2 pins this for the 14 scenarios.

## S5 - Review

**Round 1.** Four independent lenses (semantics, off identity and house rules, mutation, CI and merge integration), each finding re-checked by a refuter.
- **Confirmed and fixed (semantics).** S7's timeline plus one more run:
  - NMM.US is bought on 10 Nov and its lot is typed in on 11 Nov after the 08:40 cut, so the 11 Nov plan keeps it.
  - On 12 Nov the previous plan is 11 Nov and the buy date (10 Nov) is before it, so the first rule (buy date on or after P) skipped it.
  - The page read "1 dropped" for the bought name, the very symptom this change fixes.
  - The threshold is now the first day of the name's unbroken run of PICK days ending at P. Harness legs S7b and S16 were added.
- **Confirmed and fixed (tests).** The 8 surviving mutants above.
- **Confirmed and fixed (house rules).** A one-character whitespace change to an existing line in `knobs()` was reverted, and this commit sheet was added.
- **Confirmed, no code change (known limits, S7):**
  - a re-buy of the same name hides the plan-day lot (the held map keeps the newest buy date);
  - a pick that the next plan drops, bought after its own last run, is outside P;
  - an unreadable Buy Date.
- **Integration.** The ci.yml header block conflicts with each open daily PR (header lines only). Whoever lands later keeps both VERSION blocks, the higher number on top. The step and the manifest pin merge cleanly.

**Round 2 (Codex P2 on #954, `83926e22`).** A plan day with only PIPELINE rows (no pick at all, e.g. no deployable cash) was left out of the run walk, so the walk bridged it:
- NMM.US was picked on 9 Nov, pipeline-only on 10 Nov, picked again on 11 Nov and bought on 10 Nov.
- The 12 Nov run settled the 11 Nov row for a purchase made before that run of picks.
- Reproduced and fixed: every recorded plan date now enters the walk, with an empty pick set when the day has no pick. The S16 leg covers it; it fails on `83926e22` and passes now.
- A day with no ledger rows at all (no plan cut) still does not break a run: the previous plan stood that day.

## S6 - Owner steps (proposed; nothing is armed here)

1. Merge. There is no Render deploy; the plan runs in GitHub Actions.
2. Set the repository Variable `TFB_PLAN_SETTLE_PREV=on`.
3. On the next Daily Plan run, read the "SETTLE-PREV previous plan ..." line in the job log. The Daily_Plan page's Stability line then ends ", N executed".

Rollback: unset the Variable.

## S7 - Known limits and not done

- **Settling.**
  - Only the newest previous plan date is settled. A previous pick that the next plan dropped, and that was bought after its own day's last run, stays OPEN; its day is older than P.
  - A lot that is bought and closed before the next run is not held, so it is not settled. Neither is a lot without a readable Buy Date (the reader's 1970-01-01 sentinel).
  - When the same name is bought again, the held map keeps the newest buy date. A plan-day lot hidden behind a later re-buy reads by the later date.
  - A name bought on a day it was only in the pipeline, then picked because its lot came late, does not settle: its run of PICK days starts after the purchase.
- **Reporting.**
  - Today's row for a name bought before its own plan day (the lot typed in after the cut) reads OPEN on that day, as in 1.0.1. The next day's run settles it.
  - A pick that was INVALIDATED and then bought counts as dropped, because the plan withdrew it.
  - "kept" in the counts means picked again today. This is a superset of the "kept from the previous plan" label (which also needs rank <= TFB_PLAN_KEEP_RANK), as in 1.0.1.
- **Not done.**
  - No Variable set and no gate armed.
  - No change to the cut, the picks, the sizing, the keep rule or `pick_status`.
  - No live workbook read or write, and no provider or broker call.

## Addendum 2026-11-11 - merge of main `58910afa`, re-based on daily_plan 1.2.0

Main `58910afa` (#955-#959) was merged in with a merge commit. Main took daily_plan 1.1.0 (the evidence rule, observe) and 1.2.0 (hourly status on live prices), daily_plan.yml 1.0.1 and 1.1.0 (hourly cron `20 * * * *`), and ci.yml 1.1.78 / 1.1.79. Open #960 claims daily_plan 1.3.0 and daily_plan.yml 1.2.0.

**Re-numbered.**
- daily_plan 1.0.2 -> **1.4.0**: the version constant, the WHY header and the four `[SETTLE-PREV]` tags.
- daily_plan.yml 1.0.1 -> **1.3.0**.
- The verify_deployment pin is 1.4.0, next to main's new exit_plan and consistency_check pins.
- ci.yml keeps 1.1.85; its claim note now lists the claims after today's re-numbering.
- The file name of this sheet keeps its first number.

**Conflicts resolved.**
- The version line; `settle_prev_on` beside main's new `ev_mode` (both kept).
- The workflow header: this block above main's 1.1.0 block, verbatim.
- The workflow env: this mapping after main's four new lines.
- The manifest pin.

Main's daily_plan text and functions are untouched. Against main, `scripts/daily_plan.py` removes only the version line.

**Golden base moved.** The harness and the ci.yml step now read main `58910afa` (daily_plan 1.2.0) instead of `50f55740` (1.0.1).

**The premise, restated.** At 1.0.1 every scheduled refresh ran before the US open, so every US pick was exposed. Since daily_plan.yml 1.1.0 the plan is refreshed every hour at minute 20 (open #960 proposes 07:20-23:20 Riyadh), so the last run on a plan day is 23:20 Riyadh. The defect is now narrower. What still reads 'dropped' the next day:
- a pick whose lot reaches `_Portfolio_CostBasis` after that run (bought in the last 40 minutes of the US session, or entered the next morning);
- a scheduled run that GitHub delivers late or drops;
- the carried pick of S7b.

The WHY block says so. No count of such picks is claimed: the plan is one day old.

**Measured on the merged tree** (with `TFB_TEST_REQUIRE_BASE=1`):
- `tests/test_daily_plan_settle_prev.py` PASS 32/32. All 14 off-identity scenarios equal main's 1.2.0 (ledger, page, return codes, log).
- With main's `tests/test_daily_plan.py` (now 27 cases): 59 passed.
- Golden negative against main's 1.2.0 copy: 15/32, as before.
- compileall and ruff (CI rules) are clean, and no added line is non-ASCII.
