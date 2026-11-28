# TFB Commit Sheet — consistency_check 1.6.0 + markets_board 1.3.0 + exit_plan.yml 1.8.0 [ALIGN] (one list of market states on every page)

Date: 2026-11-28 (Saturday) · Lane: GitHub Actions (🧮 Consistency Check, 🧭 Markets Board, 🎯 Exit Plan) · Gate: no buy, sell, size or level changes · No Render change

## Why
Owner, 28 Nov 2026 at 14:03 Riyadh: he narrowed the buy hold to drawdown and crash_day himself, on the advice of that day's review. He set it in two places:
- `TFB_MARKET_GATE_HOLD` on Render, the main page's market gate;
- the repository variable `TFB_PLAN_REGIME_BLOCK`, the Daily Plan's rule.

He also limited the plan to USD with `TFB_PLAN_FUNDABLE_CCY`. At 14:24, in Arabic, he added that the important thing is that the pages agree and do not contradict each other.

The pages were read the same day under the new settings. Three of them still said otherwise:
- **Markets_Board** stated "the Daily Plan holds a new US buy unless the US is calm" twice. Its verdict read "cash stays cash" on a day the plan could buy in the US (stress).
- **The Exit Plan's sell note**, which Today's Action Plan carries on every holding, read `TFB_EXIT_MARKET_TURN_LABELS` with its own default (weak, stress, drawdown, crash_day). Every US holding in a stressed market therefore said "the market-turn rule would sell" on the day the plan may buy.
- **Nothing compared the two hand-set lists.** One sits on Render and one in the repository, so a later edit to one would split the main page from the plan unseen.

The News & Macro Gate's "NO NEW ENTRY" is not one of these contradictions. Today's Action Plan already marks it observe-only, and before any buy it shows "Read before buying ... the plan does not obey it yet".

## What
**`.github/workflows/exit_plan.yml` 1.8.0:**
- `TFB_EXIT_MARKET_TURN_LABELS` is now `vars.TFB_EXIT_MARKET_TURN_LABELS || vars.TFB_PLAN_REGIME_BLOCK || 'weak,stress,drawdown,crash_day'`.
- The sell note reads the buy hold's states unless the owner gives it a list of its own.
- The script is unchanged, and the note stays observe-only (`TFB_EXIT_MARKET_TURN` observe).

**`scripts/markets_board.py` 1.3.0:**
- **US line:** names the Daily Plan's rule as configured, for example "holds a new US buy on drawdown, crash_day (TFB_PLAN_REGIME_BLOCK): today the US reads stress - not held".
- **LIVE RULE line (new):** gives the hold labels, then which of the five stock markets the main page's gate covers are held today and which are open.
- **Market test:** the VERDICT, the last days' verdicts and the How to read line now name the page's own calm-only test (the market trial fixed on 21 Nov) as the page's test, never as the live rule.
- The views, the trial and the sections below are unchanged.

**`scripts/consistency_check.py` 1.6.0 + `.github/workflows/consistency_check.yml` 1.5.0: C16 gate rules.**
- It reads the main page's market gate from Render's `/health` (`engine_gates.market_gate_hold` and `.market_gate`) through `TFB_BACKEND_URL`, using the standard library with a 20 s limit.
- It compares that with `TFB_PLAN_REGIME_BLOCK` and `TFB_EXIT_MARKET_TURN_LABELS`. The workflow passes both with the exact expressions their own workflows use.
- MATCH: the three lists are the same set of states.
- MISMATCH: each list is named, and the row says which two settings to align.
- INFO: `/health` cannot be read.
- Without a URL (a run outside the workflow), C16 is not run.
- No account figure is involved, so the run log carries the detail.

**Also changed:** `verify_deployment` pins (`consistency_check` 1.6.0, `markets_board` 1.3.0).

## The state it reads (28 Nov, after the owner's change)
- **Render `/health`:** `market_gate` enforce, `market_gate_hold` crash_day,drawdown (commit b3a44f2).
- **Daily Plan dry run 46139321012:** `TFB_PLAN_REGIME_BLOCK` drawdown,crash_day; `TFB_PLAN_FUNDABLE_CCY` USD.
- **Expected C16 after the merge:** MATCH, "every page holds a new buy on the same states: crash_day, drawdown".
- **Markets that day:**

  | Market | Label | Live rule |
  |---|---|---|
  | US | stress | open |
  | Europe | calm | open |
  | Saudi Arabia | drawdown | held |
  | Japan | drawdown | held |
  | China / Hong Kong | drawdown | held |

## Tests
- `tests/test_consistency_check.py`: 16 pass, including two new K16 tests:
  - set parsing, and each rule's default with the sell note following the plan;
  - MATCH;
  - MISMATCH when the main page or the sell note differs;
  - the nested `engine_gates`;
  - INFO;
  - a read that never raises (http refused, 503, no block, an exception);
  - the run adds the row and its detail reaches the log, with no C16 and no network read without a URL;
  - the workflow expressions.
- `tests/test_markets_board.py`: 15 pass.
  - The texts and positions now say what the code says.
  - The new M1b reads the 28 Nov settings: the US in stress is not held; Saudi, Japan and China in drawdown are held. It also covers observe and off, and checks that the code holds no stale claim.
- `tests/test_exit_plan.py`: 52 pass. X19 reads the new fallback.
- `test_daily_plan`, `test_forecast_record`, `test_bq_views`, `test_today_action_plan` and `test_page_chains` pass, as do the manifest and workflow-flag suites.
- Workflow audit: errors 0, warnings 0.
- Fourteen hand mutants of C16 and the board's live-rule code were run, and every one fails a test.

## Owner
Merge when ready. Nothing to set: the sell note follows `TFB_PLAN_REGIME_BLOCK` by itself.

After the merge, Claude reads the next Consistency Check (C16 should read MATCH), the Markets_Board's US and LIVE RULE lines, and an Exit Plan holding in a stressed market (no sell note).

Rollback: set the repository variable `TFB_EXIT_MARKET_TURN_LABELS=weak,stress,drawdown,crash_day` for the old sell note. Reverting the PR restores the board's old texts.
