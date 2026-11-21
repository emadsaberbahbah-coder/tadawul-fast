# Commit sheet - the ready list, the switch check and their scorecard (21 Nov 2026)

Branch `feat/ready-list-and-switch` (#1046), carried to main by `fix/carry-1046-to-main` (#1048): #1046 was merged into the #1045 branch four minutes after #1045 had gone to main, so it never reached main. #1048 adds the two Codex P2 fixes of its review (marked below). Observe only: shown and logged, it controls no buy.

| File | Version |
|---|---|
| `scripts/apply_bq_views.py` | 1.18.0 -> **1.19.0** (2 views) |
| `sql/bigquery/103_v_ready_list.sql` | new |
| `sql/bigquery/104_v_ready_trial.sql` | new |
| `scripts/markets_board.py` | 1.0.0 -> **1.1.0** (sections 4-6; reads `Final_Decisions`) |
| `.github/workflows/markets_board.yml` | 1.0.0 -> **1.1.0** (the Daily Plan's regime defaults, for the live-rule column) |
| `scripts/verify_deployment.py` | pins: apply_bq_views 1.19.0, markets_board 1.1.0 |
| `tests/test_markets_board.py` | 11 tests (M5-M8 extended) |
| `tests/test_bq_views.py` | V45 (file order), V46-V47, the two PENDING pins, the order |

No new test file, so `ci.yml` is unchanged.

## Why

The owner wrote on 21 Nov 2026 at 11:03 Riyadh (the build list after the voice call):

> "3. A ready list: each day the 2-3 best qualified names from the top-ranked markets, each labelled with what it is waiting for (cash / calmer market / switch). Every page shows the same final decision; only the Today Action Plan gives orders.
> 4. A switch check: propose a swap only when a qualified name's expected return beats a holding's over the same period, after costs on both sides."

and, for both: "Everything starts in observe mode: shown and logged, but it controls no buys."

## What is built

**`v_ready_list`** has one row per archived day and qualified name.
- Qualified means: the day's last copy of the name (captured last, whichever page it is on) is INVESTABLE, final action INVEST, and meets the Top 10 panel floors (reliability 70+, data quality 80+, no block reason). These are `v_universe_latest`'s floors, read the same way: the last copy first, then the floors. A name whose last copy is blocked is out, even if an earlier copy that day qualified (Codex P2 on #1046).
- Not a holding: a name held that day (My_Portfolio's archived copy, a position above 0) is out. The ready list is for new money, like the Top 10's gates and `v_hold_vs_switch`'s candidates. Section 5 compares each holding.
- The market a name tracks is read from the name, not its currency. First match wins:
  - the market funds by name: KSA, VGK, EWJ, MCHI, GLD, IEF, SPSK and VNQ (each to its market);
  - gold futures to gold, crude futures to oil;
  - any other fund or contract: not read;
  - a US listing: US REITs when its industry is a REIT, else the US. An ADR counts as US, because the archive does not know the company's home market;
  - Tadawul REITs (4330-4350, the same codes as the Saudi REIT index) to Saudi REITs, any other Tadawul name to Saudi Arabia;
  - Hong Kong, Shanghai and Shenzhen to China / Hong Kong; Tokyo to Japan; a European listing to Europe (Turkey excepted);
  - anything else is "not read" (Korea, Thailand, Canada, Mexico, Australia, India, Qatar ...).
- The market's state is `v_market_rank` on that day. A weekend or a morning before the archive uses the last day read. The states are open, hold or not read.
- The 6-day limit runs from the market's own close to the day, for the market and for the US label beside it (Codex P2 on #1048). `v_market_rank` already carries a close up to 6 days, so measuring from the ranking day could let a close 12 days old through when the archive lags. On the archive of 21 Nov no reading was older than 6 days (242 day-market pairs checked; the oldest was 3 days), so no row changes today.
- The day's ready order:
  - open markets first, then held ones, then names whose market is not read;
  - two names per market before any third;
  - then the market's rank, the name's 1-month expected return and its overall score.
  - The top 3 are the ready list.
- Waiting for (in the view):
  - "calmer market" when its market is on hold;
  - "cash or a switch" when its market is open;
  - "market not read".
- Beside it is the live US rule, with the Daily Plan's workflow defaults: a USD name is held unless the US is calm.

**`v_ready_trial`** is the scorecard on the names themselves.
- The outcome is `v_forward_universe`: the price change one week (1W) and two weeks (2W) after the day, and the same change less the day's median name.
- It counts each name-day once in each of five groupings:
  - all qualified;
  - by its market's state;
  - on or off the ready list;
  - held or not by the market rule;
  - held or not by the live US rule.
- It reads the names once (`UNNEST`), and the worst 5% is the exact 5th percentile (`PERCENTILE_CONT`).
- The period is `forward` from Monday 23 Nov 2026, the same day as the market trial.

**The switch check** is `v_hold_vs_switch` (views 1.9.0, already installed). It compares each holding with the best qualified name of its market, over the same span (1W, 2W, 1M, 3M):
- The forecast gap is converted into what such a gap has been worth.
- Both trades are charged: 2 x (commission + spread).
- SWITCH CANDIDATE needs a proven signal at that span, a positive net and a 60%+ chance of ending ahead.

**The Markets Board 1.1.0** adds three sections.
- **4. The ready list.** Every qualified name in the ready order: its market and state, whether each rule would hold it, and what it is waiting for. The board also copies the Today Action Plan's own decision from `Final_Decisions`, so the page never states another decision. It reads that tab (A1:C400) and nothing else; it writes only `Markets_Board`.
- **What it is waiting for never contradicts that decision** (Codex P2 on #1046 and #1048). The decision is read first:
  - "cash" only when the plan's own words report a shortfall ("not funded");
  - a funded BUY waits for nothing, whatever its market's state and even when the switch check names it;
  - any other WATCH or DO NOT INVEST keeps the plan's own reason;
  - a held name reads "nothing new: it is held";
  - the market rule and the switch check, both observe only, follow the decision in brackets;
  - only a name not on `Final_Decisions` waits for them: a calmer market, a reading of its market, a switch, or a pick of today's plan.
- **5. The switch check.**
- **6. The scorecard**, with one sentence per period. The sentence first puts the ready list against the other qualified names, then each rule's held names against the ones it let through.

## Evidence (read 21 Nov 2026)

**Today's ready list.** 22 names qualify on 21 Nov (one held name is out).
- Every market is on hold, so all three ready names wait for a calmer market. They are one Tadawul name and two US REITs.
- Five qualified names are in markets that are not read yet.

**The switch check.** All holdings x 4 spans read KEEP. No forecast is proven to rank names over any span ("thin" or "no edge"), so no gap counts, however large.

**The first read of the scorecard** covers names listed 31 Oct - 14 Nov. That is 15 days for 1W and 8 days for 2W, all in one falling stretch. Windows overlap, so read the means.

| Group (backtest) | 1W: name-days / mean / vs median / rose | 2W: name-days / mean / vs median / rose |
|---|---|---|
| All qualified | 1,148 / -0.44% / -0.30 / 50.2% | 729 / -1.11% / +0.14 / 39.5% |
| Live US rule holds it | 720 / **-1.11%** / -1.00 / 43.9% | 446 / **-2.66%** / -1.38 / 25.6% |
| Live US rule does not | 428 / **+0.69%** / +0.86 / 60.7% | 283 / **+1.35%** / +2.53 / 61.5% |
| Market rule holds it | 768 / **-0.84%** / -0.73 / 45.8% | 479 / **-1.86%** / -0.58 / 32.4% |
| Market rule does not | 380 / **+0.38%** / +0.56 / 58.9% | 250 / **+0.33%** / +1.52 / 53.2% |
| Market open | 253 / +0.45% / +0.67 / 60.5% | 156 / -0.31% / +0.98 / 47.4% |
| Market not read | 127 / +0.23% / +0.34 / 55.9% | 94 / +1.39% / +2.43 / 62.8% |
| Ready list (top 3) | 43 / **-1.73%** / -0.97 / 23.3% | 22 / -1.27% / -0.04 / 36.4% |
| Other qualified | 1,105 / -0.39% / -0.28 / 51.2% | 707 / -1.10% / +0.14 / 39.6% |

Worst 5% over 1W: the live US rule's held names -10.95%, the ones it let through -6.73%; the market rule's -10.82% and -6.51%.

The last-copy fix changed no figure on this archive; leaving holdings out changed the table above (the first draft counted them).

**What the first read says, plainly.**
- Both holds kept out the weaker names on these 15 days.
- The live US rule separated them more than the market rule: 1.80 pp against 1.22 pp over 1W, and 4.01 pp against 2.19 pp over 2W. So the market-by-market rule has **not** shown it is better than the rule that is live.
- The ready list's names did **worse** than the other qualified names: -1.73% against -0.39% over 1W, and only 23% rose. This matches the forecast baselines (IC -0.04; 1-month targets +2.99% against +0.66% realised). Ordering by expected return does not yet pick the stronger names.
- Fifteen days in one falling market is a first look, not a test. Nothing is switched on. The forward test from 23 Nov and the owner's review after 10 sessions decide.

**Speed.** `v_ready_trial` with every view under it read in under two minutes on 21 Nov (486 MB), inside the board's 300-second wait. The board's one-day read of the ready list is fast. The market scorecard (#1045) reads in under a minute.

## The forward test

- It runs from Monday 23 Nov 2026 to Mon 7 Dec 2026: 10 US sessions, the same window as the market trial.
- The review is scheduled for 8 Dec with the owner.
- Only what proved itself is switched on, and only with his approval.
- The rules are frozen for the window: the floors, the market mapping, the order and the rules.

## Safety

- Observe only. The live system never reads BigQuery. No score, pick, gate, order or variable changes.
- The board reads `Final_Decisions` and writes only `Markets_Board`. It copies the Today Action Plan's decision; it states none of its own.
- The run log has counts only: no symbol, amount or quantity (test M5 checks every fixture symbol is absent).
- The broker stays read-only.
- Rollback: `TFB_MARKETS_BOARD=off`.

## Tests

- `tests/test_markets_board.py`: 11 tests.
  - The new sections, and the reading with the real first-read figures.
  - A failed `Final_Decisions` read.
  - The plan env in `markets_board.yml` equals `daily_plan.yml`'s.
  - A tie reads "no difference", and a missing mean makes no claim.
  - The waiting words for every decision kind: cash only on "not funded"; the plan's own reason otherwise; a BUY in a held market. Seven mutants of `waiting_text` were run; all were killed.
  - #1048: the decision before the switch check and the market's reading - a funded BUY the switch check names, a BUY in a market not read, a DO NOT INVEST the switch check names, an unfunded name in a held market, and an unpicked name in a held market with a switch.
- `tests/test_bq_views.py`:
  - V46 checks `v_ready_list`: the 6 days measured from each market's own close (#1048), the last copy before the floors, the floors against `v_universe_latest`, holdings out (the same test as `v_hold_vs_switch`), every market fund of `v_market_condition` mapped to its market, the same REIT codes, equality joins only, the ready order, and the live rule against `daily_plan.yml`'s defaults.
  - V47 checks `v_ready_trial`: the outcome columns of `v_forward_universe`, the five groupings, one pass, the exact percentile, the forward date shared with the market trial and the board, and every group the board's sentence names.
  - V45 checks the file order through 104. The two PENDING pins are set.
- The CI lean lane ran locally (counts in the PR).

## After the merge

1. #1045 is on main and its views are installed (21 Nov, 11:34 UTC, 70/70 match). Merge #1048.
2. Run `bq_views.yml` mode `write` on main, then `verify`. Move the PENDING pins of the installed views to INSTALLED.
3. The board then shows sections 4-6 on its next run. There is no Variable to set.

## Known limits

- **Names in markets that are not read** show as "not read": Korea, Thailand, Canada, Mexico, Australia, India, Qatar. Each needs a reading before its names can be held or opened by the market rule.
- **The outcome only covers some pages.** `v_forward_universe` scores the names of Global_Markets and Market_Leaders, and it starts on 31 Oct 2026.
- **ADRs are read as US names.**
- **Some markets are read through a fund**, which can move apart from its market (currency, make-up).

Not investment advice; the owner decides and executes every trade.
