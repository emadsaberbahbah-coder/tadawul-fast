# TFB Commit Sheet - exit_plan 1.2.0 [LOT-CARRY]: a holding whose lots change keeps its stop ratchet and its once-only TAKE PROFIT

| Item | Value |
| --- | --- |
| Date | 2026-11-11 (daily improvement routine) |
| Branch | `claude/daily-2026-11-11` |
| Base | origin/main `53f2abdc` (exit_plan 1.1.0, #956 / #957 / #959; exit_plan.yml 1.2.0; ci.yml 1.1.79) |
| Lane | GitHub Actions only (`exit_plan.yml`). No Render deploy, no Apps Script paste |
| Gate | Repository Variable `TFB_EXIT_LOT_CARRY` = on / 1 / true / yes; unset or anything else = off = 1.1.0 byte for byte (ledger, page, e-mail, log). `exit_plan.yml` maps it with default `'off'`: not armed |
| Priority | (a) a published recommendation is wrong: after a partial sale the Exit_Plan page and the 07:30 e-mail repeat TAKE PROFIT "sell half", and the published stop can move down |
| Versions | exit_plan 1.2.0 (no open PR or branch touches `scripts/exit_plan.py`). exit_plan.yml 1.3.0. ci.yml 1.1.88: main holds every number up to 1.1.79 (its 1.1.75 is a line inside its 1.1.76 block); 1.1.80-1.1.87 are claimed by open #946, #922, #923, #909, #926, #954, #938, #915 |

## S1 - Evidence

**The script.** exit_plan 1.0.0 was built today on the owner's request (Wed 11 Nov 09:30 Riyadh): each holding gets its own stop and target, recomputed after every close, and the stop never moves down. 1.1.0 (#959) moved it to completed daily bars. It writes the Exit_Plan page, the `_Exit_Plan` ledger and a 07:30 e-mail.

**The defect.** `read_lots` merges a symbol's active lots into one holding: the weighted buy price and the latest buy date. The ledger keys the holding by both: `lot_key` = `SYMBOL|latest buy date|weighted buy`. `prior_row` looks up yesterday's row by that exact key. Two things live only in that row:
- the stop ratchet (`Stop`, `StopNorm`, `Was`): "the stop never moves down" is "at least yesterday's recommended stop for the same lot";
- the once-only first target (`TP1 Reached` / `TP1 Before`): TAKE PROFIT "sell half" is issued once.

When the set of active lots changes, the key changes:
- part of one lot is sold (the weighted price moves);
- one lot is closed (the price moves, and the date moves if it was the newest lot).

`prior_row` then finds no row, and the holding starts again as if it were bought that day.

**What 1.1.0 publishes.** An offline replay (harness L1, the real `run()` on an in-memory workbook and a fake chart feed):
- Two lots: 230 @ 100.00 and 10 @ 104.00. The 15 Oct close of 110 issues TAKE PROFIT "sell half". The e-mail reads "1 action: AAA.US take profit".
- The owner sells 120 shares of the first lot. The key moves from `AAA.US|2026-10-13|100.1667` to `AAA.US|2026-10-13|100.3333`.
- On the next close 1.1.0 reads **TAKE PROFIT again**, and the e-mail subject again reads "1 action: AAA.US take profit". Following it would leave a quarter of the position, not half.

With two fills a few cents apart on one day (harness L2):
- 1.1.0 raises the stop to 105.90 (RAISE STOP), then a sale changes the key;
- on the next close it publishes **RAISE STOP to 104.06**, a lower stop labelled "raise", and the TP1 flag reads "no".

**Is the live book exposed?** The test fixture of main's own `tests/test_exit_plan.py` holds DDI.US as two lots at different prices (230 @ 13.42 on 2 Nov, 10 @ 14.00 on 3 Nov). Whether the live book holds lots at different prices cannot be read offline: no workbook read is allowed here. Any partial sale from a multi-lot holding hits the defect.

**Dedupe.** No open PR touches `scripts/exit_plan.py` or `exit_plan.yml` (all open PRs checked, codex/* included).

## S2 - Change

**`scripts/exit_plan.py` 1.1.0 -> 1.2.0.** Dated WHY block prepended; every prior WHY line verbatim. 2 functions added, 0 removed; the only removed line is `__version__`. Every added line is ASCII.
- `lot_carry_on()`: reads the gate.
- `read_lots`: adds `first`, the earliest active buy date of the holding. This is additive; no caller of 1.1.0 reads it.
- `carry_rows(ledger, symbol, key, lot_date, first)`: the ledger rows of the same symbol under another key whose buy date is on or after `first`, relabelled to the current key, with a `Carried From` field naming the old key. The ledger itself is not changed.
  - Nothing is carried when the current key's buy date is later than every earlier key's date. That is a lot bought since: a top-up stays a new position, as before.
  - Nothing is carried from an older holding that was sold out and bought again (its key's date is before `first`).
  - A malformed key, or a key of another symbol, is skipped.
- `run()`: when on and `carry_rows` returns rows, `prior_row` reads those rows together with the current key's own rows. The entry, R, ATR and target are computed from the current lots, as before. While the current key has no row on an earlier close, the holding's note says "the lots changed (a sale or a closed lot): the stop and the first-target state carry over from <old key>".
- The ledger's Lot column, the page layout, the e-mail format and every other holding are unchanged.

**`.github/workflows/exit_plan.yml` 1.2.0 -> 1.3.0.** Maps `TFB_EXIT_LOT_CARRY: ${{ vars.TFB_EXIT_LOT_CARRY || 'off' }}`. No trigger, schedule or concurrency change.

**`scripts/verify_deployment.py`.** Pins exit_plan 1.2.0.

**`.github/workflows/ci.yml` 1.1.88.** A new blocking lean-unit step, before the board scorecard step:
- `git fetch --no-tags --depth=1 origin 53f2abdc...`;
- `python -m pytest -q tests/test_exit_plan_lot_carry.py` with `TFB_TEST_REQUIRE_BASE=1`, so a failed fetch fails the golden legs.

**Rollback.** Unset the Variable or set it to off.

## S3 - Measured

**`tests/test_exit_plan_lot_carry.py`:** PASS 18/18 as a script (three runs, identical), and 43 passed under pytest together with main's `tests/test_exit_plan.py` (25), with `TFB_TEST_REQUIRE_BASE=1`. Zero network. The legs:
- **L0:** the version floor (1, 2, 0) and the gate words.
- **L1:** the sale above. On: the next close reads HOLD, TP1 kept, the stop not lower (100.47 -> 100.63), one note naming the earlier key, the e-mail "hold all". Off: TAKE PROFIT again. The other holding is the same on and off.
- **L2:** the close fills above. On: HOLD at 105.90 with TP1 kept. Off: RAISE STOP to 104.06 and TP1 lost.
- **L3:** the newest lot closed (the key's date moves earlier): carried.
- **L4 / L5 / L6:** a top-up, a holding sold out and bought again, and a single lot sold down: on equals off, with no note.
- **L7:** a second run on the same close after the change: the same action and stop, no RAISE STOP of its own.
- **L8 off identity:** all 7 scenarios against the untouched main copy, with the gate unset, off, "OFF ", no, 0, enforce and empty. Ledger, page, e-mail and every log line are equal (the version tokens folded).
- **L9:** `carry_rows` directly: the symbol, the window, the top-up guard, malformed keys, the relabel, no mutation of the ledger; `read_lots`' `first`.
- **L10:** the workflow mapping, the manifest pin and the CI step.

**Golden negative on the untouched main copy** (`TFB_TEST_CARRY_TARGET` = `git show 53f2abdc:scripts/exit_plan.py`): PASS 11/18, 7 failed. The 11 that pass are the off-identity and nothing-to-carry legs, which describe main. L1, L2 and L3 fail on their substantive assertions (TAKE PROFIT repeated, the lower stop); L0, L7, L9 and L10 fail on the missing version, function or wiring.

**Lanes:** pending (the lean and contract emulations on an export of this tree are running).

## S4 - Off identity

- **Structure.** With the gate off, `carry` is False, so `carry_rows` never runs, `prior_row` is called once as in 1.1.0, and no note is added. The extra work is one `os.environ` read that cannot raise, and the additive `first` field in `read_lots`, which nothing reads when off.
- **Harness.** L8 pins this for all 7 scenarios and 7 off words against the untouched main copy: ledger, page, e-mail and every log line.

## S5 - Review

Pending: an independent multi-lens review (semantics, off identity and house rules, mutation, CI and merge integration) is running.

## S6 - Owner steps (proposed; nothing is armed here)

1. Merge. There is no Render deploy; the exit plan runs in GitHub Actions.
2. Set the repository Variable `TFB_EXIT_LOT_CARRY=on`.
3. After the next sale from a holding with more than one lot, the holding's note on the Exit_Plan page reads "the lots changed (a sale or a closed lot): ...", and its stop is not lower than the day before.

Rollback: unset the Variable.

## S7 - Known limits and not done

- **A top-up stays a new position.** A lot bought since the last row gives a new key with a later date, and nothing is carried: the stop and target restart from the merged entry, as in 1.1.0. Whether a top-up should keep the ratchet is a judgement for the owner, not a defect fix.
- **Only the stop ratchet and the first-target state carry.** The entry, R, ATR and target are always computed from the current lots.
- **The live book.** Whether the owner's live holdings have several lots at different prices cannot be checked offline.
- **Not done.**
  - No Variable set and no gate armed.
  - No change to the exit rule, the levels, the schedule or the e-mail.
  - No live workbook read or write, and no provider or broker call.
