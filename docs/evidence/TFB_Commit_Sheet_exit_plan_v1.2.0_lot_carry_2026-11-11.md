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

**`scripts/exit_plan.py` 1.1.0 -> 1.2.0.** Dated WHY block prepended; every prior WHY line verbatim. 4 functions added, 0 removed; the only removed line is `__version__`. Every added line is ASCII.
- `lot_carry_on()`: reads the gate.
- `read_lots`: adds `first`, the earliest active buy date of the holding. This is additive; no caller of 1.1.0 reads it.
- `carry_rows(ledger, symbol, key, lot_date, first, bars=None)`: the ledger rows of the same symbol under another key that belong to the same position, relabelled to the current key, each with a `Carried From` field naming its old key. The ledger itself is not changed. A row belongs to the same position when:
  - every lot active now was already bought on the close the row was computed on: `lot_date` (the latest active buy date) is on or before the row's `Bars To`. A lot bought since - a top-up, or a holding sold out and bought again, the same day included - starts a new position, as in 1.1.0, and the rows from before it are not carried, also after a later sale;
  - its key's buy date is not before `first` (an older holding, e.g. an after-hours round trip on the day of the last row, is not carried);
  - its Lot is a well-formed `SYMBOL|date|price` and its `Bars To` is readable.
  With this run's bars, each copy's `Basis` (the dividend factor the row's stop was computed on) is moved from its own key's buy date to `lot_date` (`_factor_at`, the factor exit_levels reads for an entry date). So a dividend paid between the two buy dates does not rescale the carried stop; a dividend paid since the row still rebases it, as in 1.1.0.
- `carry_was(rows, today, bars_to)`: the stop before this close for a holding read across its keys = the highest stop recorded on the holding's latest earlier close. prior_row's "highest stop on any earlier close" can be an older key's stop, above what the current key last published; with this, a carried stop above the last published one reads RAISE STOP, not HOLD.
- `run()`: when on and `carry_rows` returns rows, `prior_row` reads those rows with the current key's own rows, and `Was` comes from `carry_was`. The entry, R, ATR and target are computed from the current lots, as before. The holding's note says "the lots changed (a sale or a closed lot): the stop and the first-target state carry over from <the latest earlier key>" while the current key has no row of its own on an earlier close, or when the carried stop is more than 0.25% above the current key's own.
- The ledger's Lot column, the page layout, the e-mail format and every other holding are unchanged.

**`.github/workflows/exit_plan.yml` 1.2.0 -> 1.3.0.** Maps `TFB_EXIT_LOT_CARRY: ${{ vars.TFB_EXIT_LOT_CARRY || 'off' }}`. No trigger, schedule or concurrency change.

**`scripts/verify_deployment.py`.** Pins exit_plan 1.2.0.

**`.github/workflows/ci.yml` 1.1.88.** A new blocking lean-unit step, before the board scorecard step:
- `git fetch --no-tags --depth=1 origin 53f2abdc...`;
- `python -m pytest -q tests/test_exit_plan_lot_carry.py` with `TFB_TEST_REQUIRE_BASE=1`, so a failed fetch fails the golden legs.

**Rollback.** Unset the Variable or set it to off.

## S3 - Measured

**`tests/test_exit_plan_lot_carry.py`:** PASS 45/45 as a script, and 73 passed under pytest together with main's `tests/test_exit_plan.py` (25) and `tests/test_verify_manifest_pins.py` (3), with `TFB_TEST_REQUIRE_BASE=1`. Zero network: the real `run()` reads an in-memory workbook and a fake chart feed (with an optional adjusted close). The legs:
- **L0:** the version floor (1, 2, 0) and the gate words ("observe", the TFB_EXIT_PLAN mode word, reads off).
- **L1:** the sale in S1. On: the next close reads HOLD, TP1 kept, the stop not lower (100.47 -> 100.63), one note naming the earlier key, the e-mail "hold all". Off: TAKE PROFIT again. The other holding is the same on and off.
- **L2:** two fills on one day. On: HOLD at 105.90 with TP1 kept. Off: RAISE STOP to 104.06 and TP1 lost.
- **L3:** the newest lot closed (the key's date moves earlier): carried.
- **L4 / L5 / L6:** a top-up, a holding sold out and bought again, and a single lot sold down: on equals off, with no note.
- **L7:** a second run on the same close after the change: the same action and stop, no RAISE STOP of its own.
- **L8 off identity:** all 22 scenarios against the untouched main copy, with the gate unset, off, "OFF ", no, 0, enforce and empty. Ledger, page, e-mail and every log line are equal, any version token folded.
- **L9:** `carry_rows`, `_factor_at`, `carry_was` and `read_lots`' `first` directly: the symbol, malformed keys and dates, the Bars To rule at its boundary, the older-holding rule, ledger order, the basis move, no mutation of the ledger, any sheet order.
- **L10:** the workflow mapping, the manifest pin and the CI step.
- **L11:** one lot, TAKE PROFIT, half sold, a top-up, then 10 shares of the old lot sold. On a close of 100: RAISE STOP 94.82 -> 95.43 (the first commit: SELL at 100.30, a stop not published since the top-up). On a close of 117: TAKE PROFIT, as without the sale.
- **L12:** two lots with an ex-dividend date on the newest lot's buy day; the newest lot closed. On: HOLD at 105.29 with "was 105.29" (the first commit: RAISE STOP to 103.41). Off: TAKE PROFIT at 103.41.
- **L13:** sold out and bought again on the same day: on equals off, HOLD at the new position's own stop on a close of 99 (the first commit: SELL at the old 99.45).
- **L14:** TAKE PROFIT under the new key, then the next close: HOLD, no second "sell half", the note gone.
- **L15:** two runs on one Riyadh date, the second on a newer close: the stop holds, HOLD.
- **L16:** a weekend re-run on the same close: TAKE PROFIT stays, as on the one-key path.
- **L17:** sold out, bought again in two lots, then a sale: the old holding's stop and TP1 do not come back.
- **L18:** a lot closed, then re-activated with a sale: carried, HOLD with TP1, the note names the key just before.
- **L19:** two sales on consecutive closes: each note names the key just before, and it is gone on the next close (log, page and e-mail).
- **L20:** a carried holding without price history (CHECK): no note.
- **L21:** the gate switched on after a change made while off: the carried 105.90 comes back as RAISE STOP 104.06 -> 105.90 with the note, and the e-mail says "raise stop" (the first commit: a silent HOLD).

**Golden negatives.**
- Main's untouched 1.1.0 (`TFB_TEST_CARRY_TARGET` = `git show 53f2abdc:scripts/exit_plan.py`): PASS 29/45, 16 failed. The 29 that pass are the 22 off-identity legs and the legs whose answer is "nothing to carry" (L4-L6, the L4 top-up, L13, L16, L20), which describe main. L1-L3, L7, L11, L12, L14, L15 and L17-L21 fail on their substantive assertions; L0, L9 and L10 on the missing version, functions or wiring.
- The first 1.2.0 commit (`ef0e70fa`): PASS 39/45. L11, L12, L13 and L21 fail: they encode the round-1 findings. Both L9 legs fail too.

MUTATION_AND_LANES_PLACEHOLDER

## S4 - Off identity

- **Structure.** With the gate off, `carry` is False, so `carry_rows`, `carry_was` and the note are never reached, and `prior_row` is called once as in 1.1.0. The extra work is one `os.environ` read that cannot raise, and the additive `first` field in `read_lots`, which nothing reads when off.
- **Harness.** L8 pins this for all 22 scenarios and 7 off words against the untouched main copy: ledger, page, e-mail and every log line, apart from the version string.
- **Fuzz.** ROUND2_FUZZ_PLACEHOLDER

## S5 - Review

ROUND_REVIEW_PLACEHOLDER

## S6 - Owner steps (proposed; nothing is armed here)

1. Merge. There is no Render deploy; the exit plan runs in GitHub Actions.
2. Set the repository Variable `TFB_EXIT_LOT_CARRY=on`.
3. After the next sale from a holding with more than one lot, the holding's note on the Exit_Plan page reads "the lots changed (a sale or a closed lot): ...", and its stop is not lower than the day before.

Rollback: unset the Variable.

## S7 - Known limits and not done

- **A lot bought since starts a new position.** A top-up, or a holding sold out and bought again, gives a key whose latest buy date is after the earlier rows' closes, and nothing from before it is carried: the stop and the target restart from the merged entry, as in 1.1.0. Whether a top-up should keep the ratchet is a judgement for the owner, not a defect fix.
- **A run inside the purchase session.** A row computed before the session of the newest lot closed (its `Bars To` is the day before) is not carried after a sale on that day. Such a row holds only the initial stop of a position with no close yet, so nothing is lost.
- **After-hours.** A lot bought after the close of the day the last row was computed on counts as bought on that close. A sell-out and re-buy after hours on that day is caught only when the old key's buy date is before the new lot's (the older-holding rule).
- **Only the stop ratchet and the first-target state carry.** The entry, R, ATR and target are always computed from the current lots.
- **The note.** It names the latest earlier key in ledger order. On a third run of the same close after a sale, when the earlier key's only row was that close's own (the same-day overwrite removes it), nothing is carried and the note is not shown; the current key's own row already holds the carried stop and flag, so the action and the stop are the same.
- **The live book.** Whether the owner's live holdings have several lots at different prices cannot be checked offline.
- **Not done.**
  - No Variable set and no gate armed.
  - No change to the exit rule, the levels, the schedule or the e-mail.
  - No live workbook read or write, and no provider or broker call.
