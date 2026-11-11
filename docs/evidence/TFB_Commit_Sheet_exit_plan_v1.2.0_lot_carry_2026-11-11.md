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
- Two lots: 230 @ 100.00 and 10 @ 104.00. The 14 Oct close of 110 (the 15 Oct 07:30 run) issues TAKE PROFIT "sell half". The e-mail reads "1 action: AAA.US take profit".
- The owner sells 120 shares of the first lot. The key moves from `AAA.US|2026-10-13|100.1667` to `AAA.US|2026-10-13|100.3333`.
- On the next close 1.1.0 reads **TAKE PROFIT again**, and the e-mail subject again reads "1 action: AAA.US take profit". Following it would leave a quarter of the position, not half.

With two fills a few cents apart on one day (harness L2):
- 1.1.0 raises the stop to 105.90 (RAISE STOP), then a sale changes the key;
- on the next close it publishes **RAISE STOP to 104.06**, a lower stop labelled "raise", and the TP1 flag reads "no".

**Is the live book exposed?** The test fixture of main's own `tests/test_exit_plan.py` holds DDI.US as two lots at different prices (230 @ 13.42 on 2 Nov, 10 @ 14.00 on 3 Nov). Whether the live book holds lots at different prices cannot be read offline: no workbook read is allowed here. Any partial sale from a multi-lot holding hits the defect.

**Dedupe.** No open PR touches `scripts/exit_plan.py` or `exit_plan.yml` (all open PRs checked, codex/* included).

## S2 - Change

**`scripts/exit_plan.py` 1.1.0 -> 1.2.0.** Dated WHY block prepended; every prior WHY line verbatim. 4 functions added, 0 removed, and no existing function changed except `run()`; the only removed line is `__version__`. Every added line is ASCII.
- `lot_carry_on()`: reads the gate.
- `carry_rows(ledger, symbol, key, lot_date, shares, bars=None)`: the rows of the key the holding was under just before the current key (the key of the symbol's last ledger row before the current key's own trailing rows), relabelled to the current key with a `Carried From` field, when that change was a sale or a closed lot; nothing otherwise. The ledger itself is not changed. On the previous key's last row:
  - its `Shares` is above the shares right after the change (the current key's first trailing row, or the active shares when the current key has no row yet). A top-up or a re-buy, a split or a bonus issue typed into the lots, and a corrected price or date do not lower the count;
  - its key's buy date is on or after the latest active buy date now (no active lot is newer than the key's newest);
  - its `Bars To` (the close it was computed on) is on or after that date too (every active lot was bought by then).
  When the current key also has rows from before the previous key's (the lots returned to an earlier key exactly: a lot deleted or closed again), nothing is carried and the key's own rows apply, as in 1.1.0. Rows whose Lot is not a well-formed `SYMBOL|date|price` are skipped. The previous key's rows hold what it carried itself, so the state follows the holding through a chain of sales. With this run's bars, each copy's `Basis` (the dividend factor its stop was computed on) is moved from the previous key's buy date to the current one (`_factor_at`, the factor exit_levels reads for an entry date): a dividend paid between the two buy dates does not rescale the carried stop, and one paid since the row still rebases it, as in 1.1.0.
- `carry_was(rows, today, bars_to)`: the stop of the last run on an earlier close (the stop published last). prior_row's "highest stop on any earlier close" can be the previous key's, above what the current key last published.
- `run()`: when on and `carry_rows` returns rows, `prior_row` reads them with the current key's own rows. `Was` becomes `carry_was`'s stop when that is more than 0.25% under prior_row's (dividend-normalised), so a carried stop above what was published last reads RAISE STOP, not HOLD. The entry, R, ATR and target are computed from the current lots, as before. The note "the lots changed (a sale or a closed lot): the stop and the first-target state carry over from <previous key>" is added while the current key has no row of its own on an earlier close, or when the carried stop is more than 0.25% above the current key's own stop before this close.
- `read_lots`, the ledger's Lot column, the page layout, the e-mail format and every other holding are unchanged.

**`.github/workflows/exit_plan.yml` 1.2.0 -> 1.3.0.** Maps `TFB_EXIT_LOT_CARRY: ${{ vars.TFB_EXIT_LOT_CARRY || 'off' }}`. No trigger, schedule or concurrency change.

**`scripts/verify_deployment.py`.** Pins exit_plan 1.2.0.

**`.github/workflows/ci.yml` 1.1.88.** A new blocking lean-unit step, before the board scorecard step:
- `git fetch --no-tags --depth=1 origin 53f2abdc...`;
- `python -m pytest -q tests/test_exit_plan_lot_carry.py` with `TFB_TEST_REQUIRE_BASE=1`, so a failed fetch fails the golden legs.

**Rollback.** Unset the Variable or set it to off.

## S3 - Measured

**`tests/test_exit_plan_lot_carry.py`:** PASS 75/75 as a script, and 103 passed under pytest together with main's `tests/test_exit_plan.py` (25) and `tests/test_verify_manifest_pins.py` (3), with `TFB_TEST_REQUIRE_BASE=1`. Zero network: the real `run()` reads an in-memory workbook and a fake chart feed (with an optional adjusted close). The legs:
- **L0:** the version floor (1, 2, 0) and the gate words ("observe", the TFB_EXIT_PLAN mode word, reads off).
- **L1:** the sale in S1. On: the next close reads HOLD, TP1 kept, the stop not lower (100.47 -> 100.63), one note naming the earlier key, the e-mail "hold all". Off: TAKE PROFIT again. The other holding is the same on and off.
- **L2:** two fills on one day. On: HOLD at 105.90 with TP1 kept. Off: RAISE STOP to 104.06 and TP1 lost.
- **L3:** the newest lot closed (the key's date moves earlier): carried.
- **L4 / L5 / L6:** a top-up, a holding sold out and bought again, and a single lot sold down: on equals off, with no note.
- **L7:** a second run on the same close after the change: the same action and stop, no RAISE STOP of its own, the note names the earlier key.
- **L8 off identity:** all 39 scenarios against the untouched main copy, with the gate unset, off, "OFF ", no, 0, enforce and empty. Ledger, page, e-mail and every log line are equal, any version token folded.
- **L9:** `carry_rows`, `_factor_at` and `carry_was` directly: the previous key, the three tests at their boundaries, the shares right after the change, the exact return, malformed rows and other symbols, the basis move, no mutation of the ledger; the last run (also on an older close, and on a weekend); `read_lots` is 1.1.0's.
- **L10:** the workflow mapping, the manifest pin and the CI step.
- **L11:** one lot, TAKE PROFIT, half sold, a top-up, then 10 shares of the old lot sold. On a close of 100: RAISE STOP 94.82 -> 95.43 (the first commit: SELL at 100.30, a stop not published since the top-up). On a close of 117: TAKE PROFIT, as without the sale.
- **L12:** two lots with an ex-dividend date on the newest lot's buy day; the newest lot closed. On: HOLD at 105.29 with "was 105.29" (the first commit: RAISE STOP to 103.41). Off: TAKE PROFIT at 103.41.
- **L13:** sold out and bought again on the same day: on equals off, HOLD at the new position's own stop on a close of 99 (the first commit: SELL at the old 99.45).
- **L14:** TAKE PROFIT under the new key, then the next close: HOLD, no second "sell half", the note gone.
- **L15:** two runs on one Riyadh date, the second on a newer close: the stop holds, HOLD.
- **L16:** a weekend re-run on the same close: TAKE PROFIT stays, as on the one-key path.
- **L17:** sold out, bought again in two lots, then a sale: the old holding's stop and TP1 do not come back.
- **L18:** a lot closed (carried: HOLD), then re-activated with a sale: the re-activated lot is newer than the key in between, so it reads like a lot bought since and the close equals 1.1.0.
- **L19:** two sales on consecutive closes: each note names the key just before, and it is gone on the next close (log, page and e-mail).
- **L20:** a carried holding without price history (CHECK): no note.
- **L21:** the gate switched on after a change made while off: the carried 105.90 comes back as RAISE STOP 104.06 -> 105.90 with the note, and the e-mail says "raise stop" (the first commit: a silent HOLD).
- **L22:** a top-up typed in after its own close was computed (round 2: the old stop 101.31 and TP1 came back), and one dated on the newest lot's buy day: on equals off, the same as the top-up typed in on time (HOLD at 95.31, TP1 no).
- **L23:** a same-day sell-out and re-buy typed in after that close was computed: on equals off, HOLD at the new position's 90.43 (round 2: the old 100.43 came back).
- **L24:** a top-up lot and one fill closed again: a sale from the topped-up position, whose state carries (RAISE STOP 96.06 -> 102.25, TP1 no, the note names the top-up key). Only the top-up lot closed: the key returns exactly, its own rows apply, on equals off.
- **L25:** L21 over a weekend, with the Saturday and Sunday runs on Friday's close: RAISE STOP 104.06 -> 105.90 (round 2: HOLD "was 105.90").
- **L26:** the first run on a close tightens the trail (a forecast under the close) and raises the stop to 103.425; a re-run on that close without the forecast keeps 103.425 and RAISE STOP, and the note names the earlier key.
- **L27:** the two 0.25% thresholds at their boundary, on a seeded ledger: an earlier key's stop 100.20 or 100.25 against 100.00 published last reads HOLD without the note; 100.255 and 100.30 read RAISE STOP (100.00 -> 100.30) with the note. A re-run of the close keeps the earlier close's "was". With a dividend between the earlier key's stop and the stop published last (95.00 on basis 0.95), the thresholds compare normalised stops: HOLD, "was 95.19".
- **L28:** L21 with a dividend factor of 0.95 on every bar: RAISE STOP 98.85 -> 100.61 with the note; L19 on a factor of 0.987654321: the note goes on the next close.
- **L29:** a single lot on a factor of 0.987654321 (not exact at 4 decimals): on equals off.
- **L30:** a sale recorded between two runs on one close: TAKE PROFIT stays for that close (issued on it), with the note; HOLD on the next close.
- **L31:** edits that are not sales, each followed by sales: a buy price typed as 1000.00 and corrected (on equals off; round 3: SELL at 920.00), a Tadawul 1-for-10 bonus typed in as 110 @ 45.4545 (on equals off; round 3: SELL at 50.15), and a 2:1 split typed into two lots (the post-split key carries, HOLD at 102.31; round 3: SELL at the pre-split stop).
- **L32:** a lot typed by mistake (10 @ 1040.00) seen by an hourly run, then deleted, or marked Closed the next morning: on equals off, HOLD at 91.00 (round 3: SELL at 128.03).
- **L33:** a sale of 120 typed as 130 and corrected on the next run: the count goes up, nothing carries, the close equals 1.1.0.
- **L34:** a raise close after carried sales, run at 00:30 and 07:30: RAISE STOP on both, no note on either (round 3: the note came back on the 07:30 run).

**Golden negatives.**
- Main's untouched 1.1.0 (`TFB_TEST_CARRY_TARGET` = `git show 53f2abdc:scripts/exit_plan.py`): PASS 51/75, 24 failed. The 51 that pass are the 39 off-identity legs and the legs whose answer is "nothing to carry" (L4-L6, the L4 top-up, L13, L16, L20, L22, L23, L29, L32, L34), which describe main. L1-L3, L7, L11, L12, L14, L15, L17-L19, L21, L24-L28, L30, L31 and L33 fail on their substantive assertions; L0, L9 and L10 on the missing version, functions or wiring.
- The first 1.2.0 commit (`ef0e70fa`): PASS 59/75. Round 1 (`6aa2ddb7`): PASS 64/75. Round 2 (`a73983a9`): PASS 67/75; L18, L24, L31-L34 and both L9 legs fail, the round-3 findings.

MUTATION_AND_LANES_PLACEHOLDER

## S4 - Off identity

- **Structure.** With the gate off, `carry` is False, so `carry_rows`, `carry_was` and the note are never reached, and `prior_row` is called once as in 1.1.0. The extra work is one `os.environ` read that cannot raise. `read_lots` is unchanged.
- **Harness.** L8 pins this for all 39 scenarios and 7 off words against the untouched main copy: ledger, page, e-mail and every log line, apart from the version string.
- **Fuzz.** ROUND2_FUZZ_PLACEHOLDER

## S5 - Review

ROUND_REVIEW_PLACEHOLDER

## S6 - Owner steps (proposed; nothing is armed here)

1. Merge. There is no Render deploy; the exit plan runs in GitHub Actions.
2. Set the repository Variable `TFB_EXIT_LOT_CARRY=on`.
3. After the next sale from a holding with more than one lot, the holding's note on the Exit_Plan page reads "the lots changed (a sale or a closed lot): ...", and its stop is not lower than the day before.

Rollback: unset the Variable.

## S7 - Known limits and not done

- **Only a sale or a closed lot carries.** Anything else that changes the key starts a new position and restarts the stop and the target from the merged entry, as in 1.1.0:
  - a top-up or a re-buy, also when typed in after its close was computed. Whether a top-up should keep the ratchet is a judgement for the owner, not a defect fix;
  - a split or a bonus issue typed into the lots (the count rises), and a corrected buy price or date (the count holds). A split typed in, then a sale, carries the post-split key (L31);
  - a sale typed with too many shares and corrected on a later run: the count goes up again, so the corrected key starts fresh and a stop raised in between is lost (L33);
  - a closed lot marked Active again (a corrected status): the lot is newer than the key in between, so it reads like a lot bought since (L18).
- **Mixed changes between two runs.** A sale and a top-up typed in between the same two runs: when the top-up is dated after the previous key's newest lot, nothing carries. When it is dated on that same day and the count still fell, the state carries. A same-day sell-out and re-buy typed in after that close was computed carries the old stop when the new lot has fewer shares than the old one (the three tests cannot tell it from a sale); with equal or more shares, or typed in on time, it starts fresh (L13, L23).
- **A return to an earlier key.** When the lots return to an earlier key exactly (a lot deleted or closed again), that key's own rows apply and nothing carries, as in 1.1.0: a mistyped lot cannot leave its stop behind (L32), and closing only a top-up lot brings back the position from before the top-up (L24). A top-up lot closed together with a sale from the older lots is a sale from the topped-up position, whose state carries (L24).
- **A run inside the purchase session.** A previous key computed only before its newest lot's first close (`Bars To` the day before) is not carried. Such a row holds only the initial stop of a position with no close yet, so nothing is lost.
- **Only the stop ratchet and the first-target state carry.** The entry, R, ATR and target are always computed from the current lots.
- **The note.** It names the key the holding was under just before. On a third run of the same close after a sale, when that key's only row was that close's own (the same-day overwrite removes it), nothing is carried and the note is not shown; the current key's own row already holds the carried stop and flag, so the action and the stop are the same.
- **The live book.** Whether the owner's live holdings have several lots at different prices cannot be checked offline.
- **Not done.**
  - No Variable set and no gate armed.
  - No change to the exit rule, the levels, the schedule or the e-mail.
  - No live workbook read or write, and no provider or broker call.
