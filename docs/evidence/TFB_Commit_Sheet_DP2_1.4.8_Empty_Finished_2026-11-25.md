# Commit sheet: Top_10_Investments_v2 reads a finished empty render live (Decision Pages v2 1.4.8)

Date: 2026-11-25. Owner, 15:59 Riyadh: Top_10_Investments_v2 still shows the 02:17 run although the page itself was
built at 14:43; do not rely on v2 until it shows the same run as the new source.

## What the workbook showed (read 16:05 Riyadh)

| Page | Status line | Seats |
|---|---|---|
| Top_10_Investments | `Last run 2026-11-25 15:22:11 \| status: ok \| output: EMPTY \| sheets pool 9727 rows` · Selected 0 / 10 · Scanned 9,727 · Passed 0 | 0 |
| Top_10_Investments_v2 (built 15:42) | `Last run 2026-11-25 02:17:42 · status ok · output HELD`, and the amber line "SEATS FROM THE LAST FINISHED RENDER (02:17:42): ... its render of 15:22:11 did not finish (status: ok \| output: EMPTY ...)" | the 02:17:42 seats |

The release of sync run 45602592475 (decision feed released 15:19, valid until 22:16) was read by the main page. The
15:22:11 render ran to its end and seated nobody: no candidate passed. That is a finished board.

## Cause

`dp2T10LastGoodAction_` (1.4.1 [LAST-GOOD]) returned `use` for **any** board with no seat. The rule was written for a
render Google stopped after it had cleared the board (status left `running…`), but it also caught a render that
finished with `output: EMPTY`. So v2 printed the kept 02:17:42 board, its status, `output HELD`, and a false
"did not finish" line.

## Change

`apps_script/27_Decision_Pages_v2.gs` 1.4.7 -> 1.4.8 [EMPTY-FINISHED]:

- `dp2T10EmptyFinished_(status)` (pure): the status reads `| status: ok`, `| output: EMPTY` and not `running`.
- `dp2T10LastGoodAction_(parsed, emptyLive)`: with `emptyLive`, such a board is read live. A board left `running`,
  in error or without a status still reads the kept copy. With one argument the 1.4.7 rule is unchanged.
- `dp2ReadTop10_` passes `dp2T10EmptyLiveOn_()` (Script Property `DP2_T10_EMPTY_LIVE`, default on).
- `dp2V148CoreOk_` and one `dp2SelfTest` line.

Display only: no gate, verdict, ticket, sizing, alert, order or broker call changes. Both v2 pages read the main page
through `dp2ReadTop10_`, so Portfolio_Decision_v2's switch research reads the same live board.

Rollback: Script Property `DP2_T10_EMPTY_LIVE=off` (the 1.4.7 rule), or Project History -> 1.4.7.

## Tests

- `tests/test_dp2_v1480_empty_finished.js` (+ `.py`): 6/6
  - E0 reversal of the six enumerated hunks (`tests/js/dp2_edits_v1480.json`) = the v1.4.7 file, SHA-256 pinned
    8d7e7daf (origin/main a739438); history chain extended (`tests/js/dp2_history.js` `v1470Source`).
  - E1 golden negative: `DP2_T10_EMPTY_LIVE` off (five spellings) builds the same v2 page as v1.4.7 on the finished
    empty board; a stopped render and a seated board equal v1.4.7 with the switch on or off.
  - E2 the 25 Nov case through the real `buildTop10V2` and `dp2ReadTop10_`: no kept seat, no amber line, the live
    run's time; v1.4.7 printed the kept board under "did not finish (status: ok | output: EMPTY".
  - E3 a stopped (`running`) render still reads the kept copy.
  - E4 the pure rule, gate spellings, self-test line. E5 four mutants, each caught.
- Every older `tests/test_dp2_*.js` harness passes through the extended chain (17 of 17); Apps Script syntax 22/22.

## After merge (owner)

Paste `apps_script/27_Decision_Pages_v2.gs` 1.4.8 into Apps Script, then run **Decision v2 -> Refresh
Top_10_Investments_v2**. The page should then read `Last run 2026-11-25 15:22:11 · status ok · output EMPTY`, 0 seats,
the same run as Top_10_Investments.
