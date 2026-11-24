# Commit sheet: News and trend now on the News Board (news_board 1.5.0)

Date: 2026-11-24. Owner ask (24 Nov, 10:10 and 10:20 Riyadh): the news analysis should show its effect on the price
reading now, not only in the forward review on 8 Dec.

## What changes

`scripts/news_board.py` 1.4.0 -> 1.5.0 adds section 11, NEWS AND TREND NOW, to the News_Board page. It reads views
that already exist (apply_bq_views 1.25.0); no new table, view, collector or ENV.

| Part | Source | What the owner sees |
|---|---|---|
| Holdings and Top 10 rows | `v_news_understanding` for each holding and each Top 10 name of the last 3 board days not already held | Event day, state, how the price answered against SPY (pp and z), trend before, the move research expects, TFB's view, agreement, the main headline |
| State record | `v_news_trend_learning`, period `forward from 2026-11-24`, trend_fit `all`, 5 sessions | Events, share that went the expected way, edge, and the view's reading. History rows are never shown as the record |
| Latest events | `v_news_understanding`, the 8 most recent events across every followed name (routine news and quiet excluded) | The same columns, so a large move on any followed name is visible the day after |
| Verdict line | All of the above | Counts only: names read, names with a lean, names leaning against TFB, events, and which states (if any) are proven forward |

A holding or Top 10 name with no company news in the followed set says so, and names the sets that are collected.

## Safety

- Observe only. The section changes no recommendation, gate or order. The page header still says only the Today Action
  Plan gives orders.
- The public run log prints the verdict line, which holds counts only. No holding symbol, quantity or amount is printed.
- If the views are missing (apply_bq_views older than 1.25.0) the section says it is not set up yet and the rest of the
  page builds as before.
- Each read is optional: a failure in section 11 never blocks the other ten sections.

## Queries

Four new reads per run (16 in total with holdings): the board names, the holdings and board rows, the latest events,
the state records. All were run live against `tfb_archive` before this commit.

## Tests

`tests/test_news_board.py`: query counts updated, the fake answers the three views, section 11 added to the section
list, and three new tests (N12): the rows, the views-not-applied path, and the proven-state verdict. The run log is
checked for holding symbols. `scripts/verify_deployment.py` pin 1.4.0 -> 1.5.0.

## After merge

Nothing for the owner to set. The next News Board run (07:55 or 16:55 Riyadh, or chained after the Pre-Open Check)
writes section 11.

## Rollback

Revert this commit. The page goes back to ten sections; no data is lost.

This is not investment advice.
