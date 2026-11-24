# Commit sheet: the news record so far (apply_bq_views 1.28.0, news_board 1.6.0)

Date: 2026-11-24. Owner, 12:17 Riyadh: "ok let's start the next stage". This is item 3 of the news plan: an interim
daily reading of the news record, so its progress is visible every day instead of only at the 8 Dec review.

## What changes

| File | Version | Change |
|---|---|---|
| `sql/bigquery/129_v_news_now_eval.sql` | new | Each hourly News Now reading scored against SPY |
| `sql/bigquery/130_v_news_now_learning.sql` | new | That record per state and horizon, with a fixed reading |
| `scripts/apply_bq_views.py` | 1.27.0 -> 1.28.0 | The two views |
| `scripts/news_board.py` | 1.5.0 -> 1.6.0 | Section 12, THE NEWS RECORD SO FAR, and one line under the verdict |
| `scripts/verify_deployment.py` | pins | apply_bq_views 1.28.0, news_board 1.6.0 |

## The event and the outcomes (fixed before any outcome was read)

**Event:** for each US symbol and session, the first hourly reading of that session (open or closed today) whose news
was refreshed in that run and whose state carries an expected move. A later reading never replaces it, so a state that
appears only after the move cannot score itself.

**Outcomes**, against SPY on the adjusted basis:

| Name | Window |
|---|---|
| `rest_pp` | from the price at that reading to the session's close (SPY from its own price at the same moment) |
| `f1_pp` | from the session's close to the next session's close |
| `f5_pp` | from the session's close to the close five sessions later |

**Reading** (the same rules as `v_news_trend_learning`):

- **collecting:** fewer than 20 events, or fewer than max(10, 2 x horizon) dates;
- **works as expected:** t_adj of 2 or more;
- **opposite of expected:** t_adj of -2 or less;
- **no clear edge:** anything else.

## Section 12 on News_Board

Two records, each per state and horizon:

- **Nightly news-and-trend:** forward from 24 Nov, 1 and 5 sessions, trend fit 'all'.
- **Hourly News Now:** rest of the session, 1 and 5 sessions.

For each state and horizon the section shows events, dates, the share that went the expected way, the edge, t and the
reading. A record that is still collecting also shows how many events and dates it holds out of what it needs.
Section 11's records come from the same query, which now asks for both horizons, so this adds no extra read of
`v_news_trend_learning`.

## Cost

- Hourly record: about 25 MB per read (checked live with the query inlined), twice a day with the board.
- Nightly record: no extra read.

## Safety

Observe only. The run log line holds counts and state names only. Until view 130 exists, the hourly row says "not set
up yet" and the page builds as before.

## After merge (my side)

Dispatch `bq_views` with mode=write on main to apply views 129 and 130.

## Rollback

Revert the commit. No table changes.

This is not investment advice.
