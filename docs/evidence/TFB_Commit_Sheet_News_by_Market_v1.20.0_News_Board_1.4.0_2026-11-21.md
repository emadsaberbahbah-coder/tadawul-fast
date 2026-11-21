# Commit sheet - the news by market and by person, and every news signal on trial (21 Nov 2026)

Branch `feat/news-per-market-and-people`, against main. Its first commit is the #1046 carry of PR #1048 (see "After the merge"); the news work is the commits after it. Observe only: shown and logged, it controls no buy.

| File | Version |
|---|---|
| `scripts/archive_news_themes.py` | 1.0.0 -> **1.1.0** (3 sources, their own table `news_entity_daily`) |
| `.github/workflows/bq_news_themes.yml` | 1.0.0 -> **1.1.0** (a `sources` input) |
| `scripts/apply_bq_views.py` | 1.19.0 -> **1.20.0** (4 views; `news_entity_daily` is a source table) |
| `sql/bigquery/105_v_market_news.sql` | new |
| `sql/bigquery/106_v_people_news.sql` | new |
| `sql/bigquery/107_v_news_trial.sql` | new |
| `sql/bigquery/108_v_news_trial_summary.sql` | new |
| `scripts/news_board.py` | 1.3.0 -> **1.4.0** (sections 8-10, three lines under the verdict) |
| `scripts/verify_deployment.py` | pins: archive_news_themes 1.1.0, apply_bq_views 1.20.0, news_board 1.4.0 |
| `tests/test_archive_news_themes.py` | N1-N11 extended, N12 (places and people), N13 (the separate table) |
| `tests/test_bq_views.py` | V37 (the loop still reads only its own table), V45 (file order to 108), V48-V51, four PENDING pins |
| `tests/test_news_board.py` | the fake answers the new views; N12 x2 |

No new test file, so `ci.yml` is unchanged.

## Why

The owner wrote on 21 Nov 2026 at 11:03 Riyadh (the build list after the voice call):

> "5. News: a news reading per market; forward event scenarios; momentum/surprise; influential figures of 19 Nov; score every news signal.
> 6. Accuracy: log and score everything. Observe for two weeks (10 sessions), then review with the owner, and switch on only what proved itself."

and, for all of it: "Everything starts in observe mode: shown and logged, but it controls no buys."

This PR covers the news reading per market, momentum and surprise, the people, and the scoring of every news signal. The forward event scenarios and the economic data surprise come next (PR-C2).

## What is built

**The loader, `archive_news_themes` 1.1.0**, counts three more things in the public GDELT tables each UTC day:
- `gdelt_events_places`: the share of the day's events that are fighting (CAMEO 18-20) located in each place: the US, Saudi Arabia, the war zone around the Gulf (Iran, Iraq, Yemen, Israel, Lebanon, Syria, Jordan, Kuwait, Qatar, the UAE, Bahrain, Oman), 16 European countries, Japan, and China with Hong Kong (FIPS codes);
- `gdelt_gkg_places`: the share of the day's documents naming each place, and their mean tone;
- `gdelt_gkg_people`: the same for 17 people. Every pattern was read on GDELT's own spellings first (17-19 Nov 2026): "muhammad ben salman", "abdulaziz al-salman", "takaichi sanae", the split "b jinping". Emmanuel Macron is left out: GDELT's Persons list almost never holds him (3 garbled entries in three days, while 522 documents of 19 Nov alone name him in another field that would triple the cost).

**They merge into their own table, `news_entity_daily`**, never into `news_theme_daily`. See "What went wrong" below.

**`v_market_news`** has one row per news day and market. It reads each market's place against its own previous 250 news days, in standard deviations:
- attention (the share of the news naming the place), tone, and fighting;
- the score: (tone - fighting) / 2, averaged over 5 days;
- signed for the market. Gold and oil read the war zone around the Gulf, and the reverse way: a worse war zone has lifted them. Bonds read the US the reverse way;
- favourable at +0.5 or more, unfavourable at -0.5 or less;
- momentum: the score against its level 5 news days before (improving / worsening at 0.5);
- a surprise: an attention z of 2 or more, with its tone.

**`v_people_news`** does the same for the 17 people. A spike is an attention z of 2 or more on 20 or more documents. The markets each person is expected to move are fixed in the view with the rule, and the trial scores a person only on those.

**`v_news_trial`** scores every signal.
- The day is a US session, as in `v_market_trial`.
- The reading is the last complete news day at least one calendar day before it: the decision day's own news is not complete at its US close.
- The outcome is `v_market_trial`'s, unchanged: the market fund's change over the next 5, 10 and 20 closes from the first close after the day.
- `forward` from Monday 23 Nov 2026, `backtest` before it.

**`v_news_trial_summary`** is the scorecard, in one pass.
- Each reading is compared with the same signal's own mean.
- The t counts one window per market per horizon length, because consecutive windows overlap.
- The worst 5% is exact.
- The rank correlation (IC) asks whether the signal ranked the markets in the order of their next changes. Tied values share their average rank.
- A forward line reads "collecting" until the reading has 10 sessions of its own.

**The News Board 1.4.0** adds three sections and one line each under the verdict:
- 8. News by market;
- 9. People in the news;
- 10. News signals on trial.

Until the views are in BigQuery, each section says so and the page is built all the same.

## What went wrong, and what was done

The first history fill (run 44896924583, 11:00 UTC) put the 52 new features into `news_theme_daily`. That was a mistake: the news loop of 19 Nov 2026 reads every feature of that table.
- Its "events + news themes" arm went from 11 active weights to 28-30, `person_*` and `place_*` among them.
- That would have changed the tilt, the shadow list and the trial fixed that day.

What was done:
1. The 58,708 rows were deleted from `news_theme_daily` at 11:12 UTC, matched by source and that run's id. The 14 themes keep their 4,516 and 11,290 rows, and the arm is back to 11 weights (checked).
2. No page was written from them. The News Board and Trial_Labels had last run at 10:41 UTC, before the fill landed, and did not run again until after the delete.
3. The loader now sends the new sources to `news_entity_daily`. Test N13 holds the line: `news_theme_daily` keeps exactly the 14 themes of 19 Nov.
4. The second fill (run 44898077288) loaded the same 58,708 rows into `news_entity_daily`. No installed view reads that table, so nothing changed.

Two more, after the PR was opened:
- **Codex P2: the headline counted every horizon.** The trial line under the verdict counted holds and forward sessions across 5, 10 and 20 closes, while it says "over 10 closes". It now reads only the 10-close lines that section 10 shows: a hold at 5 closes only is not counted, a hold at all three counts once, and the forward count waits for the 10-close outcomes. Four new asserts in `tests/test_news_board.py`.
- **#1046 did not reach main.** It was merged into the #1045 branch at 10:29 UTC, four minutes after #1045 had gone to main (10:25 UTC); the repository keeps merged branches, so GitHub did not move #1046 onto main. PR #1048 carries it to main unchanged (same tree as the #1046 head 2ade742). This branch was replayed on that commit (same tree as before) and now targets main, so it cannot land on a side branch.

## Evidence (read 21 Nov 2026)

**The fills.** Each fill read 125 GB (78.9 places, 43.2 people, 3.0 fighting), as the dry runs said: under a dollar each at on-demand rates. The daily window adds about 0.35 GB.

**The latest news day (20 Nov, UTC).**
- Favourable: the United States (+1.12), US REITs, Europe (+0.56), Japan (+0.57).
- Unfavourable: China / Hong Kong (-0.78, with unusual attention for four days running), oil and gold (-0.68: the war zone around the Gulf is calmer than its normal), and bonds.
- Neutral: Saudi Arabia (-0.12), Saudi REITs, sukuk.
- People's spikes: 19 Nov Elon Musk (positive tone), Jensen Huang (negative, results day), Xi Jinping (negative), Vladimir Putin (positive), Friedrich Merz (positive); 20 Nov Xi (negative) and Putin (positive).

**The first read of the scorecard (backtest).** About 690 US sessions from Feb 2024, 11 markets. The windows overlap: read the means.

| Signal and reading | Edge vs the signal's mean, 5 / 10 / 20 closes (pp) | t (overlap-adjusted) |
|---|---|---|
| The market's news: favourable | -0.15 / -0.40 / -0.87 | -0.77 / -1.04 / -1.51 |
| The market's news: unfavourable | +0.01 / +0.33 / +0.74 | 0.05 / 1.02 / 1.04 |
| Momentum: improving | +0.18 / +0.12 / +0.21 | 1.09 / 0.34 / 0.34 |
| Unusual attention, unfavourable tone | +0.47 / +0.68 / +1.17 | 1.11 / 0.80 / 0.61 |

The market's news ranked the markets with an IC of -0.02 / -0.06 / -0.11 (t -0.79 / -1.50 / -1.86).

No person's spike reached a t of 1.1 over 10 closes. Several lean the same contrarian way: after Trump's negative-tone spikes the markets tied to him did +1.06 pp better than his base over 10 closes, after his positive-tone spikes -0.96 pp.

**What the first read says, plainly.**
- No news reading is proven, at any horizon.
- If anything, bad news has been followed by rebounds and good news by the reverse. That fits the 19 Nov finding that no news weight beat chance yet.
- The signs were fixed before any outcome was read, and they stay fixed for the forward window. A contrarian rule would be a new rule, for the owner's review.

**Speed.** The whole chain (every view under the scorecard, PR-A's included) read in one query within three minutes. That is inside the board's 300-second wait. It processes 22 MB.

## The forward test

- It runs from Monday 23 Nov 2026 to Mon 7 Dec 2026: 10 US sessions, the same window as the market trial.
- The review is scheduled for 8 Dec with the owner.
- Only what proved itself is switched on, and only with his approval.
- The rules are frozen for the window: the places, the people and their markets, the 250-day normal, the thresholds and the signs.

## Safety

- Observe only. The live system never reads BigQuery. No score, pick, gate, order or variable changes.
- The news loop of 19 Nov is untouched. Its table holds exactly its 14 themes (test N13, test V37), and its arms read the same weights as before this work.
- The board reads only the new views and writes only `News_Board`. Its run log names no holding.
- The broker stays read-only.
- Rollback: `TFB_NEWS_BOARD=off` for the page. The loader's new sources can be left out with the `sources` input, and the daily job's kill switch is `TFB_BQ_NEWS_THEMES=off`.

## Tests

- `tests/test_archive_news_themes.py`, 13 tests:
  - N1: the 66 features are pinned by name, source and kind;
  - N2: each source's SQL;
  - N5-N8: five sources sized, loaded and isolated;
  - N10: the `sources` input is checked before use;
  - N12: the places and people patterns against GDELT's own strings, namesakes refused;
  - N13: the new sources merge into `news_entity_daily` and the loop's table keeps its 14 themes.
- `tests/test_bq_views.py`:
  - V48-V51 cover the four views: the markets equal `v_market_condition`'s, in order; the places and people equal the loader's; every reading has its expected way; no same-day news; each heavy view is read once; no window keyed on a float;
  - V37: the loop still reads only `news_theme_daily`;
  - V45: the file order through 108;
  - the four PENDING pins.
- `tests/test_news_board.py`, 25 tests:
  - the new sections, with figures from the first read;
  - the verdict's trial line reads the 10-close horizon only (Codex P2);
  - the page before the views exist;
  - an empty window;
  - any other refusal fails the run.
- The CI lean lane ran locally: 5,275 passed, 32 skipped.

## After the merge

1. #1045 is on main, and its four views were installed on 21 Nov (`bq_views.yml` write, 11:34 UTC: 70/70 objects match). Merge #1048 (the #1046 carry), then this PR. If this PR is merged first, it brings #1048's content with it and #1048 is closed.
2. Run `bq_views.yml` mode `write` on main, then `verify`. Move the PENDING pins of the installed views to INSTALLED.
3. The daily news job then loads all five sources, the new ones into `news_entity_daily`. If the merge comes more than three days after 21 Nov, dispatch `bq_news_themes.yml` once with write, start 2026-11-21, max_gb 5 and sources `gdelt_events_places,gdelt_gkg_places,gdelt_gkg_people`, so the new table has no gap.
4. The board shows sections 8-10 on its next run. There is no Variable to set.

## Known limits

- **GDELT reads the media, not the market.** Its sources lean English, a place's attention is a share of the whole world's news, and the tone is the press's tone.
- **Markets that read one place carry one reading:**
  - the US, US REITs and bonds (reversed);
  - Saudi Arabia, Saudi REITs and sukuk;
  - gold and oil (both reversed).

  The IC therefore ranks seven distinct readings across eleven markets.
- **People's patterns are GDELT's spellings.** Xi is counted by "jinping" alone. A person rarely in the news, such as the Saudi energy minister, can spike on few documents; the rule needs 20.
- **Surges are rare.** Unusual attention came on 85 to 100 of about 690 sessions per reading, so the forward window will mostly show "collecting" for them.
- **The backtest is not evidence.** The rules were not fitted to it, but they were written knowing the 19 Nov result.

Not investment advice; the owner decides and executes every trade.
