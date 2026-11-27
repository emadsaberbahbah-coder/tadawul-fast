# TFB Commit Sheet — news_macro_gate 1.13.1 [FEEDS-PRUNE] (the two Gulf feeds the runner cannot read are taken out)

Date: 2026-11-27 (Friday) · Lane: GitHub Actions (📰 News & Macro Gate) · Gate: observe-only · No verdict rule, tab or column changes · No Render deploy

## Why
First run of 1.13.0 on main (dry run 45968251004, 27 Nov):

| Feed | Result |
|---|---|
| Argaam main news + market pulse | **Read** (not in the failures). `gulf_rss` rose from 83–85 to 92 items, newest 0 min old, so the gate has an Arabic Saudi feed again. |
| Asharq Business | HTTP 403, as in every run since 26 Nov |
| Mubasher TDWL | An HTML page, not a feed |

1.13.0 said a feed that refuses or is not a feed is taken out.

## What
`scripts/news_macro_gate.py` 1.13.0 → **1.13.1**: `FEEDS` loses `asharq_business` and `mubasher_tdwl`. Their publishers keep their tiers, so a copy carried by Google News is graded as before.

## Read in the same run (no change here)
- `eodhd_news`: 16 items in the window. After folding (4 repeats) and the issuer link, 1 was kept: most EODHD headlines do not name the company. It stays, observe-only.
- The Arabic Saudi-site searches run inside `google_news` (1,377 items, against 1,287 before). Whether they return dated items about the company is read from `_News_Items` after the next written runs.
- The verdict is unchanged (NO_NEW_ENTRY).

## Tests
`tests/test_news_macro_gate.py` + `tests/test_news_llm_reader.py`: 361 pass, also with `GITHUB_ACTIONS=true`. The feed counts (23 → 21, Gulf 7 → 5) and the Gulf failure list are updated, and the test now checks that the two feeds are gone.

## Owner
Merge when you have read it. Nothing to set.

## Rollback
Revert the script to 1.13.0.

Not a licensed financial adviser; the owner decides and executes every trade.
