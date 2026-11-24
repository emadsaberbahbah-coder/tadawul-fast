# Commit sheet: Learning Watch 1.0.1 and News Now 1.1.1

Date: 2026-11-24. Owner, 22:56 Riyadh: a full review of the Render log and the workbook, with the updates it needs.
These two fixes come from the first live pages.

| File | Version | Fix |
|---|---|---|
| `scripts/learning_watch.py` | 1.0.0 -> 1.0.1 | (a) `v_confidence_calibration` gives one reading to a pair of rows: "inverted: HIGH below MEDIUM" sits on both the HIGH and the MEDIUM row of a market. Only the HIGH row is now AGAINST; the MEDIUM row, the better one, is NEUTRAL. The page had shown SA \| MEDIUM (+2.19 points, 76 % beat its market) as AGAINST. (b) A sample size is a count, so it is shown without a sign. |
| `scripts/news_now.py` | 1.1.0 -> 1.1.1 | No EODHD news call for Saudi `.SR` names. `symbol_news` has never held a `.SR` headline, and both calls failed on every run of 24 Nov. Such a name now reads "no feed": not a failed call, not counted in the budget, and its row says "no company-news feed for this market". |
| `scripts/verify_deployment.py` | pins | both scripts |

## Tests

- `tests/test_learning_watch.py`: 47 passed, including the pair-reading test.
- `tests/test_news_now.py`: 19 passed, including the no-feed test.
- Views, pins, learning guards and the shadow guard pass.

## Rollback

Revert the commit. These are observe-only pages.

This is not investment advice.
