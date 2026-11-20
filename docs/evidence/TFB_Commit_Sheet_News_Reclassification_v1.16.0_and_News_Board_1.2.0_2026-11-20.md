# Commit Sheet: the company news re-read (apply_bq_views 1.16.0, news_board 1.2.0), 2026-11-20

**Scope.** Owner, 20 Nov 2026 12:54 Riyadh (in Arabic: "let us do the expected improvement to the news classification, improve it and re-classify it"). Every archived headline gets classified again from its own text. The new classes get their own scoreboard, and the owner's holdings show their re-read news on News_Board. This is a research layer only: the live system never reads BigQuery. No score, pick, gate, count, ticket or broker action changes.

## Why
| Finding (measured 20 Nov) | Why it matters | Fix |
|---|---|---|
| The provider's sentiment says "positive" on 455 of 464 US name-days (98 %). It has a median polarity of 0.99 and a correlation of -0.03 with the next five sessions against SPY. | A signal that is almost always "positive" cannot separate good news from bad. Earnings misses are scored positive too. | Each headline is re-read: event type, direction and weight, taken from the clause before "despite" / "even as" / "while". |
| 4 in 10 of the 1,306 headlines filed under a symbol are about another company. Examples: a Citi analyst note on BJ's under C.US, Colgate under BNP.PA, Micron's CTO under CTO.US, UK takeover filings under INVP.L, Commerzbank (XTRA:CBK) under CBK.US. | The provider counts other companies' news as the holding's own. | Relevance is decided first: company / actor / roundup / noise / other. Only `company` headlines score. |
| A price move ("stock sinks", "shares jump") was read as news. | The move has already happened, so it is context, not information about what comes next. | PRICE_UP / PRICE_DOWN carry weight 0. They are kept in `price_move`. |

## Changes
| Change | Tests | Installed |
|---|---|---|
| `sql/bigquery/97_v_symbol_news_class.sql` (new). One row per symbol and article. Columns: relevance, event_type, direction, weight, score (company only), price_move, with provider_polarity beside them. 28 rules in priority order. A PREVIEW is never read as the results. Per-market exchange-prefixed tickers are used. A plain US ticker counts only for names of 3+ letters that are not a common acronym, and never right after a colon. | V39: the literals are read from the file itself, and a Python reading of the same steps checks 24 worked headlines (beat / miss / in line, the clause before "despite", a preview, a dividend raise, a class action, an alias with accents, Citi as the speaker, Micron's CTO, Commerzbank's CBK, Tenaris's TEN, a filing, a buy-list roundup). The view's steps and their order are pinned. | pending (c998f041) |
| `sql/bigquery/98_v_company_news_eval.sql` (new). For each signal and class: days, 1 and 5 sessions after the first close following the news day, excess vs SPY, beat %, corr and a reading (NOT MEASURABLE / BIASED / NO EDGE / THIN / CANDIDATE). US names only. The provider's sentiment is scored the same way beside it. | V40: entry strictly after the news day, excess vs SPY, the reading ladder, no CURRENT_DATE; the 16 Nov scoreboard is left as it was | pending (130256c0) |
| `scripts/news_board.py` 1.1.0 -> 1.2.0. New section 7 "COMPANY NEWS, RE-READ": each holding's last 7 days, with the class (POSITIVE / NEGATIVE / neutral / no company news), score, headlines about the company vs about others, main headline with its event and day, the provider's old reading, and the scoreboard line. The run log has counts only and no symbols. | test_news_board N9, N10 (22 pass) | next News Board run |
| `scripts/apply_bq_views.py` 1.15.0 -> 1.16.0 (WHY changelog). `scripts/verify_deployment.py` pins (views 1.16.0, news_board 1.2.0). | manifest pins | - |

## Pre-merge checks
- **Parity in live BigQuery (read-only, 20 Nov).** The view body was run in full against `symbol_news`, and the Python reading in V39 was run on the same 1,306 headlines. Relevance, event and direction matched on 1,306 / 1,306. The relevance split is: company 638, other 532, noise 92, roundup 19, actor 17.
- **Defect found by the worked examples and fixed before the PR.** A plain US ticker was matching another market's exchange-prefixed ticker: "Commerzbank (XTRA:CBK)" was read as CBK.US company news, and "Tenaris (BIT:TEN)" as TEN.US. After the fix exactly those 2 rows changed (company -> other), and parity was re-run at 1,306 / 1,306.
- **First read of the scoreboard (2 Oct - 20 Nov).**

  | Signal | Positive | Neutral | Negative | Corr (5d) |
  |---|---|---|---|---|
  | Company news | 57 days, +1.17 pp vs SPY over 5 sessions | 213 days, +1.14 pp | 6 days, +3.19 pp | 0.023 |
  | Provider | 455 days | 3 days | 6 days | -0.031 |

  Reading: the new classes are balanced (21 % positive, 77 % neutral, 2 % negative), but there is no edge yet (NO EDGE). The classification is observe-only and keeps being measured by the view itself.
- **Tests.** test_bq_views 40/40 (38 + V39, V40). test_news_board 22/22. With test_verify_manifest_pins: 65 passed. ruff (E9, F63, F7, F82) clean. Workflow audit errors=0 warnings=0. `check_objects` returns []. verify_deployment: 0 drift, 0 missing. Its one selftest FAIL (shariah authority, no SELFTEST line in this sandbox) is the same on main.
- **Mutants.** 10 SQL mutants were run, and each one is killed by V39 / V40:
  - a plain ticker allowed after a colon;
  - no main-clause cut;
  - rule priority reversed;
  - actor headlines scored;
  - a price move scored;
  - the actor step removed;
  - an alias dropped;
  - entry on the news day;
  - a THIN floor of 6;
  - absolute return instead of excess.

## Post-merge order (lead)
1. A fresh `bq_views` dispatch with mode=write installs the two pending bodies. Verify must then read all objects as matching, and the pins move to INSTALLED.
2. A News Board dispatch: dry run first, then a real run. Section 7 shows each holding's re-read news.
3. Read `v_company_news_eval` weekly. Nothing may move a decision on the new classes before the view reads CANDIDATE and a forward test agrees.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order; no executable count changes.
