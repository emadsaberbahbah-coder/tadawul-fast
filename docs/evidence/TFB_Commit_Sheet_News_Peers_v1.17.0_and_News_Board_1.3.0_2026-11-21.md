# Commit Sheet: the company news against its peers (apply_bq_views 1.17.0, news_board 1.3.0), 2026-11-21

**Scope.** Owner, 21 Nov 2026 02:20 Riyadh (in Arabic: "let us also review the news analysis, and review the updates for 6 hours and see the updates they need"). The review of the company-news scoreboard of 20 Nov (PR 1038) found that its comparison with the S&P 500 measured the names, not their news. `v_company_news_eval` now also sets every name-day against the same-day average of the US names, and takes its reading from that. News_Board section 7 shows the peer figures, with the S&P 500 beside them. This is a research layer only: the live system never reads BigQuery. No score, pick, gate, count, ticket or broker action changes.

## Why
| Finding (measured 21 Nov, read-only BigQuery) | Why it matters | Fix |
|---|---|---|
| Against SPY every class of the re-read company news was ahead over 5 sessions: positive +1.14 pp, neutral +1.16 pp, negative +3.19 pp, and 64 - 67 % of days ahead. Neutral days (no news to speak of) did as well as positive ones. | A class that is ahead whatever it says is not reading the news. In a falling market the archive's US names beat the index; that gap is the names', not the news'. | Compare each name-day with the average of every US name in `symbol_bars_daily` over the same 1 and 5 sessions from the same close (about 490 names a day). |
| The reading (`NO EDGE` / `THIN` / `CANDIDATE`) used the SPY correlation and the SPY means. | A future CANDIDATE could come from the market leg, not from the news. | The reading now uses `corr_5d_peer`, the peer means of the positive and negative classes and the peer matured count. |
| `JOIN spy` dropped a name-day whenever SPY had no bar for the entry date. | A stale SPY series would silently thin the record. None was dropped since 15 Sep (checked: 0 US bar days without SPY). | `LEFT JOIN spy`: such a day keeps its peer figures; its SPY columns are empty. |

## Changes
| Change | Tests | Installed |
|---|---|---|
| `sql/bigquery/98_v_company_news_eval.sql`. New CTE `peer` (per date: the mean 1- and 5-session return of the US names, and how many). New columns after the old ones, which keep their order: `mean_1d_peer_pp`, `mean_5d_peer_pp`, `matured_5d_peer`, `beat_5d_peer_pct`, `corr_5d_peer`, `peers`. The reading comes from the peer columns. Header: WHY THE PEERS and the first peer read. | V40 updated (the 1.16.0 parts that still hold). V41 (new): the peer CTE, entry close only, the peer reading ladder, THIN from the peer count, `LEFT JOIN spy`, the column order. | pending (07807483) |
| `scripts/news_board.py` 1.2.0 -> 1.3.0. The record line reads the peers: per class the 5-session mean against the peers, matured days and share of days ahead, then the reading, then the S&P 500 figures. Until the view is updated in BigQuery, BigQuery answers "Unrecognized name" for the peer columns; the run then reads the 1.2.0 columns and the line says the peer comparison is still to come. Any other refusal still fails the run, and the page stays as it was. | N5 (both queries), N10 (the exact peer line, the 1.2.0 line, a class without a matured day), N11 (new: a full run with peers; a run on the old view; any other refusal exits 2 and writes nothing; no holding in the log). | next News Board run |
| `scripts/apply_bq_views.py` 1.16.0 -> 1.17.0 (WHY changelog). `scripts/verify_deployment.py` pins: views 1.17.0, news_board 1.3.0. | manifest pins | - |

## Pre-merge checks
- **The view body was run in live BigQuery (read-only, 21 Nov).** News days 2 Oct - 18 Nov, about 490 US names per day:

  | Class | Matured days | vs same-day US names (5 sessions) | Days ahead of peers | vs SPY (5 sessions) |
  |---|---|---|---|---|
  | Company news, positive | 52 | +0.69 pp | 63.5 % | +1.14 pp |
  | Company news, neutral | 186 | +0.67 pp | 61.8 % | +1.16 pp |
  | Company news, negative | 5 | +2.55 pp | 100 % | +3.19 pp |
  | Provider, positive (98 % of days) | 391 | +0.49 pp | 60.6 % | +0.96 pp |

  `corr_5d_peer` 0.019 (company news) and -0.023 (provider). Readings: company news `NO EDGE` (positive is not above negative, and the correlation is under 0.05); provider `BIASED (class > 90 %)`. The index had added about half a point to every class. A news day of any class is still ahead of its peers on about 62 % of matured days. That is common to every class, so it more likely belongs to the names that draw news than to what the news says. It is not tested further here.
- **Peer matured count = SPY matured count today** (5 / 186 / 52); they can differ only when one of the two series is behind.
- **Mutants: 11 / 11 killed** by V40 / V41 / N10 / N11:
  1. the peers joined on the news day (look-ahead);
  2. the reading from the SPY correlation;
  3. the positive mean from SPY;
  4. THIN from the SPY count;
  5. an inner join on SPY;
  6. peers from every market;
  7. the peer 5-session leg on 1 session;
  8. the 5-session excess against the 1-session peers;
  9. the fallback on any refusal;
  10. no fallback;
  11. the line showing the SPY means as the peers'.
- **Tests and checks.**
  - Tests: test_bq_views 41/41 (V41 new) and test_news_board 23/23 (N11 new). With test_verify_manifest_pins, 67 passed.
  - ruff (E9, F63, F7, F82) is clean, and the workflow audit reports errors=0, warnings=0.
  - verify_deployment shows 0 drift and 0 missing modules. Its flag line shows the same unarmed sandbox flags as main.

## Post-merge order (lead)
1. A `bq_views` dispatch with mode=write installs the new body; verify must read every object as matching.
2. A News Board dispatch (dry run, then real). The section 7 record line must start "against the same-day average of the US names". Before step 1 it shows the 1.2.0 line and says why. The order of the two steps does not matter.
3. The weekly read of `v_company_news_eval` now uses the peer columns. Nothing may move a decision on the news classes before the view reads CANDIDATE and a forward test agrees.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order; no executable count changes.
