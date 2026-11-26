# TFB Commit Sheet — news_macro_gate 1.10.0 [ORIGINS] (a headline copied by several sites is one confirmation, not several)

Date: 2026-11-26 (Thursday) · Lane: GitHub Actions (📰 News & Macro Gate) · Gate: observe-only — it controls no buy · No tab, header or row changes · No Render deploy

## Why
Owner, 26 Nov 19:10 Riyadh (in Arabic): what matters is how an item is let through, used and classified, *"and the measure of whether the item is repeated on other sites"*. 19:13: *"if it is ready, build it now"*.

The gate measured confirmation by counting **domains**. One agency story reprinted under the same headline by a network of sites counted as that many confirmations.

Archive, 19–26 Nov (`v_news_items`, 10,121 items):
- 639 headlines appeared word for word on two or more domains: 1,838 items.
- One headline of 24–26 Nov sits on 15 sites of a single syndication network, every one an unknown source.
- High-severity market-wide situations averaged 10.6 domains a day, and 9.3 once copies count once.

## What
`scripts/news_macro_gate.py` 1.9.0 → **1.10.0**.

- **`origins()`**: members whose headline is the same are one origin. "The same" means equal after a trailing site name (` - Reuters`, ` | BBC News`) is removed and the text is normalised. The copy from the best tier speaks for the origin.
- **A short headline is never merged.** Under 25 characters, two sites can write the same words on their own ("Oil prices rise").
- **`information_grade()`** grades the origins. The domain counts in `evaluate()`'s rules (three domains, two domains) count the origins' domains.
- **A copy never raises anything.** A site that only says it quotes a wire is not given the wire's tier. It merges with the wire's own item when that item is in the cluster, and stays its own source when it is not.
- **The driver line shows both counts** when they differ: `grade 2, 3 sources (1 independent)`.
- **Nothing is dropped.** Every copy is still a row of `_News_Items`; only what a cluster is worth changes.

| A Gulf high-severity story | Before | Now |
|---|---|---|
| One wire's headline, reprinted on two more sites | NO_NEW_ENTRY (grade 1, "3 domains") | CAUTION (grade 2, 1 independent) |
| Three sites, each in its own words | NO_NEW_ENTRY | NO_NEW_ENTRY |
| Three own-worded sites plus a copy | NO_NEW_ENTRY | NO_NEW_ENTRY (4 sources, 3 independent) |

## Archive replay
BigQuery, the 67 recorded runs since 19 Nov. Every market-anchored cluster (120 cluster ids) was re-graded both ways over each run's own 24 hours, under the 1.9.0 rules.

- In all 67 runs some cluster loses domains once copies count once.
- No cluster's grade changes. No cluster's level changes.
- **No recorded verdict changes.**

Those days were confirmed many times over, so removing copies still left enough. The rule matters on a thin day, when one reprinted story is all there is. The archive has not had such a day, so this change is a guard, not a correction of a past verdict.

## Tests
- `tests/test_news_macro_gate.py`: 320 pass. New G19:
  - the headline key: site tail removed, short headlines not compared, a long tail kept as headline;
  - `origins()` keeps the best copy, in order, and merges nothing else;
  - a copy never raises the grade: wire plus two copies, two papers with one story, a syndication network, a blog quoting a wire, a denial reprinted three times;
  - the table above, and the market-anchor rule (one story on two wires' sites no longer counts as two);
  - a written run still records all three copies.
- Five older tests used one headline on three domains to stand for three confirmations. Their fixtures now give each wire its own wording; what they assert is unchanged.
- The two hashes that pin `evaluate()` are updated on purpose, with the reason beside them.
- `tests/test_news_llm_reader.py`: 22 pass. Both files also pass with `GITHUB_ACTIONS=true`.
- Seven hand mutants each fail a test: grade without origins, rule counts without origins, first copy kept instead of best, no minimum length, site tail not removed, the "independent" note removed, copies not merged.

## Limits
- Two reports of one event in different words are still two origins. That is intended: it is how independent confirmation looks.
- A site that rewrites a wire story under a new headline still counts as its own origin. Only the per-source scoring of 8 Dec can tell such sites apart.

## Owner
Merge when the checks are green. Nothing else.

## Rollback
Revert the script to 1.9.0.

Not a licensed financial adviser; the owner decides and executes every trade.
