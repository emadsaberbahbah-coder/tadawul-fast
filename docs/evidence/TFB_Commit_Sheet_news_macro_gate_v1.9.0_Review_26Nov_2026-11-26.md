# TFB Commit Sheet — news_macro_gate 1.9.0 [REVIEW-26NOV] (four corrections from the review of the day's work)

Date: 2026-11-26 (Thursday) · Lane: GitHub Actions (📰 News & Macro Gate) · Gate: observe-only — it controls no buy · No tab or header changes · No Render deploy

**Built on top of PR 1095 (public log, 1.8.1): merge that one first.**

## Why
Owner, 26 Nov 16:30 Riyadh: *"Please review and try to correct and improve all."* The review of the day's runs and of the archive since 19 Nov found four things that are wrong today, each with evidence.

## 1. The rule gap: more confirmation lowered the verdict
A Gulf high-severity story (war escalation, emergency policy, market turmoil) raised:

| What the story had | Before | Now |
|---|---|---|
| One unknown blog (grade 3) | CAUTION | CAUTION |
| The blog plus one wire (grade 2, 2 domains) | **GO** | CAUTION |
| One wire alone (grade 2) | **GO** | CAUTION |
| An official source alone (grade 1, 1 domain) | **GO** | CAUTION |
| Three domains with a wire (grade 1) | NO_NEW_ENTRY | NO_NEW_ENTRY |

A confirmed story now counts at least as much as an unconfirmed one. The driver reads `macro (few sources): …`. Denial-led and social-only stories (grades 5 and 6) still raise nothing. Anchors outside the Gulf are unchanged.

**Archive replay** (BigQuery, `v_news_items` and `v_news_gate`, the 67 recorded runs since 19 Nov, each run's Gulf clusters re-graded over its own 24 hours):
- 13 runs held a cluster in the gap.
- All 13 were already NO_NEW_ENTRY on other clusters, so **no recorded verdict changes**.
- The gap opens on a quiet day, and the archive has not had one: 64 of the 67 runs read NO_NEW_ENTRY and 3 read CAUTION.

That last figure is its own finding: a verdict that almost never changes tells the reader little. The rule that compares a new escalation with the standing baseline is the next build.

## 2. News about names was recorded late
A run records at most 200 new rows, the most severe first. 29 of the 67 runs filled that cap. A medium or low item about a name waited behind every market-wide item of the same severity.

Archive since 19 Nov, hours from publication to the row being recorded:

| Items | Count | Median | 90th percentile | More than a day late |
|---|---|---|---|---|
| Market-wide, high | 1,131 | 1.9 h | 7.7 h | 0 |
| Market-wide, medium | 8,888 | 2.3 h | 9.3 h | 0 |
| About a name, medium | 52 | 16.6 h | 63.7 h | 21 |
| About a name, low | 50 | 6.7 h | 64.3 h | 20 |

Part of the name delay is not the cap: name news is collected over 72 hours, and a name that joins the scope brings older items with it. The cap's share cannot be separated in the archive. The order is wrong either way, because the Exit Plan and Holding Read pages read names from `_News_Items`.

**Now:** within the cap, items about names come first, then severity and grade as before (`pick_new()`). Names are a few dozen rows at most. The digest says how many classified items wait for a later run and how many of them are about names. No item is dropped: a waiting item is recorded by a later run while its source still lists it. The verdict never depended on what was recorded in the same run, so it does not change.

## 3. bls.gov refused every request
`official_rss` named `bls: HTTP 403` in every run since the feed was added. The BLS terms reserve the right to block robots that give no contact for their owner. The feed is now asked with the declared contact (`TFB_SEC_USER_AGENT`), like sec.gov, and not at all without it. If BLS still refuses, the note stays and the feed goes at the 27 Nov review.

## 4. The gate lost its memory when the workbook was busy
Each run carries the items recorded by earlier runs that can still move the verdict: about 1,300 of them. In 3 of the last 15 runs, and in 2 of the 4 runs chained after the Exit Plan on 26 Nov, the read of `_News_Items` was refused with HTTP 429. That is the workbook's per-minute read quota, hit right after the hourly Exit Plan. The run then went on with "carried from earlier runs 0", and only the log said so.

On those runs the verdict rested on that one run's own collection. Today's verdicts held, because the fresh collection was enough. On a day when the confirmation came over several hours they would not have.

**Now:**
- A read refused with HTTP 429 is asked again after 20 seconds, then after 40 more. The quota is per minute, so this is usually enough. Any other failure is not retried.
- When the memory still could not be read, the digest says so in a line of its own, with the reason.

The test shows the cost: with the memory lost, a situation recorded two hours earlier as NO_NEW_ENTRY is no longer NO_NEW_ENTRY; with one or two refusals and the retry, it is carried.

## Not changed, and why
- **Asharq Business and Arab News (HTTP 403 from the runner).** These are publishers refusing a data-centre address, not a missing contact. Working around that would be wrong. They are judged at the 27 Nov review after a day of runs; a replacement must be seen working from the runner first.
- **GDELT (HTTP 429, 76–157 s a run).** It stops itself after two refusals and stays inside its budget. No evidence yet for a better setting.

## Tests
- `tests/test_news_macro_gate.py`: 315 pass. New G18:
  - the table above, row by row;
  - the ladder only climbs as sources are added;
  - a social-only Gulf story and a one-wire Taiwan story are still GO;
  - `pick_new()` order and counts;
  - a run with the cap at 1 records the name's row and says what waits, with the same verdict as an uncapped run;
  - a busy workbook: one, two and three refusals, and a refusal that is not HTTP 429.
- `tests/test_news_llm_reader.py`: 22 pass. Both files also pass with `GITHUB_ACTIONS=true`.
- The hash that pinned "no raise rule changed since v1.1.0" is updated on purpose, with the one changed rule named beside it.
- Eleven hand mutants each fail a test: the old gap restored, grades 5–6 let in, the Gulf limit dropped, names not first, BLS without contact, the waiting count zeroed, the new line raising NO_NEW_ENTRY, no retry, every error retried, the digest line removed, a stale error kept after a good retry.

## Owner
Merge PR 1095 first, then this one. Nothing else.

## Rollback
Revert the script to 1.8.1.

Not a licensed financial adviser; the owner decides and executes every trade.
