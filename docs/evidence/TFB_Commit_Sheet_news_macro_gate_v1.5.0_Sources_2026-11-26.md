# TFB Commit Sheet — news_macro_gate 1.5.0 [SOURCES] (five more sources; a feed that cannot be read is named, not counted as zero)

Date: 2026-11-26 (Thursday) · Lane: GitHub Actions (📰 News & Macro Gate) · Gate: observe-only, unchanged — the verdict controls nothing · No Render deploy · No ENV or Variable change needed · No tab or header change

## Why
Owner, 26 Nov 2026 12:42 Riyadh (in Arabic): add three to five news sources if it is not a large undertaking, and review what else would make the gate stronger and faster — most of the project's success is gathering more information before others and putting it on the right track.

The gate's own archive, 137 runs from 27 Oct to 26 Nov (`v_news_gate`, `v_news_items`):

| Source | What the archive shows |
|---|---|
| Google News | about 1,030 of the items of an average run — in practice the only news source |
| GDELT | refused (HTTP 429) in every run; nothing at all in 67 of them |
| Fed feed | answered in 134 runs and gave **0 items in all 134**, also in the 7 runs of 12–15 Nov, days after the Fed's press release of 12 Nov. No run named a fault. |
| EDGAR, economic events, S&P 500 / VIX | working |

Two things follow. The gate is blind if Google throttles the runner. And a source that cannot be read looked exactly like a quiet one: `parse_rss()` returned an empty list for any reply it could not parse.

## Change — `scripts/news_macro_gate.py` 1.4.1 → 1.5.0

### 1. Five keyless sources (no key, no cost, each optional and fail-open)
| Source | What it reads | Why it is here |
|---|---|---|
| `bing_news` | Bing News RSS, newest first: the seven English macro searches and every scope name | a second aggregator beside Google News; each item is filed under its publisher (the address inside Bing's click link), so it is tiered like any other headline |
| `yahoo_rss` | Yahoo Finance headline feed, one request per scope name (US ticker or `.SR` code) | per-name headlines, judged by the issuer-link rule of v1.1.0 |
| `press_rss` | publishers' own feeds: MarketWatch, WSJ Markets, CNBC, BBC Business, NYT Business, Investing.com, Seeking Alpha | a publisher's feed carries a story before a search index lists it, and does not depend on our search words |
| `official_rss` | SEC press releases, BEA, BLS, ECB, EIA, White House presidential actions | primary sources (tier A): a release confirms its own story |
| `gulf_rss` | Asharq Business (Arabic), Arab News, The National (business), Al Jazeera | the Gulf anchor is the one that moves this book |

`FEEDS` is the registry: feed id, group, URL, publisher domain (it sets the tier), language, window. Seventeen feeds. The Fed's own feed stays the source `fed_rss`.

Limits each new request keeps:
- a feed is asked **once** per run, for at most 12 seconds, with no retry; the next run asks again;
- a group has a time budget (Bing 90 s, Yahoo 60 s, press 75 s, official 75 s, Gulf 45 s — 345 s at the very worst, in a job of 20 minutes that today takes 2–4);
- Bing and Yahoo stop after two failed requests in a row; in a feed group each feed stands alone, so one dead publisher does not silence the others;
- an item with no readable date, or outside its window (24 h market-wide, 72 h for a name or an official release), is left out. The rule reads a headline with no date as published now, so an old undated item would have counted as fresh;
- a reply over 4 MB or one that declares XML entities is not parsed;
- sec.gov is asked with the declared EDGAR contact (`TFB_SEC_USER_AGENT`) and not at all without it. Every other feed is asked as `TFB-news-gate/1.5.0`; nothing pretends to be a browser.

### 2. A reply that is not a feed is a named failure
`parse_feed()` reads RSS 2.0, RSS 1.0 and Atom **from the reply's bytes**, removes a byte-order mark and leading blank lines, and raises when the reply is not a feed. `google_news` and `fed_rss` now use it. A block page or a mis-decoded reply appears under **Sources Failed** with its first characters; for Google News it also counts toward the two-failure breaker.

One way the silent zero happens, reproduced in the tests: `requests` decodes a `text/xml` reply that names no charset as ISO-8859-1; a UTF-8 byte-order mark then reaches the XML parser as three stray characters and the whole feed is refused. Whether that is the Fed feed's fault on the runner is **not yet known** — the first run after the merge either reads the feed or names the reason.

### 3. One story from one publisher stays one member
`fold_new_sources()`: a headline from a new source is left out when an older source, or an earlier new one, already carries the same headline from the same publisher in the same scope. Without it the same Reuters story reached through Google and through Bing would count twice in the cluster's ratios (denials, easing against falling, direction). **No item of the six older sources is ever left out by it.**

### 4. Smaller parts
- The issuer-link rule also judges `bing_news` and `yahoo_rss` name items.
- The new news sources are carried in the run-to-run memory like GDELT and Google News (24 h market-wide, 72 h for a name); official feeds are re-read whole.
- The digest and the evidence file gain one line per source: dated items kept, age in minutes of its newest and median item, items left out (outside the window / undated / repeated), requests failed.
- The **Sources Failed** cell holds 1,000 characters (was 500) so the feeds a group could not read stay named.

## What this can change in the verdict
It **can raise** a verdict or a name flag: more publishers reach the three-domain rules sooner, and an official release grades its cluster 1.

It can also **lower** one, in two corners that exist today:
- a denial arriving from a new source can turn a cluster into "denied" (grade 5) when denials are half of it;
- a gap in the Gulf rule: an unconfirmed Gulf story (grade 3–4) raises CAUTION, three confirmed publishers raise NO NEW ENTRY, but a story confirmed by **one or two** publishers (grade 1–2, fewer than three domains) raises nothing. A second source joining a thin story can therefore remove its CAUTION line. The gap is in `evaluate()` since v1.0.0 and is left as it is here; it is the first item of the rule review that follows this change.

The gate still controls nothing; the `Version` column of `_News_Gate` marks the runs, so the score can be read before and after 1.5.0.

Not changed: the verdict rules, the taxonomy, the 200-row limit per run (43 % of the runs since 19 Nov reached it; rows are cut lowest severity first, so the verdict-relevant ones are recorded), GDELT's throttling.

## What is not known until the first run
The seventeen feeds and the two endpoints were checked on 26 Nov through a web fetch, not from a GitHub runner. Some publishers refuse data-centre addresses or unknown clients. A feed that refuses is named in Sources Failed and costs one request; it is removed in a follow-up once two or three runs have shown it.

Checked and **not** added: US Treasury press feed (404), State Department (403), IMF (403), OilPrice, Argaam and Zawya (no readable feed), MarketWatch "market pulse" and CNBC economy (stale), Al Arabiya business, FT and Guardian (stale copies), SPA (not clearly a feed), Nasdaq (syndicated copies of other publishers).

## Tests
`tests/test_news_macro_gate.py`: 290 pass (261 before; 29 new under G14). The older tests run unchanged apart from the version pin and one test that now also takes the new hosts down.

G14 pins: the registry (unique ids, tiers known, budgets, rollback list, workflow text); the reader on RSS 2.0 / RSS 1.0 / Atom, on bytes with a byte-order mark, on Arabic bytes; eight kinds of reply that are not a feed; Bing items filed under their publisher; Yahoo asked per listed name and judged by the issuer rule; a dead feed named while its group goes on; every feed asked once with the 12 s limit; the time budget; sec.gov with the contact or not at all; undated and old items left out; one story one member in seven cases; **the new sources alone confirm a Gulf situation and carry it for 24 h, and two domains are still not enough**; **with every new feed empty the run equals the six-source run** — same verdict, drivers, clusters, items written.

Twenty hand mutants (bytes ignored, undated kept, copy not folded, Bing link kept, feed retried, Yahoo not judged, memory dropped, HTML accepted as a feed, Google silent again, Fed silent again, timeout unbounded, SEC without contact, group breaker, entities allowed, name window 24 h, memory row losing to a fresh copy, future items kept, legacy entities undone, new sources off by default, mark not removed) — every one fails a test.

## Files
| File | Change |
|---|---|
| `scripts/news_macro_gate.py` | 1.4.1 → **1.5.0** |
| `tests/test_news_macro_gate.py` | G14, test doubles for the new sources, version pin |
| `scripts/verify_deployment.py` | pin 1.5.0 |
| `.github/workflows/news_macro_gate.yml` | 1.1.1 → 1.1.2: header note and the text of the `sources` input. Same triggers, hours, permissions, steps and ENV |

## Owner
Merge when content. Nothing to set. If the repository Variable `TFB_NEWS_GATE_SOURCES` holds a list (it is normally blank), the five new names must be added to it — the first run's log shows whether they ran.

## After the merge (Claude)
1. Dispatch one dry run on main and read which feeds answered the runner.
2. Report the list; a follow-up removes the feeds that refuse.
3. After ten sessions, score each source from the archive: share of headlines it carried first, and whether the verdicts of 1.5.0 differ from what 1.4.1 would have read.

## Rollback
Set the repository Variable `TFB_NEWS_GATE_SOURCES` to `edgar,gdelt,google_news,fed_rss,econ,market` (the five are then never asked), or revert `scripts/news_macro_gate.py` to 1.4.1.

Not a licensed financial adviser; the owner decides and executes every trade.
