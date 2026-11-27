# TFB Commit Sheet — news_macro_gate 1.13.0 [SOURCES-REVIEW] (Yahoo off by default, Arabic Saudi searches and feeds, EODHD company news)

Date: 2026-11-27 (Friday) · Lane: GitHub Actions (📰 News & Macro Gate) · Gate: observe-only · No verdict rule, tab or column changes · No Render deploy

**Built on top of PR 1103 (1.12.1): merge that one first.**

## Why
This is the news-sources review the owner set for 27 Nov. What the runs of 26–27 Nov showed:

| Source | Finding |
|---|---|
| Yahoo | 0 headlines inside the window in every run (222 of 224 outside on 26 Nov, 221 of 221 at 09:42 on 27 Nov) |
| Asharq Business | Refused the runner (HTTP 403) in every run. The gate had **no Arabic feed at all**: the Al Jazeera feed is the English one |
| Saudi names | Searched on Google News in English only |
| EODHD company news | Read every hour by news_now, but not by the gate |
| BLS | Answers in every run since 1.9.0 (a contact in the request), so it stays |

## What
`scripts/news_macro_gate.py` 1.12.1 → **1.13.0**; workflow 1.1.7 → **1.1.8** (input description only).
1. **Yahoo off by default.** `DEFAULT_SOURCES` = every source but `OFF_BY_DEFAULT` (`yahoo_rss`). Naming it in `TFB_NEWS_GATE_SOURCES` still reads it.
2. **Arabic Google News on the Saudi sites** (Argaam, Mubasher, Al Eqtisadiah, Al Arabiya):
   - one MACRO search for the Saudi market, in place of the refused Asharq feed;
   - one search per Saudi scope name by its Tadawul code, so no Arabic name is needed. The issuer link keeps a name item only when the headline gives the code in brackets, e.g. "(2222)".
3. **`eodhd_news`:** EODHD's company news (the call news_now makes) for every scope name but the Saudi ones.
   - 72-hour window; folded with the other sources by (scope, publisher, headline); checked by the issuer link.
   - **EODHD's sentiment is not read.** `v_company_news_eval` found it positive on 455 of 464 days.
   - The key is scrubbed from every failure note.
   - EODHD's own exchange codes are asked (BARC.L → BARC.LSE, SAP.DE → SAP.XETRA), the table news_now uses; a test holds the two copies equal (Codex P2 on #1104).
4. **On trial in `gulf_rss`:**
   - Argaam's main news (`sectionid=1523`) and market pulse (`sectionid=70`), live on 26 Nov;
   - Mubasher's TDWL news, which answered a page, not a feed, to a cloud reader;
   - a Google News search of `sama.gov.sa` (SAMA has no feed).

## Not proven yet: the first dry run on main decides
- Whether the runner may read the three Arabic feeds.
- Whether Google News honours `site:` for these sites, and whether the per-code searches return dated items about the company.

A feed that refuses or is not a feed is named in Sources Failed. Anything that gives nothing dated and relevant is taken out in a follow-up, and Asharq goes with it once an Arabic source is proven.

## Not done (and why)
- **The Arabic-name map for the Saudi names**, which would tie general Arabic headlines to a name. It needs the Arabic names checked against a source, and web pages could not be opened from here without the owner's approval. The per-code searches cover the Saudi names meanwhile.
- **Saudi Exchange announcements:** the owner's Chrome was not connected, so the announcements page's requests could not be read.
- **`ARGAAM_API_KEY` on Render** is not an Argaam news key. In `core/providers/argaam_provider.py` it is the key of whichever quotes provider `ARGAAM_PROVIDER_PRESET` names (Sahmk, Twelve Data or StockerAPI): quotes, history and profile, no news. Render's environment values cannot be read from here; the preset's value is in the service's Environment page. Nothing was subscribed to.

## Tests
- `tests/test_news_macro_gate.py` + `tests/test_news_llm_reader.py`: 361 pass, also with `GITHUB_ACTIONS=true`.
- New G23:
  - Yahoo off by default and read when named;
  - the Saudi-site searches (MACRO and per code) and the issuer link on an Arabic headline;
  - EODHD parsing without sentiment, its 72-hour window and undated items, no Saudi name asked, the key scrubbed;
  - one story counted once across Finnhub and EODHD;
  - the trial feeds and the SAMA search.
- Changed with a reason:
  - source counts (13 → 14), feed counts (20 → 23, Gulf 4 → 7) and Google News query counts (16 → 19);
  - the default-source assertions and the workflow description.
- Seven hand mutants each fail a test.

## Owner
Merge #1103, then this. Nothing to set. Claude dispatches a dry run on main after the merge and reports what each new source returned.

## Rollback
Revert the script to 1.12.1. To read Yahoo again without a revert, add it to `TFB_NEWS_GATE_SOURCES`.

Not a licensed financial adviser; the owner decides and executes every trade.
