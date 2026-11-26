# TFB Commit Sheet — news_macro_gate 1.6.0 [FINNHUB] (the Finnhub key the project already holds now reads news)

Date: 2026-11-26 (Thursday) · Lane: GitHub Actions (📰 News & Macro Gate) · Gate: observe-only, unchanged · No Render deploy · No tab or header change

## Why
Owner, 26 Nov 14:49 Riyadh: the Finnhub key may already exist but be unused. It does: `FINNHUB_API_KEY` is set on Render, where `core/providers/finnhub_provider.py` uses it as the last price fallback (quotes only). No GitHub workflow passes it, and no script reads Finnhub's news.

Finnhub's free plan (personal use; 60 calls a minute, 30 a second) includes **company news for North American issuers** (one year of history and new items) and the **general market-news** feed.

## Change
- New source `finnhub`: `/news?category=general` (market-wide, 24 h window) and `/company-news` per US scope name (from 3 days back, 72 h window). It is one of the new sources of 1.5.0 for folding repeats, the run-to-run memory and the issuer-link rule.
- The key travels in the `X-Finnhub-Token` header, never in a URL, and is scrubbed from every message (the run log is public).
- Each request is asked once (12 s), 1.1 s apart; two failures in a row stop the source; time budget 45 s.
- An item is filed under the publisher of its URL. When the URL is Finnhub's own redirect, the publisher comes from its `source` field when known, else `finnhub.io` (tier D).
- Without the secret the source is skipped and named: `finnhub: FINNHUB_API_KEY not set`.
- Workflow 1.1.3 passes `secrets.FINNHUB_API_KEY` to the run step.

## Owner
Add the repository secret **`FINNHUB_API_KEY`** (GitHub → Settings → Secrets and variables → Actions → New repository secret), with the same value Render holds. Until then the source is skipped and named; nothing else changes.

## Tests
`tests/test_news_macro_gate.py`: 294 pass (4 new, G15): parser (publisher from URL or source, errors raise), key in the header and never in a message, US names only, a refusal named and asked once, no key = skipped and named, a Google copy folded, a list article dropped by the issuer rule. 1,479 tests across the files that touch the gate or the deployment pins pass. Six mutants (key out of the header, no scrub, not folded, not issuer-linked, retried, an error reply accepted as empty) each fail a test.

## Rollback
Leave `finnhub` out of `TFB_NEWS_GATE_SOURCES`, or revert the script to 1.5.0.

Not a licensed financial adviser; the owner decides and executes every trade.
