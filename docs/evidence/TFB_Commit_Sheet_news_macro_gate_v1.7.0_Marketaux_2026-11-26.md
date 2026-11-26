# TFB Commit Sheet — news_macro_gate 1.7.0 [MARKETAUX] (company news from Marketaux for the first names of the scope)

Date: 2026-11-26 (Thursday) · Lane: GitHub Actions (📰 News & Macro Gate) · Gate: observe-only, unchanged · No Render deploy · No tab or header change

## Why
Owner, 26 Nov 15:21 Riyadh: repository secrets `FINNHUB_API_KEY` and `MARKETAUX_API_KEY` added, after a review of the five news services he sent (14:53).

| Service | Free plan | Verdict |
|---|---|---|
| **Marketaux** | 100 requests a day, 3 articles a request, **no delay**, 80+ markets, entity-matched | built here |
| NewsData.io | 200 credits a day, **12-hour delay**; paid from $199.99 a month | not used |
| NewsAPI.ai | 2,000 tokens once, then paid | later, if Arabic coverage is still thin |
| Massive | 5 requests a minute, US only; overlaps Finnhub | not used |

Marketaux's terms grant personal, non-commercial use — this project's use.

**Finnhub (1.6.0), first run with the secret** (run 45818914155, dry): 93 items in 14 s, newest 22 minutes old, 6 of 17 name items dropped by the issuer rule, nothing failed, no key in the log.

## Change
- New source `marketaux`: `/v1/news/all`, one request per name for the first `TFB_NEWS_GATE_MARKETAUX_MAX` (default **8**, 0–30) scope names it lists — the scope lists holdings first — with `filter_entities`, English, published in the last 72 h.
- **Budget:** 8 requests × about 9 runs a day = 72 of the plan's 100. Past the limit the service answers HTTP 402; the source is named and stopped for that run.
- Marketaux takes the key only as the `api_token` parameter (no header), so — as for EODHD — it is scrubbed from every message before it is written.
- Each request once (12 s), 0.5 s apart; two failures in a row stop the source; 30 s budget.
- Folded, carried and issuer-linked like the other new sources. Its sentiment scores are not read: the gate classifies every headline itself.
- Without the secret: skipped and named.
- Workflow 1.1.4 passes `secrets.MARKETAUX_API_KEY`.

## Tests
`tests/test_news_macro_gate.py`: 297 pass (3 new, G16): parser and error replies, the first N names only, key scrubbed, HTTP 402 named and asked once, a 429 not retried, a confirmed negative item raises WATCH as from any source, no key = skipped. Five mutants (no scrub, unbounded, an error reply accepted, retried, not issuer-linked) each fail a test.

## Owner
Merge when content. The secret is already in place.

## Rollback
Leave `marketaux` out of `TFB_NEWS_GATE_SOURCES`, or revert the script to 1.6.0.

Not a licensed financial adviser; the owner decides and executes every trade.
