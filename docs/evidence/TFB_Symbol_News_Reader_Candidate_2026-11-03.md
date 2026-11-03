# Symbol news reader and coverage candidate — 3 November 2026

The current report combines provider headlines and news-gate events but hides
their different meanings, source tier, collection time and missing publication
times. It also repeats an article across feeds and daily captures. This candidate
adds a separate reader and a coverage row for each universe/news symbol. The
installed `v_symbol_news` and `v_symbol_rating` definitions remain unchanged.

**Code status:** [draft PR #903](https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/903),
branch `codex/symbol-news-quality-20261103`, based on main
`90b9384108771f9fa366492971bf0c3cc4384018`. The PR remains unmerged. No cloud view
was created, no ingestion job was dispatched, and no backend deployment occurred.
The report now uses read-only custom-query sources as recorded below; that live
report configuration is separate from installing the repository's new views.

## Live report verification — 3 November 2026

The root agent attached whitespace-compacted equivalents of both standalone
SELECTs through the existing report owner's BigQuery Custom Query connector.
The connector accepted both schemas and the sources were added to report
`dcc2db01-69ea-4561-81e1-73622b4d0bea`, news page `p_miszsgex8d`.
[Open the news page](https://lookerstudio.google.com/reporting/dcc2db01-69ea-4561-81e1-73622b4d0bea/page/p_miszsgex8d).
The reader replaced only the old news table's source. This did not create SQL
views or change the installed `v_symbol_news` / `v_symbol_rating` inputs.

The reader's observed BigQuery job link is
[job_ZIBiNLfduXFX9dvCaNNiLOTGD-6I](https://console.cloud.google.com/bigquery?project=project-2e3530e6-2311-4aa3-b36&j=bq:US:job_ZIBiNLfduXFX9dvCaNNiLOTGD-6I).
The Cloud Console itself returned **Site Unavailable** in this environment.
The live rows below establish that the report query executed, but a separate
server dry-run, estimated/actual bytes, billed cost and latency audit remain
unverified. The job link alone is not a cost or freshness receipt.

Viewer checks after reload:

| Selected symbol | Observed reader / coverage behavior |
|---|---|
| `2222.SR` | Zero archived articles, recent linked articles, undated articles and invalid-link articles. An explicit Arabic explanation says collection status is unknown; absence is not presented as proof that no news exists. |
| `ITRN.US` | Three distinct articles: 20 October SEC 6-K and 9 / 8 October Yahoo articles. Three archived and three recent linked articles, zero undated and zero invalid-link articles. |

Gate polarity was blank, while the EODHD provider polarity `1.0` appeared
separately. This is expected: gate event direction and provider text sentiment
remain different measures. No gate polarity is fabricated or blended into a
provider score.

The coverage source's **Date range dimension was removed** to keep its fixed
30-day measure separate from reader-date filtering. The viewer date-change test
passed after reload: with `ITRN.US` selected, native Apply produced the observed
26 October–1 November 2026 range (the UI's Last 7 days relative-clock selection).
The reader changed from three rows to zero; coverage stayed on `ITRN.US` with
three recent linked / three archived / zero undated / zero invalid-link articles,
and its Arabic partial-coverage note remained visible. The symbol selection
persisted. This verifies the tested reader/coverage date separation, not a
global five-minute cross-page symbol reset. Native field-info pop-ups
for `news_date` and `freshness_state` showed Arabic descriptions after reload;
descriptions for `time_basis` and `polarity` were also written. Reader data
freshness persistence is now verified: after report reload, reopening reader
source `ds194` showed **15 minutes**. Coverage source `ds195` initially showed
12 hours; it was set to 15 minutes, saved with Set Data Freshness and Done, and
the exact `ds195` row was reopened to confirm **15 minutes** again. Both source
editors therefore passed the saved/reopened cache-threshold check.
Their internal names still read `symbol_news`; the native chart titles and
Arabic labels distinguish reader and coverage. No source rename is claimed.
This setting controls Looker's query cache. It does not turn the nightly,
partial source collector into a 15-minute acquisition job; refreshing can show
newly archived rows when they exist, not fetch new provider articles by itself.

The news page height is 1500; its coverage table is at top 1200 with height 250,
Arabic labels and wrapped text. All six symbol selectors previously used
Show top 5000, which hid supported `ITRN.US` in viewer search. Raising the limit
to 10000 restored that search. All six selectors now explicitly read back Show
top 10000 in Setup and have Date range dimension removed, preserving page-local
symbol selection. The Overview selector required a second commit on blur before
10000 persisted. A 10000-item cap is still a cap, not unlimited symbol coverage.
No five-minute cross-page symbol-reset integration has been implemented or
verified by this work.

Related presentation checks: the Arabic Guide is the second navigation item,
after Global Overview, and persists after reload. A global help button was
removed after the viewer clipped it. The candlestick title and Arabic price
footer were corrected and persisted after reload; Top 25 / My Portfolio header
and navigation labels were also corrected. These are report configuration
changes, not new reader SQL or pricing-data verification.

These are browser-observed sample and configuration checks reported by the
root agent. They do not establish comprehensive collection, complete article
accuracy, all-symbol filtering, quote freshness or an investment outcome.

## What changes

| Object | Behavior |
|---|---|
| `74_v_symbol_news_research.sql` | One conservative URL identity per symbol, with deterministic representative selection, archive-copy/feed counts, source metadata, safe clickable URL, original publication time, first-seen time and quality states. |
| `75_v_symbol_news_coverage.sql` | One row per universe/news symbol, including symbols without archived news, with recent/stale/undated/future/missing-link counts and an Arabic coverage explanation. |
| `symbol_news_research_query.sql` | Self-contained read-only SELECT for a Looker custom-query source; needs only existing tables/views. |
| `symbol_news_coverage_query.sql` | Self-contained coverage SELECT that embeds the reader; needs no new cloud view. |

URL identity preserves path case, the entire query string and explicit port.
Only scheme, host case and fragments are normalized. Query-tracking variants
therefore remain separate: we prefer a visible duplicate over merging unrelated
articles. Blank URLs fall back to source, normalized title and publication/item
identity, so two different blank-link headlines survive. A reused article URL
may contain revisions; headline/time variant flags reveal that rather than
claiming each capture is independent evidence. Ordering has a full serialized-row
tie breaker, avoiding arbitrary row choice for equal capture times.

Clickable `link` accepts only the supported ordinary-host HTTP(S) form and a
valid numeric port. Credentials, whitespace, control characters, backslashes,
unsupported/malformed authorities and other schemes have a NULL clickable link.
This is a syntax/safety check, not evidence that a URL resolves or its claims are
correct. The raw `original_url` is text for audit, never the report's URL field.
Internationalized hosts should be supplied in punycode; unsupported hosts remain
visible as quality exceptions. Provider-supplied `domain` is preserved separately
from the actual clickable host `link_domain`.

`publisher_name` is NULL because neither input provides reliable publisher-name
metadata. Gate `source` values such as `google_news`, `gdelt` and `edgar` are
collectors and appear as `source_feed`, not as publishers. Gate source tier and
event labels are inherited evidence, not fresh independent verification.

## Looker fields and Arabic labels

| Field | Suggested title / use |
|---|---|
| `symbol`, `symbol_label` | Same symbol and label as `v_universe_latest`; verify actual cross-source filter behavior in Looker. |
| `title`, `link` | عنوان الخبر / رابط المصدر — link URL type only for `link`. |
| `link_domain`, `source_feed`, `source_kind` | نطاق رابط المصدر / قناة الجمع / مسار البيانات. |
| `topic`, `tier`, `severity`, `rumour_denial` | الموضوع / تصنيف المصدر في البوابة / أهمية الحدث / إشاعة أو نفي. Show missing values explicitly. |
| `sentiment_label`, `sentiment_basis` | النبرة أو اتجاه الحدث / أساس التصنيف. Provider text sentiment and gate event direction are different measures. |
| `published_at`, `first_seen_at`, `time_basis` | وقت النشر / أول رصد / أساس التاريخ. A first-seen fallback never populates `published_at`. |
| `display_at`, `news_date` | Display timestamp and Riyadh calendar date for the article table. Undated publication can still have a first-seen display date. |
| `freshness_state`, `link_state` | حالة حداثة الخبر / حالة الرابط. Retain exception rows in a separate quality table. |
| `article_age_hours`, `archive_age_hours` | عمر الخبر بالساعات / عمر آخر نسخة مؤرشفة بالساعات. Never substitute one for the other. |
| `archive_copy_count`, `feed_count` | عدد النسخ المؤرشفة / عدد مسارات الجمع. Neither counts independent confirmations. |
| `has_headline_variants`, `has_publication_time_variants` | تغير العنوان / تغير وقت النشر بين النسخ. |
| `recent_linked_articles_30d`, `coverage_state`, `coverage_note_ar` | أخبار مؤرخة بروابط متاحة خلال ٣٠ يوماً / حالة التغطية / شرح التغطية. |

Suggested visible explanation:

> الأخبار تعرض ما وصل إلى الأرشيف، وليست تغطية شاملة لكل المصادر. وقت النشر
> يختلف عن وقت جمع البيانات. النبرة الإيجابية أو السلبية تصف النص أو الحدث ولا
> تتنبأ بحركة السعر. افتح المصدر واقرأ السياق، وانتبه إلى الإشاعات والنفي.

Use neutral gray for missing/unknown data, amber for quality exceptions, and
positive/negative color only beside the explicit classification and its basis.
Do not color missing news green. A `denial` marker must remain visible beside a
negative headline so the original allegation is not presented as confirmed.

## Empty state and collection limits

The nightly symbol-deep collector requests news for **at most 120 core symbols**,
using a 30-day window and at most 20 returned headlines per symbol/request. Core
selection prioritizes holdings, the board, ROI candidates and extras under that
cap. Provider budget/failure guards can skip work. There is no per-symbol receipt
distinguishing successful-empty, request failure, budget skip, cap exclusion and
not requested. Gate news is another path and does not prove provider coverage.
The provider's documented payload includes article `content` and related
`symbols`; the current archive parser discards both. This reader therefore does
not invent article summaries, fundamental facts or verified entity matching.

Therefore `collection_check_state` is always
`unknown_no_per_symbol_receipt`. `not_observed_in_archive` means no archived
headline, never “no news exists” or “provider successfully checked”.
`no_recent_valid_linked_evidence` means the available rows are old, undated,
future-dated or lack a supported clickable URL. Quality counts can overlap.

Next collection improvement requires a separately reviewed per-symbol receipt
table/run manifest with requested provider symbol, canonical identity,
request/cutoff/collection times, success/empty/error/skip state, item counts and
sanitized error reason. Broader coverage must retain existing budget guards.
That schema and quota change is outside this reader patch.

## Validation and release

- `python tests/test_symbol_news_research.py`: 12 passing behavioral tests.
  They execute the actual SELECT text in SQLite with compatibility functions,
  covering both feeds, duplicates, blank links, distinct query/path/port identity,
  unsafe schemes/authorities/controls, unknown/future times, time boundaries,
  Riyadh dates, stable ties, revisions, absence and query-time aging.
- `python tests/test_bq_views.py`: 28/28. V28 runs the reader tests in the existing
  blocking SQL lane. Both pre-existing news/rating body hashes stay installed.
- `python tests/test_verify_manifest_pins.py`: 3/3.
- `python scripts/apply_bq_views.py --dry-run`: 43 objects, static checks pass,
  **no credentials, no BigQuery server dry-run, nothing created**.
- Workflow audit: zero errors and warnings (two pre-existing runtime-override
  notices). Collector regression could not run in the initial local environment
  because `pytest` is unavailable; ingestion code and workflow are unchanged.
- Independent SQL review caught collector/publisher confusion, dropped URL ports
  and permissive URL syntax. All three are corrected and covered by tests.
  Final independent review reran all 12 reader tests and found no blocking
  static/behavioral defect. The later live samples above supplement these tests;
  they do not replace the pending BigQuery server/cost audit.

Custom-query route: the root agent attached both SELECTs using the existing
owner-credential connection and observed live samples as recorded above. A
separate dry-run and estimated/actual bytes check in
`project-2e3530e6-2311-4aa3-b36.tfb_archive` remains pending because the console
was unavailable. The reader scans archived history before dedup; an outer
Looker filter may not be pushed ahead of its windows. Measure query cost and
refresh latency instead of assuming they are free. No new grant was added.

Keep coverage fixed at 30 days and label it accordingly, **without** the report
date filter. Its Date range dimension was removed and the live ITRN date-change
test above passed. Put date filtering on the article table only. Keep the quality box
outside publication-date filtering so missing/future dates do not disappear.
The live recent-news, zero-row, reader/coverage date separation and saved-cache
checks above are verified. Stale/undated exception behavior is tested offline;
the live sample is not an exhaustive all-symbol validation. Symbol changes must
update both article and coverage panels; source-field names alone do not prove
cross-source filtering works. Five-minute cross-page synchronization remains
unconnected, and collection remains partial as described above.

Optional installed-view route: review/merge first, apply with the existing
controlled BigQuery workflow, and read back all 43 definitions/hashes. This also
applies the four previously pending 1.5.0 objects, so it is broader than adding
two custom-query sources. Do not dispatch it merely to wire the news reader.

Body hashes: reader
`d81c8c6bb264bb17e5b2725626e24e298bc1e6f5dcf68ba68a92d7dae84718d4`;
coverage `2850b2c3d13b4f64bf879dfa60c735d23d0f030d16469a2eae455d0e892c8e85`.
Rollback the new report bindings/custom-query sources only; the old news/rating
source definitions and acquisition/scoring paths remain unchanged.

GoogleSQL references used in review:
[window row ordering](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/numbering_functions#row_number),
[string and regex functions](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/string_functions),
[URL host parsing](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/net_functions#nethost).
Provider contract:
[EODHD Financial News API](https://eodhd.com/financial-apis/stock-market-financial-news-api).
