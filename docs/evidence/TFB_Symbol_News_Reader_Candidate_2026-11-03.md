# Symbol news reader and coverage candidate — 3 November 2026

The current report combines provider headlines and news-gate events but hides
their different meanings, source tier, collection time and missing publication
times. It also repeats an article across feeds and daily captures. This candidate
adds a separate reader and a coverage row for each universe/news symbol. The
installed `v_symbol_news` and `v_symbol_rating` definitions remain unchanged.

**Status:** isolated branch `codex/symbol-news-quality-20261103`, based on main
`90b9384108771f9fa366492971bf0c3cc4384018`. No cloud view was created, no ingestion
job was dispatched, and no live report binding was changed by this patch. A local
test pass is not BigQuery syntax, cost or live-report acceptance evidence.

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
  static/behavioral defect. BigQuery and Looker verification remain pending.

Custom-query route: confirm the existing report really uses
`project-2e3530e6-2311-4aa3-b36.tfb_archive`; dry-run both exact SELECTs in BigQuery,
check estimated bytes and authorized existing-source access, then bind the reader
to the symbol control. The reader scans archived history before dedup; an outer
Looker filter may not be pushed ahead of its windows. Measure query cost and
refresh latency instead of assuming they are free. No new grant is required by
the candidate itself.

Keep coverage fixed at 30 days and label it accordingly, **without** the report
date filter. Put date filtering on the article table only. Keep the quality box
outside publication-date filtering so missing/future dates do not disappear.
Check a recent-news symbol, a stale-only symbol, an undated symbol and a symbol
with zero rows. Changing the master symbol must update both article and coverage
panels; source-field names alone do not prove cross-source filtering works.

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
