# Commit Sheet -- `scripts/news_archive_collector.py` v1.3.4 + `news_archive.yml` v1.3.4 [SOURCE READ REASON + ONE RETRY] -- 2026-10-26

**Scope:** a failed identity page read now says why (type + this module's own reason code) and, when armed, is re-read once after the other pages. ENV-gated; default (retries 0) is byte-identical to v1.3.3 except the two error fields on a failed entry. The workflow arms retries=1 / 45 s.
**Files:** `scripts/news_archive_collector.py` (1.3.3 -> 1.3.4), `.github/workflows/news_archive.yml`, `tests/test_news_adaptive_universe.py` (+6 tests, 18 cases), this sheet.

## S1 -- Defect (owner-supplied artifact `news-acquisition-40238751745`, scheduled slot 25 Oct ~22:00Z)
Codes `identity_source_read_incomplete, identity_coverage_incomplete`, exit 1, while the batch was fetched 60/60 and **60 rows archived**. The artifact's `selection.sources`:
| page | status | named | invalid | publication |
|---|---|---|---|---|
| Market_Leaders / Global_Markets / Commodities_FX / Mutual_Funds / My_Portfolio / Investable Symbol | OK | 255 / 6603 / 453 / 2459 / 1 / 134 | 0 / 7 / 0 / 9 / 0 / 0 | SUCCESS run 40212154238, stable, recent |
| **Top_10_Investments** | **UNAVAILABLE** `identity_source_read_failed` | - | - | OK run 40212154238 20:05:58Z, stable, recent |
The page was intact minutes later (`Rank | Symbol | Name` header at row 18, read back by gviz). The `except Exception:` recorded nothing, so the cause (Sheets API error vs. a header missing during a cockpit render vs. something else) is unknowable from the evidence, and one failed read of one page fails the whole discovery with no second try. The 16 unknown identities (7 Global_Markets: SYDB.CO, 2899.T, ALFAA.MX, KOZAA.IS, SPL.WA, KOZAL.IS, PE&OLES.MX; 9 Mutual_Funds: `SPLG` (bare ticker, a page defect), SPVU.US, HYXU.US, KLD.US, IGN.US, FM.US, NUSI.US, KRMA.US, GXG.US) are the caption gap the v1.3.3 warn policy already tolerates.

## S2 -- Change
- `_safe_error_fields(exc)`: `error_type` + `error_reason` where the reason is one of the module's own codes (`source_unavailable`, `status_header_unknown`, `status_page_ambiguous`, `research_identity_header_unknown`) else `unspecified`; the raw message is never published (the existing "private failure" test still holds).
- `TFB_NEWS_SOURCE_READ_RETRIES` (0 default, max 3) and `TFB_NEWS_SOURCE_READ_RETRY_SLEEP_S` (45 default, 0..180): the page loop becomes a queue; a failed page is appended once per allowed retry and read after the other pages, sleeping first; the final entry carries `attempts` and `failed_attempts[]` (started, type, reason). A page that still fails stays UNAVAILABLE and `source_read_complete` stays false.
- Workflow: both ENVs on the Collect step (Variables override); header WHY.

## S3 -- Not changed
Identity validation, lifecycle exclusions, publication stability/recency checks, the identity-gap policy (v1.3.3), archive writer, failure-code vocabulary, evidence schema (keys added only on a failed/retried entry).

## S4 -- Audits
| Battery | Result |
|---|---|
| `test_news_adaptive_universe.py` (+18 cases), `test_news_workflow_health.py`, `test_news_lifecycle.py`, `test_news_acquisition_evidence.py` | 169 passed |
| Golden negative against the untouched v1.3.3 module | 17/18 fail on the base (the default-off case passes, as it must), all pass on v1.3.4 |
| Default (ENV absent): single attempt, no sleep, UNAVAILABLE with type/reason | asserted |
| Retry 1: transient failure recovers (attempts 2, failed attempt recorded, read last, one 45 s sleep); persistent failure stays UNAVAILABLE after 2 calls | asserted |
| ENV bounds: retries 0..3, sleep 0..180, junk -> defaults | asserted |
| Lint | added lines ASCII; 0 functions removed, +3; compileall clean |

## S5 -- Operator steps
1. Squash-merge; the next scheduled slot (14:00Z / 20:00Z) runs with one retry.
2. If a read still fails, the artifact now names `error_type` / `error_reason` for the page -- that is the next diagnosis.
3. Rollback: Variable `TFB_NEWS_SOURCE_READ_RETRIES=0`.

## S6 -- Known limits
- The caption gap (16 rows above) is reported, not repaired: the `SPLG` bare ticker on Mutual_Funds and the 7 Global_Markets blanks belong to the name-label / universe lane (Tuesday).
