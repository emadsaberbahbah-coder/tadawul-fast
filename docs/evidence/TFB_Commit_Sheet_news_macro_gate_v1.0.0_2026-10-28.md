# Commit Sheet - `scripts/news_macro_gate.py` v1.0.0 + `news_macro_gate.yml` + taxonomy - 2026-10-28

**Scope:** program register **A7 (news layer), Phase 1**. The owner's request of 28 Oct 00:01-00:15 Riyadh: make the macro and news check that precedes every GO/NO-GO automatic, grade each source before believing it, and record every call so its accuracy can be measured. Use free sources (decision 00:11). Observe-only.

**Files:**
- New: `scripts/news_macro_gate.py`, `data/news_gate_taxonomy.json` (ASCII, Arabic escaped), `.github/workflows/news_macro_gate.yml`, `tests/test_news_macro_gate.py` (49 cases), this sheet.
- Changed: `scripts/verify_deployment.py` (pin 1.0.0) and `.github/workflows/ci.yml` 1.1.50 (lean lane).

## S1 - WHY
- Since the first ticket (26 Oct), every GO/NO-GO has needed a check by hand: war news, energy, the Fed, and each name's own news.
- FRST was bought two sessions after a 22 % EPS miss. The miss was filed with the SEC (8-K item 2.02) on 22 Oct, and no rule looked at it.
- The existing `news_archive_collector` (Google-News RSS and a lexicon score for a rotating 60 names) archives only; nothing reaches the decision.
- The 7 Oct design (`TFB_Forecasting_and_News_Intelligence_Plan_2026-10-07.md`) was never committed. This build carries forward its source grading and its no-influence-before-measured rule, with free sources only.

## S2 - WHAT (one run)
1. **Scope.**
   - Holdings (`My_Portfolio`) come first, then the latest `_Shadow_Rank_Log` day (champion, challenger and board seats), then `TFB_NEWS_GATE_SYMBOLS`. Capped at `TFB_NEWS_GATE_MAX_NAMES` (15).
   - Names come from `Market_Leaders` / `Global_Markets` columns A:B, or from the SEC ticker map.
2. **Collect.** Every source is free or already paid, optional, and fail-open (a failing source is named, never fatal):

   | Source | What | Tier |
   |---|---|---|
   | `edgar` | SEC EDGAR submissions per US symbol: 8-K / 6-K items over 30 days. Needs `TFB_SEC_USER_AGENT` (existing secret); 10 req/s fair-access limit respected. | A |
   | `gdelt` | GDELT DOC 2.0: 5 macro queries (geopolitics, oil, central banks, market stress, Saudi) over 24 h, plus a name query per US / international seat over 3 days. Spaced 6 s, one retry on 429/5xx. | per domain |
   | `google_news` | Google-News RSS: 4 Arabic macro queries (Tadawul, oil / OPEC, the Fed / rates, geopolitics), plus Saudi names in the SA edition. The `<source>` tag gives the real outlet. | per domain |
   | `fed_rss` | Federal Reserve press releases over 3 days | A |
   | `econ` | EODHD economic events, US + SA, 2 days back to 3 days ahead: estimate, actual, previous | - |
   | `market` | EODHD real-time S&P 500 and VIX | - |

3. **Classify and grade.**
   - `data/news_gate_taxonomy.json` holds 23 event types (EN + AR), rumour and denial markers, positive / negative words, and anchors.
   - Arabic anchors map to their English names, so a story told in both languages is one cluster.
   - 8-K items map to event types directly (e.g. 2.02 earnings, 4.02 non-reliance).
   - Source tier: A (official / exchange / regulator / issuer wire) .. F (unknown), by registrable domain or parent suffix.
   - Information grade per cluster: 1 (A source, or 3+ domains including a B) .. 2 (a B, or 2 B/C) .. 3 (single) .. 4 (all rumour) .. 5 (denied) .. 6 (social only).
4. **Verdict** (deterministic, observe-only):
   - **NO_NEW_ENTRY** when any of these holds:
     - S&P 500 <= -3 % on the day;
     - VIX >= 35;
     - a Gulf-anchored high-severity macro story (war escalation, emergency policy, market turmoil) graded 1-2 from >= 3 domains within 24 h.
   - **CAUTION** when any of these holds:
     - a FOMC decision (static 2026-27 schedule, 14:00 ET with DST) or a high-impact US release within the next 30 h or the last 2 h;
     - S&P <= -1.5 %;
     - VIX >= 25, or VIX up 15 % or more on the day;
     - a market-anchored high-severity story graded 1-2 from >= 3 domains;
     - a Gulf-anchored story graded 3-4;
     - confirmed sanctions or credit stress on a market anchor.
   - **GO** otherwise. Russia / Ukraine stories are recorded but do not move the verdict (ongoing background).
   - **Names:**
     - BLOCK: 8-K 1.03 / 2.04 / 3.01 / 4.02 within 30 days, or a confirmed bankruptcy / delisting / restatement story.
     - WATCH: 8-K 1.02 / 1.05 / 2.05 / 2.06 / 4.01 within 10 days, or a confirmed negative story within 72 h.
     - INFO: 8-K 2.02 within 10 days.
5. **Record** (`--write`):

   | Tab | Holds |
   |---|---|
   | `_News_Gate` | One row per run: verdict, drivers, scheduled events, market, flagged names, counts, sources OK / failed. |
   | `_News_Items` | New classified headlines, deduplicated by Item ID, at most 200 per run. Keeps first-seen and published times, tier, grade and cluster, so lead time and confirmation per source can be scored. |
   | `_Econ_Events` | High-impact US releases and all SA releases, upserted with estimate / actual / surprise: the forecast-accuracy record. |

   Evidence JSON and a markdown digest go to the artifact and the job summary.

## S3 - Gate / arming
| Item | Where | Value | Effect |
|---|---|---|---|
| `TFB_NEWS_GATE` | script default | off | no network call, exit 0 |
| | workflow | `${{ vars.TFB_NEWS_GATE \|\| 'observe' }}` | armed: writes only the three new tabs; kill with the repo Variable `TFB_NEWS_GATE=off` |
| schedule | workflow | `41 6 * * 0-4`, `47 12 * * 1-5`, `23 19 * * 1-5` (UTC) | 09:41 Riyadh before the Saudi open, 15:47 before the US open, 22:23 after a FOMC decision (either DST regime). GitHub may start late; each row records its real time. |
| dispatch | workflow | `write` (default false), `sources`, `symbols` | dry run unless `write=true` |

Nothing in the live system reads the new tabs. A rule moves to enforcement only after it is scored and the owner decides.

## S4 - Tests (`tests/test_news_macro_gate.py`, 49 passed x3; offline)
| Group | Proves |
|---|---|
| G1 | Gate off, blank, junk or `true`: no HTTP call, no Sheets call, exit 0. |
| G2 | The taxonomy is ASCII with 23 types and noise checked before legal. EN / AR classification: war, de-escalation, the Fed, earnings direction, law-firm noise, rumour, Arabic denial, Arabic earnings. Short words match whole words only. An Arabic and an English Hormuz story share a cluster. |
| G3 | Every parser on recorded shapes: GDELT; Google-News RSS with source tags and the " - Source" suffix stripped; Fed RSS; the EDGAR ticker map and the 30-day window (a 10-Q and an old 8-K left out); EODHD surprise and importance; EODHD real-time. |
| G4 | Domains and tiers, and information grades 1 to 6. |
| G5 | FOMC day gives CAUTION with "today 21:00 Riyadh". A quiet day gives GO. A Hormuz story from 3 domains gives NO_NEW_ENTRY, and the same story 49 h later gives GO. Russia / Ukraine alone: no change. An unconfirmed Gulf story gives CAUTION. Six market threshold cases. Names: BLOCK (8-K 4.02), WATCH (confirmed negative), INFO (8-K 2.02). |
| G6 | FOMC times: 18:00 UTC in EDT, 19:00 UTC in EST. |
| G7 | End to end against an in-memory workbook: the three tabs get headers; scope = holdings + the latest log day (an older day left out); a GDELT non-JSON reply is named. A second run appends nothing new, upserts the Fed release when its actual arrives (surprise 0.0) and appends a gate row. A dry run writes nothing. A failing source is named and the EODHD key never appears. All sources failing gives exit 1. |
| G8 | `main()`: `--write` without a sheet gives 2; a bad `--now` gives 2; evidence JSON and markdown are written; the job summary is appended; env symbols join the scope; the source filter works. |
| G9 | The module is ASCII; the version pin; the workflow's schedule, dry-run default, read-only permissions and secret mapping. |

Other checks:
- `scripts/audit_repository_workflows.py`: errors 0, warnings 0.
- Manifest pins and workflow suites: 35 passed.
- Live calls cannot be made from the build container (allow-listed network). Live validation is the first dispatch after merge (S6).

### Review fixes (Codex, PR #859)
| Finding | Verified path | Fix | Golden negative |
|---|---|---|---|
| A truce near Hormuz read as war | `escalation` matched inside the normalised `de escalation`, and a bare `hormuz` was a war term; war precedes de-escalation in the taxonomy. | War terms no longer include `hormuz`, `houthi`, `escalation` or Arabic `tasid` / `madiq hormuz`; those are anchors, not events. A war match that also matches de-escalation is de-escalation unless a breach marker (violat / breaks / collapse / despite; Arabic intihak / kharq / raghm) says the truce failed. Arabic `khafd al-tasid` added to de-escalation. | 3 of 4 cases fail on the first push. |
| An article in a macro and a company query lost the company scope | The Item ID was URL-only, so the first scope (macro) won. | The Item ID covers scope + URL: one row per article per scope. | Fails on the first push. |
| `--write` without usable Sheets credentials passed as a quiet dry run (Codex re-review) | `_credentials_info()` returned None, so no session was opened and `run()` skipped the write block. The job stayed green with nothing recorded. | `main()` returns 2 before collecting anything when `--write` has no usable Sheets session; a session that fails to open is reported. | Fails on the previous head. |
| An unrelated denial wiped out a situation; C-only coverage reached NO_NEW_ENTRY | Any denial gave grade 5. Gulf NO_NEW_ENTRY accepted grade 2. | A denial weighs only when denials are at least half the cluster; otherwise the denying items are left out. Gulf NO_NEW_ENTRY needs grade 1 (an A source, or 3+ domains including a licensed wire); grade 2 from 3 domains is CAUTION. | Both fail on the first push. |

Not changed: clusters stay situations (event type + anchor within 24 h), not single stories. Splitting by headline words would separate one incident told in Arabic and English, or in different wording. The 3-domain rule then never fires, which is the more dangerous miss. Several incidents around one Gulf anchor in a day are treated as one situation by design. Phase 3 scoring will measure the false-positive rate. Answered on the review thread.

## S5 - Rollback
Repository Variable `TFB_NEWS_GATE=off` (the next run collects and writes nothing), or delete the workflow. The three tabs can be deleted by hand; nothing else reads them.

## S6 - Operator steps
1. Merge.
2. Claude dispatches `📰 News & Macro Gate` as a dry run, reads the job summary (sources OK / failed, the verdict, the clusters), adjusts a GDELT query if one is rejected, then dispatches with `write=true` once.
3. Scheduled runs take over. From the second week, the scorecard compares verdict days with next-day S&P / TASI moves, and flagged names with their 1 / 5 / 20-session returns (Phase 3).

## S7 - Known limits / next
- Keyword classification, not reading: it labels events and grades sources, but writes no economic narrative. The narrative layer (a scheduled Claude brief citing these rows) is Phase 1b.
- Google-News links are redirect URLs; the outlet comes from the `<source>` tag.
- GDELT and Google-News may throttle GitHub runners. A throttled source is named in the row and the verdict uses the rest.
- Saudi official announcements (Saudi Exchange, CMA, SAMA) are reached through Google-News in this version. Direct feeds come next, if their sites offer a feed that may be read automatically.
- No X / LinkedIn / FT / Bloomberg access (cost or terms; see the 28 Oct source review).

Not a licensed financial adviser; the owner decides and executes every trade.
