# TFB Comprehensive Audit and Handover, 28 October 2026

**Scope:** a one-time audit, repair and handover across providers, the Python/API service, Apps Script, Google Sheets, the dashboard, and analysis/recommendations.
**Audit window:** 27 Oct 2026, 21:20–21:50 UTC (28 Oct, 00:20–00:50 Asia/Riyadh). Report times are Asia/Riyadh; technical timestamps are UTC.
**Repository baseline:** `main` at `b5f5ff7` (daily_sync.yml v1.0.7, PR #858, merged 27 Oct 20:49 UTC).
**Status labels:** FOUND · PREPARED · APPLIED · TESTED · VERIFIED IN PRODUCTION · BLOCKED · ROLLED BACK

---

## 1. Arabic executive summary

**الحالة الحالية:** خدمة Render تعمل بشكل سليم (الإصدار 8.14.7، فحص الصحة ناجح، ولا توجد أعطال في الخادم خلال سبعة أيام). في المقابل، المزامنة المجدولة الرئيسية «Tadawul Dashboard Advanced Sync» فشلت في 39 من آخر 40 تشغيلًا (14 يومًا). والتشغيل الوحيد الناجح كان إعادة تشغيل يدوية. في جميع حالات الفشل الـ19 التي حُلِّلت، نُسخت البيانات بنجاح، ثم رفض التدقيق النهائي نشر القرار. لذلك ظلت لوحة أفضل 10 فرص في حالة «غير قابلة للتنفيذ» طوال هذه الفترة.

**ما أُنجز في هذه الجلسة:**
1. تحديد السبب الجذري لسلسلة الفشل. فشل طابع الحالة لصفحة Global_Markets أدى إلى عدم تطابق دفعة التشغيل، فحُجبت لوحة Top 10، ثم فشل التدقيق النهائي، ثم فشل أرشيف الأخبار. الإصلاح #858 دُمج قبل هذا التدقيق ولم يُتحقق منه في الإنتاج بعد.
2. إصلاح ملخص الاستثمار اليومي (الإصدار 1.20.0). كان يكتب «قبل افتتاح السوق» في جميع الإرسالات الأربعة، مع أنها كلها تقع بعد افتتاح تداول. وكانت ساعات السوق الأمريكية ثابتة وستصبح خاطئة بعد انتهاء التوقيت الصيفي الأمريكي في 1 نوفمبر 2026. أصبحت الساعات والتسمية تُحسب من المناطق الزمنية الرسمية.
3. أداة التحقق من اللوحة (الإصدار 1.3.4) أصبحت تحتفظ بنص سبب الحجب كاملًا (حتى 1000 حرف) بدل قطعه عند 120 حرفًا.

**أهم مشكلة لم تُحل:** التدقيق النهائي يتطلب إيصال «source readiness VERIFIED (run N)» من لوحة Top 10، لكنه يعمل مرة واحدة فور انتهاء المزامنة. أما اللوحة فتُحدَّث بشكل غير متزامن عبر Apps Script. في 18 من 19 تشغيلًا كانت اللوحة أقدم من مصادرها وقت التدقيق، فلا يمكن للتدقيق أن ينجح بنيويًا. الحل المقترح يتطلب الوصول إلى Apps Script، وهو غير متاح في هذه الجلسة.

**جاهزية التوصيات:** **غير جاهزة للاستخدام التنفيذي.** على 1,026 نتيجة مكتملة لإشارات «INVESTABLE»، بلغت نسبة النجاح 48.4% بمتوسط عائد ‎-0.20% قبل الرسوم. أما الإشارات ذات الثقة المعلنة 85–100 فبلغت نسبة نجاحها 42.5%. ودرجة Brier ‏0.286 أسوأ من خط الأساس 0.25. القرار الحالي للنظام بحجب التنفيذ صحيح ويجب أن يستمر. لا يمكن التحقق من مطابقة المحفظة مع الوسيط لعدم توفر كشف حساب.

---

## 2. Access and baseline (section 3 of the brief)

| Resource | Access in this session | What was inspected | Status |
|---|---|---|---|
| GitHub `emadsaberbahbah-coder/tadawul-fast` | Full (clone + MCP) | Code, 39 workflows, 14 days of run history, job logs, artifacts | Inspected |
| Render `srv-d4hnir15pdvs739bqe1g` (`tadawul-fast-bridge`, Frankfurt, `pro`) | Read via MCP | Service config, deploys, events, logs, metrics, live `/health` | Inspected |
| Google Sheet `1QaOSNHq…G04` ("Market Share Deepseek-V3") | Drive connector: metadata plus a sample of one tab (`_Diag_Response_Shape`). The Sheets API token in this container was rejected (401). | Indirectly, through the CI validator's and the audits' own sheet reads (artifacts) | **BLOCKED** for a direct cell-level audit |
| Apps Script project `1IvEj8sw…` | No Apps Script API access | Repository copies only (`apps_script/*.gs`, 5 files). Deployed-vs-repo consistency, triggers and executions not verifiable. | **BLOCKED** |
| Dashboard `tfb-investment-platform…chatgpt.site` | HTTP 401 ("Sign in required", ChatGPT auth) | Nothing | **BLOCKED** |
| IBKR | Not used, as instructed | — | Not in scope |
| Broker statements | None supplied | — | Reconciliation **BLOCKED** |

**Deployed revisions:**
- Render live deploy `dep-dbg416ajnfac73f71f40` runs commit `f60cff3` (PR #851). It was deployed manually on 27 Oct 06:13 UTC; auto-deploy is **off**.
- `/health` reports `service_version 8.14.7`, `app_version 8.14.5`, 7 route modules mounted, 0 failed.
- `main` is 7 commits ahead (#852–#858). Of the changed files, only `core/providers/calendar_provider.py` is under `core/`. Nothing in `core`, `routes` or `main.py` imports it; it is used by the calendar workflow, which checks out `main`. **The live web service is therefore not behind on any request-path code.**

## 3. Architecture as verified

```
EODHD / Yahoo (yfinance) / other providers
        │
Render FastAPI "Tadawul Fast Bridge" (core/, routes/, main.py)  ── /v1/advisor, /v1/analysis, /sheet-rows, /quote …
        │  (HTTP, authenticated)
GitHub Actions writers (scripts/run_dashboard_sync.py …, 3 cron slots/day) ──► Google Sheet market pages + _Status
        │
Apps Script (10_My_Portfolio, 11_Manual_Refresh_Coordinator, 16_Decision_Top10, 24_Sync_Dispatch, 26_Decision_Chain)
        │  asynchronous cockpit renders: Portfolio_Decision, Top_10_Investments (+ v2 pages)
        │
Final audit (daily_sync.yml → finalize_decision_publication.py + validate_dashboard.py)
        │  promotes or withholds the decision feed (EXECUTABLE / NOT_ACTIONABLE)
        │
Downstream readers: Daily/Weekly Brief e-mail, Decision Digest, Performance Clock, Shadow Board/Scorer,
                    BigQuery archive, news archive, dashboard (ChatGPT site; not inspectable)
```

The authoritative source for each dataset (from the code and the decision policy):
- **Market data:** the provider responses via the Render engine.
- **Page publication:** the `_Status` row with `run=<id>`.
- **Decision eligibility:** the Top-10 cockpit's `source readiness VERIFIED (run N)` receipt, accepted only by the final audit.
- **Holdings and cash:** `My_Portfolio` plus the broker evidence packet. Custody stays *unverified* until the broker evidence is reconciled.
- **SAR conversion:** the SAMA USD peg of 3.75 (`policy://sama-usd-peg-3.75`). Other currencies use provided rates, with static fallbacks dated 2026-08.

## 4. Findings by component

### 4.1 Render / API: FOUND (healthy)
- `/health`, `/healthz`, `/v1/health` and `/` all return 200 in about 0.5 s; `global_auth_enforcement: true`.
- Last 7 days: 20 deploys succeeded; **0** `server_failed` or restart events.
- HTTP over about 24 h (6-hour buckets, from 26 Oct 21:00 UTC): 2,242 × 200, 6 × 401, 13 × 499 (client closed). There were no 5xx responses.
- **Resource pressure: FOUND.** Peak memory was **3.61 GB of a 4.29 GB limit (84%)** in the 27 Oct 15:00 UTC window. The previous instance peaked at 3.51 GB. There was no out-of-memory kill.
- **Provider noise: FOUND.** The logs show recurring yfinance `HTTP 401 Invalid Crumb` and "User is unable to access this feature" errors. They also show "possibly delisted" errors for ETFs such as IGN, IECS, EMBH, JKK, GXG, HWAY, IBDO and DVRE, around 20:30 UTC. Yahoo is a degraded fallback.

### 4.2 GitHub Actions, scheduled sync: FOUND (critical, availability)
- **Scheduled `daily_sync.yml` (🏦 Tadawul Dashboard Advanced Sync): 1 success in the last 40 scheduled runs (14 Oct to 27 Oct).** The single success (39871834683) was a manual *re-run* (attempt 2), not a clean pass.
- In all 19 failed runs examined (21–27 Oct), the failure was in the **"Verify final decision publication"** job. The data-sync legs passed in 18 of 19.

Root-cause classes, with counts out of 19 failed runs:

| Class | Count | In latest 5 runs |
|---|---|---|
| Top-10 never shows `source readiness VERIFIED (run N)` for the current run (`T10_SOURCE_CONSUMPTION_UNPROVEN`) | 19/19 | 5/5 |
| Pool reconciliation unproven ("does not match expected source run" ×12; "cross-page exclusions require individual native producer receipts" ×7) | 19/19 | 5/5 |
| Top-10 older than its source pages at audit time | 18/19 | 4/5 |
| Portfolio_Decision lacks receipt or is older than source (`PF_RUN_STALE` > 8 h in 6) | 13/19 | 1/5 |
| Top-10 status not OK (HELD 2, WITHHELD 4, RUNNING 1) | 7/19 | 2/5 |
| Top-10 status omits pool counts | 3/19 | 2/5 |
| Mutual_Funds declared 2,469 vs approved 2,474 | 4/19 | 0/5 |
| Coverage failure (Market_Leaders 89.8% acquisition after ID-FIREWALL quarantined 24 rows; Mutual_Funds names 97.9%) | 2/19 | 1/5 |
| Global_Markets `_Status` cohort receipt missing | 1/19 | 1/5 (newest) |

- **Newest failure (run 40556442682, 27 Oct 18:03 UTC):**
  - The Global_Markets leg wrote 6,611 rows, but its `_Status` stamp failed: two reads each hit the 60 s socket timeout within a 90 s budget.
  - Global_Markets therefore kept run 40484148059, while every other page carried 40556442682.
  - The cockpit consequently reported `HELD` with "FEED NOT ACTIONABLE — source readiness: collected pool reconciliation unverified: pool receipt cohort unverified; Global_Markets …".
  - **PR #858 (daily_sync v1.0.7)** raises the stamp budget to 300 s. It merged at 20:49 UTC, **after** that run. Status: APPLIED and TESTED by its author; **not yet VERIFIED IN PRODUCTION**, because no scheduled sync had started since the merge at the time of this audit.
- **Structural gap: FOUND.**
  - The final audit runs once, immediately after the writers finish, but the cockpit is asynchronous (Apps Script).
  - In 18 of 19 runs, Top-10 was still showing the *previous* run when audited, so the audit could not pass.
  - The fix requires the audit to wait for, or trigger, the cockpit's consumption of the current run (backlog B2). It touches Apps Script triggers, which are inaccessible here.
- **GitHub cron delay: FOUND.**
  - Every scheduled sync started **2 h 16 m to 6 h 25 m late**: typically about 5 h for the 04:17 and 12:17 UTC slots and 2–3.5 h for 20:17 UTC.
  - The "07:17 Riyadh" refresh therefore actually lands around midday.
  - Against `TFB_DECISION_SURFACE_MAX_AGE_H=8`, this pushes surfaces toward staleness: verdict ages of 334–486 min were observed.
- **news-archive: FOUND (cascade).**
  - 2 of the 4 scheduled runs since v1.3.3 failed (25 Oct 22:08, 27 Oct 19:01 UTC). In both, the news batch was fetched and archived (APPEND_ACCEPTED).
  - On 27 Oct, `identity_source_read_incomplete` was raised only because `Top_10_Investments` was `HELD` with no run receipt, which is downstream of the same cohort failure.
  - On 25 Oct, the Top-10 identity read itself was UNAVAILABLE.
  - The `warn` policy does not tolerate source-read codes, by design.
- Other workflows in the last 60 completed runs were green: Intraday Quote Refresh, Release Decision Feed, Performance Clock, Shadow Scorer, Shadow Board, Decision Digest, Daily Brief, BigQuery Archive, CI and Investment Policy CI.

### 4.3 Google Sheets data quality: FOUND (warnings only), via the validator artifact of run 40556442682 (27 Oct 19:11 UTC)
- Result: **0 FAIL, 14 WARN, 49 PASS.**
- Header contracts matched on every page; there were no duplicate symbols.
- The investability gates held: no INVEST on a SELL recommendation, no BUY with a block reason, and sizing was withheld while the feed was blocked.

The 14 warnings, by page:

| Page | Warnings |
|---|---|
| Market_Leaders | Open blank 1; Open outside the day range 11; price outside the day range 1 |
| Global_Markets | Open blank 95; Open outside the day range 31; price outside the 52-week range **88**; ROI beyond the 12-month soft cap 10 |
| Commodities_FX | price outside the day range 2; Open blank 17; Open outside the day range 11; price outside the 52-week range 4 |
| Mutual_Funds | Open blank 64; Open outside the day range **152**; price outside the 52-week range 13 |

- Page freshness at 19:02 UTC (news-collector read):
  - Market_Leaders, Commodities_FX, Mutual_Funds and My_Portfolio were published by run 40556442682 at 18:05–18:31 UTC.
  - Global_Markets was published by the *earlier* run 40484148059 at 10:45 UTC, which is the cohort split.
- Name coverage gaps: 7 Global_Markets symbols (SYDB.CO, 2899.T, ALFAA.MX, KOZAA.IS, SPL.WA, KOZAL.IS, PE&OLES.MX) and 9 Mutual_Funds symbols (SPLG, SPVU.US, HYXU.US, KLD.US, IGN.US, FM.US, NUSI.US, KRMA.US, GXG.US). Several of the Mutual_Funds symbols are the same ETFs Yahoo calls "possibly delisted"; that lifecycle is not yet registered.

### 4.4 Portfolio and consolidated SAR: FOUND / BLOCKED
- Per the 27 Oct 22:56 Riyadh brief:
  - **1 holding (FRST.US, Primis Financial Corp.).**
  - Book value 9,650 SAR against a 10,072 SAR basis, an unrealized result of **−4.2%**.
  - The holding is `BLOCKED; RECONCILIATION_REQUIRED: missing_evidence | [custody-unverified]`.
- SAR conversion for USD uses the 3.75 peg, with the source disclosed (`policy://sama-usd-peg-3.75`); the code carries a peg guard of [3.74, 3.77].
- Non-USD static fallbacks (EUR 4.10, GBP 4.75, JPY 0.024 …) are dated **August 2026**. They are a safety net behind the sheet's provided rates and their age is not disclosed per row (backlog B9).
- **Broker reconciliation:** cannot be verified. There is no broker statement or export, and IBKR was excluded by instruction. Differences in SAR are **unknown**, not zero.
- Realized P&L, income, fees and currency effects cannot be separated for this audit without the cost-basis and cash ledgers (sheet-level access was blocked).

### 4.5 Recommendations and forecast evaluation: FOUND (material)
Source: the Performance Clock artifact of run 40576271150 (27 Oct 23:33 Riyadh), `track_performance` v6.49.0.

Records:
- 25,037 records in total, of which 12,125 matured and **3,518 decided**. HOLD rows have no win/loss rule.
- `canonical_performance`: **23,166 records with unproven forecast provenance**; 752 with ambiguous units; 81 target/price mismatches; 54 duplicate matured keys.
- **Overlapping samples:** 33,535 signal rows on 8,940 keys. The dedup is *measure-only* (24,595 backlog), so n and the confidence intervals below overstate independent evidence.

| Slice (decided n) | Realized win % [95% CI] | Mean realized ROI (gross) | Stated reliability |
|---|---|---|---|
| INVESTABLE (1,026) | **48.4** [45.4–51.5] | **−0.20%** | 72.5 |
| BUY (820) | 46.0 [42.6–49.4] | −0.44% | 74.3 |
| ACCUMULATE (1,605) | 56.5 [54.1–58.9] | +1.41% | 67.2 |
| Reliability 85–100 (167) | **42.5** [35.3–50.1] | −1.03% | 87.2 |
| Reliability 0–50 (616) | 51.6 [47.7–55.5] | +0.74% | 36.7 |
| Horizon 3M (390) | 39.5 [34.8–44.4] | −1.60% | 68.2 |
| Board executions (9) | 55.6 [26.7–81.1] | +1.72% | 78.8, **insufficient** |

- **Brier score 0.2862 vs a 0.25 coin-flip baseline.** Stated reliability is not calibrated and does not rank outcomes (`monotonic_reliability=false`).
- The tool's own warning: "WATCHLIST outperforms INVESTABLE in this sample — the gate is inverted."
- The October ranking review (PR #847) found the expected-ROI rank runs backwards (Spearman −0.38 over 71 seats).
- Win definitions differ by direction (a SELL "win" is a fall), so BLOCKED (77.4%) and SELL rows are not comparable to buy-side rows.
- The returns above are **before** fees and slippage. Net-of-cost results need broker fills, which are unavailable.
- **The learning infrastructure exists and is sound:**
  - predictions are recorded before outcomes;
  - only completed horizons are evaluated;
  - shadow champion/challenger runs under a pre-registered rule (score_selection_log v1.2.x, A2);
  - calibration factors are advisory and not applied.
- **What it does not yet show is predictive edge.**

**Recommendation readiness: NOT READY.**
- The system currently publishes "No executable new-buy candidates", which matches the evidence. This audit does **not** list qualifying opportunities.
- No candidate passed the final publication audit in the last 14 days, and the measured signal quality does not justify relaxing that.

### 4.6 Daily brief presentation: FOUND → APPLIED
- Every brief printed "before market open" and "Sent before market open", although the four sends (10:30, 13:30, 18:00 and 22:00 Riyadh) all fall after the Tadawul open. The 27 Oct copy went out at 22:56 Riyadh.
- "WHEN YOU CAN ACT" hard-coded US 16:30–23:00 and Europe ~10:00–19:30:
  - US hours become 17:30–00:00 Riyadh when US daylight time ends on **Sun 1 Nov 2026**.
  - Europe's open is already 11:00 Riyadh since UK clocks changed on 25 Oct.
- Fixed in run_daily_brief v1.20.0; see the implementation register.

### 4.7 Manual refresh: BLOCKED
- An end-to-end test of page and overall refresh needs the Apps Script menu (`11_Manual_Refresh_Coordinator.gs`, `24_Sync_Dispatch.gs`) and sheet write access. Neither is available here.
- The repo design (run IDs in `_Status`, cohort receipts, supersede-at-writer-start, single-flight) matches the brief's requirements on paper. It was not exercised.

### 4.8 Dashboard: BLOCKED
- HTTP 401 with ChatGPT sign-in. No page-by-page assessment was possible, and nothing is claimed about its content.

## 5. Implementation register

| # | Defect and root cause | Change | Files | Tests | Status |
|---|---|---|---|---|---|
| I1 | The brief mislabels its send time ("before market open" on all four sends) and hard-codes session hours that break at the US daylight-time change on 1 Nov 2026 | `_send_window(when)` derives the label and the US/Europe hours from Asia/Riyadh, America/New_York and Europe/London for the send date. The footer states that holidays are not checked. Numbers, recommendations and layout are unchanged. | `scripts/run_daily_brief.py` (v1.19.0 → v1.20.0); `tests/test_daily_brief_send_window.py` (new, 15 cases); `tests/test_daily_brief_post_earnings_tag.py` (golden normalizer for the v1.20.0 lines); `.github/workflows/ci.yml` 1.1.50 | 64 passed locally, including the v1.18.1 golden byte-for-byte leg | APPLIED · TESTED (production verification: the first brief after the merge must say e.g. "Sent during Tadawul hours" at 10:30 Riyadh, and from 1 Nov "US 17:30–00:00") |
| I2 | The validator cut the cockpit FEED banner at 120 characters, so the blocking reason in logs, `Dashboard_Audit` and `final_dashboard_validation.json` was truncated mid-word in all 19 failures | Keep up to 1,000 characters (`FEED_BANNER_DETAIL_MAX`). Statuses and exit codes are unchanged. | `scripts/validate_dashboard.py` (v1.3.3 → v1.3.4); `tests/test_validate_dashboard_decision_surface.py` (V8) | 69 passed (validator, scope and final-publication workflow tests); wider sweep of all workflow/brief/validator/CI-pin suites: 671 passed, 4 skipped, 0 failed | APPLIED · TESTED |
| — | Global_Markets `_Status` stamp timeout, which split the run cohort | Pre-existing PR #858 (daily_sync v1.0.7), not authored in this session | `.github/workflows/daily_sync.yml` | Its own harness | Merged; **awaiting first scheduled run** |

**Rollback:** revert the PR's merge commit. Both changes are display/evidence-only, touch no gate, threshold or data write, and need no Render deploy. The brief and validator run from GitHub Actions on `main`.

## 6. Before/after metrics (consistent definitions)

| Metric | Baseline (27 Oct) | After this session | Note |
|---|---|---|---|
| Render availability (/health) | 200, healthy | unchanged | No Render change needed |
| Render 5xx (≈24 h) | 0 | unchanged | |
| Scheduled sync success (last 40) | 1/40 (2.5%) | not yet measurable | Next scheduled runs carry #858 |
| Final audit blocker on latest run | `decision_failed` (cohort) | not yet measurable | |
| Validator result (latest) | 0 FAIL / 14 WARN / 49 PASS | unchanged | |
| Brief send label correct (4 daily sends) | 0/4 | 4/4 in tests | Production check on the next send |
| Blocker banner preserved in evidence | ≤120 chars | ≤1,000 chars | |
| Broker reconciliation difference (SAR) | unknown | unknown | No statement supplied |
| INVESTABLE decided win rate | 48.4% (n = 1,026, overlapping) | — | Not changed by this session |
| Open incidents | Sync red (P0), cockpit deadlock (P0), cron delay (P1), news cascade (P1), memory 84% (P2) | same; I1/I2 closed | |

I do not claim an accuracy or overall improvement percentage.

## 7. Prioritized remaining backlog

| ID | Priority | Item | Impact | Dependencies | Acceptance criteria | Effort |
|---|---|---|---|---|---|---|
| B1 | Critical | Verify #858 on the next two scheduled syncs | Restores the run cohort | none | Global_Markets `_Status` carries the same `run=` as the other pages; the final audit has no `SOURCE_PUBLICATION_COHORT` | 15 min |
| B2 | Critical | Close the cockpit-consumption gap: the final audit must wait for, or trigger, Top-10/Portfolio_Decision consumption of the *current* run before judging | Without it the feed can never become EXECUTABLE on schedule | Apps Script access; `24_Sync_Dispatch.gs`; daily_sync verify job | A bounded poll (e.g. ≤45 min) for `source readiness VERIFIED (run <this run>)`; ≥3 consecutive scheduled runs where the verify job reaches a verdict on current data | 1–2 days |
| B3 | Critical | Keep recommendations fail-closed until evidence shows edge. Make evaluation de-duplicate overlapping signals (one key per symbol per horizon per day) before computing win rates and CIs, and report net-of-cost results. | Prevents acting on a gate that measures as inverted | track_performance dedup decision (pending owner) | A calibration report on de-duplicated keys, with a benchmark (SPUS / sector ETF) and fees; promotion only under the pre-registered A2 rule | 1–2 days |
| B4 | High | GitHub cron runs 2–6 h late. Trigger syncs from a free Apps Script time-driven trigger (workflow_dispatch with a fine-grained token in Script Properties), or a Render cron job (**paid; needs approval**). | Data freshness at decision time | Apps Script access, token | Start delay ≤15 min on 5 consecutive days | 0.5 day |
| B5 | High | news-archive goes red when Top-10 is HELD (no research names to read) | Alert fatigue | owner policy decision | HELD/withheld Top-10 with header verified and 0 names counts as a complete empty read under `warn`; every other source failure still exits 1 | 2 h |
| B6 | High | Broker reconciliation | Portfolio truth in SAR | owner supplies an IBKR statement/export at a cutoff | Positions, quantity, cash and cost basis matched within documented tolerances; differences listed in SAR | 0.5 day after the file |
| B7 | Medium | Render memory peaks at 84% of 4 GB | OOM risk at peak sync | none | Find the peak allocation (likely Global_Markets 6.6k-row payloads); peak ≤70% | 0.5–1 day |
| B8 | Medium | yfinance 401 "Invalid Crumb" plus delisted ETFs in fallback | Noise and slow fallbacks | none | Register lifecycle for IGN/GXG/KLD/… (existing registry); circuit-break Yahoo on 401 bursts | 0.5 day |
| B9 | Medium | Static non-USD FX fallbacks dated 2026-08 have no per-row age disclosure | Possible silent SAR error for EUR/GBP/JPY rows if the provided rate is missing | none | Rows that used a static fallback show the source and age; fallback older than N days → watch-only | 0.5 day |
| B10 | Medium | Validator WARN classes (Open outside the day range: 152 MF / 31 GM; price outside the 52-week range: 88 GM) | Data validity | provider adjustment check | Each class explained (adjusted vs. raw, stale 52-week) or fixed at source | 1 day |
| B11 | Medium | Manual refresh end-to-end test | Section 9 of the brief | Apps Script plus sheet access | Page and full refresh show run IDs, queued→running→success/partial states, no stale overwrite | 0.5 day |
| B12 | Enhancement | Dashboard review | Section 10 of the brief | Dashboard access | Page-by-page check of SAR labels, freshness and empty/stale/error states | 1 day |

## 8. Final verdict

- **Operationally healthy?** **Partly.**
  - The API service is healthy (no failures, no 5xx), with memory headroom as the only concern.
  - The production pipeline is not healthy: the main scheduled sync failed 39 of its last 40 runs, and cron starts are hours late.
- **Financially consistent?** **Not verifiable end-to-end.**
  - USD→SAR uses the documented 3.75 peg with provenance.
  - Data validators pass with warnings.
  - Custody is unverified and broker reconciliation is blocked.
  - Direct sheet, Apps Script and dashboard inspection were blocked in this session.
- **Sufficiently validated for recommendation use?** **No.**
  - The measured hit rate of INVESTABLE/BUY signals is at or below 50%, gross of costs; stated reliability is anti-calibrated at the top band; executed-board evidence is n = 9.
  - The system correctly withholds execution. That should continue until B2 and B3 are complete and the pre-registered shadow evaluation shows a net-of-cost edge.

*Prepared as decision support; not investment advice. No recurring tasks or schedules were created by this audit.*
