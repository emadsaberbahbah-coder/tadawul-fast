# TFB Comprehensive Audit and Handover, 29 October 2026

**Scope:** a one-time audit, repair and handover across providers, the Python/API service, Apps Script, Google Sheets, the dashboard, and analysis/recommendations.
**Audit window:** 28 Oct 2026, 21:15–22:30 UTC (29 Oct, 00:15–01:30 Asia/Riyadh). Report times are Asia/Riyadh; technical timestamps are UTC.
**Repository baseline:** `main` at `729a316` (#871, merged 28 Oct 13:53 UTC).
**Previous audit:** `docs/reviews/comprehensive-audit-20261028.md` (draft PR #860, unmerged and now conflicted). This report builds on it and does not repeat its full architecture section.
**Status labels:** FOUND · PREPARED · APPLIED · TESTED · VERIFIED IN PRODUCTION · BLOCKED · ROLLED BACK

---

## 1. الملخص التنفيذي (عربي)

**الحالة الحالية:** خدمة Render سليمة. تعمل بآخر نسخة من الفرع الرئيسي (`729a316`، الإصدار 8.14.8)، وفحص الصحة يستجيب خلال نحو 0.5 ثانية، ولم تُسجَّل أي أخطاء 5xx خلال 24 ساعة. في 28 أكتوبر نجحت خطوة الإصدار المستقلة «Release Decision Feed» مرتين في رفع القرار إلى «EXECUTABLE»: عند 20:52 و00:09 بتوقيت الرياض. معنى ذلك أن المسار يعمل كما صُمّم، وإن كانت المزامنة المجدولة نفسها تنتهي بالأحمر كل مرة. لكن القرار القابل للتنفيذ لا يتضمن أي تذكرة شراء: «0 تذاكر تنفيذية + 8 مقاعد معلّقة».

**ما أُنجز في هذه الجلسة:**
1. **منع انقطاع متوقع يوم الأحد 1 نوفمبر.** تنتهي صلاحية 124 من أصل 128 اسمًا موثقًا في `verified_name_labels.json` يوم 1 نوفمبر الساعة 14:07 بتوقيت الرياض، وهو يوم تداول. سبق لانتهاء الصلاحية في 19 أكتوبر أن أسقط تغطية الأسماء تحت 99%، فحُجب نشر القرار. شغّلتُ سير عمل التجديد الموجود في المشروع، فجُدد 123 اسمًا حتى 4 نوفمبر وبقيت 5 أسماء محجوزة. التغيير في المسودة **#873** ويجب دمجها قبل 1 نوفمبر.
2. **نقل إصلاح الملخص اليومي.** كان الإصلاح في المسودة #860 التي أصبحت متعارضة مع الفرع الرئيسي، فنقلتُه إلى فرع هذه الجلسة. ملخص 28 أكتوبر الذي أُرسل الساعة 23:09 ما زال يقول «قبل افتتاح السوق»، ويذكر أن أوروبا تفتح الساعة 10:00 مع أنها تفتح الساعة 11:00 منذ 25 أكتوبر. وابتداءً من 1 نوفمبر ستصبح ساعات السوق الأمريكية خاطئة أيضًا (الصحيح 17:30–00:00).
3. **التحقق من إصلاح #858 في الإنتاج.** في مزامنة 28 أكتوبر حملت جميع الصفحات رقم التشغيل نفسه، ولم يتكرر انقسام دفعة Global_Markets.

**أهم مشكلة لم تُحل:** ثلاث عمليات مجدولة تنتهي بالأحمر بحكم التصميم: المزامنة اليومية، وفحص حداثة القرار، وأرشيف الأخبار. السبب أنها تعمل قبل أن تُحدِّث لوحة Apps Script نفسها. وتوجد طلبات دمج جاهزة تعالج جزءًا من ذلك (#831 و#846) لكنها لم تُدمج. كثرة الإنذارات الحمراء الزائفة تُخفي الأعطال الحقيقية.

**جاهزية التوصيات:** **غير جاهزة للاستخدام التنفيذي.** على 1,042 نتيجة مكتملة لإشارات «INVESTABLE» كانت نسبة النجاح 48.8% بمتوسط عائد ‎-0.18% قبل الرسوم. وفئة الثقة المعلنة 85–100 نجحت بنسبة 42.5% فقط. ودرجة Brier ‏0.2859 أسوأ من خط الأساس 0.25. لا توجد فرصة مؤهلة للعرض في هذا التقرير. ولا يمكن التحقق من مطابقة المحفظة مع الوسيط لعدم توفر كشف حساب.

---

## 2. Access and baseline

| Resource | Access in this session | Status |
|---|---|---|
| GitHub repo | Full: clone, `gh` API, MCP. Workflow runs, logs and artifacts read. | Inspected |
| Render `srv-d4hnir15pdvs739bqe1g` | Read via MCP (single workspace owned by the account); live `/health` | Inspected |
| Google Sheet `1QaOSNHq…G04` | Drive connector returns only the first tab (`_Diag_Response_Shape`); the full XLSX export is refused as too large | **BLOCKED** for a direct cell audit; the sheet was audited through the CI audits' own reads (artifacts) |
| Apps Script project | No Apps Script API access; repository copies only | **BLOCKED** |
| Dashboard (chatgpt.site) | Not reachable without ChatGPT sign-in (401 in the previous audit; not retried) | **BLOCKED** |
| IBKR | Not used, as instructed | n/a |
| Broker statements | None supplied | Reconciliation **BLOCKED** |

**Deployed revisions:**
- Render deploy `dep-dbh6gltg1s2s73d292ag` (manual, 28 Oct 21:25 UTC) runs `729a316`, which is `main` HEAD.
- `/health` returns 200 in 0.55 s and reports `service_version 8.14.8`, `routes_failed_count 0`, `global_auth_enforcement true`.
- Auto-deploy is off by design.

## 3. Findings

### 3.1 Render / API: FOUND (healthy)
- **HTTP over the last ~24 h** (6-hour buckets from 27 Oct 21:00 UTC): 3,069 × 200, 6 × 401, 39 × 499 (client closed). There were no 5xx responses.
- **Memory:** the peak was **3.52 GB of 4.29 GB (82%)** in the 28 Oct 15:00 UTC window, the same band as the previous audit (84%). No out-of-memory restart.

### 3.2 Scheduled pipeline: FOUND (structural red, release path working)

**Scheduled `daily_sync.yml`:**
- 1 of the last 20 scheduled runs (22 Oct–28 Oct) ended green, and that one (39871834683, 22 Oct) passed only on a manual re-run attempt.
- In every failed run examined, the failing job was **"Verify final decision publication"**.

**What the final verifier saw on 28 Oct 18:22 UTC** (run 40744567865):
- Coverage exit 0; decision exit 2.
- The decision findings were: Top-10 `HELD`; no `source readiness VERIFIED (run 40744567865)` receipt; Top-10 older than the Global_Markets status; pool counts missing; pool reconciliation unproven ("cross-page exclusions require individual native producer receipts").
- The structural validator found **0 FAIL, 15 WARN, 48 PASS**. The warnings are non-blocking.

**What happened next:**
- The Top-10 cockpit (Apps Script) re-rendered at 22:37 Riyadh, after the verifier had run.
- `Release Decision Feed` then audited the same cohort and **released it at 00:09 Riyadh**: `EXECUTABLE | run=40744567865 | … receipts=T10:40744567865,PF:40744567865 | audits=0,0`, expiring 05:24 Riyadh.
- **The design works end-to-end, but the sync job is red by construction.** This confirms the previous audit's backlog item B2: the verifier runs before the asynchronous cockpit has consumed the run.

**Release outcomes, last 30 `release_decision_feed` runs (23–28 Oct):**

| Outcome | Count |
|---|---|
| released | 9 |
| already released | 1 |
| not accepted (audits failed, correct) | 10 |
| deferred (sync in flight) | 5 |
| **refused `superseded_by_run` (exit 3, red)** | **4** |
| refused, source not completed (exit 3, red) | 1 |

- The red refusals happen when a slot lands inside a newer sync. They write nothing and are not wrong, but they read as alarms.
- Open PR **#831** (`release_decision_feed` 1.2.1, `TFB_RELEASE_INFLIGHT_DEFER`, default off) turns exactly these into deferrals. It is mergeable but behind `main`.

**Source cohorts released, 24–28 Oct:**
- Of 13 sync cohorts, 7 were eventually released to EXECUTABLE.
- The 6 that were not were rejected by the audit (coverage or decision) or superseded.

**PR #858 (`_Status` stamp budget): VERIFIED IN PRODUCTION.**
- The 28 Oct 10:39 and 18:22 UTC scheduled syncs carried one run id across all pages.
- The release receipts name the same run for T10 and PF.
- No `SOURCE_PUBLICATION_COHORT` finding appeared.

**Other red workflows:**
- `Decision Surface Freshness` (17:23 and 19:31 UTC) runs on `workflow_run` of the sync. It therefore sees the same pre-cockpit state and fails with the same 7 findings.
- `news-archive` (19:10 UTC) archived its batch (`APPEND_ACCEPTED`, 42 rows) and then exited red. The reasons were `identity_source_read_incomplete` (the Top-10 was HELD mid-sync) and 16 symbols without a Name (§3.3).

### 3.3 Data quality and identity: FOUND

**Validator, final run of 28 Oct:**
- Header contracts matched on every page; no duplicate symbols; all investability gates passed.
- Warnings are of the same classes as on 27 Oct. Examples: Global_Markets price outside the 52-week range 112 (previously 88); Mutual_Funds Open outside the day range 167 (previously 152).
- Many "Open outside range" rows show **Open = 0.0** (e.g. BMHL.US, GBLI.US, RNRG.US, USCL.US). That is a **null-versus-zero defect at the provider boundary**: a missing Open is written as 0 rather than blank. It does not feed recommendations, but it is a data-validity issue (backlog N4).

**Verified name captions (time-critical): FOUND → APPLIED (PR #873, awaiting merge).**
- 124 of 128 bindings expire **2026-11-01 11:07Z**.
- The daily `name_label_renewal` check has been red every day since 21 Oct. Three bindings lapsed permanently on 19 Oct (PE&OLES.MX, RCM.US, SILK.US), so the red check cannot announce the real cliff.
- Details are in §5, I2.

**Dead identifiers in the approved universe: FOUND (independently checked).**
- **`SPLG` (Mutual_Funds):** the State Street SPDR Portfolio S&P 500 ETF **changed ticker to SPYM effective 31 Oct 2025**. Sources: the State Street AP notice (ssga.com) and the MIAX corporate-action alert of 30 Oct 2025. The row has carried a dead ticker for a year, so it has no name and no fresh price.
- **`FM.US`:** the iShares Frontier and Select EM ETF **was liquidated**; trading ceased 6 Jan 2025 (BlackRock announcement, June 2024).
- RCM.US (R1 RCM) and SILK.US (Silk Road Medical) cannot be renewed because their issuer sources no longer resolve. That is consistent with both companies having been acquired in 2024, but this was not independently confirmed in this session.
- Not confirmed: HYXU, KLD, IGN, NUSI, GXG, SPVU, KRMA (search found no closure notice). These need an issuer or EDGAR check.
- **These rows cannot be removed without an owner decision:** the approved universe scope pins the membership hash (`approved_universe_scope_v1.json`). Backlog N3.

### 3.4 Portfolio and consolidated SAR: FOUND / BLOCKED

From the 28 Oct 23:09 Riyadh brief:
- **1 holding (FRST.US)**: book value **9,582 SAR** against a **10,072 SAR** basis, an unrealized result of **−4.9%**. On 27 Oct it was −4.2%.
- The holding is `BLOCKED; RECONCILIATION_REQUIRED: missing_evidence | [custody-unverified]`.
- USD→SAR uses the SAMA peg of 3.75 with disclosed provenance.

What cannot be done:
- An independent price cross-check of FRST needs the share quantity, which is not in the artifacts read.
- Separating realized P&L, income and fees needs the ledgers (sheet access blocked).
- **The broker difference in SAR is unknown, not zero.**

### 3.5 Recommendations and forecast evaluation: FOUND (unchanged)

Source: the Performance Clock run 40764310766 (28 Oct 23:55 Riyadh), `track_performance` v6.49.0.

Records: 25,349 total; 12,217 matured; **3,550 decided**. The dedup is still measure-only (24,774 backlog), so n overstates independent evidence.

| Slice | Decided n | Win % [95% CI] | Mean ROI (gross) | Stated reliability |
|---|---|---|---|---|
| INVESTABLE | 1,042 | **48.8** [45.8–51.9] | −0.18% | 72.3 |
| BUY | 832 | 46.4 [43.0–49.8] | −0.41% | 74.1 |
| ACCUMULATE | 1,613 | 56.6 [54.2–59.0] | +1.41% | 66.6 |
| WATCHLIST | 2,290 | 55.9 [53.9–58.0] | +0.44% | 54.3 |
| Reliability 85–100 | 167 | **42.5** [35.3–50.1] | −1.03% | 87.4 |
| Board executions | 9 | thin [27–81] | +1.72% | 78.8 (insufficient) |

- **Brier score 0.2859 vs a 0.25 baseline.** Reliability is not monotonic, and the WATCHLIST > INVESTABLE inversion persists.
- The figures are gross of fees and slippage. Net-of-cost results need broker fills.

**Readiness: NOT READY.**
- The released feed carries **0 executable tickets** (8 grace-held, "no plan today"), which is consistent with the evidence.
- **No qualifying opportunity** is listed in this report.

### 3.6 Daily brief presentation: FOUND → APPLIED
The 28 Oct 23:09 brief still printed:
- "before market open" and "Sent before market open";
- "Europe ~10:00–19:30" (the open has been 11:00 Riyadh since 25 Oct);
- "US 16:30–23:00", which becomes wrong on 1 Nov.

The fix existed in #860 but could not merge. It is ported in this PR (§5, I1).

### 3.7 Manual refresh, Apps Script, dashboard: BLOCKED
These are unchanged from the previous audit. They need the Apps Script editor, sheet write access and dashboard sign-in. Nothing is claimed about them.

## 4. Open-PR triage (the owner's merge queue is the bottleneck)

| PR | Fixes | State | Recommendation |
|---|---|---|---|
| **#873** | Name-caption renewal; prevents the 1 Nov coverage cliff | draft, new | **Merge before 1 Nov 11:07Z** |
| **this PR** | Brief send window and US DST (supersedes #860) | draft | Merge before 1 Nov |
| #831 | Release slot inside a sync defers instead of going red | mergeable, behind | Merge, then arm `TFB_RELEASE_INFLIGHT_DEFER=1` |
| #846 | Final publication "consumer-pending" exit (sync stops ending red by construction) | draft, behind | Review: closes most of the structural red |
| #860 | Superseded by this PR | conflicted | Close after this PR merges |
| #872, #857, #845, #843, #841, #820 | Daily-routine improvements, all default off | mergeable | Owner's normal review |

## 5. Implementation register

| # | Defect and root cause | Change | Files and links | Tests | Status |
|---|---|---|---|---|---|
| I1 | The brief labels every send "before market open" and hard-codes US/Europe hours. Europe has been wrong since 25 Oct; US goes wrong 1 Nov. The fix in #860 conflicted with `main` on `ci.yml`. | Cherry-picked #860's two commits (`run_daily_brief` v1.20.0 `_send_window`; `validate_dashboard` v1.3.4, banner kept to 1,000 characters; `verify_deployment` pins). The `ci.yml` header was renumbered 1.1.60, because #859 took 1.1.50 and #778 claims 1.1.56–57. | `scripts/run_daily_brief.py`, `scripts/validate_dashboard.py`, `scripts/verify_deployment.py`, `.github/workflows/ci.yml`, 3 test files; branch `claude/vibrant-bell-qg07ya` | 112 passed, 4 skipped (brief, validator, manifest pins, CI verdict, workflow audit, final publication). Spot check: 1 Nov 10:30 gives "during Tadawul hours · US 17:30–00:00 · Europe ~11:00–19:30". | APPLIED · TESTED. Production check: the first brief after the merge |
| I2 | 124 verified Name captions lapse 1 Nov 11:07Z (7-day review rule); nothing renews them automatically; the daily check is permanently red | Dispatched the existing `name_label_renewal.yml` (`renew`, `publish_branch=true`), run 40772709901: 123 renewed to 4 Nov, 5 held | `data/verified_name_labels.json`, PR [#873](https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/873) (opened by hand; the workflow pushed the branch but cannot open PRs) | The workflow validates with the production loader; renew and publish jobs green | APPLIED (branch and PR), **awaiting merge** |
| V1 | #858 `_Status` stamp budget | — | — | Same run id on all pages in 2 scheduled syncs; released receipts T10 and PF on 40744567865 | VERIFIED IN PRODUCTION |

**Rollback:**
- I1: revert this PR. Scripts run from GitHub Actions, so no Render deploy is needed.
- I2: revert #873.
- Nothing touched a gate, threshold, Render code or the workbook.

## 6. Before/after metrics

| Metric | 28 Oct audit | This audit | Note |
|---|---|---|---|
| Render /health | 200 | 200 (0.55 s), live = `main` | |
| Render 5xx (~24 h) | 0 | 0 | |
| Render peak memory | 84% | 82% | |
| Scheduled sync green | 1/40 | 1/20 (the re-run of 22 Oct) | Structural (§3.2) |
| Sync cohorts released to EXECUTABLE (24–28 Oct) | not measured | 7/13 | via the release workflow |
| Release runs red without fault (superseded or in-flight) | not measured | 5/30 | #831 addresses this |
| Cohort split (Global_Markets stamp) | present | absent in 2/2 | #858 verified |
| Validator (latest) | 0 F / 14 W | 0 F / 15 W | |
| Name captions valid after 1 Nov 11:07Z | — | 4/128 on main; 123/128 with #873 | |
| Brief send label correct | 0/4 | 0/4 live; 4/4 in tests | Production check after merge |
| INVESTABLE decided win rate | 48.4% (n=1,026) | 48.8% (n=1,042) | Overlapping samples |
| Broker difference (SAR) | unknown | unknown | No statement |

I do not claim an accuracy or overall improvement percentage.

## 7. Prioritized remaining backlog

| ID | Priority | Item | Dependencies | Acceptance criteria | Effort |
|---|---|---|---|---|---|
| N1 | Critical | Merge #873 before 1 Nov 11:07Z; re-dispatch the renewal by 4 Nov and then weekly | Owner merge | The 1 Nov and 2 Nov syncs show no Name-floor `coverage_failed` | 10 min/week |
| N2 | Critical | Make the scheduled path green on success: merge #831 and arm its flag; review #846 or an equivalent so the verify job reports "consumer pending" instead of red when only the cockpit lag blocks | Owner review | 5 consecutive scheduled syncs: sync, freshness and release green or neutral whenever the release later succeeds; genuine rejections stay red | 0.5 day |
| N3 | High | Universe hygiene: replace SPLG→SPYM, remove FM.US, review RCM.US, SILK.US, VSME.US, VFLQ.US, HYXU, KLD, IGN, NUSI, GXG, SPVU, KRMA | Owner decision (approved-scope hash change) | Each symbol confirmed from an issuer or EDGAR source; scope file re-approved; name coverage ≥ 99% without captions for dead tickers | 0.5 day |
| N4 | Medium | Missing Open written as 0.0 (null-versus-zero) on GM, MF and CFX rows | none | Open is blank, not 0, when the provider has no open; the validator's `open_in_day_range` count drops accordingly | 2–4 h |
| N5 | Medium | `name_label_renewal` check red forever on held lapsed bindings | Owner policy | The check distinguishes "held, known" from "due within 48 h" so the cliff is visible | 2 h |
| N6 | High | Broker reconciliation | Owner supplies a statement or export at a cutoff | Positions, quantity, cash and cost basis matched; differences in SAR | 0.5 day after the file |
| N7 | Critical | Keep recommendations fail-closed until de-duplicated, net-of-cost evaluation shows edge (previous B3) | Dedup decision | Calibration on de-duplicated keys vs a benchmark, after fees; promotion only under the A2 rule | 1–2 days |
| — | — | Carried from 28 Oct: B4 cron delay, B7 memory, B8 Yahoo noise, B9 FX fallback age, B11 manual refresh, B12 dashboard | see the 28 Oct report | unchanged | — |

## 8. Final verdict

- **Operationally healthy?** **Partly.**
  - The API is healthy, and the audited release path works: two promotions between 28 Oct 17:52 and 21:09 UTC.
  - The scheduled pipeline reports red on every sync by construction. A data-availability cliff on 1 Nov is prevented only if #873 is merged in time.
- **Financially consistent?** **Not verifiable end-to-end.**
  - USD→SAR uses the documented peg with provenance, and the validators pass with warnings.
  - Custody is unverified, broker reconciliation is blocked, and dead tickers sit in the approved universe.
- **Sufficiently validated for recommendation use?** **No.**
  - INVESTABLE/BUY hit rates are at or below 50% gross of costs, the top reliability band is anti-calibrated, and board evidence is n = 9.
  - The system correctly publishes no executable new-buy tickets.

*Decision support only; not investment advice. No recurring tasks, schedules or monitoring were created by this audit. The one workflow dispatch (name-caption renewal) was a single manual run of an existing workflow.*

Sources for §3.3: [State Street AP notice, 31 Oct 2025](https://www.ssga.com/library-content/products/fund-docs/etfs/us/information-schedules/ap-notice/notice-to-aps-ticker-fund-name-and-benchmark-index-name-changes-10-31-25.pdf) · [MIAX corporate action alert](https://www.miaxglobal.com/alert/2025/10/30/miax-exchange-group-options-markets-corporate-action-alert-spdr-portfolio) · [FM liquidation (BusinessWire, June 2024)](https://www.businesswire.com/news/home/20240607459139/en) · [roic.ai FM status](https://www.roic.ai/quote/FM)
