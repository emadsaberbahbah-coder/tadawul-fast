# TFB comprehensive audit, repair and handover (21 October 2026)

Window: 2026-10-20 21:20 to 22:30 UTC (00:20 to 01:30 Riyadh, 21 Oct).
Base: `97fa991` (HEAD of `main`, also the live Render deploy `dep-dbbn3au7bikc73949a6g`, app 8.14.5).
Branch / PR: `claude/vibrant-bell-3ic3vs`, draft PR https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/809 (commit `ae95309`).
PDF copy: `docs/reviews/TFB_Comprehensive_Audit_2026-10-21.pdf`.
No workbook cell, Render setting, environment variable, deploy, provider or order was touched. IBKR was not used
(per the assignment); broker reconciliation is therefore **not verified** in this report.

Status labels: FOUND · PREPARED · APPLIED · TESTED · VERIFIED IN PRODUCTION · BLOCKED · ROLLED BACK.
"APPLIED" here means committed to the PR branch; nothing in this change set is in production until the owner
merges the PR and deploys (Render auto-deploy is off) and installs the two Apps Script files.

---

## 1. الملخص التنفيذي (العربية)

**الحالة الحالية:** الخدمة على Render تعمل (`/health` سليم، الإصدار 8.14.5 = آخر commit على `main`)، لكن خط
المزامنة اليومي لم ينجح في أي تشغيل مجدول منذ 10 أكتوبر (22 من 22 تشغيلاً فشلت بين 13 و20 أكتوبر). أسباب
الفشل اليوم: صفحة Top_10 عالقة على حالة "running…" بلا جدول منذ نحو 16:20 UTC، وانخفاض تغطية الأسماء في
Global_Markets وMutual_Funds تحت حد 99%، وصفحات القرار أقدم من بياناتها المصدرية. كما سُجّلت 4 أعطال
(SIGSEGV) لعمّال الخادم خلال 7 أيام عند استهلاك ذاكرة قارب 95% من الحد.

**ما أُنجز (في PR بانتظار الدمج):**
- إصلاح خطأ مالي جوهري: أوزان My_Portfolio كانت تحوّل أسعار البنس (GBp) بسعر الجنيه، فتضخّم الوزن 100 مرة
  (93.07% بدلاً من 11.85% في المثال المختبر). أصبح الحل يكشف العملات الفرعية قبل التحويل، ويُفصح عن أسعار
  الصرف التقديرية في عمود الحالة.
- Apps Script: قارئ سجل التكلفة في Portfolio_Decision كان يحتسب الدفعات المباعة/المغلقة؛ وقيم Top_10 كانت
  تحوّل البنس بسعر الجنيه وتعرض القيمة بالعملة الأصلية على أنها ريال عند غياب سعر الصرف؛ وأي خطأ في التحديث
  يترك الصفحة على "running…". الثلاثة أُصلحت مع اختبارات.
- المزامنة: فشل قراءة "آخر قيمة سليمة" كان يُسجَّل كأنه "لا توجد قيمة سابقة" (149 صفاً فارغاً في
  Global_Markets). أصبح يُعاد مرة ويُسمّى الفشل صراحة.
- حلقة التعلّم: احتساب الربح/الخسارة كان يتجاهل اتجاه التوصية (توصية بيع ارتفع سعرها كانت تُحتسب ربحاً)،
  وكانت نسبة شارب بوحدات خاطئة (13.42 بدلاً من 2.54)، ومعدل الإصابة بمقام مختلف، ومعدل الدوران في
  المُقيِّم الظلي يتجاهل الشراء. كلها أُصلحت.

**أهم مسألة لم تُحل:** لا يوجد تقييم فعلي لنتائج التوقعات في الإنتاج (النضج يتم فقط بتشغيل يدوي)، والسجلات
التي فاتها يوم الاستحقاق تبقى معلّقة للأبد. لذلك لا تتوفر حتى الآن أدلة كافية على دقة التوصيات.

**جاهزية التوصيات:** **غير جاهزة للاستخدام كقرار استثماري.** خط المزامنة أحمر، وصفحات القرار متأخرة عن مصادرها،
وتسوية المحفظة مع الوسيط غير مُتحقق منها، ونتائج التوقعات غير مُقيّمة. تُستخدم المخرجات كبحث فقط إلى أن تُستوفى
شروط القسم 8.

**إجراءات مطلوبة من المالك (بالترتيب):** (1) إعادة تشغيل `refreshDecisionTop10` ومراجعة سجل التنفيذ لتشغيل
16:20 UTC؛ (2) ضبط `TFB_SEC_USER_AGENT` وتشغيل تجديد أسماء الأدوات قبل 21 أكتوبر 07:44 UTC؛ (3) تحديث
My_Portfolio بإزالة المركزين المغلقين (إن لم يُحدَّث بعد)؛ (4) مراجعة الـPR ودمجه ثم النشر وتثبيت ملفي Apps Script.

---

## 2. Baseline (before any change)

### 2.1 Access

| Resource | Status | Evidence |
| --- | --- | --- |
| GitHub repo | ACCESSIBLE | clone at `97fa991`; Actions runs and logs read via the GitHub connector |
| Render `tadawul-fast-bridge` | ACCESSIBLE (read) | service, deploys, events, metrics, app logs (workspace id passed per call) |
| Google Sheet `1QaOSN…r2HwsG04` | PARTIAL | Drive metadata readable (last modified 2026-10-20 20:54 UTC / 23:54 Riyadh); content read returned only `_Diag_Response_Shape`; xlsx/csv export "not implemented"; no Sheets API credential in this environment |
| Apps Script project `1IvEj8…2-` | BLOCKED | Drive: "Requested entity was not found" (bound project not reachable); repo `apps_script/` used instead |
| Dashboard `tfb-investment-platform…chatgpt.site` | BLOCKED | HTTP 401 "Sign in required" (ChatGPT login); front-end source not in this repo |
| IBKR | NOT USED | per assignment |

### 2.2 Architecture as verified

Providers (EODHD, Yahoo chart/fundamentals, others) → FastAPI service on Render (`main.py`, `core/data_engine_v2.py`,
`routes/*`) → GitHub Actions `daily_sync.yml` (`scripts/run_dashboard_sync.py`) writes the market pages and My_Portfolio
→ Apps Script producers (`10_My_Portfolio.gs` → Portfolio_Decision, `16_Decision_Top10.gs` → Top_10 cockpit,
`26_Decision_Chain.gs`, `24_Sync_Dispatch.gs`, `11_Manual_Refresh_Coordinator.gs`) → audits
(`audit_full_refresh_coverage.py`, `audit_decision_surface_freshness.py`, `finalize_decision_publication.py`,
`validate_dashboard.py`) → the dashboard reads the Sheet through the restricted relay
(`routes/platform_source.py`, `integrations/platform_source_read.py`).

Authoritative sources: holdings and cost basis = My_Portfolio + `_Portfolio_CostBasis` ledger (owner data);
SAR money on the decision surface = `core/analysis/portfolio_actions.py` with FX from
`opportunity_builder._resolve_fx`, gated by `core/portfolio_reconciliation.certify_portfolio_inputs`;
My_Portfolio Value/Cost/P&L stay in native currency, with SAR-converted weights from
`data_engine_v2._compute_portfolio_fields`; Top_10 SAR holdings from `16_Decision_Top10.gs`.

### 2.3 Baseline metrics (denominators and periods stated)

| Metric | Value | Source / period |
| --- | --- | --- |
| Dashboard Advanced Sync, scheduled runs | **0 / 22 succeeded** | 13 Oct 17:17 to 20 Oct 17:17 UTC; last success 10 Oct 10:40 UTC (run 38045703392) |
| Provider Target Coverage Alert | 0 / 5 succeeded | 16 to 20 Oct |
| Render `/health` | 200, healthy, 0.56 s | 20 Oct 21:2x UTC |
| HTTP status mix | 200: 1,563 (97.6%); 499: 29; 502: 6 (all before the 13:52 deploy); 401: 4 | 24 h to 20 Oct 21:30 UTC, n = 1,602 |
| HTTP latency p50/p95/p99 | **not available** (Render returns empty latency series; no request logs) | — |
| Worker crashes (gunicorn code 139) | 4 | 7 days; memory at 95% of the 4.29 GB limit before the 20 Oct 09:30 crash; 87% at 21:25 UTC |
| identity_guard refusals | Mutual_Funds 71, Global_Markets 14, Commodities_FX 13 | 18 Oct 16:03 to 20 Oct 17:47 |
| Name coverage (floor 99%) | Mutual_Funds 97.90%, Global_Markets 97.84% | evening sync, 20 Oct |
| Decision surface | Top_10 status RUNNING (since ~16:20 UTC), PF 10:40 Riyadh vs My_Portfolio 20:19 Riyadh | Decision Surface Freshness run 39474537234 |
| Test suite | **5,355 passed, 36 skipped, 0 failed** | local, pinned requirements, Python 3.11 |
| Open PRs | 30 (4 drafts), oldest 6 Oct | GitHub |

---

## 3. Findings by layer

### 3.1 Scheduled workflows (FOUND)

1. **Top_10 cockpit stuck RUNNING** — the run started ~16:20 UTC never finished; the page holds 16 rows and no
   grid. `validate_dashboard.py:985-1012` cannot find the `Rank|Symbol` header, and because every sync leg validates
   Top_10, all three legs of run 39464797610 went red although each sync wrote cleanly (Global_Markets 6,613 rows,
   exit 0). Root cause in code: `refreshDecisionTop10()` wrote `running…` first and had no failure path.
   → fixed in `16_Decision_Top10.gs` v1.13.10 (§6, item 3). The 16:20 run itself needs an owner re-run.
2. **Global_Markets name floor (new)** — `[ID-FIREWALL]` quarantined 149 rows and `[FW-KEEP]` restored 0/149
   "no last-good"; the morning run restored 134/135. The same leg logged `OHLC-READBACK … UNAVAILABLE (read_failed)`.
   Root cause in code: `SheetsWriter.read_values` returns `None` on any API error and `_keep_last_good_rows`
   treated it as an empty sheet. → `run_dashboard_sync.py` v6.70.1 (§6, item 4).
3. **Mutual_Funds name floor** — the reviewed captions expired 19 Oct 12:31 UTC (known; 16 Global_Markets captions
   also expired then, 67 more expire **21 Oct 07:44 UTC**). Owner action: `TFB_SEC_USER_AGENT` + renewal dispatch.
4. **Decision freshness** — `PF_RUN_STALE`, `PF/T10_OLDER_THAN_SOURCE`, `T10_POOL_RECONCILIATION_UNPROVEN`,
   consumption receipts unproven. Owner action: re-run both producers after the sync; install `26_Decision_Chain`.
5. **Provider Target Coverage** — deadlock: the workflow only saves a baseline on exit 0 and cannot exit 0 without
   one (`baseline_at_utc 2026-08-18`). Owner action: dispatch with `bootstrap=true` after a healthy sync.

### 3.2 Render (FOUND)

- Healthy startup at 13:52 UTC (7 route modules, 110 signatures, no startup warnings). CPU peak 0.80 of 2 cores.
- **Memory is the operational risk**: 4 worker SIGSEGV (code 139) in 7 days; a single
  `POST /sheet-rows/opportunity-candidates` at 20:24 UTC added ~1.39 GB that was not released. Not a Render
  `server_failed` event, so it is invisible in the event feed. Not fixed here (needs profiling; see backlog B2).
- Commodities_FX: Yahoo returns 404 for `GRT-USD`, `COMP-USD`, `UNI-USD`, `SUI-USD`, `IMX-USD`, `STX-USD`, `APT-USD`.
  Verified on 20 Oct that Yahoo serves them as `GRT6719-USD`, `COMP5692-USD`, `UNI7083-USD`, `SUI20947-USD`,
  `IMX10603-USD`, `STX4847-USD`, `APT21794-USD` (names match). A fixture shows bare `UNI-USD` once resolved to an
  unrelated "UNICORN Token". The project already carries a native EODHD crypto route (`TFB_EODHD_CRYPTO_MAP`,
  default off); not changed here (backlog B6).
- Mutual_Funds refusals are stale-universe rows (matured iBonds, renamed ETFs: IBHA, IBDO, IBMJ, IBTD, KLD, IGN, SPLG…)
  — a universe problem, not a guard defect (backlog B7).

### 3.3 Financial logic (FOUND → APPLIED where marked)

| # | Defect | Severity | Status |
| --- | --- | --- | --- |
| F1 | `data_engine_v2._compute_portfolio_fields` upper-cased `GBp` to `GBP` before the FX lookup: pence valued at the pounds rate, weight 100x (93.07% vs 11.85%), drives REDUCE/ADD | Critical | APPLIED, TESTED |
| F2 | `16_Decision_Top10.gs dt10HoldingFromRow_`: same subunit bug, and a missing FX rate kept the native amount labelled `value_sar` (HKD 10,000 → "10,000 SAR") feeding budget base and sector caps | Critical | APPLIED, TESTED (needs install) |
| F3 | `10_My_Portfolio.gs _myPortfolioLedgerMap_` counted Sold/Closed lots (qty 20 / cost 90.5 instead of 10 / 100.5) | High | APPLIED, TESTED (needs install) |
| F4 | `portfolio_fx_status` read `OK:SAR` even when built-in INDICATIVE (floating) rates were used | Medium | APPLIED, TESTED |
| F5 | Missing price + carried stale `position_value` still drives weights / REDUCE (`TFB_MP_BLOCKED_NULLS` default off) | High | FOUND (backlog B4) |
| F6 | P&L % unit contract disagrees: schema says fraction, engine and sync write percent points | Medium | FOUND (backlog B5) |
| F7 | Two built-in FX tables disagree (GBP 4.80 vs 4.75, EUR 4.05 vs 4.10) | Medium | FOUND (backlog B5) |
| F8 | Weight denominators differ by design (My_Portfolio holdings-only, Portfolio_Decision holdings+cash) | Low (definition) | FOUND, documented here |
| F9 | Realized P&L, income/dividends, fees and FX effects are not separated anywhere; cost is converted at today's FX, so SAR P&L includes currency moves | High (reporting gap) | FOUND (backlog B8) |

Consolidated SAR today: the certified Portfolio_Decision path is subunit-safe and fails closed without a fresh
evidence packet; it is the only SAR money surface and is BLOCKED by default without broker evidence
(`PD_IBKR_EVIDENCE` off). Per-share entry/stop/target stay in the trading currency.

### 3.4 Recommendations and learning loop (FOUND → APPLIED where marked)

| # | Defect | Status |
| --- | --- | --- |
| L1 | Maturation runs only on a manual dispatch with `run_audit=true`; a record whose target session was missed can never mature (only `price_unavailable` expires) | FOUND (backlog B1, highest learning priority) |
| L2 | Win/loss ignored the call's direction (two SELLs that rose counted 100% wins) | APPLIED, TESTED (`track_performance` v6.46.0) |
| L3 | Entry-sanity check vs the median of all prior entries (±35%) locks a symbol out after any large move or bonus issue → survivorship | FOUND (backlog B3) |
| L4 | Sharpe/Sortino treated 7-90-day percent returns as daily fractions (13.42 vs 2.54) | APPLIED, TESTED |
| L5 | Hit rate by horizon included breakevens in the denominator (25 vs 50) | APPLIED, TESTED |
| L6 | Top_10 page fills to 10 from criteria-failing pools (labelled `T1/T2 BACKFILL[…]` in the selection reason; the tracker does not persist the tier) | FOUND (backlog B9) |
| L7 | Shadow-scorer turnover/cost counted exits only (adding a name cost 0) | APPLIED, TESTED (`run_shadow_scorer` v1.9.4) |
| L8 | Scorer's "last daily close" can be an intraday bar (cron 15:20 UTC) | FOUND (backlog B10) |
| L9 | Matured duplicates counted twice by default (`TRACK_DEDUP_MATURED` off) | FOUND (backlog B10) |
| — | Tracker: no fees/slippage, no benchmark, no model/rule version on records; horizons are calendar days vs policy trading days | FOUND |

Forecast performance: **not reported**. Matured observations are produced only by manual audits and include the
direction and duplicate defects above, so any accuracy figure from the current log would be misleading.

### 3.5 Dashboard and Sheet (page-by-page)

The dashboard is BLOCKED (login), so a visual page-by-page assessment was not possible. From the relay code
(`integrations/platform_source_read.py READ_LIMITS`): the dashboard can read `_Status`, `Top_10_Investments`,
`Portfolio_Decision`, `My_Portfolio`, `_Portfolio_CostBasis`, `_Cash_Snapshot`, `Daily_Review_2026_10_14`, plus up to
20 rows of `Market_Leaders`/`Global_Markets`.

- `Portfolio_Decision_v2` / `Top_10_Investments_v2` are **not readable** by the relay (FOUND, backlog B11).
- The daily-review tab is pinned to `Daily_Review_2026_10_14` (FOUND, backlog B11).
- `_Diag_Response_Shape` reports `INCOMPLETE-COLUMNS` for Global_Markets (5 concerning blank columns).
- Whether My_Portfolio still lists the two US positions closed on 19 Oct could not be checked (Sheet content not
  readable here). `docs/apps-script-source-prerequisites.md` is stale (lists `10_My_Portfolio.gs` as unavailable).

---

## 4. Implementation register

| # | Root cause | Change | Files | Tests | Status |
| --- | --- | --- | --- | --- | --- |
| 1 | F1, F4 | resolve subunit token case-sensitively before FX (`_pf_subunit_major`), divide by 100; unresolvable subunit falls into the existing whole-book `PORTFOLIO_FX_INCOMPLETE` fail-closed path; status names INDICATIVE rates | `core/data_engine_v2.py` 5.154.0 → **5.155.0** | `tests/test_pf_weight_subunit_fx.py` 6 passed; 4 of 6 fail on the old engine (golden negative) | APPLIED, TESTED |
| 2 | F3 | ledger reader excludes `inactive`/`closed`/`sold` (blank = active, as `ledger_symbols`) | `apps_script/10_My_Portfolio.gs` 1.5.13 → **1.5.14** | `tests/js/gas_ledger_fx_failed_state.test.js` 10/10; `tests/test_gas_ledger_fx_failed_state.py` 3 passed | APPLIED, TESTED; install = owner |
| 3 | F2 + stuck RUNNING | `dt10HoldingFxRate_` (subunit-safe, mirrors `_resolve_fx`); missing rate → `value_sar: null` + `_input_incomplete`; `refreshDecisionTop10` wraps the body and writes a terminal `status: FAILED | failed_at_utc=… | <error>`, `_Status` ERROR, clears the run marker, rethrows (all three steps individually guarded). A hard 6-minute Apps Script timeout cannot be caught. | `apps_script/16_Decision_Top10.gs` 1.13.9 → **1.13.10**; JS fingerprints/counts in `tests/js/*` | same JS suite; all 10 JS suites green | APPLIED, TESTED; install = owner |
| 4 | last-good read failure misreported | one retry (3 s) on `read_values` = None; `KLG-READ` warning; FW-KEEP says "last-good READ FAILED" | `scripts/run_dashboard_sync.py` 6.70.0 → **6.70.1** | `tests/test_sync_klg_read_retry.py` 3 passed | APPLIED, TESTED |
| 5 | L2, L4, L5 | direction-aware outcome (`_directional_outcome`; HOLD → `neutral_records`), per-horizon Sharpe/Sortino in fraction units, hit rate = W/(W+L). The Performance_Log `Outcome` column and ROI values are **unchanged** | `scripts/track_performance.py` 6.45.0 → **6.46.0** | `tests/test_learning_metric_fixes.py` 37 passed | APPLIED, TESTED |
| 6 | L7 | one-way turnover = Σ|Δw|/2; cost charged on buys and sells (full swap still 0.36%) | `scripts/run_shadow_scorer.py` 1.9.3 → **1.9.4**; pins in `tests/test_s1_*`, `tests/test_shadow_scorer_regret_read_failure.py` | same file + existing scorer suites | APPLIED, TESTED |
| 7 | CI | enrol the new suites in the blocking lanes | `.github/workflows/ci.yml` 1.1.27 → **1.1.28**; `scripts/verify_deployment.py` pins | — | APPLIED |

Rollback: every change is a version bump on one file with its predecessor in `97fa991`; revert the PR commit (or
re-install `10_My_Portfolio.gs` v1.5.13 / `16_Decision_Top10.gs` v1.13.9). No data migration, no new flag, no new
dependency, no cost.

Behaviour boundaries to note in the register: from the deploy day, (a) Shadow_History cost drag charges buys
(historical rows untouched), (b) tracker win rates and calibration buckets score SELL-family calls by direction and
exclude HOLD, (c) My_Portfolio Actual Weight % of `.L`/JSE/TASE holdings drops by 100x to the correct value.

Production verification: **not possible in this run** — the PR is not merged and Render auto-deploy is off.
After deploy, verify: `/health` app/engine versions; one My_Portfolio refresh showing `portfolio_fx_status`; the
next sync log containing `KLG-READ` only on a real read failure; one manual `refreshDecisionTop10` ending in OK.

### Final verification (integrated tree)

- Whole suite: **5,404 passed, 36 skipped, 0 failed** (baseline at `97fa991`: 5,355 passed, 36 skipped, 0 failed; +49 new tests).
- All JS suites under `tests/js/` and `tests/test_dt10_calendar_completeness.js` pass; `apps_script_syntax` compiles
  the 5 sources.
- `scripts/audit_repository_workflows.py --fail-on-warnings`: same 2 pre-existing `daily_sync.yml` warnings as `main`.

---

## 5. Before / after

| Measure | Before | After (code) | Production |
| --- | --- | --- | --- |
| VOD.L weight in the F1 example | 93.07% | 11.85% | after deploy |
| Ledger qty/cost in the F3 example | 20 / 90.5 | 10 / 100.5 | after install |
| HKD holding without a rate on Top_10 | 10,000 "SAR" | null + funding withheld | after install |
| Top_10 status after a thrown error | RUNNING forever | FAILED with UTC time and error | after install |
| Two SELLs that rose +5%/+3% | 100% win rate | 0% | next tracker audit |
| 1M Sharpe on [2,-1,3,1]% | 13.42 | 2.54 | next tracker report |
| Basket {A} → {A,B} turnover / cost | 0 / 0 | 50% / 0.18% | next scorer run |
| Sync success rate | 0 / 22 | unchanged until the owner actions in §7 are done | — |

No overall accuracy or improvement percentage is claimed.

---

## 6. Prioritized remaining backlog

| ID | Item | Impact | Depends on | Acceptance criteria | Effort |
| --- | --- | --- | --- | --- | --- |
| B1 | Schedule the tracker audit; mature a missed target session from the historical daily close (or EXPIRE with a reason) | Critical for learning | none | scheduled audit runs daily; no ACTIVE record older than target + 5 sessions | M |
| B2 | Profile `/sheet-rows/opportunity-candidates` memory; as a stop-gap set `TFB_MAX_REQUESTS`/`_JITTER` on Render | High (crashes) | owner for env | 7 days without code 139; memory < 80% | M |
| B3 | Entry-sanity check against a rolling window / independent quote / corporate-action adjustment | High (survivorship) | none | a 40% move does not exclude future picks | S |
| B4 | Null price-derived fields and suppress book weights when a held price is missing (arm `TFB_MP_BLOCKED_NULLS` after a dry run) | High | none | missing price never produces a weight or REDUCE | S |
| B5 | One P&L % unit across schema/engine/sync/audit; one shared FX table with source and as-of on the sheet | Medium | owner decision | audit_workbook no longer flags valid rows; FX source/time visible | M |
| B6 | Crypto identity on Commodities_FX: arm `TFB_EODHD_CRYPTO_MAP` after a dry run, or add the verified Yahoo IDs | Medium | dry run | 0 quarantines for the 7 tokens | S |
| B7 | Retire matured/renamed Mutual_Funds symbols via `universe_maintenance` | Medium | owner review | no `pre_existing_blank_shell` refusals | S |
| B8 | Separate realized P&L, income, fees, FX effect and external flows; historical-FX cost basis | High (reporting) | broker statements | reconciled SAR bridge per position | L |
| B9 | Persist Top_10 tier/backfill flag on tracker records; publish "No qualifying opportunity" when no row passes | Medium | none | backfill outcomes separable | S |
| B10 | Completed-session check in the scorer's `_last_bar`; arm `TRACK_DEDUP_MATURED` | Medium | none | no intraday close; duplicates counted once | S |
| B11 | Relay: v2 decision tabs and a non-dated daily-review tab (security review first) | Medium | owner | dashboard shows v2 pages | S |
| B12 | Relabel the Performance_Log `Outcome` column by direction behind a gate (calibration_report, tfb_backtest read it) | Medium | B1 | backtests score SELL calls by direction | M |
| B13 | Fees/slippage and a TASI/SPUS benchmark in the tracker; trading-day horizons per policy §7.1 | Medium | B1 | net-of-cost results vs benchmark with CIs | M |
| B14 | Triage the 30 open PRs (oldest 6 Oct); several overlap this area | Medium | owner | ≤ 10 open PRs with a decision each | M |

---

## 7. Owner actions (cannot be done from this session)

1. Apps Script Executions: inspect the ~16:20 UTC `refreshDecisionTop10` failure; re-run Top_10 and Portfolio_Decision.
2. Before **21 Oct 07:44 UTC**: set repository variable `TFB_SEC_USER_AGENT`, dispatch `Verified Name Label Renewal`
   with `mode=renew`, commit the renewed registry.
3. My_Portfolio: confirm the two US positions closed on 19 Oct are removed.
4. Review and merge the PR; deploy on Render; install `10_My_Portfolio.gs` v1.5.14 and `16_Decision_Top10.gs` v1.13.10.
5. Dispatch Provider Target Coverage with `bootstrap=true` after the first healthy sync.

---

## 8. Final verdict

- **Operationally healthy:** **No.** The API is up, but the scheduled sync has not succeeded since 10 Oct, the
  decision cockpit is stuck, and workers crash under memory pressure.
- **Financially consistent:** **Not yet.** Three confirmed currency/ledger defects are fixed in the PR but not
  deployed; P&L components are not separated; broker reconciliation was not verified.
- **Validated for recommendation use:** **No.** Forecast outcomes are not evaluated in production, and until B1 is
  done there is no sample from which to judge accuracy. Treat Top_10 and Portfolio_Decision as research output only.

Readiness criteria: a green scheduled sync on 3 consecutive days; the PR deployed and installed; B1 running with at
least 30 matured, direction-scored, de-duplicated records per horizon; Portfolio_Decision certified against a
current broker statement.
