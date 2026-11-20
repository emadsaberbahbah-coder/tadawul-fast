# TFB: One-time audit, repair and handover (20 Nov 2026)

Report time: 21 Nov 2026, about 00:45 Riyadh (20 Nov 2026, 21:45 UTC). Technical timestamps are given in UTC.
Scope: one run, no recurring task created. Status labels used: **FOUND · PREPARED · APPLIED · TESTED · VERIFIED IN PRODUCTION · BLOCKED · ROLLED BACK**.

---

## 1. الملخص التنفيذي (العربية)

::: {dir="rtl"}
**الحالة الحالية:** خدمة Render تعمل بشكل سليم. نقطة `/health` تعيد 200 خلال نحو 0.5 ثانية، ولم تُسجَّل أي استجابة 5xx خلال آخر 24 ساعة. بلغ عدد تشغيلات GitHub Actions المكتملة في النافذة التي فحصتُها (20:02–21:22 UTC) ستين تشغيلًا، فشل منها ثلاثة. سبب الفشل الثلاثة واحد: أعطال عابرة في Google Sheets، وهي تجاوز حصة القراءة الدقيقية (HTTP 429) وخطأ 503 مؤقت.

**ما أُنجز:** عالجتُ السبب الجذري لهذه الإخفاقات في أربعة سكربتات، فأصبحت تنتظر انقضاء الخطأ العابر ثم تعيد المحاولة بدل أن تُسجّل التشغيل فاشلًا. التعديلات مختبرة (153 اختبارًا ناجحًا) ومقدمة في طلب الدمج [#1040](https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/1040) كمسودة بانتظار مراجعتك. لم تتغير أي قاعدة استثمارية أو قيمة أو بوابة.

**أهم مسألة لم تُحل:** لم أستطع التحقق المستقل من البيانات المالية. جداول Google Sheets لم تُقرأ إلا جزئيًا، ولم يكن هناك وصول إلى Apps Script، ولوحة المعلومات محمية بتسجيل دخول (401). لهذا لم تُطابَق المحفظة بالريال السعودي مع كشف الوسيط، ولم تُراجَع التوصيات الحالية سطرًا بسطر. ويظهر أيضًا في سجلات Render خطأ متكرر من Yahoo/yfinance (401 "Invalid Crumb") يؤثر على بعض البيانات الأساسية.

**جاهزية التوصيات:** النظام سليم تشغيليًا، لكن لا أستطيع أن أشهد بصحة التوصيات الحالية أو بدقتها التنبؤية من هذه الجلسة. لا تُستخدم أي توصية لاتخاذ قرار إلا بعد مراجعة صفحة Portfolio_Decision ومطابقتها يدويًا مع كشف IBKR.
:::

---

## 2. Detailed report (English)

### 2.1 Resource access (baseline)

| Resource | Status | What was possible |
|---|---|---|
| GitHub repo | Accessible | Full code read. Branch `claude/sharp-heisenberg-obvxwb` pushed. Draft PR #1040 opened. |
| GitHub Actions | Accessible | Run list, failed-job logs. |
| Render `srv-d4hnir15pdvs739bqe1g` | Accessible, read-only use | Service config, deploys, app and request logs, live `/health`. |
| Google Sheets workbook | **BLOCKED (partial)** | The Drive connector returned only one tab (`_Diag_Response_Shape`). No direct Sheets API credentials in this session, so `My_Portfolio`, `Portfolio_Decision`, `Top_10_Investments`, cash and cost basis could not be read. |
| Apps Script project | **BLOCKED** | No Apps Script tool in this session. The repository copy (`apps_script/*.gs`) was the only source available. Repo-to-deployed consistency was not checked. |
| Dashboard (chatgpt.site) | **BLOCKED** | HTTP 401: the page needs sign-in. No page-by-page assessment was possible. |
| IBKR | Not used (as instructed) | No broker reconciliation at a current cutoff. |

### 2.2 Architecture as verified

Market-data providers (Yahoo chart, yfinance, EODHD, among others) feed the FastAPI backend on Render (`main.py`, `routes/*`, `core/data_engine_v2.py`). Scheduled GitHub Actions jobs (about 50 workflows) call the backend and the Google Sheets API, and write the workbook pages: market pages, `My_Portfolio`, `Portfolio_Decision`, `Top_10_Investments`, Today Action Plan, Exit Plan, News Board, Evidence Board and others. Apps Script (`apps_script/*.gs`) runs inside the workbook: menus, refresh buttons, `My_Portfolio` ledger posting and Final Decision columns. Every scheduled job uses **one shared service account**, so they all draw on the same 60-reads-per-minute Sheets read quota. That shared quota is the main operational fragility found in this audit.

Key facts:
- Render runs commit `160c248` (#1035), deployed manually on 19 Nov at 13:59 UTC. Auto-deploy is **off**.
- `main` is at `1e075a9` (#1039), four PRs ahead. The diff between the two (`git diff 160c248 1e075a9`) touches only GitHub-Actions scripts, `.gs`, SQL, tests and docs. No `core/`, `routes/` or `main.py` file changed, so **the Render lag does not affect the backend** and no redeploy is needed for it.
- Backend health: `app_version 8.14.5`, `service_version 8.14.8`, `routes_failed_count 0`, `effective_failed_modules []`, 101 live routes.

### 2.3 Findings

| # | Priority | Finding | Status |
|---|---|---|---|
| F1 | High | Three scheduled jobs failed on 20 Nov between 20:29 and 20:37 UTC because of passing Sheets faults: Today Action Plan (503, not retried), Release Decision Feed (429, retried 5 s later inside the same quota minute) and Full Refresh Coverage (same minute, no retry). In each case the page or feed **kept older content** with no new release. | FOUND → APPLIED (branch) → TESTED. Production verification is pending merge. |
| F2 | High | Render app log, 20 Nov: repeated `yfinance HTTP Error 401 "Invalid Crumb"` and "User is unable to access this feature". Yahoo is refusing the quoteSummary/fundamentals endpoints for this client. Fields from that path may fall back or go blank. | FOUND. Not fixed: a fix needs a provider decision and live testing (see backlog B2). |
| F3 | Medium | `yfinance` reports "possibly delisted" for `BH.A`, `INM`, `DYNC`, `AIXC` and `HCWC`. These are universe hygiene issues: either a symbol-format mismatch (`BH.A` vs `BH-A`) or real delistings. | FOUND (backlog B3). |
| F4 | Medium | One service account shares one 60-reads/min quota across many concurrent workflows, so jobs collide at shared trigger minutes (`workflow_run` cascades after Daily Sync). F1 removes the symptom for four scripts. The structural fix is staggering or a second account. | FOUND (backlog B1). |
| F5 | Info | USD→SAR uses the SAMA peg 3.75 with explicit provenance (`policy://sama-usd-peg-3.75`, in `10_My_Portfolio.gs`) and a separate FX As Of. The broker's market rate (for example 3.75547) differs by about 0.15%. The difference is documented in code, which is acceptable. | Reviewed (code only). |
| F6 | Info | The `_Diag_Response_Shape` tab (the only tab readable) reports `INCOMPLETE-COLUMNS` for `Global_Markets` (5 concerning blank columns). Sample row AAPL shows `provider_engine_conflict TRUE` and the result `DO_NOT_INVEST`, so the gate downgraded the row correctly instead of passing a provider "BUY". | FOUND. The date of that diagnostic is unknown. |
| F7 | Info | In this sandbox, `test_release_decision_feed::n33_supersede_cli_contract` fails on unmodified `main` because of a local `cryptography`/`_cffi_backend` panic. It is an environment issue, not a code defect. CI is the reference. | Noted. |

### 2.4 Page-by-page assessment

**BLOCKED.** No pages could be assessed because neither the workbook pages (`Portfolio_Decision`, `Top_10_Investments` and their v2 versions, `My_Portfolio`, the cash and cost-basis ledgers, the audit logs) nor the dashboard could be read in this session. I make no claims about their current content, freshness or SAR totals.

### 2.5 Before/after metrics (consistent definitions)

| Metric | Baseline | After | Notes |
|---|---|---|---|
| Render `/health` | 200, 0.51 s | Same (no deploy needed) | Single probe, 21:34 UTC |
| Render 5xx requests, 24 h | 0 | n/a | Render request logs, 19 Nov 21:00 to 20 Nov 21:35 UTC |
| Actions completed runs, 20:02–21:22 UTC | 60 runs: 3 failure, about 26 skipped, about 31 success | Pending merge | GitHub run list (a sample, not a 7-day census) |
| Failures caused by passing Sheets faults in that window | 3 of 3 | Expected 0 for the same faults (unit-tested, not yet observed live) | No claim made until post-merge runs are seen |
| Affected test suites | Previous versions | 153 passed, 10 skipped, 1 env-only failure (pre-existing) | Local run |
| Portfolio reconciliation difference (SAR) | **Not measured** | **Not measured** | BLOCKED: no sheet or broker access |
| Recommendation eligibility and rejections | **Not measured** | **Not measured** | BLOCKED |
| Forecast performance | **Not measured** | **Not measured** | Shadow scorer and track_performance exist in the repo; their outputs live in the workbook and BigQuery, which were not read |

No accuracy or improvement percentage is claimed.

### 2.6 Forecasting and learning process (code-level observation)

The repository already has a persistent pre-outcome pipeline:
- `scripts/track_performance.py`: Signal_History and Performance_Log.
- `scripts/run_shadow_scorer.py` and `shadow_board`: shadow mode.
- `backtest_harness` and `thesis_exit_backtest` workflows.
- `pit_snapshot`: point-in-time archive.
- BigQuery views, including `v_investable_vs_market` and `v_missed_winners` added on 19 Nov.
- Observe-only trial labels.

This structure matches the requested design: record before the outcome, shadow before promotion, observe-only gates. I did not evaluate the measured results (sample size, calibration, drawdown) because they are stored in the workbook and BigQuery, which were not readable here.

---

## 3. Implementation register

| Item | Root cause | Files | Commit / PR | Tests | Production | Rollback |
|---|---|---|---|---|---|---|
| R1: retry 5xx in Today Action Plan | `with_retry` classified only 429 as retryable | `scripts/today_action_plan.py` (1.8.0→1.8.1) | `63c666f` / [#1040](https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/1040) (draft) | `test_v181_passing_server_fault_is_waited_out` + full suite | Pending merge | Revert the commit, or set `TFB_TAP_RETRIES=0` |
| R2: quota retry waits into the next minute | The 5 s retry stayed inside the same per-minute quota window | `scripts/release_decision_feed.py` (1.4.1→1.4.2) | same | `case_n31_…` + full suite | Pending merge | Revert the commit |
| R3: page reads retry passing faults | `read()` and `read_symbol_column()` had no retry | `scripts/audit_full_refresh_coverage.py` (1.3.2→1.3.3) | same | `test_v133_…` + full suite | Pending merge | Revert the commit |
| R4: shared classifier | No 5xx helper existed | `scripts/sheets_quota.py` (1.0.1→1.1.0) | same | `test_q9_server_transient_v110` | Pending merge | Revert the commit |
| R5: manifest pins | Pins must follow the source versions | `scripts/verify_deployment.py` | same | `test_verify_manifest_pins` | n/a | Revert the commit |

Status of R1–R5: **APPLIED (on branch) · TESTED**. They are **not VERIFIED IN PRODUCTION**: these scripts run in GitHub Actions and take effect only after you merge #1040. Render needs no deploy.

---

## 4. Prioritized remaining backlog

| # | Item | Impact | Dependencies | Acceptance criteria | Effort |
|---|---|---|---|---|---|
| B0 | Review and merge #1040, then check the next 24 h of Actions runs | Removes recurring red runs and stale pages from passing faults | Owner review, CI green | No failure whose cause is 429/503 in the next 24 h; logs show "waiting … then try" lines where faults occurred | 15 min |
| B1 | Reduce Sheets quota contention: stagger `workflow_run` cascades, or use a second service account for read-only audits | Root cause of F4 | Workflow map; second account adds no cost | Zero 429s per day across the job fleet for 7 days | 0.5–1 day |
| B2 | Yahoo 401 "Invalid Crumb" on the yfinance fundamentals path | Fundamentals may go stale or blank | Provider choice (EODHD is already integrated); `yfinance` version check | 0 crumb errors per day, or affected fields sourced and stamped from another provider | 0.5 day |
| B3 | Universe hygiene for `BH.A`, `INM`, `DYNC`, `AIXC`, `HCWC` | Avoid wasted calls and blank rows | Symbol mapping policy | Each symbol mapped (`BH-A`), confirmed delisted and removed, or marked inactive | 2 h |
| B4 | Portfolio reconciliation in SAR against an IBKR statement at one cutoff | Financial consistency | Owner supplies a statement or export; `scripts/tfb_reconcile_portfolio.py` exists | Per-symbol quantity and cost difference = 0; SAR total difference explained (FX basis 3.75 vs market rate, timing) | 2–3 h |
| B5 | Read-only access for audits: Sheets credentials or Apps Script API for this kind of session; a dashboard export | Unblocks page-by-page and recommendation review | Owner decision | The next audit can read the named pages | Owner |
| B6 | Clear the `Global_Markets` INCOMPLETE-COLUMNS diagnostic (5 blank columns) | Completeness | Fresh run of the diagnostic | Verdict COMPLETE, or the blanks documented as expected | 2 h |

---

## 5. Final verdict

- **Operationally healthy:** **Yes, with a known fragility.** The backend is healthy, with zero 5xx in 24 h. Scheduled jobs fail sometimes on passing Sheets faults; #1040 addresses that once merged.
- **Financially consistent:** **Not verified.** The workbook, broker and dashboard data were not accessible, so SAR totals, P&L split and reconciliation were not checked. The code-level FX method is documented and sound.
- **Sufficiently validated for recommendation use:** **Not established by this audit.** Gates are visibly conservative (the AAPL sample was downgraded on a provider/engine conflict), but I could not review current recommendations or forecast evaluation results. Treat the system's output as decision support, to be checked against `Portfolio_Decision` and the broker before acting. No profitability or accuracy claim is made.
