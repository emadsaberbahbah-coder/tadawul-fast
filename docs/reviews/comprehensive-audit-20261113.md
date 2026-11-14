# TFB Comprehensive Audit and Handover, 13 November 2026

**Scope:** one-time audit, repair and handover across providers, the Python/API service (Render), Apps Script, Google Sheets, the dashboard, and analysis/recommendations.
**Audit window:** 13 Nov 2026, 21:15–22:30 UTC (14 Nov, 00:15–01:30 Asia/Riyadh). Report times are Asia/Riyadh. Technical timestamps are UTC.
**Repository baseline:** `main` at `c822c69` (#976).
**Previous audit:** `comprehensive-audit-20261106.md` (draft PR #924, never merged). Its backlog R1–R13 is carried forward and re-checked here.
**Status labels:** FOUND · PREPARED · APPLIED · TESTED · VERIFIED IN PRODUCTION · BLOCKED · ROLLED BACK

---

## 1. الملخص التنفيذي (عربي)

**الحالة الحالية**

- **خدمة Render:** سليمة. لا توجد أي استجابة 5xx خلال آخر 24 ساعة (1,652 من 1,655 طلبًا بحالة 200). لا توقف بسبب نفاد الذاكرة ولا إعادة تشغيل. أعلى استهلاك للذاكرة 84% (كان 88% في التدقيق السابق). النسخة المنشورة `a36764e` متأخرة بـ26 دمجًا عن الفرع الرئيسي، وكلها سكربتات تعمل في GitHub Actions ولا تحتاج نشرًا على Render.
- **GitHub Actions:** من آخر 300 تشغيل لم يفشل سوى 5. اثنان منها، في Exit Plan وEvidence Board، سببهما حصة القراءة في Google Sheets، وقد أُصلحا اليوم في #975. أما إلغاءات CI الستة فكلها تشغيلات استبدلتها دفعات أحدث، وليست أعطالًا.
- **أخطر ما وُجد:** **مصنف Google Sheets بلغ 86% من الحد الأقصى المطلق البالغ 10 ملايين خلية** (8,631,214 خلية).
  - يزيد بنحو 68–90 ألف خلية يوميًا، فيُتوقع امتلاؤه بين 29 نوفمبر و3 ديسمبر. عند الامتلاء تتوقف الكتابة في كل الصفحات.
  - التشغيل التجريبي لأداة تقليص الشبكة الحالية يحرر 511 ألف خلية فقط من `_Run_Log` (نحو أسبوع من النمو).
  - الحل الدائم نقل صفحات الأدلة المتنامية (Performance_Log وSignal_History) إلى مصنف مستقل أو أرشفتها في BigQuery.
- **حلقة التعلّم من التوقعات متوقفة عمليًا:**
  - **حجم المتأخرات:** 7,686 توقعًا حلّ أجلها ولم تُقيَّم. أقدمها مستحق منذ 3 أكتوبر. أنضج تشغيل الساعة الأخير 0 منها.
  - **السبب:** التقييم يتم بسعر حيّ في يوم الاستحقاق نفسه، وهذا مستحيل بعد مرور اليوم. المسار البديل الذي يستخدم سعر الإغلاق التاريخي موجود ومختبر، لكنه ما زال في وضع «المراقبة» منذ 10 تشغيلات على الأقل.
  - **الأثر:** الفئات التي لم تُقيَّم بعد تتراكم أسرع (نحو 180 يوميًا) مما يُقيَّم (نحو 90 يوميًا).

**ما أُنجز (طلب سحب مسودة #977 وطلب #978)**

1. **حدّدت ثلاثة عيوب في قراءة لقطة IBKR وأصلحتها:**
   - أسهم الفئات مثل «BRK B» كانت تُسجَّل باسم في المراكز وباسم آخر في الأوامر، فقد يقول النظام إن السهم غير موجود لدى الوسيط أو إنه بلا وقف خسارة.
   - أمر شراء غير مقروء كان يُحسب نقدًا متاحًا.
   - (`ibkr_capture` 1.1.0 و`today_action_plan` 1.4.1).
2. **صحّحت رقمًا مضللًا في ملاحظة كل تذكرة:**
   - كانت الملاحظة تقول «ann 216.7%» لعائد تقييمي قدره 33.4%، لأن الرقم مركّب على 3 أشهر فقط.
   - أصبحت تذكر المدة المفترضة: «ann 216.7% if reached in 3m».
   - (`opportunity_builder` 1.29.1، ويحتاج نشرًا على Render).
3. **أضفت انتظار حصة Sheets إلى تدقيق تغطية أهداف المحللين.** اليوم فشلت قراءة الصفحات الأربع كلها بالخطأ 429 (1.3.0).
4. **صحّحت ختم إقلاع العامل في `/health`.** صار يتغير عند إعادة تدوير العامل (main 8.14.9).
5. **جدّدت 122 اسمًا موثقًا حتى 20 نوفمبر** بتشغيل واحد لسير العمل الموجود. طلب السحب #978 بانتظار الدمج.

**أهم مشكلة لم تُحل:**

- **سعة المصنف:**
  - **الآن:** نسخة احتياطية ثم تقليص `_Run_Log`.
  - **قبل 1 ديسمبر:** نقل صفحات الأدلة خارج مصنف القرار.
  - هذا قرار وإجراء للمالك.
- **ما زال الوصول إلى صفحات المحفظة والقرار متعذرًا (BLOCKED):** الملف أكبر من حد التصدير، والمسوّغات غير متاحة لهذه الجلسة.
- **لوحة التحكم الخارجية** تطلب تسجيل دخول ChatGPT.
- **لم يُقدَّم كشف من الوسيط**، فالفرق بالريال مقابل الوسيط **غير معروف، وليس صفرًا**.

**جاهزية التوصيات: غير جاهزة للاستخدام التنفيذي.**

| المؤشر | القيمة |
|---|---|
| نسبة نجاح فئة INVESTABLE | 48.2% (n=1,427) بمتوسط عائد ‎-0.40% قبل الرسوم |
| نسبة نجاح فئة WATCHLIST | 55.7%، أي أن البوابة ما زالت معكوسة |
| درجة Brier | 0.2892 مقابل 0.25 لرمي العملة، وأسوأ من 0.2864 قبل أسبوع |
| فئة الثقة 85–100 | نجاح 43.5% فقط |

**لا توجد فرصة مؤهلة** في هذا التقرير.

---

## 2. Access and baseline

| Resource | Access this session | Status |
|---|---|---|
| GitHub repo | Clone, MCP (runs, logs, PRs), workflow dispatch | Inspected |
| Render `srv-d4hnir15pdvs739bqe1g` | MCP read (service, deploys, events, logs, metrics) + public probes | Inspected (read-only) |
| Google Sheet `1QaOSNHq…wsG04` | Metadata OK (16.2 MB, `canEdit:false` via Drive connector). Content read gives the first tab only. xlsx export "File too large". ODS timed out ×2. Metadata snippet covers 9 tabs | **BLOCKED** for My_Portfolio, Portfolio_Decision(_v2), Top_10_Investments(_v2), cash/cost ledgers, Performance_Log, _Run_Log. Plan_Of_Record, _Plan_Of_Record, _Shadow_Rank_Log, _Data_Audit, Diagnostic_Report, Data_Dictionary and _Selection_Log (truncated) were partly read |
| Apps Script project | No Apps Script API in this session; repository copies only | **BLOCKED** (deployed-vs-repo parity unverifiable) |
| Dashboard (chatgpt.site) | Every path → HTTP 401 "Sign in required" (ChatGPT login). Source is not in this repo. It reads BigQuery (per #976) | **BLOCKED** (4th audit in a row) |
| IBKR | Not used, as instructed. The read-only capture logic in the repo was reviewed as code only | n/a |
| Broker statements | None supplied | Reconciliation **BLOCKED** |

**Revisions**
- Render live deploy `dep-dbp455c9v7es73ahtpvg` (manual, 9 Nov 22:03 UTC) → commit `a36764e` (#947). `/health`: entry 8.14.8, engine 5.160.0, `render_git_commit a36764e`. Auto-deploy is off by design.
- `main` is 26 commits ahead. None of them touch `main.py`, `routes/`, `core/` or `requirements.txt`. All are `scripts/` (Actions), `apps_script/`, `.github/`, tests, docs and `data/news_gate_taxonomy.json` (read by an Actions script). **A redeploy is not needed for current main.**

**Acceptance criteria set before editing**
1. Each fix has a reproduced root cause, a failing-then-passing test, and leaves gates, thresholds and verdict logic unchanged unless stated.
2. Every touched suite passes locally. The PR's CI is green.
3. No production write, Render deploy, env change or baseline acceptance is made on the owner's behalf. One-off dispatches are limited to existing workflows that write only a review branch or are read-only.

## 3. Findings

### 3.1 Render / API — FOUND

| Metric (Render MCP, 12 Nov 21:30 → 13 Nov 21:30 UTC) | Value |
|---|---|
| Requests | 1,655: 200 ×1,652, 499 ×3, 5xx ×0 |
| Events, 7 days | 7 deploys (all OK), 0 `server_failed`, 0 OOM, 0 restarts |
| Memory | 1.77–3.60 GB of 4.29 GB, **peak ≈ 84 %**. It falls to ~2.0 GB when gunicorn recycles workers (7 clean recycles in 24 h) |
| CPU | 0.002–0.45 vCPU (2-h buckets), limit 2 |
| Probes | `/readyz` 200 0.28 s · `/health` 200 0.47 s · `/livez` 200 0.44 s · `/docs` 404 (intended) |
| Latency series | Render returned no `http_latency` data (unchanged since last audit) |

- **F-R1 (Medium) — no Render health check is configured.** `healthCheckPath` is `""`. `scripts/start_web.sh:18,59,90,226` says `/readyz` is configured in the dashboard. `/readyz` is cheap and never returns 503 unless `TFB_READYZ_STRICT` is set (`main.py:490-519`). Owner action: set Health Check Path = `/readyz`.
- **F-R2 (Low) — `/health worker_boot_utc` did not change on worker recycles.** The pid went from 862 to 1109 while the stamp stayed at the 9 Nov deploy time. → **I1**.
- **F-R3 (Low, noise) — Yahoo throttling.** Over 100 `yfinance` ERROR lines in a 35-minute sample: "possibly delisted", `401 Invalid Crumb`, `Too Many Requests`. There were 47 `[YF-CB] OPEN` circuit-breaker trips. No crashes. The 6 Nov history-miss fix (#924) is still unmerged.

### 3.2 GitHub Actions — FOUND

Census: last 300 runs (13 Nov 09:13–20:52 UTC): **189 success, 100 skipped (gated `workflow_run` legs), 6 cancelled, 5 failed.**

| Run | Workflow | Error | Root cause | Status |
|---|---|---|---|---|
| 43432968251, 43445144923 | Exit Plan | `APIError … nothing written` | Sheets 429. The 3 retries 20 s apart all fell in one quota minute | Fixed upstream by #975 (65 s wait). Later runs green |
| 43445243620 | Evidence Board | `[429]: Quota exceeded … Read requests per minute per user` | Same, with no retry at all | Fixed by #975 |
| 43445156127 | Provider Target Coverage | `CH_BASELINE_STALE baseline is 459.0h old > max 72h` + 4 × `CH_READ_FAIL HttpError 429` | (a) No quota wait. (b) **Lock-in:** a stale baseline is a CONTROL failure, and a failed run never saves a baseline. **Red every day since 27 Oct; last green 26 Oct** | (a) → **I2**. (b) Owner decision (R6) |
| 43423786590 | Name Label Renewal (check) | `"expiring":128,"expired":6 … EXPIRED` | Captions expire 7 days after review. 122 were due 13 Nov | → **I5** (renewed) |

- **CI:** **0 failures.** All 6 cancellations are superseded runs (`concurrency: ci-${{ github.ref }}`, `cancel-in-progress: true`, ci.yml:994-996). For example, 43454343968 was cancelled and 43456874240 then passed on main.
- **Overlapping jobs:** the 14:41Z failures came from Exit Plan (dispatch), Evidence Board and a Provider Target Coverage run that started 8 h late, all reading in the same minute with one service account.

### 3.3 Google Sheets — FOUND / BLOCKED

- **F-S1 (Critical) — the workbook is near the 10,000,000-cell hard limit.** Performance Clock `[CAPACITY]` lines:

  | Run (UTC) | Allocated cells | Used |
  |---|---|---|
  | 8 Nov 19:57 | 8,258,474 | 82.58 % |
  | 10 Nov 08:45 | — | 83.66 % |
  | 12 Nov 21:55 | — | 85.53 % (breach of the 85 % threshold) |
  | 13 Nov 18:47 | **8,596,076** | **85.96 %** |

  - Growth is ≈ 68k cells/day over 5 days and ≈ 90k over the last 24 h. At that rate the limit is reached around **29 Nov – 3 Dec**.
  - The growth comes from appends to Performance_Log (~30.6k rows), Signal_History (~38.4k rows) and _Run_Log.
  - At 100 % every Sheets append and resize fails. The Performance Clock currently only *observes* the breach (`mode=observe`).
  - **The existing remedy is not enough.** The dry run of `grid_capacity_repair.yml` dispatched in this audit (§4, I6; 13 Nov 21:42Z) measured **8,631,214** allocated cells, 35k more than at 18:47Z. Its map:
    - skips every market page, which is already small (My_Portfolio 8 rows, Global_Markets 6,613);
    - would archive and prune only `_Run_Log` (24,669 → 5,000 rows), freeing **511,394** cells;
    - projects **8,119,820 (81 %) → "closure FAIL"** against its own < 7,000,000 bar.

    Pruning `_Run_Log` buys about a week. The growth is in tabs the tool does not cover (Performance_Log, Signal_History and the other evidence tabs).
- **F-S2 (High) — the ticket note shows a misleading annualized figure.** `_Plan_Of_Record` 11-12 TEN reads "valuation upside 33.4% (ann 216.7%)" (1.334⁴ − 1 = 216.7 %). ZTO reads 32.8 → 210.9 and TEN 11-13 reads 32.2 → 204.9.
  - `ann_roi_pct` compounds a 12-month-style valuation upside over the 3-month panel period. In effect it assumes the whole upside arrives within 3 months.
  - The figure also feeds `exp_gain` ("gain ann-basis"), the ann-ROI gate and ranking. Those are documented model choices (`opportunity_builder.py` ~3999, 4996, 7638) and are not changed here.
  - Only the label was unclear. → **I3**.
- **F-S3 (Medium) — R/R is printed and gated on TP2.** Each ticket's R/R is (TP2 − entry)/(entry − stop): 4.02–4.37. The plan's first exit is TP1, where R/R is 2.01–2.18, right at the "Min Risk/Reward 2" gate.
  - The owner chose this explicitly in v1.8.0 (`opportunity_builder.py:4545-4570`), so it is recorded as an analytical concern (R10), not a defect.
- **F-S4 (Medium) — stale diagnostics still read OK.** `_Data_Audit` was generated 2026-06-19 (147 days old) and covers 1 page and 11 rows. `Diagnostic_Report` was last run 2026-06-06. Carried from R9.
- **F-S5 (Low) — API and sheet values differ in `_Diag_Response_Shape` for AAPL.** upside −0.1301 vs −0.1237 and score 54.55 vs 54.62. There is no timestamp to say which is newer.
- **Checks that passed** (4 ticket rows read):
  - Direction: stop < entry zone < TP1 < TP2. For example, TEN: 42.35 < 44.65–46.49 < 53.72 < 61.42.
  - FX: USD with the 3.75 SAMA peg, applied in the right direction (44.648 × 3.75 = 167.43).
  - Ticket: 29 × 46.03 × 3.75 = 5,006.
  - The plan price equals the EODHD cross-check (Δ 0.00 %).
  - No "live" claim on delayed quotes (quotes older than 15 minutes are deferred).
  - 0 `#REF!`, `#N/A` or `NaN` strings across the 9 tabs read.
- **BLOCKED:** My_Portfolio, Portfolio_Decision(_v2), Top_10_Investments(_v2), cash and cost-basis ledgers, Today_Action_Plan, Daily_Plan, Exit_Plan, _Status, _Run_Log, Performance_Log. No holding, quantity, cash, SAR total or v1/v2 comparison could be recomputed.

### 3.4 Portfolio and consolidated SAR — FOUND (code) / BLOCKED (data)

- **Code paths checked and correct:**
  - USD→SAR multiplies by the 3.75 peg everywhere: `daily_plan` `FX_TO_SAR`, `capture_cash`, `pick_cost`/`commit_cost`, `cash_cells`.
  - A non-USD capture cash balance is refused. The Consistency Check C6 FX-gap formula is correct.
  - Working buys are reserved once, never twice.
  - A buy order without a limit price withholds the broker cash.
  - Cash used = min(cash, settled) ≥ 0. Sizing uses whole shares.
  - The withholding-tax unit is right. A missing price → CHECK, never a −100 % P&L.
  - Riyadh dates are used for plan days. Tadawul is Sun–Thu 10–15 and NY Mon–Fri 9:30–16 via `zoneinfo`.
- **F-P1 (High) — IBKR capture: share-class names are split.**
  - Positions mapped "BRK B" → `BRK.B` (no `.US`), while orders mapped "Buy 3 BRK B" → `BRK.US`. Neither matches the workbook's `BRK-B.US` or `BRK.B.US`.
  - Consequences on the Today Action Plan:
    - the holding reads "not at IBKR";
    - its stop is not seen, so step 2.4 says "no stop working, place one", which risks a duplicate stop;
    - a working buy on such a name is not recognised, so the buy is repeated and the cash counted twice.
  - Proof: `positions {'BRK.B': 3.0}`, `stops {'BRK.US': [(450.0, 3.0)]}`. The old test asserted the mismatch. → **I4**.
- **F-P2 (Medium) — IBKR capture: unclear working orders counted as free cash.**
  - An order with an unreadable description was dropped, and any side other than exactly "BUY" was read as a sell. Either way a buy reservation could vanish and cash be overstated.
  - The workbook reader (`10_My_Portfolio.gs:3736-3738`) marks such orders incomplete instead. → **I4**.
- **F-P3 (High) — Exit Plan: a top-up resets the holding's stop history.**
  - `lot_key = SYMBOL|latest buy date|weighted buy` (`scripts/exit_plan.py:604`). A top-up changes the key, `prior_row` returns `{}`, and then:
    - the "stop never moves down" ratchet and the once-only TAKE PROFIT flag are lost;
    - on the top-up day `fresh` forces HOLD for the **whole** position.
  - Repro: one lot at 105 reads TAKE PROFIT, stop 123.5. After a 10-share top-up at 130 it reads HOLD, stop **108.1**.
  - Open PR #970 (LOT-CARRY, default off) covers lot changes through **sales only** and deliberately excludes top-ups. It is also based on exit_plan 1.1.0 while main is at 1.3.1.
  - Not changed here, to avoid a conflicting second edit of the same module. → backlog R4.
- **Broker difference in SAR: unknown, not zero.** No statement was supplied and the portfolio tabs are blocked.

### 3.5 Recommendations and forecast evaluation — FOUND

Source: Performance Clock run [43485951636](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/43485951636) (13 Nov 22:02 Riyadh), `track_performance` v6.53.0.

- **Records:** 30,609 total. 13,633 matured, of which **4,121 decided**.
- **Method:** returns are gross of fees and slippage. Same-day repeat keys are measured, not removed: 64 of 71 new signals would be skipped as duplicates. The n figures therefore overstate independent evidence.

| Slice | Decided n | Win % [95 % CI] | Mean ROI | Stated reliability |
|---|---|---|---|---|
| INVESTABLE | 1,427 | **48.2** [46–51] | −0.40 % | 71 |
| WATCHLIST | 2,476 | 55.7 [54–58] | +0.30 % | 61 |
| BLOCKED | 204 | 76.5 [70–82] | −2.07 % | 55 |
| Reliability 85–100 | 177 | **43.5** [36–51] | −0.99 % | 87 |
| Board executions | 70 | 47.1 [36–59] | −0.91 % | 74 |
| 1W target achievement (unit-sentry, n=250) | — | 37.2 | MAE 4.78 pp, bias −2.33 pp | — |
| 2W target achievement (n=101) | — | 24.8 | MAE 6.74 pp, bias −3.27 pp | — |

- **Calibration:** Brier **0.2892** vs 0.25 for a coin flip (6 Nov: 0.2864). Not monotonic in stated reliability.
- **Risk-adjusted:** Sharpe (gross) 1W −0.61, 2W −0.49, 1M 0.16, 3M −0.46.
- **Benchmark:** the regret check (board vs the best alternative Top-10) is +2.37 % on n=89 vs n=10, which is too thin to read.
- **Provenance:** `unproven_forecast_provenance` = 23,166 of 30,609 records. Most stored forecasts cannot prove they were recorded before the outcome.
- **F-L1 (High) — the evaluation loop is stalled by design.** The latest run matured **0** of 7,686 due records (oldest target 3 Oct). The last 10 runs matured 0–103 each.
  - `scripts/track_performance.py:3576-3599` accepts only a quote *from the target session* ("never use a later session"). A target session that passed before an audit priced it can therefore never mature from a live quote. The backend-priced 839 symbols carry today's quotes, and the fallback priced 0/375 (Yahoo 429).
  - The built-in remedy, `TRACK_HIST_TARGET_CLOSE`, uses the BigQuery archive close of the target session. It witnessed 3,359 of 3,494 candidates but is still in `observe`, which only tallies (`:10819-10820`, `:10989`).
  - The arming plan (`:1938-1940`) was "observe for one audit, then enforce". Observe has now run for at least 10 passes.
  - **Look-ahead check passed:** only a bar dated exactly the target session from a daily source is used, and 135 hourly-rebuilt bars were refused. Tests `test_tracker_hist_target_close.py` and `test_track_performance_v6530_archive_bar.py` give 49 passed, 1 skipped.
  - The backlog grows ≈ 180/day while maturation runs ≈ 90/day.
- **Readiness: NOT READY.** INVESTABLE underperforms WATCHLIST, the top reliability band is anti-calibrated, and the live calibration factors (INVESTABLE ×0.68) say stated reliability is ~30 % over-confident. **No qualifying opportunity** is listed.

### 3.6 Manual refresh, Apps Script triggers, dashboard — BLOCKED

These need the Apps Script editor, sheet edit rights and the dashboard sign-in. The Python/Actions side of the refresh chain looked healthy today:
- `Release After Sync`, `Sync Outcome Audit`, `Decision Surface Freshness`, `Full Refresh Coverage` and `Daily Plan` were all green at 20:24Z.
- `Plan_Of_Record` was refreshed 14 Nov 00:24 Riyadh.

Nothing is claimed about end-to-end manual-refresh behaviour, run-ID propagation or the protection against older results overwriting newer ones on the Apps Script side.

## 4. Implementation register

Draft PR [#977](https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/977), branch `claude/sharp-heisenberg-ofdrac`. Each item is a separate commit, so each can be reverted alone.

| # | Defect / root cause | Change | Files | Tests | Status |
|---|---|---|---|---|---|
| I1 | F-R2: worker inherits the master's import-time boot stamp | `main` 8.14.9: `os.register_at_fork` + pid guard; `_current_boot_stamp()`. Provenance only | `main.py`, `scripts/verify_deployment.py`, auth-wall test pins, `tests/test_main_worker_boot_stamp_v8149.py` | 4 new (fork test); 30 passed / 7 skipped across the touched suites | **PREPARED · TESTED**. Needs merge + manual Render deploy |
| I2 | §3.2: Provider Target Coverage read the 4 pages once, so a quota minute gave 4 × READ_FAIL | `audit_provider_target_coverage` 1.3.0: `_read_quota_aware` waits 65 s on HTTP 429, at most twice (`scripts/sheets_quota.py`); other errors are unchanged. Baseline rules, thresholds and exit codes unchanged | `scripts/audit_provider_target_coverage.py`, `tests/test_ptc_quota_wait_v130.py` | 5 new + existing 41 (incl. `--selftest`) pass | **PREPARED · TESTED**. Actions-side: effective on merge. Does **not** clear the stale-baseline lock-in (R6) |
| I3 | F-S2: "ann" figure without its period | `opportunity_builder` 1.29.1: note reads "(ann X% if reached in Nm)". Note text only. The version bump retires 1.29.0 frozen board-funding snapshots, so deploy outside a sync | `core/analysis/opportunity_builder.py`, `scripts/verify_deployment.py`, `tests/test_ob_ann_note_period_v1291.py` | 4 new; 255 opportunity/board tests pass | **PREPARED · TESTED**. Needs merge + Render deploy |
| I4 | F-P1, F-P2: share-class symbol split; unclear orders read as free cash | `ibkr_capture` 1.1.0: `symbol_of('BRK B')`→`BRK.B.US`; the order regex keeps the class letter; `key()`/`rekey()`; an unclear working order is filed as an unpriced buy, so reserved cash is incomplete. `today_action_plan` 1.4.1 reads the capture under the workbook's spelling | `scripts/ibkr_capture.py`, `scripts/today_action_plan.py`, `scripts/verify_deployment.py`, `tests/test_ibkr_capture.py` (+4 tests; C1 expectation corrected) | 81 plan/capture/daily-plan tests pass | **PREPARED · TESTED**. Actions-side: effective on merge |
| I5 | §3.2: 122 name captions expiring 13 Nov | One dispatch of the existing `name_label_renewal.yml` (renew, publish_branch) | Run [43511586440](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/43511586440) ✅. Branch `name-label-renewal/43511586440`, draft PR [#978](https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/978): 122 → expire 2026-11-20, 6 held | Validated by the workflow's production loader | **APPLIED** (review branch + PR), awaiting owner merge |
| I6 | F-S1: no current per-tab cell plan | One dispatch of the existing `grid_capacity_repair.yml` with `apply=false` (dry run; read-only; joins the production-write concurrency group) | Run [43513080009](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/43513080009) ✅: allocated 8,631,214; only `_Run_Log` prunable (−511,394); projected 8,119,820 = closure FAIL | — | **APPLIED** (dry run only; nothing resized). Shows the tool cannot close the gap |
| — | CI list | `ci.yml` 1.1.80: the PTC and note suites in the blocking lean lane (the main.py suite needs fastapi and runs in the whole-suite job; first push had it in the lean lane, CI caught it, fixed) | `.github/workflows/ci.yml` | YAML parses; CI-referencing suites 282 passed (2 failures in `test_fw2_stale_pe.py` also fail on the base commit: they need git history absent from this shallow clone) | **TESTED** |

**Full suite** (local, Python 3.11, 15 min 45 s): **6,819 passed, 71 skipped, 4 failed.**
- 2 failures are `test_fw2_stale_pe.py` (a1b, w1b). They need git history that this shallow clone lacks, and they fail identically on `main`.
- 2 failures did not reproduce on rerun: `test_today_action_plan.py::t4` and `test_venue_calendar_ext.py::t7`, now 73/73 passed. The long run overlapped the edits to version pins.

The PR's own CI result is on #977.

**Owner steps to put I1–I4 into production**
1. Review and merge #977.
2. For I1 and I3, use a Render **manual deploy** of the merged commit outside a sync window, then confirm `/health` shows `entry_version 8.14.9` and that `verify_deployment.py` pins pass.
3. I2 and I4 take effect on the next Actions run after the merge.

**Rollback:** revert the single commit for the item. For I1/I3, redeploy the previous commit on Render. For I5, close #978.

**Not done on the owner's behalf:**
- no Render deploy;
- no env or repository-variable change;
- no baseline acceptance;
- no grid resize;
- no workbook write.

## 5. Before / after metrics

| Metric | 6 Nov audit | This audit (13 Nov) | Note |
|---|---|---|---|
| Render 5xx | 1 × 502 in 7 days | 0 in 24 h; 0 failure events in 7 days | |
| Render peak memory | 88 % | 84 % | recycling keeps it under the limit |
| Live deploy vs main | 43 behind (mostly Actions-side) | 26 behind (all Actions-side) | |
| Failed runs | — | 5 of 300 (2 quota, fixed by #975; 1 quota + lock-in; 1 expiry) | CI 0 failures |
| Provider Target Coverage | 0/8 green | still red since 27 Oct | I2 removes the quota cause only |
| Name captions valid on main | 0 → 122 (PR) | 122 expiring 13 Nov → 122 to 20 Nov on PR #978; 6 held | |
| Workbook cells | not measured | **85.96 %** of 10M, +68–90k/day | new Critical |
| Forecast records matured | 13,001 | 13,633 (+632 in 7 days) | due backlog 7,686 and growing |
| INVESTABLE decided win rate | 48.9 % (n=1,242) | 48.2 % (n=1,427) | overlapping samples |
| Brier | 0.2864 | 0.2892 | worse |
| Broker difference (SAR) | unknown | unknown | no statement |
| Sheet tabs readable | 9 diagnostic | 9 diagnostic | decision tabs still BLOCKED |

No overall accuracy or improvement percentage is claimed. I1–I4 have no production effect until they are merged (and, for I1/I3, deployed).

## 6. Prioritized remaining backlog

| ID | Priority | Item | Dependencies | Acceptance criteria | Effort |
|---|---|---|---|---|---|
| R1 | **Critical** | Workbook capacity (F-S1), in three steps:<br>(a) **now:** full backup (`workbook_backup.yml`), then apply the `_Run_Log` prune (`grid_capacity_repair.yml` `apply=true`, `have_backup=yes`, `confirm=RESIZE`). This takes ~511k cells (≈ 81 %) and buys about a week;<br>(b) **before ~1 Dec:** move the growing evidence tabs (Performance_Log, Signal_History) out of the decision workbook. Either use the existing `TFB_TP_EVIDENCE_SPREADSHEET_ID` route to a dedicated workbook, or archive-and-prune rows that are already in BigQuery (`archive_to_bigquery.py` copies them daily but never deletes);<br>(c) set `TFB_TP_APPEND_FATAL=1` so a full grid turns the job red | Owner (+ a code change for archive-and-prune if chosen) | `[CAPACITY]` < 75 % and flat week on week | (a) 1 h · (b) 0.5–1 day |
| R2 | **Critical** | Keep recommendations fail-closed. Fix the INVESTABLE/WATCHLIST inversion and calibration before any promotion. Evaluate net of costs on de-duplicated keys | R3 | Brier < 0.25 on a chronological holdout; INVESTABLE ≥ WATCHLIST after costs | days |
| R3 | High | Arm `TRACK_HIST_TARGET_CLOSE=enforce` (keep `TRACK_HIST_CLOSE_SOURCE=bq_archive`) so due forecasts mature from the target-session close | Owner variable | The next Performance Clock shows `matured>0` on the HIST-CLOSE line, with CA-GUARD refusals reviewed; the due backlog falls week on week | 15 min + review |
| R4 | High | Exit Plan top-up (F-P3): carry the symbol's highest published stop across a key change caused by a top-up, and base `fresh` on the first buy. Rebase #970 onto exit_plan 1.3.1 and extend it | none | A top-up never lowers the published stop and never resets TP1; a regression test is added | 3 h |
| R5 | High | Give the audit read access to the decision tabs: a read-only Sheets credential, or a small JSON/BigQuery snapshot of My_Portfolio, Portfolio_Decision(_v2) and Top_10(_v2). Also a reviewer path for the dashboard | Owner | Next audit recomputes holdings and SAR totals | 2–4 h |
| R6 | High | Provider Target Coverage lock-in: after a verified healthy sync, dispatch once with `bootstrap=true` (the documented recovery). Decide whether a stale-only baseline should renew itself | Owner policy | Workflow green on the next schedule | 15 min |
| R7 | High | Broker reconciliation at a cutoff | Owner statement/export | Positions, quantity, cash and cost basis matched; differences in SAR | 0.5 day |
| R8 | Medium | Name captions: merge #978, review the 6 held bindings, and automate the weekly renewal (expires again 20 Nov) | Owner | Daily check green | 10 min/week |
| R9 | Medium | Set the Render Health Check Path to `/readyz` (F-R1) | Owner | Render shows the health check; deploys stay green | 5 min |
| R10 | Medium | R/R gate on TP2 while the plan exits at TP1 (F-S3): shadow-test a TP1-basis gate | none | Shadow report of ticket changes and outcomes | 0.5 day |
| R11 | Medium | Merge queue: 30 open PRs, including 5 earlier audit drafts (#795, #809, #835, #842, #846, #924) never merged or closed | Owner | Each merged or closed with a reason | 1–2 h |
| R12 | Medium | Retire or stamp-stale `_Data_Audit` and `Diagnostic_Report` (June) | Owner (Apps Script) | The tab shows its age or is removed | 1 h |
| R13 | Medium | Yahoo noise and dead tickers (F-R3): merge/arm #924 (`TFB_YC_HISTORY_MISS_TTL_SEC`) and retire dead ids at the source | Owner | Fewer 429/crumb errors in 24 h of logs | 30 min |
| R14 | Low | Verify the `_Selection_Log` "Cash Available (SAR) 100,000" default against `_Cash_Snapshot` | R5 | Logged sizing equals the real cash at the time | 1 h |

## 7. Final verdict

- **Operationally healthy? Mostly, with one time-bound risk.**
  - Healthy: the API is up with 0 5xx and no OOM, and today's red runs are explained (two already fixed upstream, one fixed here, one renewed here).
  - Risk: **the workbook will hit Google's 10M-cell limit in roughly 2–3 weeks unless R1 is done.** The existing grid-repair tool frees only about a week of growth, so a structural move of the evidence tabs is needed. The Provider Target Coverage control also stays red until R6.
- **Financially consistent? Code paths: yes, with three defects found.** The SAR peg, cash funding, sizing, units and null handling are correct where they could be checked. Two IBKR-capture defects are fixed in #977, and the Exit Plan top-up defect is open (R4). **Data: not verifiable.** Holdings, cash and SAR totals could not be read, and broker reconciliation is blocked.
- **Sufficiently validated for recommendation use? No.**
  - INVESTABLE wins 48.2 % gross against WATCHLIST's 55.7 %.
  - Brier 0.2892 is worse than a coin flip.
  - The evaluation loop is not maturing due forecasts (R3).
  - The system should keep treating its output as decision support only.

*Decision support only; not investment advice. No recurring tasks, schedules or monitoring were created. The two workflow dispatches (name-label renewal and grid-capacity dry run) were single manual runs of existing workflows: one writes a review branch, the other is read-only.*
