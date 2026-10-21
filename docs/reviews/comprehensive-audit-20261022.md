# TFB comprehensive audit — 22 Oct 2026 (one-time)

Audit window: 21 Oct 2026 21:00 UTC – 22 Oct 2026 (Asia/Riyadh 22 Oct 00:00 onward).
Base commit: `264d3a9` (main). Live Render deploy: `55b47f2` (one commit behind main).
Follows the 21 Oct audit (PR #809, safe subset merged in #810). This run carries the
remaining learning-metric fix from #809 (with its review defect corrected), fixes
three portfolio-math defects, and records the operational state.

Status labels: FOUND · PREPARED · APPLIED · TESTED · VERIFIED IN PRODUCTION · BLOCKED · ROLLED BACK

---

## 1. الملخص التنفيذي (عربي)

**الوضع الحالي:** خدمة Render تعمل وتستجيب (جميع نقاط الفحص 200 خلال 0.3–0.7 ثانية) وتشغّل الإصدار `55b47f2`. مزامنة بيانات الأسواق نفسها ناجحة (Market_Leaders وGlobal_Markets بـ6,611 صفاً وMutual_Funds بـ2,474 صفاً كُتبت بلا أخطاء). لكن **صفحات القرار غير محدَّثة**: لوحة Top_10 عالقة على حالة "running…" منذ 21 أكتوبر 19:40 بتوقيت الرياض، وPortfolio_Decision لم تُحدَّث منذ 08:43 صباحاً بينما تحدّثت My_Portfolio الساعة 20:21. لذلك تفشل حواجز التحقق (وهي تعمل بشكل صحيح)، ولم تنجح أي مزامنة مجدولة كاملة في 22 من آخر 22 تشغيلاً منذ 14 أكتوبر.

**ما تم إنجازه في هذا التدقيق (مُطبَّق ومُختبَر في فرع، لم يُنشر بعد):**
1. أوزان المحفظة: سهم مملوك بلا سعر كان يُستبعد من المقام فتتضخم أوزان باقي الأسهم وقد تظهر توصية REDUCE خاطئة. الآن تُترك الأوزان فارغة للمحفظة كلها مع حالة `PORTFOLIO_VALUE_INCOMPLETE`.
2. أسعار الصرف: مفتاح `GBp` (بنس) كان يستبدل سعر الجنيه `GBP` فيُقيَّم المركز أقل بـ100 إلى 10,000 مرة. تم الإصلاح.
3. أوامر الخروج: كمية كسرية مثل 10.6 سهم كانت تقترح بيع 11 سهماً. الآن لا يتجاوز البيع ما هو مملوك.
4. مقاييس التعلّم: توصية بيع ارتفع سعرها كانت تُحسب "ربحاً". الآن تُقيَّم كل توصية حسب اتجاهها، وصُحِّحت نسبة Sharpe (كانت 13.42 بدلاً من 2.54 على المثال)، ويقارن معدل الثقة المعلن بالسجلات المحسومة نفسها.

**المشكلة الرئيسية غير المحلولة:** تشغيل `refreshDecisionTop10` في Apps Script لا يكتمل، ولا يُعاد تحديث `Portfolio_Decision` بعد المزامنة. مشروع Apps Script لم يكن متاحاً لي (BLOCKED)، فالإصلاح يتطلب إجراءً من المالك.

**جاهزية التوصيات:** **غير جاهزة للاستخدام حالياً.** صفحات القرار قديمة ومختلطة المصادر، والحواجز تمنع نشرها عن حق. لا تعتمد على Top_10 أو Portfolio_Decision حتى يكتمل تحديث ناجح وتنجح بوابة Decision Surface Freshness.

---

## 2. Access and baseline

| Resource | Access | Notes |
|---|---|---|
| GitHub repo | Full (read/write via branch + PR) | 723 tracked files, ~323k lines of py/gs/js; 38 workflows; 21 open PRs |
| GitHub Actions | Read logs | No re-runs triggered |
| Render service `srv-d4hnir15pdvs739bqe1g` | Read (events, logs, metrics) | Auto-deploy **off**; deploys are manual. No deploy triggered by this audit |
| Google Sheet `1QaOSNH…` | **Partial** — metadata + first 5 tabs via Drive text export | Decision tabs (Portfolio_Decision, Top_10_Investments, v2, My_Portfolio, _Status) **BLOCKED**: the export truncates before them; xlsx export not supported by the connector |
| Apps Script project `1IvEj8…` | **BLOCKED** — "Requested entity was not found" (container-bound script, not visible to Drive) | Repo copy in `apps_script/` could not be compared with the deployed script |
| Dashboard site | Not inspected | No authenticated browser in this session — **BLOCKED** |
| IBKR | Not used (per instructions) | Broker reconciliation **not verified** |

### Baseline metrics (sources and denominators)

| Metric | Value | Window / source |
|---|---|---|
| Live probes `/health /healthz /readyz /livez /ping /meta /v1/meta` | 8/8 HTTP 200, 0.30–0.70 s | 22 Oct 00:28 Riyadh, single probe each |
| Live commit | `55b47f2` = `/meta deploy.render_git_commit` | main is `264d3a9` (#813, Athens venue calendar) — not deployed |
| HTTP status, 7 days | 200: 13,555 · 499: 212 · 401: 84 · 502: 6 (0.04%) | Render metrics 14–21 Oct |
| HTTP status, 24 h | 200: 2,456 · 499: 50 · 401: 75 · 5xx: 0 | Render metrics |
| Worker SIGSEGV (`code 139`) | 5 in 7 days | 18 Oct 08:55Z, 19 Oct 23:48Z, 20 Oct 09:30Z ×2, 20 Oct 23:17Z |
| Memory per instance | climbs to 3.8–4.2 GB of 4.29 GB before each deploy resets it | current instance 0.73 → 3.80 GB in 7 h |
| Latency p50/p95 | **not available** (Render `http_latency` returned empty) | — |
| Scheduled Dashboard Advanced Sync | **0 of 22** succeeded | scheduled runs 14–21 Oct |
| Decision Surface Freshness | **0 of 10** non-skipped runs succeeded | last 100 runs (90 skipped by filter) |
| Data-sync step inside the failed 21 Oct 17:20Z run | 3/3 legs `failed=0`; rows written: Global_Markets 6,611, Mutual_Funds 2,474 | job logs |
| Offline test suite at base | 5,438 passed · 37 skipped · 0 failed | local venv with requirements.txt pins, Python 3.11 |

---

## 3. Architecture findings (Providers → Python/API → Apps Script → Sheets → dashboard)

1. **Providers → Render (FastAPI).** Healthy. Error logs are dominated by Yahoo throttling (`yfinance circuit OPEN`, 404 "Quote not found" for real tickers such as BK and SPLG). The breaker and EODHD fallback work as designed: EODHD `f429=0`, no 5xx in 24 h. FOUND: `[FRESHNESS v1.27.0] venue calendar unavailable` for `.AT .VN .NS .AE .QA .BH`. `.AT` is fixed on main (#813) but not deployed. The rest have no calendar in exchange_calendars 4.13.2 and stay fail-closed by design.
2. **Render runtime.** FOUND (High): memory grows steadily on every instance until a deploy restarts it. Five native segfaults happened in 7 days, two of them at 95% of the memory limit. Frequent manual deploys are currently what prevents OOM kills. Root cause is not located (no Python traceback; likely a native library under memory pressure). Backlog B2.
3. **GitHub Actions sync → Sheets.** The market-data legs write cleanly. Every leg then fails `Validate Dashboard` on `decision.header_found`, because the Top_10 cockpit is still `running…`, 51–108 min after its stamp. The guard is correct and was added in #810.
4. **Apps Script decision layer (FOUND, Critical, BLOCKED for repair).** `refreshDecisionTop10` does not finish, and Portfolio_Decision is not re-run after syncs. v1.13.10 writes FAILED when the refresh throws, but the live status is RUNNING. That means either the 6-minute Apps Script timeout (which skips the catch) or v1.13.10 not being installed. I could not tell which, because the script project is inaccessible.
5. **Freshness gate.** 12 FAILs on 21 Oct 18:30Z: `PF_RUN_STALE`, `PF_OLDER_THAN_SOURCE`, `T10_STATUS_NOT_OK`, `T10_OLDER_THAN_SOURCE` ×4, `T10_POOL_COUNT_MISSING` ×4 and `T10_POOL_RECONCILIATION_UNPROVEN`. All follow from item 4. Loosening the gate would publish stale decisions, so it was left unchanged.
6. **Version reporting.** FOUND (Low): `/meta app_version` reads 8.14.5 while `entry_version` reads 8.14.6. `main.py:1034` takes `APP_VERSION` from the environment, so the Render env var is stale. This is a config change for the owner.
7. **FX configuration.** FOUND (Medium): Render logs `PF-WEIGHT-FX operator FX rates are 61 days old (TFB_PF_FX_AS_OF), limit 30`. The operator-supplied non-peg rates used for My_Portfolio SAR weights need a refresh, which is a config change for the owner.

### Page-by-page assessment

| Page | Producer | State at audit | Assessment |
|---|---|---|---|
| Market_Leaders / Global_Markets / Commodities_FX / Mutual_Funds | `run_dashboard_sync.py` (Actions) | Synced 21 Oct, `failed=0` | Data refresh working; Global_Markets meets 6,512 floor (6,611) |
| My_Portfolio | sync + `10_My_Portfolio.gs` | Refreshed 21 Oct 20:21 Riyadh | Weights affected by defects D1/D2 (fixed in this PR, pending deploy) |
| Portfolio_Decision | Apps Script | Last run 21 Oct 08:43 Riyadh | **Stale** — older than its source; do not act on it |
| Top_10_Investments | Apps Script `16_Decision_Top10.gs` | `running…` since 19:40 Riyadh, no grid | **Unavailable** — no qualifying opportunities can be confirmed |
| v2 decision pages | Apps Script | **BLOCKED** (not readable) | — |
| `_Data_Audit`, `Diagnostic_Report` | legacy | Last written Jun 2026 | Abandoned; their timestamps can mislead readers. Backlog B7 |
| Dashboard site | external | **BLOCKED** | Not inspected |

---

## 4. Implementation register

Branch `claude/vibrant-bell-3xt7y7`, one PR (link in the PR list). Nothing is deployed: Render auto-deploy is off and deploying is the owner's call.

| ID | Defect and root cause | Files | Status |
|---|---|---|---|
| D1 | **Unpriced holding inflated every other weight.** `_compute_portfolio_fields` summed only rows with a `position_value`, so a held row with no price left the denominator. Example: 2222.SR 100@25 plus 1120.SR 100@None gave 2222.SR weight 100%, gap −50 and a drift REDUCE, all under status `OK:SAR`. Fix: such a book is blanked like `PORTFOLIO_FX_INCOMPLETE`, with status `PORTFOLIO_VALUE_INCOMPLETE: <symbols>` and no drift ADD/REDUCE. A carried `position_value` still counts. | `core/data_engine_v2.py` 5.155.0 → 5.156.0 | APPLIED · TESTED |
| D2 | **Pence FX key overwrote the pounds rate.** `_pf_fx_table` upper-cased every key, so `GBp:0.048` replaced `GBP:4.8`. Fix: `_pf_fx_key` files GBp/ZAc/ILa under GBX/ZAC/ILA, which the per-row lookup already reads as SAR per subunit. | `core/data_engine_v2.py` | APPLIED · TESTED |
| D3 | **EXIT oversold fractional holdings.** `int(round(10.6))` = 11. Fix: floor to whole shares, and price per share at value ÷ true quantity, so the remainder stays visible in post-trade weight. The delta stays a signed integer (the 10_My_Portfolio contract). | `core/analysis/portfolio_actions.py` 1.16.0 → 1.16.1 | APPLIED · TESTED |
| D4 | **Learning metrics scored the price, not the call** (carried from #809). A SELL that rose counted as a win. Hit rate divided by all records. Sharpe treated percent horizon returns as daily fractions (13.42 vs 2.54). Fix: direction-aware outcomes (HOLD neutral), decided-only rates, and per-horizon Sharpe/Sortino. Stored Outcome/ROI cells are unchanged. | `scripts/track_performance.py` 6.45.0 → 6.46.0 | APPLIED · TESTED |
| D4b | **#809 review defect.** The calibration "claimed" rate averaged reliability over all records (HOLD and breakeven included), while the realized rate used decided records only. Fix: claimed rate over the same decided records; `mean_claimed_reliability` keeps the all-n average. | `scripts/track_performance.py` | APPLIED · TESTED |
| — | Manifest pins (engine 5.156.0, portfolio actions 1.16.1, tracker 6.46.0); CI enrolment | `scripts/verify_deployment.py`, `.github/workflows/ci.yml` 1.1.30, `tests/test_core_repair_tiny_quotes.py` | APPLIED |

**Not carried from #809:** the `run_shadow_scorer` 1.9.4 turnover/cost change. It is a methodology change with no switch or version-break marker, and it would restart the S-1 evaluation window. It needs an owner decision.

**Tests.** New: `tests/test_pf_weight_value_complete.py` (14 cases) and `tests/test_learning_metric_fixes.py` (30 cases). Golden negative on base `264d3a9`: 39 of 44 fail and all 44 pass with the fix. The 5 that pass on base are the no-regression cases. Both suites also pass under the lean CI lane dependencies (numpy + pytest). Whole suite on the branch: **5,482 passed, 37 skipped, 0 failed** (base 5,438 + 44 new).

**Rollback.** Revert the PR's commit. There are no flags, migrations, schema or sheet-contract changes. Behaviour visible after deploy:
- A My_Portfolio book with any unpriced holding shows blank weights and gaps with `PORTFOLIO_VALUE_INCOMPLETE`, not inflated numbers.
- Tracker win rates score SELL-family calls by direction. Sharpe/Sortino print per horizon.
- `Calibration claimed` rates move to the decided-record basis.

---

## 5. Prioritised remaining backlog

| # | Priority | Item | Dependency | Acceptance criteria | Effort |
|---|---|---|---|---|---|
| B1 | Critical | Make `refreshDecisionTop10` finish: confirm v1.13.10 is installed, check `_Run_Log` for ABORTED_PREV / ~360 s durations, re-run Top_10 and Portfolio_Decision after the sync | Apps Script access (owner) | Cockpit status OK with `pool_reconciliation_v1=` receipt; Decision Surface Freshness passes; next scheduled Advanced Sync green | 1–3 h (owner) |
| B2 | High | Render memory growth and SIGSEGV workers | Render access, profiling | 24 h without memory above 80% or code 139; consider `--max-requests` worker recycling (no cost increase) | 0.5–1 day |
| B3 | High | Deploy main (`264d3a9`) plus this PR after merge | Owner merge + manual deploy | `/meta render_git_commit` equals the merge SHA; `verify_deployment --strict` CLEAN | 15 min |
| B4 | Medium | Refresh `TFB_PF_FX_RATES` / `TFB_PF_FX_AS_OF` (61 days old) and correct the stale `APP_VERSION` env | Render env (owner) | No `PF-WEIGHT-FX … days old` log line; `app_version` = `entry_version` | 10 min |
| B5 | Medium | Decide on the shadow-scorer turnover/cost methodology (#809) with a version-break marker | Owner decision | S-1 window restart documented, or change gated | 2 h |
| B6 | Medium | Run the Top_10 cockpit check in one sync leg only, so a stuck cockpit doesn't turn three healthy data legs red | Owner decision (alerting change) | Data-leg failures reflect data problems only | 1 h |
| B7 | Low | Retire or re-date the `_Data_Audit` and `Diagnostic_Report` tabs (last written Jun 2026) | Sheet access | No stale diagnostic tabs visible to readers | 1 h |
| B8 | Low | Triage the 21 open PRs, several with overlapping version claims (engine 5.155.0 also claimed by #794) | Owner | Open PR count reduced; no duplicate version claims | 2 h |
| B9 | Low | Turn on the peg-band check (`TFB_OPP_FX_SANITY`, default off), so an inverted USD/SAR rate cannot be accepted | Owner decision | Inverted 0.2667 rate rejected in a test | 1 h |

---

## 6. Before / after

| Measure | Before (base `264d3a9`) | After (this branch) | Verified in production? |
|---|---|---|---|
| Unpriced-holding book: 2222.SR weight / action | 100% / REDUCE under `OK:SAR` | blank / no drift action, `PORTFOLIO_VALUE_INCOMPLETE: 1120.SR` | No — awaits deploy |
| `GBP:4.8,GBp:0.048` → GBP table rate | 0.048 | 4.8 (GBX 0.048) | No |
| EXIT of 10.6 shares → delta | −11 | −10 (0.6 remains in post-trade weight) | No |
| Two SELL calls that rose: win rate | 100% | 0% | No |
| 1M ROIs [2,−1,3,1]% → Sharpe | 13.42 | 2.54 | No |
| Scheduled Advanced Sync success | 0/22 | unchanged — blocked on B1 | — |
| Offline suite | 5,438 passed / 37 skipped / 0 failed | 5,482 passed / 37 skipped / 0 failed | n/a |

Forecast performance: not reported. The tracker's direction-aware metrics only take effect after deploy, so no completed-horizon sample under the corrected definitions exists yet.

---

## 7. Final verdict

- **Operationally healthy:** *Partially.* The API is up, serving the expected commit, and the market-data refresh works. But the decision layer has not produced a complete Top_10 / Portfolio_Decision refresh since 21 Oct morning. The Render workers leak memory and segfault several times a week.
- **Financially consistent:** *Not yet in production.* Three portfolio-math defects and four learning-metric definition defects are fixed and tested on the branch, but not deployed. FX operator rates are 61 days old. Broker reconciliation was not verified (IBKR deliberately not used; no statements supplied).
- **Sufficiently validated for recommendation use:** **No.** The decision surfaces are stale and the freshness gates correctly block them. Treat any current Top_10 / Portfolio_Decision content as not qualifying until B1 is resolved and the gate passes.
