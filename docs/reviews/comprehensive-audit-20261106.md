# TFB Comprehensive Audit and Handover, 6 November 2026

**Scope:** one-time audit, repair and handover across providers, the Python/API service (Render), Apps Script, Google Sheets, the dashboard, and analysis/recommendations.
**Audit window:** 6 Nov 2026, 21:20–22:30 UTC (7 Nov, 00:20–01:30 Asia/Riyadh). Report times are Asia/Riyadh; technical timestamps are UTC.
**Repository baseline:** `main` at `a85edfc` (#921, merged 5 Nov 11:12 UTC).
**Previous audits:** `comprehensive-audit-20261029.md` (backlog N1–N7, B4–B12 carried). This report builds on it and does not repeat its architecture section.
**Status labels:** FOUND · PREPARED · APPLIED · TESTED · VERIFIED IN PRODUCTION · BLOCKED · ROLLED BACK

---

## 1. الملخص التنفيذي (عربي)

**الحالة الحالية:** خدمة Render تعمل وتستجيب (فحص الصحة 200 خلال 0.3–0.5 ثانية). لم تُسجَّل أي استجابة 5xx منذ 2 نوفمبر، وسُجّل 502 واحد فقط في 1 نوفمبر. لكن الذاكرة بلغت **3.78 جيجابايت من 4.29 (88%)**، وهي أعلى من 82% في التدقيق السابق. النسخة المنشورة هي `729a316` (28 أكتوبر). الفرع الرئيسي متقدم عليها بـ43 دمجًا، غالبها سكربتات GitHub Actions وملفات SQL لا تحتاج نشرًا على Render. آخر مزامنة مجدولة (19:31 UTC) نجحت، وتقول ورقة `Plan_Of_Record` إن النشرة «EXECUTABLE» حتى 06:33 صباح 7 نوفمبر، لكن **بلا خطط ولا تذاكر تنفيذ**: «plans 0 | tickets 0».

**ما أُنجز في هذه الجلسة:**
1. **إصلاح مصدر ضغط حدود Yahoo** (PR مسودة، مُختبَر وغير منشور). تُظهر سجلات Render أن رموزًا متوقفة مثل HEWW وFM وKLD وIGN وSPLG تعيد 404 من خادمي Yahoo، ومع ذلك يعيد النظام طلب المعرّف نفسه عبر yfinance حتى ثلاث مرات ولكل صيغة من صيغ الرمز. هذا الهدر يستهلك حصة الطلبات، فيُرفض بعدها تاريخ أسعار رموز حية (EEX وBK) بخطأ 429. الإصلاح `yahoo_chart_provider` 8.16.0 مُعطَّل افتراضيًا. **عند تفعيله** يتوقف الرجوع إلى yfinance بعد 404 مؤكد من كل الخوادم، ويُحفظ الرمز كـ«مفقود» لمدة محددة. لا يُحفظ أبدًا أي فشل من نوع 429 أو 5xx أو انقطاع الشبكة. أُضيف 15 اختبارًا.
2. **تجديد الأسماء الموثقة.** كانت جميع الروابط الـ128 منتهية الصلاحية منذ 4 نوفمبر، والفحص اليومي أحمر 8 مرات من 8. شغّلتُ سير العمل الموجود مرة واحدة، فجُدِّد **122** اسمًا حتى 13 نوفمبر وبقيت 6 محجوزة. يلزم دمج طلب السحب الخاص بها.

**أهم مشكلة لم تُحل:** **لا يمكن التحقق من المحفظة ولا من صفحات القرار مباشرة.** أداة Drive تقرأ تبويبًا واحدًا فقط من ملف حجمه 14.7 ميغابايت، ويرفض التصدير الكامل لكبر الحجم. لذلك لم يمكن قراءة My_Portfolio وPortfolio_Decision وTop_10 بنسختيها. ولم يُقدَّم كشف حساب من الوسيط، **فالفرق بالريال مقابل الوسيط غير معروف، وليس صفرًا**. وفي 6 نوفمبر الساعة 15:12 بتوقيت الرياض فشلت مزامنة My_Portfolio بسبب «هوية ممسوكة أجنبية أو فارغة أو مكررة»، ثم عادت للنجاح مساءً.

**جاهزية التوصيات:** **غير جاهزة للاستخدام التنفيذي.**
- البيانات: سجل الأداء بتاريخ 7 نوفمبر الساعة 00:21، على 3,865 نتيجة محسومة.
- فئة «INVESTABLE» حققت نسبة نجاح 48.9% بمتوسط عائد ‎-0.29% قبل الرسوم، بينما حققت فئة «WATCHLIST» نسبة 55.9%، أي أن البوابة معكوسة.
- درجة Brier بلغت 0.2864 مقابل خط أساس 0.25، وفئة الثقة 85–100 نجحت بنسبة 42.7% فقط.
- **لا توجد فرصة مؤهلة** في هذا التقرير.

---

## 2. Access and baseline

| Resource | Access this session | Status |
|---|---|---|
| GitHub repo | Clone, MCP (Actions runs/logs, PRs), workflow dispatch | Inspected |
| Render `srv-d4hnir15pdvs739bqe1g` | MCP read (service, deploys, logs, metrics) + live `/health` | Inspected (read-only) |
| Google Sheet `1QaOSNHq…wsG04` | Metadata OK (" Market Share Deepseek-V3", 14.7 MB, modified 2026-11-06T21:25Z, `canEdit:false`); content read returns 1 tab; xlsx export "too large", ODS timed out, CSV = first sheet only | **BLOCKED** for My_Portfolio, Portfolio_Decision(_v2), Top_10_Investments(_v2), cash/cost ledgers. 9 diagnostic tabs partly readable |
| Apps Script project | No Apps Script API; repository copies only (`apps_script/*.gs`) | **BLOCKED** (deployed-vs-repo parity unverifiable) |
| Dashboard (chatgpt.site) | Every path returns HTTP 401 "Sign in required" (ChatGPT login gate behind Cloudflare) | **BLOCKED** (3rd audit in a row) |
| IBKR | Not used, as instructed | n/a |
| Broker statements | None supplied | Reconciliation **BLOCKED** |

**Deployed revisions**
- Render live deploy `dep-dbh6gltg1s2s73d292ag` (manual, 28 Oct 21:25 UTC) → commit `729a316` (#871). Auto-deploy off by design.
- `main` is 43 commits ahead. Of the non-test/doc/workflow/apps-script diff, only `core/providers/calendar_provider.py` is service-side Python; the rest is `scripts/` (run in Actions), `sql/bigquery/`, `config/`, `data/`. Whether the calendar change needs the web service is UNVERIFIED (it is consumed by `run_calendar_sync` in Actions).
- `/health`: 200, `service_version 8.14.8`, `entry_version 8.14.8`, **`app_version 8.14.5`**, `routes_mounted_count 7`, `global_auth_enforcement true`.

## 3. Findings

### 3.1 Render / API — FOUND

| Metric (source: Render MCP) | Value |
|---|---|
| HTTP 31 Oct–6 Nov (daily buckets) | 200: 12,912 · 401: 25 · 499: 138 · 502: 1 (1 Nov) · other 5xx: 0 |
| Peak memory 4–6 Nov (6 h buckets) | **3.78 GB / 4.29 GB = 88 %** (6 Nov 18:00Z); previous audits 84 % → 82 % → now 88 % |
| CPU (6 h avg) | 0.05–0.34 vCPU |
| Latency metric | Render returned no `http_latency` series; `/health` measured 0.28–0.51 s |

**F1 (High) — dead tickers waste the Yahoo budget and starve live symbols. FOUND → PREPARED/TESTED (I1).**
Render app logs 6 Nov 21:19–21:20 UTC: `raw chart history failed for FM.US: raw_chart_failed:http_404 — falling back to yfinance`, followed by `History fetch failed for FM.US: Too Many Requests` for the same id, repeated for the `.US` and bare spellings (HEWW, KLD, IGN, IBML, IBDO, EMBH, IECS, JKE, JKK, FM). In the same second live symbols' history was also rate-limited (`EEX`, `BK`, `SPLG`). Root cause: `fetch_history_raw` (`core/providers/yahoo_chart_provider.py`) had no miss memory, and a definitive symbol-local 404 still fell through to yfinance, which queries the same `/v8/finance/chart/{id}` endpoint with a retry ladder. The quote path already had a `quote_miss` cache; the history path did not. Several of these ids are already proven dead (FM liquidated Jan 2025; SPLG renamed SPYM — see 29 Oct audit §3.3); #921 retired 52 Mutual_Funds rows on 5 Nov, but some ids are still being requested by another caller (the caller is UNVERIFIED — the log carries no page/route context).

**F2 (Medium) — memory at 88 % of the instance limit.** No OOM restart in the window (single instance `-8ch6m` throughout). Trend is upward across three audits. Carried B7.

**F3 (Medium) — `/health` `app_version 8.14.5` ≠ `entry_version 8.14.8`.** Per `docs/render_env_contract.md` §A, `TFB_APP_VERSION` / `APP_VERSION` must be unset (service ENV and env group `tfb-server`). Provenance only; owner action.

**F4 (Low/efficiency) — repeated scoring passes.** Logs show each KSA symbol scored 3–4 times within ~20 ms with different verdicts on the first pass (e.g. `2050.SR overall=52.21 conf=35.68 REDUCE` → `57.16 conf=77.65 HOLD`; `1831.SR SELL` → `HOLD`). This matches the documented settle loop (pass-1 on partial data). Not a defect if pass-1 is never published; this was not verifiable from logs alone.

### 3.2 GitHub Actions — FOUND

Census 30 Oct 00:00Z – 6 Nov ~21:05Z, 2,182 runs (lower bound; pagination gaps possible).

| Workflow | Runs | Success | Failure | Latest |
|---|---|---|---|---|
| daily_sync | 203 (24 schedule/dispatch) | 180 | 19 (18 of 24 schedule/dispatch) | ✅ 6 Nov 19:31 |
| decision_surface_freshness | 214 (28 not skipped) | 21 | 14 | ✅ 20:45 |
| release_decision_feed | 57 | 45 | 12 | ✅ 20:49 |
| track_performance | 20 | 20 | 0 | ✅ 21:03 |
| board_scorecard | 6 | 6 | 0 | ✅ |
| ci.yml | 321 | 255 | 6 | latest main push (a85edfc) **cancelled**; no completed CI on `main` HEAD |
| **name_label_renewal** | 8 | 0 | **8** | ❌ 12:09 → fixed by I2 dispatch (✅ 21:34) |
| **provider_target_coverage** | 8 | 0 | **8** | ❌ `[CH_BASELINE_STALE] baseline is 290.7h old > max 72h` |
| **news_archive** | 12 | 1 | **11** | ❌ `identity_coverage_incomplete` (rows were written, APPEND_ACCEPTED) |
| workflow_repair_tests | 28 | 25 | 3 | ❌ on all 3 main pushes (live-state audit exits 2, not a code bug) |

Root causes:
- **daily_sync 6 Nov 12:12 UTC:** `Portfolio response contract failed: foreign, blank or duplicate holding identity` (`scripts/run_dashboard_sync.py:5361`) → My_Portfolio FAILED → `decision_surface_freshness` (`PF_SOURCE_NOT_VALID`) and `full_refresh_coverage` (My_Portfolio `fresh_pct=0.0`) red. Green again at 19:31/20:45. Which holding row tripped it is UNVERIFIED (sheet blocked). Other failures sampled: final-publication `coverage_failed` / `decision_failed` (structural, N2 of 29 Oct).
- **provider_target_coverage:** a failed run never saves a new baseline, so it cannot self-recover; needs an owner `accept_baseline`/bootstrap dispatch (not done here: accepting a baseline is a policy decision).
- **name_label_renewal:** all 128 bindings expired (3 on 19 Oct, 1 on 31 Oct, 1 on 1 Nov, 123 on 4 Nov 21:32Z) — FOUND → APPLIED (I2).
- **ci.yml 3 Nov:** lean JS suite `FAIL E5 no rows -> empty map` on #910's push; later pushes passed.
- **183 open PRs** (#1 is 387 days old; ~120 single-file "Update/Create" PRs from Aug–Sep). The merge queue remains the main bottleneck (29 Oct §4).

### 3.3 Google Sheets — FOUND / BLOCKED

Readable tabs only (quoted values):
- **`Plan_Of_Record`** (fresh, refreshed 7 Nov 00:24 Riyadh): "feed EXECUTABLE (run 42280335882, until 11-07 06:33) | plans 0 | tickets 0 | executed 0"; "latest cockpit render 2026-11-06 23:59, output QUALIFIED_UNFUNDED, 10 rows"; "exchange holidays and half days are not modelled". `_Plan_Of_Record` has headers only.
- **`_Data_Audit` stale and self-contradictory (High):** "Generated: 2026-06-19T20:53:55Z" (~140 days) yet "Refresh freshness: OK"; "Pages audited: 1", "Rows analyzed: 11". A stale tab that says OK is misleading; it should be retired or stamped stale.
- **`Diagnostic_Report` stale (Medium):** last sweep 2026-06-06.
- **`_Selection_Log` historical FX (Medium):** 17 of 103 parsed USD rows use a USD/SAR rate off the 3.75 peg by > 0.01; worst **3.7829 (30 Jul, NMM.US, GPOR.US; +0.88 %)** and **3.7317 (31 Jul, VICI; −0.49 %)**. USD/SAR is pegged at 3.75 by SAMA; deviations > 0.3 % indicate a derived/cross rate, not the peg. These are historical rows (Jul–Aug); whether current rows are on the peg is UNVERIFIED (the snippet was truncated at 7 Aug).
- **`_Selection_Log` checks that passed** (220/220 parsed rows): Price SAR = price × FX within 0.5 %; long ordering stop < entry-low ≤ entry-high < TP1 ≤ TP2; shares × price ≈ ticket; GBX treated as pence.
- **`_Shadow_Rank_Log`** current (last 6 Nov 06:22); cohort sizes irregular (11 on 3 Nov, 35 on 5 Nov vs normal 30).
- **`Data_Dictionary`:** "Country" typed `number,#,##0.00`; "Previous Close" and "P/E (TTM)" typed `string` — type metadata errors (Low).
- **My_Portfolio, Portfolio_Decision(_v2), Top_10_Investments(_v2), cash/cost-basis:** **BLOCKED** (UNVERIFIED). No holdings, quantities, cash, SAR total or v1/v2 comparison could be recomputed.

### 3.4 Portfolio and consolidated SAR — BLOCKED
The 29 Oct audit recorded one holding (FRST.US, −4.9 %, `BLOCKED; RECONCILIATION_REQUIRED: missing_evidence | [custody-unverified]`). Current holdings and cash could not be read. **Broker difference in SAR: unknown, not zero.** USD→SAR method in code: SAMA peg 3.75 with provenance (29 Oct verified); historical off-peg rows above are a separate defect.

### 3.5 Recommendations and forecast evaluation — FOUND

Source: Performance Clock run [42293576098](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/42293576098) (7 Nov 00:21 Riyadh), `track_performance` v6.53.0. Records 28,465; matured 13,001; **decided 3,865**. Gross of fees and slippage. Same-day repeat keys are measured, not de-duplicated, so n overstates independent evidence.

| Slice | Decided n | Win % [95 % CI] | Mean ROI | Stated reliability |
|---|---|---|---|---|
| INVESTABLE | 1,242 | **48.9** [46–52] | −0.29 % | 71 |
| WATCHLIST | 2,405 | 55.9 [54–58] | +0.38 % | 61 |
| BLOCKED | 204 | 76.5 [70–82] | −2.14 % | 55 |
| Reliability 85–100 | 171 | **42.7** [36–50] | −1.00 % | 87 |
| Board executions | 27 | 55.6 [37–72] | +0.97 % | 75 |
| 1W target achievement | 106 | 46.2 | MAE 5.02 pp, bias −1.28 pp | — |

- **Brier 0.2864 vs 0.25 baseline** (29 Oct: 0.2859) — no better than a coin flip. Reliability not monotonic; WATCHLIST > INVESTABLE inversion persists.
- BLOCKED shows a 76.5 % win rate with a −2.14 % mean ROI. A win definition that disagrees in sign with mean return should be checked (direction convention or a few large losses). FOUND, not investigated further.
- Canonical-performance provenance counts: `unproven_forecast_provenance` 23,166 of 28,465; `percent_points` 3,683; `ambiguous_units` 752; `schema_price_mismatch` 633 — most stored forecasts cannot prove they were recorded before the outcome, which limits any learning claim.
- Sharpe (gross): 1W −0.51, 2W −0.51, 1M 0.17, 3M −0.39.

**Readiness: NOT READY.** The live feed carries 0 tickets, which is consistent with the evidence. **No qualifying opportunity** is listed in this report.

### 3.6 Manual refresh, Apps Script triggers, dashboard — BLOCKED
Unchanged: these need the Apps Script editor, sheet edit rights and dashboard sign-in. `manual_refresh_priority.yml` last ran 13 Oct (success), `page_refresh_recovery.yml` 23 Oct. Nothing is claimed about end-to-end manual refresh behaviour.

## 4. Implementation register

| # | Defect and root cause | Change | Files and links | Tests | Status |
|---|---|---|---|---|---|
| I1 | Dead ids 404 on both raw hosts, then yfinance re-asks the same chart id (up to 3 tries, per spelling, per caller) → 429s that also hit live symbols' history | `yahoo_chart_provider` 8.16.0: with `TFB_YC_HISTORY_MISS_TTL_SEC` > 0, a symbol-local 404 on every host returns `[]` without the yfinance fallback and is held per Yahoo id for the TTL (bounded 60–86,400 s). 429/5xx/auth/network outcomes are never cached. Default 0 = byte-identical to 8.15.1 | `core/providers/yahoo_chart_provider.py`, `scripts/verify_deployment.py` (pin + FLAGS row), `docs/render_env_contract.md` (regenerated, `--check` OK), `tests/test_ycp_history_miss_cache.py`, `tests/test_ycp_loopguard_b6.py` (version pin); branch `claude/sharp-heisenberg-ompct5`, commit `5563209` | 15 new tests pass; all 15 fail on the base commit. Related suites (29 files): 673 passed, 23 skipped; 12 failures (`test_auth_wall_scope_path` W*, `test_fw2_stale_pe` w1b) fail identically on base (environment) | **PREPARED · TESTED**. Not merged, not deployed |
| I2 | All 128 verified Name captions expired (4 Nov 21:32Z); daily check red 8/8 | Dispatched the existing `name_label_renewal.yml` once (mode=renew, publish_branch=true) | Run [42297810424](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/42297810424) ✅; branch `name-label-renewal/42297810424`: 122 bindings → expire 2026-11-13, 6 held | The workflow validates with the production loader | **APPLIED** (branch + draft PR), awaiting owner merge |

**To arm I1 (owner):** merge → Render manual deploy → set `TFB_YC_HISTORY_MISS_TTL_SEC=21600` on the service. Acceptance: in the next 24 h of Render logs, `YC-MISS` lines appear for dead ids, `History fetch failed … Too Many Requests` count falls versus the same window today, and no live symbol is held as a miss.
**Rollback:** I1 — unset the variable (restart) or revert the commit; the off state is byte-identical. I2 — close/revert the renewal PR. Nothing touched a gate, threshold, the workbook or Render configuration.

## 5. Before / after metrics

| Metric | 29 Oct audit | This audit | Note |
|---|---|---|---|
| Render /health | 200 | 200 (0.28–0.51 s) | |
| Render 5xx | 0 (~24 h) | 1 × 502 in 7 days | |
| Render peak memory | 82 % | **88 %** | worse |
| Live deploy vs main | equal | 43 commits behind (mostly Actions-side) | |
| Scheduled daily_sync failures | 19/20 | 18/24 (window 30 Oct–6 Nov) | latest green |
| Name captions valid | 4/128 on main (123 pending #873) | 0/128 on main → 122/128 on renewal branch | |
| provider_target_coverage | — | 0/8 green (stale baseline) | |
| news_archive | red | 1/12 green | |
| INVESTABLE decided win rate | 48.8 % (n=1,042) | 48.9 % (n=1,242) | overlapping samples |
| Brier | 0.2859 | 0.2864 | |
| Executable tickets in live feed | 0 | 0 | |
| Broker difference (SAR) | unknown | unknown | no statement |

No overall accuracy or improvement percentage is claimed. I1 has no production effect until it is armed.

## 6. Prioritized remaining backlog

| ID | Priority | Item | Dependencies | Acceptance criteria | Effort |
|---|---|---|---|---|---|
| R1 | Critical | Keep recommendations fail-closed (N7): de-duplicate same-day keys, evaluate net of costs, fix the INVESTABLE/WATCHLIST inversion before any promotion | Dedup decision | Calibrated on de-duplicated keys vs a benchmark after fees; Brier < 0.25 on a chronological holdout | 1–2 days |
| R2 | High | Give the audit read access to the decision tabs (a per-tab CSV export by gid, a read-only Sheets API credential, or a small JSON snapshot published by the existing sync) | Owner | My_Portfolio, Portfolio_Decision(_v2), Top_10(_v2) recomputed in SAR next audit | 2–4 h |
| R3 | High | Broker reconciliation (N6) | Owner statement/export at a cutoff | Positions, qty, cash and cost basis matched; differences in SAR | 0.5 day after the file |
| R4 | High | Find the My_Portfolio "foreign, blank or duplicate holding identity" row from 6 Nov 12:12Z | Sheet access | Root cause named; recurrence prevented or explained | 2 h |
| R5 | High | Merge the renewal PR (I2); re-dispatch weekly until renewal is automated (N5) | Owner merge | Name-label check green; 13 Nov expiry renewed | 10 min/week |
| R6 | High | Merge + arm I1; then add the still-requested dead ids to `TFB_YC_SYMBOL_SKIP` or retire them at the source (N3) | Owner merge/deploy | Fewer 429s in Render logs; no live symbol held as a miss | 30 min |
| R7 | Medium | Re-bootstrap `provider_target_coverage` baseline | Owner policy dispatch | Workflow green on next scheduled run | 15 min |
| R8 | Medium | Render memory at 88 % (B7): profile caches / worker count before any plan change | none | Peak < 80 % over 7 days, or a costed proposal | 0.5 day |
| R9 | Medium | Retire or stamp-stale `_Data_Audit` and `Diagnostic_Report` (June) so they cannot read "OK" | Owner (Apps Script) | Tab shows its age, or is removed | 1 h |
| R10 | Medium | Remove `TFB_APP_VERSION`/`APP_VERSION` from Render ENV and `tfb-server` | Owner | `/health` app_version == entry_version | 5 min |
| R11 | Medium | Investigate BLOCKED bucket: 76.5 % wins with −2.14 % mean ROI | none | Win definition and sign convention documented | 2 h |
| R12 | Medium | Complete CI on `main` HEAD (latest push run was cancelled) | none | One green ci.yml on `a85edfc` or later | 15 min |
| R13 | Enhancement | Dashboard: document its data source, refresh cadence and currency rules in `docs/`; provide a reviewer path that needs no personal login (B12) | Owner | Next audit reviews every page | 2 h |
| — | — | Carried: N2 structural red on final publication (#831, #846), N4 Open=0.0 null-vs-zero, B4 cron delay, B9 FX fallback age, B11 manual refresh | see earlier reports | unchanged | — |

## 7. Final verdict

- **Operationally healthy?** **Partly.** The API is up with no 5xx since 1 Nov, and the latest sync and release cycle was green. But memory is at 88 %, three workflows are permanently red (name labels, which this session addressed, provider coverage and news archive), the Yahoo budget is wasted on dead ids (fix prepared), and `main` HEAD has no completed CI.
- **Financially consistent?** **Not verifiable end-to-end.** Historical selection-log arithmetic is internally consistent (220/220). Some historical USD rows used off-peg FX. Holdings, cash and the SAR totals could not be read, and broker reconciliation is blocked.
- **Sufficiently validated for recommendation use?** **No.** INVESTABLE wins 48.9 % gross, the top reliability band is anti-calibrated (42.7 %), and Brier is 0.2864 against 0.25. The system correctly publishes 0 executable tickets.

*Decision support only; not investment advice. No recurring tasks, schedules or monitoring were created. The one workflow dispatch (name-label renewal) was a single manual run of an existing workflow; it pushes a review branch and never writes to `main`.*
