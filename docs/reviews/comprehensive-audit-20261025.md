# TFB comprehensive audit — 25 Oct 2026 (one-time)

Audit start: 24 Oct 2026 21:26 UTC (25 Oct 00:26 Asia/Riyadh).
Base commit: `4123c83` (main, #834). Live Render deploy: `115200a` (#833),
deploy `dep-dbegqei9lscs73b41ntg`, live since 24 Oct 19:57 UTC. The only
difference between live and main is `apps_script/10_My_Portfolio.gs` and its
tests (#834). Apps Script is pasted by hand, so it does not ship with a Render
deploy.

This audit follows the 22 Oct audit (`comprehensive-audit-20261022.md`) and the
24 Oct revisions. It does not repeat their findings. It records the current
state and fixes five new reproduced defects: three are applied and tested, one
is applied behind its existing flag, and one is prepared as a patch.

Status labels: FOUND · PREPARED · APPLIED · TESTED · VERIFIED IN PRODUCTION · BLOCKED · ROLLED BACK

---

## 1. الملخص التنفيذي (عربي)

**الوضع الحالي:** خدمة Render تعمل بشكل سليم. نقاط الفحص `/health` و`/readyz` و`/meta` ردّت بـ 200 خلال 0.3–0.8 ثانية. الخدمة تشغّل الإصدار `115200a`، وهو مطابق لفرع main في كل كود Python. خلال آخر 24 ساعة لم يُسجَّل أي خطأ 5xx، ولا يوجد أي حدث تعطّل أو قتل بسبب الذاكرة خلال 7 أيام. لكن الذاكرة ما زالت تتراكم: وصلت إحدى النسخ إلى 3.92 من 4.29 غيغابايت (91%) قبل أن يعيد نشرٌ يدوي تشغيلها. في المساء أُصدرت نشرة القرار بنجاح (`released=true` الساعة 20:09 UTC، وصلاحيتها حتى 03:04 بتوقيت الرياض). أما الإخفاقات الحمراء اليوم فكلها ضوابط تعمل كما صُممت، وليست أعطالاً في الكود: صفحات قرار أقدم من مصدرها، وتغطية هويات الأخبار غير مكتملة.

**ما أُنجز في هذا التدقيق:**

1. **سعر صفري يضخّم الأوزان:** سهم مملوك جاء سعره صفراً بسبب فشل التسعير كان يُعامل كقيمة حقيقية. النتيجة: خسارة وهمية بنسبة −100%، ووزن 100% لبقية المحفظة، وتوصية REDUCE خاطئة. الآن يُعامل كسعر مفقود، فتُترك أوزان المحفظة كلها فارغة مع الحالة `PORTFOLIO_VALUE_INCOMPLETE`.
2. **المراكز القصيرة:** الكمية السالبة كانت تنتج أوزاناً سالبة لا يصل مجموعها إلى 100%، ونسبة ربح بإشارة معكوسة. الآن تُترك الأوزان فارغة مع الحالة `PORTFOLIO_SHORT_UNSUPPORTED`، وتتبع إشارة نسبة الربح إشارة الربح نفسه.
3. **إفصاح سعر الصرف:** إدخال سعر للبنس `GBp` كان يُخفي أن سعر الجنيه `GBP` المستخدم تقديري ثابت. تم الإصلاح.
4. **أسهم الفئات الأمريكية:** رمز مثل `BRK.B.US` كان يصل إلى Yahoo بصيغة النقطة فيُرفض (404). الآن يتحول إلى `BRK-B`. الإصلاح يعمل خلف المفتاح الموجود أصلاً، وهو ما زال مطفأً في الإنتاج.
5. **النقد المعتمد من IBKR:** إذا كان النقد الوحيد بالريال ووُجد أمر شراء، كان النقد المعتمد يُبالغ فيه (8,000 بدلاً من 2,500 ريال في المثال). الإصلاح جاهز كملف تعديل لـ Apps Script (PREPARED)، ويحتاج لصقاً يدوياً من المالك.

**المشكلة الرئيسية غير المحلولة:** صفحات القرار تتأخر عن المصدر بعد كل مزامنة. مثال من اليوم: Top_10 صدرت الساعة 16:44 بتوقيت الرياض، بينما صدرت المصادر الساعة 19:04–19:08، ورسالة Portfolio_Decision المضمّنة فارغة (EMPTY). لذلك ترفض بوابة Decision Surface Freshness النشر في معظم الدورات. هذا يتطلب إجراءً في Apps Script أو جدولة سلسلة القرار، ومشروع Apps Script غير متاح لي (BLOCKED).

**جاهزية التوصيات:** **جاهزة جزئياً فقط.** النشرة الحالية مقبولة حتى 03:04 بتوقيت الرياض. لا تعتمد على أي صفحة قرار لم تجتز بوابة الحداثة في الدورة نفسها. الحساب كله نقد منذ 22 أكتوبر، ومطابقة الوسيط لم تُتحقق في هذا التدقيق. إصلاحات هذا التدقيق مُختبرة لكنها **لم تُنشر بعد**، والنشر يدوي ويقرره المالك.

---

## 2. Access and baseline

| Resource | Access | Notes |
|---|---|---|
| GitHub repo | Full (branch + PR) | 792 tracked files; 38 workflows; ~50 open PRs (oldest from 31 Aug) |
| GitHub Actions | Read (runs, jobs, logs, artifacts) | Nothing re-run or dispatched |
| Render `srv-d4hnir15pdvs739bqe1g` | Read (service, deploys, events, logs, metrics) | Auto-deploy **off**. No deploy or env change made by this audit |
| Google Sheet `1QaOSNH…` | **Partial** — metadata and the first 5 tabs via a Drive text snippet | Decision tabs, My_Portfolio, `_Status`, `_Run_Log`, ledger and history tabs **BLOCKED**: the export ends inside `_Selection_Log`; no range or xlsx reader is available. File modified 24 Oct 21:25:42 UTC (live writers are active) |
| Apps Script `1IvEj8…` | **BLOCKED** — "Requested entity was not found" (container-bound) | Repo copy in `apps_script/` could not be compared with the installed copy |
| Dashboard site | **BLOCKED** — HTTP 401 "Sign in required" | Not inspected |
| IBKR | Not used (per instructions) | Broker reconciliation **not verified**. The repo records an all-cash account since the 22 Oct DDI sale (`10_My_Portfolio.gs` v1.5.15 header) |

### Baseline metrics

| Metric | Value | Window / source |
|---|---|---|
| Live probes `/health /readyz /meta` | 3/3 HTTP 200, 0.28–0.76 s; `routes_failed_count 0` | 24 Oct 21:26 UTC, one probe each |
| Version reporting | `app_version 8.14.5` vs `entry_version 8.14.7` | Stale `APP_VERSION` Render env (open since 22 Oct, B4) |
| HTTP status | 23 Oct 21:00 → 24 Oct 21:00 UTC: 200 = 1,687, 401 = 29, 499 = 21, 5xx = 0. Since 24 Oct 21:00 UTC: 200 = 2,039 · 401 = 5 · 499 = 17 · 5xx = 0 | Render metrics, daily buckets |
| Latency p50/p95 | **not available** (`http_latency` returned empty) | — |
| Server failures (OOM, crash, health) | **0** events | Render events, 7 days |
| Worker SIGSEGV / code 139 | **0** log lines | 21–24 Oct (the 22 Oct audit counted 5 in the 7 days before) |
| Peak memory per instance | 3.92 GB / 4.29 GB (91%) on instance `w2kq2`, 24 Oct 11:00–13:00 UTC, reset by the 14:40 deploy | Render metrics, 2 h buckets |
| Deploys | 8 manual/API deploys in 48 h; each one resets memory | Render deploys |
| Operator FX age | `TFB_PF_FX_AS_OF` **66 days** old, limit 30 | Render warning, 24 Oct 16:04 UTC |
| Yahoo dot-form class tickers | `$BRK.B` 404 ("possibly delisted") in production | Render error log 24 Oct 21:10 UTC; Yahoo chart `BRK-B` = 200, `BRK.B` = 404 (checked 21:3x UTC) |
| Main-branch failed runs on 24 Oct | 8 red: news-archive ×2, Advanced Sync ×1, Decision Surface Freshness ×2, Release Decision Feed ×2 (+ green-but-not-accepted scheduled release 40127064446) | Actions logs (section 3) |
| Decision feed | Released at 20:09 UTC from source run 40109762443: `accepted=true, released=true, audit_exit_codes [0,0]`, expires 25 Oct 03:04 Riyadh | Run 40128541279 |
| Offline test suite at base | CI green on main for #833 / #834 push | GitHub Actions `✅ CI` |

---

## 3. Architecture findings (Providers → Python/API → Apps Script → Sheets → dashboard)

1. **Providers → Render.** Healthy apart from Yahoo. Error logs are dominated by Yahoo 404s for real and delisted tickers (EMBH, HWAY, IGN, GXG, JKK, BEDZ, DVIN, FM, BRK.B). **FOUND (High, fixed here, F4):** `BRK.B` is a live NYSE share class. Yahoo needs `BRK-B`. `to_yahoo_symbol` has had a dash rule since v5.3.2 behind `TFB_YAHOO_SHARE_CLASS_DASH` (default off), but the `.US` branch returned before reaching it. So the EODHD-style `BRK.B.US` stayed in dot form even with the flag on.
2. **Render runtime.** No crashes in 7 days. Memory still grows on every instance (0.56 → 2.2 GB in 2 h after the 19:57 deploy; 3.92 GB peak on the morning instance). Frequent deploys are what currently prevents OOM kills (B2 from 22 Oct, still open).
3. **GitHub Actions.** All 8 red runs on 24 Oct are fail-closed controls, not code defects:
   * `news-archive` (40114359237, 40110932603): `fetched 60/60`, `APPEND_ACCEPTED`, but `failure_codes ["identity_coverage_incomplete"]` with 18 unknown identities (e.g. `SYDB.CO, 2899.T, ALFAA.MX, KOZAA.IS, PE&OLES.MX, SPLG, JKJ.US`). This is data hygiene in the source sheets (`scripts/news_archive_collector.py:540`).
   * Advanced Sync 40109762443 → `blockers ["decision_failed"]`. Decision Surface Freshness 40114406991 / 40108445333: `PF_STATUS_NOT_OK (Embedded status is EMPTY)`, `T10_OLDER_THAN_SOURCE` ×4 (Top-10 16:44:12 Riyadh vs sources 19:04–19:08), `T10_POOL_RECONCILIATION_UNPROVEN`.
   * Release Decision Feed 40118389197 refused `newer_run` (an operator passed an older source). 40119170200 was `not_accepted` on the same stale surfaces. The same source run was released at 20:09 UTC, once the surfaces caught up.
   * Open PR #831 (`TFB_RELEASE_INFLIGHT_DEFER`) addresses none of these.
4. **Apps Script decision layer.** The decision pages still lag every sync, and the freshness gate correctly refuses most cycles. The Apps Script project is **BLOCKED**, so I cannot tell whether the installed copy matches the repo (v1.14.0 Top-10, v1.5.15 My_Portfolio).
5. **Portfolio math (`core/data_engine_v2.py`).** **FOUND, fixed here (F1–F3):** three remaining gaps in the v5.156.0 fail-closed weight contract (section 4).
6. **IBKR evidence reader (`apps_script/10_My_Portfolio.gs` `pdIbkrPlan_`).** **FOUND, prepared (F5):** a working BUY limit order is reserved in the units of the single cash currency. Orders carry no currency and links are USD stocks only, so SAR-only cash under-reserves by a factor of 3.75.
7. **FX configuration.** Operator non-peg rates are 66 days old (Render warning on every My_Portfolio pass). Rows priced in a floating currency are disclosed per row as `INDICATIVE:<ccy>` (F3 restores this disclosure for the `GBp` case). Pegged GCC currencies and USD 3.75 are built-in constants and correct.
8. **Legacy tabs.** `_Data_Audit` (2026-06-19), `Diagnostic_Report` (2026-06-06) and `_Selection_Log`, whose visible rows end 2026-08-08, are the first tabs a reader sees. They are stale and their dates can mislead. Backlog B7, still open.

### Page-by-page assessment

| Page | Producer | State at audit | Assessment |
|---|---|---|---|
| Market_Leaders / Global_Markets / Commodities_FX / Mutual_Funds | `run_dashboard_sync.py` (Actions) | Source status 19:04–19:08 Riyadh, 24 Oct | Data refresh working. **BLOCKED** for direct cell inspection |
| My_Portfolio | sync + `10_My_Portfolio.gs` | Not readable | Engine weights affected by F1–F3 in edge cases (fixed, not deployed). Operator FX 66 days old |
| Portfolio_Decision | Apps Script | Embedded status EMPTY at 17:03 UTC check | Repo records an all-cash book. v1.5.15 (#834) writes `ok` for a broker-proven all-cash book, if installed |
| Top_10_Investments | `16_Decision_Top10.gs` | Last render 16:44 Riyadh before the 19:0x sync; release accepted at 20:09 UTC | Use only while the released feed is unexpired and the gate passes |
| v2 decision pages, `_Status`, `_Run_Log`, signal histories | Apps Script / scripts | **BLOCKED** | — |
| `_Data_Audit`, `Diagnostic_Report`, `_Selection_Log` | legacy | June to Aug 2026 | Stale. Misleading if read as current |
| Dashboard site | external | **BLOCKED** (401) | — |

---

## 4. Implementation register

Branch `claude/vibrant-bell-7w3gch`, one draft PR. Nothing is deployed: Render auto-deploy is off, and deploying is the owner's call.

| ID | Defect and root cause | Files | Status |
|---|---|---|---|
| F1 | **A held row quoted at 0 inflated every other weight.** `_compute_position_math` accepted price 0, so `position_value = 0` passed the v5.156.0 `is None` check and `total_mv` skipped it. Repro: 2222.SR 100@0 + 1120.SR 100@90 → 1120.SR weight 100, gap −50, REDUCE, 2222.SR P&L −100%, all under `OK:SAR`, while `_insights_base_totals` already rejected the same book. Fix: a price ≤ 0 is treated as missing (it falls back to `price`), and a held row valued ≤ 0 blanks the book with `PORTFOLIO_VALUE_INCOMPLETE`. | `core/data_engine_v2.py` 5.158.0 → 5.159.0 | APPLIED · TESTED |
| F2 | **Shorts produced negative weights and a sign-flipped P&L %.** `unrealized_pl / position_cost` with a negative cost. Repro: AAPL −10 @ cost 100, price 90 → P&L +100 but −10%, weight −37.5%, weights summed to 62.5%, ADD/REDUCE emitted. portfolio_actions and the broker capture already reject shorts. Fix: a negative quantity blanks the book with `PORTFOLIO_SHORT_UNSUPPORTED: <symbols>`; the P&L % divides by `abs(cost)` (+10%). | `core/data_engine_v2.py` | APPLIED · TESTED |
| F3 | **An operator `GBp` key hid the INDICATIVE GBP disclosure.** `_operator_set` upper-cased keys, while the FX table files `GBp` under GBX. Repro: `fx_rates={"GBp":0.048}` with a GBP row → `OK:SAR` with no disclosure, though the row used the built-in 4.80. Fix: `_operator_set` uses `_pf_fx_key`. Weights are unchanged. | `core/data_engine_v2.py` | APPLIED · TESTED |
| F4 | **`.US` share-class tickers reached Yahoo in dot form.** `to_yahoo_symbol` returned the stripped base for `ex == "US"` before the v5.3.2 dash step. Fix: the same dash rule, under the same flag, in the `.US` branch. With the flag unset the output is byte-identical. | `core/symbols/normalize.py` 5.6.0 → 5.6.1 | APPLIED · TESTED (flag still off in production) |
| F5 | **IBKR certified cash overstated for SAR-only cash with a BUY order.** Repro (vm harness, real file): SAR 10,000 settled + BUY 10 @ 200 → `cashCents 800000` (8,000 SAR); expected 2,500 SAR or withheld. The patch declares reservations incomplete (`BUY order currency unproven`) unless the only cash currency is USD. USD output is unchanged (250001 cents). It is not applied to the `.gs` because the v1.5.15 golden-diff harnesses pin the file; applying it means bumping to v1.5.16 and moving those pins in the same delivery. | `docs/evidence/patches/10_My_Portfolio_v1.5.16_ibkr_usd_reservation.patch` | PREPARED (verified on a patched copy) |
| — | Manifest pins (engine 5.159.0, normalize 5.6.1); CI v1.1.36 enrols both new suites in the lean model lane | `scripts/verify_deployment.py`, `.github/workflows/ci.yml`, `tests/test_core_repair_tiny_quotes.py` | APPLIED |

**Tests.** New `tests/test_pf_weight_zero_short_fx.py` (13 cases) and `tests/test_yahoo_share_class_dash.py` (3 cases). Golden negative: on base `4123c83`, 12 of the 16 fail and all 16 pass with the fix. The 4 that pass on base are the no-regression cases (positive long book, zero-qty watch row, operator GBP key, flag-off identity). The neighbouring suites `test_pf_weight_value_complete`, `test_pf_weight_subunit_fx`, `test_core_repair_tiny_quotes` and `test_verify_manifest_pins` pass. Whole suite on the branch (`pytest -n 4`, Python 3.11, requirements.txt
pins): **5,987 passed, 48 skipped, 1 failed**. The failure,
`test_sync_publication_clock.py::test_current_producer_keeps_consumer_compatible_wire_format[America/New_York]`,
passes in isolation on both base and branch (102/102). It is an ordering
interaction under xdist in an unrelated module, not caused by this change.

**Rollback.** Revert the PR commit. There are no migrations, sheet-contract or schema changes, and no new flags. Behaviour visible after deploy:

- A My_Portfolio book with a 0-priced or short holding shows blank weights and gaps with a named status, instead of inflated numbers and drift actions.
- A short's P&L % sign follows its P&L.
- `INDICATIVE:GBP` appears when an operator supplies only a pence rate.
- F4 is inert until `TFB_YAHOO_SHARE_CLASS_DASH=1`.

---

## 5. Prioritised remaining backlog

| # | Priority | Item | Dependency | Acceptance criteria | Effort |
|---|---|---|---|---|---|
| B1 | Critical | Decision pages lag the sync (Top-10 16:44 vs sources 19:0x; PF status EMPTY). Make the post-sync decision chain run (open PR #788 `26_Decision_Chain`, or the installed trigger) and confirm v1.5.15 / v1.14.0 are pasted | Apps Script access (owner) | Decision Surface Freshness passes on the scheduled sync; `PF_STATUS_NOT_OK` absent | 1–2 h owner |
| B2 | High | Merge this PR and deploy; then set `TFB_YAHOO_SHARE_CLASS_DASH=1` on Render | Owner merge + manual deploy | `/meta render_git_commit` = merge SHA; `verify_deployment --strict` CLEAN; no `$BRK.B` 404 lines; BRK.B.US priced from Yahoo when EODHD is missing | 20 min |
| B3 | High | Render memory growth (91% peak 24 Oct) | Profiling; no cost change | 24 h without memory above 80% and without a deploy; consider gunicorn `--max-requests` with jitter | 0.5–1 day |
| B4 | Medium | Refresh `TFB_PF_FX_RATES` / `TFB_PF_FX_AS_OF` (66 days) and fix the stale `APP_VERSION` (8.14.5 → 8.14.7) | Render env (owner) | No `PF-WEIGHT-FX … days old` line; `app_version` = `entry_version` | 10 min |
| B5 | Medium | Apply F5 (10_My_Portfolio v1.5.16): bump the version, move the v1.5.15 harness pins, add P07b (SAR-only cash + BUY → withheld) | Owner paste into Apps Script | Patched harness green; Logger line shows v1.5.16 | 1 h + owner paste |
| B6 | Medium | Clear the 18 unknown news identities (`SYDB.CO`, `2899.T`, `ALFAA.MX`, `KOZAA.IS`, `PE&OLES.MX`, `SPLG`, `JKJ.US`, …) in the source sheets or lifecycle registry | Sheet access | `news-archive` green with `identity_coverage_complete true` | 1–2 h |
| B7 | Low | Retire or re-date `_Data_Audit`, `Diagnostic_Report` and `_Selection_Log` (June–Aug) | Sheet access | No stale diagnostic tab visible as first tab | 1 h |
| B8 | Low | Triage ~50 open PRs (many Aug–Sep owner patch branches; overlapping version claims such as #794 engine 5.155.0) | Owner | Open PR count reduced; no stale version claims | 2 h |
| B9 | Low | Regenerate `docs/flags.md` (line references already out of sync on base) and consider a CI freshness check | — | `scripts/flag_registry.py` output equals the committed file | 30 min |

---

## 6. Before / after

| Measure | Before (`4123c83`) | After (this branch) | Verified in production? |
|---|---|---|---|
| 2222.SR 100@0 + 1120.SR 100@90: 1120.SR weight / action | 100% / REDUCE under `OK:SAR` | blank / HOLD, `PORTFOLIO_VALUE_INCOMPLETE: 2222.SR` | No (awaits deploy) |
| 2222.SR 100@0: unrealized P&L | −2,500 (−100%) | blank (missing price) | No |
| Short AAPL −10 @100 → 90: P&L % / weight | −10% / −37.5% | +10% / blank, `PORTFOLIO_SHORT_UNSUPPORTED: AAPL` | No |
| `fx_rates={"GBp":0.048}` + GBP row: status | `OK:SAR` | `OK:SAR INDICATIVE:GBP` | No |
| `to_yahoo_symbol("BRK.B.US")`, flag on | `BRK.B` (Yahoo 404) | `BRK-B` (Yahoo 200) | No (flag off in production) |
| IBKR SAR-only 10,000 + BUY 10@200: certified cash | 8,000 SAR | withheld (`BUY order currency unproven`) | No (prepared) |
| 5xx, 24 h | 0 | — | Yes (baseline) |
| Server failures, 7 days | 0 | — | Yes (baseline) |

Forecast performance is not re-reported here. The 24 Oct decision-integrity revision measured 11,773 unique valid matured records with 54.77% directional correctness (1,849 wins / 1,527 losses; HOLD and breakeven excluded; overlapping horizons are correlated; not net of costs). S-1 was last read `NOT_DECIDABLE` at 21/28 scored days. No completed-horizon sample changed during this audit, and nothing here alters ranking, scoring or the learning pipeline.

---

## 7. Final verdict

- **Operationally healthy:** *Mostly.* The API is up with 0 5xx and no crash events in 7 days, and market-data syncs complete. Memory still climbs to about 90% of the limit between deploys, and the decision pages lag the sync often enough that most release attempts are correctly refused.
- **Financially consistent:** *Improved, not yet in production.* Three My_Portfolio weight and P&L edge-case defects are fixed and tested on the branch. The IBKR SAR-cash reservation defect is prepared as a patch. Operator FX rates are 66 days old. Broker reconciliation was **not verified** (IBKR deliberately not used; no statements supplied; the account is recorded as all cash since 22 Oct).
- **Sufficiently validated for recommendation use:** **Conditionally.** Act only on a decision feed released by Release Decision Feed with `accepted=true` that has not expired (the current one expires 25 Oct 03:04 Riyadh), and only while Decision Surface Freshness passes for the same cycle. Directional correctness of about 55% on overlapping horizons is not evidence of net profitability. No predictive improvement is claimed by this audit.
