# TFB Comprehensive Audit — 27 Nov 2026 (one-time)

Run: 28 Nov 2026 00:20–01:10 Riyadh (27 Nov 21:20–22:10 UTC). Branch `claude/sharp-heisenberg-bww453`, PR #1110.
Status labels: FOUND · PREPARED · APPLIED · TESTED · VERIFIED IN PRODUCTION · BLOCKED · ROLLED BACK.

---

## 1. الملخص التنفيذي (العربية)

**الوضع الحالي:** الخدمة على Render تعمل وتستجيب، وتشغّل آخر إصدار من الفرع الرئيسي (`7f3181d`، نُشر الساعة 23:28 بتوقيت الرياض). من آخر 300 تشغيل لـ GitHub Actions (من 16:42 إلى 23:51 بتوقيت الرياض يوم 27 نوفمبر) فشل تشغيل واحد فقط. كل نقاط فحص الصحة تُرجع 200 خلال أقل من ثانية.

**أهم ما وُجد:** لم تصل تسميات الأسماء الموثّقة إلى الفرع الرئيسي، وعددها 122 من 128. التجديد التلقائي يعمل ويعيد التحقق من المصادر، لكن GitHub يمنع Actions من فتح طلب الدمج، والسر `TFB_RENEWAL_TOKEN` غير مُعرَّف، فيبقى الفرع المجدَّد دون دمج. إن لم يُدمج التجديد قبل **29 نوفمبر الساعة 13:34 بتوقيت الرياض** تنتهي صلاحية التسميات الـ122 وتفرغ خانات الأسماء. هذا الفراغ قد يُفشل حد تغطية الأسماء ويوقف نشر القرار، وقد حدث ذلك في 19 أكتوبر و20 نوفمبر.

**ما أُنجز:**

1. **التجديد (APPLIED، TESTED):** طبّقت تجديد التشغيل المجدول على الفرع الرئيسي الحالي بقاعدة المشروع نفسها، فجُدّدت 122 تسمية حتى 4 ديسمبر وبقيت 6 محجوزة ومعترف بها. الفحص أصبح يمر بعد أن كان يفشل.
2. **إفصاح سعر الصرف (APPLIED، TESTED):** أصبح الإفصاح يذكر سعر الصرف الثابت المستخدم لمراكز الجنيه الإسترليني بالبنس وما يماثلها، ولم يكن يذكره من قبل. لا تتغير أي قيمة أو توصية.

**المشكلة الرئيسية التي لم تُحل:** يلزم دمج PR #1110 قبل الموعد المذكور، ثم تعريف السر `TFB_RENEWAL_TOKEN` حتى لا تتكرر المشكلة. تعذّر عليّ أيضاً قراءة صفحات الجدول الرئيسية: فهو بحجم 22 ميغابايت ولم تُرجع أداة Drive إلا تبويباً تشخيصياً واحداً. كما أن صفحة اللوحة تتطلب تسجيل الدخول (401). لذلك **لم أتحقق** من قيم المحفظة بالريال ولا من قائمة أفضل 10.

**جاهزية التوصيات:** النظام سليم تشغيلياً، لكن قدرته التنبؤية **غير مثبتة إحصائياً**. حسب تقرير المشروع الصادر اليوم، المنافس التجريبي t = +0.47 على 37 يوماً، والحد المطلوب t ≥ 2. لذلك تُعامل التوصيات كأبحاث لا كإشارات مُعايَرة.

---

## 2. Detailed report (English)

### 2.1 Access and baseline

| Resource | Access | What was verified |
|---|---|---|
| GitHub repo | ✅ read/write (branch + PR) | HEAD `7f3181d` (#1109); 25 open PRs, most of them stale daily-flag PRs; workflow runs |
| Render `srv-d4hnir15pdvs739bqe1g` | ✅ read (service, deploys, logs, metrics, events) | Live deploy `dep-dc4ufdm1egvs739t66c0` = commit `7f3181d`, finished 20:28:48 UTC; autoDeploy **off** (manual deploys) |
| Live API `tadawul-fast-bridge.onrender.com` | ✅ public health only | `/health` `/healthz` `/readyz` `/v1/health` → 200 in 0.24–0.77 s; `status: healthy`, app 8.14.5 / entry 8.14.8, 101 live routes, 0 failed modules |
| Google Sheet `1QaOSNHq…` | ⚠️ **BLOCKED** for content | Metadata only (22.3 MB, modified 21:25 UTC = 00:25 Riyadh). The Drive connector returned only `_Diag_Response_Shape`; the xlsx export refused with "File too large". No access to Portfolio_Decision(_v2), Top_10(_v2), My_Portfolio, _Status or _Run_Log. |
| Apps Script project | ⚠️ **BLOCKED** (no Apps Script API access) | Only the copies in the repo (`apps_script/*.gs`) were read. Whether the deployed script matches them is **not verified**. |
| Dashboard `tfb-investment-platform…chatgpt.site` | ⚠️ **BLOCKED** | HTTP 401 (login required) |
| IBKR | Not used (per instruction) | Broker reconciliation **cannot be verified** in this run. |

**Workflow health baseline:** the last 300 runs (27 Nov 13:42–20:51 UTC) were 208 success, 83 skipped (chained jobs with nothing to do), 8 cancelled (concurrency groups) and **1 failure** (Verified Name Label Renewal, run 46011221552).

**Render baseline (24 h to 21:00 UTC):**

- Requests: 2,043 × HTTP 200, 1 × 401, 8 × 499 (client gave up).
- Memory: 3.60–3.69 GB of the 4.29 GB limit (84–86 %) for hours after warm-up.
- CPU: below 0.51 of a core.
- No `server_failed` events in 7 days; 18 successful deploys in 7 days.
- Render `healthCheckPath` is **unset**.

**Log signals (last 12 h, warning and above):**

- Yahoo throttles us (HTTP 429). The circuit breaker `[YF-CB v6.9.1]` opens and EODHD supplies fundamentals instead, so this is handled.
- There is "possibly delisted" noise for share-class tickers (BRK.A, BIO.B, TAP.A).
- Freshness reports `venue calendar unavailable for .VN .NS .QA .AE .BH -> old quote cannot be certified`, so old quotes from those venues are correctly refused certification.
- Two worker SIGTERMs at 20:30 UTC were the deploy rollover.

### 2.2 Findings

| # | Priority | Finding | Status |
|---|---|---|---|
| F1 | **High** | 122 of 128 verified name captions expire 29 Nov 10:34 UTC. The scheduled renewal of 26 Nov succeeded and pushed `name-label-renewal/45935990975`, but `gh pr create` was refused ("GitHub Actions is not permitted to create or approve pull requests") and `TFB_RENEWAL_TOKEN` is not set, so the renewal never reached main. The same failure mode has happened before (19 Oct, 20 Nov). | APPLIED + TESTED in PR #1110; merge pending (owner) |
| F2 | **High (process)** | The root cause of F1 is outside the code: a repository setting or secret. Every scheduled renewal stops at a pushed branch until the owner sets `TFB_RENEWAL_TOKEN`, or allows Actions to create PRs. | PREPARED (owner action, §4) |
| F3 | Medium | `portfolio_actions` meta `fx.static_fallback_used` missed GBp/GBX/ZAc/ILA holdings priced from the built-in FX table (`fx_source == "static/100"`). Reproduced: `[]` before, `['GBp']` after. | APPLIED + TESTED (1.20.1); needs merge + Render deploy |
| F4 | High (design, owner decision) | When no rate is supplied, the backend still values non-SAR holdings at **hard-coded rates** (about 40 currencies, `opportunity_builder.FX_STATIC_TO_SAR`). The numbers come out normal and are not flagged per row. This is documented as deliberate (it keeps the FX MAJOR gate from failing), but it means SAR totals can rest on an assumed rate. The rate is disclosed only in meta (and, after F3, for minor units too). | FOUND; backlog B3 |
| F5 | Medium | `_research_fx_rejected` and the funding check look up `fx_rates[ccy]` with the raw minor-unit code (`GBp`). A caller that supplies `GBP` therefore makes any LSE holding a `holding_fx_mismatch` and withholds funding for the **whole** portfolio. This fails closed (it withholds; it never mis-funds). | FOUND; backlog B4 |
| F6 | Medium | Apps Script `27_Decision_Pages_v2.gs:4240` uses `lfx = l.fx \|\| fx`, where `fx` is the USD→SAR rate (default 3.75). A non-USD lot with a blank FX→SAR cell is valued at the USD rate. Cash also falls back silently to 3.75 (`:1763`). | FOUND (repo copy only; deployed script BLOCKED); backlog B5 |
| F7 | Low | P&L in SAR converts cost at the current FX rate, not the trade-date rate (documented). So no separate currency effect is reported. No realized-P&L calculation exists in the backend (stated as deliberate in `core/portfolio_reconciliation.py:150`). Fees are a planning constant (`TFB_PF_FEE_SAR`, 9 SAR). | FOUND; backlog B6 |
| F8 | Medium (reliability) | Memory sits at 84–86 % of the limit. Likely holders: the engine page and symbol snapshots (`core/data_engine_v2.py:19510`) have no size cap or TTL and store each row twice; the advisor snapshot store (`core/investment_advisor_engine.py:1619`) expires a key only when that key is read again; per-symbol Prometheus labels are never pruned (`yahoo_chart_provider.py:1529`). Gunicorn `max_requests` is 0 (off). Not measured on the instance. | FOUND; backlog B2 |
| F9 | Low (reliability) | Render `healthCheckPath` is empty, so Render cannot replace a hung instance. `/readyz` answers in 0.43 s. | PREPARED (dashboard setting, §4) |
| F10 | Low (data) | `_Diag_Response_Shape` (AAPL): `forecast_source = provider_target` and `upside_pct` filled while Target Price is blank. The API and sheet values differ slightly (upside −0.130 vs −0.124), which is consistent with two different refreshes. No timestamp is shown on that tab. | FOUND; backlog B7 |
| F11 | Info | 25 open PRs, most of them stale daily-flag PRs from October–November. The owner-run `close_stale_prs.yml` exists. | FOUND |

### 2.3 Page-by-page assessment

Portfolio_Decision(_v2), Top_10_Investments(_v2), My_Portfolio, cash and cost basis, data audit and signal histories, and every dashboard page: **BLOCKED**. Their content could not be read in this run (see §2.1). The indirect evidence is all green today:

- Decision Surface Freshness, Release After Sync, Sync Outcome Audit, Full Refresh Coverage Audit, Daily Plan and Today Action Plan.
- Release Decision Feed (20:33 UTC) and Exit Plan (20:41 UTC).

That shows the pipelines ran. It does not show that the values on the pages are correct.

### 2.4 Forecasting and learning (from the project's own records, not re-computed)

- The shadow challenger S-1 has t = +0.47 on 37 paired days to 26 Nov, against a significance bar of t ≥ 2 (commit #1106). **No statistically supported edge.**
- `v_outcome_sector`: 2 of 196 cells show "beats its market", 30 "no clear edge" and 164 "too few calls" (#1106).
- The forward books of the challenger start being frozen on **Mon 30 Nov 2026** (#1109), so no forward-test result exists yet.
- The pipeline does record predictions before outcomes, evaluates only matured horizons, freezes books and computes after-cost alpha. The controls asked for are in place. The evidence base is still too small.

### 2.5 Before / after (verified)

| Measure | Before | After | Source |
|---|---|---|---|
| Name-label check | `DUE_SOON`, exit 1, 122 due < 48 h | `HELD_ACKNOWLEDGED`, exit 0, 0 due (on the PR branch) | `renew_verified_name_labels.py --check` |
| Bindings valid at 29 Nov 10:34 UTC | 0 of 122 renewable | 122 of 122 (to 4 Dec 03:45 UTC), **once merged** | registry |
| GBp static-FX disclosure (verified path) | not disclosed | disclosed | `tests/test_pf_fx_static_minor.py` |
| Workflow failures, last 300 runs | 1 | — (re-measure after the merge) | Actions |

None of these changes are in production yet. They become VERIFIED IN PRODUCTION only after the merge (and a Render deploy for F3).

---

## 3. Implementation register

| Change | Root cause | Files | Tests | Production | Rollback |
|---|---|---|---|---|---|
| R1: renew 122 name captions | Actions cannot open the renewal PR; no `TFB_RENEWAL_TOKEN` | `data/verified_name_labels.json` (only review/expiry times and source receipts) | renewal, registry, lifecycle and name-identity suites: 217 passed; `--check` exit 0 | Pending merge of #1110. No Render deploy needed: the registry is read by the Actions sync, not by the web service. | Revert the commit |
| R2: `portfolio_actions` 1.20.1 [FX-STATIC-MINOR] | Meta filter matched only `"static"`, not `"static/100"` | `core/analysis/portfolio_actions.py`, `scripts/verify_deployment.py` (pin), `tests/test_pf_custody_tier.py`, `tests/test_pf_total_return.py` (pins), `tests/test_pf_fx_static_minor.py` (new), `.github/workflows/ci.yml` 1.1.107 | 4 new; tests/test_pf_*, test_portfolio_*, board funding and workflow audits: 876 passed; the 55 files that read the manifest: 1,544 passed, 31 skipped | Pending merge, then a **manual Render deploy** (autoDeploy is off). After the deploy, the health manifest must show portfolio_actions 1.20.1. | Revert, then redeploy the previous commit `7f3181d` |

PR: https://github.com/emadsaberbahbah-coder/tadawul-fast/pull/1110 (draft; CI runs on every push).

---

## 4. Prioritized backlog

| ID | Item | Impact | Dependency | Acceptance | Effort |
|---|---|---|---|---|---|
| B1 | **Merge #1110 before 29 Nov 10:34 UTC**, then set the secret `TFB_RENEWAL_TOKEN` (fine-grained, this repo only, Contents + Pull requests read/write) | Names stay filled; the decision feed is not blocked | Owner | Next scheduled renewal opens its own PR; daily check green | 10 min |
| B2 | Bound the engine snapshots and advisor snapshot store (max entries + TTL sweep); consider `TFB_MAX_REQUESTS=2000`, `TFB_MAX_REQUESTS_JITTER=200` | Removes the memory creep toward the 4 GB limit | Code + Render env | Memory plateau < 70 % over 24 h; no latency regression | 0.5–1 day |
| B3 | Owner decision on the static-FX fallback: keep it, but flag each row as `fx_assumed` with the rate and source in the page; or blank the SAR value | Prevents SAR totals from resting silently on assumed rates | Owner decision | Each SAR figure names its FX source and time | 0.5 day |
| B4 | Normalise minor-unit codes (GBp→GBP ÷ 100) in the funding FX check | Stops one LSE holding from withholding all funding | B3 | GBp holding with GBP rate → funding eligible | 2 h |
| B5 | Apps Script DP2: a non-USD lot with a blank FX is shown as "FX missing", not valued at the USD rate; flag the cash 3.75 fallback | Correct SAR return for non-USD lots | Re-paste of the Apps Script | Harness case: an EUR lot with no FX → blank + flag | 3 h |
| B6 | Separate the currency effect (trade-date FX on cost) and realized P&L once the ledger carries the trade-date rate | Full P&L attribution | Ledger column | Price, FX and income components sum to the total | 1–2 days |
| B7 | `_Diag_Response_Shape`: add the response and sheet timestamps; explain upside when the target is blank | Traceability | — | Each value carries an as-of | 2 h |
| B8 | Set Render `healthCheckPath=/readyz` | Render replaces a hung instance automatically | Dashboard | Setting visible; deploy stays green | 5 min |
| B9 | Give the audit a readable copy of the key tabs (for example, a nightly CSV export of Portfolio_Decision_v2, Top_10_v2 and My_Portfolio to a small sheet) | Enables an independent page audit and SAR reconciliation | Owner | Next audit reads every focus tab | 1 h |
| B10 | Owner runs `close_stale_prs.yml` (preview first) | Less noise | Owner | < 10 open PRs | 10 min |

---

## 5. Final verdict

- **Operationally healthy:** **Yes, with one deadline.** The API is up, the deploy matches main, and 299 of 300 recent runs are green. The one red check (name labels) needs #1110 merged before 29 Nov 13:34 Riyadh, or Names go blank and the coverage floor can block the feed.
- **Financially consistent:** **Not verified in this run.** The FX direction and minor-unit handling in the backend are correct. Defects F4–F7 are known and listed. The live SAR totals and the Top-10 pages could not be read (BLOCKED), and broker reconciliation was not possible without IBKR or a statement.
- **Sufficiently validated for recommendation use:** **No.** The controls (pre-registration, matured-only evaluation, after-cost scoring, frozen forward books) are in place. The evidence does not show an edge: S-1 t = +0.47 on 37 days, and the first forward book forms 30 Nov. Treat the output as screened research, not calibrated buy signals.
