# TFB comprehensive audit — 27 October 2026 (Riyadh)

This was a one-time run. It made no schedule, trigger or background-monitor change.
The work ran from 26 Oct 21:05 to 22:40 UTC (27 Oct 00:05–01:40 Riyadh).
Baseline: `main` at `d25468a` (#844). Render live revision: `08a15bc` (#839).

Status labels used below: **FOUND · PREPARED · APPLIED · TESTED · VERIFIED IN PRODUCTION · BLOCKED · ROLLED BACK**.

---

## 1. الملخص التنفيذي (Arabic executive summary)

**الحالة الحالية:**
- خدمة Render تعمل بشكل سليم:
  - `/health` و`/readyz` يعيدان 200 خلال أقل من ثانية.
  - نحو 2,043 طلباً ناجحاً يومياً، مقابل 3 طلبات 401 و7 طلبات 499.
  - لم تقع أي أعطال أو عمليات قتل بسبب نفاد الذاكرة خلال 7 أيام.
- جميع مراحل جلب البيانات في المزامنة اليومية نجحت.
- ومع ذلك تنتهي المزامنة المجدولة باللون الأحمر في 5 من آخر 6 تشغيلات، وفحص Decision Surface Freshness أحمر في 5 من 5.

**السبب الجذري:**
- خطوة التدقيق النهائي تفحص Top_10 وPortfolio_Decision قبل أن تتمكّن سلسلة Apps Script (`26_Decision_Chain`) من تحديثهما.
- والسلسلة لا تبدأ بالتحديث إلا بعد أن يكتب هذا التدقيق نفسه `NOT_ACTIONABLE run=N`.
- النتيجة: كل مزامنة مجدولة تفشل بحكم التصميم، فيضيع الفشل الحقيقي وسط إنذارات متكررة.

**ما تم إنجازه:**
- `finalize_decision_publication` الإصدار 1.2.0 يصنّف حالة "المستهلك لم يُحدَّث بعد لنفس الدفعة" تصنيفاً ضيقاً ودقيقاً.
- في هذه الحالة تنتهي المهمة بتحذير بدل الفشل.
- يبقى قفل القرار كما هو دون تغيير:
  - تُكتب `NOT_ACTIONABLE`.
  - يبقى `exit_code` في الأدلة مساوياً 2.
  - لا يحدث أي ترقية للتغذية.
- أي حالة أخرى تبقى حمراء، مثل:
  - بيانات قديمة لأكثر من 8 ساعات.
  - خلل في المصادر.
  - عدم تطابق المجمّع.
  - غياب شاهد البنية.
- الحالة: مُطبَّق ومُختبَر (APPLIED · TESTED) في طلب دمج مسودة. لم يُدمج ولم يُتحقق منه في الإنتاج بعد.

**أهم مسألة غير محلولة:**
- تعذّر قراءة علامات تبويب Google Sheets الحية (`_Status` و`My_Portfolio` و`Portfolio_Decision` و`Top_10`) من هذه البيئة.
- لذلك لم يتم التحقق من:
  - مطابقة المحفظة بالريال.
  - أسعار الصرف.
  - التوصيات الحالية.
- لوحة المعلومات الخارجية تتطلب تسجيل دخول (401).

**جاهزية التوصيات:**
- **غير مُتحقَّق منها في هذا التشغيل.**
- آخر تقييم متاح من جلسة سابقة (25 أكتوبر، غير مُعاد التحقق منه) أظهر أن فئة INVESTABLE لم تتفوق على WATCHLIST: نسبة فوز 47.2% مقابل 56.1%، وBrier = 0.287.
- لا ينبغي الاعتماد على التوصيات للتنفيذ قبل:
  1. إعادة التحقق من هذه المعايرة.
  2. التحقق من مطابقة المحفظة.

---

## 2. Access and baseline

| Resource | Access | What was verified |
|---|---|---|
| GitHub repository | Full (read/write via branch + PR) | `main` = `d25468a`. There are 30 open PRs, including three earlier unmerged audit drafts (#809, #835, #842). |
| GitHub Actions | Read | 100 completed `main` runs (25 Oct 12:39Z → 26 Oct 20:45Z), plus the logs of every failure type. |
| Render `srv-d4hnir15pdvs739bqe1g` | Read (logs, metrics, events, deploys) | Live deploy `dep-dbf7glt9fdbs7393qrog` = `08a15bc`, live since 25 Oct 21:46:43Z. `autoDeploy: no`. |
| Render health endpoints | Public HTTP | `/health`, `/healthz` and `/readyz` all 200. Measured latency 0.54–0.72 s. Version 8.14.7. |
| Google Sheets (live tabs) | **BLOCKED** | The Drive connector returns only six diagnostic tabs, frozen at 8 Aug. The `.xlsx` export is "not implemented". Shell credential use was refused by policy. |
| Apps Script (deployed project) | **BLOCKED** | No tool access. Only the repository copies in `apps_script/` were read. Deployed-versus-repo consistency is unverified. |
| Dashboard (`*.chatgpt.site`) | **BLOCKED** | HTTP 401 "Sign in required". |
| IBKR | Not used (by instruction) | No broker reconciliation was attempted. **Current broker reconciliation cannot be verified.** |

## 3. Findings

### F-1 (High) — every scheduled sync ends red by construction. **FOUND → APPLIED · TESTED**

**Evidence**

`daily_sync.yml` scheduled runs and their final verdicts:

| Run | Time (UTC) | Verdict |
|---|---|---|
| 40212154238 | 25 Oct 16:32 | fail |
| 40241114682 | 25 Oct 22:38 | fail |
| 40247921723 (dispatch) | 26 Oct 00:08 | fail |
| 40305932747 | 26 Oct 10:15 | fail |
| 40380186439 | 26 Oct 18:42 | fail |

Across these runs:
- Every data leg succeeded: core-pages, mutual-funds, global-markets and recovery.
- Only the "Verify final decision publication" job failed. Run 40380186439 reported `audit_exit_codes` = `[0, 2]`: coverage passed, decision failed.
- The blocker was published every time.

The 24 Oct census (`docs/reviews/github-run-census-20261024.md`) shows the same pattern on 20 of 21 scheduled syncs.

Decision Surface Freshness failed 5 of 5 on the same codes:
- `T10_OLDER_THAN_SOURCE` ×4 (one per source page)
- `T10_POOL_RECONCILIATION_UNPROVEN` with the message "does not match one consumed/current source cohort"

**Root cause**

`apps_script/26_Decision_Chain.gs` v1.0.2 renders Top_10 for run N only after `'TFB Decision Feed'` carries `run=N`.

That value is written by this same final job, as `NOT_ACTIONABLE(final_publication) | run=N`. So the audit always meets decision pages that cannot yet have consumed run N.

The red status is therefore a designed intermediate state, not a defect in the data. But it:
- trains operators to ignore red,
- fires the `notify-run-verdict` `::error::` and the Slack alert on every slot, and
- would mask a genuine failure of the source.

**Change**

`scripts/finalize_decision_publication.py` 1.1.2 → **1.2.0**.

- New pure function `classify_consumer_pending()`. It returns true only when **all** of these hold:
  - coverage exit ≤ 1 and decision exit = 2;
  - blockers = `["decision_failed"]` exactly;
  - the blocker write was confirmed;
  - nothing was promoted, and there is no fatal or publication error;
  - the same-job structure witness is present, with exit ≤ 1;
  - every decision finding is a `FAIL` drawn from: `PF_OLDER_THAN_SOURCE`, `PF_SOURCE_CONSUMPTION_UNPROVEN`, `T10_OLDER_THAN_SOURCE`, `T10_SOURCE_CONSUMPTION_UNPROVEN`, `FALSE_FULL_UNIVERSE_CLAIM`, or `T10_POOL_RECONCILIATION_UNPROVEN` (pool only when its message names another run), and at least one finding is not the full-universe claim.
- Under `TFB_FINAL_CONSUMER_PENDING=warn`, only the **process** exit becomes the coverage code. This is 0, or 1 if coverage had warnings.
- `run()` is untouched, and `release_decision_feed.py` still imports it and re-runs the real audits. The evidence `exit_code` stays 2, and the blockers and the `NOT_ACTIONABLE` write are unchanged.
- The JSON evidence gains `consumer_pending` and `process_exit_code`.
- With the variable unset, behaviour is unchanged from 1.1.2.

`.github/workflows/daily_sync.yml` (final job only):
- The step-scoped env `TFB_FINAL_CONSUMER_PENDING: ${{ vars.TFB_FINAL_CONSUMER_PENDING || 'warn' }}`. The job env is left untouched, because `release_decision_feed.yml` must mirror it byte for byte.
- A `::warning::` is emitted when a run is classified as pending.
- The summary table shows the two new keys.

**What stays red**
- `PF_RUN_STALE` and `T10_RUN_STALE` (the chain has not rendered for more than 8 h).
- Missing or NOT_OK surfaces.
- Pool-count and approved-membership faults.
- Any source or coverage failure.
- A missing or failed structure witness.
- Any WARN-severity decision finding.
- A failed or unconfirmed blocker write.

**What still catches a broken chain**
- `release_decision_feed.yml` re-audits the cohort on its own slots, and fails or defers if the board never consumes it.
- Decision Surface Freshness is unchanged.

**Downstream consumers checked**
- `release_decision_feed.check_provenance` accepts a final job concluding `success` or `failure` alike.
- `sync_outcome_audit.yml` audits both conclusions.
- Nothing reads `steps.final_audit.outputs.audit_exit`.

**Rollback:** set the repository variable `TFB_FINAL_CONSUMER_PENDING=off`; no code change is needed. Alternatively, revert the PR.

### F-2 (Medium) — Render memory runs close to its limit between worker recycles. **FOUND**

- Memory reached 3.66 GB at 07–09 UTC and 4.00 GB at 19 UTC on 26 Oct, against a 4.29 GB limit (93%).
- Gunicorn recycling, armed by the 25 Oct env change (`TFB_MAX_REQUESTS=200`), works:
  - 10 clean `Worker exiting` / `Booting worker` pairs in 24 h;
  - memory dropped to 0.64 GB after the 10:24–10:36Z recycles;
  - no `server_failed` events in 7 days.
- The headroom is still thin during the global-markets sync window.
- **Proposal:** lower `TFB_MAX_REQUESTS` to 100, or profile the Global_Markets bulk path. Either needs owner approval; there is no cost change.

### F-3 (Medium) — Render has no health-check path. **FOUND · PREPARED (owner action)**

- `healthCheckPath` is empty on the service, so Render cannot detect a hung instance or gate a zero-downtime deploy.
- `/readyz` exists and answers in 0.5 s.
- **Action:** Render dashboard → Settings → Health Check Path = `/readyz`. This is free and reversible.
- The connected Render tool can only change env vars, not this setting.

### F-4 (Medium) — verified name-label bindings expired. **FOUND (owner action)**

- `name_label_renewal.yml` check at 26 Oct 10:11Z reported 128 bindings: **3 expired** and 3 expiring within 48 h, with status `EXPIRED`.
- The workflow's own next action is to dispatch `mode=renew, publish_branch=true` and review the branch. Expiry must never be extended by hand.

### F-5 (Low) — known stale provider identifiers keep producing error logs. **FOUND**

- Render error logs show repeated yfinance 404 / "possibly delisted" errors for several symbols:
  - `SPLG`: renamed to `SPYM` on 31 Oct 2025. This is already recorded as a verified legacy identity in `data/verified_name_labels.json`.
  - `BK`, `HEWW`, `IECS`, `IBML`, `IBDO`, `DVRE`, `VLLU`.
- These are expected misses for legacy or ETF identities, but they make up most of the error-level logs. That hides real errors.
- **Backlog:** demote known-legacy misses to INFO.

### F-6 (Process) — a large backlog of unmerged work. **FOUND**

- There are 30 open PRs. Many are "default off" features that never ship.
- The three earlier comprehensive-audit drafts (#809, #835, #842) are unmerged. #842 is `behind` `main`.
- Its Render env change is live, but its `start_web.sh` 2.7.0 default and `run_daily_brief` 1.19.0 fix are not.
- Fixes that are never merged cannot improve production. Triage is the owner's decision.

### Carried forward, not re-verified (Sheets blocked)

These come from the 25 Oct audit (#842):
- **Calibration:** INVESTABLE won 47.2% (n = 974, CI 44–50), against WATCHLIST's 56.1%. The 85–100 confidence band won 42%. Brier = 0.287, against 0.25 for a constant forecast.
- **Data:** zero `Open` prices were published as data.

They are listed here for continuity only. **This run did not re-measure them.**

## 4. Page-by-page assessment

| Surface | Assessment this run |
|---|---|
| `Portfolio_Decision` / `_v2` | **BLOCKED.** Not readable. The freshness audit text shows `PF_*` lag codes only on sync slots. |
| `Top_10_Investments` / `_v2` | **BLOCKED.** Not readable. The audit shows the pool is internally consistent (Mutual_Funds 2469/2474 after 5 cross-page duplicates), but lags each new cohort until the chain renders. |
| `My_Portfolio`, cash and cost basis | **BLOCKED.** There is no SAR reconciliation. Quantities, FX rate, FX direction and valuation time could not be verified. |
| `_Status`, `_Run_Log` | **BLOCKED** directly. Indirectly, the audit evidence shows source pages complete: Market_Leaders 255/255, Global_Markets 6611/6611, Commodities_FX 453/453, Mutual_Funds 2474/2474. |
| External dashboard | **BLOCKED** (401). |

## 5. Before and after metrics

| Metric | Baseline (25 Oct 12:39Z – 26 Oct 20:45Z) | After |
|---|---|---|
| Scheduled/dispatch sync runs ending red | 5 of 6 | Not yet measured; the change is unmerged. Expected: red only on non-lag findings. |
| Of those, data legs that failed | 0 of 15 leg executions | — |
| Decision Surface Freshness red | 5 of 5 executed (10 skipped) | Unchanged by design (backlog B-2). |
| Release Decision Feed red | 3 of 9 | Unchanged (#831 addresses in-flight slots). |
| Render HTTP 2xx / 4xx per day | 2,043 / 10 (3 × 401, 7 × 499) | — |
| Render OOM or `server_failed` (7 d) | 0 | — |
| Render peak memory | 4.00 GB of 4.29 GB | — |
| Verified name bindings expired | 3 of 128 | Owner action. |

No accuracy or improvement percentages are claimed.

## 6. Implementation register

| ID | Status | Root cause | Files | Tests | Production | Rollback |
|---|---|---|---|---|---|---|
| I-1 | APPLIED · TESTED (draft PR) | F-1 | `scripts/finalize_decision_publication.py` (1.2.0), `.github/workflows/daily_sync.yml`, `.github/workflows/ci.yml` (1.1.39), `scripts/verify_deployment.py` (pin), `tests/test_final_consumer_pending.py` (new) | See below | **Not verified:** awaits merge and the first scheduled sync | `vars.TFB_FINAL_CONSUMER_PENDING=off`, or revert |

**Tests for I-1**
- `tests/test_final_consumer_pending.py`: 47 tests.
  - A pure classifier battery with 30 negative cases.
  - The real finalizer and the real strict freshness auditor on a real signed pool receipt for the previous run.
  - `main()` exit and evidence.
  - The real workflow shell block run with a stub audit.
  - The step-scope guard.
- Golden negative: the same tests on the 1.1.2 script fail, 43 of 43 (written before the two workflow tests were added).
- Neighbouring suites (final publication, workflow, release feed and auto source, matrix scope, rollout, approved universe, name identity, deployment pins): 865 passed, 21 skipped.
- `ruff` (the E9/F63/F7/F82 CI gate) is clean. YAML parses.
- `audit_repository_workflows.py`: errors 0, warnings 0.

**Acceptance in production (after merge)**

On the next scheduled sync, the "Verify final decision publication" job should:
- end `success` with the `::warning::Sources accepted; ...` annotation when only lag codes are present;
- keep `final_decision_publication.json` at `exit_code: 2`, `blocker_published: true`, `consumer_pending: true`;
- show `_Status` `TFB Decision Feed` as `NOT_ACTIONABLE(final_publication) | run=N`;
- show a later `release_decision_feed` run re-auditing the cohort.

## 7. Prioritised remaining backlog

| # | Item | Impact | Depends on | Acceptance | Effort |
|---|---|---|---|---|---|
| B-1 | Give this audit environment a read path to the live Sheets tabs (for example the Sheets API with a read-only service account, or a scheduled CSV export) | Unblocks SAR reconciliation, FX, price-freshness and recommendation review | Owner credentials decision | `_Status`, `My_Portfolio` and both cockpits are read with timestamps | S |
| B-2 | Apply the same lag classification in `decision_surface_freshness.yml`, or trigger it after the chain renders | Removes the second always-red signal | I-1 merged and observed | Red only on non-lag codes | S |
| B-3 | Renew the 3 expired and 3 expiring name labels through `name_label_renewal.yml` with `mode=renew` | Identity integrity | Owner review of the renewal branch | Check status OK | S |
| B-4 | Set the Render health-check path to `/readyz` | Deploy safety | Owner (dashboard) | `healthCheckPath` = `/readyz` | XS |
| B-5 | Re-measure calibration (INVESTABLE versus WATCHLIST, Brier) on completed horizons, after fees, chronologically held out | Decides whether recommendations are fit for use | B-1 | A report with n, period, CI, drawdown and turnover | M |
| B-6 | Triage the 30 open PRs; land or close the earlier audit drafts (#842 brief fix, #835 SAR-weight fixes, #809) | Turns prepared fixes into production fixes | Owner | Each PR merged or closed with a reason | M |
| B-7 | Memory headroom: `TFB_MAX_REQUESTS=100`, or profile the Global_Markets bulk path | Avoids OOM during the sync window | Owner approval | Peak below 80% for 7 days | S–M |
| B-8 | Demote known-legacy yfinance misses (SPLG and similar) to INFO | Makes real errors visible | — | Error-level logs contain no verified-legacy symbols | S |

## 8. Final verdict

- **Operationally healthy:** *mostly.*
  - The API is up and fast, with no OOM or crashes.
  - Data acquisition legs complete with full row counts.
  - The always-red final job is a signalling defect; I-1 addresses it once merged.
- **Financially consistent:** *not verified.* SAR consolidation, FX and cost basis could not be read in this run (Sheets blocked), and broker reconciliation was out of scope.
- **Sufficiently validated for recommendation use:** **No.**
  - The last available calibration evidence (25 Oct, not re-verified) shows the top tier under-performing the watchlist.
  - The current recommendation pages could not be inspected.
  - Treat Top-10 output as research input only until B-1 and B-5 are complete.
