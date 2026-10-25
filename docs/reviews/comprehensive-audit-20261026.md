# TFB comprehensive audit — 26 Oct 2026 (one-time)

Audit window: 25 Oct 2026 19:40 – 22:10 UTC (25 Oct 22:40 – 26 Oct 01:10 Asia/Riyadh).
Base commit: `08a15bc` (main, #839). Live Render deploy at audit start: `08a15bc`,
`dep-dbeuvc29lscs7387q21g`, live since 25 Oct 12:03 UTC. Live code matched main.

This audit follows the 25 Oct audit (draft PR #835, `comprehensive-audit-20261025.md`,
still unmerged) and does not repeat its findings. It records the current state,
adds four new evidence-backed findings, and ships two code fixes plus one
production configuration change.

Status labels: FOUND · PREPARED · APPLIED · TESTED · VERIFIED IN PRODUCTION · BLOCKED · ROLLED BACK

---

## 1. الملخص التنفيذي (عربي)

**الوضع الحالي:** واجهة Render تعمل. نقاط الفحص `/health` و`/readyz` و`/meta` ردّت بـ 200 خلال 0.3–0.6 ثانية. لا توجد أخطاء 5xx خلال آخر 48 ساعة، ولا أي حدث تعطّل خلال 7 أيام. لكن الذاكرة كانت على وشك النفاد: النسخة العاملة وصلت إلى 4.06 من 4.29 غيغابايت (94.5%) بعد 9.5 ساعات فقط من النشر. السبب أن عمّال Gunicorn لا يُعاد تشغيلهم أبداً. نشرة القرار صدرت مساء 25 أكتوبر بنجاح (`accepted=true`، الساعة 23:00 بتوقيت الرياض، وتنتهي صلاحيتها 03:33).

**ما أُنجز:**

1. **الذاكرة:** فعّلتُ إعادة تدوير العمّال في الإنتاج عبر متغيرات البيئة: `TFB_MAX_REQUESTS=200` و`TFB_MAX_REQUESTS_JITTER=50` و`MALLOC_ARENA_MAX=2`. أطلق هذا التغيير نشراً للإصدار نفسه `08a15bc`. هذا تغيير في إعدادات الإنتاج، وتراجعه سهل (انظر القسم 4). في الكود جعلتُ هذه القيم افتراضية في `start_web.sh` v2.7.0.
2. **النشرة اليومية:** نشرة 21:15 كتبت "0 holdings" بينما المحفظة فيها مركز فعلي (صكوك <bdi>5023.SR</bdi>). الآن تذكر النشرة أي مركز في My_Portfolio لا تعرضه صفحة Portfolio_Decision، وتحجب إجمالي المحفظة إذا كان جزئياً بدل عرضه كأنه كامل. كذلك كانت كل نسخ النشرة تقول "قبل افتتاح السوق" حتى نسخة العاشرة مساءً، وأصبحت تذكر حالة جلسة تداول الفعلية وقت الإرسال.

**أهم ما وُجد ولم يُحلّ:**

- **جودة التوصيات:** على 974 نتيجة محسومة، كانت نسبة نجاح إشارات INVESTABLE ‏47.2% (فترة الثقة 44–50%). هذا أقل من WATCHLIST ‏(56.1%)، وفئة الموثوقية 85–100 نجحت بنسبة 42% فقط. مؤشر Brier بلغ 0.287، وهو أسوأ من مستوى رمي العملة (0.25). المعنى: تصنيف "قابل للاستثمار" ودرجة الموثوقية لا يميّزان الفرص الرابحة حالياً.
- **تأخر الجدولة في GitHub:** كل مزامنة مجدولة هذا الأسبوع بدأت متأخرة. الوسيط 4 ساعات و39 دقيقة، و23 من 23 تشغيلاً كانت متأخرة. ولم يعمل 12 من 47 موعداً لنشر القرار أصلاً. لذلك تصل البيانات بعد افتتاح تداول، لا قبله كما صُمّم.
- **صفحة Top_10 تتأخر عن المزامنة** نحو 30 دقيقة. لهذا يُرفض النشر في كل دورة مزامنة، ثم يُقبل لاحقاً في موعد الإصدار المجدول. الحل يحتاج Apps Script (غير متاح لي).

**جاهزية التوصيات:** **غير كافية للاعتماد عليها كإشارة تداول.** التحقق التاريخي لا يُظهر أن فئة INVESTABLE تتفوق على البدائل. لا تتصرف إلا بنشرة `accepted=true` غير منتهية الصلاحية، وعامل التوصيات كفرز أولي يحتاج تحققاً مستقلاً. مطابقة حساب الوسيط لم تُتحقق، فـ IBKR لم يُستخدم حسب التعليمات.

---

## 2. Access and baseline

| Resource | Access | Notes |
|---|---|---|
| GitHub repo + Actions | Full (branch, PR, run logs, artifacts via `gh run download`) | Nothing re-run or dispatched |
| Render `srv-d4hnir15pdvs739bqe1g` | Read (service, deploys, events, logs, metrics) + one env update | Workspace `tea-d3m0uuumcj7s73aatrpg` (the only one on the account) |
| Google Sheet `1QaOSNH…` | **BLOCKED** for direct cell reads (Drive text export ends in the early legacy tabs). Sheet state was read indirectly from today's Actions artifacts (validator, coverage, decision-surface and release JSON) | — |
| Apps Script `1IvEj8…` | **BLOCKED** (container-bound, not reachable) | Repo copy only |
| Dashboard site | **Not inspected** (the 25 Oct audit got HTTP 401) | — |
| Authenticated API routes (`/v1/enriched/quote`) | **BLOCKED** (401, no token held) | — |
| IBKR | Not used (per instructions) | Broker reconciliation **not verified** |

### Baseline metrics

| Metric | Value | Window / source |
|---|---|---|
| Live probes `/health /readyz /meta` | 3/3 HTTP 200, 0.30–0.57 s | 25 Oct ~19:45 UTC |
| Running revision | `render_git_commit 08a15bc` = main | `/meta` |
| Version reporting | `app_version 8.14.5` vs `entry_version 8.14.7` | Stale `APP_VERSION` env (open since 22 Oct) |
| HTTP status | 24 Oct 20:00 → 25 Oct 20:00 UTC: 200 = 2,045 · 401 = 5 · 499 = 21 · 5xx = 0. 25 Oct 20:00 → 21:44 UTC: 200 = 2,095 · 401 = 3 · 499 = 5 · 5xx = 0 | Render metrics, daily buckets |
| Server failures / restarts | 0 events | Render events, 7 days |
| Memory, live instance `59hw2` | 1.92 GB (14:00) → 2.10 → 2.72 → 4.02 (20:00) → **4.06 GB at 21:30 UTC = 94.5% of 4.29 GB** | Render metrics |
| Memory, earlier instances | `dhs8l` 3.77 GB, `qxvn9` 3.68 GB, `jbqlz` 3.64 GB before their replacing deploys | Render metrics |
| Gunicorn recycling | `--max-requests` off (boot line shows workers=2, no max_requests) | Render log 12:03:32 UTC |
| Today's red main runs | Advanced Sync 40212154238 (final publication `decision_failed`), Decision Surface Freshness 40217476485 (`T10_OLDER_THAN_SOURCE` ×4, `T10_POOL_RECONCILIATION_UNPROVEN`), Release Decision Feed 40213415492 (`superseded_by_run`) | Actions artifacts |
| Decision feed | Released 25 Oct 23:00 Riyadh by run 40228587464, `accepted=true`, audits `[0,0]`, expires 26 Oct 03:33 Riyadh; Top-10 receipt 21:07, PF 19:37 Riyadh | Release artifact |
| Dashboard validator (final) | 0 FAIL · 13 WARN · 50 PASS · 1 SKIP; ML 255, GM 6,611, CFX 453, MF 2,474 rows (all FULL) | job 121241344083 |
| Active holdings | `["5023.SR"]` (sukuk), sourced from `_Portfolio_CostBasis` | Coverage audit / sync log |
| Scheduled-run start delay | daily_sync median 279 min (min 131, max 388), 23/23 runs late; release feed 35 of 47 slots fired; daily_brief median 141 min; track_performance median 175 min | GitHub REST, 18–25 Oct |
| Forecast evaluation | 24,481 records; 11,860 matured; 3,410 decided; directional win rate 54.8% | Performance Clock 40221764956 |
| Open PRs | ≥101 open (pagination stopped at 100+1), ≥65 opened before 1 Oct | GitHub REST |
| Local test suite at base + this change | 6,005 passed · 45 skipped · 2 failed. One is the known xdist-order flake (passes 102/102 in isolation). The other was a manifest pin, fixed here and re-tested | `pytest -n 4`, Python 3.11, requirements.txt |

---

## 3. Findings (architecture: Providers → Python/API → Apps Script → Sheets → brief/dashboard)

### N1 · High · Render memory at 94.5% — APPLIED (production env) · TESTED (code)

Gunicorn never recycled a worker. A code read (no profiling was done) finds per-worker
stores sized by the ~10k-symbol universe:

- engine `_symbol_snapshots`, unbounded, ~15 KB per row: `core/data_engine_v2.py:18463`, written at `:18644`
- EODHD quote/fund/history caches capped at 8,000/6,000/6,000, where expired entries are evicted only on read: `core/providers/eodhd_provider.py:2118`, `:2283`
- LKG stores (20k cap, 72 h TTL)
- Yahoo caches
- per-symbol Prometheus labels: `core/providers/yahoo_chart_provider.py:1529`
- `lru_cache` tables in `core/symbols/normalize.py`

On top of that, glibc keeps freed memory in per-thread arenas because `MALLOC_ARENA_MAX` was unset. Only the owner's frequent manual deploys had prevented an OOM kill.
Fix: recycle each worker after 200 ± 50 requests, roughly once per full sync per worker and
staggered, and cap arenas at 2.

Safety:
- The sync client times out at 30 s and retries 3× on reset or 5xx (`scripts/run_dashboard_sync.py:3266`), both inside the 45 s graceful timeout.
- `/meta` shows `engine_target_klg_redis: true` and `engine_fund_lkg_redis: true`, so LKG continuity survives a recycle.

### N2 · High · GitHub scheduled runs start 2–6 h late — FOUND

The delays in section 2 are GitHub's run-creation times, not job runtime. Effects:

- The "07:17 Riyadh before Tadawul open" sync lands ~12:00 Riyadh, after the open.
- The brief labelled "10:30 Tadawul just opened" arrives ~12:50.
- 12 of 47 release slots never fired.
- `intraday_refresh.yml` (`*/15 7-11 * * 0-4`, 100 slots/week) produced 12 scheduled runs in the week.

No cron edit fixes delays of this size. The free fixes are:
(a) chain dependent workflows with `workflow_run`, e.g. release → brief, which the repo already does for the post-sync audits;
(b) trigger the sync from an Apps Script time-driven trigger via `workflow_dispatch`. Apps Script triggers fire within ~15 min. This needs a fine-grained PAT stored in Script Properties, which is an owner action.

Both change email cadence or credentials, so this is left as a proposal (backlog B2).

### N3 · Critical (recommendation validity) · INVESTABLE does not out-perform — FOUND

Performance Clock 40221764956 (25 Oct 21:55 Riyadh, `calibration.json`):

| Bucket | Decided | Win rate | 95% CI | Mean claimed reliability |
|---|---|---|---|---|
| INVESTABLE | 974 | 47.2% | 44.1–50.4 | 72.9 |
| WATCHLIST | 2,235 | 56.1% | 54.0–58.1 | ~61 |
| BLOCKED | 187 | 79.7% | 73.3–84.8 | 48.6 |
| Reliability 85–100 | 162 | 42.0% | 35–50 | ~87 |
| Reliability 50–70 | 1,063 | 58.3% | 55–61 | ~58 |

Brier score 0.2873 against a 0.25 coin-flip baseline.

Limitations:
- "Win" is directional at horizon maturity, not net of fees or slippage.
- Horizons overlap and are correlated, so the effective sample is smaller than 3,410.
- 23,166 of 24,481 records lack canonical forecast provenance (`unproven_forecast_provenance`), so the canonical-unit evaluation has no by-horizon sample yet.

Even so, the pattern is clear. The gate and the stated reliability do not rank outcomes, and
the top band is the worst. No engine change was made here: changing scoring needs a shadow
evaluation first (backlog B1). Until then, treat INVESTABLE and Top-10 as a screen, not a forecast.

### N4 · Medium · Learning pipeline pricing — FOUND

Maturation price coverage dropped from ~1,010/1,375 (22–23 Oct) to ~560/1,390 (24–25 Oct), and
the pending backlog grew 3,692 → 4,364 with 90 records matured in three days. The Yahoo fallback
returns HTTP 429 on all 16 attempts every run and then skips the rest, so it contributes nothing.
The coverage drop coincides with the weekend: 24–25 Oct are Sat/Sun, so US/EU quotes are Friday
closes, which the tracker's session check (`_outcome_quote_session`) rejects. A defect is not
established. **Verify on Mon 26 Oct:** coverage should return to ~1,000. If it does not,
investigate `_outcome_price_from_row` rejections, whose per-symbol reasons are not logged today.

### N5 · Medium · Daily brief hid a held position and mislabelled its send time — APPLIED · TESTED

The 25 Oct 21:15 brief (run 40220333814) printed "Book snapshot. 0 holdings". The sync that day
wrote one active holding (5023.SR), and the coverage audit listed `active_holdings ["5023.SR"]`.

Root cause: the count is the sum of the Portfolio_Decision action groups, and the brief never
compared it with My_Portfolio. A holding absent from that page simply vanished. A partial
decision page would also have shown a partial MV/cost as the book total.

The header and footer printed the literal "before market open" on all four sends.

Fix (`scripts/run_daily_brief.py` 1.18.1 → 1.19.0):
- Ledger symbols missing from Portfolio_Decision are named in the input notice (HTML, PDF, text).
- The snapshot reads "N of M holdings".
- Book MV/cost/P&L become "unavailable" rather than a partial total.
- The send label shows the Tadawul session state at the Riyadh send time.

Why 5023.SR was absent from Portfolio_Decision at 21:15 is not established, because the sheet is
not readable here. The brief now says so instead of hiding it.

### N6 · Medium · Zero "Open" prices published as data — FOUND

The final validator lists `Open = 0.0` on 30 Mutual_Funds, 9 Global_Markets and 1 Commodities_FX
rows (e.g. `GJH.US`, `PERF.US`, `ACA.US`, `CMCI.US`). Yahoo's chart API independently shows
positive opens (GJH 9.57, PERF 1.99, ACA 148.90). So 0 is a missing value written as zero.
The engine's OHLC-coherence guard treats a non-positive open as "no judgement"
(`core/data_engine_v2.py:13414`), and its master switch defaults off. Which provider emits the 0
could not be confirmed without the authenticated quote API, so no patch was written (backlog B6).

### Carried from earlier audits (state re-checked)

- **Decision pages lag the sync (25 Oct B1).** Still open. Today Portfolio_Decision (19:37) beat its source (19:33), which is an improvement: `PF_STATUS_NOT_OK` is gone. Top-10 (17:55 → 21:07) still lagged the 19:36 sources, so the in-sync final check was red and the scheduled release accepted it later. Fixing it needs Apps Script (open PR #788).
- **Release slot inside a sync (#831).** The 16:48 UTC release refused with `superseded_by_run` while sync 40212154238 was in flight. That is exactly the case #831 (`TFB_RELEASE_INFLIGHT_DEFER`) addresses. #831 is unmerged.
- **Stale `APP_VERSION`** (8.14.5 vs 8.14.7) and the **operator FX age**: still open (owner env).
- **Draft PR #835** (engine 5.159.0 portfolio-weight fixes, normalize 5.6.1): unmerged, so its fixes are not live.

### Page-by-page (from today's artifacts)

| Page | State | Assessment |
|---|---|---|
| Market_Leaders | 255/255, status 19:36 Riyadh, fresh 252/255 | OK. Warns: 2360.SR blank Open; 4700.SR outside its 52W range |
| Global_Markets | 6,611 rows FULL | OK. 28 blank and 9 zero Opens (N6); 7 ROI-12M soft-cap warnings |
| Commodities_FX | 453 FULL; vintage 446 rows 25 Oct, 7 rows 23 Sep | 7 rows a month stale. 16 blank Opens |
| Mutual_Funds | 2,474 FULL; Top-10 pool used 2,469/2,474 | 64 blank and 30 zero Opens |
| My_Portfolio | 1 row (5023.SR), 122 cols, fresh | Brief omitted it (N5, fixed) |
| Portfolio_Decision | run 19:37 Riyadh | Fresh vs source today |
| Top_10_Investments | 17:55 → 21:07 Riyadh; banner "FEED ACTIONABLE — HELD … 0 EXECUTABLE TICKETS + 10 GRACE-HELD" | Lags each sync (B1). Selection quality per N3 |
| Daily brief | 21:15 send reported feed withheld (feed released later at 23:00) | Brief runs before the release slot (N2); 0-holdings fixed (N5) |
| Dashboard site, v2 pages, `_Run_Log`, signal histories | **BLOCKED** | Not inspected |

---

## 4. Implementation register

Branch `claude/vibrant-bell-w75ywx`, one draft PR (link in the PR list). Base `08a15bc`.

| ID | Change | Files | Tests | Status |
|---|---|---|---|---|
| N1-prod | Render env: `TFB_MAX_REQUESTS=200`, `TFB_MAX_REQUESTS_JITTER=50`, `MALLOC_ARENA_MAX=2` (merge, no other variable touched). Triggered deploy `dep-dbf7glt9fdbs7393qrog` of the same commit `08a15bc` at 21:44:55 UTC, with no workflow run in progress | Render service env | — | **APPLIED**. Production verification: see section 6 |
| N1-code | `start_web.sh` 2.6.1 → 2.7.0: the same values become defaults when unset; boot line now prints `max_requests`, `max_requests_jitter`, `malloc_arena_max` | `scripts/start_web.sh` | `tests/test_start_web_recycling.py` (5): runs the real launcher with `--print-plan` and a stub gunicorn. 5/5 pass on the branch; 4/5 fail on base (golden negative) | APPLIED · TESTED (not deployed; needs merge + deploy) |
| N5 | Brief ledger reconciliation + send label, two kill-switches (`TFB_BRIEF_HOLDINGS_RECONCILE=0`, `TFB_BRIEF_SEND_LABEL=0` restore v1.18.1) | `scripts/run_daily_brief.py`; pin in `scripts/verify_deployment.py` | `tests/test_daily_brief_ledger_reconcile.py` (12): 12/12 on the branch; 10/12 fail on base. Existing `test_daily_brief_blocked_inputs.py` still passes | APPLIED · TESTED (runs from Actions on `main`, so live once merged) |
| CI | ci.yml 1.1.37 → 1.1.38 adds both suites to the blocking lean lane | `.github/workflows/ci.yml` | YAML parses | APPLIED |

Whole suite on the branch (`pytest -n 4`): 6,005 passed, 45 skipped, 2 failed.
- `test_verify_manifest_pins`: the brief version pin. Fixed, then re-run passing.
- `test_sync_publication_clock[UTC]`: an xdist ordering flake in an unrelated module (102/102 in isolation, as recorded by the 25 Oct audit).

**Rollback.**
- Env: set `TFB_MAX_REQUESTS=0`, delete `TFB_MAX_REQUESTS_JITTER` and `MALLOC_ARENA_MAX`, and redeploy. This restores the previous command line exactly.
- Code: revert the PR commit. Each brief behaviour also has its own kill-switch above.
- No schema, sheet-contract or data migration is involved.

---

## 5. Prioritised remaining backlog

| # | Priority | Item | Dependency | Acceptance criteria | Effort |
|---|---|---|---|---|---|
| B1 | Critical | Recommendation validity (N3). Run the existing shadow scorer and backtest harness on chronological holdouts. Test whether the 85–100 reliability band and the INVESTABLE gate add value over a simple baseline (e.g. WATCHLIST-equivalent or momentum-only). Report results net of costs. Until then, label Top-10 as a screen | Owner decision; existing `shadow_scorer.yml` / `backtest_harness.yml` | Walk-forward report with n, CI, net-of-cost win rate and Brier per bucket; INVESTABLE CI lower bound > WATCHLIST, or the gate is revised in shadow first | 1–2 days |
| B2 | High | Scheduling (N2): chain release → brief with `workflow_run`, and/or dispatch the sync from an Apps Script time trigger | Owner: email cadence, PAT in Script Properties | Sync start within 20 min of target on 5 consecutive days; brief sent after the release it reports | 2–4 h + owner |
| B3 | High | Merge this PR and draft #835, deploy, then confirm 24 h memory stays below 80% without a manual deploy. Bound `_symbol_snapshots` (LRU cap) in a follow-up engine release | Owner merge/deploy | Peak memory < 3.4 GB over 24 h; boot line shows `max_requests=200` | 30 min + follow-up 0.5 day |
| B4 | High | Top-10 refresh after each sync (25 Oct B1; PR #788) and #831 in-flight defer | Apps Script access; owner merge | Decision Surface Freshness green on the in-sync check | 1–2 h owner |
| B5 | Medium | Verify N4 on Monday. Log per-symbol rejection reasons from `_outcome_price_from_row`. Replace the dead Yahoo fallback from GitHub runners (always 429) with backend retry | — | Weekday maturation coverage ≥ 70%; pending backlog falling | 2–4 h |
| B6 | Medium | Zero Open (N6): treat open ≤ 0 as missing when price > 0, at the provider boundary that emits it, once identified | Authenticated quote trace | Validator `open_in_day_range` shows no `=0.0` entries | 1–2 h |
| B7 | Medium | Refresh `TFB_PF_FX_RATES`/`TFB_PF_FX_AS_OF`; set `APP_VERSION` to 8.14.7 | Render env (owner) | No FX-age warning; `app_version` = `entry_version` | 10 min |
| B8 | Medium | Why Portfolio_Decision omitted 5023.SR at 21:15 (N5 root cause on the sheet side) | Sheet access | Brief shows "1 holdings" with no ledger notice | 1 h |
| B9 | Low | Triage ≥101 open PRs (≥65 older than 1 Oct); regenerate `docs/flags.md` (908 flags now vs 869 committed) | Owner | Open PRs < 30; registry current | 2 h |

---

## 6. Before / after

| Measure | Before | After | Verified in production? |
|---|---|---|---|
| Gunicorn worker recycling | off (never) | `--max-requests 200 --max-requests-jitter 50` via env | Partly. Deploy `dep-dbf7glt9fdbs7393qrog` live at 21:46:43 UTC on new instance `zww4k`; `/health` and `/readyz` 200; `render_git_commit 08a15bc`. The deployed v2.6.1 boot line does not print max_requests, so the first recycle ("Booting worker" after ~200 requests) remains to be observed |
| `MALLOC_ARENA_MAX` | unset | 2 | Same as above |
| Memory, live instance | 4.06 GB / 4.29 GB (94.5%) | Reset by the deploy. Sustained effect needs ≥ 24 h of observation | Not yet (needs a day of metrics) |
| Brief, 0 decision rows + 1 ledger holding | "0 holdings", no notice | "0 of 1 holdings" + "My_Portfolio holds 1 position(s) that Portfolio_Decision does not list (5023.SR)" | No (after merge, next scheduled brief) |
| Brief, partial decision page (1 of 2 listed) | Partial MV/cost shown as book total | "unavailable" + ledger notice | No |
| Brief at 21:15 Sunday | "before market open" | "after Tadawul close" | No |
| 5xx, 48 h | 0 | — | Yes (baseline) |

Forecast performance (N3) is a baseline, not an improvement claim. This audit changed no
scoring, ranking, gating or learning logic.

---

## 7. Final verdict

- **Operationally healthy:** *Partly.*
  - The API is up with 0 5xx, and market syncs complete with full coverage.
  - Memory was at 94.5%; the recycling fix is live via env, but its sustained effect is unverified.
  - GitHub starts every scheduled run hours late.
  - The decision pages still lag each sync, so the in-sync publication check is red every cycle.
- **Financially consistent:** *Mostly, for the visible book.*
  - The single holding (sukuk 5023.SR) is priced and fresh.
  - Draft #835's weight/P&L fixes are still not deployed.
  - Operator FX rates are stale.
  - Broker reconciliation was **not verified** (IBKR not used; no statement supplied).
- **Sufficiently validated for recommendation use:** **No.**
  - On 974 decided outcomes, INVESTABLE signals won 47.2% (CI 44–50%), below WATCHLIST.
  - The highest stated reliability band won 42%, and calibration is worse than a coin flip.
  - The decision feed's gating is working as designed, but what it releases is not shown to have predictive value.
  - Use it as a screening aid with independent verification, and only while a feed with `accepted=true` is unexpired.
