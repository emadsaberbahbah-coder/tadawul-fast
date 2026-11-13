# Universe Cap Raise and Refresh Segmentation Design

**Task:** CG-13  
**Status:** `WINDOW-SAFE` — design/documentation only  
**Base:** `main` at `b8636ebf21e1528a409e656d7f2c20ba6cd3b75e`  
**Decision boundary:** This document changes no route, provider, scoring, ranking, recommendation, portfolio, Sheet-write, trigger, or deployment behavior.

## 1. Executive decision

Raising a single `5000` limit is necessary but not sufficient.

The safe target is a two-part design:

1. **The live analysis route supports the complete requested universe through stable pagination.**
2. **Large refreshes are split into deterministic venue-aware fetch segments, but only one validated aggregate is allowed to publish to Google Sheets.**

The design must preserve four existing safety properties:

- no symbol is silently dropped because it falls beyond a read or request bound;
- no partial matrix job clears or overwrites the production page;
- missing or failed segments remain explicit and fail closed;
- manual Apps Script refresh keeps priority through renewable, owner-scoped lease checkpoints.

The first production target is `Global_Markets`, because it is the largest page and has already demonstrated a long-running partial state. The design then extends to `Mutual_Funds`, `Market_Leaders`, and `Commodities_FX` only after the identical acceptance gate passes.

---

## 2. Current state — live source evidence

### 2.1 Route request cap

The mounted advanced sheet-rows implementation currently accepts `limit`, `offset`, and `top_n`, but both `limit` and `top_n` are hard-clamped to `5000`; an omitted `limit` defaults to `2000`:

```python
limit = max(1, min(5000, _maybe_int(merged_body.get("limit"), 2000)))
offset = max(0, _maybe_int(merged_body.get("offset"), 0))
top_n = max(1, min(5000, _maybe_int(merged_body.get("top_n"), limit)))
```

**Source:** `routes/advanced_analysis.py:~3153-3155`  
**Blob:** `fec4ac49297c96faa0f592aa027db0f757b8eb22`

Current consequences:

- one request cannot ask the route for more than 5,000 rows;
- a caller that omits `limit` receives at most 2,000;
- `offset` exists, but there is no documented immutable snapshot/cursor contract proving that multiple offset calls represent one stable universe;
- a page larger than 5,000 therefore depends on repeated requests whose source state may change between calls.

### 2.2 Page read bound and symbol cap in the Python sync

`run_dashboard_sync.py` already contains the first generation of universe-cap protection:

- under `TFB_SYNC_UNIVERSE_CAP_V2`, market-page symbol readback requests `A1:E{_page_read_row_bound()}` instead of the legacy `A1:E5000`;
- the documented default for `TFB_SYNC_PAGE_READ_MAX_ROW` is `12000`;
- the workflow currently sets `TFB_SYNC_MAX_SYMBOLS_MARKET="7000"`;
- the code-side ceiling was raised to 20,000 under the existing universe-cap switch.

The active readback path still applies `max_symbols` after reading and ordering the symbols. Thus the effective requested universe is the minimum of the page contents, read bound, workflow cap, code ceiling, and available execution budget.

**Sources:**

- `scripts/run_dashboard_sync.py:~2860-2870` — bounded page readback and legacy `A1:E5000` fallback;
- `scripts/run_dashboard_sync.py` v6.24.3 WHY block — `TFB_SYNC_PAGE_READ_MAX_ROW`, 12,000 default and 20,000 request ceiling;
- `.github/workflows/daily_sync.yml:~390-430` — `TFB_SYNC_MAX_SYMBOLS_MARKET="7000"` and rationale.

**Blobs:**

- `scripts/run_dashboard_sync.py`: `c2edb6ab01e996a67781735bf334552456b4460b`
- `.github/workflows/daily_sync.yml`: `ad198987614251794602b1929bbe0faf151bb0c1`

### 2.3 Current batch and time-budget model

The production workflow currently uses:

- `TFB_SYNC_SYMBOL_BATCH_SIZE="25"`;
- `TFB_SYNC_TIME_BUDGET_SEC="3600"`;
- a dedicated `Global_Markets` matrix leg;
- a 90-second stagger between the core and Global Markets legs;
- a 115-minute outer GitHub job timeout;
- a single production workbook concurrency lease shared by scheduled/manual production syncs.

The repository comments explain why: a complete Global Markets request fan-out can exceed the backend edge window, while an external job cancellation before the final write can discard an hour of fetched work.

This model is safer than one giant request, but it still keeps too much responsibility in one long process:

- it plans, fetches, retries, assembles, validates, and publishes;
- its 3,600-second internal budget may end before the full page is fetched;
- partial-write persistence protects existing rows, but the page may require multiple runs to converge;
- a process restart loses in-memory segment progress.

### 2.4 GAS manual-pause behavior

The live repository coordinator is v1.0.2 and currently defines:

```javascript
MANUAL_PAUSE_TTL_MS: 20 * 60 * 1000
```

It supports:

- `DocumentLock` for coordinator state;
- `ScriptLock` for actual refresh work;
- automatic entrypoint recheck after lock acquisition;
- cooperative automatic yield only between safely persisted steps;
- one owned deferred trigger;
- cleanup that clears only the finishing request's pause.

The live main file does **not** contain a renewable manual heartbeat. `tfbExecuteManualHandler_` extends once and invokes `configured.fn()` without passing a renewal capability.

**Sources:**

- `apps_script/11_Manual_Refresh_Coordinator.gs:~20-40` — 20-minute TTL;
- `apps_script/11_Manual_Refresh_Coordinator.gs:~300-355` — automatic safe-boundary yield contract;
- `apps_script/11_Manual_Refresh_Coordinator.gs:~500-590` — manual execution and one-time extension.

**Blob:** `9430f7c943027c1c6e78b06cf7f9d0913442d69e`

CG-9 separately designs the renewable lease. This document consumes that contract; it does not implement or redeploy it.

### 2.5 Observed operating symptom

The task evidence records a long-running `Global_Markets` manual refresh observed at `1400/6190`. That observation is operational evidence supplied in the CG-9/CG-13 task pack; it is not inferred from the repository source.

The important design implication is not merely that the page is large. It is that one logical refresh may outlive:

- a 20-minute GAS pause;
- a backend edge request;
- a single process's comfortable fetch window;
- an operator's ability to distinguish slow progress from a dead run.

---

## 3. Problem statement

The current system has four independent bounds:

1. **Route bound:** 5,000 rows per route request, defaulting to 2,000.
2. **Sheet-read bound:** 12,000 rows under the current v2 switch, 5,000 under rollback.
3. **Requested-universe cap:** workflow currently 7,000, with a 20,000 code ceiling.
4. **Execution bound:** 3,600 seconds internal and 115 minutes external for a workflow leg, plus the backend edge limit per request.

Changing only the route clamp creates a larger single request and increases timeout/provider pressure. Changing only the workflow cap allows more symbols into the worklist but does not guarantee completion. Increasing only the job timeout delays failure and increases the amount of in-memory work that can be lost.

The design therefore treats **capacity, segmentation, snapshot consistency, and publication atomicity** as one system.

---

## 4. Target architecture

### 4.1 One immutable universe manifest per refresh

A planner creates one manifest before any segment fetch starts:

```json
{
  "run_id": "uuid",
  "page": "Global_Markets",
  "universe_hash": "sha256",
  "created_at_utc": "...",
  "source_row_count": 6190,
  "ordered_symbols": ["..."],
  "segments": [
    {
      "segment_id": "Global_Markets:US:0001",
      "venue": "US",
      "start_index": 0,
      "end_index": 499,
      "symbol_count": 500,
      "symbols_hash": "sha256"
    }
  ]
}
```

Requirements:

- the ordered symbol list is captured once;
- canonical symbol normalization occurs before hashing;
- duplicate and blank symbols fail the plan;
- every segment carries indexes into the original order;
- the manifest is retained as evidence;
- retries reuse the same manifest and `run_id`, rather than rebuilding the universe.

### 4.2 Stable route pagination

The route keeps `limit` and `offset` compatibility, but adds a stable pagination contract:

- first call resolves a snapshot and returns `snapshot_id`, `universe_total`, `page_size`, `next_offset` or `next_cursor`, and `universe_hash`;
- later calls must present the same `snapshot_id`/cursor;
- a missing/expired snapshot fails explicitly instead of silently rebuilding a different universe;
- response rows preserve canonical universe order;
- a healthy full-universe caller can request all pages until `next_cursor` is null;
- the route never fabricates rows to fill a page.

The raised absolute ceiling is a safety maximum, not the recommended request size. Normal production requests remain paginated in bounded pages.

### 4.3 Venue-aware fetch segmentation

The planner groups the manifest by verified venue, then chunks each group by a bounded segment size.

Recommended first production plan:

- `Global_Markets` only;
- deterministic venue groups derived from canonical listing metadata/suffix mapping;
- unknown venue instruments placed in an explicit `UNKNOWN` segment, never dropped;
- initial segment size: 400-500 symbols;
- each segment internally keeps the existing symbol batch size of 25;
- initial maximum parallel fetch segments: 1;
- raise to 2 only after rate-limit, timeout, order, and identity gates remain unchanged.

Venue grouping reduces simultaneous pressure on the same upstream market/provider family and creates smaller, independently diagnosable failure domains.

### 4.4 Fetch workers are zero-write

Segment matrix jobs must not write Google Sheets.

Each worker:

1. receives one immutable segment definition;
2. fetches only that segment's symbols;
3. returns structured rows and per-symbol evidence;
4. records requested, attempted, fresh, blocked, unavailable, and failed counts;
5. writes a JSON artifact named by `run_id` and `segment_id`;
6. exits non-zero when its segment verdict is failed.

A worker may be retried without risking workbook overlap because it owns no publication path.

### 4.5 One aggregate validator and one publisher

A final aggregator downloads every segment artifact and checks:

- every manifest segment is present;
- segment and universe hashes match;
- every requested symbol is classified exactly once;
- no duplicate symbol or index exists;
- no index gap exists;
- row identity, venue, currency, and schema gates pass;
- error/rate-limit metrics do not regress;
- the final row order matches the manifest;
- all rows belong to the same `run_id`/snapshot.

Only after those checks pass does one publisher perform the existing guarded Sheet merge/write.

Publication rules:

- one workbook writer only;
- no clear before a complete validated matrix is available;
- current KEEP-LAST-GOOD, persistence, identity firewall, coverage floor, and page verdict remain active;
- if any required segment is missing or failed, no full-page publication is claimed;
- targeted recovery reruns only failed segments against the same manifest;
- the final verdict distinguishes `complete`, `partial_preserved`, and `failed_no_publish`.

### 4.6 GAS manual refresh integration

After CG-9 is live, a manual GAS refresh uses the same conceptual segment boundaries:

- retain the coordinator-owned `requestId`;
- call the backend for one route page/segment at a time;
- complete write, verification, and checkpoint for that segment;
- call `tfbRenewManualPause_(requestId)` only after the safe checkpoint;
- never renew after a page clear and before a completed write;
- yield or queue continuation at a segment boundary;
- stop at the six-hour hard ceiling with an explicit partial/checkpoint state.

This does not replace the Python production write lease. It protects the separate bound Apps Script manual/automatic execution path.

---

## 5. Data contracts

### 5.1 Route page response

Required metadata:

```json
{
  "run_id": "...",
  "snapshot_id": "...",
  "page": "Global_Markets",
  "offset": 0,
  "page_size": 500,
  "universe_total": 6190,
  "universe_hash": "...",
  "next_offset": 500,
  "next_cursor": "...",
  "rows": [],
  "requested_count": 500,
  "returned_count": 500,
  "status": "success"
}
```

A response with fewer rows than requested must explain the reason and classify every missing symbol; it must not present a normal `success` status with silent omissions.

### 5.2 Segment artifact

Required fields:

- `run_id`, `snapshot_id`, `segment_id`, `page`, `venue`;
- `start_index`, `end_index`, `symbols_hash`;
- exact ordered requested symbols;
- exact ordered output rows;
- requested/attempted/fresh/preserved/blocked/unavailable/failed counts;
- provider HTTP/error counters;
- start/end times and elapsed time;
- process/deployment revision;
- zero-write assertion.

### 5.3 Aggregate publication verdict

Required fields:

- expected versus received segment count;
- expected versus classified symbol count;
- duplicates, missing indexes, extra symbols;
- schema and identity results;
- final ordered-universe hash;
- rows written and rows preserved;
- publication status and reason;
- source segment artifact references;
- `do_not_publish` boolean.

---

## 6. Rollout plan

| Stage | Change | Kill switch | Acceptance test | Rollback |
|---|---|---|---|---|
| **0 — Design approval** | Review this document only. | N/A | Emad and Claude agree on contracts, ENV scope, and publication model. | Close PR; no runtime effect. |
| **1 — Route pagination shadow** | Add pagination metadata and bounded pages; keep existing 5,000 ceiling and existing callers unchanged. | `TFB_ADV_SHEET_ROWS_PAGINATION=0` | Fixture and live no-write run prove ordered pages reconstruct the current response exactly with zero duplicates/missing rows. | Disable flag; old route behavior returns. |
| **2 — Full-universe route canary** | Raise ceiling under flag; keep page size bounded. | `TFB_ADV_SHEET_ROWS_PAGINATION=0` or restore ceiling to 5,000 | Read-only 6,190+ symbol request completes through pages, stable snapshot/hash, no increase in 429/5xx/timeouts. | Restore old ceiling/flag. |
| **3 — Segmented workflow shadow** | Planner + zero-write segment matrix + aggregator artifacts for `Global_Markets`. | `TFB_SYNC_SEGMENTATION=0` | Same requested universe/order as sequential run; 100% classified; no Sheet writes; rate and timeout metrics no worse. | Disable segmentation; existing GM leg remains. |
| **4 — Single-page production publication** | One aggregator publishes validated Global Markets result. | `TFB_SYNC_SEGMENTATION=0` | Three clean cycles; one writer; full-row audit passes; no mixed run IDs; KLG/identity gates unchanged. | Disable segmentation and revert to current GM leg. |
| **5 — Controlled parallelism** | Raise segment parallelism from 1 to 2. | `TFB_SYNC_SEGMENT_MAX_PARALLEL=1` | Same universe/order, zero identity regressions, no rise in 429/5xx/timeouts, memory within limit. | Return to 1 immediately. |
| **6 — Additional pages** | Enable Mutual Funds, then remaining pages one at a time. | `TFB_SYNC_SEGMENT_PAGES` | Each page passes the identical three-cycle gate before the next is added. | Remove the page from the list. |
| **7 — GAS manual segmentation** | Wire CG-9 renewal/checkpoint behavior in the bound script. | Remove handler integration / restore prior bound version | Manual-during-automatic canary proves safe yield, renewal, continuation, and no duplicate trigger. | Restore archived GAS source and trigger inventory. |

---

## 7. Acceptance test matrix

### Route correctness

- 4,999, 5,000, 5,001, 6,190, 7,000, and 20,000-symbol fixture universes.
- `limit` omitted still follows documented default.
- page/cursor reconstruction equals the canonical ordered universe.
- no duplicate or omitted index.
- stale or invalid cursor fails explicitly.
- snapshot change between pages is detected.
- schema width and order remain identical on every page.

### Segment planning

- every symbol is assigned exactly once;
- canonical order can be reconstructed after venue grouping;
- unknown venue symbols remain present in `UNKNOWN`;
- duplicate canonical symbols fail planning;
- segment hash changes when any symbol/order changes;
- retry plan selects only failed segments and preserves `run_id`.

### Fetch and provider behavior

- zero-write assertion per worker;
- first 402/429/5xx/timeout behavior remains owned by provider protections;
- no increase in aggregate 429, 5xx, or timeout rates versus the same sequential universe;
- process memory remains within the Render plan limit;
- one segment failure cannot cancel completed artifact evidence from other segments.

### Aggregation and publication

- missing artifact blocks full publication;
- duplicate segment blocks publication;
- mismatched `run_id`, snapshot, universe hash, or schema blocks publication;
- final output order equals manifest order;
- one Sheet write path only;
- no clear occurs before aggregate validation;
- full-refresh coverage and decision-surface audits pass after publication;
- page verdict includes requested, classified, fresh, preserved, blocked, and written counts.

### Manual GAS behavior

- matching request ID renews only after a persisted checkpoint;
- stale/replaced request ID cannot renew;
- automatic run yields only at a safe boundary;
- deferred continuation is unique and owner-scoped;
- hard ceiling ends the lease and leaves an explicit resumable checkpoint;
- no trigger unrelated to the coordinator is deleted.

---

## 8. Environment-variable design

No variable below is added by this documentation PR. These are the complete variables the later implementation would introduce.

### 8.1 Render-scoped variables — proposed

| Variable | Proposed default | Purpose | Rollback behavior |
|---|---:|---|---|
| `TFB_ADV_SHEET_ROWS_PAGINATION` | `0` | Master switch for stable snapshot/cursor pagination. | `0` preserves the current route contract. |
| `TFB_ADV_SHEET_ROWS_LIMIT_CEILING` | `5000` | Absolute route ceiling; raise to `20000` only after Stage 2 gate. | Set `5000`. |
| `TFB_ADV_SHEET_ROWS_PAGE_SIZE_MAX` | `1000` | Maximum rows per backend response page, independent of absolute ceiling. | Reduce without changing universe. |
| `TFB_ADV_SHEET_ROWS_SNAPSHOT_TTL_SEC` | `900` | Lifetime of immutable pagination snapshot/cursor state. | Disable pagination or redeploy to clear process-local state. |

These are backend/provider-data variables and therefore belong in **Render**, not GitHub workflow ENV.

### 8.2 GitHub-workflow-scoped variables — proposed

| Variable | Proposed default | Purpose | Rollback behavior |
|---|---:|---|---|
| `TFB_SYNC_SEGMENTATION` | `0` | Master switch for planner/matrix/aggregator path. | `0` runs the current page legs. |
| `TFB_SYNC_SEGMENT_PAGES` | `Global_Markets` | Comma list of pages eligible for segmentation. | Remove a page or blank the list. |
| `TFB_SYNC_SEGMENT_BY_VENUE` | `1` | Group by verified venue before chunking. | `0` uses stable order-only chunks. |
| `TFB_SYNC_SEGMENT_SIZE` | `500` | Maximum symbols in one fetch-worker artifact. | Lower for timeout/rate pressure; raise only after evidence. |
| `TFB_SYNC_SEGMENT_MAX_PARALLEL` | `1` | Maximum concurrent zero-write segment workers. | Return to `1`. |
| `TFB_SYNC_SEGMENT_TIMEOUT_SEC` | `900` | Per-segment worker wall-clock budget. | Lower/raise within the job ceiling; failed segment remains explicit. |
| `TFB_SYNC_SEGMENT_REQUIRE_COMPLETE` | `1` | Require every planned segment before full-page publication. | Must remain `1` for production; `0` is shadow diagnostics only. |
| `TFB_SYNC_SEGMENT_ARTIFACT_RETENTION_DAYS` | `30` | Evidence retention for plans, shards, and aggregate verdicts. | Retention-only change. |

These are sync orchestration variables and therefore belong in the **GitHub Actions workflow/job environment**.

### 8.3 Workflow-generated context — not operator settings

The matrix/plan may pass these internal values to workers:

- `TFB_SYNC_SEGMENT_RUN_ID`
- `TFB_SYNC_SEGMENT_ID`
- `TFB_SYNC_SEGMENT_VENUE`
- `TFB_SYNC_SEGMENT_START_INDEX`
- `TFB_SYNC_SEGMENT_END_INDEX`
- `TFB_SYNC_SEGMENT_SYMBOLS_HASH`

They are generated from the immutable plan; Emad should not set them manually in Render or repository variables.

### 8.4 Existing variables retained

The implementation should reuse rather than replace:

- `TFB_SYNC_UNIVERSE_CAP_V2`
- `TFB_SYNC_PAGE_READ_MAX_ROW`
- `TFB_SYNC_MAX_SYMBOLS_MARKET`
- `TFB_SYNC_SYMBOL_BATCH_SIZE`
- `TFB_SYNC_TIME_BUDGET_SEC`
- `TFB_SYNC_PAGE_ORDER`
- `TFB_SYNC_HEAL_FIRST`
- `TFB_SYNC_OLDEST_FIRST`
- the production workflow concurrency lease
- CG-9's bounded coordinator constants and owner-scoped renewal function.

---

## 9. Observability and evidence

Every segmented run must expose one summary line and one JSON artifact with:

- route revision and deployed Git SHA;
- page, run ID, snapshot ID, universe hash;
- planned/completed/failed segment counts;
- requested/attempted/classified/fresh/preserved/blocked/unavailable counts;
- 402/429/5xx/timeout totals;
- oldest/newest row age;
- aggregate order/hash verdict;
- write count and zero-write worker proof;
- publication decision and rollback switch values.

A green fetch matrix is not sufficient. The aggregate publication verdict and the existing full-row workbook audit must both be green.

---

## 10. Security and operational controls

- No credentials, tokens, symbol manifests, or service-account JSON in `workflow_dispatch` inputs.
- Segment artifacts must not contain secrets.
- Fetch workers receive read-only backend credentials and no Google Sheets write credentials where practical.
- Only the final publisher job receives Sheet-write credentials.
- Production publisher remains inside the existing non-cancelling workbook lease.
- CI and shadow segmentation use separate cancellable concurrency groups and never acquire the production write lease.
- A no-op or partial segment may never be converted to zero-valued facts or synthetic recommendations.

---

## 11. Open implementation decisions for Emad and Claude

1. **Snapshot storage:** process-local cursor cache, Redis, or PostgreSQL shadow store. Process-local is simplest but invalid across worker/deploy changes; Redis/PostgreSQL is safer for a multi-request production cursor.
2. **Venue authority:** canonical instrument master versus suffix-derived temporary grouping. Verified instrument metadata is the target; suffix grouping is acceptable only as an explicitly temporary fallback.
3. **Artifact aggregation versus operational store:** GitHub artifacts are sufficient for the first workflow canary; PostgreSQL is the longer-term source of truth.
4. **Initial segment size:** 400 or 500. Choose from measured p95 segment latency and memory, not intuition.
5. **Publisher partial policy:** recommended production rule is full planned coverage or no full publication; any preserved partial mode must remain clearly labeled and must never claim a complete refresh.

---

## 12. Explicit non-goals

This design does not:

- enable concurrency above the currently approved level;
- change provider order or retry behavior;
- change scoring, ranking, recommendation, portfolio, or trading logic;
- expand the investment universe by itself;
- deploy or edit the live bound Apps Script project;
- create PostgreSQL tables;
- modify Render or GitHub environment variables;
- treat a partial refresh as a complete one.

---

## 13. Source inventory

| File | Live base blob | Relevant evidence |
|---|---|---|
| `routes/advanced_analysis.py` | `fec4ac49297c96faa0f592aa027db0f757b8eb22` | Route `limit/top_n` hard cap 5,000; default limit 2,000; offset already present. |
| `scripts/run_dashboard_sync.py` | `c2edb6ab01e996a67781735bf334552456b4460b` | v6.24.3 universe read bounds/caps, symbol readback, batching, time budget, persistence and KLG safety. |
| `.github/workflows/daily_sync.yml` | `ad198987614251794602b1929bbe0faf151bb0c1` | GM matrix leg, 90-second stagger, 115-minute timeout, batch 25, 3,600-second budget, market cap 7,000. |
| `apps_script/11_Manual_Refresh_Coordinator.gs` | `9430f7c943027c1c6e78b06cf7f9d0913442d69e` | 20-minute pause, safe-boundary yield, owner-scoped trigger/cleanup; no renewable heartbeat in live main. |

## 14. Hand-back

- **Changed paths:** one Markdown file only.
- **AST inventory:** N/A — no Python or GAS source changed; function/class removals = 0.
- **Operator ENV:** none for this PR.
- **Window status:** `WINDOW-SAFE`.
- **Review gate:** no implementation should begin until Emad and Claude approve the route contract, segment publication model, and ENV list in this document.
