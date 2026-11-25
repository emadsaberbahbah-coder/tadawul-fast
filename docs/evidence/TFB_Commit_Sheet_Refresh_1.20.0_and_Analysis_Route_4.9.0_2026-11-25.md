# Commit sheet: 05_Refresh.gs 1.20.0 (failed-batch ledger) and analysis_sheet_rows 4.9.0 (compact response)

Date: 2026-11-25. Week plan, day 3, package 3: refresh reliability and a compact response.

## 1. apps_script/05_Refresh.gs 1.19.0 -> 1.20.0 [FAILED-LEDGER]

**Why.** A market page refreshes in chunks of symbols. Before this change, when a chunk failed, the run did three things:
- it counted the chunk once in `failedBatches`;
- it moved the checkpoint past the chunk;
- it closed as WARN without naming which symbols kept yesterday's row.

A pause (timeout, manual priority, tripwire) lost even that count.

**What changed.**
1. **Ledger** (always on). This step only observes and logs.
   - `checkpoint.failed` records each symbol of a failed chunk with its number of attempts. It is saved with the checkpoint, so it survives a pause and a resume.
   - A symbol comes off the ledger only when its row is written. A symbol that the response leaves out, that the keep-last-good filter withholds, or that the writer defers stays on it (Codex P2 on #1078).
2. **One bounded retry.** This needs Script Property `REFRESH_FAILED_RETRY = on`.
   - Each failed symbol is queued once more at the tail of the run. `checkpoint.retried` holds it to one retry.
   - At most `REFRESH_FAILED_RETRY_MAX_` symbols (60) are retried in a run.
   - The retry goes through the same fetch, guard and write path as any other chunk.
3. **PARTIAL status.** This needs the same property.
   - A run that ends with symbols still on the ledger closes PARTIAL and names them.
   - A failure that the retry recovered no longer turns the run WARN.
   - The rows of unwritten symbols are never written, so their last-good values stay, because the upsert never deletes.
4. **`baseCount` on new checkpoints.**
   - `isCheckpointCompatible_` compares the requested prefix of the symbol list. A run paused after a retry or a v1.13.0 requeue therefore resumes where it stopped, instead of starting the page again.
   - A chunk stops at `baseCount`, so a first try never shares a request with a symbol that already failed.
   - A checkpoint without `baseCount` is compared and cut exactly as in 1.19.0.

**Observe mode** (property unset or anything but `on`): the run is the 1.19.0 run plus one warning line, `[v1.20.0 FAILED-LEDGER observe]`. That line names the symbols that kept their previous rows and says what `on` would do.

**Code.** No function was removed. Six functions were added:
- `_refreshFailedRetryMode_`
- `_refreshNoteFailedChunk_`
- `_refreshLedgerKey_`
- `_refreshClearFailedChunk_`
- `_refreshWrittenKeys_`
- `_refreshFailedSymbols_`

**Rollback:** leave the property unset.

**Tests.** `tests/test_refresh_v1200_failed_ledger.js`, a node harness over the real file, collected through `tests/test_refresh_v1200_failed_ledger.py`. It passes 11/11:
- **R1** – version lockstep; the function inventory is +6 and −0.
- **R2** – golden negative: observe without a failure equals the 1.19.0 run. That covers fetches, writes, panel, status, log and return. The old file is rebuilt from the hunks in `tests/js/refresh_edits_v1200.json` and pinned by SHA-256.
- **R3** – observe with a failure adds exactly one line and makes no retry.
- **R4** – `on`: a failure is recovered and the run closes SUCCESS.
- **R5** – `on`: a persistent failure is retried once and the run closes PARTIAL; the failed symbols are never written.
- **R6** – a write that throws is treated like a failed fetch.
- **R7** – the retry is bounded at 60.
- **R8** – a pause keeps the ledger and the retry tail, and the next run resumes rather than restarts.
- **R9** – a checkpoint without `baseCount` is judged as in 1.19.0.
- **R10** – the pure helpers.
- **R11** – a retry that answers with no rows, or with only some rows, or a row the writer defers, keeps those symbols on the ledger; only written rows count as recovered.

## 2. routes/analysis_sheet_rows.py 4.8.4 -> 4.9.0 [COMPACT-SHAPE]

**Why.** The default envelope repeats a lot:
- the row objects five times over (`row_objects`, `items`, `records`, `data`, `quotes`), then again keyed by header in four display copies;
- the matrix three times;
- the contract seven times.

For a 25-row page of 115 fields, most of the bytes are repeats.

**What changed.** A request carrying `response_shape=market-objects.v1` gets one compact response. The parameter can be sent:
- in the query string, on GET or POST;
- in the POST body.

The compact response has these fields: `response_shape`, `status`, `request_id`, `version`, `page`, `route_family`, `contract {keys, headers, count}`, `rows` (one object per row, in contract-key order), `count`, `detail`, `error`, and `meta` (the legacy meta plus `response_shape`).

- It is cut from the finished legacy envelope, so it holds the same values after the same projection, coherence pass and rank pass. Nothing is recomputed.
- The parameter is taken out of the body before the request is served.
- Without the parameter, or with any other value, the response is the 4.8.4 one.
- No caller uses it yet.

**Code.** No function was removed. Three functions were added: `_requested_response_shape`, `_without_response_shape` and `_compact_market_objects`. One constant was added: `RESPONSE_SHAPE_COMPACT`.

**Tests.** `tests/test_analysis_compact_shape.py` passes 7/7:
- **C1** – the default and other values give the legacy envelope.
- **C2** – compact equals legacy on all 115 fields of every row, checked against `row_objects`, `display_row_objects` and `rows_matrix`.
- **C3** – contract, status, request id and meta are carried over.
- **C4** – the parameter works from the query on GET and from the POST body, and the caller's body is unchanged.
- **C5** – the runtime fallback is compacted.
- **C6** – the compact response is less than a third of the legacy size.
- **C7** – version pins.

**Pins moved in place to 4.9.0:**
- `scripts/verify_deployment.py`
- `tests/test_auth_wall_scope_path.py`
- `tests/test_analysis_sheet_rows_rank_withheld.py`
- `tests/test_core_repair_tiny_quotes.py`

**Other test updates.**
- `tests/test_route_error_redaction.py` loads the three new helpers into its isolated namespace.
- CI 1.1.103 runs both new tests in the route lane.

## Not in this PR

- **Portfolio_Decision_v2 dividend fix.** When the page falls back to cached price history, its v2 rule reads a pre-dividend high (SUPN on 24 Nov: "stop 33.76 hit"). The cache keeps bars but not the dividends. This needs its own 27_Decision_Pages_v2 release, together with the page's pinned history harness.
- **Apps Script export.** The bound Apps Script export has not been received, so there is no repository diff to report.

Nothing here is deployed or switched on: the Apps Script file goes in only when the owner pastes it, and `REFRESH_FAILED_RETRY` stays unset until the owner decides.
