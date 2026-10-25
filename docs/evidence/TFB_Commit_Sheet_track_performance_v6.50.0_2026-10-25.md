# Commit Sheet - `scripts/track_performance.py` v6.50.0 [LOG-READ] - 2026-10-25

**Scope:** Performance_Log numbers are read back at full precision, so canonical v2 targets survive the read-back and an audit pass stops rewriting rounded values. ENV-gated, default OFF (`TRACK_LOG_UNFORMATTED_READ`).

**Files:**
- `scripts/track_performance.py` (6.49.0 -> 6.50.0)
- `.github/workflows/track_performance.yml` (1.0.5 -> 1.0.6, one env line)
- `.github/workflows/daily_sync.yml` (the same env line in the tracker step, ENV PARITY; header unchanged, as with #827)
- `scripts/verify_deployment.py` (pin only)
- `tests/test_workflow_flag_mappings.py` (tracker default)
- `tests/test_tracker_canonical_publications.py` (two guards in the blocking lean lane)
- `tests/test_tracker_log_read_precision.py` (new harness)
- this sheet

## S1 - Defect (evidence)

`PerformanceStore.load_records` read the log with `ws.get(rng)`. gspread 6.2.1 sends a render option only when one is given, so the Sheets API default `FORMATTED_VALUE` applied. The API documents that mode as values formatted according to the cell's formatting. Every number therefore came back as its displayed text: about 10 significant digits under the Automatic number format. The digit count is inferred from the production counts below; it cannot be measured offline.

The tracker writes the log RAW at full float precision (`append_rows(..., "RAW")`; `update` defaults to `raw=True`). `canonical_performance_report` proves a v2 target with `math.isclose(stored, (target/entry - 1) * 100, rel_tol=1e-9, abs_tol=1e-8)`. For every 1W/2W target (|ROI| under 10 pp) the binding limit is the absolute 1e-8 pp floor.

**Why 1W/2W and not 1M/3M:** a 1W/2W checkpoint target price is the interpolation `entry*(1-frac) + forecast*frac` (frac 7/30 or 14/30) and carries about 16 significant digits. Rendered at about 10 digits, it moves the price-implied ROI by up to about 5e-8 pp, past that floor, whatever the ROI size. Rounding the stored ROI alone changes nothing (about 5e-11 relative). 1M/3M target prices are the engine's 4 dp forecast prices, which fit in 10 digits. A target that was exact when written therefore fails as `schema_price_mismatch` once read back.

**Production lines:** `[CANONICAL-PERFORMANCE]` in three job logs.

| Job | Tracker step (UTC) | schema_price_mismatch | percent_points (canonical v2) |
|---|---|---|---|
| 120950277470 | 24 Oct 18:21 | 6 | 34 |
| 121006724938 | 24 Oct 23:28-23:29 | 6 | 222 |
| 121127954714 | 25 Oct 10:08-10:09 | 43 | 249 |

The 188 rows (47 symbols x 4 horizons) recorded at 23:29 UTC, 25 Oct Riyadh, were exact in memory on the run that wrote them. On the next run, which read them back, 37 more targets failed. The log lines carry no per-horizon split; the replay below attributes such losses to 1W/2W only.

**Offline replay:** 400 sources x 4 horizons, rendered `.10g`.
- 1W lost 129/400 and 2W 135/400.
- 1M/3M lost 0.
- A raw round-trip loses 0.

**Permanence:** `run_once` runs load, then audit, then `_publish_s1_calibration`, then `save_records`. `save_records` rewrites every row from the loaded, rounded floats. An `--audit` pass therefore makes the loss permanent, whether manual or scheduled with `TRACK_SCHEDULED_AUDIT=1`.

**Consumers of the canonical target:**
- S-1 criterion 4 (`_S1_Calibration`, enforce basis);
- the canonical unit counts and by-horizon buckets;
- the outcome witness at maturity (`target_achieved_at_horizon`, `forecast_error_pp`).

## S2 - Change (additive; zero removals)

| Mode | Read | Log | Records |
|---|---|---|---|
| off (DEFAULT; unset or any other word, '1' included) | `ws.get(rng)` exactly as v6.49.0 | none | v6.49.0 |
| observe | the off read, plus ONE extra read of the same range with `UNFORMATTED_VALUE` + `FORMATTED_STRING` | one WARNING `[v6.50.0 LOG-READ] mode=observe rows=N unformatted_rows=N precision_only=P divergent={..} text_differs={..} misaligned=M canonical_mismatch formatted=A unformatted=B`; a failed extra read logs one WARNING and changes nothing | the off records |
| enforce | `ws.get(rng, value_render_option="UNFORMATTED_VALUE", date_time_render_option="FORMATTED_STRING")` | one INFO `[v6.50.0 LOG-READ] mode=enforce rows=N` | parsed by the unchanged `from_sheet_row` |

**Observe counters.** Both reads are parsed by `from_sheet_row` and compared field by field, which is exactly what enforce would change:
- `precision_only` counts numbers within 1e-6 (absolute below 1, relative above).
- `divergent` counts, per header, numbers that differ beyond that (for example a percent number format).
- `text_differs` counts, per header, any other parsed difference (for example a typed boolean, or a number typed into a text column).

**Additions:** `_log_read_mode`, `_log_read_observe`, `_LOG_READ_TAG`, `_LAST_LOG_READ`. Zero top-level functions removed (AST inventory). Signal_History and every other reader are untouched.

## S3 - Known limits

- **No repair.** Rows already rewritten from a rounded read by an earlier audit pass stay rounded. A repair would be a separate owner decision and is not proposed here.
- **Percent or boolean cells.** A cell the owner typed with a percent number format would change meaning under enforce: the unformatted value is the fraction. A typed boolean reads `True` instead of `TRUE`. Observe names both before enforce is set (L5).
- **Lean lane.** The full harness runs in the advisory whole-suite job. Three guards (off identity, observe = off records, and the enforce round trip) run in the blocking lean lane through `tests/test_tracker_canonical_publications.py`. `ci.yml` is not touched: open #778/#779 claim 1.1.38/1.1.39.

## S4 - Tests

**`tests/test_tracker_log_read_precision.py`:** PASS 7/7 as a script; 7 passed under pytest; identical under PYTHONHASHSEED 0, 1 and 12345.
- With no git base (`GIT_DIR=/nonexistent`, as on CI's depth-1 checkout): 6 passed, 1 skipped. L2 then checks `OFF_GOLDEN = 39dae323624dc93e`, the base digest. This holds even with stray `TRACK_RISK_BLANK=1` / `TRACK_DEDUP_MATURED=enforce` in the caller's environment, because every case clears the tracker variables.
- The fake sheet leaves trailing empty cells out of each row, as `values.get` does.
- In mixed pytest sessions with the board-coverage, HIST-CLOSE and canonical suites, in both orders: 64 passed, 1 skipped.

| Case | What it pins |
|---|---|
| L1 | version floor `>= (6, 50, 0)`; exact-word parser (`'1'`, `'on'`, typos -> off) |
| L2 | off, five spellings: one `get("A6:AF1005")` with no keyword, no log line at all, records and log lines equal the base's, digest equal. The formatted read loses 1W/2W canonical targets and no 1M/3M one: the defect, pinned so L3 is not vacuous. |
| L3 | enforce: zero `schema_price_mismatch` (asserted first); one get of the off range with exactly the two options; every reloaded canonical equal to the in-memory one; strings, dates, blank and numeric Realized ROI parse as off; a blank-Record-ID row is read (`rows=161`) and skipped; one INFO line |
| L4 | enforce load -> `save_records` -> enforce load is exact. An off load -> save keeps the rounding (the residual). |
| L5 | observe: records equal off; two gets; one WARNING (`precision_only=220 divergent={} text_differs={} canonical_mismatch formatted=35 unformatted=0`). Each surprise below is named, and the records stay equal to off:
- a percent format on `Target ROI %` (`divergent`);
- a `0.00` format on `Target Price` (`divergent`, 74 of the 80 interpolated 1W/2W prices);
- seven boolean `Entry Selected` cells (`text_differs`);
- five numbers typed into `Entry Investability`, which parse as '0' formatted and '' raw (`text_differs`);
- two swapped unformatted rows (`misaligned=2`, never compared);
- a blank-Record-ID row (skipped in both reads).

A failed extra read logs `comparison skipped: OSError`, and the load still returns the off records. |
| L6 | the real gspread `Worksheet.get` / `HTTPClient.values_get`: both modes request `A6:AF1005`; off sends no render option; enforce sends `valueRenderOption=UNFORMATTED_VALUE&dateTimeRenderOption=FORMATTED_STRING`. Skipped without gspread. |
| L7 | ASCII added lines; zero functions removed; on 6.50.0 exactly `_log_read_mode` and `_log_read_observe` added (a later release may add more) |

**Golden negative:** module under test = `git show 08a15bc0:scripts/track_performance.py`.
- Result: 6 failed, 1 passed (L2, off identity).
- L3 fails on the outcome: `{'1W': 19, '2W': 16}` mismatches, 35 of the 80 1W/2W targets.

**Blocking-lane guards** (`tests/test_tracker_canonical_publications.py`): 32 passed. On the base tree the enforce and observe guards fail, and the three off guards pass.

**Mutations: 22/22 caught.** Each one maps to the case(s) that catch it:

| Mutation | Caught by |
|---|---|
| enforce without options | L3, L4, L6 |
| enforce without the date option | L3, L6 |
| off reading unformatted | L2, L4, L5, L6 |
| off passing an explicit `None` | L2, L5 |
| parser accepting `'1'` | L1, L2 |
| case-sensitive parser | L1 |
| observe acting as enforce | L5 |
| observe silent | L5 |
| observe tolerance 1e-2 | L5 |
| observe exception escaping | L5 |
| observe without the text check | L5 |
| enforce without its log line | L3 |
| off logging | L2 |
| observe counting the wrong parse | L5 |
| version not bumped | L1 |
| observe without the misalignment check | L5 |
| observe without row padding | L5 |
| observe without the blank-ID filter | L5 |
| observe comparing raw cells, not parsed fields | L5 |
| enforce reading the wrong range | L3, L6 |
| enforce logging the record count, not the row count | L3 |
| an untagged log line on the off path | L2 |

**Repo checks:**
- `compileall` clean.
- Tracker, workflow-mapping, manifest-pin, board-coverage, HIST-CLOSE, outcome-evidence, flag-mapping and P-180 suites: 164 passed, 1 skipped.
- The workflow audit and the blocking ci.yml lean list: see the PR.

**Adversarial review (before the PR).** A four-lens workflow ran: off identity, enforce semantics against the real log, evidence, and the harness. Each non-nit finding went to an independent refuter.
- **Off identity: no findings.**
- **Enforce semantics: one nit.** Observe compared raw cells, so a number typed into a text column could pass observe while enforce parses it differently. Observe now compares the parsed fields. A full-history check found no writer that ever wrote Performance_Log with USER_ENTERED, a formula, or percent or date strings: every write is RAW. gspread 6.2.1 forwards both options verbatim.
- **Evidence:** one minor finding and two nits, all corrected here:
  - the binding tolerance is `abs_tol=1e-8`, and the rounding acts on the interpolated 1W/2W target price, not on small ROIs;
  - the tracker step times;
  - the `precision_only` rule.
- **Harness:** four confirmed minor findings and four nits, all fixed here:
  - the enforce range is pinned;
  - trailing empty cells are trimmed;
  - any off-path log line fails;
  - L7 is release-scoped;
  - an observe guard is in the blocking lane;
  - the counter edges are covered;
  - the percent case's records are asserted;
  - the tracker environment is isolated.

## S5 - Operator steps (nothing is armed here)

1. Merge. No Render deploy is needed: the tracker runs in GitHub Actions.
2. Set the repository Variable `TRACK_LOG_UNFORMATTED_READ=observe`, then let one Performance Clock run complete (or dispatch it). Read the `[v6.50.0 LOG-READ] mode=observe` line. Require all of:
   - `divergent={}`;
   - `text_differs={}`;
   - `misaligned=0`;
   - `canonical_mismatch unformatted` below `formatted`.
3. Set `TRACK_LOG_UNFORMATTED_READ=enforce`. Both tracker lanes read the same Variable.
4. On the next runs, the `[CANONICAL-PERFORMANCE]` `schema_price_mismatch` count should stop growing for rows recorded after arming.

**Rollback:** unset the Variable, or set it to `off`.

## S6 - Not done here

- No repair of rows already rounded.
- No change to the canonical tolerance (`rel_tol=1e-9`, `abs_tol=1e-8`; a safety threshold).
- No change to the Signal_History loader (`SignalHistoryStore.load`), which has the same default read; follow-up candidate.
- No cron or `ci.yml` change.
