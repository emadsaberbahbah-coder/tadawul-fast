# TFB Commit Sheet — scripts/track_performance.py v6.44.0 [P-191 SIGNAL_HISTORY SAME-DAY REPEATS: TRACK_DEDUP_SIGNAL_HISTORY observe-only build] + tests/test_tp_dedup_signal_p191.py

Date: 2026-10-11 (Saturday) · Lane: GitHub/Python (both tracker steps: `track_performance.yml` run step and the `daily_sync.yml` tracker step) · WP-7 pass 1 (week 1) · Protocol: One-Pass · File status: script **REVISED** (full file) · harness **NEW** · Companion sheet: `TFB_Commit_Sheet_tracker_workflows_env_parity_2026-10-11.md` (the mapping of the three flags in both workflows, same commit)

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base commit | `a6668c235b526394158726a3e34c8ffaeb95b6b7` (branch `claude/pensive-hypatia-smnxqe`); last commit touching this file `a356ed8576f45944f6f0812e8bc37ae641e5d47f` (2026-10-09, #742). Build worktree was created three commits behind (`616f6060`) and reset to the base before any edit; `scripts/track_performance.py` is byte-identical between the two. |
| Base file | `scripts/track_performance.py` v6.43.0 — sha256 `6451afefedb0f1af798e3af1c15c20e3988dff7a277bcb6af9e83faaa1213818`, **10,290 lines**, 300 defs (module-level + one nesting level), 66 module-level names; embedded self-test `PASS 20/20` through the real `PerformanceTrackerApp._track_selftest_` |
| Delivered file | sha256 `71dbbbdb7e84724935cf7def93b79d648469e932354b8828146b315d6259952a`, **10,615 lines** (+327 / −2; the two "removed" lines are the constant bumps `SCRIPT_VERSION = "6.43.0"` → `"6.44.0"` and `total = 20` → `22`, both re-emitted with their prior comment text kept), `SCRIPT_VERSION = "6.44.0"` (SERVICE_VERSION follows), 304 defs, 67 module-level names · `py_compile` PASS · ruff `--select E9,F63,F7,F82` PASS (the ci.yml:298 gate) · 0 non-ASCII and 0 smart quotes in added lines · `python scripts/track_performance.py --help` rc 0 |
| Harness | `tests/test_tp_dedup_signal_p191.py` sha256 `3455cc5130f1355fe697e53cd0c9b040f3d238f47aa7175f06df1f57dc869264`, 443 lines; runs under pytest (`test_all`) or directly; ruff PASS |
| Export measured | workbook export of 2026-10-10 (GitHub run 38071484709), `Signal_History` tab read with openpyxl `read_only=True, data_only=True` into a scratchpad TSV (sha256 `bfc6cf5cdb51ee39d93d12165b28e9003f375edba7be987a2d7bd8745579c47e`, 29,472 lines incl. header) — **never committed**; identified by content: 29,471 snapshot rows, dates 2026-06-18..2026-10-10, last `[PERF-VERDICT v6.43.0] ... selftest=PASS 20/20` |

## S2 — Design (WHY, as written in the file header)
**Evidence.** Signal_History holds 29,471 rows but only 7,801 unique `Symbol|YYYYMMDD` Keys: 21,670 rows (73.5 %) are same-day repeats, 5,373 keys carry more than one row, max 19 rows per key (histogram 1:2428 2:1255 3:687 4:1122 5:929 6:308 ... 17:80 18:52 19:60). `Key == Symbol|YYYYMMDD` on 29,471/29,471 rows, so the key is sound and only the write-side guard is missing. On 10-10 the four `--record` runs (Riyadh 04:08 / 14:42 / 19:25 / 21:21) built 46 / 47 / 48 / 40 snapshots of which 0 / 46 / 43 / 36 repeated a key already on the sheet. `_Run_Log` carries 284 `[PERF-VERDICT]` lines, none with a dedup token.

**Root cause.** `SignalHistoryStore.append_snapshots` dedups only against `self.cache_keys`, which starts empty and is filled only by `load_snapshots`; `run_once` calls `record_signal_snapshots` BEFORE the only `load_snapshots` call (`_compute_signal_trends`), in a fresh Actions process every run — 4-5 runs per Riyadh day across `daily_sync.yml` (`17 4,12,20`) and `track_performance.yml` (`47 11` / `47 15`). The v6.19.0 WHY block already said the first-of-day idempotency "is not enforced on the live sheet"; the v6.7.0 contract text and the workflow headers repeated the claim (corrected in the companion sheet).

**Gate `TRACK_DEDUP_SIGNAL_HISTORY = off (DEFAULT) | observe | enforce`** (explicit words, case/space-insensitive; anything else incl. `1` → off):
- **off**: nothing new runs — no extra Sheets read, no log line, no verdict token, no Details key; `append_snapshots` untouched. **Byte-identical** to v6.43.0 on every Signal_History / Performance_Log value (proved in S4).
- **observe**: ONE bounded read of the Key column (`B2:B<end>`, clamped to the grid by the same three lines as `load_snapshots`) before the append; PURE measure `would_skip` (this run's snapshots whose Symbol|Date is already on the sheet) / `new` / `sheet_rows` / `sheet_keys` / `backlog = rows − keys` / `max_per_key` / 12 sample keys; one WARNING `[v6.44.0 DEDUP-SIGNAL] mode=observe would_skip=k/built new=n | sheet rows=R keys=K backlog=B max_per_key=x | sample=...`; `[PERF-VERDICT]` token ` | dedup_signal observe would_skip k/built backlog B` and a `dedup_signal` object in its Details JSON. **Values untouched**: every snapshot is still appended; `cache_keys` is never primed (priming it would silently be the first-of-day enforce).
- **enforce**: accepted by the reader but **MEASURE-ONLY in v6.44.0** — behaves as observe, logs a second WARNING `enforce requested but NOT built in v6.44.0 (policy last|first pending owner decision) - measuring only, values untouched`, and its token reads `dedup_signal enforce(measure-only) ...` with read-back `applied: False`. The apply step (last-of-day upsert vs first-of-day skip; open question Q1/Q7) and the one-off compaction of the ~21.7k backlog are the v6.45.0 follow-up.
- **Failed read is reported, never zeroed** (challenge fix #1): `load_key_column` returns `None`, read-back `read_ok: False`, log `key-column read FAILED - no measurement this run`, token ` | dedup_signal <mode> read-failed` (no numbers). The append proceeds.

**Additions (AST +4 defs, +1 module dict, 0 removed):** `_dedup_signal_mode`, `_dedup_signal_measure` (PURE; plain-dict counting — the module's `Counter` name is the prometheus metric class, so the spec's `Counter(...)` would have built a metric, not counted; challenge B1), `_dedup_signal_token` (PURE; the verdict suffix, called by the real `_append_runlog_verdict`; challenge fix #2), `SignalHistoryStore.load_key_column`, `_LAST_DEDUP_SIGNAL`. Embedded self-test 20 → 22 (case 21: reader vocabulary + the measure on the canned vector built 3 / would_skip 2 / new 1 / rows 5 / keys 3 / backlog 2 / max 3; case 22: edge cases, sample cap 12, `None` inputs, the three token wordings, `ws=None` → `[]` without I/O). The `total = 22` line keeps the v6.42.0 comment text (challenge fix #4). No import line changed.

**Findings surfaced, not acted on (register candidates):** (a) Analyst Rating is the literal string `'None'` on 15,072 of 29,471 Signal_History rows (`_safe_str(None)` → `'None'` in `_build_signal_snapshots`); a value change, so it needs its own gate. (b) "one row per symbol-day on write is safe for every reader" stays a hypothesis: `SignalTrendAnalyzer`'s intraday flip count and `audit_data_quality.py`'s flap meter consume intraday versions.

## S4 — Audits (REAL functions; the only stand-ins are an in-memory worksheet that records `get()` ranges and `append_rows()` calls, and a fake `_Run_Log` worksheet that records `append_row`)
| Check | Result |
|---|---|
| `py_compile` (venv311 3.11) | PASS |
| AST zero-removal (base vs delivered; module-level + one nesting level) | defs 300 → 304, **0 removed**, +4: `SignalHistoryStore.load_key_column`, `_dedup_signal_measure`, `_dedup_signal_mode`, `_dedup_signal_token`; module names 66 → 67, 0 removed, +1 `_LAST_DEDUP_SIGNAL` |
| ruff `E9,F63,F7,F82` on the script + both new tests | All checks passed |
| Embedded self-test (real `_track_selftest_`) | delivered **PASS 22/22**; base PASS 20/20 (both inside the harness, P1 / P7) |
| OFF byte-identity (standalone proof, base and delivered driven through the REAL `record_signal_snapshots` + `_append_runlog_verdict` on one 5-row fixture, gate unset) ×3 | appended-rows sha256 `030470134ccce2122f19c7a014f5c9ed62307313e818803b192a608f83706adf` on BOTH files; `get()` calls `[]` on both; Details JSON digest `8f542b25bd16bf71…` identical; verdict line identical apart from `v6.43.0`→`v6.44.0` and `selftest=PASS 20/20`→`22/22` — **True ×3** |
| Harness P1–P9, full mode (`TP_BASE`=v6.43.0 file + `TFB_TEST_SIGNAL_TSV`=export TSV): `python tests/test_tp_dedup_signal_p191.py` ×3 (venv311) ×3 (venv_full) and `python -m pytest -q -s` ×3 (venv311) ×3 (venv_full) | **37 PASS / 0 FAIL, digest `f337ff396774b601` ×12** |
| Harness CI mode (no base, no TSV; P7/P8 SKIP as CI will) ×3 pytest/venv311, ×3 direct/venv_full | **27 PASS / 0 FAIL, digest `9d661be8a1b18096` ×6** |
| Harness against the BASE file (`TP_FILE=<v6.43.0>`) | **fails as required**: `AssertionError: 6.43.0` at the version floor (direct rc 1; pytest "1 error during collection") |
| P2 OFF through the real wiring | 3 rows appended, `ws.gets == []`, no `[DEDUP-SIGNAL]` line, read-back dict == default; masked appended-rows digest `030470134ccce212` |
| P3 OBSERVE | exactly one `get("B2:B19")` (7 fixture rows → `row_count` 19; challenge B3), rows == OFF rows (Snapshot ID / Recorded At masked), read-back `built 3 / would_skip 2 / new 1 / sheet_rows 6 / sheet_keys 3 / backlog 3 / max_per_key 3 / keys [AAA.US\|today, BBB.US\|today]`, one WARNING with the counts |
| P4 ENFORCE | same rows and counts; `mode`/`requested` `enforce`, `applied False`; "NOT built" warning present |
| P5 verdict | real `_dedup_signal_token`: ` \| dedup_signal observe would_skip 2/3 backlog 3`, ` \| dedup_signal enforce(measure-only) would_skip 2/3 backlog 3`, `''` for off; real `_append_runlog_verdict`: OFF line has no token / no JSON key; OBSERVE line ends with the token and Details JSON carries `dedup_signal` minus `keys`; `selftest=PASS 22/22` |
| P6 fail-open | `get()` raises → 3 rows still appended; `read_ok False`, `built 3`, zeros not claimed; token ` \| dedup_signal observe read-failed`; both failure lines logged |
| P7 dual-tree | OFF rows == base rows; env `observe` inert on base (no read, same rows, no line); base lacks the four additions; OFF verdict == base verdict modulo version tag / selftest count |
| P8 real export (session only) | `load_key_column` → one `get("B2:B29483")`, **sheet_rows 29,471 / sheet_keys 7,801 / backlog 21,670 / max_per_key 19**; replay of the 10-10 21:21 run against the rows recorded before it → **built 40 / would_skip 36 / new 4** (against all rows incl. its own: 40/40; challenge fix #3) |
| Existing tracker suites on the delivered file (`test_tp_dedup_matured_p189b`, `test_tp_target_unit_sentry_p158`, `test_track_force_coverage_p180`, `test_tp_zero_baseline_p201`) | 11 passed (venv311), 15 passed with the two new suites (venv_full) |
| `tests/test_verify_manifest_pins.py` | **RED by design until the integrator pins**: `manifest 6.43.0 != source 6.44.0` (the file is outside this WP's FILES; exact line in S6) |
| Privacy grep (digits near SAR/cash/NAV/qty/shares/cost) over every changed/new file and both sheets | 0 hits; fixtures carry market symbol rows only |

Harness digest note: the digest covers the PASS lines (with today's date normalised) + the real-export counts + the masked OFF appended-rows digest; it is identical under pytest and direct execution because the log tap records WARNING+ only (pytest leaves the module logger at WARNING; a direct run also emits an INFO line). The two documented digests differ only by the SKIP lines of P7/P8.

### Measurement from the 10-10 export (WP-0 numbers, S4 table)
| Quantity | Value |
|---|---|
| Signal_History rows / unique Symbol\|Date keys / same-day repeat rows | 29,471 / 7,801 / 21,670 (73.5 %) |
| keys with > 1 row / max rows per key | 5,373 / 19 |
| Key == Symbol\|YYYYMMDD | 29,471 / 29,471 |
| 10-10 runs (04:08 / 14:42 / 19:25 / 21:21) built → repeats | 46/47/48/40 → 0/46/43/36 |
| `_Run_Log` `[PERF-VERDICT]` lines / with `dedup_matured` / with `dedup_signal` | 284 / 0 / 0 |
| Analyst Rating literal `'None'` rows (finding a) | 15,072 |

## S5 — Delivery
| File | Status |
|---|---|
| `scripts/track_performance.py` v6.44.0 | full file (this sheet) |
| `tests/test_tp_dedup_signal_p191.py` | NEW |
| `.github/workflows/track_performance.yml`, `.github/workflows/daily_sync.yml`, `tests/test_track_flags_mapped.py` | same commit — see the companion sheet |
| `docs/evidence/TFB_Commit_Sheet_track_performance_v6.44.0_2026-10-11.md` | NEW (this sheet) |
| NOT touched (outside this WP's files; integrator step) | `scripts/verify_deployment.py` (pin), `.github/workflows/ci.yml` (enumeration + comment index) |

## S6 — Operator / integrator steps (one action each)
1. **Integrator — manifest pin** (`tests/test_verify_manifest_pins.py` is red until this lands): `scripts/verify_deployment.py` line 136 → `    ("track_performance", "6.44.0", "track performance"),` and line 81 → prepend to the SCRIPT_VERSION comment: `SCRIPT_VERSION = "1.0.45"  # v1.0.45 (2026-10-11) - MANIFEST PIN ONLY: track_performance 6.43.0->6.44.0 (P-191 Signal_History same-day repeat measurement behind TRACK_DEDUP_SIGNAL_HISTORY, default off; AST inventory 0 removed, +4 added); no verifier logic changed.  # v1.0.32 ...` (rest of the comment verbatim).
2. **Integrator — ci.yml** (base `a6668c23`): after line 411 (`            tests/test_tp_dedup_matured_p189b.py \`) insert `            tests/test_tp_dedup_signal_p191.py \` and `            tests/test_track_flags_mapped.py \`; in the comment index after line 89 (`#     tests/test_tp_zero_baseline_p201.py       (P-201 zero-forecast baseline)`) insert `#     tests/test_tp_dedup_signal_p191.py       (P-191 Signal_History same-day repeats)` and `#     tests/test_track_flags_mapped.py         (tracker flag env mapping + parity)`.
3. **Commit** (no ENV change). Read-back on the next scheduled tracker run in either lane: `_Run_Log` `[PERF-VERDICT v6.44.0] ... selftest=PASS 22/22 | ...` with **no** `dedup_signal` token; Signal_History still grows by every snapshot. This is the OFF cycle.
4. **Arming (separate sitting)**: set the repo Variable `TRACK_DEDUP_SIGNAL_HISTORY=observe` (both lanes read it). Read-back on the next run: job log `[v6.44.0 DEDUP-SIGNAL] mode=observe would_skip=0/~46 ...` on the first run of a Riyadh day and `would_skip=~36-46/~40-48` on later runs, with `sheet rows=29,5xx keys=7,8xx backlog=21,7xx max_per_key=19`; `_Run_Log` token ` | dedup_signal observe would_skip k/built backlog 217xx` and a `dedup_signal` object in the Details JSON. Values unchanged by design; `tfb_export_audit` P-191 stays WARN and DQ-61 stays FAIL until the v6.45.0 enforce + compaction.
5. Optional, same sitting: `TRACK_DEDUP_MATURED=observe` → `[v6.41.0 DEDUP-MATURED] mode=observe would_expire=27 (W 20 / L 7 / BE 0)` and ` | dedup_matured observe 27`. Do **not** arm `TRACK_RISK_BLANK=1` until `tfb_export_audit.py` learns blank ≠ zero (its risk check would still FAIL and the arming would look inert).
6. **Rollback**: unset the Variable (off is the default) — the next run is v6.43.0-identical on every value; or restore v6.43.0 of this file (the harness floor then refuses to run, as intended).

## Known limits (stated, not hidden)
- `enforce` is **measure-only** in v6.44.0; the word is accepted so an operator who asks for it still gets the measurement, and every surface says `(measure-only)` / `applied: False`. Q7 (map `enforce` → `observe` instead) is an owner decision; the read-back carries `requested` either way.
- This pass closes **no** DQ by value: DQ-61 and export_audit P-191 stay red until v6.45.0 (enforce + one-off compaction, expected keep 7,801 / archive 21,670 on the 10-10 shape).
- Observe adds one Sheets GET of the Key column (~29.5k cells) per `--record` run in both lanes; fail-open and bounded by the `load_snapshots` clamp.
- The `_LAST_DEDUP_SIGNAL` dict is module state: a fresh Actions process per run, so a stale value cannot leak between runs; the harness resets it per leg.
- The 'None' Analyst Rating artefact (15,072 rows) is reported, not fixed (value change → own gate).
- P8 needs the export TSV (scratchpad, never committed); CI runs P1–P6 and P9 and skips P7/P8 with an explicit SKIP line.
- The ci.yml wiring test (`test_ci_runs_both_new_suites` in the spec) is not delivered: ci.yml is integrator-owned under this WP's house rules, and a test that skips while unwired would not guard anything; the two lines are in step 2 above.
