# TFB Commit Sheet - track_performance 6.59.0 [CALIB-CALL-SPLIT]: the calibration's call mix, like for like

| Item | Value |
| --- | --- |
| Date | 2026-11-07 (daily improvement routine) |
| Branch | `claude/daily-2026-11-07` |
| Base | origin/main `a85edfc6` (track_performance v6.53.0, track_performance.yml 1.0.7, ci.yml 1.1.61) |
| Lanes | GitHub Actions only (track_performance.yml 'Run performance clock' and daily_sync.yml's tracker step, both `--analyze`). No Render deploy, no Apps Script paste |
| Gate | `TRACK_CALIB_CALL_SPLIT` = on / 1 / true / yes (repository Variable); default off = v6.53.0 byte for byte (the version strings aside) |
| Priority | (a) a published outcome reading is wrong or unreadable: the calibration's "the gate is inverted" warning and the BLOCKED row compare numbers whose conventions and call mixes differ, and the owner's next decision (R1 in the 6 Nov audit) depends on them |
| Versions | track_performance 6.59.0 (6.54.0-6.58.0 are claimed by the open PRs #845, #857, #915, #922 and #923). track_performance.yml 1.0.13 (1.0.8-1.0.12 claimed). ci.yml 1.1.71 (1.1.62-1.1.70 claimed). Whichever lands later re-numbers |

## S1 - Evidence

**The published rows.** Performance Clock run 42352240437 (schedule, 2026-11-07 07:40Z, main `a85edfc6`), 'RELIABILITY CALIBRATION', 'By investability bucket':

```
BLOCKED        n=770 decided=204 win= 76.5% CI[70,82] claim~  55% roi= -2.14% factor*1.36
INVESTABLE     n=1249 decided=1246 win= 48.8% CI[46,52] claim~  71% roi= -0.31% factor*0.69
NONE           n=30  decided=14  win= 35.7% CI[16,61] claim~   0% roi= +6.36% factor*1.00
WATCHLIST      n=10978 decided=2410 win= 55.9% CI[54,58] claim~  61% roi= +0.37% factor*0.92
WARNING: WATCHLIST outperforms INVESTABLE in this sample -- the gate is inverted here; investigate before trusting the split.
```

The same run published `TFB_CALIBRATION_FACTORS=BLOCKED:1.357,INVESTABLE:0.688,WATCHLIST:0.919`.

**Why the rows cannot be read as printed.**
- `win` is the outcome of the CALL (v6.46.0 [DIR-OUTCOME]): a buy-family call wins when the price rose, a sell-family call (REDUCE / SELL / STRONG SELL / AVOID, EXIT and TRIM) when it fell, and HOLD is neutral, so it is not in `decided`.
- `roi` is the PRICE return averaged over every matured record of the bucket, HOLD included (`ReliabilityCalibrator._bucket`: `mean_roi` over all records with a ROI). v6.46.0 kept it a price aggregate on purpose.
- So a bucket of sell calls that were right prints a high win rate next to a negative roi. That is the BLOCKED row's shape (R11 in `docs/reviews/comprehensive-audit-20261106.md` on #924: "Investigate BLOCKED bucket: 76.5 % wins with -2.14 % mean ROI").
- The warning compares pooled win rates. INVESTABLE is decided on 1,246 of 1,249 records: practically every call is directional. WATCHLIST is decided on 2,410 of 10,978 (78% not decided: HOLD or breakeven). Neither bucket's buy / sell split is printed anywhere.
- The engine's verdict chain (`core/data_engine_v2.py` around line 8472) sends every sell-family call to WATCHLIST / DO_NOT_INVEST. INVESTABLE is the buy calls that clear price, forecast, data quality, fundamentals and a non-negative forecast; the other buy calls are WATCHLIST. So the like-for-like test of the gate is INVESTABLE buys against WATCHLIST buys. A WATCHLIST win rate carried by sell calls in a falling sample raises the same warning with the gate working.
- The audit makes the inversion a blocker before any promotion (R1). The report cannot currently tell the two cases apart.

The harness fixture `fixture_artifact` reproduces the shape on synthetic records:
- INVESTABLE and WATCHLIST buys are both 50% right, and WATCHLIST also holds 200 sells that are 80% right;
- the untouched main copy prints the pooled "gate is inverted" warning (O1, X1);
- BLOCKED reads `win=77.0%` with `roi=-1.39%`.

**Dedupe.** No open PR prints a call-mix split or changes the calibration report. All open PRs were checked, Codex branches included:
- #924 lists R11 / R1 as backlog only;
- #845 / #857 change the audit's maturity counting and tally, not the report.

**Survey of the last 24 h (scheduled Actions on main).**
- daily_sync 42215928918 (6 Nov 12:12Z) failed at core-pages with `Portfolio response contract failed: foreign, blank or duplicate holding identity`. The diagnosis:
  - same code `a85edfc6` in all four runs; only the `_Portfolio_CostBasis` active-holding count changed (8 -> 6 -> 5 -> 5);
  - the next three scheduled runs (19:32Z, 00:08Z, 11:10Z) passed, with My_Portfolio `rows=5`;
  - so it was a ledger edit caught by the identity contract, not a code defect, and nothing to build.
- track_performance is green. Its audit priced 434 of 1,139 due symbols: the Yahoo fallback was rate-limited (429 x16, then 689 skipped).
  - `[HIST-CLOSE v6.47.0] mode=observe` would mature from archive bars (`witnessed=1735`). That is an owner arming step (`TRACK_HIST_TARGET_CLOSE`), not a build.
- `Invalid matured excluded: 77` equals `matured-fresh=77` in the same run: the same-run counting gap that open #845 fixes.
- name_label_renewal (#925), news_archive and provider_target are known operator items.

## S2 - Change

**`scripts/track_performance.py` 6.59.0** (title, WHY block before `SCRIPT_VERSION`, version):
- `_calib_call_split_enabled()` reads `TRACK_CALIB_CALL_SPLIT`: 1 / on / true / yes is on, anything else off.
- `_calib_call_split(cal, inv_groups)` takes each investability bucket's matured records and splits them by the entry call's sign (`_reco_trade_sign`: +1 buy family, -1 sell family, 0 HOLD or unknown). For each bucket it records:
  - `buy` / `sell`: each family scored by the calibrator's own `_bucket` (decided, wins, Wilson CI, min_sample sufficiency), with its distinct-symbol count;
  - `hold`: the rest;
  - `call_roi`: the call-signed edge over the buy and sell records with a finite ROI (a buy's price return, a sell's move avoided = price ROI x -1);
  - `price_roi`: the plain mean over the same records. A rounded -0.0 is normalised to 0.0.

  `like_for_like` holds the calibrator's own `_discrimination` note, recomputed on the INVESTABLE and WATCHLIST records of the buy family alone and of the sell family alone. When either side holds no call of the family it reads `n/a: <bucket> holds no <family> calls in this sample (INVESTABLE n=a, WATCHLIST n=b)`. On real data the sell line reads n/a, because the engine routes sell calls to WATCHLIST. An error logs one WARNING (`[v6.59.0 CALIB-CALL-SPLIT] call split unavailable: <class>`) and returns `{}`.
- `_fmt_call_family` and `_calib_call_split_lines` render the block in `_fmt_cal_bucket`'s notation:
  - a six-line header names the two conventions and the overlap caveat;
  - one row per bucket follows, then the two like-for-like lines;
  - a family with records but nothing decided prints `decided=0`;
  - an empty or malformed split prints one `unavailable` line;
  - no split (gate off: no attribute) prints nothing.
- `ReliabilityCalibrator.calibrate` builds the report into `_rep`. When the gate is on it sets `_rep.call_split` inside a try (an error: one WARNING and `{}`). `call_split` is an instance attribute rather than a dataclass field, so `to_dict()` and the `_calibration.json` export do not change even when on. It returns `_rep`.
- `render_calibration_report` prints `_calib_call_split_lines(getattr(rep, "call_split", None))` right after the discrimination note.
- Definitions: 4 functions added, 0 removed (at any depth, classes included). One line was replaced besides the version lines (`return CalibrationReport(` -> `_rep = CalibrationReport(`); the calibrate guard and the render hook are insertions.

**Workflows** (ENV PARITY; not armed):
- `track_performance.yml` 1.0.13: a header block plus `TRACK_CALIB_CALL_SPLIT: ${{ vars.TRACK_CALIB_CALL_SPLIT || 'off' }}` in 'Run performance clock', after `TRACK_DEDUP_SIGNAL_HISTORY`. No open PR edits those lines.
- `daily_sync.yml`: the same mapping in the tracker step, at the same anchor.
- No cron, concurrency or other step changes.

**CI:** `ci.yml` 1.1.71 adds its own blocking lean-unit step after the board scorecard harness. It fetches main `a85edfc6` by SHA and runs `tests/test_calib_call_split.py` with `TFB_TEST_REQUIRE_BASE=1`, so the base-copy legs FAIL rather than skip when the base is missing. The lean venv (numpy, pytest, PyYAML) imports the tracker.

**Verifier:** `scripts/verify_deployment.py` pins track_performance 6.59.0, with a dated comment.

**Archive-bar test:** in `tests/test_track_performance_v6530_archive_bar.py`:
- the exact 6.53.0 pins become floors, the same change as #922 / #923 with its own comment;
- its removed-line cap (<= 16) now applies to 6.53.0 itself only. This branch already uses all 16 lines, and a merge with #923 would make it 17.

**Rollback:** unset `TRACK_CALIB_CALL_SPLIT` (or set it off).

## S3 - Measured

**`tests/test_calib_call_split.py`: PASS 37/37**:
- script runner and pytest (tfb-venv);
- lean venv with `TFB_TEST_REQUIRE_BASE=1`;
- each of five trial merges (main + #845 / #857 / #915 / #922 / #923 + this change, conflicts resolved as "this lands last"). The archive-bar n2 / w1 legs pass on the same merges.

In a copy without git history: 34 passed, 3 skipped, and with `TFB_TEST_REQUIRE_BASE=1` those 3 fail. CI fetches the base, so they run there.

| Case | What it checks |
| --- | --- |
| G1 | unset and 15 off words (incl. `enable`, `observe`, `enforce`, `onn`) read off; 6 on spellings read on |
| O1 | off (4 spellings): no `call_split` attribute; the rendered report (plain and verbose) equals the embedded v6.53.0 golden; no log record at any level and no stderr; to_dict, both renders, both fixtures, log records, stderr and `_calibration_env_string` equal the untouched main copy |
| O2 | on: to_dict and `TFB_CALIBRATION_FACTORS` unchanged, `call_split` not in to_dict, every bucket's dict unchanged, no log record; the report is the off report with the block inserted right after the discrimination note; block lines ASCII |
| N1 | the call-mix artifact. Pooled: "gate is inverted". WATCHLIST: buys 50% (100 records, 20 symbols), sells 80% (200 records, 28 symbols), hold 300, call roi +1.20 / price roi -1.20. Like-for-like: the buy line "not yet statistically distinguishable", the sell line "n/a: INVESTABLE holds no sell calls ...". Exact rendered rows |
| N2 | BLOCKED (the R11 shape): pooled win 77.0% / roi -1.39%; buys 50% (10), sells 80% (90, 18 symbols), hold 200; call roi +2.16 / price roi -2.16 |
| N3 | a real inversion inside buy calls (40% vs 70%) stays a WARNING in the like-for-like buy line |
| N4 | STRONG_SELL / AVOID as sells, ACCUMULATE as a buy, an UNSPECIFIED bucket, a thin bucket "(thin)", EXIT / TRIM text as sells and an unknown word or blank as hold, a breakeven-only family `decided=0`, a rounded zero `+0.00`, HOLD-only `call roi=n/a`, both-sides-absent `n/a` lines, the empty report, both sides present (the calibrator's own note) |
| R1 | None / NaN / inf / text ROIs; a raising calibrator -> `{}` and one WARNING; junk -> no lines; malformed splits -> one `unavailable` line; a split that raises inside `calibrate()` -> one WARNING, `call_split {}`, the off report plus one `unavailable` line |
| W1 | both tracker steps map the Variable once with default 'off' (YAML parsed: the clock step by name, the daily_sync step that runs the tracker); the 1.0.13 header present and above 1.0.7; the reader's fallback is "off" |
| V1 | version floor >= 6.59.0, title, WHY order (6.53.0 < 6.59.0 < `SCRIPT_VERSION`), verifier pin, the harness file itself ASCII. ci.yml parsed: exactly one lean-unit step runs the harness, with no `if`, no `continue-on-error`, `TFB_TEST_REQUIRE_BASE=1`, the base SHA fetched, no `|| true`, and the pytest line last |
| V2 | against main `a85edfc6`: the title, version and `return CalibrationReport(` lines replaced; no main WHY line removed; added lines ASCII; no definition removed. Exactly those removals and exactly the 4 helpers when this release stands alone; the subset form after merges |
| X1 | the main copy has no split and prints the pooled warning only |

**Golden negative** (`TFB_TEST_CALL_SPLIT_TARGET` = the untouched main copy): 31 failed, 6 passed. The passes are the off legs (O1 x5) and X1, which describe main itself.

**35 / 35 mutants killed** (failing tests in brackets):
- **gate:** default on (7), any word on (11), `enable` read as on (3), split not attached (6), gate bypassed (7), call-site guard dropped (1);
- **render hook:** hook dropped (2), hook before the note (2);
- **numbers:**
  - signs: call roi unsigned (3), price roi signed (2), families swapped (5), an unknown call read as a buy (3);
  - counts: symbols counted as records (2), hold count wrong (3);
  - like-for-like: computed from the pooled buckets (1), families swapped (3), n/a rule dropped (3), labels swapped (1);
  - errors: an exception escaping (1), the WARNING dropped (1), the `unavailable` line dropped (1);
  - shape: `call_split` made a dataclass field (7), thin rule dropped (1), `decided=0` shape dropped (1), -0.0 kept (1), roi rounded to 1 decimal (1), buckets unsorted (2), header omitted (1);
  - a DEBUG log line added to `calibrate()` on the off path (7);
- **lanes:** daily_sync unmapped (1), clock default on (1), the CI pytest line replaced (1), base not required (1), base fetch dropped (1), step made `continue-on-error` (1).

**Other checks:**
- compileall (main.py core routes scripts tests) clean.
- ruff (CI rules E9,F63,F7,F82, whole tree) clean, and F401 / F841 clean on the harness.
- Related suites, 22 files, the harness included: 546 passed, 1 skipped:
  - workflow flag mappings, verify pins, rollout flags, workflow audit and audit repairs, activation and rollout evidence;
  - harness IC, learning metrics and guards, summary integrity, calibration report, shadow calibration;
  - tracker flags, record-only workflows, hist-close, log read, board and force coverage, canonical publications, the archive-bar test.
- LEAN and contract-lane emulations on an export of this head: see S5.

## S4 - Off identity

With the gate unset, `off` or any other word:
- `calibrate()` returns the same object it built, without the attribute;
- `render_calibration_report` prints the v6.53.0 report line for line (O1 against the embedded golden and against the main copy);
- the export, `_calibration_env_string`, `_S1_Calibration` rows and the `_Run_Log` line read only dataclass fields, so they are unchanged;
- no log record is added on either setting.

With the gate on, only the stdout block is added (O2).

## S5 - Review and lanes

**Round 1** (independent four lenses: semantics, off identity and house rules, harness, workflow / CI / merge integration; each lens's findings re-checked by its own refuter). No off-identity break and no wrong number was found. All findings were fixed in `5631947`:

| Finding | Verdict | Fix |
| --- | --- | --- |
| W1 pinned the clock header to line 3; the next header bump (or any open PR's) would turn the blocking lane red (major, three lenses) | CONFIRMED | presence + order (1.0.13 above 1.0.7) |
| V2's exact removed-line / helper checks fail after a merge with any open tracker PR; the archive-bar cap <= 16 fails after a merge with #923 | CONFIRMED | exact checks only for this release alone, subset form otherwise; the archive-bar cap applies to 6.53.0 only; trial merges pass |
| a literal en dash in an added harness line | CONFIRMED | `\u2013` |
| the sell-calls like-for-like note always read "insufficient sample" (the engine routes sell calls to WATCHLIST) | CONFIRMED | `n/a: ... holds no sell calls ...` |
| Wilson CIs over overlapping daily records are optimistic; recording date not controlled | CONFIRMED | per-family symbol counts, a header caveat, KNOWN LIMITS |
| base-copy legs always skipped in CI; the runner counted skips as passes | CONFIRMED / PLAUSIBLE | own lean step fetching `a85edfc6` with `TFB_TEST_REQUIRE_BASE=1`; skips no longer counted |
| no log / stderr pin on the off path | CONFIRMED | O1 / O2 capture every log record and stderr (a DEBUG-line mutant is killed) |
| R1's last leg used a non-raising stub | PLAUSIBLE | a raising split, plus a try at the call site |
| a failing split printed nothing with the gate on | PLAUSIBLE | one WARNING and one `unavailable` line |
| breakeven-only family printed like an empty one | CONFIRMED | `decided=0` |
| `-0.00%` next to `+0.00%` | PLAUSIBLE | normalised |
| "what following the calls returned"; "78% HOLD"; "a header line"; the `_calib_call_split_lines` docstring; no dated comment on the verifier pin; dead helpers; X1 named a golden negative | CONFIRMED / PLAUSIBLE (nits) | reworded / added / removed / renamed |

**Round 2** (a fix-by-fix check and a fresh regression hunt, each re-checked by its own refuter).

Every round-1 fix held, except one:
- **(major, CONFIRMED)** the harness title template still had a literal en dash; the editor had turned the escape back into the character. Fixed in `24e3a04` with the `\u2013` escape, and V1 now asserts the harness file itself is ASCII. The new CI step name was made ASCII as well.

Smaller fixes (CONFIRMED, fixed):
- **(minor)** this sheet's owner guidance read every non-WARNING like-for-like outcome as a call-mix artifact. S6 now separates the four outcomes.
- **(nits)**
  - V2's regex for later releases;
  - the replaced-line count in S2;
  - the archive-bar test's N2 docstring.

The regression hunt drove `PerformanceTrackerApp.run_once --analyze --calibrate --export --verbose --format all` over a fake store and sheet:
- 16,000 records over 1W / 2W / 1M / 3M, with every bucket spelling, ACTIVE and MATURED records, duplicates, and NaN and zero ROIs;
- timestamps were frozen;
- with the gate unset, off, `enable` or `observe`, this head and the main copy match on stdout, stderr, every log record, the `_calibration.json` / CSV / HTML exports and every sheet operation (`_Status`, `_S1_Calibration` A12:D18, the `_Run_Log` append).

**Lanes on an export of the head:**
- LEAN FAILS 0 on `e573b10` and again on `5631947`; on `5631947` the new step ran with the base fetched: 37 passed;
- the contract-lane emulation rc 0 on both.

## S6 - Owner steps (proposed; nothing is armed here)

1. Merge. No Render deploy: the tracker runs in GitHub Actions.
2. Set the repository Variable `TRACK_CALIB_CALL_SPLIT=on`. It adds report lines only, so it is safe at once.
3. On the next clock run, read the 'Call mix' block under 'By investability bucket':
   - **the like-for-like buy line answers R1, in one of four ways:**
     - `WARNING: WATCHLIST outperforms INVESTABLE`: the inversion holds within buy calls, so the investability gate is what to investigate;
     - `INVESTABLE significantly outperforms WATCHLIST`: the pooled warning was the call mix;
     - `not yet statistically distinguishable` or `insufficient sample`: not settled. The buy-only subsets are smaller than the pooled buckets, so their intervals are wider. Compare the buy-only point estimates, the CI widths and the `sym` counts, and wait for more buy records before calling the pooled warning an artifact.

     Read every interval against the `sym` counts: overlapping daily records make them optimistic;
   - **the BLOCKED row answers R11:** its sell calls' win rate, and its call roi next to its price roi.

Rollback: unset the Variable.

## S7 - Known limits and not done

- The like-for-like note keeps `_discrimination`'s wording and its rule (non-overlapping Wilson intervals at min_sample).
  - That rule counts every matured record as an independent trial. A symbol is recorded daily per horizon, so its records overlap and the intervals are optimistic; the `sym` count is the effective sample to read them by.
  - It holds the call family fixed, not the recording date (market regime).
  - It does not measure returns net of costs or against a benchmark (R1's other asks).
- The split is printed to stdout and the job log only. The `_calibration.json` export, the summary panel and the `_Run_Log` line do not carry it (on purpose: those surfaces stay byte-identical).
- Records whose entry call does not parse count as hold, as `_reco_trade_sign` has them.
- No change to factors, `TFB_CALIBRATION_FACTORS`, the pooled warning itself, the maturation rules, the cron or the concurrency.
- No live workbook read or write and no provider call: everything is offline.

## Addendum 2026-11-08 - merge of main `4b478e7`

Main `4b478e7` (#935-#937) was merged in with a merge commit. The ci.yml header conflicted: main took 1.1.68-1.1.70.

- ci.yml: this release keeps 1.1.71 (still above main).
- The header's claim note lists the open claims after the re-numbering: 1.1.71 #926, 1.1.72 #938, 1.1.73 #915, 1.1.74 #922, 1.1.75 #923, 1.1.76 #909.
- Main's 1.1.70 / 1.1.69 / 1.1.68 blocks follow verbatim. Against main, the ci.yml diff adds lines only.
- After the merge: `tests/test_calib_call_split.py` 37 passed with `TFB_TEST_REQUIRE_BASE=1`. The verifier pins, workflow flag mappings and tracker flags suites pass (49 passed). compileall and ruff (CI rules) are clean, and no conflict marker remains.

## Addendum 2026-11-09 - merge of main `86edd72`

Main `86edd72` (#939-#945) was merged in with a merge commit. Main took ci.yml 1.1.71-1.1.73, and its 1.1.71 raises the advisory whole-suite job's cap from 15 to 25 minutes.

- ci.yml: this release is re-numbered **1.1.71 -> 1.1.77** (its header, its `v1.1.77` line and its step comment).
- The claim note now reads: 1.1.74 #922, 1.1.75 #923, 1.1.76 #909, 1.1.77 #926, 1.1.78 #938, 1.1.79 #915. Only the three claims main took moved.
- Main's 1.1.73 / 1.1.72 / 1.1.71 blocks follow verbatim. Against main, the ci.yml diff adds lines only. No other file conflicted.

## Addendum 2026-11-10 - merge of main `e008a514`

Main `e008a514` (#947-#952) was merged in with a merge commit. Main took ci.yml 1.1.74, 1.1.76 and 1.1.77; it now holds every number up to 1.1.77 except 1.1.75.

- ci.yml: this release is re-numbered **1.1.77 -> 1.1.84** (its header, its `v1.1.84` line and its step comment).
- The claim note now reads: 1.1.78 #938, 1.1.79 #915, 1.1.80 #946, 1.1.81 #922, 1.1.82 #923, 1.1.83 #909, 1.1.84 #926. Every open claim sits above main's newest block: the three numbers main took moved, and so did #923's 1.1.75, which would otherwise sit below main's 1.1.76 and 1.1.77.
- Main's 1.1.77 / 1.1.76 / 1.1.74 blocks follow verbatim. Against main, the ci.yml diff adds lines only.
- No other file conflicted.
