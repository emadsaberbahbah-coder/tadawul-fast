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
- The warning compares pooled win rates. INVESTABLE is decided on 1,246 of 1,249 records: practically every call is directional. WATCHLIST is decided on 2,410 of 10,978 (78% HOLD). Neither bucket's buy / sell split is printed anywhere. A WATCHLIST win rate carried by sell calls in a falling sample raises the same warning with the gate working.
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
  - `buy` / `sell`: each family scored by the calibrator's own `_bucket` (decided, wins, Wilson CI, min_sample sufficiency);
  - `hold`: the rest;
  - `call_roi`: the mean of price ROI x call sign over the buy and sell records with a finite ROI;
  - `price_roi`: the plain mean over the same records.

  `like_for_like` holds the calibrator's own `_discrimination` note, recomputed on the INVESTABLE and WATCHLIST records of the buy family alone and of the sell family alone. An error returns `{}`.
- `_fmt_call_family` and `_calib_call_split_lines` render the block in `_fmt_cal_bucket`'s notation. A header names the two conventions, then one row per bucket, then the two like-for-like lines. Anything that is not a split renders as no lines.
- `ReliabilityCalibrator.calibrate` builds the report into `_rep`. When the gate is on it sets `_rep.call_split`, an instance attribute rather than a dataclass field, so `to_dict()` and the `_calibration.json` export do not change even when on. It returns `_rep`.
- `render_calibration_report` prints `_calib_call_split_lines(getattr(rep, "call_split", None))` right after the discrimination note.
- Definitions: 4 functions added, 0 removed (at any depth, classes included). Two lines were replaced besides the version lines: `return CalibrationReport(` -> `_rep = CalibrationReport(`, and the hook.

**Workflows** (ENV PARITY; not armed):
- `track_performance.yml` 1.0.13: a header block plus `TRACK_CALIB_CALL_SPLIT: ${{ vars.TRACK_CALIB_CALL_SPLIT || 'off' }}` in 'Run performance clock', after `TRACK_DEDUP_SIGNAL_HISTORY`. No open PR edits those lines.
- `daily_sync.yml`: the same mapping in the tracker step, at the same anchor.
- No cron, concurrency or other step changes.

**CI:** `ci.yml` 1.1.71 adds `tests/test_calib_call_split.py` to the blocking lean-unit list, after `tests/test_performance_summary_integrity.py`. The lean venv (numpy, pytest, PyYAML) imports the tracker.

**Verifier:** `scripts/verify_deployment.py` pins track_performance 6.59.0.

**Archive-bar test:** `tests/test_track_performance_v6530_archive_bar.py`'s exact 6.53.0 pins become floors. This is the same change as #922 / #923, with its own comment.

**Rollback:** unset `TRACK_CALIB_CALL_SPLIT` (or set it off).

## S3 - Measured

**`tests/test_calib_call_split.py`: PASS 37/37** (script), 37 passed (pytest, tfb-venv), 37 passed (lean venv).

| Case | What it checks |
| --- | --- |
| G1 | unset and 15 off words (incl. `enable`, `observe`, `enforce`, `onn`) read off; 6 on spellings read on |
| O1 | off (4 spellings): no `call_split` attribute; the rendered report equals the embedded v6.53.0 golden; to_dict, both fixtures' renders and `_calibration_env_string` equal the untouched main copy |
| O2 | on: to_dict and `TFB_CALIBRATION_FACTORS` unchanged, `call_split` not in to_dict, every bucket's dict unchanged; the rendered report is the off report with the block inserted right after the discrimination note; block lines ASCII |
| N1 | the call-mix artifact: pooled "gate is inverted"; WATCHLIST buys 50% (100), sells 80% (200), hold 300, call roi +1.20 / price roi -1.20; like-for-like buys "not yet statistically distinguishable"; sells "insufficient sample (INVESTABLE decided=0, ...)" ; exact rendered rows |
| N2 | BLOCKED (the R11 shape): pooled win 77.0% / roi -1.39%; buys 50% (10), sells 80% (90), hold 200; call roi +2.16 / price roi -2.16 |
| N3 | a real inversion inside buy calls (40% vs 70%) stays a WARNING in the like-for-like buy line |
| N4 | STRONG_SELL / AVOID as sells, ACCUMULATE as a buy, an UNSPECIFIED bucket, a thin bucket "(thin)", EXIT / TRIM text as sells and an unknown word as hold, HOLD-only "call roi=n/a", the empty report |
| R1 | None / NaN / inf / text ROIs, a raising calibrator -> `{}`, junk input -> no lines, a failing split inside calibrate prints the off report |
| W1 | both tracker steps map the Variable once with default 'off' (YAML parsed: the clock step by name, the daily_sync step that runs the tracker); header 1.0.13; the reader's fallback is "off" |
| V1 | version floor >= 6.59.0, title, WHY order (6.53.0 < 6.59.0 < `SCRIPT_VERSION`), verifier pin, ci lean entry, the tag |
| V2 | against main `a85edfc6`: only the title, version and `return CalibrationReport(` lines removed; added lines ASCII; no definition removed; exactly the 4 helpers added |
| X1 | golden negative: the main copy has no split and prints the pooled warning only |

**Golden negative** (`TFB_TEST_CALL_SPLIT_TARGET` = the untouched main copy): 31 failed, 6 passed. The passes are the off legs (O1 x5) and X1, which describe main itself.

**24 / 24 mutants killed** (failing tests in brackets):
- **gate:** default on (6), any word on (11), `enable` read as on (3), split not attached (6), gate bypassed (6);
- **render hook:** hook dropped (1), hook before the note (1);
- **numbers:** call roi unsigned (3), price roi signed (2), families swapped (5), like-for-like from the pooled buckets (2), like-for-like families swapped (2), hold count wrong (3), an exception escaping (1), `call_split` made a dataclass field (6), thin rule dropped (1), an unknown call read as a buy (3), roi rounded to 1 decimal (1), buckets unsorted (2), header omitted (1), like-for-like labels swapped (1);
- **lanes:** daily_sync unmapped (1), clock default on (1), ci entry removed (1).

**Other checks:**
- compileall (main.py core routes scripts tests) clean; ruff (CI rules E9,F63,F7,F82) clean.
- Related suites, 22 files: 65 + 419 passed, 1 skipped, plus the archive-bar test (25 passed): workflow flag mappings, verify pins, harness IC, learning metrics, summary integrity, tracker flags, record-only workflows, workflow audit, rollout flags, hist-close, log read, board coverage, force coverage, canonical publications, learning guards, shadow calibration, calibration report, activation.
- LEAN and contract-lane emulations on an export of this head: see S5.

## S4 - Off identity

With the gate unset, `off` or any other word:
- `calibrate()` returns the same object it built, without the attribute;
- `render_calibration_report` prints the v6.53.0 report line for line (O1 against the embedded golden and against the main copy);
- the export, `_calibration_env_string`, `_S1_Calibration` rows and the `_Run_Log` line read only dataclass fields, so they are unchanged;
- no log record is added on either setting.

With the gate on, only the stdout block is added (O2).

## S5 - Review and lanes

(filled after the review; see below)

## S6 - Owner steps (proposed; nothing is armed here)

1. Merge. No Render deploy: the tracker runs in GitHub Actions.
2. Set the repository Variable `TRACK_CALIB_CALL_SPLIT=on`. It adds report lines only, so it is safe at once.
3. On the next clock run, read the 'Call mix' block under 'By investability bucket':
   - **the like-for-like buy line answers R1:** if it still says WATCHLIST outperforms INVESTABLE, the inversion is real within buy calls, and the investability gate is the thing to investigate; if not, the pooled warning is a call-mix artifact;
   - **the BLOCKED row answers R11:** its sell calls' win rate, and its call roi next to its price roi.

Rollback: unset the Variable.

## S7 - Known limits and not done

- The like-for-like note keeps `_discrimination`'s wording and its rule (non-overlapping Wilson intervals at min_sample). It does not measure returns net of costs or against a benchmark (R1's other asks).
- The split is printed to stdout and the job log only. The `_calibration.json` export, the summary panel and the `_Run_Log` line do not carry it (on purpose: those surfaces stay byte-identical).
- Records whose entry call does not parse count as hold, as `_reco_trade_sign` has them.
- No change to factors, `TFB_CALIBRATION_FACTORS`, the pooled warning itself, the maturation rules, the cron or the concurrency.
- No live workbook read or write and no provider call: everything is offline.
