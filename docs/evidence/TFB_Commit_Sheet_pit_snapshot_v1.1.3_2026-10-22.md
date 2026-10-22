# TFB Commit Sheet - pit_snapshot v1.1.3 [PIT DAY KEY]

| Item | Value |
| --- | --- |
| Date | 2026-10-22 (daily improvement routine) |
| Branch | `claude/daily-2026-10-22` |
| Base | main `57391476` (#817); `scripts/pit_snapshot.py` is v1.1.1 there |
| Files | `scripts/pit_snapshot.py` 1.1.1 -> 1.1.3, `.github/workflows/pit_snapshot.yml` v1.1.2 (one step env line), `scripts/verify_deployment.py` (manifest pin only), `tests/test_pit_day_key.py` (new) |
| Gate | `TFB_PIT_DAY_KEY` = `wallclock` (default) / `observe` / `slot`, read from repo variable `vars.TFB_PIT_DAY_KEY` |
| Default | off: byte-identical to v1.1.1 on rows, stderr, `_Run_Log` and exit code |
| Version note | 1.1.2 is claimed by open #785 and #795 (header-migration fail-closed). Neither touches the day key. Whichever lands second takes a trivial rebase on the docstring title and `__version__` |

## Why

`_PIT_Fundamentals` is the point-in-time archive for the future PIT backtest and the Postgres seed corpus. Its row key is `(Snapshot Date, Symbol)`:

- `Snapshot Date` is the Riyadh wall-clock date at RUN time (`_today_riyadh`).
- The duplicate guard (`existing_keys_for` + `build_rows`) skips any symbol that already has a row under that label.

The workflow fires at 20:10 UTC, which is 23:10 Riyadh. GitHub fires it late on most days:

- A run that starts on time (20:10-20:59 UTC) on day D is labelled D.
- A late run of the PREVIOUS slot (21:00 UTC on D-1 or later, up to 20:09 UTC on D) is ALSO labelled D.
- When a late run is followed by an on-time run, the second run's symbols are skipped as duplicates. The run still ends green and `_Run_Log` says OK, so that session's capture is lost for good.

## Evidence (Actions job logs, read 2026-10-22)

| Run | Slot | Fired (UTC) | Log line |
| --- | --- | --- | --- |
| 31136275420 | 6 Aug | 2026-08-07 00:54 | `date=2026-08-07 symbols=53 to_write=53 skipped_dup=0` |
| 31217800255 | 7 Aug | 2026-08-07 20:53 | `date=2026-08-07 symbols=129 to_write=93 skipped_dup=36` |

The 7 Aug (Fri) captures of 36 symbols were never archived. For the non-Tadawul listings among them, that includes their 7 Aug close; Tadawul does not trade on Friday. The split cannot be read without opening the workbook. Both runs succeeded with `_Run_Log` status OK.

The adversarial evidence check re-read both job logs and the full run list of all 93 runs (every one event=schedule, attempt 1). It found every figure here exact, including the 19 D-labelled runs #18-#36 and the single duplicate label 2026-08-07.

Other figures:

- The 26 Aug slot fired at 23:25 UTC and was labelled 2026-08-27, so 2026-08-26 has no rows.
- Of the 93 scheduled runs from 21 Jul to 21 Oct, 74 fired after 21:00 UTC and carry the D+1 label. That includes all of the last 57 runs, which were 111-490 minutes late. Only the 7-25 Aug runs carry D.
- **Current exposure is nil** while GitHub keeps firing the slot late. The loss recurs on the next late run that is followed by an on-time one.
- At today's symbol set the stake is about 11 symbols per event (run 39691974424: Top_10 9, My_Portfolio 2, Shadow_Board 0).

**Not affected:** no decision or outcome surface reads `Snapshot Date`. Its only readers are:

- the DQ-66 distinct-date count (`tfb_workbook_dq_audit.py`);
- `tfb_export_audit.check_pit`, which reads Forecast Reliability only;
- the legacy-row repair tool, whose window is fixed to 07-22..07-28.

This is evidence integrity (priority b/c of the routine), not a published number.

**Precedent:** `run_shadow_scorer` v1.9.0 P-176 (`resolve_evidence_day`) fixed the same bug class for S-1.

## What changed

`scripts/pit_snapshot.py`:

| Site | Change |
| --- | --- |
| `_pit_day_key_mode()` (new) | `TFB_PIT_DAY_KEY` lower-cased. `observe` and `slot` arm the gate; unset, empty, `wallclock` and every other word read as `wallclock` |
| `slot_day_key(now_utc)` (new, pure) | One label per scheduled slot: the slot that starts at 20:10 UTC on D, and runs to 20:10 UTC on D+1, is labelled D+1, the date it ends on. The input is converted to UTC first |
| `_captured_at(row)` (new, pure) | Reads `Captured At (UTC)`: column 24, or column 16 of a legacy 16-column row. Returns None when the value is absent or unparseable |
| `cross_slot_dups(values, label, slot, symbols)` (new, pure) | Counts the run's symbols that already have a row under `label` captured in a DIFFERENT slot. These are the captures the guard drops. A capture it cannot parse or place in a slot (for example a 9999-12-31 sentinel) is skipped, never raised |
| `main()` | `today = _today_riyadh()` is unchanged. Then:<br>- **wallclock:** nothing else happens.<br>- **observe:** keeps the wall-clock label, rows and guard, and adds one `[PIT-DAYKEY v1.1.3] mode=... key=... wallclock=... slot=... cross_slot_dups=N` line plus a `day_key` object in the OK `_Run_Log` details.<br>- **slot:** uses `slot_day_key` as the label and the duplicate key, and logs the same line and object.<br><br>Both armed modes read the slot clock AFTER the page and ledger reads, right before `build_rows` stamps `Captured At`. A run that starts at 20:09:50 UTC and captures at 20:10:20 is therefore labelled by its capture, on the same clock `cross_slot_dups` uses for the ledger |
| Docstring | Title v1.1.3; ENV lists `TFB_PIT_DAY_KEY` |
| Version chain | One pointer line `# v1.1.3 WHY (2026-10-22)` above `__version__`. The full WHY block (evidence, per-site change, rollback) sits above `_pit_day_key_mode()`. Every prior WHY block is verbatim |

Other files:

- `.github/workflows/pit_snapshot.yml` v1.1.2: the run step gets `TFB_PIT_DAY_KEY: ${{ vars.TFB_PIT_DAY_KEY || 'wallclock' }}`. This is step level, so the job env is unchanged.
- `scripts/verify_deployment.py`: pin `("pit_snapshot", "1.1.3", ...)` with a dated comment. `SCRIPT_VERSION` is unchanged and nothing is registered in FLAGS, so the render env contract is untouched.

Not changed: `existing_keys_for`, `build_rows`, the header migration, the harvest, the cron, the concurrency group, and every other workflow.

### Why `slot` labels D+1 and not D

| Property | Slot labelled D+1 |
| --- | --- |
| Agreement with today's labels | Equals the wall-clock label for every fire from 21:00 UTC to 20:09 UTC the next day. That covers all 57 recent runs, so arming changes no label for them |
| Where it differs | Only for on-time fires in [20:10, 21:00) UTC, which are exactly the runs that can collide |
| Look-ahead | Never earlier than the capture's Riyadh date, so no PIT row is dated before the information it holds |
| Archive convention | Continues the archive's majority D+1 convention (74 of 93 days) |
| Arming | Needs no migration: a slot label can never equal an older wall-clock label from a different slot |

A D-labelled slot key was considered and rejected:

- It would switch the convention for the 74 D+1 days.
- It would stamp captures fired as late as 04:20 UTC with the previous day, which is a look-ahead for PIT.
- Its arming day would put two slots under one label.

## Measured results (offline, venv Python 3.11)

**Harness** `tests/test_pit_day_key.py`: `PASS 13/13` as a script and `13 passed` under pytest.

**Golden negative.** The base copy is `git show 57391476:scripts/pit_snapshot.py`, run as the delivered module with `TFB_TEST_PIT_DELIVERED=<copy>`:

- Result: 11 failed, 1 passed (P2, off identity), 1 skipped (N2, exact-diff checks apply to 1.1.3 only).
- P1 on base gives `[3, 3, 0, 3]` rows for the four real August fire times, which reproduces the production loss. Delivered `slot` gives `[3, 3, 3, 3]`.

**Off identity (P2).** Compared with the base on rows, `_Run_Log` rows, stderr (version string normalised) and exit codes:

- gate values: 9 (unset, `wallclock`, empty, `WallClock`, `on`, `1`, `true`, `enforce`, `next`);
- ledger states: 3 (current header, legacy 16-column header, absent tab);
- fire times: 10 per sequence (6 grid times + the 4 August times).

Every comparison is equal.

**Mutations** (`-x` per mutation): 18 of 18 caught. The mutations were:

- slot without +1;
- anchor 20:00;
- slot label not applied;
- inverted cross-slot test;
- an extra armed word;
- wrong legacy column;
- no log line;
- `day_key` always present;
- no `T` separator handling;
- no UTC conversion;
- observe relabels;
- unparsed captures counted;
- case-sensitive symbol set (twice);
- the wall-clock path not via `_today_riyadh`;
- no lower-casing;
- no calendar-end guard;
- slot read at run start instead of after the ledger read (the pre-review build, which P11 also fails when run as the delivered module).

**Compatibility with open #785.** A trial merge of this branch into `pr785` has one conflict, on the docstring title and the version lines. With both WHY blocks kept and 1.1.3 chosen, #785's `tests/test_pit_snapshot_schema.py` passes 17/17 on the merged file (it pins the label through `_today_riyadh`).

On that merged tree this harness's P2/N2 fail as expected, because their base must then move to the post-#785 main. That is a one-line `BASE_SHA` change at merge time.

**Repo checks:**

- `python -m compileall -q main.py core routes scripts tests`: clean.
- `tests/test_learning_guards.py tests/test_pit_repair.py tests/test_verify_manifest_pins.py tests/test_render_env_contract.py tests/test_flag_registry.py`: 134 passed, 1 skipped.
- `--selftest`: 6/6.
- The ci.yml blocking lean list: see the PR body.

## Harness cases

| Case | Proves |
| --- | --- |
| P1 | slot: the real Aug fire times write every capture (labels 08-06..08-09, 12 unique keys) |
| P2 | off (9 spellings x 3 ledger states x 10 fires) == the v1.1.1 base on every surface; the base drops the 7 Aug captures |
| P3 | observe: rows, labels and exit codes equal off; exactly one extra stderr line; `cross_slot_dups` is 0,0,3,0 over the Aug sequence |
| P4 | `slot_day_key` table, incl. year and leap-day roll and Riyadh-aware input; differs from wall-clock exactly on [20:10, 21:00) UTC, never earlier (every minute over 72 h) |
| P5 | arming after wall-clock runs: no lost and no doubled (label, symbol) |
| P6 | slot keeps the guard: re-runs inside one slot write nothing; the next slot writes |
| P7 | `cross_slot_dups` pure table: 24-wide, legacy 16-wide, ISO `T`, unparseable, other label, other symbol, lower-case input; 9999-12-31 / 0001-01-01 captures are skipped, and an observe or slot run over such a row still exits 0 and writes |
| P8 | `_Run_Log` details: off has exactly v1.1.1's keys; slot adds `day_key` |
| P9 | wallclock and observe read the module-global `_today_riyadh` (the #785/#795 tests pin it) |
| P10 | slot dry-run prints the slot label and writes nothing |
| P11 | the slot is read on the capture clock: the clock moves when the ledger is read (start 20:09:50, capture 20:10:20). The slot label equals `slot_day_key(Captured At)` for every row, and observe's count agrees with what slot does on the next run |
| N1 | workflow mapping; the slot anchor equals the cron; version floor `>= (1, 1, 3)`; docstring title; verifier pin; WHY order |
| N2 | added lines ASCII (the pre-existing em dash in the title excepted); removed lines exactly the title, the version line and the 3-line OK `_Run_Log` call; no function removed; exactly four added |

**CI:** the harness runs in the advisory whole-suite job (`pytest -q tests`). P2 and N2 need the base copy and skip on a shallow checkout. It is not enrolled in the blocking lean list, because ci.yml versions 1.1.33-1.1.35 are claimed by open #818 and #778. Enrolment is proposed below.

## Operator steps (owner) - nothing is armed by this PR

1. Merge, then set the repository variable `TFB_PIT_DAY_KEY=observe`. The next runs log `[PIT-DAYKEY v1.1.3] ... cross_slot_dups=N`. N > 0 is a capture the wall-clock key is dropping.
2. Arm with `TFB_PIT_DAY_KEY=slot`. It can go straight to slot: for every fire at or after 21:00 UTC (all recent runs) the label is unchanged, so there is no discontinuity.
3. Optional follow-up: enrol `tests/test_pit_day_key.py` in the blocking lean list in the next ci.yml bump.

## Known limits

- **Disarming** (slot -> wallclock, or slot -> observe, since observe keeps the wall-clock label) right after an on-time slot run (fired in [20:10, 21:00) UTC) reproduces the old collision once: the next on-time run gets the same label and is skipped. Disarm after a run that fired at or after 21:00 UTC. This is stated in the code ROLLBACK and in the workflow header.
- **Residual clock race:** the slot is read milliseconds before `build_rows` stamps `Captured At`, with no I/O in between. A run whose two reads straddle 20:10:00.000 UTC would get a label one slot below `slot_day_key(Captured At)`.
- **A manual dispatch before 20:10 UTC** belongs to the previous slot, so it dedups against that slot's capture. This is the same as wall-clock behaviour for fires after 21:00 UTC the day before. The `[PIT-DAYKEY]` line shows both labels.
- **The slot anchor** (`_SLOT_UTC_HM = (20, 10)`) must move with the cron. N1 fails if they diverge. The post-1 Nov US standard-time issue (the 20:10 UTC fire is before the 21:00 UTC close) is unchanged and still registered in the workflow header.
- **Historical labels are not rewritten.** The 2026-08-07 loss is permanent. The 2026-08-26 gap is a label shift, not a loss: those captures sit under 08-27, and `Captured At (UTC)` distinguishes them.

## Rollback

Delete the repository variable or set it to `wallclock`; no redeploy is needed. Or `git revert` the commit.
