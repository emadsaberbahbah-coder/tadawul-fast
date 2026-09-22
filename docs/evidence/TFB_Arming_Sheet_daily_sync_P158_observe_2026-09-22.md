# TFB Arming Sheet — daily_sync.yml — P-158 TARGET-UNIT SENTRY = observe

Date: 2026-09-22 (Riyadh)  
Lane: GitHub Actions (`.github/workflows/daily_sync.yml`) — NOT Render  
Item: P-158 — `scripts/track_performance.py` v6.39.0 target-unit sentry, observe mode  
Build reference: `docs/evidence/TFB_Commit_Sheet_track_performance_v6.39.0_2026-09-21.md`

## Why the GitHub lane, not Render

`track_performance.py` runs inside the `📈 Track Performance (record + audit)` step of the
`sync-dashboard` job (global-markets leg only, `continue-on-error: true`). GitHub Actions never
sees Render environment variables, so the gate must live in the step's `env:` block.
Hardcoded value, not a repository Variable — a Variable can be silently unset (S-1 freeze,
2026-09-10 lesson).

## Base pin

| | |
|---|---|
| Base file | `.github/workflows/daily_sync.yml` at HEAD `b191320be87ed53207114dae003f4fcc4546629f` |
| Base SHA-256 | `ab760153d74d3796809c8f143f10ab06ebaa4d1c3facd4aad025081710d43aa0` (byte-identical to the P-154 armed delivery of 2026-09-21 — zero drift) |
| Base lines | 1,648 |

## Change

One anchored insertion (anchor `TRACK_HORIZONS: "1W,2W,1M,3M"`, count == 1): an 11-line comment
block plus the functional line

```yaml
          TFB_PERF_TARGET_UNIT_SENTRY: "observe"
```

inside the `env:` block of the `📈 Track Performance (record + audit)` step, directly after
`TRACK_HORIZONS`.

| | |
|---|---|
| Armed SHA-256 | `b0baca62f2e9f8cc24270cfc163b36d6f87f43ed9ec70b591019dc638513e21e` |
| Armed lines | 1,660 (+12 / −0) |
| Functional diff | exactly one env key (`TFB_PERF_TARGET_UNIT_SENTRY`), parsed YAML otherwise identical |
| YAML parse | base and armed both parse; the step exists in one job only (`sync-dashboard`) |

## Deploy

1. Replace `.github/workflows/daily_sync.yml` on `main` with the armed file (one commit, message
   `arm: TFB_PERF_TARGET_UNIT_SENTRY=observe (P-158)`). No Render deploy; no backend change.
2. Commits never spend provider calls (push runs `ci-tests` only). The arming takes effect on the
   first scheduled leg after the commit (cron `0 */4 * * *` UTC = 03/07/11/15/19/23 Riyadh), or on a
   fresh `Run workflow` dispatch (a Re-run replays the old SHA).

## Read-back (positive proof)

- `_Run_Log`: one `[PERF-UNIT v6.39.0]` line on the run — legacy vs unit-corrected MAE / signed
  error, fraction / percent / unresolved counts, creation would-scale counts.
- `_S1_Calibration!A4:D9`: the legacy-vs-corrected block populated; row 2 (criterion-4 row)
  unchanged; `_S1_Calibration!J2` = 6.39.0.
- Zero stored Performance_Log rows rewritten (observe never writes back).
- Expected shape: corrected 1W/2W MAE materially larger than the published 2.60 / 3.72 pp, because
  engine-supplied targets currently enter at ~1/100 scale (P-158 mechanism).

## Rollback

Delete the one line (or the whole inserted block) and commit — v6.39.0 default `off` is
byte-identical legacy behaviour.

## Next sitting (separate)

`enforce` — new records store percent-point targets, criterion 4 reads the corrected basis with a
Detail disclosure; requires the S-1 criterion-4 boundary note before flipping.
