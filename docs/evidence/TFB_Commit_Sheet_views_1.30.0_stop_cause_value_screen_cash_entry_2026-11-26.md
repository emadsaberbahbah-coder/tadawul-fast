# TFB Commit Sheet — apply_bq_views 1.30.0 (six views: stop judgment, value screen, cash entry) + learning_watch 1.1.0

Date: 2026-11-26 (Thursday) · Lane: BigQuery research views · Gate: none (research only: no decision, gate or count reads these views) · No Render deploy · No order is placed, changed or cancelled

## Why (owner, 26 Nov 2026, Riyadh)
- 01:07 — is the fall temporary or lasting; adjust the stops or keep them; "the system must have reason, wisdom and flexibility".
- 01:22 — "there is cash and the market is falling, so there are big chances to improve the gains ... but that is not used ... look with the eye of an expert in valuing shares and the eye of a wise investor".
- 01:37 — "please do the required build now".

The answer of that night proposed three things to measure before anything is switched on (ground rules of 21 Nov: observe, log, score, review, then switch on only what proved itself). This change is the measuring.

## Change
| # | File | What it holds |
|---|---|---|
| 131 | `v_stop_cause_events` | US and Saudi shares with bars: a close crossing −8 % over 15 sessions (a share's first crossing, then the first one more than 20 sessions after the last kept event); the cause from the market's own 15 sessions (with the market ≤ −5 %, on its own ≥ −1 %, mixed); the verdict of the ladder under test (hard limit at −15 %, wait on a fall with the market inside 5 points, else stop stands); the next 5, 10, 20 sessions, the worst close, and the rule's own exits |
| 132 | `v_stop_cause_learning` | its record against selling at the fall, per period, market, cause, verdict and arm, with a fixed reading |
| 133 | `v_value_screen` | each archived night, the US shares passing: market cap ≥ 2 bn, 0 < forward P/E ≤ 15, free cash flow > 0, quality ≥ 65, analyst upside ≥ 20 %, lower 35 % of the 52-week range, data quality ≥ 80, reliability ≥ 70 — beside the engine's label |
| 134 | `v_trial_value_screen` | that list and the engine's INVESTABLE names against all liquid US names of the same night, 1W and 2W on |
| 135 | `v_cash_entry_events` | the US market's first not-calm reading of a spell; cash into SPY all at once, in three parts (sessions 0, 5, 10) or after the first calm close; 20 and 60 sessions on |
| 136 | `v_cash_entry_trial` | its record against cash |
| — | `scripts/apply_bq_views.py` 1.29.0 → **1.30.0** | header only; no file of 1.29.0 changes |
| — | `scripts/learning_watch.py` 1.0.1 → **1.1.0** | three records join the registry (21 read): 132, 134, 136 |
| — | `scripts/verify_deployment.py` | pins `apply_bq_views 1.30.0`, `learning_watch 1.1.0` |

Every rule is fixed on 26 Nov 2026. Every row before that day is in-sample and reads "in-sample, not evidence" whatever its numbers; Learning Watch groups it IN-SAMPLE.

## First reading (history, in-sample — context, not evidence)
Run on the archive on 26 Nov with the SQL of these files.

**Why a share fell, and what holding 20 sessions did after it (selling at the fall = 0)**

| Market | Cause · verdict | Events | Days | Mean | Higher | Against the market | t by day |
|---|---|---|---|---|---|---|---|
| US | with the market · wait | 379 | 22 | +6.82 % | 73.4 % | +0.16 pts | 0.96 |
| US | with the market · wait, the rule's exits | 379 | 22 | +4.41 % | 54.1 % | — | 1.22 |
| US | mixed · stop stands | 770 | 81 | +2.55 % | 60.4 % | +1.53 pts | 1.47 |
| US | on its own · stop stands | 1,767 | 359 | +0.68 % | 51.3 % | +0.37 pts | 1.45 |
| US | on its own · hard limit | 112 | 83 | −9.73 % | 39.3 % | −9.77 pts | −1.93 |
| Saudi | with the market · wait | 291 | 33 | +1.81 % | 56.7 % | +0.16 pts | 0.16 |
| Saudi | with the market · wait, the rule's exits | 291 | 33 | −0.15 % | 34.0 % | — | −0.63 |
| Saudi | on its own · stop stands | 412 | 136 | −2.49 % | 35.7 % | −1.41 pts | −0.90 |
| Saudi | mixed · stop stands | 1,220 | 194 | −1.69 % | 39.3 % | −1.46 pts | −1.74 |

What it says, and what it does not: after a fall with the market, US shares were higher 20 sessions later in 73 % of cases — but level with the market itself (+0.16 points), on 22 days only, and not statistically clear (t 1.0). The gain is the market's rebound, not the share's. The rule's own exits cost about 2.4 points of it. A share that fell 15 % on its own kept falling (−9.7 %). Saudi shares that fell on their own or in a mixed fall kept falling, and for them the rule's exits gave nothing. Nothing here reaches t 2.

Events are one per share per 20 sessions counted from the last kept event (Codex P2 on #1085; the first cut counted from the last crossing and held 314 US "wait" cases — the reading is the same).

**Value screen against all liquid US names (19 nights, 31 Oct – 18 Nov, a falling market)**

| Arm | Horizon | Nights | Names a night | Mean | Edge | Nights ahead | t |
|---|---|---|---|---|---|---|---|
| value screen | 1W | 19 | 42 | −1.44 % | +1.55 pts | 73.7 % | 1.06 |
| value screen | 2W | 12 | 42 | −2.82 % | +2.57 pts | 75.0 % | 0.84 |
| engine INVESTABLE | 1W | 19 | 56 | −2.28 % | +0.72 pts | 63.2 % | 0.80 |
| engine INVESTABLE | 2W | 12 | 66 | −3.39 % | +1.99 pts | 100 % | 1.18 |

Both lists lost money in those weeks; both lost less than the average US name. Neither edge is clear.

**Cash into SPY after the first not-calm reading (10 matured signals since Nov 2023; the 11th is 5 Nov 2026)**

| Way in | 20 sessions | Above cash | Worst | 60 sessions | Above cash | Worst |
|---|---|---|---|---|---|---|
| all at once | +1.41 % | 8 of 10 | −4.36 % | +5.13 % | 8 of 10 | −3.27 % |
| three parts (0, 5, 10) | +1.20 % | 6 of 10 | −1.70 % | +4.94 % | 9 of 10 | −4.38 % |
| after the first calm close | +1.08 % | 7 of 10 | −0.54 % | +4.60 % | 9 of 10 | −4.63 % |

Ten signals in three rising years. t between 1.1 and 1.9.

## Tests
| Battery | Result |
|---|---|
| `tests/test_bq_views.py` (66; new `case_v66`) | order, kinds, pinned body hashes (six in PENDING), static checks, the fragments of each rule, history tested first in every reading, no clock, the ladder mirrored, the cooldown's array steps mirrored in Python against the plain greedy rule (300 random cases, Codex's example, a row read on the archive), the Exit Plan's knobs equal to the view's limits when both are in the tree |
| `tests/test_learning_watch.py` (+8 cases) | 21 records; each reading of the three views lands in its group forward and IN-SAMPLE for history |
| `tests/test_verify_manifest_pins.py`, `tests/test_bq_snapshots.py` | pass |
| BigQuery | each view's SELECT run on the archive on 26 Nov (results above); the kept events of the 80 shares with the most crossings read back and checked by hand on three |

## After merge (no owner step)
Claude dispatches `bq_views` write on main (six views created, 98 unchanged), then reads back `--verify`. The next Learning Watch run shows the three records under IN-SAMPLE; their forward rows start collecting from 26 Nov.

## Rollback
The views are read by nothing but Learning Watch. `DROP VIEW` of the six, or revert the commit and re-apply; `TFB_LEARNING_WATCH=off` stops the page.

## Known limits
- The fall study covers the shares the project followed (545 US, 262 Saudi), not the whole market, and the survivors among them.
- A 15-session fall is a proxy for a stop that was hit, not the holding's own stop from its own entry.
- The value screen's history is four weeks of one falling market; analyst targets lag a falling price.
- The cash-entry record is one fund in one country, with no costs or taxes.
- A stop-execution scorecard (each stop fill against the session's first trade, low and close) needs the broker's fills in BigQuery; they are in the workbook only. It follows.
