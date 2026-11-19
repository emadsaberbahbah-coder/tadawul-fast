# Commit Sheet: the two open trials beside each stock (trial_labels 1.0.0, 31_Final_Decision_Column 1.4.0, views 1.15.0), 19 Nov 2026

## S0. Why

Owner, 19 Nov 2026 18:46 Riyadh, verbatim: "This new updat for news is linked with the recommdadtions pages like protofilo or investment pages". At 18:50, in Arabic: do the best and most suitable for the project.

The answer at 18:46 was "partly". The news gate already reached Today_Action_Plan (a READ FIRST step), Portfolio_Decision (the Holding Read news column) and Exit_Plan (negative news tightens a stop). The sector tilt and the news shadow list of that day (#1030, #1032), and trial C-2 (#1026), stood only in BigQuery and on News_Board. Neither had evidence, so neither could change a decision.

This build shows both trials, observe-only, in the row of each stock on Top_10_Investments and Portfolio_Decision. No decision, score, list or order changes.

## S1. What

| File | Version | Change |
|---|---|---|
| `sql/bigquery/96_v_c2_calls.sql` | new | Every buy call with what C-2 does with it: `c2_keep`, `upside_rank`, `eligible_calls`, `keeps`, `why_out`. The WITH block is `v_trial_c2`'s own, character for character (case_v38). |
| `scripts/apply_bq_views.py` | 1.14.0 → 1.15.0 | Changelog; the new view joins the apply order. |
| `scripts/trial_labels.py` | new, 1.0.0 | Writes the `Trial_Labels` tab: one row per stock of Final_Decisions, each holding and each buy call of the latest call day. Columns: Symbol, Final decision, C-2 trial, News trial, Call day, News week, Sector. |
| `.github/workflows/trial_labels.yml` | new, 1.0.0 | 08:05, 12:05 and 17:05 Riyadh, plus by hand (dry run on request). |
| `apps_script/31_Final_Decision_Column.gs` | 1.3.0 → 1.4.0 | Two columns, C-2 TRIAL and NEWS TRIAL. On Top_10_Investments they sit right of FINAL DECISION (AG-AH); on Portfolio_Decision, right of the Holding Read columns (AE-AF). |
| `scripts/verify_deployment.py` | pins | `apply_bq_views` 1.15.0; `trial_labels` 1.0.0 (new). |
| `.github/workflows/ci.yml` | 1.1.89 | `tests/test_trial_labels.py` in the blocking lean lane. |

### The labels

**C-2 trial** (from `v_c2_calls`) takes one of four forms:
- `KEEPS (all horizons) - analyst upside +33.5%, 1st of 5 eligible US calls, top 3 kept`.
- `DROPS (all horizons) - HIGH confidence, Tadawul`, naming the conditions the call fails.
- `PARTLY - drops 1W: ...; keeps 1M: ...` when the horizons differ.
- `NO BUY CALL - none on 19 Nov; C-2 judges buy calls only`.

**News trial** reads the tilt in force and the latest list day, as News_Board does. It also takes several forms:
- `AVOIDED - Financials: political events AVOIDED, events + news themes AVOIDED; news list of 19 Nov: would leave (both arms)`.
- `FAVOURED`, `MIXED` (the arms disagree), `NEUTRAL`, `NO TILT IN FORCE` or `NO SECTOR`.
- Outside the US the label adds that the tilt is measured on US sector funds.

### The columns (31 1.4.0)

- **Formulas:** each cell looks up the symbol of its own row on Trial_Labels (INDEX/MATCH on `INDIRECT` of `ROW()`), with columns found by header name.
- **Headers:** they carry the call day and the news week, and say "observe only, not a decision or an order". The C-2 header says OLD when the call day is more than 3 days back.
- **Colours:** violet, blue, amber and grey. Never the decision's green or red.
- **Without the tab:** the headers say "Trial_Labels not read" and no formula is written.
- **Placement:** the same 10-minute tick (`fdcTick`) writes the columns. There is no new trigger and no setup.

## S2. Live checks (read-only, 19 Nov 2026 ~19:30 Riyadh)

**The rule matches the trial exactly.** The view body was run as a query on the archive. Its counts, scored calls and calls kept, match `v_trial_c2`'s arms exactly at every horizon:

| Horizon | Scored | C-2 kept |
|---|---|---|
| 1W | 1,437 | 443 |
| 2W | 1,253 | 373 |
| 1M | 1,492 | 223 |
| 3M | 849 | 0 |

**The calls of 19 Nov** (recorded 01:44 Riyadh):

| Group | Kept | Dropped |
|---|---|---|
| US, 5 eligible calls, top 3 kept | EFC (+33.5%), NMM (+32.6%), MBIN (+30.2%) | SB (+28.9%), BNY (+10.8%), on rank |
| Other markets | BBOX.L (+33.9%) | MEGACPO.MX on rank |
| Tadawul and HIGH confidence | | 1030.SR, 3020.SR, 4040.SR, 6015.SR, 7040.SR and 0700.HK |

**The news tilt in force, 17 to 23 Nov** (news week of 9 Nov):

| Arm | Favoured | Avoided |
|---|---|---|
| Political events | Information Technology, Consumer Discretionary, Utilities | Health Care, Financials, Consumer Staples |
| Events + news themes | Information Technology, Utilities | Energy, Financials, Consumer Staples |

The two trials therefore disagree on some names. C-2 keeps EFC and MBIN, both Financials, which the news tilt avoids. Showing both is the point.

**The in-sample figures moved after the matured-call bars.** The bars (`archive_symbol_deep` 1.2.0) added the matured calls. Its first run, 19 Nov 10:35 Riyadh, covered 761 outcome symbols (active and matured) and got bars for 741. The one-month figures now read:

| | Calls | Against the market | Ahead |
|---|---|---|---|
| C-2 | 223 | +3.95 points | 76.2% |
| Champion | 1,492 | +1.26 points | 56.5% |

The overlap-adjusted t is 0.37. These are still in-sample, not evidence.

## S3. Tests

| Suite | Result | What it checks |
|---|---|---|
| `tests/test_bq_views.py` | 38/38 | case_v38: the WITH block is identical to `v_trial_c2`'s; membership is decided over every call (no `WHERE scored`); `why_out` names exactly the conditions that fail; the 19 Nov US example. 6 of 6 SQL mutants caught. |
| `tests/test_trial_labels.py` | 9 passed | The stock list (order, aliases, duplicates); every C-2 and news wording; a list day outside the tilt's window claims no move; a closed window claims no tilt; the SQL is SELECT only and dated; a full run through the real BigQuery reader; an empty run; dry run, off switch and each failing view (the previous tab stays); the quota wait; the workflow manifest; and that the header names the GAS file looks for are the ones written. 17 mutants: 15 caught, 2 equivalent (the 10th ordinal; the minimum row count under the fake sheet). |
| `tests/test_fdc_v100_final_decision_column.js` | 16/16 | S14 to S16 are new: positions, formulas, headers, OLD, not read, header-name lookup, Date cells, colours, a second tick, a stale header, rebuild and rollback. S6 and S13 now expect the trial columns in the rollback message. 13 mutants: 11 caught, 2 equivalent (`!ts.cols` for `!ts.ok`; every column page has trials). |

Related suites (pins, CI, workflows, news board): 1,156 passed, 38 skipped. Workflow audit: errors=0 warnings=0.

## S4. Deploy (order matters)

1. Merge.
2. **BigQuery Looker Views** with `mode=write`. It creates `v_c2_calls`; the verify step then reads 56/56.
3. **Trial Labels** by hand: a dry run, then a real run. The first run creates the tab.
4. **Owner:** paste `apps_script/31_Final_Decision_Column.gs` 1.4.0 (682 lines) into the bound project and save. There is no setup; the next 10-minute tick adds the columns. Check with `fdcStatus` (self-test ok).
   - SHA-256 of the file: `cbfb933f473f8e10ba0801e5128006943532fa8e96b08f9298a3eac26cf4c861`

## S5. Rollback

- **Labels tab:** repository variable `TFB_TRIAL_LABELS=off`.
- **Columns:** run `fdcRemoveColumn` (which also takes off FINAL DECISION and the Holding Read columns; the next tick puts them back), or re-paste 1.3.0.
- **View:** none needed. Nothing reads it but the script.

## S6. Not in this version

- The v2 pages (`27_Decision_Pages_v2.gs`) and Today_Action_Plan do not show the labels yet.
- C-2 judges the calls recorded around 01:00 to 02:00 Riyadh and archived in the morning. A buy call added later in the day is labelled the next morning.

Not investment advice; I am not a licensed advisor.
