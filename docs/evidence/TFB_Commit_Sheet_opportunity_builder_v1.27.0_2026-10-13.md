# TFB Commit Sheet - opportunity_builder v1.27.0 (Top_10 data quality)

Renumbered 2026-10-19: main shipped a different 1.27.0 first (#797, conviction sizing). This build is now **v1.27.1**. Main's default venue-calendar map (v1.26.2, kill switch `TFB_TICKET_VENUE_CAL_EXT`) already covers most suffixes, so `TFB_OPP_VENUE_CAL_EXT` now adds only .NS, .AT and .SZ. The figures below are as measured on 2026-10-13 against 1.26.1.

Date: 2026-10-13. Base: main 278f148 (builder 1.26.1). Gates: three, all default OFF.

## Why this item

The operator exported Top_10_Investments at 2026-10-13 20:37 Riyadh (17:37 UTC) and reported a
data-quality problem. The page showed:

- 0 of 10 selected (withheld), 2 qualified;
- Quote Freshness as the top blocker on 481 of the 500 written audit rows (96.2%);
- six grace-held seats (AED.BR, BBOX.L, INDV.US, CRC.US, MGY.US, JMSB.US) with blank Rel, DQ,
  Conf and Engine ROI;
- `missing_fx 13` (USX) and `missing_valuation 3428` alerts.

## Evidence

Breakdown of the 481 Quote Freshness rows in the export, by venue and failure mode:

| Rows | Venues | Failure | Cause |
|---|---|---|---|
| 396 | .US 385, .TO 11 | STALE_PRICE intraday | daily_sync runs at 04:17, 12:17 and 20:17 UTC. The 12:17 run is before the NYSE open, so the sheet held the 12 Oct close (1296 min old) while NYSE traded. |
| 41 | .L 15, .PA 8, .MC 6, .ST 5, .SW 4, .AS 3, .DE 2, .BR 2 (some rows outside this list) | STALE_PRICE pre-close | Quoted around 12:20 UTC; the venues closed at 15:30 UTC with no post-close sync yet. |
| 41 | .NS 11, .AX 5, .JK 5, .BK 4, .OL 3, .MX 3, .AT 2, .TW 2, .KL 1, .CO 1, .HE 1, .VI 1, .VN 1 | UNVERIFIED_PRICE, venue calendar unavailable | No calendar mapped for the suffix, so even a quote at the venue's latest close cannot be certified. |

The gate itself is correct (v1.4.0 W-2 rationale). What the page lacked:

- any statement that the cause was run timing;
- a calendar for 41 rows that were deferred only for lack of one.

Grace-held seats:

- None of the six symbols is in the written audit grid. The builder caps the written audit with
  `TFB_OPP_AUDIT_ROWS_MAX` and keeps only selected, unfunded, near-miss and INVEST rows.
- The cockpit's stability layer holds those seats, but never tells the builder which symbols it
  is holding.
- `dt10StabIdFallback_` therefore renders an identity-only shell with blank metrics.

## What changed

`core/analysis/opportunity_builder.py` 1.26.1 -> 1.27.0:

1. **`TFB_OPP_VENUE_CAL_EXT=1`: venue calendar extension.**
   - `_venue_state` resolves the venue through the new `_venue_code`. That function holds the
     same suffix logic, moved verbatim, then falls back to `_VENUE_CAL_MAP_EXT` when the gate is
     armed. The extension covers 30 suffixes.
   - Every code was checked to exist in exchange_calendars 4.13.2.
   - Suffix-to-venue was matched to the pool's own Market labels: NSE India, ASX, IDX, SET, Oslo
     Bors, BMV, ATHEX, TWSE, KLS, Copenhagen, Helsinki, Wiener Boerse.
   - `.AT` is Athens (ASEX), not Vienna.
   - The rule is unchanged: a quote passes if it is live within `TFB_TICKET_MAX_QUOTE_AGE_MIN`
     in session, or if it is the witnessed latest close.
2. **`TFB_OPP_FRESHNESS_ALERT=1`: stale-quote alert.**
   - One countable `stale_quotes` alert built from the FULL audit.
   - It splits failures into: prior-session price while the venue trades, older than the latest
     close, no calendar, and no acquisition proof.
   - It names the top venues and the next close in both UTC and Riyadh time.
   - The near-miss "How To Qualify" text for a Quote Freshness row says when the row
     re-qualifies.
   - Display only.
3. **`TFB_OPP_AUDIT_KEEP=1`: keep held symbols in the audit.**
   - `criteria.audit_keep_symbols` (a list or comma string, at most 60) fills the room the
     audit cap leaves after the always-written rows, in audit order, ahead of the depth-ordered
     filler. The cap never grows; held rows that do not fit are reported as missing.
   - Review fix (Codex P2 on #770): the first build added held symbols to the always-written set,
     so a full cap could grow by up to 60 rows. W10b proves the cap now holds (it fails on the
     first build).
   - `meta.audit_keep` reports requested, written and missing.
   - The key is read from the RAW criteria. `make_criteria` drops unknown keys, so
     `criteria_snapshot` is unchanged.
   - The route already passes unknown keys through (`_opp_criteria_from_body`, proven by W13).

Other files:

- `scripts/verify_deployment.py` 1.0.63: pins the builder at 1.27.0 and registers the three
  flags. It is numbered after #775 (1.0.61) and the open #773 (1.0.62).
- `tests/test_ob_top10_data_quality.py`: new harness.

Not touched: `ci.yml`. The new harness runs in the whole-suite job, which installs
`requirements.txt`. Its real-calendar cases skip in the lean job, which has no
exchange_calendars or pandas. Enrolling it in the lean list is a follow-up once the open stack's
`ci.yml` headers land.

## Default OFF identity

- With all three unset, the payload equals v1.26.1 except the version stamp.
- The check (G1) uses the real `build_opportunity_payload` on two pools at a pinned clock.
  Seven OFF spellings were checked (N1).
- AST inventory: 0 functions removed and 11 added:
  - `_env_venue_cal_ext`, `_env_freshness_alert`, `_env_audit_keep`;
  - `_venue_suffix`, `_venue_code`, `_venue_next_close`, `_utc_riyadh_hm`;
  - `_freshness_class`, `_freshness_improve_note`, `_freshness_alert`, `_audit_keep_symbols`.
- Every added line is ASCII.

## Measured results (offline, no provider, no workbook)

- `python3 tests/test_ob_top10_data_quality.py`: PASS 19/19 (2 golden cases skip without a
  base copy).
- With `TFB_TEST_OB_BASE` set to the output of
  `git show 278f148:core/analysis/opportunity_builder.py`: PASS 21/21.
  - G1: the OFF payload is byte-identical to v1.26.1.
  - G2: the armed env does nothing on v1.26.1. There is no `stale_quotes` alert, 1295.KL stays
    uncertified and there is no `meta.audit_keep`. This proves the armed cases detect the
    feature.
- Lean env (no calendars): 10 passed, 10 skipped with reasons.
- Existing builder suites (opportunity_builder, quote_evidence, successful_acquisition_freshness,
  intraday_quote_refresh_contract, board_funding, usx_subunit, research_diversify,
  settle_unresolved, cash_floor_pct, portfolio_certification, blocked_invariant, manifest pins):
  347 passed before the pin bump, then the pin test passed.
- Every test file that imports opportunity_builder, routes.advanced_analysis or verify_deployment
  (29 files, including the new harness), run in the project venv: 681 passed, 14 skipped, 0 failed.
- Synthetic replay at 17:37 UTC Tuesday 13 Oct 2026:
  - **All armed:** 1295.KL quoted at 09:00:30 UTC (Bursa close 17:00 MYT) passes as "last
    close". AAA.US (12 Oct 20:00 close) still fails STALE_PRICE intraday. BBB.L (12:20 UTC)
    still fails STALE_PRICE pre-close. A 2330.TW quote 60 min old during the Taipei session
    still fails intraday, and a 5 min old one passes live.
  - **Alert text:** "Quote Freshness deferred 3 candidate(s): 1 prior-session price(s) while the
    venue trades (.US 1) - .US closes 20:00 UTC / 23:00 Riyadh; 1 price(s) older than the venue's
    latest close (.L 1) - no post-close sync yet; 1 on venues without a trading calendar (.KL 1)
    - TFB_OPP_VENUE_CAL_EXT=1 certifies their latest close. ..."

## Known limits

- The 396 prior-session and 41 pre-close rows are a run-timing matter, and no code here changes
  that. Three ways to address it:
  - Run Top_10 after the 20:17 UTC (23:17 Riyadh) daily_sync lands, roughly 00:30-03:00
    Riyadh, when every venue's latest close is on the sheet.
  - Run it after the 04:17 UTC sync, before the Tadawul open. Asian venues are then mid-session.
  - Dispatch Intraday Quote Refresh (apply=true, from main) first. It refreshes up to 400
    decision symbols harvested from the Top_10 page.
- `.VN` (and any venue without an exchange_calendars code) remains calendar-less.
- MWG.VN is labelled NASDAQ/NYSE and USD on the sheet while its price is in VND (74,700), so its
  Price SAR reads 280,653. This is an upstream identity defect. It is reported, not fixed here,
  and Quote Freshness defers the row today.
- AUDIT-KEEP needs both halves: the backend env and the pasted cockpit v1.13.10 with its Script
  Property. Either alone is inert.

## Cockpit half: 16_Decision_Top10.gs v1.13.9 -> v1.13.10

Built 2026-10-13 as an unreleased v1.13.9 draft; #774 then shipped a different v1.13.9 (entered-cash label
and holding UTC provenance), so this change was re-applied on top of it as v1.13.10 on 2026-10-14. The
designed regions are byte-identical apart from the version words.

- Script Property `DT10_AUDIT_KEEP`. Exactly `on` (trimmed, any case) arms it; anything else, or
  a property-service failure, is off.
- Armed, `body.criteria.audit_keep_symbols` carries the sorted stability members (seated,
  grace-held and fast-track; at most 60) from `DT10_STAB_STATE_V1`, the blob `dt10StabCore_`
  already uses.
  - The allocation replay deep-copies the body, so it carries the same list.
  - Display context only: no gate, score, seat or ticket moves.
- Off: every request and render is identical to v1.13.9.
- Functions added: `dt10AuditKeepOn_` and `dt10AuditKeepSymbols_` (0 removed). Vars added:
  `DT10_AUDIT_KEEP_PROP` and `DT10_AUDIT_KEEP_MAX`.
- Harness `tests/js/dt10_audit_keep.test.js`: the full source runs in a node vm with the
  cohort-binding fixtures.
  - K1-K5: the reader, the pure list, and REAL `refreshDecisionTop10` off, on, and with an
    empty or broken state.
  - D1: off POST bodies equal v1.13.9.
  - D2: the delivered file minus the exact-text v1.13.10 regions equals v1.13.9 byte for byte,
    and a stray line inside a region is rejected.
  - Results: PASS 8/8 with `DT10_BASE` (main's v1.13.9, 030e22e), and 6/6 plus 2 skipped without it.
- Version pins moved 1.13.9 -> 1.13.10 in `decision_input_truth` (census 193/105) and
  `decision_source_readiness`.
  - `tests/js/dt10_feed_cohort_binding.test.js` and `decision_input_truth.test.js` strip the v1.13.10
    regions byte-exact through the shared `tests/js/dt10_audit_keep_regions.js`, then hand the
    v1.13.9 text to #774's history helper (`tests/js/dt10_cash_utc_history.js`), whose digest still
    verifies. The H1 mutation checks run on the stripped text, so they still fail for their own reason.
  - Cohort binding passes 27/27 with the v1.13.7 base, so G4 of `tests/test_release_decision_feed.py` still
    holds.
- CI enrolment of the new node harness is added once the open stack's `ci.yml` headers land.
  Until then the updated pinned harnesses run in CI and this one runs locally.

Paste (after merge): paste the whole `apps_script/16_Decision_Top10.gs` and save, then run
`dt10SelfTest` and expect `[DT10 v1.13.10] self-test`. Leave `DT10_AUDIT_KEEP` unset until
`TFB_OPP_AUDIT_KEEP=1` is on Render. Rollback: delete the property, or re-paste v1.13.9.

## Operator steps (Render ENV; nothing is armed by this PR)

1. `TFB_OPP_FRESHNESS_ALERT=1`. This is display only, with no verdict change. Refresh Top_10 and
   read the `stale_quotes` row.
2. `TFB_OPP_VENUE_CAL_EXT=1`. This is a declared eligibility change: rows on extension venues
   whose quote is the latest close become eligible for the later gates. Compare Passed before and
   after on one refresh.
3. `TFB_OPP_AUDIT_KEEP=1` on Render, then Script Property `DT10_AUDIT_KEEP=on` after the
   v1.13.10 paste.
4. Rollback: unset each one. No redeploy is needed (each is read at call time).

Separately, to clear `missing_fx 13` today, use either of these:

- add the FX row `USX = 0.0375` to `_Lists_Config`, which already works without a deploy;
- or set `TFB_OPP_FX_USX_SUBUNIT=1` (v1.26.0).

## Owner decisions owed

- Whether `TFB_OPP_VENUE_CAL_EXT` should become default ON after one observed refresh. The house
  rule for protective guards is to ship armed. This is not a protective guard: it admits more
  rows, so it stays opt-in.
