# TFB G4 — Diagnosis: Global_Markets "invalid" rows (285 today, 449 yesterday)

**Date:** 2026-10-17 · **Brief item:** Codex Brief v2 §5 G4 ("diagnosis only; run duration 4h37m on the 23:17Z slot is the first hypothesis") · **Status:** diagnosis, no fix. Every number below is read from the GitHub job logs of the `daily_sync.yml` Global_Markets leg (job ids in the table); the artifact zips themselves could not be downloaded from this session (the Azure blob host is not reachable through the egress proxy), so the per-row telemetry (`sync_row_telemetry_*.jsonl`) is the follow-up read for the error-class × provider table.

## 1. What "invalid" is

`[PAGE-VERDICT]` counts `fresh_rows` against `requested_rows`; the brief's "invalid" is **requested − fresh**, i.e. rows whose `Last Updated (UTC)` stamp did not advance in the run. It is **not** a provider error count: `[EODHD-QUOTA]` reports `f429=0 f404=0 fetch_failed=0` in both runs.

| Run (GM leg) | Start (UTC) | Sync step | fresh / requested | not fresh | identity quarantine (restored from last-good) | fetch-missed (preserved) | OHLC lake flagged / blank open | rank withheld low_trust / no_score | EODHD day quota used at start |
|---|---|---|---|---|---|---|---|---|---|
| 39018080003 / job 117354201040 | 17 Oct 08:40 | 53 m 53 s (3,233 s) | 6,324 / 6,609 (95.69 %) | **285** | **82** | 5 (NVEI.TO, HEES.US, CCRN.US, RCM.US, TRTN.US) | 5 / 231 | 267 / 138 | 5.7 % (22,888 / 400,000) |
| 38978392280 / job 117233234937 | 16 Oct 22:56 | 58 m 37 s (3,516 s) | 6,280 / 6,609 (95.02 %) | **329** | **126** | 7 (TRTN.US, STM.PA, SMAR.US, PDCO.US, PE&OLES.MX, SILK.US, THS.US) | 113 / 259 | 279 / 103 | 30.5 % (122,110 / 400,000) |
| 38653145144 / job 116156549273 (the 23:17Z slot) | 14 Oct 23:20 | 56 m 18 s; the run then spent **52 m 57 s in "Recover missing market pages", which FAILED** (00:18 → 01:11) | n/a from the tail | n/a | n/a | n/a | n/a | n/a | n/a |

The brief's 449 is not reproduced on the 16 Oct 22:56Z leg (329); it may belong to the 16 Oct 16:45Z leg (not read). The direction is the same: the late slot carries more non-fresh rows than the morning slot.

## 2. Error class × provider × time (what the logs allow)

**Class A — identity firewall quarantine (82 today, 126 yesterday; 29–38 % of the non-fresh rows).** `[ID-FIREWALL] quarantined N identity-broken outgoing row(s)` followed by `[FW-KEEP] restored N/N from last-good`: the provider returned a row whose identity (name/symbol/listing) failed the FW gate, the row was replaced by yesterday's row, so its stamp did not advance. Examples today: AMBP.US, SMTC.US, LEU.US, MBRX.US, KPLT.US, RPGL.US, JBDI.US, RNG.US, IBO.US, FORM.US, NNNN.US, HPE.US, NCEL.US, CISS.US, 3968.HK; yesterday: FTFT.US, BBWI.US, SMTC.US, 000660.KS, ASX.US, DWSN.US, EQX.US, DFH.US, 6754.T, P.US, 7729.T, DELL.US, INFY.US, SBSW.US, ANET.US. SMTC.US repeats. Provider: the GM leg is served by the backend (`/v1/analysis/sheet-rows`, 268 batch POSTs, all HTTP 200); the firewall sees the *backend's* merged row, so the provider behind each quarantine is in the row telemetry, not the job log.

**Class B — fetch-missed, preserved (5 / 7).** `[SYMBOL-PERSISTENCE] preserved N last-good row(s)`: the backend returned no row for the symbol; TRTN.US repeats on both days (delisted-class candidate).

**Class C — the long tail (≈ 198 today, ≈ 196 yesterday = non-fresh − A − B).** Not named in any WARNING line. The matching population is the "198 repeatedly fetched unavailable symbols" the WP-05 review counted on 10 Oct: symbols the backend answers with an unchanged stale quote (no new bar), so the stamp does not move. These are the rows the retire-tab control (`TFB_SYNC_RETIRE_TAB`, mapped, off) was built to list.

**Time clustering.** The morning leg runs 268 batches at a median 10.2 s gap with no gap above 83 s; the 22:56Z leg has the same 268 batches but a **258 s maximum gap and only 5 batches in its first 10 minutes** (vs 33), i.e. the backend/provider path is slower late in the UTC day. The duration difference (3,516 s vs 3,233 s) is entirely in those stalls. The 14 Oct 23:17Z run's extra hour is the **recovery job**, not the leg: "Recover only pages that did not refresh" ran 53 minutes and failed — that is the "4h37m-class" duration, and it is a second full pass over the pages the verdict called not fresh.

## 3. Conclusion

1. The non-fresh count is **dominated by the identity firewall (A) and the unavailable-symbol tail (C)**, not by provider failures or quota; both are **data-shape problems with a stable population** (SMTC.US, TRTN.US repeat).
2. The late-slot excess (329 vs 285; 126 vs 82 quarantines) coincides with the slow backend window (258 s stalls) — a stale provider cache served into the firewall, consistent with the brief's time-clustering hypothesis, but the lever is the provider freshness, not the slot.
3. The recovery job on the 23:17Z slot doubles the run time and fails: it re-fetches the same tail and gets the same answer.

## 4. Recommended next steps (not built here)

- Read `sync_row_telemetry_*.jsonl` from one artifact (job 117354201040) to produce the real error-class × provider table (the brief's deliverable); this session could not download it.
- Arm `TFB_SYNC_RETIRE_TAB=observe` (Actions Variable, Emad) so the class-C tail is listed in `_Retired_Symbols` with its last-good age; retire by list on a GO (D3 is the same decision for nameless rows, G1).
- Keep the firewall; add the quarantined-symbol set to the Dashboard_Audit scope so the 82/126 are visible on the page, not only in the log.
- Move the recovery job behind the page verdict (`fresh_pct >= 95` already passes) so a passing leg does not trigger a 53-minute second pass.
