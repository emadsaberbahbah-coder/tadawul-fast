# TFB Improvement Register — Update 2026-09-02 (Day 3 of the 10-day program)
Provisional IDs continue from P-33 (09-01). Next free IR number still unconfirmed.

## Closed today (evidenced)
| Item | Evidence |
|---|---|
| Top_10 audit/near-miss ordered by gate depth (builder v1.19.1) | deployed 10:20; 10:22 board shows real gates; harness + 42 tests |
| Valuation sanity firewall at publish boundary (engine v5.135.0) | deployed 11:02; pages rebuilt 11:34–12:34; page effect measured on 09-03 export |
| R/R floor 2.0 → 1.5 (panel) — H-RR-1.5 | probe 0→3; stop = 8% floor so 2.0 ≡ ROI ≥ 16% |
| Require Investable → No (panel) — H-INV-OFF | 11:10 board Passed 110 (was 0) |
| Reliability Cluster gate off + audit 500 (Render) — H-B4B-OFF | 11:10 board; 66% of provider_target rows were cluster-labelled |
| Readback tolerance + ENV-ECHO (sync v6.56.0/v6.56.1) + acceptance v1.0.5 + yml mappings | committed; arming waits on the Variables (P-55) |
| Venue eligibility gate (builder v1.19.2) | committed, inert until `TFB_T10_VENUE_ALLOWLIST` (P-59) |
| Coherence instrument `scripts/tfb_reco_coherence.py` v1.0.0 | baseline JSON: PASS 2 / WARN 2 / FAIL 14 |
| Duplicate cohorts physically dropped (tracker v6.38.0) | acceptance D10-3b duplicate_key=0, overdue 0% |

## Open — new today
| ID | Item | Evidence / note | Phase |
|---|---|---|---|
| P-34 | STALE_PRICE un-discovers names; make it a ticket-stage deferral (funding state QUOTE_STALE) | 3 KSA qualifiers vanished at 10:22 (quote 200m, venue open, max 15m) | 1 / v1.19.3 |
| P-35 | Recommendation ladder vs realized score distribution (raw p50 52 vs BUY ≥ 70) | C1 80.25% SELL-tier, C2 0.45% BUY, C5 p50 52.06 | 2.1 |
| P-36 | Overall Penalty Factor unexplained (p50 0.88; cuts raw ≥ 70 from 202 → 93) | export 09-02 | 2.2 |
| P-37 | SELL-tier reco against engine forecast > +10% | C3 982 rows GM, 35 ML | 2.3 |
| P-38 | Analyst-vs-reco polar clash not surfaced (Conflict Type blank on 4,915) | C4 34.5% GM | 2.4 |
| P-39 | Sukuk 5023.SR scored as equity (Rel 26.2, low_data_coverage) | My_Portfolio | 2.5 |
| P-40 | Decision layer: sell-tier action on a position with Rel ≥ 70 and positive engine forecast requires explicit human override (OTIS EXIT frozen) | Portfolio_Decision 09:08 | 2 / D |
| P-41 | Deployable disagreement: Top_10 3,218 vs Portfolio_Decision 12,020 SAR | both exports 09-02 | 3.3 |
| P-42 | My_Portfolio field contract: Target Price + Analyst Rating blank on all holdings | export | 3.4 |
| P-43 | `_Decision_Diagnostics` 22 days stale (engine 5.127.0, builder 1.10.3 reported PASS) | export | 1.5 |
| P-44 | CFX + MF in the Top_10 pool with 0 targets and 0 rows Rel ≥ 70 (2,927 rows scanned for nothing) | census | 1.4 |
| P-45 | Volatility 30D in two units (ML percent p50 24.16; GM 65% fraction p50 0.69) | census | 1 |
| P-46 | Stop model: volatility term never fires (units); R/R = ROI/8 on the board | builder v1.2.0 note; board R/R reproduces ROI/8 | 4 |
| P-47 | Forecast cap-band saturation: 25/112 qualified at the 35% wall; top ranks ordered by score not forecast | 11:10 board | 2 |
| P-48 | ROI % (TP1) blank on qualified rows while R/R (TP2) is computed (ROI-TRUTH display) | 11:10 board | 3 |
| P-49 | "Shariah (KSA)" / "Shariah (Model)" still in GATE_ORDER — retired by standing rule; confirm inert, remove | builder GATE_ORDER | 1 |
| P-50 | Readback `UNAVAILABLE (read_failed)` stamps SUCCESS — latent false-green (v6.51 rule only demotes DIVERGENT) | run #3849 CFX | 1 |
| P-51 | Percent Change degenerate (p10 −2.31, p50 −0.01, p90 +0.07) — field not computed for most rows | census | 1 |
| P-52 | Pool duplicates bare vs suffixed (OTIS / OTIS.US) survive the 5-symbol dedup | 10:22 near-miss | 1 |
| P-53 | Investability control precedence: the GAS panel key `investability_gate_enabled` overrides Render `TFB_OPP_INVESTABILITY_GATE`; a panel re-seed to "Yes" undid the 07-24 retirement. Document; default re-seed must be No | 10:22 board (57 first-fails) | 1 |
| P-54 | Reliability is a canned label on 73% of GM provider_target rows (76.5/75.4/70.4/71.5); H-28 says non-separating — display "n/a" instead of a fake number? | B4 | 2 |
| P-55 | Fill-guard / tolerance Variables not reaching jobs (07:00 armed enforce; 11:41 and 14:03 off). Root cause unknown; ENV-ECHO now prints the effective state each run | runs #3835, #3849 | 1 |
| P-56 | Node 20 deprecation on actions/checkout@v4 / setup-python@v5 — bump | every run | housekeeping |
| P-57 | EODHD / TFB_TOKEN rotation calendared 09-01 — overdue | calendar | housekeeping |
| P-58 | Operating rule until P-34 ships: refresh the board 08:00–09:59 Riyadh | freshness gate | ops |
| P-59 | Venue allow-list arming needs the IBKR Trading Permissions exchange list (operator) | 11:10 Selected = HDFCBANK.NS | 4 |
| P-60 | Sell-Class is now the top blocker (239) — a consequence of P-35; do not touch the gate | 11:10 board | 2 |

## Claude errors logged today
1. `TFB_OPP_RANK_BY_ENGINE_ROI` called "the single unlock" — it orders the post-gate INVEST list only.
2. "Stale pool" adjudication (09-01) — falsified by the 09-02 full rebuild.
3. Cash drop flagged as unexplained — it was the EPRT purchase.
4. Second-pass claim that the `Require Investable` panel cell is inert — the GAS sends the key the builder reads; the cell is live.

## Hypotheses registered today (before/after boards attached to each)
H-RR-1.5 · H-INV-OFF · H-B4B-OFF (all recommendation-touching; S-1 re-baseline on Day 10 as planned).
