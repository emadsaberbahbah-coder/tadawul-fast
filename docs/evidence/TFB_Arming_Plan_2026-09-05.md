# TFB ARMING PLAN — builds of 2026-09-04 (engine 5.136.0 / 5.137.0, builder 1.19.3, acceptance 1.0.6, normalize 5.5.0)

Rules applied: one ENV arming per evidence run, max two runs per day; every arming has a kill-switch; MECHANISM verdicts are valid on manual `workflow_dispatch` / cockpit refresh, DISTRIBUTION baselines only on scheduled runs; values must be typed exactly (IR-020: an unparseable value collapses silently to OFF). Emad applies every ENV in the Render dashboard (Web service `tadawul-fast-bridge`) unless marked "GitHub Variable".

Read-back instrument for every step: `tfb_acceptance v1.0.6` (06:45Z run, or `python scripts/tfb_acceptance.py --export-dir <exports>` on a fresh export) — rows A1..A4 — plus the item-specific line below.

| Run | When | ENV (exact) | Mechanism read-back (PASS condition) | Kill |
|---|---|---|---|---|
| 0 (inert) | tonight, with the deploy already triggered by the 5 commits | `TFB_ENGINE_TARGET_KLG_REDIS=1` · `TFB_ENGINE_TARGET_KLG_TTL_H=168` (prereq: `REDIS_URL` present) | boot banner / health: `engine_target_klg_redis: true`, `tgt_lkg_redis_state: idle` (or `nourl` if the URL is missing — fix before Run 4). No decision impact: master gate still OFF | unset both |
| 1 | Sat 09-05, first run | `TFB_T10_VENUE_ALLOWLIST=US,SR,T,HK,L,PA,AS,BR,DE,MI,MC,LS,VI,SW,TO,AX,OL,SI,MX` | cockpit refresh: board seats only mapped venues; audit first-fail "Eligibility (Venue)" on `.NS/.BA/.AT/.JK`; **A1 → PASS**. A board with 0 executable tickets is the truthful outcome at 3,825 SAR cash | unset |
| 2 | Sat 09-05, second run | `TFB_OPP_FX_SANITY=1` — and fix the `_Lists_Config` → `TFB_FX_LOOKUP` USD cell to the peg (config, same run) | audit rows Price SAR / Price = 3.75; PD SAR values fall ≈1.5%; **A2a, A2b → PASS** | `0` |
| 3 | Sun 09-06, first run | `TFB_SYM_BARE_ROOT_EQUITY=1` + GitHub Variable `TFB_SYNC_FORCE_REFETCH_SYMBOLS=KE.US,NG.US,SI.US,PL.US,HG.US,LINK.US` for ONE sync (then clear it) | the six names resolve to issuers, sectors fill; **A3-Global_Markets → PASS**; `identity_quarantined` / `name_from_chart_meta` no longer carry contract names | `0` (Render restart needed: import-time flag) |
| 4 | Sun 09-06, second run — the R-6 arming, graduated | `TFB_ENGINE_TARGET_KLG=1` (K-L-G) + `TFB_OPP_HELD_TARGET_NO_NEW_MONEY=1` (inert without tags → one mechanism) | rows whose target leg failed keep `forecast_source=provider_target` with `analyst_lkg:<age>h`; banner `tgt_lkg_redis_stats.writes` > 0 then `hits` > 0 on later syncs; audit deferral "Held target (analyst_lkg:…) — no new money" on carried rows; GM↔MP forecast-source split for the same symbol shrinks. First day observation-only vs a Top_10 dry-run (v5.131.0 standing note). DISTRIBUTION (share of `phase_ii_synthetic` in GM) judged on scheduled runs only, from the following morning | `0` |
| 5 | Mon 09-07, first run | `TFB_ENGINE_CHART_META_CLASS_GUARD=1` | expected **silent** once Run 3 is ON (the provider never fetches the contract); any `identity_class_refused:*` tag = the belt caught a path the root fix missed → report it | `0` |

Not in this plan (no code shipped): the GAS PD writer Δ Shares mapping (A4 stays FAIL until the `.gs` fix), cockpit trigger timing (`13_AutoRefresh.gs`), label truth (`16_Decision_Top10.gs`) — all need the GAS sources pasted as `.txt`.

Repo debt still open after tonight: `scripts/Harness r6b v5 136 0·py` and `scripts/Harness ob v1 19 3·py` (delete); four sheets to create under `docs/evidence/`: engine v5.136.0, builder v1.19.3, acceptance v1.0.6, engine v5.137.0 (normalize v5.5.0 is in).

Stop rule for the whole plan: any run whose read-back does not PASS is reverted with its kill-switch before the next arming; disagreement between the acceptance instrument and the cockpit is reported, never averaged.
