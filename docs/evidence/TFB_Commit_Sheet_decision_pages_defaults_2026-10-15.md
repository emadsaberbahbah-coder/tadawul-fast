# TFB Commit Sheet - decision-pages launch defaults (2026-10-15)

opportunity_builder 1.27.0 -> 1.28.0 - portfolio_actions 1.16.0 -> 1.17.0 - data_engine_v2 5.155.0 -> 5.156.0 - daily_sync.yml 1.0.4 -> 1.0.5 - core_repair_readback.yml + verify_core_repair_readback 1.2.1 -> 1.2.2 - verify_deployment 1.0.64 -> 1.0.65 - tests/conftest.py - tests/test_decision_pages_defaults.py (NEW)

Renumbered 2026-10-22 (merge of the re-merged #778 bundle, which now carries main #813/#815/#816): the engine half is **data_engine_v2 5.158.0** (5.157.0 is now the bundle's fallback, renumbered over #816's 5.156.0); portfolio_actions stays 1.17.0 and now sits on #816's 1.16.1 FRAC-QTY fix, so `TFB_PF_RESEARCH_VIEW=off` restores 1.16.1; opportunity_builder stays 1.28.0 on the bundle's 1.27.3. Arming `TFB_OPP_VENUE_CAL_EXT` by default now adds exactly one venue, .SZ (XSHG): #813 put .AT in the default-on map and keeps .NS fail-closed, and the bundle dropped both from its extension. Pins: verify_deployment 1.0.65 (builder 1.28.0, actions 1.17.0, engine 5.158.0), tiny-quotes ENGINE 5.158.0. `tests/test_decision_pages_defaults.py` G1 now loads the pre-flip copies from the bundle head c5133148 (builder 1.27.3, actions 1.16.1, engine 5.157.0). No default changed in the renumbering.

Renumbered 2026-10-27 (merge of the re-merged #778 bundle 0d2f989c, which now carries main #844, #847-#853): main's #848 shipped a different opportunity_builder 1.28.0 (ranking challenger in shadow, `TFB_OPP_RANK_MODE` default legacy) and the bundle's data-quality gates moved to 1.28.1 over it, so the builder half of this PR is **opportunity_builder 1.28.2** (authored as 1.28.0); `TFB_OPP_*=0` restores 1.28.1. portfolio_actions stays 1.17.0 and data_engine_v2 stays 5.160.0. Pins: verify_deployment 1.0.65 (builder 1.28.2). `tests/test_decision_pages_defaults.py` G1 now loads the pre-flip copies from the bundle head 0d2f989c (builder 1.28.1, actions 1.16.1, engine 5.159.0). No default changed in the renumbering.

## S0 - Decision and base
| Item | Value |
|---|---|
| Owner decision | 2026-10-15, in session: "PF research view, FW-2 scope enforce, Top_10 calendars + diversify, .VN currency fix" - all four recommended options selected. The owner merges the PRs himself. |
| Base | `claude/decision-pages-bundle-2026-10-15` @ `8c5b1e54` (PR #778 = main `11ef1345` + #773 + #770 + #777, all default off) |
| Pristine copies | `git show 8c5b1e54:<path>` for the three modules, used by golden G1 (`TFB_TEST_LAUNCH_BASE`) |

## S1 - Evidence (export of 2026-10-15 ~01:00 Riyadh)
- **Portfolio_Decision:** all 4 holdings BLOCK `RECONCILIATION_REQUIRED: missing_evidence`; MV / cost / P&L / stop / TP and every KPI blank. The gate is correct; the page tells the owner nothing about his own book. Live read-only IBKR on 2026-10-14 22:35 UTC: CWBC 100, DDI 229, KRP 130 match the sheet exactly.
- **Top_10:** feed NOT_ACTIONABLE (`coverage_failed,decision_failed`); 4 of 500 pass. 441 stop at Quote Freshness, of which 56 are "venue calendar unavailable". 3 of 4 research seats are Real Estate with Max Per Sector 2.
- **Global_Markets:** FW-2 left 167 symbol-only stubs (AMAT, MRVL, SMCI, UAL, SOFI, ...): names 96.05% < 99%, acquisition 93.71% < 95%.
- **Wrong numbers:** VNM.VN 216,308.99 SAR, PLX.VN 136,035.15, BID.VN 124,620.92 (VND prices labelled NASDAQ/NYSE USD).

## S2 - What changed (only defaults move; every rule is unchanged)
| Site | Before | After | Kill switch |
|---|---|---|---|
| `_env_venue_cal_ext` | unset = off | unset = on | `TFB_OPP_VENUE_CAL_EXT=0` |
| `_env_freshness_alert` | unset = off | unset = on (display only) | `TFB_OPP_FRESHNESS_ALERT=0` |
| `_env_research_diversify` | unset/blank = off | unset/blank = on | `TFB_OPP_RESEARCH_DIVERSIFY=0` |
| `_env_research_view_mode` | unset/blank = off | unset/blank = enforce | `TFB_PF_RESEARCH_VIEW=off` |
| `_suffix_locale_fallback_enabled` | unset = off | unset = on | `TFB_ENGINE_SUFFIX_LOCALE_FALLBACK=0` |
| daily_sync.yml `TFB_SYNC_FW2_SCOPE` (both writer jobs) | fallback `off` | fallback `enforce` | Variable `TFB_SYNC_FW2_SCOPE=off` |
| core_repair_readback.yml declared view | fallback `off` | fallback `enforce` | Variable `TFB_PF_RESEARCH_VIEW_EXPECT=off` |
| verify_core_repair_readback `--expect-pf-research-view` | default `off` | default `enforce` | pass `off` |

`TFB_OPP_AUDIT_KEEP` stays off; it is inert until the 16_Decision_Top10 v1.13.10 paste.

The research view never sizes, funds or unblocks a row. Every row stays BLOCK `; not executable` with zero money, and the readback's enforce checks (amounts only on BLOCK rows tagged `[custody-unverified]`) are unchanged.

## S3 - Tests
- `tests/test_decision_pages_defaults.py` (NEW): PASS 11/11 as a script, 12 passed under pytest. It covers:
  - unset reads armed;
  - MWG.VN reads VND;
  - workflow and readback lockstep;
  - verifier pins and flag defaults;
  - six explicit off spellings disarm every gate;
  - misspelt research view never arms;
  - audit keep stays off;
  - ASCII added lines;
  - G1 golden negative: the same unset environment reads OFF on the 8c5b1e54 copies.
- `tests/conftest.py`: pins the five legacy values with `setdefault`, the same way it pins the auth defaults, so batteries written for the default-off era keep testing that behaviour. A test that deletes or sets a name still controls it.
- Batteries that clear the environment themselves were updated to name the legacy value explicitly; no assertion was weakened.

## S4 - Rollout (owner)
1. Merge #778, then this PR.
2. Render > Manual Deploy (auto-deploy is off; verified 2026-10-14 23:40 UTC, readback 38655521187).
3. Dispatch `core_repair_readback.yml`. Expect the served commit to equal main and `pf_research_view: enforce`.
4. The next scheduled `daily_sync` applies FW-2 enforce (GitHub Actions lane; no deploy). Read back `fw2=enforce` in the `[ID-FIREWALL]` _Run_Log line and the GM `_Status` acquired / data fields.

## S5 - Known limits
- The Top_10 feed stays NOT_ACTIONABLE until three more things happen:
  - the 16_Decision_Top10 cockpit is pasted (installed v1.13.2 lacks the receipts);
  - GM names reach 99% (about 58-97 evidence-backed retirements are still needed);
  - a release is dispatched after a clean audit.
- Portfolio_Decision is certified only after the owner sets `PD_IBKR_EVIDENCE=enforce` and a fresh capture exists.
