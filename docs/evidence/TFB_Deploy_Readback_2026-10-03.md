# TFB Deploy Read-back — 2026-10-03 16:22 Riyadh (Render `tadawul-fast-bridge`)

Evidence: Render deploy log (build 13:21–13:22Z, boot 13:22:49–13:22:51Z) and `/health` at 13:24:05Z, both supplied by Emad. Compared against the 10-01 deploy read-back and Delivery Manifest v2.

## Verdict: Render PASS — nothing in the engine changed, as expected for an operator-tool commit

| Check | Value | vs 10-01 read-back |
|---|---|---|
| Build | pip install clean, "No broken requirements found", build 13:22:14Z, upload 5.1 s | PASS |
| Boot | `start_web.sh v2.6.1`, gunicorn 23.0.0, 2 workers (pids 114/116), timeout 180 / graceful 45 / keepalive 25, Python 3.11.9; startup complete in 2 s | same |
| Commit | **`a8f8b1e4a295dfc8f797faeb7a118d31132e6a4e`** on `main` (new HEAD; 10-01 read-back was `eedcd4d`) | NEW |
| Service | `srv-d4hnir15pdvs739bqe1g`, instance `…-5cfdb7cdfc-mm25l`, env production, `global_auth_enforcement: true` | same |
| Routes | mounted 6 / duplicate 0 / failed 0; signatures 109; live 100; `missing_required_keys []`; `canonical_path_owner_mismatches {}` | same |
| Versions bound | engine **5.151.0**, entry/service **8.14.0**, route advanced_analysis **4.16.0**, opportunity_builder **1.23.0**, portfolio_actions **1.14.0**, trend_signals 1.0.0, sai 1.4.1 | identical (manifest v2 ✓) |
| `startup_warnings` | `[]` | PASS |
| Engine guards | identity on · price_coh on · pe_coh on · ohlc_coh on · ohlc_final on (ohlc_mode **observe**) · fund_identity on · snapshot_refusal on · final_action_invariant on · fund_lkg on · **fund_unit_sentry enforce** · **fund_cache enforce** · w52_ceiling on · scoring_settle observe · **fc_tuple observe** · **crypto_pair_class observe** · rel_path_tag observe · **margin_publish off** · surface_warn_invest OFF · surface_row_sanity OFF · mp_blocked_nulls OFF | unchanged |
| PF gates | `TFB_PF_CONFIRM_SESSION` observe · `TFB_FORECAST_BASIS` observe · `TFB_PF_DD_EXIT` observe (8 %) · identity gate 1 · block-missing-cost-basis 1 · switch scan 1 · VF conflict guard 1 · cash floor 10 % | unchanged |
| Not reported by `/health` | `TFB_T10_W52_TIMING`, `TFB_PF_ADD_LOSER_VETO` (A1/A2) — the key list in main.py is fixed | still open (register item 7) |
| Caches | `fund_cache_stats` all 0, Redis LKG states `idle` | **L1 fund cache wiped by the deploy** (known cost of every deploy) |
| Health probe | Render probed `HEAD /` → 200 (root alias) | health-check path still empty; set `/health` (A4) |
| Cosmetic | `app_version` constant reads **5.111.0** while the engine is 5.151.0 | stale constant; nothing keys off it as far as the read-back shows |

## What the read-back does NOT prove
The Render side cannot show whether the four delivered files landed at the intended paths with the intended bytes. GitHub-side read-back owed: on the repo checkout run `python scripts/tfb_export_audit.py --selftest` → must print `46/46 PASS cases-digest=3de969895c97a7d6`; and `sha256sum scripts/tfb_export_audit.py tests/test_export_audit_v1.py` → `eabdd9a6…` / `15822e74…`.

## Two leads from the gate list (change the script register)
1. **`fc_tuple_coherent: observe`** — the engine already carries a forecast-tuple coherence guard. The 35 forecast/ROI pair mismatches (P-158, register item 2) may be an **arming** (`observe → enforce`) rather than a code change. Before any build: pull the `[FC-TUPLE …]` observe lines from the Render log for one sync run and confirm they name the same 35 symbols (1030.SR, 1288.HK, 3328.HK, ACB.VN, ALKEM.NS …). If they do, item 2 becomes A-list arming, one ENV change, with the export audit as the read-back (`forecast_roi_pairs_total` must drop to 0).
2. **`crypto_pair_class: observe`** — a crypto-pair classifier exists in observe. It may route `-USD` pairs to the right provider class but it did not stop 7 wrong-instrument names (SUI, GRT, APT, UNI, IMX, STX, ARB). Read its observe lines too; if the classifier only tags and never validates the name, item 1 (P-192) stays a code change in `identity_guard.py` / the provider name pin, but its scope narrows to the name check.

## Deploy hygiene
The 10-02 reconciliation recorded Render Auto-Deploy **OFF** (A3). This deploy happened within minutes of a commit that contains only an operator script, a test and two docs — either Auto-Deploy is back on, or it was a manual deploy. Either way the deploy wiped the L1 fund cache, so the next sync re-fetches fundamentals against an EODHD counter that was over 90k on 6 of the last 7 days. Recommendation (one Render setting): add a **build filter** with ignored paths `docs/**`, `tests/**`, `scripts/tfb_export_audit.py` so commits that do not touch the service never redeploy it; keep Auto-Deploy as decided on 10-02.

## Register moves
- HEAD pin for any further build today: **`a8f8b1e4`** (supersedes `eedcd4d`).
- Item 2 (P-158): reclassified **"arming candidate — verify fc_tuple observe lines first"**.
- Item 1 (P-192): scope note added (crypto_pair_class observe exists).
- Item 7 (`/health` keys) and A4 (health path) confirmed still open by this read-back.
- New INFO: `app_version` constant 5.111.0 stale.
