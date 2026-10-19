# Decision pages: menu bar, cash-aware allocation, sell-to-fund rotation — commit sheet & operator guide

<p class="meta">20 Oct 2026 (Riyadh) · One-Pass delivery · three lanes: Apps Script (paste), GitHub (PR, this branch), Render (ENV + manual deploy, owner) · base pins: 27_Decision_Pages_v2 v1.1.0 (sha f6248cb06537), 01_Menu v2.20.2 (sha b391ee1bd756), opportunity_builder 1.27.0 (main f24e9ff, live on Render since 00:11 today)</p>

<div class="verdict"><strong>Bottom line.</strong> Two of your three requirements already exist in the backend but are switched off or invisible: <strong>conviction-weighted sizing</strong> (OB v1.27.0, merged yesterday, default <code>cap</code>) and the <strong>rotation funding plan</strong> (sell the weakest holding to fund a better seat; v1.18.0, on by default but only visible when the feed is released and only on the engine's 12-month basis). What was missing: the menu-bar entry, a research view that shows the allocation and the switch <em>today</em> while the feed is still withheld, and a 3-month basis for the rotation (your "20% vs 14% over 3 months" case). This delivery adds all three: <strong>27_Decision_Pages_v2 v1.2.0</strong> + <strong>01_Menu v2.20.3</strong> (paste), <strong>opportunity_builder v1.27.1</strong> (PR, default off), and the exact Render ENV values to arm the production behaviour.</div>

## 1. Your requirements → where each one lives now

| # | Requirement (your words) | Status before | What this delivery does |
|---|---|---|---|
| 1 | The two v2 sheets in the menu bar for manual refresh | Items existed only inside 🧭 Today (v2.20.2 hook) | New top-level menu **📊 Decision v2**: Refresh both / Refresh Portfolio_Decision_v2 / Refresh Top_10_Investments_v2 / Open each tab / Dynamic-exit status, test alert, on-off / Install-remove hourly refresh / Seed IBKR / Self-test. One guarded line in 01_Menu onOpen (v2.20.3); the function lives in 27_Decision_Pages_v2 v1.2.0. |
| 2 | Ticket ≈ 4K SAR is not an investment decision; size by opportunity, risk and the cash in the box | The v2 page showed a flat 5 %-of-NAV *reference*; the engine's real sizing (`TFB_OPP_SIZING_MODE=conviction`, OB v1.27.0) is default **off** and sizes only when the feed is released | Top_10_Investments_v2 now runs a **research allocation that mirrors the engine's conviction sizing**: weight = plan ROI × risk multiplier (Low 1.0 / Medium 0.7 / High 0.4) × reliability (0.5–1.0), water-filled against the **deployable cash** (cash − 10 % NAV floor) with a **per-seat cap of 15 % of (portfolio + deployable)** and a 2,000 SAR minimum ticket; only FAST-TRACK plans are sized. New ALLOCATION block: deployable, allocated, cash after, expected 3-month gain, risk if every initial stop is hit, reward per riyal at risk. **Production:** arm the same rule in the engine with the ENV in §3. |
| 3 | No cash → compare holdings with the best opportunity and tell me to sell X and buy Y when the expected ROI is better | Engine rotation plan exists (one rotation per run, 8 pp hurdle on the **12-month** forecast after 1.1 % cost) but is invisible while the feed is withheld, and the 3-month basis you reason on did not exist | Both v2 pages now carry **FUNDING PLANS & ROTATION**: for each unfunded plan, FUNDABLE NOW / FUNDABLE BY ROTATION (trim or exit the held name with the lowest forward ROI that trails the seat by ≥ 8 pp on the 12M basis — and, as a second line, ≥ 5 pp on the 3M basis) / CAPITAL CALL. Portfolio_Decision_v2 adds a **SWITCH** action, a limits row and a 'SWITCH candidate' tag in the advisory e-mail. **Production:** OB v1.27.1 adds `TFB_OPP_ROTATION_BASIS=plan3m` (default `12m`, byte-identical) so the engine's own funding plan can use the 3-month plan ROI with its own hurdle `TFB_OPP_ROTATION_EDGE_PP_3M` (default 5). |

Why today's page says "no rotation needed": you hold 53,769 SAR of cash. Deployable 44,458 SAR covers all four plans (XIOR.BR, ATAT.US, CRC.US, RDN.US) at 9–12.5 K each. The rotation only speaks when cash cannot cover a plan — exactly your requirement. The harness scenario with 3,000 SAR of cash produced: *"XIOR.BR — FUNDABLE BY ROTATION: trim CWBC.US (5,901 SAR) → ticket 5,901 SAR · engine 12M: XIOR.BR 34.6 % vs CWBC.US 15.3 % → edge +18.2 pp after 1.1 % cost"* and the 3M line *"plan 3M: 17.3 % vs 6.4 % → edge +9.8 pp"*.

## 2. The rules, in one table

| Element | Rule (research view = engine mirror) | Engine parameter |
|---|---|---|
| Deployable | cash − 10 % of NAV (floor) | `TFB_OPP_CASH_FLOOR_SAR` + `TFB_OPP_CASH_FLOOR_MODE=enforce` (absolute floor, already enforced); `TFB_OPP_CASH_FLOOR_PCT` (percent, optional) |
| Weight | plan ROI % × risk mult × reliability/100 (clamped 0.5–1), tilt 1.0 | `TFB_OPP_SIZING_MODE=conviction`, `TFB_OPP_SIZING_TILT` (0 = equal split … 3 = aggressive) |
| Per-seat cap | 15 % × (portfolio value + deployable) | `TFB_OPP_MAX_WEIGHT_PCT` (15) |
| Minimum ticket | 2,000 SAR (research); seats whose share falls below it are released and the cash re-spread | `TFB_OPP_MIN_TICKET_SAR` (production default 0 — set it, see §3) |
| Sizing cohort | FAST-TRACK plans in rank order, ≤ 2 per sector, ≤ 10 per market | panel `T10: Max Per Sector / Market` |
| Rotation eligibility | held ≥ 7 d; not within 3 % of its TP1; sukuk / outside scope excluded; value ≥ minimum ticket | `TFB_OPP_ROTATION_MIN_HELD_DAYS` (7), `TFB_OPP_ROTATION_TP1_PROXIMITY_PCT` (3), `TFB_OPP_ROTATION_EXCLUDE` (5023.SR) |
| Rotation test | opportunity ROI − holding forward ROI − 1.1 % round-trip cost ≥ hurdle; pick the holding with the **lowest** forward ROI; one rotation per run | `TFB_OPP_ROTATION_COST_PCT` (1.1), `TFB_OPP_ROTATION_EDGE_PP` (8, 12M), **new** `TFB_OPP_ROTATION_BASIS=plan3m` + `TFB_OPP_ROTATION_EDGE_PP_3M` (5) |
| Forward ROI of a holding | 12M: engine forecast (My_Portfolio *Expected ROI 12M*). 3M: the engine uses the plan-TP1 ROI (TP1 − price)/price from the pool row (`_tp1_plan_roi`); the research page approximates it with My_Portfolio *Expected ROI 3M* because the held names' TP1 ladder is not on the sheet while the feed is withheld | — |
| Exits (unchanged, v1.1.0) | 2 × ATR14 initial stop, breakeven lock after +1R, trail 2 × ATR (1.5 × after +2R), SMA10 profit protection, e-mail alerts | `DP2X` in the script |

The 12M basis is the engine's forecast horizon; the 3M basis is the plan horizon your Top 10 is built on (Required ROI 12 % per 3 months). The page shows both so the two never get confused; your example (20 % vs 14 %) gives 4.9 pp after cost — below the 5 pp default on the 3M basis, so set `TFB_OPP_ROTATION_EDGE_PP_3M=4` if you want that pair to rotate. Below ~4 pp the cost, slippage and forecast error eat the edge; I would not go lower.

## 3. Render — the ENV that turns the production behaviour on (your step)

| Variable | Set to | Effect | Default today |
|---|---|---|---|
| `TFB_OPP_SIZING_MODE` | `conviction` | Engine sizes seats by ROI × risk × reliability instead of "first seats at the cap, the rest with scraps" | `cap` |
| `TFB_OPP_SIZING_TILT` | `1` (optional) | 1 = proportional; 0.5 = flatter; 2 = concentrated | 1 |
| `TFB_OPP_MIN_TICKET_SAR` | `2000` | Needed for the rotation plan to have a "need" amount; sub-floor seats are deferred with a reason instead of 1–2-share tickets | 0 |
| `TFB_OPP_ROTATION_BASIS` | `plan3m` (after the PR is merged and deployed) | Rotation compares 3-month plan ROI; text and alert say "plan 3M" | `12m` |
| `TFB_OPP_ROTATION_EDGE_PP_3M` | `5` (or `4`) | Hurdle on the 3M basis | 5 |
| `TFB_OPP_ROTATION_EDGE_PP` | leave `8` | Hurdle on the 12M basis (used when basis = 12m) | 8 |
| `TFB_OPP_CASH_FLOOR_SAR` / `_MODE` | keep as set | Your absolute cash buffer, already enforced (#799) | enforced |

Render: Dashboard ▸ tadawul-fast-bridge ▸ Environment ▸ add/edit ▸ Save, then **Manual Deploy ▸ Deploy latest commit** (autoDeploy is off). Order: merge the PR first if you want `plan3m`; the other variables work on the code that is live now (f24e9ff carries v1.27.0). Nothing changes until the decision feed is released (the scheduled Release workflow, armed by `vars.TFB_DECISION_RELEASE=1`); until then the v2 pages are the place where you see the same rule applied to today's numbers.

## 4. Apps Script — paste steps (10 minutes)

| Step | Action | Expect |
|---|---|---|
| 1 | `27_Decision_Pages_v2.gs` → select all → paste the attached v1.2.0 (2,040 lines, sha `3ef2837b0019`) → Ctrl+S | Header `Version: 1.2.0` |
| 2 | `01_Menu.gs` → select all → paste the attached v2.20.3 (2,078 lines, sha `38bede3442cd`) → Ctrl+S | `var SCRIPT_MENU_VERSION = '2.20.3'` |
| 3 | Reload the spreadsheet (F5) | A third menu **📊 Decision v2** appears next to **⏸️ TFB Sync Hold**; 🧭 Today keeps its v2 items |
| 4 | 📊 Decision v2 ▸ 🧪 Self-Test | PASS lines incl. `research allocation: water-fill {"A":45,"B":27.5,"C":27.5} · rotation pick CWBC.US +18.2pp · no pick when the holding is better` |
| 5 | 📊 Decision v2 ▸ 🔄 Refresh both v2 pages | Top_10_Investments_v2: SEATS with **Alloc SAR / Buy qty**, the ALLOCATION block and FUNDING PLANS & ROTATION; Portfolio_Decision_v2: Rotation candidate row in LIMITS, SWITCH action when applicable |
| 6 | Tests ▸ 🧪 Self-Check: Menu Wiring | `27_Decision_Pages_v2 menu bar (dp2_addTopMenu)` present |
| rollback | paste v1.1.0 / v2.20.2 back | — |

PR #801 (another session, open) proposes a 550-line generic menu block with a set-up dialog that asks you to pick the renderer functions by number. It was written without access to this renderer; the native hook above does the same job in 3 lines with the real function names, so my recommendation is to install this version and not the block (the PR's second part, repair_stores v1.0.2, is independent and fine).

## 5. GitHub — PR contents (branch `claude/rotation-basis-plan3m-20261020`)

| File | Change |
|---|---|
| `core/analysis/opportunity_builder.py` | v1.27.1: `_env_rotation_basis`, `_rotation_basis_label`; basis-aware `_env_rotation_edge_pp`, `_holding_roi_map`, ordered needs; label in `_funding_plan_text`, the `rotation_proposal` alert and additive `kpis.rotation_basis`. Default `12m` = v1.27.0 byte-identical. On `plan3m` both sides use the plan-TP1 ROI `_tp1_plan_roi(cand)` = (TP1 − price)/price — the "ROI % (TP1)" column — never `cand["roi_pct"]`, which is the valuation/TP2 upside (Codex P1 finding on the first push, fixed in the second). |
| `scripts/verify_deployment.py` | pin 1.27.0 → 1.27.1 |
| `tests/test_opp_rotation_basis_v1271.py` | T1–T6: default/invalid → 12m; plan3m map/hurdle/label on normalized-candidate shapes whose valuation `roi_pct` deliberately differs from the TP1 figure; a held name without a TP1 ladder never rotates on 3M; the owner's 20-vs-14 case (no rotation at 5 pp, rotates at 4); the live XIOR/CWBC pair on both bases (+18.2 pp 12M, +9.8 pp 3M); degraded-forecast guard; text/alert labels; manifest pin. 138 tests pass across the builder/funding/venue suites. |
| `apps_script/27_Decision_Pages_v2.gs` | first repository mirror of the v2 renderer (v1.2.0) |
| `apps_script/01_Menu.gs` | first repository mirror of the live menu (v2.20.3) |
| `docs/evidence/…_2026-10-20.md` | this sheet |

Harness evidence: `node --check` on both .gs files; ES5 scan clean; 20 unit checks (exit rule, water-fill, rotation pick, signal precedence, keys, menus); end-to-end mock build of both pages over your 19-Oct workbook export (current cash → all plans funded, no rotation; 3,000 SAR scenario → TRIM CWBC → XIOR on both bases; cooldown and cache paths re-verified).

## 6. Principles worth adding — what is in, what I propose next

Adopted in this delivery (all are standard portfolio-construction practice, none is a black box):

1. **Expected value per riyal at risk.** Each seat shows expected 3-month gain and the loss if its initial stop is hit; the page totals both and prints EV/risk. Below ~1.5× the board is paying too much risk for its return.
2. **Conviction sizing with hard caps** (ROI × risk × reliability, 15 % seat cap, 10 % cash floor, minimum ticket) — size follows the opportunity, never a flat number.
3. **Opportunity-cost rule with a hurdle** — replace a holding only when the alternative beats it by more than costs plus a margin for forecast error; one switch per run so the book is not churned.
4. **Volatility-scaled exits** (v1.1.0) — the stop and the profit-protection rule are in ATR units, so every name is held to the same risk language.

Proposed next (each is a small, default-off PR — say which you want):

5. **Probability-weighted ROI.** Your calibration record (`TFB Calibration`: INVESTABLE hit rate 0.654, 961 matured records) can haircut every plan ROI: EV = p × ROI − (1 − p) × stop distance. That turns "17 % plan" into "≈ +8 % expected" and makes sizing and rotation compare like with like.
6. **Risk-budget sizing.** Cap each seat so that qty × (entry − 2 × ATR) ≤ 1 % of NAV, and take the smaller of the conviction target and the risk cap. Prevents a volatile name from carrying the same riyals as a quiet one.
7. **Correlation heat cap** — `core/risk_limits.py` already computes an effective heat with sector/country/theme correlation proxies; wire its verdict into the funding plan so three REITs do not pass as three independent bets.
8. **Rotation on remaining upside, net of tax/fees per venue** — the venue cost table (`rt_cost_pct`) exists; use it per pair instead of the flat 1.1 %.
9. **Rebalance bands** — trim a winner back to the cap only when it exceeds it by a band (e.g. 15 % → 18 %), to avoid round-trips on noise.
10. **Switch audit** — log every proposed switch with the two forward ROIs and, 3 months later, the realised difference (the Regret_Ledger tab already exists for this); it is the only way to know whether the hurdle is set right.

## 7. Files

| File | Purpose |
|---|---|
| `27_Decision_Pages_v2.gs` v1.2.0 (sha `3ef2837b0019`) | paste |
| `01_Menu.gs` v2.20.3 (sha `38bede3442cd`) | paste |
| PR `claude/rotation-basis-plan3m-20261020` | merge, then Render manual deploy |
| this sheet (.md in the repo and the project, .pdf) | guide |
