# TFB Commit Sheet — core/analysis/opportunity_builder.py v1.21.0 [PRICE-XCHECK]

**Date:** 2026-09-20 (Sunday, Riyadh) · **Item:** accuracy gap 1 — second-source price verification for the Top-10 seats (register candidate; Emad's GO: "go with the best option that shaping the project and improve the accuracy") · **Engineer:** Claude · **Protocol:** One-Pass Script Protocol S0→S6.

## S0 — Freeze inventory

| File | Role | Pinned at HEAD |
|---|---|---|
| core/analysis/opportunity_builder.py | target (ticket sizing, seats, alerts, meta) | v1.20.0, 5,666 lines, sha256 `5f14a9be07e8e3de9c9739ec6f50a0af9578e971f570e43a8e76710bf317b0cb` |
| core/symbols/normalize.py | canonical symbol mapper (`to_eodhd_symbol`) — reused, untouched | 1,607 lines |
| core/providers/eodhd_provider.py | key/base env resolution + `_EODHD_SUFFIX_CANONICAL` — mirrored, untouched | v4.18.0, sha `efbd7c14…` |
| tests/test_ob_f1b_plan_basis.py | harness conventions reused (`make_criteria`, `build_opportunity_payload`, row shape) | sha `08adf8fa…` |

## S1 — Live-fetch and pin

- Repo `emadsaberbahbah-coder/tadawul-fast`, branch `main`, HEAD `aeb4f2a102d97adc42586d533e58f7391fa9da3f` (tree page `currentOid`, 2026-09-20; GitHub REST rate-limited this session).
- Dual-ref raw fetch (branch `main` and commit `aeb4f2a…`) byte-identical for all six files read; base = the v1.20.0 delivery of 2026-09-19 exactly (zero drift).

## S2 — Design (gate `TFB_T10_PRICE_XCHECK`, read per call, no restart)

| Mode | Behaviour |
|---|---|
| `off` (default/unset/other) | Byte-identical v1.20.0: no network call, no note, no alert, no meta key (measured: payload diff paths = 2, both the version string). |
| `observe` | For each candidate reaching SIZING in `_select_and_size` (≤ max_selected seats + xcheck re-tries; never the audit grid): ONE EODHD `/real-time` quote, compared with `cand.price`. Verdicts: `verified` (\|Δ\| ≤ tol), `diverge`, `single_source` (no/failed quote), `budget` (past MAX_FETCH / BUDGET_S), `skipped` (no primary price). Selection, sizing, funding, gates, KPIs untouched. Read-back: `[price-xcheck observe] …` on every ticket note, `detail.price_xcheck`, ONE countable `price_xcheck` alert, `meta.price_xcheck`. |
| `enforce` | As observe, plus a `diverge` seat is DEFERRED before sizing (`PRICE_XCHECK DIVERGE Δ… — sizing deferred`, near-miss gate "Price Verification") and the seat passes to the next candidate. `TFB_T10_PRICE_XCHECK_STRICT=1` also defers `single_source`/`budget` (fail-closed; a total outage = 0-seat board by design). Default STRICT=0 = fail-open, tagged. |

Symbol mapping = `core.symbols.normalize.to_eodhd_symbol` → provider alias table mirrored verbatim (`.L→.LSE`, `.XETR/.ETR→.XETRA`, `.TASE→.TA`); FX/futures/crypto shapes identity. Network: direct synchronous httpx (urllib fallback), per-call timeout, per-build cache by symbol, per-build time budget, every exception → `single_source`. The async provider client is deliberately not entered from this sync path (P-110 loop class). Harness seam: `_XCHECK_FETCH_OVERRIDE`.

## S3 — Build (anchored edits, every replacement asserted `count == 1`)

| # | Site | Change |
|---|---|---|
| E1 | imports | `import time` (additive) |
| E2 | header | v1.21.0 WHY block + `OPPORTUNITY_BUILDER_VERSION = "1.21.0"` |
| E3 | after `_env_freshness_fallback_h` | helper block: `_env_xcheck_*` ×6, `_xcheck_eodhd_symbol`, `_xcheck_parse_quote`, `_xcheck_fetch_eodhd`, `_xcheck_reset`, `_price_xcheck`, `_xcheck_should_defer`, `_xcheck_fmt_px`, `_xcheck_summary_text`; state `_XCHECK_STATE`, `_XCHECK_CACHE`, `_XCHECK_FETCH_OVERRIDE`, `_XCHECK_SUFFIX_CANONICAL` |
| E4a/b | `_select_and_size` | per-build context after `picked, deferrals = [], {}`; the check immediately before `_size_one(...)` |
| E5a/b | `_build_ticket` | note append before `ticket = {`; `detail.price_xcheck` before `return ticket` |
| E6 | `_near_miss_rows` | `elif "PRICE_XCHECK" in _reason:` → gate "Price Verification" (same class as the v1.0.15 floor / v1.0.17 duplicate label fixes) |
| E7 | alerts | `price_xcheck` alert before `# 4) audit grid sorted by score` |
| E8 | meta | `meta["price_xcheck"]` only when armed (kept off byte-identical) |

Delivered: **6,048 lines**, sha256 `9ad67f5fd2be2f2bd36a3e7303219d27e90ae2b6bbf601266dade48e4965c9d2`. `py_compile` PASS. AST defs 150 → 166 (**+16: 14 named + 2 nested helpers, 0 removed**). Smart quotes 0. Net-new non-ASCII outside comments 0.

## S4 — Internal audits ×3

**Repo harness** `tests/test_ob_price_xcheck.py` (283 lines, sha256 `63567ed3a309b36fcf687f25e0ef33ea4a389bd487c5fe88f24dec84c7f12309`) — REAL module, recorded EODHD payload shapes through the real parser via the override seam: T1 helpers/mapping/parser · T2 off identity · T3 observe (selection/KPIs/deferrals == off; notes + detail + one alert + meta) · T4 enforce fail-open (divergent seat deferred, "Price Verification" near-miss, seat passed to the next name, single-source funded) · T5 enforce STRICT (single-source deferred) · T6 fail-open + MAX_FETCH/BUDGET_S caps · T7 idempotence · T8 wiring (one site each). **ALL PASS ×3.**

**Dual-tree replay on the REAL 2026-09-20 export** (`replay_dualtree.py`, 4 pages, 9,791 rows, frozen clock 2026-09-20T06:00Z in both trees, panel-mirroring criteria, cash 24,763.73):

| Run | Result |
|---|---|
| R1 off: base v1.20.0 vs delivered | payload diff paths = **2** (`version`, `meta.versions.opportunity_builder`); identical modulo those strings; off digest `c146fe7e33e6be5b` ×3 |
| R2 observe, second source = sheet price | selection / KPIs / deferrals == off; 10/10 tickets tagged; meta verified 10, diverge 0; alert text as designed |
| R3 observe, total outage (fetcher raises) | selection == off; single_source 10; builder never raised |
| R4 enforce, one seat −3.00% | victim deferred with `PRICE_XCHECK DIVERGE Δ-3.00%: eodhd 1.04 vs sheet 1.07 (tol 1.0%) — sizing deferred`; seat passed to the next candidate (8 seats kept); fetched 11 |
| R5 enforce STRICT, total outage | 0 seats; 15 single_source + 115 budget deferrals (fail-closed by design) |
| Digest of all five payloads | `bd043d3ef6d36860` **identical ×3**; each build 4.1–4.9 s locally |

Harness discoveries kept: (1) `_fmt_num` renders prices at 1 dp — a 0.03 divergence would have printed as "54.1 vs 54.1"; the note now uses 2 dp (4 dp below 1.0). (2) `elapsed_s` must be rounded for deterministic meta. (3) The near-miss table is capped by `near_miss_n` under depth order, so an enforce deferral is always visible in the audit-grid Deferral column and reaches NEAR MISS by the same rule as sector-cap deferrals. (4) Without `exchange_calendars` locally the freshness gate falls back to 78 h in both trees (logged; irrelevant to the diff).

## S5 — Delivery (full files, convention paths)

- `core/analysis/opportunity_builder.py` (v1.21.0)
- `tests/test_ob_price_xcheck.py`
- `docs/evidence/TFB_Commit_Sheet_opportunity_builder_v1.21.0_2026-09-20.md` (this sheet)

## ENV (Emad applies; Render lane; one ENV per evidence run)

| Var | Default | Purpose |
|---|---|---|
| `TFB_T10_PRICE_XCHECK` | off | **the arming**: observe → enforce |
| `TFB_T10_PRICE_XCHECK_TOL_PCT` | 1.0 | divergence tolerance (%) |
| `TFB_T10_PRICE_XCHECK_STRICT` | 0 | enforce: 1 = single-source/budget also defer |
| `TFB_T10_PRICE_XCHECK_MAX_FETCH` | 15 | quotes per build |
| `TFB_T10_PRICE_XCHECK_TIMEOUT_S` | 4.0 | per quote |
| `TFB_T10_PRICE_XCHECK_BUDGET_S` | 20.0 | per build |
| `EODHD_API_KEY` / `EODHD_BASE_URL` | existing | already on Render; nothing new to add |

Deploy is behaviour-identical (gate unset). Quota: ≤ 15 EODHD calls per board build (~6 builds/day) vs 400k/day.

## S6 — Observe cycle and read-back

Arming = `TFB_T10_PRICE_XCHECK=observe` on Render (today's one ENV change if the deploy lands today). Positive read-back on the next Top_10 run: every seat's Advisor Note carries `[price-xcheck observe] …`, ALERTS shows one `price_xcheck` row with counts, no ticket/KPI/selection change vs the prior run. Expect intraday `.SR` deltas (EODHD real-time delay vs the sheet quote) — the observe week calibrates `TOL_PCT` before any enforce sitting. Enforce flip = separate sitting after ≥ 3 clean observe boards.

## Rollback

Unset `TFB_T10_PRICE_XCHECK` (no deploy) or `git revert`.

## Deliberate cuts

Holdings (Portfolio_Decision / portfolio_actions) not touched; the W-2 freshness gate untouched (age ≠ value); no second-source price written back (disclosure + deferral only); no async provider client entry; no GAS change (the note, alert and deferral render through existing columns).
