# Commit sheet - no new buy in a market that is not calm; a share only with an analyst consensus well above its price at low risk (21 Nov 2026)

Branch `feat/selection-and-market-gate`, against main, after #1050 (merged 21 Nov 2026 15:57 UTC). **Default off: with neither switch set, every row is byte-identical to 5.160.0.** Arming is the owner's decision (Render ENV), and so is the deploy.

| File | Version |
|---|---|
| `core/data_engine_v2.py` | 5.160.0 -> **5.161.0** (two DEMOTE-only tiers, `[MARKET-HOLD]` and `[SELECTION]`; six health keys) |
| `core/market_condition.py` | new, **1.0.0** (the live condition of five stock markets; stdlib only) |
| `scripts/verify_deployment.py` | pins: data_engine_v2 5.161.0; core.market_condition 1.0.0 (new) |
| `docs/render_env_contract.md` | regenerated (seven new `engine_gates` rows) |
| `.github/workflows/ci.yml` | 1.1.94 -> **1.1.95** (the two new test files) |
| `tests/test_market_condition.py` | new (M1-M7) |
| `tests/test_de_market_and_selection_gate.py` | new (G1-G10) |
| `tests/test_de_evidence_cohort.py`, `tests/test_eodhd_analyst_target.py`, `tests/test_core_repair_tiny_quotes.py` | engine pin 5.161.0 |
| `tests/test_verify_manifest_pins.py` | the file of the new manifest module |

## Why

The owner wrote on Saturday 21 Nov 2026 at 19:10 Riyadh, quoting the Top_10_Investments_v2 tile:

> "MODEL EVIDENCE 47.60% INVESTABLE hit rate, matured records · since 2026-11-10: 11.1% (n 45) · targets of the last 30 days: 49.8% (n 1,048) need to improve"

His stated targets: above 70 % (10 Nov), 55-60 % at least (9 Nov); 2-3 investable recommendations a day; and (21 Nov) to stay away from market turmoil.

## The evidence

The 2,384 matured INVESTABLE WIN / LOSS `Performance_Log` records the tile reads (recorded 28 Jun - 13 Nov 2026), each joined to the label its market carried at the last close before the record (`v_market_condition`, the market read from the symbol exactly as `core.market_condition.market_of` reads it) and to the analyst target and risk score `Signal_History` recorded for it that day:

| Rule | Records | Names | Won | Matured 11-21 Nov | Symbol-weeks |
|---|---:|---:|---:|---:|---:|
| all INVESTABLE | 2,384 | 219 | 47.6 % | 42.6 % (383) | 47.1 % (444) |
| market calm | 1,150 | 141 | 52.5 % | 58.9 % (146) | 51.5 % (229) |
| analyst upside >= 15 % and risk < 30 | 729 | 45 | 60.8 % | 45.3 % (234) | 53.5 % (113) |
| **both** | **374** | **25** | **69.8 %** | **63.2 % (87)** | **62.9 % (59)** |

- **The analyst target exists from 31 Aug 2026**, when `Signal_History` began recording Target Price. The two rows that need it therefore cover Sep - Nov, where all INVESTABLE records won 45.3 % (1,590) and the calm ones 52.2 % (791): the comparison for "both" is 69.8 % against 45.3 %.
- **A missing target fails.** From 31 Aug, a share with no target won 36.7 % outside Tadawul (60 records) and 20.9 % on it (115), against 51.1 % (1,206) and 28.2 % (209) with one.
- **Under 15 % upside** won 23.7 % in calm markets (114) and 14.0 % in the others (164).
- **Tadawul** shares entered while TASI was in a drawdown won 31.5 % (533, over a fifth of all INVESTABLE records). **Since 10 Nov**, 40 of the 45 records were US shares entered while the US was weak; 4 won.
- **By market** (Sep - Nov, shares passing the selection rule): US calm 68.8 % (320) against not calm 46.5 % (269), and on the records that matured 11-21 Nov 52.8 % (53) against 30.9 % (139); Tadawul calm 1 of 6, not calm 52.0 % (50, 3 names); the other markets 83.3 % calm (48) and 86.1 % not calm (36). **The hold is measured in the US**; elsewhere the samples are small and show no difference. `TFB_MARKET_GATE_MARKETS` narrows the hold if the owner wants more picks from the other markets.
- **Honest expectation: 60-70 % live**, not 70 %: 25 names, one Sep - Nov stretch, and the symbol-week count (62.9 %) is the less clustered reading.
- **The cost is supply.** Under both rules: 3-6 INVESTABLE rows a day in the calm US sessions of October, 0-1 a day since 15 Nov. The rules choose *when* to buy as much as *what*.

## What is built

**`core/market_condition.py` 1.0.0.** The five labels of `sql/bigquery/99_v_market_condition.sql` (crash_day, drawdown, stress, weak, calm; first match wins; a figure not known never matches; figures and limits rounded to 4 decimals; k = 1 in the US, elsewhere the market's normal yearly volatility / 16, none under 120 changes) for the five stock markets whose shares the engine labels:

| Market id | Reading | Symbols |
|---|---|---|
| `us` | SPY's price close and the VIX of the same day | `.US`, a bare ticker (`GOOGL`; no suffix is the US, as `v_outcome_engine` reads it), and a US share class (`BRK.B`, `HEI.A`: `core.symbols.normalize`'s LETTERS.LETTER, its exchange letters `.F .L .N .T .V` excepted; Codex P2) |
| `sa` | Tadawul All Share `^TASI.SR` | `.SR`, a bare four-digit code |
| `eu` | STOXX Europe 600 `^STOXX` | `.L .LSE .DE .XETRA .F .PA .AS .MI .MC .SW .BR .ST .HE .CO .OL .LS .IR .VI` |
| `jp` | Nikkei 225 `^N225` | `.T .TSE .JP` |
| `cn` | Hang Seng `^HSI` | `.HK .SS .SZ .SHG .SHE` |

It reads two years of daily bars from the Yahoo chart service (the two hosts, 10 s timeouts), sets aside the bar of a session still open (`currentTradingPeriod`), and labels the **last completed session**; a reading with fewer than 30 daily bars is rebuilt from hourly bars, one day per venue-local date with its last close (Yahoo serves one daily bar for `^TASI.SR`; `archive_market_series` does the same). A market more than 7 days old is not read. `labels()` keeps the read in the process for `TFB_MARKET_CONDITION_TTL_SEC` (default 1800) and refreshes it in a background thread, so a verdict never waits on the network; a market not read for 12 hours drops out. It decides nothing and writes nothing.

**`data_engine_v2` 5.161.0: two DEMOTE-only tiers** in `_apply_investability_gate`, after the evidence tier and before the settle-unresolved cap:

- **`[MARKET-HOLD]`** (`TFB_MARKET_GATE` off | observe | enforce). A still-INVESTABLE row of a market listed in `TFB_MARKET_GATE_MARKETS` (default `us,sa,eu,jp,cn`) whose label is in `TFB_MARKET_GATE_HOLD` (default `weak,stress,drawdown,crash_day`; `calm` can never hold) becomes WATCHLIST / WATCH with `block_reason` "Market on hold: the US market on a crash day as of 2026-11-20 (S&P 500 fund -3.6% on the day)". Commodities, currencies, indices, bonds, sukuk, crypto and futures are never held; a fund on a held market is. A market with no label (not read yet, not read for 12 hours, not one of the five) never holds.
- **`[SELECTION]`** (`TFB_SELECTION_GATE` off | observe | enforce). A still-INVESTABLE share - not an ETF, fund, fund-vehicle industry, commodity, currency, index, bond, sukuk, crypto or future; a blank asset class counts as a share - becomes WATCHLIST / WATCH with `block_reason` "Selection gate: ..." when it has no analyst consensus price (`target_mean_price`, else the projected `target_price`), a consensus under `TFB_SELECTION_MIN_UPSIDE` (default 15, in points; 0.15 also reads as 15) above the price, or a risk score at or over `TFB_SELECTION_MAX_RISK` (default 30). A row without a risk score passes that test, as the v5.81.0 risk cap does.
- **observe** appends `market_hold_watch:<market>_<label>` or `selection_watch:<codes>` (`no_analyst_px`, `upside_lt15`, `risk_ge30`, joined by `+`): substring-safe for the reliability scan, at most one per family, replaced in place and removed when the row no longer fails, so a repeat pass is byte-identical. No verdict changes.
- **enforce** writes the reason, not the tag. The market tier runs first, so a row failing both carries the market reason.
- Never promotes; no column; the schema stays 115.
- **Health** (`engine_gates`): `market_gate`, `market_gate_hold`, `market_gate_markets`, `market_labels` (what this process knows and from when; `{}` while off), `selection_gate`, `selection_min_upside_pp`, `selection_max_risk`.

## Checked

- **Against the real view.** The last 290 archived closes of SPY and `^TASI.SR` (`market_series_daily`) through `metrics()` / `classify()`, against `v_market_condition` on its last 40 closes of each: 80 of 80 labels equal, and every figure (k, 1- and 5-close change, distance from the 50-close average, 20-close volatility, distance from the high, normal volatility) within 0.0001.
- **The labels today** (the view, last closes): the US **crash_day** on 20 Nov (SPY -3.6 %, -8.6 % over 5 closes, VIX 35.9); Europe crash_day; Japan crash_day (Nikkei -5.1 %); Hong Kong drawdown; Saudi Arabia drawdown (19 Nov, 12.2 % under its high). **Armed now, no share or fund of these five markets would stay INVESTABLE until its market is calm again**; other markets (Canada, Kuwait, Poland, ...) and commodities, currencies and sukuk are not held.
- Tests: `tests/test_market_condition.py` 52 passed; `tests/test_de_market_and_selection_gate.py` 48 passed; the 20 `test_de_*` suites and every test that touches the gate or `engine_gates` (607 passed, 10 skipped); the full suite locally, 7,671 passed - its two failures fail on main without this change too (`test_l17_tracecontext`, the sandbox's opentelemetry; `test_tracker_log_read_precision` L2, in no CI lane).

## Arming (owner)

1. Merge, then deploy on Render (his decision).
2. Render ENV: `TFB_MARKET_GATE=enforce` and `TFB_SELECTION_GATE=enforce` (or `observe` for both, to watch for two weeks first). Recommended: enforce both - they only demote, never promote.
3. Read-back: `GET /health` -> `engine_gates.market_gate` / `selection_gate` show the mode; `market_labels` fills within a minute of the first page read after the restart.
4. Optional: Script Property `DP2_EVIDENCE_SINCE` = the arming time, so the tile shows the hit rate of the INVESTABLE records made under the new rules.

**Rollback:** unset the two switches (byte-identical to 5.160.0), or set them to `observe`.

## Limits

- The first seconds after a restart have no labels (the read runs in the background): rows gated then are not held; the next pass over the cached row holds them.
- Each web worker reads its own labels (6-7 Yahoo calls every 30 minutes per worker).
- A weekend or holiday keeps the label of the last completed session.
- The selection rule leaves Tadawul shares without an analyst consensus out; most have none.
