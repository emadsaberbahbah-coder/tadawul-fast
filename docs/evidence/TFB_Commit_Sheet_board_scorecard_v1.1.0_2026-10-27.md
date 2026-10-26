# Commit Sheet — `scripts/score_selection_log.py` v1.1.0 [RANKING HEALTH] + `board_scorecard.yml` v1.1.0 [ARMING] — 2026-10-27

**Scope:** the nightly board scorecard now reviews the engine's own ranking against actual daily data (rank buckets, Engine ROI cap bucket, confidence, sector; Spearman; one verdict per window) and measures every seat against a sector / venue reference benchmark next to SPUS. ENV-gated (`TFB_BOARD_SCORECARD_RANKING`, default off, byte-identical off). The workflow arms the gate and moves the scheduled run from observe to write.
**Files:** `scripts/score_selection_log.py` (1.0.0 → 1.1.0; 1,277 → 1,821 lines, zero removals), `tests/test_board_scorecard_p182.py` (legs P9–P11, pins 20/20 + v1.1.0), `.github/workflows/board_scorecard.yml` (v1.0.1 → v1.1.0, own commit), this sheet.

## S1 — WHY (evidence)

Source: `claude/TFB_October_2026_Recommendation_Review.md` (27 Oct 2026, `_Selection_Log` 1–26 Oct, IBKR daily bars, every headline figure independently recomputed).

| Finding | Value |
|---|---|
| Newly seated names, 1–26 Oct | 71 (62 % beat their market, mean excess +2.6 pp) |
| Same, vs their **own sector ETF** (US names) | n = 55: **44 % won, mean −1.1 pp** — the SPUS edge was sector exposure (73 % of seats were banks, insurers, REITs) |
| Engine ROI at seat vs realised excess | Pearson −0.32, **Spearman −0.38**, n = 71 |
| Ranks 1–3 at first seat beat the market | **38 %** (avg +1.5 pp) vs **70 %** for ranks 7–10 (avg +3.2 pp) |
| Engine ROI ≥ 34 (the cap bucket) | 43 % vs 70 % for < 34 |
| Confidence High vs Medium | 71 % vs 53 % |

The v1.0.0 scorecard measured none of the first effect (it never read Rank / Engine ROI / Conf) and the second only against SPUS. The learning loop could not see that its order runs backwards, nor that the sector, not the pick, was paying. v1.1.0 makes the nightly scorecard measure both from the same read-only `_Selection_Log` pass, so the loop can correct itself (ranking fix in shadow is the next build; this is its instrument).

## S2 — WHAT (per site; all of it only when `TFB_BOARD_SCORECARD_RANKING=on`)

`scripts/score_selection_log.py`
- Docstring: dated v1.1.0 WHY/WHAT/GATE/USAGE/ROLLBACK block prepended; v1.0.0 and v0.1.0 blocks verbatim; ASCII only in added lines. `SCRIPT_VERSION = "1.1.0"`.
- `board_events()` also captures `Rank` (int or None), `Engine ROI %`, `Rel` (floats, trailing `%` tolerated), `Conf`, `Market` (text) — every one optional (`_cell` → `''` when the header is absent). `build_episodes()` carries the FIRST-seat values: `rank_at_seat`, `eng_roi_at_seat`, `rel_at_seat`, `conf_at_seat`, `market_at_seat`.
- `ref_symbol(symbol, sector)`: `.US` (or bare) names → US sector ETF by the seat's Sector text, matched case-insensitively on the leading word(s) — `Financials` / `Financial Services` → XLF.US, `Real Estate` → VNQ.US, `Energy` → XLE.US, `Information Technology` / `Technology` → XLK.US, `Health Care` / `Healthcare` → XLV.US, `Industrials` → XLI.US, `Consumer Discretionary` / `Consumer Cyclical` → XLY.US, `Consumer Staples` / `Consumer Defensive` → XLP.US, `Utilities` → XLU.US, `Materials` / `Basic Materials` → XLB.US, `Communication Services` → XLC.US (a bare `Consumer` is ambiguous → none). Other suffixes → the venue index on Yahoo: `.L` ^FTSE, `.BR/.PA/.MC/.LS/.AS/.DE/.MI` ^STOXX50E, `.HK` ^HSI, `.TO` ^GSPTSE, `.AX` ^AXJO, `.MX` ^MXX, `.SR` ^TASI.SR, `.SI` ^STI, `.T` ^N225; unmatched → no reference.
- `run_scorecard()`: the reference symbols ride the same price path as SPUS (`--fetch-yahoo` / `--prices-csv` / `--fetch`); they never count towards price coverage. `score_episode(..., ref=)` computes `vs Ref +k pp` / `vs Ref Now pp` with the same date alignment as SPUS (`_bench_ret`). `detail_rows(ranking=True)` appends seven columns at the END — `Rank, Engine ROI %, Rel, Conf, Ref, vs Ref +5 pp, vs Ref Now pp` (`DETAIL_HEADER_RANKING`); existing columns keep their positions.
- `spearman()` / `_avg_ranks()`: plain-Python Spearman (average-tie ranks, Pearson on the ranks); no numpy / scipy. `ranking_health()` + `ranking_verdict()`: per window (`Since --since`, each seat month) rows under `RANKING_HEADER = Window | Cut | Bucket | Episodes | With Data | Hit % (vs ref) | Mean vs Ref pp | Mean vs SPUS pp` for Rank 1-3 / 4-6 / 7-10, Engine ROI ≥ 34 / < 34 (`ROI_CAP_BUCKET = 34.0`), Conf High / Medium / Other, Sector (≥ 5 episodes, rest pooled as Other, largest first); two Spearman rows (Engine ROI vs excess, Rank vs excess, n); ONE verdict row: `RANKING_INVERTED` (n ≥ 30 and ρ(ROI) ≤ −0.20, or both 1-3 and 7-10 buckets hold ≥ 8 scored episodes and the 1-3 hit rate is ≥ 20 points lower), `RANKING_OK` (n ≥ 30, ρ(ROI) ≥ +0.10 and 1-3 hit rate ≥ 7-10), `INSUFFICIENT_DATA` (n < 30), else `RANKING_NEUTRAL`. "vs ref" falls back to vs SPUS when the episode has no reference excess (unmapped, or the series missing that night).
- Outputs when on: the RANKING HEALTH table in stdout, the MD artifact and the step summary (Since window + every month's Verdict row), a third block in `board_scorecard_summary.csv`, the tab rectangle below CHURN and above DETAIL; the meta line gains `| refs k/n | ranking <verdict>`; the final stdout line gains ` ranking=<verdict>`; in write mode the `_Run_Log` message gains ` | ranking: RANKING_INVERTED (spearman -0.38, n=71)` and the details JSON a `ranking` object.
- Untouched: `_Selection_Log` READ ONLY; the v0.1.0 ticket path; every v1.0.0 function (signatures extended with optional keywords only).

`tests/test_board_scorecard_p182.py` — see S4. `.github/workflows/board_scorecard.yml` — see S3.

## S3 — GATE / ARMING

| Env | Before | After | Effect |
|---|---|---|---|
| `TFB_BOARD_SCORECARD_RANKING` | (absent) | `"on"` in the job env | RANKING HEALTH + reference benchmark. Off/unset → every v1.0.0 output byte-identical (P9). |
| `TFB_BOARD_SCORECARD` | `"observe"` | **`"write"`** | The scheduled run (22:23 UTC Mon–Fri) now rewrites `_Board_Scorecard` as one rectangle and appends one `_Run_Log` row per night. |

ARMING rationale: the scorecard has had more than twenty green observe runs since 1 Oct (artifacts and job summaries read sensibly every night); the owner approved arming on 27 Oct. Writes stay guarded by the v1.0.0 coverage floor (`TFB_BOARD_SCORECARD_MIN_COVERAGE`, default 0.5 → DEGRADED row, tab untouched, exit 1). Cron, concurrency group and the `ubuntu-24.04` runner pin are unchanged. The workflow change is its own commit (`arm: board scorecard writes _Board_Scorecard nightly; ranking-health on`).

## S4 — Tests

- `python scripts/score_selection_log.py --selftest` → **SELFTEST 20/20** (16 base + 4 new: 17 Spearman incl. the hand-checked 6-point tie case 14.5/√(17·17.5) = 0.84067 and the textbook −29/165; 18 reference mapping (GICS + Yahoo spellings, every venue suffix, unmatched); 19 ranking health rows and all four verdicts on synthetic scored sets, hit-rate rule alone, SPUS fallback; 20 gate parsing, column capture / absence, first-seat carry, detail / rectangle / markdown wiring).
- `tests/test_board_scorecard_p182.py` → **PASS 84 / FAIL 0** as a script (P2 / P4 skip without the optional env fixtures, as before); `python -m pytest tests/test_board_scorecard_p182.py -q` → **1 passed**. New legs: **P9** golden negative — the untouched v1.0.0 base (`git show d25468a5:scripts/score_selection_log.py`; `SSL_BASE_V1` fallback, SKIP line when neither exists) and v1.1.0 run on the same 50-seat fixture with the gate unset and `off`: stdout, the three artifacts, the step summary, the write-mode rectangle, write calls and the `_Run_Log` row are byte-identical (version tag normalised); the base never emits RANKING HEALTH even with the gate on. **P10** gate on — three fixture variants (INVERTED / OK / INSUFFICIENT_DATA), reference per row, mapped-but-missing series (XLK / XLU) → blank vs-Ref, SPUS fallback; the seven DETAIL columns at the END with hand-checked values (FS03.US: +5 4.0 / now 17.6 / vs XLF +5 3.0 / now 13.2 / vs SPUS 3.5 / 15.4); every `To Now` / `vs SPUS Now` / `vs Ref Now` cell of all 50 rows equals an exact independent computation; every RANKING HEALTH row of the Since window equals an independent re-aggregation byte for byte; Spearman rows equal an independent implementation; month windows; write mode (rectangle block placement, `_Run_Log` verdict, details JSON, live == TSV). **P11** selftest 16 → 20.
- `python -m compileall scripts/score_selection_log.py` PASS. Added lines ASCII-only (checked). Zero function or constant removals (AST diff against d25468a5).

## S5 — Rollback
- `TFB_BOARD_SCORECARD_RANKING=off` (or delete the line) → v1.0.0 outputs byte for byte, next run.
- `TFB_BOARD_SCORECARD=observe` → no sheet writes (artifacts + job summary only); `off` → nothing computed.
- Nothing else depends on either; the v0.1.0 ticket path and every other workflow are untouched. Reverting the two commits restores d25468a5 exactly.

## S6 — Known limits
- Sector mapping is by the seat's Sector **word** only (GICS / Yahoo spellings); sub-industries are not distinguished (a mortgage REIT maps to VNQ, a regional bank to XLF, not KRE).
- Non-US names get the **venue index**, not a sector reference (^FTSE, ^STOXX50E, ^HSI, ^GSPTSE, ^AXJO, ^MXX, ^TASI.SR, ^STI, ^N225); other suffixes have none → SPUS fallback.
- Yahoo index symbols may be missing on some days (or ^TASI.SR entirely) → blank `vs Ref` cells for those seats and SPUS fallback in the statistics, never an error; the meta line shows `refs k/n`.
- Verdict thresholds (n ≥ 30, ρ ≤ −0.20 / ≥ +0.10, 8 per bucket, 20-point gap) are module constants, chosen from the October sample; they are a tripwire, not a significance test.
- Engine ROI / Conf / Rank are the first-seat values; re-seats start a new episode with their own values (as v1.0.0 already treats episodes).

## S7 — Operator steps
1. Squash-merge. Nothing else to configure: both gates sit in the job env of `board_scorecard.yml`.
2. Read the first nightly `_Board_Scorecard` tab (01:23 Riyadh): the title cell ends `| refs k/n | ranking <verdict>`; the RANKING HEALTH block sits below CHURN; DETAIL has 29 columns; `_Run_Log` carries one `[BOARD-SCORECARD v1.1.0] ... | ranking: ...` row. The job summary shows the Since window and the month verdicts.
3. If the tab or the row looks wrong: set `TFB_BOARD_SCORECARD: "observe"` (and/or the ranking gate to `off`) in the workflow; the next run is artifact-only again.
