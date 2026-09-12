# Commit Sheet — run_shadow_scorer v1.8.0 "S-1 BOARD-FRESH GUARD" + shadow_scorer.yml schedule fix
**Date:** 2026-09-12 · **Protocol:** One-Pass (S0→S5 complete; S6 = the next nightly)
**Register item:** the S-1 `no-challenger` blocker — root-caused this session on live source, two structural handoff defects fixed, one 60-second check left with Emad.

## Identity
| | |
|---|---|
| Destinations | `scripts/run_shadow_scorer.py` · `.github/workflows/shadow_scorer.yml` |
| Base scorer (pinned) | v1.7.3 — live-fetched from GitHub main; base selftest **89/89 PASS** on a real assembled tree (board module + `core/{compliance_gate, shariah_authority, regime, risk_limits, regret}` + analysis modules, all live-fetched) |
| Delivered scorer | v1.8.0 · SHA256 `8a7b4476fbc6a3ff…` · 2,073 lines |
| Delivered workflow | SHA256 `213afada4551e22c…` — cron `40 14` → `20 15` UTC; guard env **pre-wired but commented** (arming = a separate one-line step) |
| Tests delivered | `tests/test_s1_board_fresh_guard.py` (K1–K5, real module) |

## Root cause — proven on live source, superseding the compliance-flag theory
**The compliance unblock worked.** Both nightlies after the 8f22df3 hardcode show `blocked={}` and compliance-eligible counts of 4–6 in the board log. The basket is emptied downstream, in the board→scorer handoff:

1. **The challenger filter (pinned):** scorer line ~1221 — `chal_syms = rows where last cell == "YES"`. The board's last column is `Gen2 Eligible`, and `YES` requires `invest_eligible AND Edge Verdict == "TRADE"` (`run_shadow_board.py:384`). The board log's `eligible=` counts **compliance only** — a "healthy" log line says nothing about YES. So the visible contradiction (board eligible=4, scorer chal=0/0) is not a contradiction at all.
2. **D-1 — schedule race (fixed here):** board crons 05:10/14:10 UTC; scorer cron 14:40 UTC — 30 minutes of headroom that GitHub cron jitter routinely eats. **Observed 2026-09-11: the board COMPLETED at 17:44:11 Riyadh, four minutes after the scorer's scheduled start** (scorer logged 17:53:00, `chal fresh=0/0`). The scorer asserted nothing about which day's board it read.
3. **D-2 — non-atomic board write (documented; board-side fix queued):** `write_board()` is `clear()` **then** `update()` — a reader in the gap sees an empty or partial tab. The scorer is exactly such a reader.
4. **The one datum still needed (60 seconds, Emad):** open the **Shadow_Board tab** and read the `Edge Verdict` + `Gen2 Eligible` columns on today's rows. If any row says `YES` → the race alone explains the failures and this delivery ends it. If all say `NO`, the Edge Verdict names the per-row sub-cause (`NO_ROI` is the likely one — the cockpit's `ROI % (TP1)` cells are blank on grace/suspended rows, and the board maps `roi` to that column; `NO_COST_MODEL` is ruled out for `.US` at rt = 0.10%, and `EDGE_BELOW_COST` needs rt > ~1.4%, implausible at 0.10). **Whether blank-TP1 rows should fall back to Engine ROI 12M is an S-1 evidence-lane semantics decision — deliberately NOT patched here without your direction.**

## What v1.8.0 does
Gate `TFB_S1_BOARD_FRESH_GUARD = off | observe | enforce` (**default OFF ⇒ byte-identical**; env lives in the workflow YAML, same lane as the compliance flags):
- `board_asof_date()` parses the board's own meta stamp (`as of YYYY-MM-DD HH:MM Riyadh`); the authority list's `as_of=` is explicitly NOT mistaken for it (K2). Missing stamp **fails open** as fresh — the guard can never manufacture an exclusion.
- **observe:** one `[S1-BOARD-FRESH] asof=… mode=observe [STALE]` segment on the verdict line. Nothing else changes.
- **enforce + stale:** bounded re-read (`TFB_S1_BOARD_WAIT_MIN`, default 8 min, 60 s polls) — **recovers the race day** instead of losing it; if still stale, the day records the honest new reason **`stale-board`** (precedence: after venue-non-trading, before `no-challenger` — calling a stale read "no-challenger" was label untruth).
- `_board_extract()` is the v1.7.3 main-path extraction **verbatim-moved** so main and the retry share one implementation.

## Audit results (S4)
| Check | Result |
|---|---|
| `py_compile` | PASS |
| AST zero-removal | PASS — removed NONE; added the 5 guard functions |
| Smart-quote / NBSP | CLEAN |
| **K1** full existing selftest battery on v1.8.0 | **89/89 PASS ×3** |
| **K2** asof parsing: live meta shape → date; authority `as_of` ignored; garbage → None (fail-open) | PASS ×3 |
| **K3** mode reader with junk fail-off | PASS ×3 |
| **K4** override truth table incl. non-trading precedence | PASS ×3 |
| **K5** retry recovers the race in 2 polls (injected sheet, `chal=[KRP.US]`); an exhausted window closes STALE with no invented YES | PASS ×3 |
| Triple-run digest | `b24e56ece4e156e1` identical ×3 |

## Deploy + arming (Emad; one change per evidence run)
1. **Commit both files** — the scorer and the workflow. This alone = cron moved to 15:20 UTC + guard code deployed OFF ⇒ tonight's run is behavior-identical except it starts ~18:20 Riyadh.
   https://github.com/emadsaberbahbah-coder/tadawul-fast/blob/main/scripts/run_shadow_scorer.py
   https://github.com/emadsaberbahbah-coder/tadawul-fast/blob/main/.github/workflows/shadow_scorer.yml
2. **The 60-second check:** read Shadow_Board's `Edge Verdict` / `Gen2 Eligible` columns and tell me what they say — that decides whether a NO_ROI fallback build is even needed.
3. **Arming run (observe):** uncomment `TFB_S1_BOARD_FRESH_GUARD: "observe"` in the YAML. Read-back = the `[S1-BOARD-FRESH]` segment on the next nightly verdict.
4. **Enforce** in a separate sitting. Rollback anywhere: re-comment the line / `git revert`.

## Explicitly not in this build (queued)
Board-side atomic `write_board` (pad-and-single-update, kills D-2 at the source) — slot #5 candidate · the NO_ROI → Engine-ROI-12M fallback (evidence-lane semantics, **your call after the 60-second check**) · P-134 cockpit cash cell + Copper quarantine (GAS-side).
