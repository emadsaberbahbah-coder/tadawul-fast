TFB Commit Sheet — scripts/run_shadow_scorer.py v1.9.1 [P-186 BASE POLICY: pair every basket against the last SCORED row]
Date: 2026-10-01 (Thursday) · Lane: GitHub/Python (shadow_scorer.yml) · Build #2 of the day · Protocol: One-Pass · File status: OLD file, rebuilt (full-file delivery)
S0/S1 — Base pin
Item	Value
Base	`scripts/run_shadow_scorer.py` v1.9.0 at HEAD `7575c7d` (sha `0fc887f6eb40deae…`, 2,243 lines) — live in the GitHub lane (`[S1-GATE v1.9.0]` row 09-30 20:04Z)
Delivered	sha `628d339c5b6b6bce…`, 2,476 lines, `SCRIPT_VERSION = "1.9.1"`
Edits	11 anchored edits, count==1 each (header+version · helpers · base-map selection · need set · freshness denominator + read-back · S1-FRESH denominator + verdict token · `_freshness_detail` signature · denominator override · selftest +5 · gate meta cell · `_Run_Log` JSON)
AST	functions 63 → 68 (+4 top-level `_base_policy`, `last_scored_row_for`, `_sg_filter_prev`, `_measure_one`, +1 nested `_f`; 0 removed) · `py_compile` PASS · 0 net-new non-ASCII
S2 — Design (WHY on the file header)
Measured 2026-10-01: each basket's day-D return pairs today's bars against the PREVIOUS ROW's stored prices; an excluded row keeps fresh bars where they existed and carries older prices where they did not, so baskets enter the next scored day with bases of different vintages. 09-30 specimen: the challenger's PINE leg paired against a 16.89 base carried from the 09-28 close (the 09-29 row was the mis-dated #76 evaluation, fresh=1/5; GOOGL carried 342.75 likewise) while the benchmark legs paired against true 09-29 closes — a 2-session challenger interval against a 1-session benchmark interval, counted as scored evidence. The red-team 10-01 §12 found the same mechanism. Also: `[S1-FRESH] fresh=5/3` — the numerator counts paired legs of the PREVIOUS basket (daily-rebalanced convention), the denominator today's seat count.
`last_scored_row_for(history, basket)`: most recent row with a numeric Daily Return, else the seed row, else None.
`TFB_S1_BASE_POLICY` = legacy (default, v1.9.0 byte-identical) | observe (legacy rows written; the last-scored pairing ALSO measured for CHALLENGER and BENCHMARK and printed) | lastscored (prev = last scored row for every basket — pairing, turnover, cost drag, carried prices and the non-trading test key off it; index chains unchanged since excluded rows carry the index).
Under lastscored the fresh-floor fraction and the S1-FRESH line divide by the pairable legs of the base row (`fresh=5/5`), never by today's seat count.
`[S1-BASE v1.9.1] mode=.. chal legacy=..@date lastscored=..@date legs=n/m | bench legacy=.. lastscored=.. | alpha legacy=.. lastscored=.. delta=..pp | fresh_den legacy=a pairable=b` appended to the verdict line, the S1_Gate meta cell and the `_Run_Log` JSON (`base_policy` key) ONLY when the policy is not legacy.
S-1 boundary note (P-139/P-175 class): measurement repair of the evidence lane; criteria, benchmark mix, fresh floor, counters, HISTORY_HEADER byte-untouched; prior scored days stand. The lastscored flip is an evidence-lane decision for the Saturday sitting after one observe read-back.
S4 — Audits (real module, real main() on the in-memory sheet stub — the P-176 harness pattern)
Battery	Result
Embedded `--selftest`	104/104 ×3 (99 base + 5 new: last-scored selection, seed fallback, policy reader, denominator override, `_measure_one` parity)
New `tests/test_s1_base_policy_p186.py` (sha `dfe4dfe3b6155b16…`) — B1 selftest · B2 pure · B3 legacy reproduces the live asymmetry (chal +0.5588 % on 2-session legs vs bench −0.5394 % on 1 session; `fresh=5/3`; no token) and is BYTE-IDENTICAL to the v1.9.0 base on the same stub (history / S1_Gate / _Run_Log rows), new env inert on base · B4 observe (legacy rows written; token shows both pairings, 09-28 base, legs=5/5, alpha delta +0.4366 pp) · B5 lastscored (chal +0.4139 % vs bench −1.1209 % — both 2-session; `fresh=5/5`; index chains consistent) · B6 normal-day parity (lastscored rows == legacy rows) · B7 `--dry-run` zero-write	7/7 PASS ×3, digest `6ebd38ce7b9cd6bf` ×3
Existing batteries, re-pinned to v1.9.1 (delivered)	`tests/test_s1_day_key_p176.py` D1–D7 digest `389bcc74906e2c85` ×3 (sha `59765fb78ad6c6b4…`) · `tests/test_s1_board_fresh_guard.py` K digest `15f197a9f7012eaa` ×3 (sha `ce28110ec54c9b3a…`) — pins only (version string, 104/104, token versions)
S5 — Delivery (paths at convention)
`scripts/run_shadow_scorer.py` (OLD, rebuilt) · `tests/test_s1_base_policy_p186.py` (NEW) · `tests/test_s1_day_key_p176.py` (OLD, re-pinned) · `tests/test_s1_board_fresh_guard.py` (OLD, re-pinned) · `docs/evidence/TFB_Commit_Sheet_run_shadow_scorer_v1.9.1_2026-10-01.md` (NEW)
S6 — Arming / read-back (GitHub lane, no Render deploy)
Commit the four files. The scheduled 15:20Z run executes HEAD → first live proof = `[S1-GATE v1.9.1]` in `_Run_Log` (legacy, byte-identical numbers).
Arming (your hand edit, one line in `.github/workflows/shadow_scorer.yml` under the scorer env): `TFB_S1_BASE_POLICY: "observe"`. Read-back = the `[S1-BASE v1.9.1] mode=observe …` token on the next scored day's verdict / S1_Gate row 3 / `_Run_Log` JSON `base_policy`.
lastscored = Saturday 10-03 sitting (record the boundary note in the evidence register; counter continues).
Rollback: remove the env line (legacy) or `git revert`.
