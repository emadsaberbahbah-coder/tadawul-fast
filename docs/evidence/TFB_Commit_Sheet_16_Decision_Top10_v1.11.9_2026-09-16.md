# TFB Commit Sheet — 16_Decision_Top10.gs v1.11.9 — 2026-09-16
## P-145: GRACE SIZING SUSPENSION (src-present leg) — Option A, operator-adopted ("go with the best option")

## Base pin
- v1.11.8, Emad's same-day upload, sha256 `cb86852dfdb6bbd1…` — **byte-identical to the 2026-09-15 delivery, zero drift**. 5,199 content lines, pure CRLF.

## Root (pinned on this source, base line numbers)
- `dt10StabCore_` GRACE branch (~L2271): predicate `!rawSet[sym] || co>0` labels a seat GRACE, but the **src-present** case (seat qualifies TODAY while a prior miss counter is still >0) had **no suspension leg** — the builder's full plan copied at ~L2233 rode through with rank, ticket, Funds From and INVEST prose. Ghosts were always blank by construction; both FAST-TRACK branches suspend explicitly; only this leg leaked.
- Reachability = exactly the 2026-09-16 TSM sequence: day-advance miss inside `rank_buffer` → exit clock **PAUSES** (~L2098, co stays 1) → same-day re-run → clocks **FROZEN** (~L2105) → seat re-qualifies with co=1 → GRACE label + 13,976 SAR ticket. Count-base split follows: `heldByGrace` (same predicate) said "5 grace" while the banner (plan-presence) said "1 EXECUTABLE + 4 GRACE-HELD". **Seeded by P-144** (the stale-epoch 07:21 pass) — the 08:45 trigger move shrinks this class too.

## Delivered
- sha256 `5295762e4e43d1fbab9f91e8a0ff01f3b881ffd5765cd7cde98cac275bb9ed3b`
- 5,323 lines, **pure CRLF** (5,322 CRLF, 0 lone LF); **7 anchored edits, each count==1** (E1 header version, E2 WHY block, E3 DT10_VERSION, E4 kill reader, E5 helper, E6 branch leg, E7 selftest).
- Functions **106 → 108** (+`dt10GraceSuspend_`, +`dt10GraceSizingLegacy_`, **0 removed**); `node --check` PASS (ES5); 0 smart quotes; non-ASCII in new lines = 1 = the version-bumped header line carrying the file's own pre-existing em dash (**0 net-new non-ASCII**). All prior WHY blocks verbatim.

## What it does
- `dt10GraceSuspend_(tk, status)` (pure): the dt10FastTrackSuspend_ dash vocabulary on the seven sizing fields + `funds_from` copy-on-write (P-77 class), **plus `_grace_hold = true`** — so every existing grace surface takes over unchanged: D-4 rank dash, whole-row amber tint, seat classifier (banner counts), SelLog seated marker; the _Selection_Log row keeps its numeric rank (D-4). Countable `_p145_suspended` marker stamped.
- GRACE branch calls it when `src` exists. **DEFAULT ON**; kill = Script Property `DT10_P145_GRACE_SIZING_LEGACY='1'` (the OFF state IS the defect — P-142/P-127 precedent; `dt10FundingWithholdLegacy_` pattern, never throws).
- `dt10SelfTest` gains **'grace sizing core: ok'** (real-numbers fixture = today's TSM ticket; copy-on-write + idempotence asserted).
- Deliberate cuts, recorded: same-day requalification (Option B) NOT implemented — operator chose A; co-reset semantics untouched (day-advance presence remains the only clear); banner counting code untouched (agreement is now automatic via `_grace_hold`); KPI headline basis stays P-108/F02a scope.

## Evidence
- Dual-tree node harness `tests/test_dt10_p145_grace_sizing.js` **T1–T5 PASS ×3, identical digest `4342e1620bd590b6`**, fixtures = the real 2026-09-16 board (TSM co=1 src-present, KRP carried-FT, PINFRA/2286 ghosts, strict ON, same-day frozen clocks):
  T1 base v1.11.8 **reproduces the live defect** (golden-negative; kill-tree deep-equal to base, byte-behavior); T2 v1.11.9 suspends (status GRACE(1/3), 7 fields + funds_from dashed, markers set, note honest; KRP FT-suspend and both ghosts untouched); T4 renderer rank '—' + seat classifier exec 1→0 / grace 2→3; T5 embedded `dt10SelfTest` replayed in node — **'grace sizing core: ok' + 'funding containment core: ok' + 'cash source core: ok', zero FAIL**.

## Deploy & read-back
- **Paste v1.11.9 INSTEAD of v1.11.8** (it contains v1.11.8 verbatim — one paste closes P-142 and P-145 deploys together) → run `dt10SelfTest` → expect the THREE ok lines above.
- Read-back on the next board where a src-present co>0 seat occurs: Stability GRACE + rank '—' + dashed sizing + "re-qualifying seat; sizing suspended" note; **status-note and banner grace counts agree**. If pasted before tomorrow's run and TSM repeats today's state: banner reads "0 EXECUTABLE … + 5 GRACE-HELD" — honest.
- Rollback: set the kill property (or re-paste v1.11.8).
