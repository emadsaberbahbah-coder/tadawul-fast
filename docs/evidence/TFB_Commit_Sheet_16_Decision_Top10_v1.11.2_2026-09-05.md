# Commit sheet — 16_Decision_Top10.gs v1.11.2 (P-61 live fix · P-62 · P-67) — 2026-09-05

| File | Where | Exists? | Delivered | Version |
|---|---|---|---|---|
| 16_Decision_Top10.gs | Apps Script project (bound to workbook `1QaOSNHqKlCaqlnaB45O4RlajWeMNe5cAE-r2HGwsG04`) — replace the file body | replace | `16_Decision_Top10.txt` · SHA256 `72e3ccfeceba06f3…` · 4,562 lines · CRLF preserved · ES5 (acorn ecmaVersion 5 parse OK) · 0 smart quotes | v1.11.1 (`ec5eb9be47ced4cf…`) → **v1.11.2** |

Repo copy of this sheet (optional): `docs/evidence/TFB_Commit_Sheet_16_Decision_Top10_v1.11.2_2026-09-05.md` (new). No repo code changes.

## Correction first (Claude error #7, today)
The live 03:40 → 07:46 → 08:08 AEFES.IS sequence was produced by **this file's** stability layer (`dt10StabCore_` — the note text "sizing suspended under strict until confirmed (seat filled to avoid under-fill; not an executable ticket today)" is `dt10FastTrackSuspend_`), not by `core/analysis/top10_selector.py`. The cockpit posts to `/sheet-rows/opportunity-candidates` (builder v1.19.3) and applies stability client-side. `top10_selector` v4.31.0 [BC-6] fixed the same design defect in the selector's own path (harmless, correct there) — but the cockpit read-back I asked for will appear **only after this paste**, not after the Render deploy.

## Defect (proven in node on v1.11.1 with the 09-05 replay)
03:40 `FAST-TRACK (day 1)`, shares `—`, note "sizing suspended…" → same-day re-run `ACTIVE (day 1)`, shares **2693**, advisor note = the builder's full plan, `dt10OutputStatus_` = **EXECUTABLE** with an EXECUTABLE feed. Root cause: fast-track fill sets `member=true, since=today` with no `ft` memory; label and v1.8.10 suspension key on the current run's `fastTracked` list only.

## Edits (18 anchored, each matched exactly once; 84/92 functions byte-identical; 1 added; 0 removed; 9 old lines altered — all signature/literal extensions)
- **P-61** `dt10StabParseState_` carries `ft`; default record `ft:false`; seating sets `ft = fast-track-filled-this-run`, cleared when `ci >= confirm_days`; label chain gains one branch `FAST-TRACK (day n, ci/confirm confirmed)` that re-applies `dt10FastTrackSuspend_(tk, status)` under strict; `dt10FastTrackSuspend_` accepts an optional label (default text byte-identical); audit `fast_track_unconfirmed`; stab note `N ft-carried`.
- **P-62** `dt10SelLogSignature_(tickets, outputState)` adds an `OUT~<state>` token; `dt10AppendSelectionLog_` takes/uses it; `refreshDecisionTop10` passes `dt10OutputStatus_(payload)`.
- **P-67** `tfbMorningCockpitRefresh` writes one `_Run_Log` row (INFO on success / ERROR on failure) via new `dt10MorningRunLog_` (10-column house format, never throws).
- Toggles (kill-switches, default ON): `DT10_V1112_FASTTRACK_PERSIST`, `DT10_V1112_SELLOG_OUTPUT_SIG`, `DT10_V1112_MORNING_RUNLOG`.
- `dt10SelfTest()` +3 pure lines: fast-track persistence core (replays 09-05 incl. kill-switch), sellog output-keyed signature, morning run-log helper.

**Untouched:** membership pass, exits, displacement, clocks, hist, ordering, the UV gate, BC withholding index maps, panel/criteria, POST body.

## Verification (node, GAS globals stubbed; pure functions only)
- ES5 parse OK (both versions) · smart quotes 0 · CRLF round-trip proven on the original.
- v1.11.1 replay: 07:45 `ACTIVE (day 1)` / shares 2693 / output EXECUTABLE (defect reproduced).
- v1.11.2 replay: 07:45 `FAST-TRACK (day 1, 1/3 confirmed)` / shares `—` / `_ft_suspended=true` / output **HELD** / note `stab: 5 grace, 1 ft-carried`; day 2 `2/3 confirmed`; day 3 `ACTIVE (day 3)` with sizing; kill-switch → `ACTIVE (day 1)`.
- SelLog signatures distinct across HELD / WITHHELD / EXECUTABLE; 1-arg legacy signature unchanged (`A~S-~|B~S-~`).
- Built-in self-test: 0 FAIL on both versions; the three new lines read `ok`.

## Post-paste checks (Emad) — in this order
1. Apps Script editor → replace the body of `16_Decision_Top10.gs` with the .txt content → save → run `dt10SelfTest()` → the alert/log shows `fast-track persistence core: ok …`, `sellog output-keyed signature: ok`, `morning run-log helper … present`.
2. Run `refreshDecisionTop10()` (or the cockpit menu refresh) → Top_10: AEFES.IS Stability = **`FAST-TRACK (day 1, 1/3 confirmed)`**, Ticket/Shares `—`, banner not EXECUTABLE for it; status line contains `stab[strict]: 5 grace, 1 ft-carried`; `_Selection_Log` gets a new snapshot (`SelLog: +N`) because the OUT token changed. That is the P-61 mechanism read-back — **do it before arming Run 1** (the venue gate removes AEFES).
3. Run `tfbMorningCockpitRefresh()` once manually → `_Run_Log` gains a row `INFO | tfbMorningCockpitRefresh | Top_10_Investments | OK | Morning cockpit refresh (time-driven) completed | feed: …` (P-67 read-back; tomorrow's 08:07 trigger repeats it).
