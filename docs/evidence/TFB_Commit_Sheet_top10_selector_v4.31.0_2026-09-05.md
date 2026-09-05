# Commit sheet — top10_selector v4.31.0 [BC-6] FAST-TRACK PERSISTS (2 files, ONE commit) — 2026-09-05

| File | Repo path | Exists? | Delivered | Version |
|---|---|---|---|---|
| top10_selector.py | `core/analysis/top10_selector.py` | replace | SHA256 `98ea0c722283d8fe…` · 5499 lines | v4.30.0 (`28f12de5e4425f2f`) → **v4.31.0** |
| test_top10_selector.py | `tests/test_top10_selector.py` | replace | SHA256 `f3d0f6d89d581423…` · 317 lines | +5 tests (20 total) |

Project copy of this sheet: `TFB_Commit_Sheet_top10_selector_v4.31.0_2026-09-05.md` · repo copy: `docs/evidence/` same name (new).
**Commit message:** `top10_selector v4.31.0 [BC-6]: fast-track seats stay sizing-withheld until confirmed (P-61)`

## The defect (proven on the 2026-09-05 export + the live v4.30.0 code)
- `_Selection_Log`: AEFES.IS 03:40 **"FAST-TRACK (day 1) — sizing suspended under strict until confirmed … not an executable ticket today"** (output HELD) → 07:46 **"ACTIVE (day 1)"** (WITHHELD only because the GM feed was aged) → 08:08 board **EXECUTABLE 3,824 SAR / 2,693 sh**, with `T10: Stability Confirm Days = 3` and ci = 1.
- Root cause (`_apply_selection_stability`): the fast-track fill seats the symbol as a FULL member (`member=True, since=today`) with no memory of the fast track; the "FAST-TRACK (day 1)" label comes only from THIS call's `fast_tracked` list. Any later run finds `member=True`, seats it as a survivor and labels it "ACTIVE (day n)" although `ci < confirm_days`. Both sizing guards (BC-4 redaction, GAS v1.11.1 output-label truth) key on the FAST-TRACK*/GRACE* prefix → sizing released on an unconfirmed seat.

## The fix (9 anchored edits, each anchor matched exactly once; additive only)
1. Changelog block + `TOP10_SELECTOR_VERSION = "4.31.0"`.
2. New `_t10_fasttrack_legacy_enabled()` — kill switch `TFB_T10_FASTTRACK_LEGACY=1`.
3. `_stability_parse_state`: carries `ft` (absent in v≤4.30 blobs ⇒ False).
4. New-symbol default record gains `"ft": False`.
5. Seating loop: `st["ft"] = sym in fast_tracked` on entry; cleared the first run on which `ci >= confirm_days` (graduation).
6. `_ft_legacy` resolved once before the output loop.
7. Label chain: one **inserted `elif`** before the unchanged `else`: `"FAST-TRACK (day n, ci/confirm confirmed)"` for persisted unconfirmed fast-track seats (same prefix ⇒ BC-4 and the GAS guard withhold sizing). GRACE keeps precedence.
8. Audit gains `fast_track_unconfirmed` (read-back instrument). 9. Meta gains `fast_track_legacy`.

**Byte-untouched:** membership (`final`), entries, exits, displacement, clocks, hist, ordering — proven on the 09-05 replay: final_order and state (minus the additive `ft` key) identical between v4.30.0 and v4.31.0; only the label differs (and BC-4 then redacts advisor_note/funds_from/suggested_sar on the re-run). S-1 champion basket unaffected.

## ENV
**None required.** Default ON (the only direction it moves a board is sizing-withheld → never a new executable ticket). Kill switch: Render `TFB_T10_FASTTRACK_LEGACY=1` → v4.30.0 labels/rows byte-identical. Deploy = the commit (Render auto-deploy of the Web service).

## Verification (real repo tree, real module)
- `py_compile` + `compileall main.py core routes scripts tests` OK.
- AST additive-only proof: 131/133 defs byte-identical; `_stability_parse_state` additive; `_apply_selection_stability`: 1 inserted statement + 1 inserted `elif` (3 old branches and the else identical), everything else identical; 1 def added; version constant the only module-level change.
- New tests vs v4.30.0: **5 failed / 15 passed** (they detect the defect). v4.31.0: **20/20 ×3**.
- Real-module replay of the 09-05 board (5 GRACE incumbents + AEFES fast-track, two same-day runs): 03:40 "FAST-TRACK (day 1)" → 07:45 **"FAST-TRACK (day 1, 1/3 confirmed)"**, `suggested_sar=None`, `funds_from=—`; kill switch → "ACTIVE (day 1)".
- Lean CI (ci.yml list incl. `tests/test_top10_selector.py`): **237 passed**.

## Post-commit checks (Emad) — ORDER MATTERS
1. `https://github.com/emadsaberbahbah-coder/tadawul-fast/blob/main/core/analysis/top10_selector.py` → `TOP10_SELECTOR_VERSION = "4.31.0"`; no unexpected new files.
2. Wait for the Render deploy to finish (service log shows the new build live).
3. **BEFORE arming Run 1 (`TFB_T10_VENUE_ALLOWLIST`)**: one manual cockpit refresh of Top_10 → read-back = the AEFES.IS seat reads **"FAST-TRACK (day 1, 1/3 confirmed)"** and the cockpit output is no longer EXECUTABLE for it (GAS v1.11.1 prefix guard); `_Selection_Log` Stability column shows the same. That is the mechanism verdict (valid on a manual refresh).
4. Then arm Run 1 as planned (AEFES exits on the venue gate — the read-back must already be in hand).
5. Tomorrow's scheduled boards: any surviving fast-track seat counts up "2/3 confirmed" and graduates to ACTIVE on its third qualifying day.
