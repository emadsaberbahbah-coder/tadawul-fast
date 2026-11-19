# Commit Sheet - `scripts/news_macro_gate.py` v1.2.0 [TOPICS] and taxonomy 1.1.0 - 2026-11-19

**Scope:** owner, Thu 19 Nov 2026. 11:35 Riyadh: is the China summit in the news analysis? 12:15: world news (politics, wars, the economy, the fear of AI, elections) is part of valuing any stock. 13:06: "Let's Start build the new news ...".

**Files:**
- `data/news_gate_taxonomy.json` 1.0.0 -> 1.1.0: four event types added after the older MACRO types (108 lines added, the version line changed, no older line touched).
- `scripts/news_macro_gate.py` 1.1.0 -> 1.2.0: header, version, eight searches added; the classifier reads the `label_only` mark (two lines). `evaluate()` is unchanged.
- `scripts/verify_deployment.py`: pin moved to 1.2.0.
- `tests/test_news_macro_gate.py`: five pins moved; `test_g10_*` added (13 classification cases, 20 mixed headlines and four tests).

## S0 - Evidence (the archive, 12 - 19 Nov 2026)
- The gate searched five subjects: fighting, oil, rates, market stress, Saudi. Of about 3,000 headlines in the week, 23 were on trade.
- In the week of the Shenzhen APEC summit, the headline "What's at stake for Trump and Xi in Shenzhen: A trade truce deadline, new export controls, and Iran" was filed as war de-escalation, because "truce" is a ceasefire word.
- "FOMC minutes and APEC Summit in focus." was filed as a central-bank story. There was no type for a summit, a trade deal, AI or an election.

## S1 - WHAT
1. **Four MACRO event types**, English and Arabic terms, all marked `"label_only": true`:
   - `trade_deal` (positive);
   - `summit_diplomacy`, `ai_tech`, `election_politics` (neutral: the direction comes from the headline's own positive and negative words).
   - They name a subject and can change no older reading (see S2 and S6).
2. **Eight searches**, after the existing macro searches and before the names:
   - GDELT and Google News in English: `trade china`, `ai tech`, `politics`;
   - Google News in Arabic: `trade china ar`, `ai tech ar`.

## S2 - What does not change
The verdict, its drivers, the easing line and the name flags are the same for any input. Three things make that so:
- **Order.** The four new types stand after every older MACRO type, and the first match wins. A headline that had a type keeps it: a recession, a default, a tariff, a crash or a strike named beside a summit, AI or an election is read as before.
- **Names.** A name's own headline never takes a label-only type, so no new WATCH can appear on a name.
- **Rules.** `evaluate()` is not touched and names none of the new types, so they cannot raise CAUTION or NO_NEW_ENTRY.

Tests: `test_g10_label_only_types_change_no_older_reading` reads every test headline with the taxonomy and with the same taxonomy without the four types, and requires the same reading wherever there was one, for the market and for a name. `test_g10_new_types_never_move_the_verdict`: three wires on a summit, AI and election story read GO; a recession story with an election word still reads CAUTION; a name's own AI headline raises no flag.
- No tab, header, workflow or schedule changes. The gate stays observe-only.

## S3 - Not in this version
- **Two older misreadings stay as they were.** A "trade truce" headline is still read as `de_escalation` (the word "truce"), and a tariff cut as `trade_sanctions` (the word "tariff"). Correcting them changes the easing line and a CAUTION rule, so it needs its own change with its own evidence. Both are pinned in `MIXED_CASES` so the change will be seen when it is made.
- The GDELT document API still answers HTTP 429 to the runner on most runs, so its searches are skipped (the latest run: 3 items, 14 searches skipped). Google News carries the headlines (946 items in 28 s on that run; the five added searches add a few seconds, inside its 120 s budget). The daily themes now reach BigQuery through the public GDELT tables (`scripts/archive_news_themes.py`); moving the headlines the same way is the next step.

## S4 - Tests
- `pytest tests/test_news_macro_gate.py` run 3 times: 135 passed.
- With `test_holding_read.py`, `test_today_action_plan.py` and `test_verify_manifest_pins.py`: 201 passed.
- Mutants on the final files: 12 of 12 on the taxonomy (a new type moved before an older one, four ways; a lost or false `label_only`; an older type marked `label_only`; a dead term put back, English and Arabic; a direction; a severity; missing Arabic) and 5 of 5 on the classifier (names take label-only types; the mark ignored, inverted or read loosely; the market scope drops the types). Both files restored byte-identical.
- `python3 scripts/audit_repository_workflows.py`: `errors=0 warnings=0`.

## S5 - Deploy and rollback
- **Deploy:** merge. The next scheduled gate run uses it; nothing to paste.
- **Rollback:** revert the two files.

## S6 - Review finding (Codex P2 on #1031, verified and fixed)
- **Finding:** the first version placed `summit_diplomacy`, `ai_tech` and `election_politics` before `credit_stress`. The first match wins, so "China faces recession as election uncertainty grows" from two wires was read as an election story and the verdict fell from CAUTION to GO.
- **Verified:** true, and wider than reported. `trade_deal` stood second, so it also took headlines from `emergency_policy`, `market_turmoil` and `de_escalation` (9 of 11 mixed headlines tried changed type). A second gap, not in the finding: a name's own headline on AI or an election took a new type, and a negative one would have flagged the name WATCH.
- **Fix:** the four types moved after `credit_stress`, in the older order plus four; they are marked `label_only` and a name's headline never takes them. Nine terms that could no longer match were removed from `trade_deal` (eight English with "tariff" or "truce", one Arabic), so every listed term is alive.
- **Cost of the fix:** the trade-truce headline of S0 is again read as `de_escalation`, as before this change (S3).

