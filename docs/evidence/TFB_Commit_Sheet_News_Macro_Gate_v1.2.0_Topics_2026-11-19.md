# Commit Sheet - `scripts/news_macro_gate.py` v1.2.0 [TOPICS] and taxonomy 1.1.0 - 2026-11-19

**Scope:** owner, Thu 19 Nov 2026. 11:35 Riyadh: is the China summit in the news analysis? 12:15: world news (politics, wars, the economy, the fear of AI, elections) is part of valuing any stock. 13:06: "Let's Start build the new news ...".

**Files:**
- `data/news_gate_taxonomy.json` 1.0.0 -> 1.1.0: four event types added (113 lines added, the version line changed, nothing else).
- `scripts/news_macro_gate.py` 1.1.0 -> 1.2.0: header, version, eight searches added. No function changed.
- `scripts/verify_deployment.py`: pin moved to 1.2.0.
- `tests/test_news_macro_gate.py`: five pins moved; `test_g10_*` added (13 classification cases and three tests).

## S0 - Evidence (the archive, 12 - 19 Nov 2026)
- The gate searched five subjects: fighting, oil, rates, market stress, Saudi. Of about 3,000 headlines in the week, 23 were on trade.
- In the week of the Shenzhen APEC summit, the headline "What's at stake for Trump and Xi in Shenzhen: A trade truce deadline, new export controls, and Iran" was filed as war de-escalation, because "truce" is a ceasefire word.
- "FOMC minutes and APEC Summit in focus." was filed as a central-bank story. There was no type for a summit, a trade deal, AI or an election.

## S1 - WHAT
1. **Four MACRO event types**, English and Arabic terms:
   - `trade_deal` (positive), placed before `de_escalation` so a trade truce is not read as a ceasefire;
   - `summit_diplomacy`, `ai_tech`, `election_politics` (neutral: the direction comes from the headline's own positive and negative words), placed after `trade_sanctions`.
2. **Eight searches**, after the existing macro searches and before the names:
   - GDELT and Google News in English: `trade china`, `ai tech`, `politics`;
   - Google News in Arabic: `trade china ar`, `ai tech ar`.

## S2 - What does not change
- `evaluate()` is not touched and names none of the new types, so they cannot raise CAUTION or NO_NEW_ENTRY (`test_g10_new_types_never_move_the_verdict`: three wires on a summit, AI and election story read GO).
- A ceasefire is still `de_escalation`; fighting still comes first; a mixed "FOMC and summit" headline is still `central_bank`; an OPEC+ summit is still `oil_supply`; a tariff threat is still `trade_sanctions`; a company's own results keep their company type.
- No tab, header, workflow or schedule changes. The gate stays observe-only.

## S3 - Not in this version
The GDELT document API still answers HTTP 429 to the runner on most runs, so its searches are skipped (the latest run: 3 items, 14 searches skipped). Google News carries the headlines (946 items in 28 s on that run; the five added searches add a few seconds, inside its 120 s budget). The daily themes now reach BigQuery through the public GDELT tables (`scripts/archive_news_themes.py`); moving the headlines the same way is the next step.

## S4 - Tests
- `pytest tests/test_news_macro_gate.py` run 3 times: 134 passed.
- With `test_holding_read.py`, `test_today_action_plan.py` and `test_verify_manifest_pins.py`: 200 passed, run 3 times.
- Mutants: 5 of 5 on the script and 6 of 6 on the taxonomy caught (order of `trade_deal`, order of `summit_diplomacy`, a direction, a missing term, missing Arabic, a wrong scope); both files restored byte-identical.
- `python3 scripts/audit_repository_workflows.py`: `errors=0 warnings=0`.

## S5 - Deploy and rollback
- **Deploy:** merge. The next scheduled gate run uses it; nothing to paste.
- **Rollback:** revert the two files.
