# Commit Sheet - `scripts/news_macro_gate.py` v1.3.0 [TRADE-RELIEF] and taxonomy 1.2.0 - 2026-11-19

**Scope:** owner, Thu 19 Nov 2026, 15:33 Riyadh: "Lest move to the next" - part 1 of the next stage: the two misreadings that v1.2.0 left on purpose (its commit sheet, S3).

**Files:**
- `data/news_gate_taxonomy.json` 1.1.0 -> 1.2.0: three word lists added (`trade_truce`, `trade_relief`, `trade_hedge`). No event type added or changed.
- `scripts/news_macro_gate.py` 1.2.0 -> 1.3.0: the classifier reads the three lists and applies two rules. `evaluate()` is byte-identical.
- `scripts/verify_deployment.py`: pin moved to 1.3.0.
- `tests/test_news_macro_gate.py`: four pins moved; two cases moved out of `MIXED_CASES`; `test_g11_*` added (86 headlines, three tests).

## S0 - Evidence
- **The misreadings.** "truce" is a ceasefire word, so a trade truce was put on the EASING line as war de-escalation. "tariff" is a trade-pressure word, so a tariff cut was read as `trade_sanctions`, a type that raises CAUTION on a market anchor.
- **The gate's own archive, 27 Oct - 19 Nov** (before it searched trade): 344 de-escalation headlines, one of them a trade truce; 47 trade-pressure headlines, one of them a tariff relief. Neither moved a verdict.
- **World headlines, 5 - 18 Nov** (GDELT, 933 distinct titles with "tariff" or "truce"): 106 of 790 "tariff" titles (13%) speak of relief; 13 of 58 "truce" titles (22%) are trade truces, and none of the 13 names a ceasefire or peace talks.
- v1.2.0 began searching trade on 19 Nov, so these headlines now reach the gate every day.

## S1 - WHAT
In `Classifier.classify()`, after the older rules:
- **(A) A trade truce is not a ceasefire.** A `de_escalation` reading whose only ceasefire word sits inside a trade-truce phrase ("trade truce", "tariff truce", and the Arabic) becomes `trade_deal`. It is shown as falling when the headline says the truce is in doubt.
- **(B) A tariff cut is not trade pressure.** A `trade_sanctions` reading becomes `trade_deal` when all of these hold:
  - the headline states tariff relief: a listed phrase ("tariff relief", "tariff cut", ...) or a relief verb up to two words before "tariffs" ("cuts", "lifts all steel");
  - no other trade-pressure word remains once that phrase is set aside;
  - it carries no hedge (threat, demand, plan, seek, talks, if, no, would, end, ruled out, a question mark, ...), no breach, denial or rumour word;
  - no relief verb stands in front of the relief phrase ("scraps tariff relief"), and no past form of one after it ("tariff pause lifted").
- The words between the verb and "tariffs" stay in the text that is checked for pressure, so "lifts sanctions and tariffs" keeps its older reading.
- For a name's own headline the reading is dropped instead: a name never takes a label-only type.

## S2 - Safety, by construction
- A reading can only move from `de_escalation` or `trade_sanctions` to `trade_deal` (or to none, for a name). `trade_deal` is label-only and in no verdict rule.
- So the change can remove an EASING line, a CAUTION driver or a WATCH flag. It cannot add one.
- `evaluate()` is unchanged; its fingerprint is pinned in the tests and equals v1.1.0 and v1.2.0.
- The rules need `trade_deal` to be a label-only type. Without the type, with it un-marked, or without the three lists, the classifier reads exactly as v1.2.0.

## S3 - Not in this version
- **Sanctions relief** ("lifts sanctions on ...") stays `trade_sanctions`. The archive holds no such headline.
- **A fighting headline that also names a trade truce** is still not read as fighting: the older truce rule turns it into de-escalation, and now into `trade_deal`. Closing this can RAISE a verdict, so it waits for the owner's word.
- **A hedged relief headline** ("seeks lower tariffs", "discuss tariff cuts") keeps its older, cautious reading. So does a relief verb more than two words away from "tariffs".
- An opinion headline in the imperative ("lower tariffs!") is read as relief.

## S4 - Tests
- `pytest tests/test_news_macro_gate.py` run 3 times: 223 passed.
- 86 headlines (world headlines of 5 - 18 Nov, the archive's own, and probes of each guard) are read before and after: 26 move, 60 stay whole. For a name the moved reading is dropped.
- Verdict level, three wires each: a tariff relief on China raised CAUTION and now reads GO; a trade truce stood on the EASING line and now does not; a tariff threat, a hedged relief and a relief beside new tariffs are CAUTION before and now; a real ceasefire still stands on the EASING line; a name's tariff-relief headline no longer flags it WATCH, a tariff-threat one still does. One run with all 86 headlines: no driver, easing line or flag that was not there before.
- With the Holding_Read, Today Action Plan, manifest-pin, News Board and news workflow suites: 329 passed.
- Mutants: 21 of 21 on the classifier and 23 of 23 on the word lists caught; both files restored byte-identical. Two further mutants (the rules widened to other event types) are equivalent: the "no other word of that type remains" check stops them.
- `python3 scripts/audit_repository_workflows.py`: `errors=0 warnings=0`.

## S5 - Deploy and rollback
- **Deploy:** merge. The next scheduled gate run uses it; nothing to paste.
- **Rollback:** revert the two files.

## S6 - Review findings (Codex P2 x2 on #1033, both verified and fixed)
- **A pressure word between the verb and "tariffs" was lost.** In "China lifts sanctions and tariffs on Iran" the relief pattern took "lifts sanctions and tariffs" whole and set it aside, so "sanctions" was no longer seen and the headline moved to `trade_deal`. Now only the verb and "tariffs" are set aside; the words between them stay and still count.
- **A rejection in the past tense slipped through.** The guard list had "rule out" and "rules out" but not "ruled out", so "China tariff cuts ruled out by officials" moved. Fixed, and the cause was wider than one word, so the guards were rebuilt:
  - every rejection or request verb is listed as a stem and tested in each tense (36 verbs, 139 forms); the test found five more gaps ("raise", "warn", "urging", "pledging", "hoping") before the push;
  - a past form of a relief verb after the relief phrase now undoes it ("tariff pause lifted", "tariff cuts dropped");
  - "no", "would", "unlikely", "talks" and "tariff-free" guard as well.
- Both headlines read CAUTION before and now in the verdict-level test.
