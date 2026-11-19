# Commit Sheet - `scripts/news_macro_gate.py` v1.4.0 [TRUCE-READ] - 2026-11-19

**Scope:** owner, Thu 19 Nov 2026, 16:09 Riyadh: "for what you are waiting for my decision go and select the best option and best recommended one". The two readings that v1.3.0 left open for his word (its commit sheet, S3) are both closed here.

**Files:**
- `scripts/news_macro_gate.py` 1.3.0 -> 1.4.0: two rules in the classifier, one block in `evaluate()` (the EASING line). The taxonomy file is unchanged.
- `scripts/verify_deployment.py`: pin moved to 1.4.0.
- `tests/test_news_macro_gate.py`: pins moved; two cases moved out of `TRADE_CASES`; `test_g12_*` added (four tests, 38 headlines); the `evaluate()` pin split in two (see S2).

## S0 - Evidence
- **The gate's own archive, 27 Oct - 19 Nov** (`_News_Items`, read 19 Nov): 352 market-wide de-escalation headlines.
  - 29 carry an English breach word ("despite", "collapse", "violations", "stalls", "rejects", ...). Every one was recorded as easing (direction +1). Six sat in a grade-1 cluster, so they stood on the EASING line:
    - "Trump's Board of Peace Warns Gaza Ceasefire Could Collapse" (Bloomberg);
    - "Iran attacks US bases in Gulf countries despite diplomatic efforts" (Reuters);
    - "Iran defends two-day proposal for peace talks after US rejects conditions" (Reuters, NBC);
    - "Crude Oil Prices Finish Higher as Progress Stalls in US-Iran Peace Talks".
  - One of the 29 denies the breach ("Russia and Ukraine confirm no ceasefire violations ..."). It stays easing (S1, rule 2).
  - One headline names a trade truce, and it has no fighting word. So rule 1 below moves no archived headline; it closes a gap before it is hit.
- The dry run on main of 19 Nov (v1.3.0) still showed the Bloomberg headline on the EASING line, a day after Israel launched an offensive in central Gaza.

## S1 - WHAT
1. **Fighting beside a trade truce is fighting.** The older truce rule turns a fighting headline into de-escalation when it also names a truce. It now looks for the ceasefire word with the trade-truce phrases set aside, so "Missile strike reported as US-China trade truce holds" stays `war_escalation`. A real truce beside the fighting still turns it ("Missile fire stops as Gaza truce takes hold"), unless it is reported broken - both as before.
2. **A ceasefire that is breaking is not easing.** A market-wide `de_escalation` headline that carries a breach word is read as falling (direction -1).
   - A denied breach does not count: "no ceasefire violations", "without any breach", "zero reported breaches". One word may stand between, not two.
   - A name's own headline is left as it was, so no name gains a flag.
   - Only ceasefire readings are turned: "OPEC+ raises output despite price collapse" keeps its direction.
3. **The EASING line** lists a de-escalation cluster only for its members that are not falling:
   - no falling member: the line is exactly as before;
   - otherwise the holding reports must outnumber the falling ones and be confirmed (grade 1-2) on their own; the line then quotes one of their headlines with their own grade and source count.

## S2 - Safety
- **Rule 1 can raise a verdict, and that is its purpose.** A Gulf-anchored fighting story from three wires now reads NO_NEW_ENTRY also when the headline mentions a trade truce. The same strike without the truce words read NO_NEW_ENTRY already.
- **Rules 2 and 3 can only remove an easing line.** A de-escalation cluster raises nothing, before and now, whatever its direction.
- `evaluate()` is pinned twice: the whole function, and the function without its easing block. The second fingerprint equals v1.1.0 - v1.3.0, so no rule that raises a verdict was edited. The easing block is checked to hold no verdict step, no driver and no name flag.
- The gate stays observe-only. Orders come only from the Today Action Plan.

## S3 - Not in this version
- **The fighting words are unchanged.** "Iran attacks US bases" carries none of them ("attack" alone is not listed: it is also "attacks the Fed chair"). After this change it is no longer easing, and it is not yet read as fighting. Widening the fighting words can raise verdicts and needs its own evidence.
- "A ceasefire holds despite clashes" reads as falling: "despite" is a breach word. The line then misses a mixed headline - the cautious side.
- A denial with two words between ("no major ceasefire violations") is read as a breach - also the cautious side. A wider window would read "no deal after violations" as calm.
- The denial guard is English only.

## S4 - Tests
- `pytest tests/test_news_macro_gate.py` run 3 times: 260 passed.
- Rule 1: seven headlines read before and after; three move from de-escalation to fighting, four stay. Verdict level, three wires on a Gulf anchor: GO with an easing line before, NO_NEW_ENTRY with one driver now.
- Rule 2: 27 headlines for the market and as a name's own headline (the archive's own, each breach word, each denial form and its limits); four headlines of other types that carry a breach word keep their reading.
- Rule 3: a holding ceasefire is listed as before; a breaking one is not and raises nothing; mixed clusters at 3:1, 3:2 and 2:1 are listed with the holding reports' own grade and count; 1:2, 3:3, 2:2, 1:1 and 0:3 are not; two unranked sites beside one wire's warning are not.
- With every suite that reads the gate or pins a version (Holding_Read, Exit Plan, Today Action Plan, News Board, BigQuery views, manifest pins, workflow audits): 930 passed.
- Mutants: 32 of 32 on the new code caught, and the 44 of v1.3.0 still caught; the file restored byte-identical each time.
- `python3 scripts/audit_repository_workflows.py`: `errors=0 warnings=0`.

## S5 - Deploy and rollback
- **Deploy:** merge. The next scheduled gate run uses it; nothing to paste.
- **Rollback:** revert `scripts/news_macro_gate.py` to v1.3.0.
