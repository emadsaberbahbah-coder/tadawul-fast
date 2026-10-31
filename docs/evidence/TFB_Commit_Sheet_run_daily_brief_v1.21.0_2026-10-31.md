# TFB Commit Sheet - run_daily_brief v1.21.0 [UNSIZED-ADD]

| Item | Value |
| --- | --- |
| Date | 2026-10-31 (daily improvement routine; the 2026-10-29 and 2026-10-30 firings arrived on 10-31 and are folded into this pass) |
| Branch | `claude/daily-2026-10-31` |
| Base | origin/main `05b51ebe` (#873, run_daily_brief v1.20.0) |
| Lane | GitHub Actions only (`daily_brief.yml`); no Render deploy, no Apps Script paste |
| Gate | repository Variable `TFB_BRIEF_UNSIZED_ADD` = off \| observe \| enforce, default off = v1.20.0 byte for byte |
| Priority | (a) a published recommendation / book number is wrong or missing |

## S1 - Defect and evidence

`extract_decision` reads every Portfolio_Decision row's amount with `_note_sar`, the FIRST `<n> SAR` anywhere in the Advisor Note. `portfolio_actions._advisor_sentence` writes:

- a sized ADD as `ADD <n> sh (~<amt> SAR, <funds>); <reason>; stop S / TP1 T1 / TP2 T2 SAR; ...`;
- an unsized ADD as `ADD (unsized - no deployable capital); <reason>; stop S / TP1 T1 / TP2 T2 SAR; ...` (or `stop S SAR; no TP ladder ...`);
- a sukuk ADD with the D-9 ladder note, which has no SAR figure.

So an unsized ADD's "amount" is its TP2 or stop LEVEL, and a sukuk's is 0. The v1.8.0 floor (`_enforce_min_ticket`, 200 SAR) then treats each row as a ticket. Reproduced offline on main `05b51ebe` with notes from the real `_advisor_sentence`:

| Case | v1.20.0 output |
| --- | --- |
| ADD 1120.SR 10 sh ~1,250 SAR + unsized 2222.SR (TP2 66.40) | ticket **1,316.4 SAR / 11 sh** with "+66 SAR from 2222.SR"; 2222.SR in no group |
| Unsized ADD whose TP2 is 262.50 | a phantom **ADD ticket of ~262 SAR** |
| Two unsized ADDs with sub-floor levels 120 + 110 | one phantom **ADD ticket of ~230 SAR** (the rule promotes the larger and folds the other into it) |
| Held sukuk 5023.SR, unsized ADD, MV 10,000 / cost 9,950 | **"0 holdings"** over a 10,000 SAR book: every ADD is under the floor and the folded total still misses it, so the rows go to `dropped_small`, which no renderer prints |
| Sized ADDs of 120 + 50 SAR | both dropped; 0 holdings |
| Sunday fold 14 + 98 + 1,992 SAR | one 2,104 SAR ticket (cash right), but 3030.SR and 4040.SR leave the holdings count, the hold table and the PDF portfolio tables |

Live context: #842 (commit cbdb8f6e) reports the brief of 25 Oct 18:15Z / 21:15 Riyadh (run 40220333814) printing "0 holdings" while 5023.SR was held, and attributes it to Portfolio_Decision omitting the line. Today's final-job validator (job 124524928800) reads My_Portfolio with 2 rows while the 28 Oct brief listed 1. That is consistent with, but does not prove, a dropped line: the live sheet cannot be read from this container, which is why observe exists.

## S2 - Change

| Site | Change |
| --- | --- |
| `scripts/run_daily_brief.py` | v1.21.0 WHY block above `__version__`; `__version__ = "1.21.0"`. Added `_unsized_add_mode`, `_sized_add`, `_plan_unsized_adds`, `_log_unsized_add_observe`, `_unplaced_label`, `_settle_unplaced`, `_unplaced_phrase` (seven, module level). Zero removals. |
| `extract_decision` | new optional `_ua_probe` argument (observe probe only: forces off / enforce and tags each ADD record with its input position; never set on the published decision). Tail: off and observe run the v1.20.0 call; enforce runs `_plan_unsized_adds`. |
| `_log_unsized_add_observe` (build_model, observe only, after the veto, coherence and settle passes) | builds its own off and enforce decisions from the page rows, runs the same final-feed veto, coherence guard and settle pass on each, and prints one stderr line per input ADD row whose PUBLISHED place differs: ticket (and its size), BLOCK (final feed withheld), VERIFY (coherence) or held. Rows are paired by input position, not symbol, so a duplicated symbol gets one line per row. Never alters the published decision. |
| `_plan_unsized_adds` (enforce) | amount and shares only from the leading clause `ADD <n> sh (~<amt> SAR`; a row without it is never a ticket and never folded: `unsized` when the note says so, `unparsed` otherwise (BUY row, blank or hand-edited note). Sized rows meet the unchanged v1.8.0 floor and fold on copies (that rule rewrites its target before it decides to drop everything). Every ADD row that is not a ticket stays in the hold group with 0 SAR and `unplaced_add` = unsized / unparsed / below_floor / folded; folded and under-floor rows get a note suffix, unsized rows keep their own note (it already says so). |
| `_settle_unplaced` (build_model, after the veto and coherence passes) | a folded row whose target ticket is later withheld (final-feed veto) or demoted to VERIFY is relabelled `folded_withheld`: "its cash was to join the X ticket, which is not placed today". |
| Hold surfaces | `_hold_mix` counts `unplaced` (key present only when such a row exists); hold-section intro (counts by reason via `_unplaced_phrase`), Book-snapshot phrase, chip "ADD NOT PLACED TODAY - SEE NOTES" when every held row is one; HTML hold row adds "ADD not placed: <reason>"; hero and the text ONE MOVE line say "No ADD can be placed today" when no ticket is left and neither a BLOCK row nor the final-feed veto is present (the existing execution-withheld copy wins then); the text email prints "ADD NOT PLACED TODAY (still held): SYM (reason); ...". The same copy holds with `TFB_BRIEF_HOLD_NARRATIVE_V2=0` (the legacy "data too weak" copy is never applied to these rows). The PDF lists each row with its note. All unchanged when no row carries `unplaced_add`. |
| `.github/workflows/daily_brief.yml` | `TFB_BRIEF_UNSIZED_ADD: ${{ vars.TFB_BRIEF_UNSIZED_ADD \|\| 'off' }}` in the "Render & send brief" step env (not job env). |
| `scripts/verify_deployment.py` | pin run_daily_brief 1.21.0 (pin only). |
| `.github/workflows/ci.yml` | 1.1.64 (1.1.62 / 1.1.63 claimed by open #778): the new harness in the daily brief step, and a SHA fetch of `05b51ebe` so its golden leg runs on CI. |
| `tests/test_daily_brief_send_window.py` | exact 1.20.0 pin -> 1.20.0 floor. |

Unchanged in every mode: the floor (`TFB_MIN_TICKET_SAR`), folding of sized cash and its conservation, TRIM/EXIT freed cash, BLOCK rows, MV/cost totals, the coherence guard, send_digest. Under enforce a ticket changes only where v1.20.0 had read an unsized or unparsed row's level as cash (folded into it, promoted to a phantom ticket, or outranking the sized rows).

## S3 - Tests

`tests/test_daily_brief_unsized_add.py`: **PASS 48/48** (script) and 48 passed (pytest); stable under PYTHONHASHSEED 0 / 1 / 77. The ADD notes of the WORKS fixtures come from the real `portfolio_actions._advisor_sentence`.

- WORKS W1-W13 (enforce unless noted): W1 sized ticket keeps 1,250 SAR / 10 sh, the unsized row keeps its own note, and the hero never says "No ADD can be placed" over the ticket; W2 sukuk stays in the count ("1 holdings") and on the hold intro, Book snapshot, chip, hero title, HTML row and text; W3 no phantom ticket from one level or two summed, and the ticket moves to the sized row an unsized level outranked (w3c); W4 under-floor rows held with the reason and no fold text; W5 Sunday fold keeps the 2,104 SAR / 42 sh ticket and holds the two folded lines; W6 the "no TP ladder" note; W7 observe prints exactly the expected line per affected input row on all 14 fixtures (none on sized-only) and adds nothing else to stderr; W7b observe compares the PUBLISHED places on a final-feed-veto day and on a coherence-demotion day; W8 / W8b a folded row whose target is withheld by the veto, or demoted to VERIFY, says so on every surface; W9 the narrative kill-switch keeps the true copy and the mixed chip; W10 BUY / blank rows read "no ticket size in its note"; W11 mixed book text lines and chip; W12 the PDF lists the held row (reportlab + pypdf); W13 EXIT freed cash with only held ADDs: the freed-cash hero names them.
- NEVER N1-N7: off under 11 spellings equals v1.20.0 on 14 fixtures (decision dict, HTML, text: OFF_GOLDEN digests) and logs nothing; observe == off; sized tickets at or above the floor, freed cash, BLOCK, totals unchanged; floor disabled; no input mutation; the parser table; the gate words.
- BASE N1b-N1e: the OFF_GOLDEN digests are the v1.20.0 base copy's; the PDF text equals the base's (footer version normalised); the full off stderr equals the base's (plain, veto and demotion days); with `TFB_BRIEF_HOLD_NARRATIVE_V2=0` off equals the base and observe equals off (fixture n_mixed_holds covers every hold bucket).
- GOLD G1: every WORKS check (W1-W11, W7b, W8b, W13) fails on the untouched v1.20.0 (`git show 05b51ebe`), each in its own monkeypatch context; the base also reproduces 1,316.4 SAR / 11 sh, the ~230 SAR phantom and "0 holdings".
- V1 / V1b: version floor, verifier pin, step-env mapping (step env only), the pytest line in ci.yml; against the base: ASCII added lines, exactly the six removed lines (`__version__`, the `extract_decision` signature, the two-line hold guard twice, `if d["hold"]:`), no def removed (nested included), exactly the seven module-level functions added.
- Mutation check: the reviewers' mutants (hero guard dropped, chip condition widened, settle moved before the coherence guard, observe without the veto / coherence passes, hero title dropped) each turn at least one check red.
- Golden negative of the file (`TFB_TEST_BRIEF_DELIVERED=<v1.20.0 copy>`): 41 failed, 6 passed, 1 skipped. N1b-N1e and G1 compare against the base by construction; N2 passes trivially because v1.20.0 ignores the gate (observe == off); V1b applies to 1.21.0 only.

Other: daily brief, pin, flag and env-contract suites 144 passed, 1 skipped (R11 base leg, env unset); compileall clean; workflow audit errors=0; render_env_contract `--check` OK.

## S4 - Operator steps

1. Merge (GitHub Actions only; no deploy).
2. Set the repository Variable `TFB_BRIEF_UNSIZED_ADD=observe`. On the next brief run, the "Render & send brief" log shows `[UNSIZED-ADD v1.21.0 observe] ...` lines when a row would change; the email is unchanged.
3. If the lines match the book (for example `5023.SR: would stay in the book (ADD not placed: unsized - no deployable capital) - v1.20.0 drops it from the holdings`), set `enforce`.

Rollback: unset the Variable.

## S5 - Known limits

- The note contract lives in `portfolio_actions._advisor_sentence`. If its leading `ADD <n> sh (~<amt> SAR` clause ever changes, enforce treats a sized ADD as unsized: no ticket rather than a wrong one (fail-safe direction). The harness builds its notes with the real function, so such a change turns it red.
- Draft #842 edits the same extract/snapshot area. Its reconcile would label an ADD that the min-ticket rule drops as "Portfolio_Decision omits it"; with this change armed the line is listed, so #842 should be rebased after this lands.
- The live occurrence is inferred, not read; observe is there to confirm it before arming.
- A five-lens review (correctness, regression, tests, docs, operator copy) of the first cut found: observe paired rows by symbol (a duplicated symbol logged the wrong row), two sub-floor levels could still sum into a phantom, a withheld fold target was not named, the kill-switch path fell back to "data too weak" copy, BUY / blank rows were labelled unsized, and the WHY block's (b)/(c) wording and #842 citation were loose. All are fixed in this commit and pinned by W3, W7, W8, W9, W10 and the n_dup_symbol / w3b fixtures.
- Codex review on PR #882 (P2): observe skipped a row that v1.20.0 folds away but enforce makes the ticket (sized 120 + 100 SAR and an unsized TP2 of 150: v1.20.0 publishes ~370 SAR for the unsized symbol, enforce ~220 SAR / 2 sh for the first sized one). Observe now prints "would be an ADD ticket of ~220 SAR / 2 sh - v1.20.0 drops it from the holdings"; unplaced rows also keep their Portfolio_Decision order. Pinned by fixture w3c in W3 and W7 (off digest proven against the base in N1b).
- A second independent review (five lenses, each finding adversarially verified) confirmed 15 findings on the reviewed head. The two code findings are the same defect: observe compared the plan before the final-feed veto and the coherence guard, so on such a day its lines misdescribed both versions. Observe now compares the published places (W7b). The other 13 were test gaps and doc wording, all fixed above. It rejected 10 others: paths real Portfolio_Decision rows cannot produce, or the PDF's existing layout.
