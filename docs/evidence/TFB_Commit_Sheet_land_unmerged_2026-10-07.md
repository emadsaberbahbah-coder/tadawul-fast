# TFB Commit Sheet - LAND UNMERGED CONTENT 2026-10-07 [GitHub review: open-PR triage]

Date basis: Asia/Riyadh (2026-10-07). Base: `main` at `26fe496` (PR #719).
Lane: repository hygiene. No runtime code path changes. This commit lands
eight evidence or register documents, one Apps Script file that is mirrored
here (the live source is the Apps Script editor), one passing test, and one
archived harness.

## Why

The repository has **137 open pull requests**. The oldest is #1 and only #720
is current. A GitHub review checked each of the other 136 against `main` (full
history, `git merge-base`). It ran reverse and forward `git apply --check` of
each PR's own diff. Nine of them each add one document or Apps Script file
that never reached `main` and is still worth keeping. The most material is
`apps_script/25_Trade_Notes.gs` (PR #627). Its harness,
`tests/test_gas_trade_notes_v100.js`, IS on `main`, but the file it tests was
never merged. So on `main` the harness dies with `ENOENT` and exits 1.

This commit lands those files byte-for-byte from their PR heads. Historical
documents keep their original characters; only this sheet and the README note
are new text. It also lands `tests/test_audit_tz_aware.py` (PR #42), which
passes 10/10 against today's `main`, and archives the misnamed harness from
PR #340 under the PR #718 convention. The open PRs can then be closed without
losing content.

| PR | Source commit | File landed |
| --- | --- | --- |
| #389 | `26e9e90` | `docs/TFB_Improvement_Register_Update_2026-09-03.md` |
| #392 | `7137d53` | `docs/evidence/TFB_Commit_Sheet_data_engine_v2_v5.136.0_2026-09-04.md` |
| #395 | `830199a` | `docs/evidence/TFB_Commit_Sheet_opportunity_builder_v1.19.3_2026-09-04.md` |
| #397 | `9dc5e32` | `docs/evidence/TFB_Commit_Sheet_tfb_acceptance_v1.0.6_2026-09-04.md` |
| #401 | `6b965d5` | `docs/evidence/TFB_Commit_Sheet_data_engine_v2_v5.137.0_2026-09-04.md` |
| #411 | `d267d24` | `docs/evidence/TFB_Universe_Hygiene_Worklist_2026-09-05.csv` |
| #447 | `84ce25c` | `docs/TFB_Improvement_Register_Update_2026-09-09.md` |
| #598 | `a00455e` | `docs/evidence/TFB_Commit_Sheet_16_Decision_Top10_v1.11.11_2026-09-26.md` |
| #627 | `35da09e` | `apps_script/25_Trade_Notes.gs` (`TFB_TRADE_NOTES_VERSION = '1.0.0'`) |
| #42 | `0ede5d3` | `tests/test_audit_tz_aware.py` (audit_full_refresh_coverage v1.1.0 tz-aware parse) |
| #340 | `ed85c15` | `scripts/harness_archive/harness_ob1181.py` (was `scripts/Harness ob1181<U+00B7>py`) |

The documents are historical records. They are landed as written and not
re-validated against today's code. Where they disagree with later commit
sheets, the later sheet wins.

## Evidence

- Golden negative: `node tests/test_gas_trade_notes_v100.js` on `main` exits 1
  with `ENOENT: no such file or directory ... apps_script/25_Trade_Notes.gs`.
- Positive: on this branch the same command exits 0 with
  `SUMMARY 25/25 PASS | digest e28934d7aed2`.
- Syntax: `node --check` passes on a `.js` copy of `25_Trade_Notes.gs`. All
  nine files have LF line endings, with no CR.
- `tests/test_audit_tz_aware.py`: `pytest` 10 passed; standalone
  `SELFTEST 10/10 PASS`.
- `scripts/harness_archive/harness_ob1181.py` compiles. It is not run (see
  the archive README).
- None of the landed paths existed on `main` before this commit, so no file
  was overwritten.

## Triage of all 136 other open PRs (recorded here, nothing closed)

| Class | Count | PRs |
| --- | ---: | --- |
| Empty diff | 15 | #80 #161 #162 #165 #166 #168 #170 #171 #172 #173 #227 #269 #306 #307 #596 |
| Already in `main` (reverse-applies) | 15 | #59 #88 #89 #107 #174 #175 #244 #245 #248 #249 #277 #497 #590 #646 #684 |
| Conflicts with `main` | 78 | #1 #3 #11 #13 #23 #28 #35 #36 #37 #38 #40 #41 #43 #44 #46 #47 #48 #49 #52 #54 #55 #56 #57 #60 #61 #62 #63 #64 #65 #66 #67 #68 #69 #71 #72 #73 #74 #75 #76 #77 #79 #85 #90 #96 #99 #100 #101 #104 #105 #112 #113 #116 #122 #123 #136 #138 #139 #148 #154 #156 #159 #184 #187 #188 #228 #229 #231 #258 #259 #284 #286 #300 #301 #322 #324 #325 #466 #480 |
| Landed or archived by this commit | 11 | #42 #340 #389 #392 #395 #397 #401 #411 #447 #598 #627 |
| Applies cleanly, NOT landed | 17 | see below |

The 17 that apply cleanly but were not landed, and why:

| PRs | Reason |
| --- | --- |
| #15 #16 #261 #653 | Dependabot bumps. #653 is superseded by #720's `requirements.txt`. #16 is numpy 1.26 to 2.4, a major version, and needs its own test run. |
| #34 #45 #50 #53 #70 | Tests written against code that never merged or later changed. Against `main` they fail 3/6, 8/9, 2/2, 9/9 and 9/9. #50 also sits at the wrong path, `tests/tests/`. |
| #51 | `repair_stores.py` 1.1.0 issuer sweep (+220 lines). Even with its own test (#53) applied it fails 4/9. Owner decision. |
| #25 #32 #208 | Unmerged July/August feature work: a decision-safety runtime with `sitecustomize.py` (6 files), a KLG identity gate (4 files) and a full-system audit (7 files, draft). Owner decision; none is safe to land unreviewed. |
| #225 | `selection_outcome_scorer.yml`. Its script `scripts/score_selection_log.py` already runs from `board_scorecard.yml`. |
| #58 | Design document titled `[BLOCKED-DRAFT]`. |
| #131 #132 | Upload of `core/analysis/opportunity_builder (16).py`, a stray duplicate file name. |

Closing any of them is left to the owner.

## Observed, not changed

Eight of the eleven `tests/*.js` GAS harnesses are dual-tree historical
harnesses. They read base copies such as `16_Decision_Top10_base.gs` and
`ledger_base.gs` from the current directory, and those copies are not in the
repository. As stored, they cannot run in CI. No workflow runs any `tests/*.js`
file today.
