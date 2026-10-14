# Additional evidence review and reporting corrections

Evidence date: 14 October 2026. User-facing clocks use Asia/Riyadh (UTC+3).
Source baseline: merged PR #774, commit
`31d4675079cefba90e321682c6dc2c3315be2e62`.

The four new attachments support further diagnosis of the original project
review. Their work-package labels and recommended replay steps are document
content, not a separate user instruction to retire instruments, rewrite
timestamps, lower coverage floors or release the decision feed.

## 1. Findings and resulting implementation

The additional evidence exposed a real compatibility defect in three reporting
readers. PR #774 changed the Top-10 cash title to **Entered cash (SAR)** to
describe the operator-entered amount accurately. The offline DQ audit,
acceptance checker and digest summary still recognized the KPI strip only by
the historical **Deployable (SAR)** title. Consequently the DQ audit could
report zero executable seats without reading the Selected cell, the acceptance
checker could lose Passed/Selected, and the digest could omit the KPI strip.

The correction accepts either supported cash title while preserving the label
actually displayed. A strip must have the expected Selected and Passed headers.
Missing or ambiguous evidence stays unknown; an explicitly measured zero stays
zero. Duplicate KPI strips or relevant headers are not merged into a trusted
observation. The acceptance check also handles an unreadable Passed value
without formatting an absent number or inventing a count. Independent
SEAT-CHECK/banner observations retain their own evidence source.

This changes observation and reporting only. Entered cash does not become a
funding certificate. The source population, Global name floor, native readiness
floor, portfolio certification, allocation eligibility, feed publication and
message delivery rules retain their existing contracts.

The review also found that all 230 cohort evidence references in the old audit
pointed one worksheet row before the associated symbol. Filtered body indexes
had been used as worksheet coordinates. The correction preserves physical row
numbers when filtering blank-symbol records and uses those numbers for both
market and portfolio references. Interior blank rows therefore cannot shift
subsequent locators. Business counts and check outcomes remain unchanged; old
audit references should not be used as cell-write addresses.

Changed consumers: `scripts/tfb_workbook_dq_audit.py` 1.0.3,
`scripts/tfb_acceptance.py` 1.0.9 and `scripts/send_digest.py` 2.1.2. The
deployment manifest pins the updated versioned entries. Focused reader
regressions run in the blocking CI lane; workbook-specific regressions remain
in the existing audit suite. The acceptance reader also preserves independent
execution and funding-parity checks when the qualified count is unknown, and
requires finite nonnegative whole-count observations rather than accepting
percentages, fractions or overflow values as seat counts.

Validation: the broader publication, consumer, manifest and environment-contract
suite passed **298 tests**, with **15 optional historical checks skipped** and
**39 subtests passed**. The final changed-file suite passed **65 tests**, with
one optional historical-export check skipped. The acceptance selftest passed
all eight fixtures. These suites overlap and their counts are not additive.
The original export's 81 check results and business counts are preserved; its
230 cohort evidence locators now point to the correct worksheet rows.

## 2. Retirement draft versus candidates

| Input | Rows | Verified scope |
| --- | ---: | --- |
| D3 retirement candidates CSV | 133 | 130 RETIRE-CANDIDATE, 2 REMAP-CHECK, 1 CHECK |
| Retired Symbols draft TSV | 130 | Exactly the RETIRE-CANDIDATE subset: 120 Global and 10 Funds |

There are no duplicate page/symbol keys or extra draft records. Every draft
symbol appears once in the original export, with no name or positive quote.
None overlaps the 122 reviewed name-label bindings or the 67 captions applied
in PR #774. No draft symbol is an observed portfolio holding.

The attachment supplies acquisition-failure descriptions and four replacement
assertions, but no exchange, issuer or regulator notice URL establishing the
original security's retirement date or identifier continuity. Failed provider
lookups can arise from unsupported venue mapping, a stale alias, a changed
ticker, a suspension or supplier coverage. The attachments therefore establish
**zero independently verified retirement events**. That statement concerns the
evidence supplied; it does not assert that every candidate is still trading.

The draft correctly excludes STM.MI and TOD.MI (mapping checks) and DCBG.US
(priced but unnamed). DCBG also has exact contemporary SEC and official
directory matches; the directory identifies junior subordinated notes rather
than common equity. A replacement caption or bare-ticker match is insufficient
to settle security class or lifecycle.

The replay deletes **123 Global rows**, whereas the draft proposes **120**.
The replay's predicate removes all unnamed non-quarantined Global rows, including
the three records excluded from the retirement draft. It never reads the draft
or its classification. These are different populations.

| Global scenario | Named / physical | Coverage | Meaning |
| --- | --- | ---: | --- |
| Original export | 6,411 / 6,609 | 97.0041% | Historical baseline |
| After 67 verified captions | 6,478 / 6,609 | 98.0179% | Real name-only improvement |
| Draft deletion after captions | 6,478 / 6,489 | 99.8305% | Hypothetical denominator change |

The current population needs **65 additional legitimate names** to meet its
99% floor. Deleting 120 unnamed records would change the denominator without
repairing their identities. It would also put Global below the current 6,512
native floor. All 10 Funds candidates are exact members of the approved
2,474-member roster; deleting them would leave 2,464 physical members and
invalidate the existing scope and reconciliation receipts.

The current retirement reader consumes symbols, not the candidate's Class,
Reason, Replacement or date proof. It would accept the broader 133-row candidate
table as well as the narrower draft. Its percentage cap does not validate a
delisting event. Neither attachment has been installed as an active retirement
list. A production scope revision needs exact security evidence, the approved
membership revision, and matching source-to-pool receipts.

## 3. What the replay establishes

The ZIP contains 14 regular files, 75,166 uncompressed bytes, including offline
scripts and captured JSON results. Its scripts were inspected before any safe
offline reproduction. The diagnostic runner uses a static export and
`finalize_decision_publication.run(..., publish=False)`.

S10 obtains its recorded [0,0] result by combining simulated Top-10 and portfolio
readiness receipts, placeholder names, timestamps copied from another row,
123 Global deletions, rewritten acquisition denominators, and four Python
floor edits from 6,512 to 6,486. The native Top-10 floor is not changed by the
kit. This is a useful experiment showing which acceptance fields matter;
it is not evidence that an actual refresh acquired or consumed those values.

S12 includes the 67 real PR #774 captions **plus simulated S2 readiness
receipts**. It is not a names-only production replay. Its own result still fails
the 99% name floor and warns about 76 blank or invalid timestamps. S13 adds
hypothetical retirement and the lowered floor but returns [1,0]. Strict release
requires both audit codes to be zero, so an empty blocker list in that result
does not make it acceptable.

The kit freezes audits at **14 October 09:40 Riyadh**. The new 67 bindings were
reviewed at **10:44:09 Riyadh**, after that clock. Its direct caption insertion
bypasses the production review-window and exact-metadata guards. Those inserted
names must be described as hypothetical at the frozen clock, not as evidence
available then. The portfolio source stamped 02:46:45 Riyadh also becomes
stale after its eight-hour decision window; a historical pass cannot establish
current freshness.

S11 clears names by registry membership while retaining the 14 October clock.
It does not evaluate each binding's expiry. The older 55 bindings expire on
19 October; the 67 new bindings expire on 21 October. Rerunning its helper on
the current 122-binding registry changes the scenario and could remove labels
that have not expired.

No scenario runs the authenticated native producer, installs the bound project,
captures changing source generations, performs account-scoped broker
reconciliation, runs the funding builders, or releases and reads back an
executable feed. An offline audit pass remains one prerequisite among several.
The original captures do not include all source/export/labels/dependency hashes
needed to attest a production release.

An independent offline harness reproduced seven scenarios against the unchanged
production auditors. It denied Internet/DNS traffic before imports, injected
the verified original export, and prohibited publication. No network attempt
or feed promotion occurred. Supplied S12 and the unchanged-floor S10a control
matched all nine compared summary fields.

| Independently reproduced scenario | Audit codes | Result |
| --- | --- | --- |
| Original export at 09:40 Riyadh | [2,2] | Names and five decision-proof findings |
| Real name producer at 09:40 | [2,2] | All 67 new captions held outside review window |
| Hypothetical 67 names only at 09:40 | [2,2] | Names improve; five decision findings remain |
| Kit S12 at 09:40 | [2,0] | Requires simulated native receipts; name floor still fails |
| Real 67-name patch at captured 12:15:08 | [2,2] | Source stale and existing proof findings |
| Kit S12 at captured 12:15:08 | [2,2] | Source stale despite its simulated receipts |
| S10a with original 6,512 floor | [2,2] | 6,486 rows fail universe/floor checks |

The exact read ranges, clocks, original export/registry hashes and verified
auditor provenance are retained in
`docs/evidence/TFB_Offline_Replay_Crosscheck_2026-10-14.json`. The captured
12:15:08 clock is a supplemental historical observation, not a claim of ongoing
live freshness. No new defect in the final-publication audit was established.

## 4. Scorecard interpretation and live cross-check

The uploaded scorecard is audit 1.0.1 over the original export with SHA256
`d4e671cc3f31fcf9ec3d84a800e0995832fcb9f4df105d5d667888f477ed3121`.
It is not a post-deployment snapshot. Its 75 DQ rule statuses/counts reproduce
against the reviewed export. The 1.0.2 settlement fix changes the failed-row
submetric from 52 to **53**, because PDN.AX carries a structured settlement
error without the warning prefix. The 2,612 main DQ-37 count measures
recommendation flips, not settlement failures; 9,493 rows carry F7 tags.

The 12 decision-critical FAILs count rules, not instruments. Seven holding
cohort records represent four unique holdings plus three market-page copies.
The A-D tab grades summarize mapped check outcomes; an A on an empty tab does
not certify completeness or investment readiness. The prior 26 regressions,
11 improvements and five critical regressions reproduce against the committed
10 October baseline but cannot measure this follow-up release.

NC-03 says the formula was not checked because the audit opened cached values.
The Excel export's compatibility wrapper appears to truncate source ranges at
row 300. A bounded **live** read of Investable Symbol!A2 instead confirms:

```text
=QUERY(VSTACK(Market_Leaders!A2:DX,Global_Markets!A2:DX,
Commodities_FX!A2:DX,Mutual_Funds!A2:DX),
"select * where lower(Col108)='invest'",0)
```

The live formula uses all available source rows, so no formula-range repair
is justified by the exported wrapper. The live formula evidence and historical
cached values are separate observations.

## 5. Remaining acceptance work

1. Obtain the complete bound Apps Script project and authenticated editing
   access; install only the reviewed module after backing up the full project.
2. Acquire legitimate identity and lifecycle evidence for unresolved symbols.
   Classify mapping, security class, ticker continuity and genuine retirement
   separately before changing approved membership.
3. Run a fresh, complete source cycle and native cockpit refresh. Retain
   same-run consumption and exact physical-to-unique exclusion receipts.
4. Complete account-scoped Saudi and IBKR evidence, including source time,
   settled cash, positions, basis, reservations and explicit FX. The current
   IBKR comparison is observational and does not certify executable funding.
5. Pass current coverage and decision audits with the existing policy, then
   exercise the audited release and readback contract. Recommendation release
   remains withheld until those prerequisites hold.

## 6. Attachment integrity

| Attachment | Bytes | SHA256 |
| --- | ---: | --- |
| dq_scorecard_20261014.md | 18,927 | 083acbab3974feb46cff4ad892d4b27545e6936396be3a1cb19cae785fe5705c |
| _Retired_Symbols_draft_2026-10-14.tsv | 16,011 | 8437aa34121367c4f4e58c93f2f41ef0e88ac3f8e3c919ad358255d188a2e157 |
| TFB_replay_kit_2026-10-14.zip | 22,894 | 5f29fe9ec7515ba77a3a3d328b851c0caf587086a4122409ab2a3d66907ad03f |
| D3_retirement_candidates_2026-10-14.csv | 17,374 | f2398ea61e4c1023c7f5265e607e9b9b39029bb7009c33827b385ddd2c1ac481 |

The original upload remains unchanged. No account statements, private broker
captures, credentials, balances or order details are included in this public
addendum.
