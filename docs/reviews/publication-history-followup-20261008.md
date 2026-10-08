# Publication and history-read follow-up — 8 October 2026

This bounded follow-up starts from deployed main
`56d29882b0dd19e6594fcdf74d43335cf7fed6c1`. It addresses code witnesses
remaining after the failed-envelope and session-calendar repairs in PR #738
and the share-class compatibility repair in PR #740.

## Reproduced behavior

Actual dashboard-runner tests with fake external clients showed that a
successful Insights envelope with blank column labels, or a row containing
only a timestamp, could overwrite the prior view and return exit 0. The
nonempty-header-list and any-nonblank-cell checks do not certify the declared
table contract or meaningful analytical content.

The shadow scorer catches regret-ledger read failures as an empty list. Its
deduplication then has no prior fork evidence, and later writes may append a
duplicate fork or replace the summary. A Shadow_History worksheet lookup
failure similarly becomes empty history and bypasses prior evidence duplicate
protection. A failed read is not evidence that a ledger is empty.

## Bounded contracts

Dashboard sync **6.64.13** validates Insights schema, raw table shape and
required row labels before rectification or clear/write. It supports the
current repository schema and its explicit predecessor, preserves numeric
zero values and usable partial content, and retains legitimate empty-board
contracts. Invalid required Insights preserve the prior table and fail the
run.

Shadow scorer **1.9.3** distinguishes successful empty/header-only reads from
unavailable or malformed evidence. Both history reads precede board/price
acquisition, duplicate checks, gate evaluation and publication. Unavailable
evidence returns a fixed sanitized diagnostic and nonzero status, preserving
the prior gate, histories and summary. No historical evidence is repaired or
rewritten by this reader guard. Retained cumulative-index bases must be present
and finite; missing evidence cannot silently reset a base to 100. A retained
index of zero keeps that zero through geometric chaining. A table confirmed
blank by a successful read receives its canonical header alongside
the first appended records, without clearing or replacing existing data.

The upstream composite also validates per-page publication clocks. Explicit
numeric offsets are parsed as instants; missing, malformed, nonfinite, future
or offsetless/unverified times cannot certify executable data. The current
writer's SPACE/full-second/numeric-offset format and existing configured
trailing-page freshness window remain unchanged. This is timestamp integrity,
not proof that independent page writers belong to one coherent run.

Deployment verifier **1.0.39** pins both source versions. CI **1.0.13** adds
the actual-scorer history-read regressions to the existing required lean
verdict lane. Existing protective and policy checks remain required.

## Validation and acceptance limits

Owner and independent offline checks passed **97 Insights**, **66 history**
and **91 publication-clock** regressions. The required legacy publisher harness
uses current producer timestamps and retains an explicit unverified legacy-time
case. Exact selected CI results and the frozen tree are recorded in the PR's
validation receipt. A draft publication does not establish merge, deployment,
scheduled execution or native installation.

This change does not complete coherent cross-writer bundle activation,
cross-writer run identity validation, approved membership, installed
Apps Script parity, durable outcome storage, financial reconciliation or
chronological forecast evaluation. Those requirements remain in the
[audit status register](finalization-status-20261008.md). Confirmation,
settlement and margin policy activation remain separate evidence decisions.
