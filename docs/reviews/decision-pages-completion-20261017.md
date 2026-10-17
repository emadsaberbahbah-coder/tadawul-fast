# Decision-page completion follow-up — 17 October 2026

This is a new implementation phase authorized after the bounded workflow
observation window ended. It builds on PR #791 (`afbfc13b`), which in turn
preserves PR #785 (`2981d478`). It does not extend the observation window or
authorize publication, live settings changes, trades or weaker quality gates.

## Refresh completion

The pending PR #788 orchestrator could record a Top-10 refresh as complete
when the native producer intentionally deferred because a newer source sync
had started. The readiness check and native consumption are separate reads;
a successful earlier check is not proof that the planned cohort was rendered.
The follow-up integrates the default-off orchestrator and binds completion
to the producer's explicit result and actual source cohort. A legitimate
deferral must not consume the operational-failure retry budget or acknowledge
the cohort. A stale worker must not clear another worker's claim.

A render without matching consumed-cohort proof is `UNVERIFIED`, not a
refundable no-action deferral. It retains the bounded attempt budget so a
configuration/evidence problem cannot trigger an endless sequence of backend
requests. If completion state cannot be committed, do not acknowledge success,
refund a different owner's reservation or blindly perform a fourth refresh;
report the unverified state for reconciliation.

Completion identity includes the actual publication/finalization stamp as
well as the workflow run number. A new publication during a rerun of the
same GitHub run must not be suppressed as already refreshed. An old completion
record without that stamp is unknown for the same run and needs reconciliation;
it is neither replayed blindly nor silently accepted as verified completion.

Execution uses the existing manual/automatic coordinator's whole-callback
lock and pause recheck. The candidate-tab writer retains a lock inherited
from its caller instead of releasing that caller's lock. Missing coordinator
support, ambiguous/nonterminal feed stamps or invalid persisted state cannot
authorize another writer. HTTP/network failures retain the bounded operational
retry budget; a source deferral is separate from those failures.

The runner also writes composite decision-feed stamps before final audits.
A same-run `EXECUTABLE` or generic `NOT_ACTIONABLE` value therefore does not
prove finalization. The chain requires a recognized final-audit/release witness,
rejects early supersession/downgrade markers, and checks its timestamp against
the actually consumed source generation. Reusing a run number in a workflow
rerun cannot make an older finalization stamp current. A healthy audit that
does not publish a recognizable completion witness remains unverified; it must
not be inferred from a green HTTP request or a matching run number.

This contract distinguishes a completed rendered `WITHHELD` result from an
unrendered deferred attempt. Neither grants permission to fund an order.
Native concurrency outside the repository's known callers remains unverified;
the bound project's complete call graph is required before enabling another
automatic writer.

## Price-only precision

The existing backend `TFB_PRICE_PRECISION` option already preserves seven
significant digits for small prices, but decision output could round those
values again to two decimal places. Native price formats could then display
a positive value as zero. The repair extends the existing option across the
decision boundary; it does not introduce a separate native flag.

Backend payload metadata signals the enabled mode to native rendering. When
the option is off, the metadata is absent and legacy price precision remains.
Monetary totals, fees, returns, scoring, ranking and investment eligibility
retain their existing contracts. No option is enabled by this change.

The compatible default-off Portfolio panel-preservation implementation from
PR #780 is retained: enforce preserves an identified nonblank operator input
and aborts before a layout clear if its snapshot fails. It does not recover
lost inputs or certify stale cash. The default-off native held-seat audit
handoff from #770/#778 is retained as integration context; its backend
`TFB_OPP_AUDIT_KEEP` companion remains in the separate pending backend bundle.
Do not arm a native half and assume the backend feature has been deployed.

## External prerequisites

The uploaded workbook's name gaps remain an evidence problem: 129 Global
Markets names and 25 fund names are blank. At least 63 and one respectively
need verified identities to meet the existing 99% floor. A symbol or a stale
caption is not a substitute for an authoritative identity. Do not weaken the
floor or manufacture names to produce an executable board.

Replaying the existing reviewed-caption registry on the blank-name subset
produces zero applicable replacements: 20 matches conflict with the exact
Exchange metadata fence, one lacks the required nonactionable-state evidence,
and 133 are unregistered. Case normalization alone cannot reconcile those
Exchange changes. Keep the fences intact until authoritative instrument names,
identifiers, listing state and current metadata receipts resolve the conflicts.
PR #789 supplies bounded diagnostics, not identity repairs.

Fresh account-scoped reconciliation evidence, native same-run source
consumption and deduplication receipts are still required. Earlier snapshot
clearance has a freshness window; it is not current approval. Missing
reconciliation must remain unknown and executable funding must remain withheld.

Render automatic deployment is disabled and the installed Apps Script source
hash is unavailable through the connected tools. Repository tests establish
the reviewed code's behavior, not which code is currently installed. Before
publication, compare the complete bound project with these sources, preserve
unrelated files and caller locks, and verify the resulting backend commit and
strict native receipts. Publication requires separate explicit authorization.

The bounded connected-Drive search found no authentic `02_Core.gs`,
`13_AutoRefresh.gs` or matching project export. Workbook metadata is readable
but exposes no bound source, and no Apps Script content endpoint is available.
This is an access/evidence limit, not proof those scripts are absent.

The publication sequence is reviewable code first: preserve the stack
`#785 -> #791 -> this follow-up`, then reconcile overlapping PRs before any
authorized merge/deploy. Verify backend SHA and pinned module versions after
deployment. Compare the full bound project before replacing the reviewed
native files, retain unrelated sources/properties, and confirm that automatic
callers share the coordinator rather than installing a second writer.
Keep the chain off while inspecting source versions and trigger ownership;
use its read-only status without treating an unevaluated automation guard as
a positive execution preview. Any later observe/on transition is an explicit
operator change, followed by actual source-bound consumption receipts and a
strict freshness/publication audit on that new run.

## Independent stored-backup check

The actual Drive object from backup run `38990567104` was downloaded through
the connected read-only interface, without re-exporting the live Sheet.
Its 23,735,837 bytes and SHA-256 match the same-run export/DQ receipt. All
171 ZIP members pass CRC reads; all 168 XML/relationship parts parse, and the
required OOXML bindings are present and unique. The 52 actual tab names match
the same-run inventory exactly: no missing or extra tabs, six of six core
tabs, nine visible and 43 hidden. The inventory's 53rd entry is a dictionary
ghost, not a lost backup tab.

An isolated openpyxl read in both formula and cached-value modes successfully
opened every tab using bounded reads. No financial cell values are retained
in this report. This verifies stored-byte integrity and isolated readability;
it does not verify Google import/recalculation, full semantic equivalence,
Apps Script, trigger or permission restoration, or a production restore.
The DQ audit remains an observe-mode failed measurement, so an intact backup
does not certify healthy business data.

## Validation

The combined working tree passed **5,550 tests, 33 skipped and 252 subtests**
on the pinned production environment. The three pytest return-value warnings
are in unchanged legacy tests. The genuine lean run passed **38 cases**,
covering the new regressions and manifest pins. The 35 newly discovered
Python cases include native batteries with 248 checks; those counts overlap.

Compilation, fatal Ruff, all five native-source syntax checks, manifest pins,
CI/Render contracts and the strict workflow audit passed (zero audit errors
or warnings). Independent reviews closed the reproduced retry, ownership,
same-run identity, invalid-clock and finalization-witness findings. Existing
historical expected hashes and the prior panel golden remain unchanged.
Exact-head GitHub results are recorded in the PR body after completion.

No production writer,
email replay, provider retry, baseline bootstrap or live deployment is part of
this implementation.
