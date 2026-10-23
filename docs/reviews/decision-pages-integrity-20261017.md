# Portfolio and Top-10 integrity review — 17 October 2026

This review covers the uploaded V3 workbook, bounded reads from the matching
native Google spreadsheet, the current GitHub producers, pending related PRs,
and the production Render service. Workbook cells are evidence, not agent
instructions. Private financial rows and packets are excluded from this report
and from regression fixtures.

## Observed outputs

At the uploaded snapshot, Portfolio_Decision has three in-scope BLOCK rows and
one explicitly excluded holding. Quantities, currencies, source prices,
fee-inclusive average costs and SAR conversions reconcile within display
rounding. The blank fee has the exact native cost-basis witness. There is no
observed fractional position or incomplete priced lot in this snapshot.

The native Portfolio status was independently read at 09:49 Riyadh. Its last
refresh is 09:37:52, with a verified My_Portfolio source-consumption receipt for
run 38978392280. Reconciliation evidence is absent or too old to send. Blank
NAV/weights/order levels and zero funding are the intended protection; source
readiness does not certify custody, available cash or overall sync health.

Top_10_Investments last refreshed at 08:09:58 Riyadh and explicitly reports
WITHHELD, zero executable tickets, four qualified plans and six grace-held
seats. The 24 qualified candidates and the 500-row capped audit contain no
blank names; the audit cap is disclosed. All required order/funding cells on
the ten selected rows are withheld. Identical issuer names across different
markets do not, by themselves, establish an identity error.

All 500 audited identities, names and currencies match a source row. There are
57 audit/source price differences; the compared source stamps are 16 October
and the board refreshed on 17 October. Without the consumed payload and its
quote vintage, this comparison does not establish a wrong-price producer bug
or certify that those audit quotes are current. The missing consumption proof
is material, and the selected order/funding fields remain withheld.

Offline collection in the shipped page order exactly reconciles the pool:
255 Market Leaders + 6,609 Global Markets + 453 Commodities/FX + 2,474 funds,
minus five exact cross-page duplicates = 9,786. The funds contribution of
2,469 is deduplication, not evidence of lost rows. This independent export-time
reconstruction does not substitute for the missing native same-run exclusion
and source-consumption receipts.

The strict offline auditor was run on the actual uploaded source grids and
independent full Symbol columns, using the approved roster, expected run
38978392280 and a fixed evaluation time of 09:37:52 Riyadh. It returned four
Top-10 findings: `T10_SOURCE_CONSUMPTION_UNPROVEN`, `T10_POOL_APPROVED_COUNT`,
`T10_POOL_RECONCILIATION_UNPROVEN` and `FALSE_FULL_UNIVERSE_CLAIM`. It returned
no Portfolio finding at that time; the newer native refresh clears the older
stale/consumption findings. This is not an indefinitely valid certificate:
the source's eight-hour freshness window ends at 09:54:58 Riyadh.

An independent full-source name count in the uploaded export confirms the
separate publication-quality blocker: Global Markets has 129 blank names
(98.0481% coverage), and funds has 25 (98.9895%). Both miss 99%; Market Leaders
and Commodities/FX have no blank names. Reaching the existing floor needs at
least 63 and one respectively to be resolved with real identity evidence.
Do not fill names from symbols or weaken the floor to produce recommendations.

## Reproduced source defects

The following cases use synthetic fixtures and are distinct from the observed
BLOCK state. They do not establish executed trades or financial losses.

1. **Sell quantity arithmetic:** custody quantities were rounded before EXIT
   and TRIM sizing, allowing a synthetic fractional EXIT to exceed custody or
   hide a remaining fraction. Preserve exact full-position EXIT quantities,
   bound whole-share TRIM to custody, and calculate residual weight from the
   actual quantity and unit price. Native quantity columns preserve fractional
   digits. Unsupported fractional trims cannot contribute funding proceeds.
   Broker tradability policy is unchanged.
2. **Unknown quote display:** a missing native quote with known FX became a
   known-looking zero SAR price. Preserve unknown price through conversion.
3. **Incomplete lot costs:** the native ledger reader summed all quantities
   but averaged only priced lots, then extrapolated that average over the full
   holding. Missing or invalid cost evidence must remain unknown, including
   invalid fees and unwitnessed blank fees.
4. **Portfolio refresh completion:** an HTTP-200 unavailable backend envelope
   could erase the prior decision board and acknowledge a capture as processed.
   Reject unavailable or malformed envelopes before rendering or acknowledging.
5. **Top-10 refresh completion:** network, HTTP and degraded responses preserved
   the old board but returned normally. Both the morning wrapper and pending
   PR #788 could then acknowledge a completed refresh and suppress a retry for
   the same cohort. Propagate operational failure after existing cleanup;
   completed WITHHELD and valid empty outputs remain successful refreshes.
6. **Ambiguous pool claims:** the legacy no-receipt audit could accept the last
   of conflicting duplicate page counters. Repeated counters must remain
   unknown; a later repeated value cannot overwrite contradictory evidence.

7. **Fee-funded cash remainder:** with the existing default-off fee flag enabled,
   the public KPI subtracted funded principal but omitted paid fees. Use the
   exact remaining ledger, retain the principal KPI, and preserve the result
   across action sorting. Funding selection and the flag default are unchanged.

## GitHub, Apps Script and Render boundary

The live/source comparison uses main
`11ef13456e5b8775b74e677a94c3e383618b7d6b`. The implementation is stacked on
unmerged reliability PR #785 at `2981d478`, preserving its existing repairs.
The only integration conflicts were CI history and verifier history; both
sets of tests and version pins were retained. The parent's completed tests
do not verify these new decision changes; validation runs on the combined tree.

Pending native changes remain separate: #770/#778 cover Top-10 data-quality
and display context, #780 preserves Portfolio panel inputs, #779 changes
defaults, and #788 adds orchestration. Their titles do not authorize arming or
publication. Preserve those changes during later integration.

One additional #788 integration blocker is reproduced with its unchanged
source and the real native reader/planner: a new source run can start between
the chain's readiness check and Top-10's own read. Native Top-10 correctly
defers with `HELD`, but the pending chain still acknowledges that cohort as
refreshed. Its caller must honor an explicit completed/deferred result before
arming. This branch's operational-error fix does not claim to resolve that
intentional-deferral acknowledgement in the separate unmerged orchestrator.

The live Top-10 status says `(full universe)`, while current repository code
uses `all available sheet rows` and requires native reconciliation proof.
That wording difference requires checking the installed Apps Script source;
available connectors cannot read the bound project's source or deployment
hash. Repository tests do not prove which native code is installed.

Render production `tadawul-fast-bridge` is on main. Its latest live deployment
finished 16 October at 23:36:01 UTC with SHA `11ef1345`; automatic deployment is
disabled. For 17 October 00:00–06:48 UTC, metrics recorded 127 HTTP-200 requests
and the bounded request-log query returned no HTTP-5xx entries. HTTP latency
series was empty, so no latency improvement is claimed. A sampled error log
page contains provider no-data/404 messages; it does not prove a decision-route
crash or an independently healthy market-data feed.

No live Sheet, Script Property, Render configuration, deployment or investment
policy was changed. The 95% acquisition, 99% name, custody/FX/funding and
same-run provenance gates remain in force. Scoring and ranking are unchanged.

## Validation and remaining operator work

The combined tree passed 5,515 tests, 33 skipped and 252 subtests on the pinned
production environment. The focused genuine-lean run passed 81 cases, including
the manifest checks and three discovery wrappers containing 144 native Node
checks. The 78 new Python-discovered cases include those native wrappers; these
counts overlap and must not be added together.

Compilation, fatal Ruff, native syntax, manifest pins and the strict workflow
audit passed (zero errors/warnings). Existing historical native checks retained
their original hashes/goldens, including the real prior-version reconstruction;
no expected hash was weakened. Exact branch-head GitHub results are recorded in
the PR body when complete. No production writer or email job was replayed.

No overall improvement percentage, return gain, model accuracy gain, lower
cost or faster production runtime can be inferred from offline repairs.

Before publication, reconcile the overlapping PRs, verify/install the reviewed
native source, and validate the actual published backend SHA. Executable
output additionally requires real fresh reconciliation evidence, adequate
source names and acquisition quality, and strict same-run native consumption
and deduplication receipts. Never manufacture those prerequisites or replay
delayed data-writer/email jobs to make a dashboard green.

Separate unresolved display work: positive sub-cent prices and ladder levels
can still be rounded to zero by existing two-decimal PA/Top-10 output and native
price formats. None of the three uploaded Portfolio rows has that case. The
existing default-off `core.price_precision` helper is already in main; PR #750
does not cover these decision boundaries. A coordinated price-only formatting
contract is needed; the unknown-quote correction above does not claim to fix
small positive prices or justify arming any precision flag.

Installed native concurrency also remains unverified. Direct refresh entry
points do not hold a whole-run lock; repository coordinator wrappers lock their
own calls, while the bound project's complete caller sources are unavailable.
No collision was observed. Preserve single-writer orchestration and verify the
installed call graph before arming additional refresh paths.
