# Provider-reference and verified-name workflow recovery

Audit baseline: `561db60c74c1d56d866d4b72756f8ef9a9df57d5`.
Review cutoff: 24 October 2026, 08:05:38 UTC / 11:05:38 Riyadh.
These findings distinguish working failure controls from execution defects.

## Observed provider-reference failures

Every one of the seven scheduled provider audits in the reviewed week failed
with exit 2, `CH_BASELINE_EMPTY` and `CH_BASELINE_MISSING` for Market_Leaders
and Global_Markets. The audit code selftests passed and the source reads
succeeded. Each restored the same cache `ptc-lastgood-35091988613`, whose
reference timestamp is 18 August and whose accepted `pages` map is empty.
No eligible reference update was written. Re-running the schedule cannot
initialize this reference because scheduled initialization is forbidden.

The seven inspected runs were `39026626201`, `39122743513`, `39260943758`,
`39417521964`, `39596075562`, `39775139988`, and `39948573365`.
[Latest provider audit](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39948573365).
No provider coverage percentage is approved as a baseline by this review.

### Recovery prerequisites

1. Complete and independently verify one coherent acquisition/source cycle:
   required pages, full row scope, current quote/session evidence and source
   receipts must agree. Resolve the current live control failures first.
2. Review `provider_target_observation.json` and the applicable provenance
   policies. Retain the restored reference and current reports as evidence.
3. Only after that review, explicitly dispatch `provider_target_coverage.yml`
   once with `bootstrap=true`, `accept_baseline=false`, on the intended main
   revision. Bootstrap is a proposal here, not an action performed by this
   patch. Do not use lower-baseline acceptance to initialize an empty map.
4. Verify the report's exit, source scope and `persist_status=written`, the
   nonempty schema/config-bound reference, and the actual cache-save result.
   A later scheduled audit must restore that same valid reference and compare
   against it. Existing freeze, row-drop, source and age safeguards stay active.
5. If read evidence is missing, the reference is wrong/corrupt, or coverage
   fails, retain the failure and repair that condition. Do not delete or
   overwrite the old cache merely to produce a green run.

## Observed name-label checks

The three scheduled name checks correctly failed due to authority expiry,
with no Python exception: run `39576601014` reported 122/122 bindings within
48 hours of expiry; `39755458529` reported 4/122; `39931436275` reported 4/124.
[Latest failed check](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/39931436275).

Manual renewal `39584281713` re-captured sources and renewed 118/122 bindings;
four were held unchanged: PE&OLES.MX (caption not corroborated), RCM.US,
SILK.US and VFLQ.US (HTTP fetch failures). The reviewed branch path succeeded.
Its workflow success means a partial candidate was produced, not that every
authority review passed. The newer main registry has since repaired VFLQ.US.
At this audit cutoff, three current bindings remain overdue: PE&OLES.MX,
RCM.US and SILK.US. Old run counts must not be presented as current counts.

Dispatch `name_label_renewal.yml` in `mode=renew`, `publish_branch=true` only
to re-capture the actual public authority sources. Review and merge the
resulting candidate. Supply a legitimate declared SEC contact User-Agent via
the existing secret when required. A rejected/unavailable source or changed
caption stays held; never change expiry dates, identities, quarantine flags
or source digests manually to clear the check. Some unresolved historical or
terminated identities may require a new explicit identity review rather than
an automatic renewal of their old caption.

## Implemented changes and limits

- Provider audit 1.2.1 emits a recovery proposal in its existing JSON reports.
  Unknown measurement and coverage failures take priority over a bootstrap
  proposal. Wrong/stale/corrupt references require investigation/restoration.
- Its workflow preserves red exit codes and always publishes the actual
  finding codes, page measurements and required recovery path in the summary.
  The existing supported base64 Sheets credential fallback is also mapped.
- Name renewal 1.1.1 distinguishes already-expired bindings from the advance
  warning window and identifies the proposed verified-review workflow.
- Its summary always identifies expired/due bindings and marks a partial
  manual renewal as `PARTIAL`, with each held reason. Verified partial review
  branches remain supported; held bindings stay byte-identical.

These changes do not bootstrap, accept a baseline, change registry entries,
make provider calls, alter expiry/freshness thresholds, release a feed or
certify recommendations. Operational failures remain unresolved until their
specific recovery evidence passes. Offline tests cover the missing/empty,
wrong-reference, unreadable, coverage-loss, frozen, healthy, expired, due-soon
and partial-renewal paths.
