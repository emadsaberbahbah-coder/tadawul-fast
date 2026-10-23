# Deployment and provider-control review, 16 October 2026

Reviewed current main `11ef13456e5b8775b74e677a94c3e383618b7d6b`, with
read-only GitHub run/cache inspection and unauthenticated backend health checks.
No workflow was dispatched, no cache or workbook was changed, and no Render
setting or rollout flag was changed.

## Runtime and deployment

At 07:28 UTC, `/health` reported healthy and `/readyz` reported ready.
The deployed commit matched current main exactly. Runtime Python was 3.11.9,
matching `runtime.txt`; app, entry and service versions all read 8.14.5 and
the engine read 5.154.0. No route failures, missing required keys, initialization
errors, readiness reasons or startup warnings were reported.

The failed Core repair deployed readback run
[38655521187](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38655521187)
received HTTP 200 from `/health` but rejected a deployment commit that did not
yet match its workflow commit. That was a correct provenance rejection;
the present deployment matches. Do not disable the commit check to clear it.

There is no tracked Render Blueprint or reusable `workflow_call` definition.
`scripts/start_web.sh` 2.6.1 documents the Dashboard-managed start command and
`/readyz` check. The Procfile still attributed configuration to a nonexistent
Blueprint and described launcher features the current script does not have.
This review corrects only those comments; all process commands remain identical.
No runtime or requirements change is supported by the inspected failures.

## Provider-target monitor is missing an accepted reference

The scheduled provider-target runs on 13, 14 and 15 October
([38412531825](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38412531825),
[38560891464](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38560891464),
[38728887012](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38728887012))
all failed with `CH_BASELINE_EMPTY` and missing Market_Leaders/Global_Markets
baseline entries. Dependency installation, pure self-tests and page reads
succeeded. The latest observation read all four pages without truncation or
reader errors:

| Page | Rows | Provider-target rows | Provider share |
| --- | ---: | ---: | ---: |
| Market_Leaders | 255 | 100 | 39.22% |
| Global_Markets | 6,609 | 3,357 | 50.79% |
| Commodities_FX | 453 | 0 | Structural zero |
| Mutual_Funds | 2,474 | 0 | Structural zero |

The cache API showed one reference, `ptc-lastgood-35091988613`, 341 bytes,
created 16 September and last accessed 15 October. Its reference timestamp is
18 August and its accepted page map is empty. This is an unusable reference
that is still being restored, rather than an expired cache or failed page read.

The source cache run
[35091988613](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/35091988613)
failed its audit but successfully saved the baseline. Its historical workflow
used unconditional `if: always()` caching. Current main already fixes that:
it requires a nonempty page map, `persist_status == "written"`, and an accepted
exit/mode before saving a unique run-attempt key. The state path in restore,
audit invocation, validation and save is the same working-directory file.
The explicit bootstrap input is correctly mapped into the CLI's environment;
no persistence-path or write-permission failure appeared in the inspected runs.

The owner action is conditional: first obtain an independently verified healthy
full sync. Current Global_Markets evidence has 449 invalid/nonfresh rows, so
that prerequisite is not satisfied by a successful HTTP response or by the
backend's healthy status. Review the sync's full-refresh and final-publication
evidence before selecting an initial provider reference.

Once that prerequisite is met, dispatch **Provider Target Coverage Alert** on
main with `bootstrap=true`, `accept_baseline=false`, and the existing policy
settings unchanged. For an authorized operator, the equivalent command is:

```sh
gh workflow run provider_target_coverage.yml --ref main \
  -f bootstrap=true -f accept_baseline=false
```

This command is documented and was not run during the review. It needs no cache
deletion: the current controller can replace the existing empty initial state
when explicitly bootstrapped. Verify the resulting artifact has a nonempty
accepted page map, `mode="bootstrap"`, `exit_code=0`, and
`persist_status="written"`; verify the cache-save step succeeds and the next
scheduled run restores the new key. Do not automatically bootstrap from the
hourly monitor, accept a lower reference, or weaken the control to turn it green.

## Validation and remaining evidence limits

The provider controller self-test passed 16/16; all 17
`tests.test_workflow_audit_repairs` cases passed, including explicit repair of
an empty initial state and refusal to save failed measurements.
`scripts/render_env_contract.py --check` confirmed the committed contract is
current. Procfile noncomment process lines match the base commit exactly.

The runner's P/E firewall rejected and preserved 252 Global_Markets rows in
the latest recovery. Logs identify sample symbols but omit the price/EPS/P-E
triples and source receipts needed to prove a provider defect. Static inspection
confirms EODHD and Yahoo supply their own trailing P/E and EPS fields; a fresh
price does not prove those vendor snapshots share its instant. The engine's
looser check and runner's stricter check therefore do not justify changing the
runner tolerance. Count-only recovery telemetry is being added separately;
the retained acquisition proof must remain ineligible for fresh certification.

## Independent backup retention review

Reviewed the proposed `backup_workbook.py` 1.5.1 retention change separately
from its implementation. The new planner counts distinct dated names and the
delete loop visits each expired file ID once. Existing copies of retained dates,
nonmatching operator filenames and records without IDs are preserved. A dry run
still returns before upload, listing or pruning, and an upload failure still
returns before retention. Destination selection, credentials, GCS prefix handling
and all GCS transport helpers are unchanged.

Compared the actual base planner to the new planner over unique daily/monthly
names for 16 retention configurations: all results matched. This covers the
ordinary flat GCS layout, where object names are unique. The initial four real
`tests/test_workbook_backup_retention.py` cases passed across Drive, OAuth and
GCS dispatch. Integration expanded these to seven cases, including separate
daily/monthly retained-date counts; all seven passed. The script's offline
self-test passed 7/7. No blocking
retention defect was found in the change. No live storage operation was run.
