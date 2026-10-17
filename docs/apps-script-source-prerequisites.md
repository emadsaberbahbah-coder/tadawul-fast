# Apps Script source prerequisites

The repository contains authentic sources for `10_My_Portfolio.gs`,
`11_Manual_Refresh_Coordinator.gs`, `16_Decision_Top10.gs` and
`24_Sync_Dispatch.gs`. The decision-completion follow-up also integrates
`26_Decision_Chain.gs` from PR #788, with its completion contract repaired;
the source installs no trigger on load and defaults to off. Actual live properties
and trigger ownership require operator verification before enabling it.
`node tests/js/apps_script_syntax.test.js` compiles
every `.gs` found there with `vm.Script`, without invoking Google services.
The same test automatically covers additional authentic sources when added.

The bound-project files `14_Portfolio_PnL.gs`, `15_Lists_Config.gs`,
`21_Portfolio_Ledger.gs` and `25_Trade_Notes.gs` are still unavailable.
The complete current `02_Core.gs` and `13_AutoRefresh.gs` callers are also
unavailable, so the installed refresh call graph and its lock ownership
cannot be fully certified. Earlier searches of locally available refs with
`git log --all --name-only -- '*My_Portfolio*' '*Portfolio_PnL*'
'*Lists_Config*' '*Portfolio_Ledger*' '*Trade_Notes*'` and
`git rev-list --all --objects` found Ledger/Trade Notes evidence documents
but none of those four authentic source blobs. Older unrelated bridge/menu
sources in git history do not supply them. Evidence documents and archived
harnesses describe delivered behavior but cannot establish the currently
bound code.

Before publication, export the complete current bound Apps Script project,
preserving version headers and identifying the export date. Compare all
installed callers with the reviewed sources; the repository alone does not
prove installation. In particular, do not add a second automatic writer
while a legacy caller can refresh the same surface independently.
Tokens and credentials must remain in Script Properties. Before integrating
an export, inspect it for embedded credentials and compile it. Then restore
the archived Ledger/Trade Notes harnesses against the authentic sources and
their required fixtures. Add the Python/GAS FX-subunit lockstep test only
when the actual `15_Lists_Config.gs` seed list is available.
Portfolio_Decision evidence-forwarding tests now run against the authentic
`10_My_Portfolio.gs` caller; they do not attest the installed copy. Do not
replace the missing project files with guessed stubs.

Syntax coverage alone does not establish function execution, FX lockstep or
live deployment. Installed-source identity and reconciliation forwarding
remain unresolved until the current source export and an operator diagnostics
readback are available; the repository's offline forwarding tests are a
separate verified boundary.
