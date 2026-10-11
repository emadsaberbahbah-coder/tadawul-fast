# Apps Script source prerequisites

Three authentic `.gs` sources are present under `apps_script/`:
`11_Manual_Refresh_Coordinator.gs`, `16_Decision_Top10.gs` and
`24_Sync_Dispatch.gs`. `node tests/js/apps_script_syntax.test.js` compiles
every `.gs` found there with `vm.Script`, without invoking Google services.
The same test automatically covers additional authentic sources when added.

The bound-project files `10_My_Portfolio.gs`, `14_Portfolio_PnL.gs`,
`15_Lists_Config.gs`, `21_Portfolio_Ledger.gs` and `25_Trade_Notes.gs` are
still unavailable. Searches of all locally available refs with
`git log --all --name-only -- '*My_Portfolio*' '*Portfolio_PnL*'
'*Lists_Config*' '*Portfolio_Ledger*' '*Trade_Notes*'` and
`git rev-list --all --objects` found Ledger/Trade Notes evidence documents
but none of those five authentic source blobs. Older unrelated bridge/menu
sources in git history do not supply them. Evidence documents and archived
harnesses describe delivered behavior but cannot establish the currently
bound code.

The owner must export the current five files from the bound Apps Script
project, preserving their version headers and identifying the export date.
Tokens and credentials must remain in Script Properties. Before integrating
an export, inspect it for embedded credentials and compile it. Then restore
the archived Ledger/Trade Notes harnesses against the authentic sources and
their required fixtures. Add the Python/GAS FX-subunit lockstep test only
when the actual `15_Lists_Config.gs` seed list is available, and add the
Portfolio_Decision evidence-forwarding test against its real caller.

Syntax coverage does not establish function execution, FX lockstep,
reconciliation forwarding or live deployment. Those remain unresolved until
the current source exports and an operator diagnostics readback are available.
