# TFB Commit Sheet - 01_Menu Decision Pages v2 block v1.0.0 [menu-bar refresh for Portfolio_Decision_v2 and Top_10_Investments_v2]

Date: 2026-10-19 (Monday, Riyadh afternoon). Owner request: "for this new 2 pages I need to be added in Menu bar as well to
enable me to make auto refresh when needed Top_10_Investments_v2, Portfolio_Decision_v2 ... add this to the 01_Menu.gs".
Delivered: `apps_script/01_Menu_Decision_Pages_v2_append.gs` (NEW, paste block), `tests/js/menu_pages_v2.test.js` (NEW,
31 checks), `.github/workflows/ci.yml` 1.1.26 (lean lane step), this sheet.
Not edited: every mirrored `.gs` file, the live `01_Menu.gs` body (the owner pastes the block and one line), triggers,
gates, Script Property values.

## Why it is a block with a setup step

- The live `01_Menu.gs` is not mirrored in the repository. The last copy read from the editor is v2.18.0 (17 Oct). A
  v2.19.0 append block exists on branch `claude/new-session-twoifp`, but nothing records that it was installed. So the
  change is a block pasted at the end of the file plus one line in `onOpen()`, and it does not rely on the body of
  `buildMenu_()`.
- The code that writes `Portfolio_Decision_v2`, `Top_10_Investments_v2`, `_DP2_Exit_Engine` and `_DP2_Exit_State` is not
  on any of the 785 branches. `docs/apps-script-source-prerequisites.md` (branch `fix/github-workflow-review-20261018`)
  records it as unavailable. The workbook export through Drive stops before those tabs, so their function names cannot be
  read from it either.
- Hard-coding a guessed name would either fail on click or run the wrong function. Instead, the block lists the project
  functions that look like each renderer, the owner confirms one per page, and the choice is saved as a Script Property.

## What the owner gets

Menu bar entry **🆕 Decision Pages v2**:

| Item | Handler | Effect |
| --- | --- | --- |
| 🔄 Refresh Portfolio_Decision_v2 | `tfbPagesV2RefreshPortfolioDecision` | runs the confirmed PD v2 renderer on its tab |
| 🔄 Refresh Top_10_Investments_v2 | `tfbPagesV2RefreshTop10` | runs the confirmed Top 10 v2 renderer on its tab |
| 🔄 Refresh both (Portfolio, then Top 10) | `tfbPagesV2RefreshBoth` | Portfolio first, then Top 10; one shared function runs once |
| 📍 Open ... (two items) | `tfbPagesV2OpenPortfolioDecision`, `tfbPagesV2OpenTop10` | activates the tab |
| ⚙️ Set up refresh functions | `tfbPagesV2Setup` | choose or change the function for each page (`-` removes it) |
| 🩺 Status | `tfbPagesV2Status` | tabs, saved functions, suggestions, wrapper presence |

- Script Properties: `TFB_V2_PD_REFRESH_FN` and `TFB_V2_T10_REFRESH_FN` (a function name, or `Library.function`). A
  renderer declared as a top-level `const`/`let` is not listed among the suggestions, because it is not a property of the
  global object, but it can be typed in and it resolves.
- "Auto refresh when needed" is read as on-demand refresh from the menu. The block adds no trigger. The commit sheets for
  the decision pages warn against a second automatic writer while another caller can refresh the same surface.

## Safety rules (each pinned by a check in `tests/js/menu_pages_v2.test.js`)

- **Only a confirmed function runs.**
  - First use, or a saved name that no longer exists, opens the setup prompt. Cancel runs nothing.
  - An unknown name, a number outside the list, a class, or one of the block's own functions is refused and nothing is
    saved.
  - The own-function check uses the resolved function and its receiver, so a dotted path
    (`globalThis.tfbPagesV2Setup`), `.call` or `.apply` to one of them is refused too. The check runs again at refresh
    time, in case such a name was put in the Script Property by hand.
- **Suggestions are conservative.**
  - A suggested name must contain `v2` or `dp2`.
  - Private helpers (trailing `_`), the v1 refreshers and the block's own functions are never suggested.
  - Names containing test, install, remove, delete, clear, reset, purge, trigger, status, diag, debug, menu, setup,
    probe, audit, log, seed or layout are never suggested.
  - Names matching the page (`portfolio`/`pdv2` or `top10`/`t10`) rank first, then strong verbs (refresh, render, run,
    update, rebuild, generate, publish), then weak verbs (build, write, make, compute, create, draw).
- **One run per renderer function and per tab at a time.**
  - Two document-cache flags are claimed together: one keyed by the function's own global name (so an alias such as
    `globalThis.name` is the same function) and one keyed by the tab. One function saved for both tabs is guarded once,
    and a different function for a tab that is already refreshing is refused.
  - The flag is claimed under `LockService.getUserLock()`, cleared in `finally` (also when the refresh is deferred or
    fails), and expires after 7 minutes.
  - The user lock is used instead of the document lock because a 05_Refresh pass can hold the document lock for
    minutes. No other project code takes the user lock, so the claim only waits for another click.
  - If the user lock cannot be taken within 10 s, the click is refused ("try again in a moment"). The flag is never
    claimed without the lock (Codex review, PR #801).
  - A second click on a running function is refused with "already running".
  - Without a cache, which a bound script always has, no guard is possible and the click runs.
- **Refresh both.**
  - Portfolio runs first, then Top 10.
  - A function shared by both tabs runs once per click, even when it fails. If it did not run (busy, deferred), the
    message says "not retried" rather than "run once".
  - A Portfolio failure still runs a different Top 10 function.
  - Top 10 is not started once 4 of the 6 execution minutes are used, counting time spent in setup dialogs, and the
    alert says so.
- **Manual priority.** When the live menu defines `menuRunWithManualRefreshPriority_`, each refresh runs through it,
  which is how the existing morning-cockpit item yields automatic refresh. If that wrapper does not call the renderer,
  whatever it returns, the item reports "not run now" instead of "done". An error thrown by the wrapper itself is
  attributed to the coordinator, not to the renderer, and the message says whether it came before the renderer ran or
  after it finished.
- **Errors are visible.** An alert names the function and its error. The renderer's own return value (for example
  `'HTTP 503'`, `{status}` or `{message}`) is shown in the toast. Without a Ui, for example when run from the editor, the
  handlers return quietly and never throw.
- **Nothing happens at load.**
  - Loading the block makes no service call, shows nothing and changes no existing global.
  - `tfbPagesV2AddMenu_()` swallows its own errors, so the main menu is unaffected.
  - The block defines no `onOpen`, `onEdit` or `onInstall` and creates no trigger.
- **Active Page items are left alone.** An earlier draft added both tabs to the v2.19.0 `MENU_DECISION_OWNED_EXTRA_`.
  Review showed that v2.19.0's `menuDecisionOwnedAction_` sends every decision-owned page except `Portfolio_Decision` to
  the v1 Top 10 build/refresh. Build would then have cleared the v1 `Top_10_Investments` tab. The draft was removed, so
  the main menu's Active Page items treat the `_v2` tabs exactly as before. Use this menu for them.

## Install (owner, about 5 minutes)

1. Apps Script editor, `01_Menu.gs`: paste the whole of `apps_script/01_Menu_Decision_Pages_v2_append.gs` at the end of the
   file. It may instead be added as its own file, but not both. Ctrl+S, no red marks.
2. In `onOpen()`, directly after the line that builds the main menu (`buildMenu_();`), add this line, then press Ctrl+S
   again:
   `tfbPagesV2AddMenu_();`
3. Reload the spreadsheet (after saving). The menu bar shows **🆕 Decision Pages v2**.
4. Click **⚙️ Set up refresh functions**. For each page, press OK on the suggested name if it is the function that writes
   that tab; otherwise type the right name. Then click **🩺 Status**: both functions should read "(found)".
5. Use **🔄 Refresh ...** whenever needed.

Rollback: delete the one `onOpen` line and save (the menu disappears and the block is inert). Only then, optionally,
delete the block and the two Script Properties. If the block is deleted while the `onOpen` line is still there, `onOpen`
stops at that line.

There is a single install route on purpose. Adding the items inside `buildMenu_()` instead was dropped in review: on a
v2.18.0 file a later v2.19.0 install renames that builder, so the items would disappear silently. That route also has no
error guard, so a mistake there would break the main menu.

## Audits

| Battery | Result |
| --- | --- |
| `node tests/js/menu_pages_v2.test.js` | 31 checks passed |
| Mutation check: 45 mutants of the block (guard keys and lock, ranking, exclusions, numbering, own-function refusal incl. receiver and run time, class and const resolution, both-order, time budget incl. setup, deferral, wrapper errors before/after, shared-run notes, load-time calls, status) | every mutant fails at least one check |
| Adversarial review (3 lenses, each finding verified independently) | code and docs: 1 high (decision-owned routing, found by two lenses), 2 medium (out-of-range number, shared-function guard), 6 low (dotted own-name bypass, stale setup toast, coordinator errors and deferrals, const renderers, 6-minute limit, overstated docs); tests: 16 coverage gaps; all fixed; 5 findings refuted |
| Codex review (PR #801) | 1 P2 (unsynchronized claim when the document lock times out), fixed with the user lock |
| Second adversarial review of the rewrite (2 lenses) | 2 medium (time budget missed setup dialogs; install route and rollback), 8 low (.call/.apply own bypass, coordinator error after the renderer, shared-run notes, class accepted, alias and tab keys, save step, alternative-route wording, alternative route lost on a later v2.19.0 install); all fixed; 1 refuted |
| `node tests/js/apps_script_syntax.test.js` (every `apps_script/*.gs`, including the block) | 6 sources compiled |
| Tests that read `ci.yml` | 80 passed, 4 skipped |
| `scripts/audit_repository_workflows.py` | 0 errors (2 warnings already on main) |

Open item for the owner: once the bound project is exported, the v2 generator source should be added to the repository.
The block can then name the renderers directly, and the setup step becomes a one-time confirmation.
