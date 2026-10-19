# TFB Commit Sheet - 01_Menu Decision Pages v2 block v1.0.0 [menu-bar refresh for Portfolio_Decision_v2 and Top_10_Investments_v2]

Date: 2026-10-19 (Monday, Riyadh afternoon). Owner request: "for this new 2 pages I need to be added in Menu bar as well to
enable me to make auto refresh when needed Top_10_Investments_v2, Portfolio_Decision_v2 ... add this to the 01_Menu.gs".
Delivered: `apps_script/01_Menu_Decision_Pages_v2_append.gs` (NEW, paste block), `tests/js/menu_pages_v2.test.js` (NEW,
20 checks), `.github/workflows/ci.yml` 1.1.26 (lean lane step), this sheet.
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
| ⚙️ Set up refresh functions | `tfbPagesV2Setup` | choose or change the function for each page |
| 🩺 Status | `tfbPagesV2Status` | tabs, saved functions, suggestions, wrapper presence |

- Script Properties: `TFB_V2_PD_REFRESH_FN` and `TFB_V2_T10_REFRESH_FN` (a function name, or `Library.function`).
- "Auto refresh when needed" is read as on-demand refresh from the menu. The block adds no trigger. The commit sheets for
  the decision pages warn against a second automatic writer while another caller can refresh the same surface.

## Safety rules (each pinned by a check in `tests/js/menu_pages_v2.test.js`)

- **Only a confirmed function runs.**
  - First use, or a saved name that no longer exists, opens the setup prompt. Cancel runs nothing.
  - An unknown name, or one of the block's own functions, is refused and nothing is saved.
- **Suggestions are conservative.**
  - A suggested name must contain `v2` or `dp2`.
  - Private helpers (trailing `_`), the v1 refreshers and the block's own functions are never suggested.
  - Names containing test, install, remove, delete, clear, reset, purge, trigger, status, diag, debug, menu, setup,
    probe, audit, log, seed or layout are never suggested.
  - Names matching the page (`portfolio`/`pdv2` or `top10`/`t10`) rank first.
- **One refresh per page at a time.**
  - A document-cache flag is claimed under the document lock, cleared in `finally`, and expires after 7 minutes.
  - A second click is refused with "already refreshing".
  - A briefly busy document lock does not block a click.
- **Manual priority.** When the live menu defines `menuRunWithManualRefreshPriority_`, each refresh runs through it,
  which is how the existing morning-cockpit item yields automatic refresh. If that wrapper declines to run the function,
  the item reports "not run now" instead of "done".
- **Errors are visible.** An alert names the function and its error. The renderer's own return value (for example
  `'HTTP 503'` or `{status}`) is shown in the toast.
- **The menu never breaks.** `tfbPagesV2AddMenu_()` swallows its own errors, so the main menu is unaffected. The block
  defines no `onOpen`, `onEdit` or `onInstall`, creates no trigger, and makes no service call at load time.
- **Decision-owned.** When the block is pasted after the v2.19.0 block, both `_v2` tabs join
  `MENU_DECISION_OWNED_EXTRA_`, so "Active Page > Refresh" does not run the generic engine refresh on them. Without
  v2.19.0 this does nothing, and 🩺 Status says so.

## Install (owner, about 5 minutes)

1. Apps Script editor, `01_Menu.gs`: paste the whole of `apps_script/01_Menu_Decision_Pages_v2_append.gs` at the end of the
   file. It may instead be added as its own file, but not both. Ctrl+S, no red marks.
2. In `onOpen()`, directly after the line that builds the main menu (`buildMenu_();`), add this line:
   `tfbPagesV2AddMenu_();`
   To put the items inside the main menu instead, add `.addSubMenu(tfbPagesV2Menu_(ui))` to the chain in `buildMenu_()`
   and skip this line.
3. Reload the spreadsheet. The menu bar shows **🆕 Decision Pages v2**.
4. Click **⚙️ Set up refresh functions**. For each page, press OK on the suggested name if it is the function that writes
   that tab; otherwise type the right name. Then click **🩺 Status**: both functions should read "(found)".
5. Use **🔄 Refresh ...** whenever needed.

Rollback: delete the one `onOpen` line (the menu disappears and the block is inert). Optionally delete the block and the
two Script Properties.

## Audits

| Battery | Result |
| --- | --- |
| `node tests/js/menu_pages_v2.test.js` | 20 checks passed |
| `node tests/js/apps_script_syntax.test.js` (every `apps_script/*.gs`, including the block) | 6 sources compiled |
| Tests that read `ci.yml` | 80 passed, 4 skipped |
| `scripts/audit_repository_workflows.py` | 0 errors (2 warnings already on main) |

Open item for the owner: once the bound project is exported, the v2 generator source should be added to the repository.
The block can then name the renderers directly, and the setup step becomes a one-time confirmation.
