# TFB Commit Sheet - core/analysis/portfolio_actions.py v1.16.0 [P0-6 Build 1: portfolio research view]

Date: 2026-10-13. Base: `dda1211` (origin/main). Item: P0-6, review blocker 4, Build 1 only.
The D4 daily custody tier (Build 2) is not part of this commit.

## Why this item

Since portfolio actions 1.15.0, every holding without a fresh account-scoped
position and cash packet is rendered as a BLOCK row. Market value, cost, P&L,
stop, TP1 and TP2 are blank. Nothing in the repository supplies that packet,
so the live Portfolio_Decision page shows 4 of 4 holdings as
`RECONCILIATION_REQUIRED: missing_evidence` with no figures.

The blank page also hides something worse. On the unverified path the rule
chain is never evaluated: `decide_action`, the drawdown guard and the
section 4.6 EXIT-BY-RULE are not called. A compliance exit or a sell-tier
exit on a held name is therefore invisible, not just unexecutable.

The real fix is a supplier of reconciliation evidence, which is owner
decision D4. This build makes the protective page informative while that
decision is pending. It shows the rule verdict and the display figures, and
it still never sizes, funds or executes anything.

## Evidence

- `core/portfolio_reconciliation.py:33` sets `DEFAULT_MAX_AGE_SECONDS = 900`.
  `portfolio_actions._build` and `opportunity_builder` call the certifier with
  that default, and no environment variable overrides it.
- `core/portfolio_reconciliation.py:327`: a failure at packet level returns
  `invalid_evidence` when a packet was sent and `missing_evidence` when none
  was. A structurally valid packet captured 3 hours ago therefore becomes
  `invalid_evidence` on every row (harness R7).
- `routes/advanced_analysis.py:4410-4412` and `:4511`: Portfolio_Decision
  (`POST /sheet-rows/portfolio-actions`) reads evidence only from
  `body.reconciliation_evidence`, which its native Apps Script caller sends.
  That caller is not in this repository.
- `apps_script/16_Decision_Top10.gs:3129` is the only reader of the document
  property `TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1`, and it forwards the
  packet only in the Top10 opportunity body. No code in the repository writes
  the property.
- `_unverified_portfolio` (v1.15.1) nulls seven fields per row, forces BLOCK,
  and never calls the rule chain. Checked locally: a STRONG_SELL holding
  renders BLOCK, and `decide_action` is called 0 times.
- `5023.SR` is always classified as a sukuk (`core/compliance_gate.py:207`).
  The P-153 display rule blanks its stop and targets by default. Even a fully
  certified page therefore shows levels on at most 3 of 4 rows. Harness R18
  uses the real symbol and the real classifier.
- Review findings folded in (SKEPTIC amendments 1-5, 10 and 11):
  - Amendment 1, the cost-basis scrub: a row whose verdict is a cost-basis
    BLOCK has its cost, P&L and P&L % blanked.
  - Amendment 2: the holdings KPIs follow the F3/F7 rules.
  - Amendment 3: the docstring header was bumped as well as the constant.
  - Amendment 4: harness cases R16, R17 and R18 were added.
  - Amendment 5: the DQ-46 and DQ-53 notes below.
  - Amendment 10: the verifier requires a declared mode.
  - Amendment 11 does not apply, because `main.py` is not touched.
- Second review round (three minor findings), all reproduced before fixing:
  - Enforce totals ignored portfolio-level reasons that prove them wrong
    (`unrepresented_positions`, `holding_fx_mismatch`). A row whose FX the
    funded path rejects still showed SAR figures. Fixed in code; see R20 and
    R21.
  - The harness stamped its main rows at import, so a late start in the
    long lean CI process could age the quotes past 15 minutes. Fixed in the
    harness; see H1.
  - The mode is not visible on `/health` `pf_gates`. This is documented in
    Known limits and Operator steps. `main.py` is unchanged (item G stays
    optional and separate).

## What changed

1. `core/analysis/portfolio_actions.py` 1.15.1 -> 1.16.0. The constant is
   bumped, and so is the header on line 4 ("Version: 1.16.0"). A dated block
   `v1.16.0 (2026-10-13) - [P0-6 RESEARCH VIEW]` records WHY, WHAT, GATE, NOT
   changed, the functions added and `Removed: 0`.
   - The only change on the existing path is the dispatch in `_build`. When
     funding is not eligible, the `off` mode calls `_unverified_portfolio`
     exactly as before. Any other mode calls the new
     `_research_view_portfolio`.
   - Four functions were added:
     - `_env_research_view_mode` reads `TFB_PF_RESEARCH_VIEW` on each call.
       `off` is the default; `observe` and `enforce` are accepted in any
       letter case, and anything else counts as `off`.
     - `_research_verdict` runs `decide_action` with weights None, then
       `_apply_deminimis`, `_apply_reduce_policy` and
       `_apply_drawdown_guard`. It never calls the ADD confirmation, the
       confirmation session shadow, the ADD-loser veto or the F-1a tag.
     - `_research_fx_rejected` (review fix) applies the funded path's FX
       rule from `_build` to one research row. SAR must be exactly 1, any
       other currency must equal the caller rate, and row FX columns are
       checked too. A value it cannot evaluate counts as rejected.
     - `_research_view_portfolio` takes the off output as its skeleton and
       adds a verdict to each row.
   - Six constants were added: `RESEARCH_UNVERIFIED_STATES`,
     `RESEARCH_TAG_UNVERIFIED` (`[custody-unverified]`), `RESEARCH_TAG_MATCHED`
     (`[custody-matched]`), `RESEARCH_ADD_WITHHELD`,
     `RESEARCH_KPI_BLOCKING_REASONS` and `RESEARCH_FX_WITHHELD`.
   - Each row gets a tier from the 900 s certification:
     - A trusted row whose funding is withheld is `matched`.
     - A row with missing, invalid, unknown, stale or incomplete custody is
       `unverified`.
     - Contradicting evidence (`quantity_mismatch`, `position_closed`,
       `position_missing`, `position_identity_mismatch`, `inactive_zero` or
       any other status) keeps the legacy blank row, unchanged.
   - `observe` changes only text: ` | [research-observe] <tag> research
     verdict <X>: <reason>` is appended to the reason and the advisor note,
     and `detail.research_view` and `meta.research_view` are added.
   - `enforce` keeps the action at BLOCK and keeps the legacy reason prefix,
     then appends ` | <tag> research verdict EXIT|TRIM|HOLD|BLOCK: <reason>;
     not executable`. Market value, cost, P&L, P&L % and stop/TP1/TP2 are
     shown.
   - An ADD verdict is never named or sized. The row reads "new-money verdict
     withheld until fresh positions and settled cash are reconciled".
   - The holdings KPIs follow F3/F7, and `meta.research_view.kpi_basis` and
     `kpi_withheld` record the basis.
   - Review fix, enforce only. Two certification reasons show that a total
     is wrong, so either one withholds all four holdings KPIs:
     - `unrepresented_positions`: the packet holds a positive position that
       no sheet row links to.
     - `holding_fx_mismatch`: a holding FX differs from the caller rate.
     Every research row is also put through `_research_fx_rejected`. This
     matters because `_build` checks FX only after custody and cash pass. A
     rejected row shows none of the seven SAR display fields, and its reason
     gains `; SAR figures withheld: holding FX differs from the supplied
     rate` before `; not executable`. `detail.research_view.fx_rejected` and
     `meta.research_view.fx_rejected_rows` record the result.
   - An exception inside the view falls back to the off output and adds
     `meta.research_view.status = "error"`.
2. `scripts/verify_core_repair_readback.py` 1.1.1 -> 1.2.0 (WHY block), then 1.2.1 after PR review: under a declared enforce every research amount on the probe row (MV, cost, P&L, P&L %, stop, TP1, TP2) and the four holdings KPIs must be published; a blank one fails the readback, because the probe supplies every input.
   - A new `--expect-pf-research-view off|observe|enforce` option was added,
     with `off` as the default. `verify(..., expected_pf_research_view="off")`
     and `require_blocked_portfolio(payload, version, expected_research_view="off")`
     take the new argument.
   - `off` keeps every v1.1.1 check, in the same order and with the same
     messages, and adds no artifact key.
   - `enforce` accepts a row's market value, cost, P&L, P&L % and levels, and
     the four holdings KPIs, as blank or as finite numbers. It does so only
     when the served page declares `enforce` and the row is BLOCK, carries
     `[custody-unverified]`, ends with `; not executable` and names no ADD
     verdict.
   - Every check on money, execution, `position_evidence_matched`, weights,
     sectors, cash KPIs and alerts is kept.
   - Two helpers were added: `_research_amount` and `_require_research_view`.
3. `.github/workflows/core_repair_readback.yml`: the readback step passes
   `--expect-pf-research-view "$TFB_PF_RESEARCH_VIEW_EXPECT"`, where
   `TFB_PF_RESEARCH_VIEW_EXPECT: ${{ vars.TFB_PF_RESEARCH_VIEW_EXPECT || 'off' }}`.
   A dated comment was added. No trigger, concurrency, permission or
   environment changed.
4. `scripts/verify_deployment.py`: the pin for `portfolio_actions` moves from
   1.15.1 to 1.16.0, and `SCRIPT_VERSION` moves from 1.0.56 to 1.0.57 (this
   change is stacked on P0-1, which takes 1.0.56), with a dated WHY entry.
   `TFB_PF_RESEARCH_VIEW` is not added to FLAGS. The existing portfolio mode
   gates (`TFB_PF_DD_EXIT`, `TFB_PF_ADD_LOSER_VETO`, `TFB_PF_CONFIRM_SESSION`)
   are not registered either.
5. `tests/test_pf_research_view_p06.py` (new) covers R0, R0b, R0c, R0d,
   R1-R4, R5, R6-R11, R15-R21, R21b, H1, V1, V2 and G1-G3. It runs as a
   script and prints `PASS k/k`, and it also runs under pytest. Its golden
   negative is read from the env var `TFB_TEST_PA_BASE`.
   - Review fix: `run()` and every case that builds rows call `main_rows()`
     when the case runs. The quote stamps are therefore never older than the
     case itself. The module-level `ROWS` now only gives the fixture shape
     (symbols, quantities, row count).
   - H1 ages the import-time `ROWS` by 16 minutes and reruns R2, R15, R8 and
     R20, which must still pass.
6. `tests/test_core_repair_readback.py`: a new class,
   `PortfolioResearchViewLockstepTests`, is added. Nothing existing was
   changed.
7. `.github/workflows/ci.yml` 1.1.14 -> 1.1.15 (stacked on the runner pin
   1.1.13 and P0-1 1.1.14): the harness was added to the
   blocking lean pytest list, after
   `tests/test_portfolio_action_reconciliation.py`. Nothing was removed or
   moved.
8. `docs/portfolio_reconciliation_contract.md`: a new section was appended,
   "Research view (portfolio actions 1.16.0, default off)". It includes the
   transport correction: Portfolio_Decision gets evidence only from
   `body.reconciliation_evidence` from its out-of-repo Apps Script caller,
   and the document property feeds only the Top10 cockpit.

AST inventory against `dda1211`. No module lost a function:

| File | defs base -> delivered | added | removed |
| --- | --- | --- | --- |
| `core/analysis/portfolio_actions.py` | 111 -> 115 | `_env_research_view_mode`, `_research_verdict`, `_research_fx_rejected`, `_research_view_portfolio` | 0 |
| `scripts/verify_core_repair_readback.py` | 21 -> 23 | `_research_amount`, `_require_research_view` | 0 |
| `scripts/verify_deployment.py` | 14 -> 14 | none | 0 |
| `tests/test_core_repair_readback.py` | 23 -> 37 | the new lockstep class and its methods | 0 |

All new text is ASCII. Across the diff and the new files (this sheet
excluded), 1,706 lines were added. The only one with non-ASCII characters is the existing
`SCRIPT_VERSION` line of `verify_deployment.py`, where the literal changed
from "1.0.56" to "1.0.57" and the em dashes belong to the older inline
comment.

File hashes (sha256, first 16 hex characters):

| File | Base | Delivered |
| --- | --- | --- |
| `core/analysis/portfolio_actions.py` | `0c390a15ccb11a13` (4,041 lines) | `3b4442e8e2f779dc` (4,341 lines) |
| `scripts/verify_core_repair_readback.py` | `845e6e34a1c126af` (575 lines) | `e6775e7c8314d24e` (666 lines) |

## Default OFF identity

With `TFB_PF_RESEARCH_VIEW` unset (or set to `OFF`, `bogus`, an empty value,
`enforced` or `1`, or with only `TFB_PF_CUSTODY_DAILY_MAX_AGE_S=86400` set),
the output is byte-identical to v1.15.1 except for the version string. The
check covered all six fixtures:

1. no packet
2. an empty packet `{}`
3. a packet captured 3 hours ago
4. a quantity mismatch
5. a cash request mismatch
6. a fresh valid packet, which takes the funded path

The comparison serializes the full JSON after setting both version fields to
the same placeholder. Harness R0 compared 42 pairs (6 fixtures x 7 env
spellings) against the base copy, and all were identical.

R0b runs without the base. It replaces `_research_view_portfolio` with a
function that raises, and shows that no off spelling ever enters it.

R0d calls `_research_view_portfolio` directly with the gate off. It returns
the `_unverified_portfolio` output unchanged, which covers an env change
between the dispatch read and the function's own read.

R15 shows that a funding-eligible run gives the same output under `observe`
and `enforce` as under `off`, so the research view never runs when funding is
eligible.

## Measured results with exact commands and outputs

Base copy for the golden negatives:
`git -C /home/user/wt/p0-6-pf-research-view show dda1211:core/analysis/portfolio_actions.py > <scratch>/base_portfolio_actions.py`

Harness as a script, without the base copy (the CI shape):

    $ python tests/test_pf_research_view_p06.py
    PASS 28/28  (skipped 2)

Harness as a script, with the base copy:

    $ TFB_TEST_PA_BASE=<scratch>/base_portfolio_actions.py python tests/test_pf_research_view_p06.py
    G1 gate-on predicates: base fails 13/13, delivered passes 13/13
    G2 defs base=111 delivered=115 added=['_env_research_view_mode', '_research_fx_rejected', '_research_verdict', '_research_view_portfolio'] removed=[]
    G3 303 lines added vs base, all checked for ASCII
    R0 off byte-identical to base on 42 fixture x spelling pairs
    PASS 30/30

Harness under pytest:

    $ python -m pytest -q tests/test_pf_research_view_p06.py
    28 passed, 2 skipped
    $ TFB_TEST_PA_BASE=<scratch>/base_portfolio_actions.py python -m pytest -q tests/test_pf_research_view_p06.py
    30 passed

Golden negative (G1). With the gate on, the base module fails all 13 gate-on
predicates:

- R1, observe text only
- R2, enforce levels equal to the certified build
- R3, rule verdicts
- R3b, the drawdown guard and the rule exit
- R4, the withheld ADD text
- R7, the stale packet
- R8, matched rows
- R16, the cost-basis scrub
- R17, the missing price
- R18, the real 5023.SR row
- R20, an unlinked custody position withholding the totals
- R21, an FX-rejected row showing no SAR figure
- V2, the lockstep readback accepting enforce

The delivered module passes all 13. Without `TFB_TEST_PA_BASE`, R0 and G1
skip with a message.

Review-fix negatives, run from a scratch script against the first commit of
this branch (`5715373`, module and harness taken with `git show`):

    pre-fix 5715373 pred_r20_unrepresented_withholds_totals False | delivered True
    pre-fix 5715373 pred_r21_fx_rejected_row_blank False | delivered True
    H1 with import-time rows: FAIL AssertionError (expected)
    H1 delivered harness: PASS

The H1 negative was measured at 07:48 UTC on 2026-10-13, inside the Tadawul
session. Outside the session a 16-minute-old local quote can still pass when
`exchange_calendars` is installed. The lean CI job does not install it, so
there the old harness could fail at any hour.

Before the fix, an enforce run with a fresh packet plus one unlinked position
published `holdings_value_sar` 5,010, `cost_basis_sar` 4,900 and `pnl_sar` 110
with `kpi_withheld` empty. A `[custody-matched]` row with `FX To SAR` 3.75 on
a SAR price of 100 showed a market value of 3,750 and a stop of 345. After
the fix, both runs publish no holdings KPI (`kpi_withheld`
`['unrepresented_positions']` and `['holding_fx_mismatch']`), and the FX row
shows no SAR figure.

Verifier golden negative. The new `tests/test_core_repair_readback.py` class
was run against the `dda1211` readback script in a scratch tree:

- Eight of its ten tests fail outright.
- Every subtest of a ninth fails.
- Only the workflow-wiring test passes, because it reads the delivered
  workflow.
- The 18 existing tests still pass against the base.

Synthetic enforce example (harness fixture, no packet; five holdings of 10
units each). Every row stays BLOCK with zero money:

| Holding | Research verdict | MV | Cost | P&L | Stop / TP1 / TP2 |
| --- | --- | --- | --- | --- | --- |
| ADDME.SR | withheld (ADD) | 1,000 | 900 | 100 | 92 / 115 / 130 |
| SELLME.SR | EXIT (sell-tier) | 1,000 | 900 | 100 | 92 / 102 / 104 |
| TRIMME.SR | TRIM (valuation) | 1,000 | 900 | 100 | 92 / blank / blank |
| DDOWN.SR | HOLD | 1,000 | 1,200 | -200 | 92 / 102.5 / 105 |
| SYNTHSUKUK.SR | HOLD | 1,010 | 1,000 | 10 | blank (sukuk) |

Totals: holdings_value_sar 5,010, cost_basis_sar 4,900, pnl_sar 110 and
pnl_pct 2.2. portfolio_value, cash and cash_pct are blank. The four funding
KPIs are 0, and there is one `portfolio_inputs_unverified` alert.

Related existing suites, all run in the venv:

    $ python -m pytest -q <the 91 files the ci.yml lean step runs (90 listed plus the existence-gated EXTRA), harness included, one process>
    3476 passed, 5 skipped, 2 warnings, 162 subtests passed in 240.02s
    $ python -m pytest -q <the lean "work-brief capture, repair and pure contract" list>
    275 passed
    $ python -m pytest -q tests/test_core_repair_readback.py tests/test_core_repair_tiny_quotes.py tests/test_name_identity_workflow.py
    56 passed, 188 subtests passed
    $ python -m pytest -q tests/test_portfolio_reconciliation_route.py tests/test_daily_brief_blocked_inputs.py tests/test_board_funding_route.py tests/test_main_health_pf_gates_v8140.py tests/test_pf_dd_fee_guards.py tests/test_pf_dd_guard_sukuk_exempt.py tests/test_capture_evidence.py tests/test_flag_registry.py tests/test_workflow_flag_mappings.py tests/test_verify_optional_rollout_flags.py
    135 passed, 3 skipped   (the 3 skips: reportlab not installed locally)
    $ python -m pytest -q tests/test_pf_add_loser_veto_p183.py      -> 14 passed
    $ python -m pytest -q tests/test_pf_sukuk_ladder_p153.py        -> 14 passed
    $ python -m pytest -q tests/test_de_f7_scoring_settle.py tests/test_de_f7_intrinsic_display_basis.py
    99 passed
    $ python -m pytest -q tests/test_portfolio_action_reconciliation.py tests/test_portfolio_reconciliation.py tests/test_portfolio_exact_funding.py tests/test_ob_portfolio_certification.py
    113 passed
    $ python tests/test_verify_manifest_pins.py                     -> [MANIFEST PIN GUARD] 3/3 PASS
    $ node tests/js/portfolio_reconciliation_inputs.test.js         -> 16 native reconciliation input checks passed

## Known limits

- **The view does not supply evidence.** Every holding stays BLOCK until a
  valid 900 s packet arrives through the Portfolio_Decision caller.
- **DQ-46 stays red.** `scripts/tfb_workbook_dq_audit.py` DQ-46 counts
  trailing runs in which every action is BLOCK. It stays FAIL or WARN until
  D4 ships with a supplier, because `enforce` keeps every row BLOCK.
- **DQ-53 stays red.** DQ-53 fails on any blank stop or TP cell, so it stays
  red while a sukuk such as 5023.SR is held. P-153 blanks that ladder on
  purpose. Neither audit is changed in this build, and no target is lowered.
- **Cap trims stay hidden.** Position-cap and sector-cap trims (TRIM-BY-RULE)
  need weights, and weights need verified cash. This is intentional.
- **Reasons may name ADD.** A HOLD whose engine reason mentions ADD (for
  example "Low confidence capped ADD -> HOLD" or "PRECEDENCE ... blocks ADD")
  keeps that text. These are restraints, not ADD verdicts. A real ADD verdict
  is always replaced by the withheld text.
- **Legacy non-ASCII text.** The legacy reason strings contain non-ASCII
  characters, for example the em dash in "Data gate: missing price - manual
  review required" (shown here with a hyphen). They are concatenated after
  the ASCII tag unchanged.
- **Book totals in the brief.** Under `enforce`,
  `scripts/run_daily_brief.py` (L880-L904) can total row market value and
  cost into its book line, because every row then carries them. Those
  figures come from sheet quantities whose custody is not proven. The rows
  are still BLOCK, so their SAR is forced to 0 and they never reach the
  executable groups.
- **Sheet effect unproven.** The Portfolio_Decision Apps Script writer is not
  in the repository. Whether it writes the market value, stop and TP cells
  for BLOCK rows cannot be verified here, so the visible effect on the sheet
  is unproven until that source is pulled.
- **Not shown on /health.** `TFB_PF_RESEARCH_VIEW` is read on every call
  and prints no boot line, but it is not in `main.py` `_PF_GATE_ENVS`. An
  armed mode is therefore not visible in `/health` `pf_gates`, which the
  v8.14.1 rule expects for per-call PF gates. Until the optional item G
  lands, the only attestation is a readback run with
  `TFB_PF_RESEARCH_VIEW_EXPECT` set to the armed word, together with
  `meta.research_view.mode` on the served page. Item G was left out of
  Build 1 on purpose. It adds the name to `_PF_GATE_ENVS` and moves the
  length pin in `tests/test_main_health_pf_gates_v8140.py` (line 117) from
  24 to 25.
- **Unproven quotes and FX still total.** Under `enforce`,
  `holding_quote_unverified` (for example a `kept_last_good` quote) and
  `fx_unverified` (the packet did not prove the caller rate) do not withhold
  the holdings KPIs. Those totals use the sheet quote and the caller rate.
  The reason prefix names the code on trusted rows. Only reasons that show a
  total is wrong (`unrepresented_positions`, `holding_fx_mismatch`, a
  contradicted row, incomplete holdings) withhold it. Whether unproven inputs
  should also withhold the totals is owner decision (h).
- **FX rule is stricter than the funded path's order.** The research view
  checks every research row's FX even when no packet was sent. A non-SAR row
  whose currency is missing from the caller rates therefore shows no SAR
  figure under `enforce`. The funded path would never reach that row.
- **Manual readback only.** `core_repair_readback.yml` runs only on
  `workflow_dispatch` from main. A mismatched arming is caught on the next
  manual readback, not by a schedule.
- **flags.md not regenerated.** `docs/flags.md` was not regenerated. It was
  already out of date at base (449 diff lines before this change), and
  regenerating it would mix unrelated churn into this commit.

## Operator steps (arming and rollback)

1. Merge this commit. Nothing is armed, and the page is unchanged.
2. Set `TFB_PF_RESEARCH_VIEW=observe` on Render. The next Portfolio_Decision
   run keeps every number blank, and each row's reason gains
   `[research-observe] [custody-unverified] research verdict ...`. Run "Core
   repair deployed readback" with `TFB_PF_RESEARCH_VIEW_EXPECT` unset. It
   must stay green, because observe passes the unchanged probe. Optionally,
   set the Variable to `observe` and run it again to confirm the tag is
   served.

   `/health` `pf_gates` does not list this gate (see Known limits). Confirm
   the armed mode from the readback and from `meta.research_view.mode` on a
   served page, not from `/health`.
3. Read the observe verdicts on the page or in the brief for a few runs. In
   particular, check whether any held name shows `research verdict EXIT`.
4. Arm `enforce` only after this commit (readback 1.2.1) is on main. Set
   `TFB_PF_RESEARCH_VIEW=enforce` on Render and the Actions Variable
   `TFB_PF_RESEARCH_VIEW_EXPECT=enforce` at the same time, then run the
   readback. It must be green.

   If the Variable is set without arming Render, the readback fails with
   "declared portfolio research view mode not served". If Render is armed
   without the Variable, it fails with "uncertified portfolio amount was
   published: holdings_value_sar".
5. Rollback:
   1. Unset `TFB_PF_RESEARCH_VIEW` on Render. The output returns to v1.15.1,
      except for the version string.
   2. Unset `TFB_PF_RESEARCH_VIEW_EXPECT`.

   No redeploy is needed, because the gate is read on every call. Reverting
   this commit is a full rollback.

## Owner decisions owed (D4)

- (a) **Arming order on Render.** `observe` first (the live readback stays
  green), then `enforce` only after the verifier lockstep in this commit is
  on main.
- (b) **Daily statements.** Decide whether a statement up to 24 h old may
  promote HOLD, TRIM and EXIT to the action column. The certifier's hard
  maximum is 86,400 s. Promoted EXIT and TRIM rows would join the daily
  brief's executable sell group. This is Build 2 and is not built.
- (c) **The supplier.** Choose an IBKR Flex token or a statement-capture
  process. A holdings-only mode in `scripts/tfb_capture_evidence.py` is also
  needed, because it refuses packets that are not funding-eligible at 900 s.
  The owner must also decide who writes the document property, which is
  limited to 9,000 characters.
- (d) **The Portfolio_Decision caller.** Its native Apps Script caller is not
  in the repository. It must be pulled with clasp and changed to forward
  `reconciliation_evidence`. Otherwise the page stays `missing_evidence`
  whatever the property holds.
- (e) **Al Rajhi sukuk custody.** It needs its own monthly statement. Under
  per-row promotion it would stay in research.
- (f) **The readback verifier change.** Accept the change in this commit. It
  accepts levels and holdings KPIs only on a declared research-view run, and
  every money and execution check is kept.
- (g) **Book totals.** Decide whether research-view holdings KPIs, built from
  unverified sheet quantities, may appear as book totals. The brief also sums
  row MV and cost (`run_daily_brief.py` L880-L904).
- (h) **Unproven inputs and the totals.** Decide whether
  `holding_quote_unverified` and `fx_unverified` should also withhold the
  enforce holdings KPIs, as `unrepresented_positions` and
  `holding_fx_mismatch` now do. Adding them is a one-line change to
  `RESEARCH_KPI_BLOCKING_REASONS`.
- (i) **Optional item G. BUILT (2026-10-13, follow-up commit):** `main.py` 8.14.4 lists
  `TFB_PF_RESEARCH_VIEW` in `_PF_GATE_ENVS`, so `/health` `pf_gates.portfolio_actions`
  shows the armed mode (or `unset`); the pin in `tests/test_main_health_pf_gates_v8140.py`
  moves 24 -> 25 and `verify_deployment` pins `APP_ENTRY_VERSION` 8.14.4. Original note: Decide whether to schedule the separate `main.py`
  build that lists `TFB_PF_RESEARCH_VIEW` in `_PF_GATE_ENVS`, so that an
  armed mode shows on `/health`.
