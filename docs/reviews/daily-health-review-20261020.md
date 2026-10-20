# Daily health review, 20 October 2026 (00:30 Riyadh cycle)

Date: 2026-10-19 21:26 to 22:30 UTC (00:26 to 01:30 Riyadh, 20 Oct). Base `f24e9ff` (HEAD of `main`).
Method: Render service and events, live `/health`, the GitHub Actions scheduled-run list, the failed job log and
the final-publication artifact of the evening sync, the IBKR account through the read-only connector, the
repository at HEAD. No workbook cell, Render setting, deploy, provider or order was touched. No account amounts,
quantities, prices or P&L are reproduced here.

## 1. Service

| Check | Observed | Verdict |
| --- | --- | --- |
| Render `tadawul-fast-bridge` | live, pro, Frankfurt, auto-deploy off; owner manual deploys at 09:23, 10:52, 13:03 and 21:11 UTC all succeeded | healthy |
| `/health` 21:32 UTC | `healthy`, app 8.14.5, 7 route modules mounted, auth enforced | healthy |
| Render events (7 days) | builds and deploys only; no `server_failed`, OOM or restart | clean |
| Render error log | not read this cycle: the Render connector has no workspace selected and the connector requires the owner to choose one | open (owner, one click) |

## 2. Scheduled workflows since the morning cycle

| Workflow | Run | Result |
| --- | --- | --- |
| Tadawul Dashboard Advanced Sync | #4926 (10:01 UTC), #4945 (18:44 UTC) | FAIL at `Verify final decision publication` |
| Release Decision Feed (P0-1) | #2, #3 (scheduled) | green (refusals end green by design, v1.1.0) |
| Provider Target Coverage Alert | #75 | FAIL (not investigated this cycle) |
| Brief, Digest, Shadow Board/Scorer, Performance Clock, Calendar, News, Acceptance, Dependency review | | PASS |

### Sync #4945: what changed versus the morning

`final_decision_publication.json` (run 39305103867): exit 2, blockers `coverage_failed` and `decision_failed`.

| Page | Status | Note |
| --- | --- | --- |
| Market_Leaders | PASS | 255 rows, 99.2% fresh, names 100% |
| Global_Markets | WARN | **1** blank timestamp (7 on the 14:01 Riyadh run, 8 on 18 Oct): the `.HK` reporting-currency band of `run_dashboard_sync` v6.70.0 healed 6 of the 7 identity stubs |
| Commodities_FX | PASS | 95.4% fresh |
| Mutual_Funds | **FAIL** | `name coverage 97.98% below 99.00%` (50 blank Names, 24 allowed) |
| My_Portfolio | PASS | 4/4 |

The Mutual_Funds failure is the one predicted in section 11 of the 19 Oct review: the 39 reviewed fund captions
in `data/verified_name_labels.json` expired at 12:31 UTC, so the sync no longer fills those blank Names. The
remaining 67 Global_Markets captions expire on **21 Oct 07:44 UTC**; Global_Markets would then fail its floor too.

Decision side: unchanged shape (8 findings, all cockpit-layer: `PF_SOURCE_CONSUMPTION_UNPROVEN`,
`PF_OLDER_THAN_SOURCE`, four `T10_OLDER_THAN_SOURCE`, `T10_SOURCE_CONSUMPTION_UNPROVEN`,
`T10_POOL_RECONCILIATION_UNPROVEN`). Portfolio_Decision last rendered 14:53 Riyadh and Top_10 19:03 Riyadh, both
before the 21:46 Riyadh My_Portfolio publication. Fix path unchanged: install `26_Decision_Chain` v1.0.1.

## 3. IBKR read (read-only connector, 21:27 UTC)

**The account changed today.** Two of the three US positions were closed by the owner on 19 Oct (a market sell at
13:51 UTC and a GTC limit sell filled at 19:11 UTC); one US position remains. The workbook has not caught up:
the 18:44 UTC sync's coverage audit still read `active_holdings` = `5023.SR`, `CWBC.US`, `DDI.US`, `KRP.US`.

Consequences until My_Portfolio is edited:

- Portfolio_Decision will keep producing HOLD / TRIM guidance for two positions that no longer exist;
- the IBKR capture evidence will no longer link 3/3 and the receipt will degrade;
- the board's cash / deployable budget understates the cash the two sales released, so Top_10 sizing is
  computed on the wrong budget.

Owner action: remove (or zero) the `CWBC.US` and `KRP.US` rows on My_Portfolio, keep `DDI.US` and `5023.SR`,
then refresh Portfolio_Decision and Top_10. If the project should record realised exits for the learning loop,
`scripts/tfb_import_executions.py` (offline replay and reconciliation of a captured broker JSON) is the existing entry point.

## 4. Built in this change set: renewing reviewed Name captions

The seven-day review window is deliberate (a caption must rest on a current authority review), but nothing
performed the renewal, so every week the Name floors fall off a cliff and the decision feed gains a
`coverage_failed` blocker for reasons unrelated to data quality.

### 4.1 `scripts/renew_verified_name_labels.py` v1.0.0 (new)

For each binding: re-fetch **every** cited source (HTTPS; redirects only on the same host; 40 MB bound), then
re-check the caption against the fresh bodies.

- Nasdaq-directory bindings (67, identifier `directory_financial_status`): the exact ticker row in
  `nasdaqlisted.txt` / `otherlisted.txt` must be unique and still carry the same Security Name, venue and
  financial status, and `company_tickers_exchange.json` must still map the exact ticker to the same CIK.
  `directory_creation_time` is re-read from the file trailer.
- All other bindings: the exact exchange code (or a recorded series / class / CUSIP / CIK) and the normalised
  caption must each appear in at least one fresh non-PDF body. PDF-only evidence is held.

Only `reviewed_at_utc`, `expires_at_utc` (= review + 7 days), the source receipts (sha256 and capture time),
`directory_creation_time` and `review_receipt_sha256` change. Name, state, metadata fence and quarantine flag are
never edited. Any failure holds the binding byte-identical (it stays expired; nothing is guessed). The output is
validated with the production loader `core.verified_name_labels._load_registry` before it is written.

sec.gov refuses undeclared automated clients; the tool reads a declared contact User-Agent from
`TFB_SEC_USER_AGENT` and never defaults one. Without it, the 120 bindings that cite sec.gov are held with reason
`sec_user_agent_missing`; with a held-only run the output file is byte-identical to the committed registry
(verified).

Calibration on today's live non-SEC sources (SEC bodies excluded, so this is a lower bound): 109 of 122
bindings already corroborate. Held for honest reasons: three dead company pages (`RCM.US`, `SILK.US`, and the
Vanguard Form 8937 PDF of `VFLQ.US`), PDF-only non-SEC evidence (five Mutual_Funds, two Global_Markets, likely
cured by their SEC HTML), and four company pages that do not print the ticker (`HEES`, `PDCO`, `SMAR`, `THS`,
also likely cured by the SEC bodies). Even if all 6 Mutual_Funds holds persist, 33 renewed captions bring the
blank Names from 50 to 17, inside the 24 the floor allows.

### 4.2 `.github/workflows/name_label_renewal.yml` v1.0.0 (new)

- `check` (daily 04:15 UTC and on dispatch, no network): RED when any binding expires within 48 h, so the cliff
  shows up two days ahead instead of as a sync failure.
- `renew` (dispatch only): runs the tool with `vars.TFB_SEC_USER_AGENT` and uploads
  `renewed_verified_name_labels.json` plus the report as an artifact. It never pushes; the renewed registry
  reaches `main` only through a reviewed PR. `permissions: contents: read`.

### 4.3 CI

`ci.yml` v1.1.26 enrols `tests/test_renew_verified_name_labels.py` (T1 to T7, 15 tests, scripted fetcher, no
network) in the blocking lean lane next to `tests/test_verified_name_labels.py`.

## 5. Verification

- new suite 15 passed; with `test_verified_name_labels`, the workflow audit suites, runner pin, flag mapping and
  flag registry suites: 136 passed, 3 skipped (same skips as `main`);
- `scripts/audit_repository_workflows.py --fail-on-warnings`: errors 0, the same 2 pre-existing `daily_sync.yml`
  warnings as `main`;
- `ruff check --select E9,F63,F7,F82,F401,F841` and `py_compile` clean;
- live CLI dry run: 122 held (120 `sec_user_agent_missing`), output byte-identical to the committed file;
  `--check` reports all 122 expiring within 48 h (exit 1), as expected today.

## 6. Owner checks, in priority order

1. **My_Portfolio**: remove the two closed US positions (section 3), then refresh Portfolio_Decision and Top_10.
2. **Caption renewal before 21 Oct 07:44 UTC**: merge this PR, set repository variable `TFB_SEC_USER_AGENT` to a
   name and contact email (SEC fair-access format, e.g. `Your Name you@example.com`), dispatch
   `Verified Name Label Renewal` with `mode=renew`. The next routine cycle reads the artifact, re-validates it and
   opens the registry PR; or commit `renewed_verified_name_labels.json` over `data/verified_name_labels.json`
   yourself.
3. Render connector: select the workspace once so the routine can read error logs again.
4. Unchanged from 19 Oct: install `26_Decision_Chain` v1.0.1; set `DT10_FEED_COHORT_BIND=on`.

## 7. Not built, and why

- No auto-renewal inside the sync: the seven-day window exists so that a caption rests on a fresh review; the
  sync must not renew its own evidence.
- No edit to My_Portfolio: holdings are owner data and the routine never writes the workbook.
- Provider Target Coverage Alert #75: not investigated this cycle; first item for the next cycle.
