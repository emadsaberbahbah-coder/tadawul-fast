# Verification of the 13 Oct 2026 full project review

Date: 2026-10-13. Code read at `dda1211` (HEAD of `main`, the same commit the
review read). Method: one reader per blocker or claim group, then an
independent skeptic per reader that re-read the cited code and tried to
refute both the finding and the proposed fix. All reads were offline; the
live workbook, Render dashboard, repo Variables and mailbox were not read.

## Verdicts

| Review item | Verdict | What changes |
| --- | --- | --- |
| Blocker 1: the feed cannot return to EXECUTABLE | Confirmed | The ratchet is wider than stated: the sync never writes EXECUTABLE, never creates the key, and never lifts any NOT_ACTIONABLE (harness_w1a6 S12.14b/15b/18 pin this on purpose). It has not been the binding cause since 10 Oct, because the final job re-vetoes every production run (blocker 2). |
| Blocker 2: the final check fails on every scheduled run | Confirmed, with a second cause | Ordering is real (final job runs after the sync legs and recovery, cockpits refresh only on Apps Script time triggers). Independently, Portfolio_Decision never emits `source readiness VERIFIED (run N)`, so PF_SOURCE_CONSUMPTION_UNPROVEN fails every strict run whatever the order. Strict mode also applies to manual full_sync runs. |
| Blocker 3: the check covers every page | Partly confirmed | True for the coverage audit, but four all-pages gates exist (coverage, decision audit, native cockpit readiness, sync upstream composite). Build Spec section 5A is not in the repo; the in-repo spec (docs/AUTOMATIC_REFRESH_COMPLETENESS_V1.md) says every page gates. The US holdings (DDI, CWBC, KRP and others) sit on Global_Markets, so GM always contributes and its 97.79% names keep the coverage audit red. P0-3 changes no live outcome before P0-4. |
| Blocker 4: portfolio evidence has no supplier | Confirmed, premise corrected | Nothing writes `TFB_PORTFOLIO_RECONCILIATION_EVIDENCE_V1`, but that property feeds only the Top_10 cockpit's board body. Portfolio_Decision gets evidence only from `body.reconciliation_evidence` sent by its out-of-repo caller (10_My_Portfolio.gs). On the blocked path TRIM and EXIT are never evaluated, not merely blanked. Quantities are already visible. 5023.SR is a sukuk, so the best achievable is levels on 3 of 4 holdings. |
| Blocker 5: live Apps Script lags the repo | Partly confirmed | The PF half is a code gap (a producer that never emits the receipt), not an install gap. The `installed: false` flag records that the PR did not install v1.13.6; it is not a reading of the live version. The Top_10 half needs v1.13.7 or later (the pool reconciliation token), not v1.13.6. A full-project `clasp push` from today's 3-file mirror would delete the other bound files; import must add missing files only. |
| ci.yml whole-suite lane is advisory | Confirmed, by design | Advisory for one green week from #751 (12 Oct). Removing `continue-on-error` alone would not make it required; the owner adds the check to the ruleset after about 19 Oct. |
| 55 unpinned `ubuntu-latest` runners | Confirmed | No job installs apt packages; daily_sync preflight relies on the image's `curl jq python3 pip3`. Pinned in this change set. |
| `/health` app_version 5.111.0 | Confirmed, cause corrected | Not a code constant: the Render ENV `APP_VERSION` (or `TFB_APP_VERSION`, possibly in the `tfb-server` env group) overrides the 8.14.3 default. Fix is deleting the ENV key, not code. |
| Decision Digest silent after 10 Oct | Not by design | An enabled, non-dry-run, non-error digest run always sends mail. Under NOT_ACTIONABLE the subject is `TFB Brief - <session> - NOT ACTIONABLE (...)`, not `TFB Digest`. Owner checks below. |
| `docs/render_env_contract.md` (H-8) absent | Confirmed | Not built in this change set (work-in-progress cap of 3 builds). |

## Built in this change set (all default OFF, nothing armed)

1. PR #762: runner pin to `ubuntu-24.04` plus a manual 26.04 canary workflow (merge before 19 Oct).
2. PR #763 (stacked on #762): P0-1 release step, `scripts/release_decision_feed.py` and a dispatch-only workflow. Dry-run unless `TFB_DECISION_RELEASE=1`; five arming prerequisites are listed in its commit sheet.
3. PR #764 (stacked on #763): P0-6 Build 1, portfolio research view, `TFB_PF_RESEARCH_VIEW=off|observe|enforce` in portfolio_actions 1.16.0, with a lockstep in the readback verifier.

Merge in that order. Each has a commit sheet with arming steps and rollback. The stacked branch passed the full ci.yml lean list offline (3512 passed, 0 failed).

## Not built, and why

- P0-3 (scope the final gate): inert before P0-4 because GM always contributes through the US holdings; it lowers a recorded 99% acceptance floor through a Variable; the section 5A text it relies on is not in the repo. Needs D2 plus the committed section 5A text first.
- P0-4 (retire 154 delisted symbols and re-approve rosters): needs the owner's D3 list.
- P0-5 (install cockpit, clasp mirror and push lane): owner paste and OAuth, and D7 (public vs private) must come first.
- P0-6 Build 2 (24h custody tier): needs D4 and a statement supplier; the capture builder refuses holdings-only packets today.
- Portfolio_Decision receipt: needs `10_My_Portfolio.gs` in the repo.

## Owner checks (read-only, minutes each)

1. `_Status` L1:M60: the `TFB Decision Feed` value should carry the latest scheduled run id with `coverage_failed,decision_failed`. `TFB Upstream Feed Diagnostic` exists only when the repo variable `TFB_SYNC_UPSTREAM_VERDICT` is armed.
2. Render: delete `APP_VERSION` and `TFB_APP_VERSION` from the service ENV and from the `tfb-server` env group (batch with other ENV edits; saving redeploys).
3. Digest: Gmail search `subject:"TFB Brief" after:2026/10/10` including Spam and All Mail; the log lines of digest runs #297 and #298 (`email sent to`, `DRY RUN`, `DIGEST_ENABLE=0`); the `DIGEST_ENABLE` repo variable.
4. Live cockpit version: Top_10_Investments!B2 must contain exactly one `pool_reconciliation_v1=` token for v1.13.7 or later.
5. Zero-code ordering experiment: after a scheduled sync completes, refresh Top_10 and Portfolio_Decision by hand, then re-run only that run's `Verify final decision publication` job (never "Re-run failed jobs"). Expected: the T10 receipt check passes and the PF check still fails, which proves the ordering half without any new code.
