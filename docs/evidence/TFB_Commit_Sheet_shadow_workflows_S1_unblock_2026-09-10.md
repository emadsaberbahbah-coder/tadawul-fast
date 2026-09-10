# TFB Commit Sheet — S-1 challenger unblock (P-79 root-cause fix, both shadow lanes)
Date: 2026-09-10 · One commit, two files · Register: closes the 2026-09-09 root-cause finding (S-1 frozen 3/28, excluded_reason=no-challenger)

## Identity
| File | Base @ HEAD 87ce75fa | Delivered | Change |
|---|---|---|---|
| .github/workflows/shadow_scorer.yml | sha256 `7c50ed6f2dd0bfd3…` (77 ln) | sha256 `f211e903914f1997…` (87 ln) | +10 lines: `TFB_COMPLIANCE_SCREEN_RETIRED: "1"` hardcoded in the `score` job env (flag was entirely ABSENT) + WHY block |
| .github/workflows/shadow_board.yml | sha256 `f1ec4f6fb27b495f…` (131 ln) | sha256 `adf932c60874d24a…` (136 ln) | `${{ vars.TFB_COMPLIANCE_SCREEN_RETIRED }}` (unset Variable → resolved EMPTY → off) replaced with hardcoded `"1"` + WHY block |

## Why
The retired-Shariah decision (operator, 2026-08-13, restated 2026-09-09) was armed on Render but never in the nightly GitHub lanes: the scorer yml lacked the flag entirely; the board yml read an unset repo Variable. compliance_gate v1.1.0 therefore fell back to the stale authority list (as_of 2026-03-31; 120-day max age breached 2026-07-29), failed every symbol, emptied the challenger basket, and froze the S-1 clock at 3/28 (`chal fresh 0/0`). Hardcode chosen over the repo Variable deliberately: a Variable can be silently unset; git history cannot, and it matches the existing `TFB_COMPLIANCE_GATE_ENABLED: "1"` precedent one line above in both files.

## Audits
Anchored edits 2/2 (count==1 asserted) · YAML parse PASS · flag resolves to `'1'` at job env in both files (verified by parse) · smart-quote scan on added lines PASS · zero removals beyond the one replaced Variable line.

## Arming-discipline note
This is a GitHub-lane arming with its own independent read-back (tonight's scheduled shadow runs), running alongside — not stacked on — the Render-lane `TFB_FUND_UNIT_SENTRY=observe` arming whose read-back is sheet export tags. Different systems, different evidence artifacts, attribution preserved.

## Read-back (defines DONE)
First nightly `shadow_scorer` line showing **`chal fresh > 0`** and `excluded_reason` ≠ `no-challenger`; S-1 scored-day counter advances past 3/28 on the first clean night. Note: GitHub **Re-run** replays the old SHA — tonight's *scheduled* run picks up the new commit automatically; for same-day proof, use a fresh **Run workflow** dispatch on `shadow_board` after committing. Rollback: revert the commit.
