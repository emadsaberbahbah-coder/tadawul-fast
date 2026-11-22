# Commit sheet - the Exit Plan's run log names no holding (exit_plan 1.6.1, 22 Nov 2026)

Branch `fix/exit-plan-log-privacy`, against main after #1052 (merged 22 Nov 2026 06:09 UTC).

| File | Version |
|---|---|
| `scripts/exit_plan.py` | 1.6.0 -> **1.6.1** (`[LOG-PRIVACY]`) |
| `scripts/verify_deployment.py` | pin exit_plan 1.6.1 |
| `tests/test_exit_plan.py` | X20 (2 tests); the suite's fixture pins `TFB_EXIT_LOG_DETAIL=on`; pins 1.6.1 |

## Why

The owner's standing rule: "Repo logs are public: no amounts, quantities, account numbers or holdings' symbols in run logs." The repository is public. Since 1.0.0 (11 Nov) every Exit Plan run printed one line per holding into its GitHub Actions log - the symbol, the last close, the stop, the target and the reasons - and one line per plan pick with its levels; the e-mail subject printed there named the holdings that had an action. Seen on the first 1.6.0 run (dry run, 22 Nov 06:10 UTC). The other plan workflows (Daily Plan, Today Action Plan, Holding Read, Markets Board) print no holding.

## What changes

- `log_detail()`: `TFB_EXIT_LOG_DETAIL` = auto (default: no detail when `GITHUB_ACTIONS=true`, the full lines elsewhere) | on | off. The workflow does not map it, so Actions runs take the default.
- Without detail: "holding 3 of 8: HOLD (market not calm)", "plan pick 1 of 2: levels computed", and the subject as "TFB Exit Plan <date> - <n> actions".
- The page, the ledger and the e-mail are unchanged.

## Checked

`tests/test_exit_plan.py` and `tests/test_sheets_quota.py`: 54 passed (X20: on Actions no symbol, price, name or pick in the output while the page, ledger and e-mail still carry them; locally and with `TFB_EXIT_LOG_DETAIL=on` the full lines).

## Open (owner)

The logs of the Exit Plan runs since 11 Nov still name the holdings. Deleting them is permanent: it waits for the owner's word.
