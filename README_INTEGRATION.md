# Integration Notes

Copy these files into the repository using the same relative paths.

## Activation order

1. Add the JSON policy and central loader.
2. Run `python -m unittest tests/test_investment_policy.py`.
3. Keep `runtime_enabled=false`.
4. Wire `evaluate_speculation_gate()` into `opportunity_builder` before scoring/ranking.
5. Wire `validate_price_plan()` and `validate_recommendation_card()` into portfolio actions and API responses.
6. Extend the immutable recommendation snapshot written by `track_performance.py`.
7. Build outcome cohorts and shadow-learning reports.
8. Add project-wide integration tests.
9. Activate only after explicit approval and rollback validation.

## Required behavior

All recommendation-producing modules must consume the same policy version. A local module flag may be used in development, but production must fail startup when policy versions diverge.

The files intentionally do not generate prices or trading actions. They prevent incomplete, speculative, or falsely precise outputs from being labeled executable.
