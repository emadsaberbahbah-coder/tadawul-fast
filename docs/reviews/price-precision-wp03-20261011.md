# WP-03 price precision review

The 2026-10-10 brief records SHIB-USD at 5.47e-6, zero forecast prices
for SHIB-USD and COMP-USD, and twelve further sub-cent instruments losing
useful digits. Fixed four-decimal rounding reproduces the zero-price defect
through both the real engine Phase-II function and the canonical scorer.

`TFB_PRICE_PRECISION` is unset/off by default. Values `1`, `true`, `on`, and
`yes` arm one shared rounding contract. Prices with magnitude below 10 retain
at least seven significant digits; prices at or above 10 keep four decimals.
Zero/non-finite engine values and the scorer's null behavior are unchanged.
Every updated four- or six-decimal call preserves its exact original rounding
when disarmed. No return rounding, score threshold, horizon policy or schema
changes are included.

The six-decimal analyst-target anchors join the precision lane because an
otherwise preserved tiny provider target would disappear before downstream
forecasting. Price changes, historical/52-week quotes, portfolio amounts and
their six-decimal contracts remain outside this work package.

The brief proposes a fixed 0.00005/current-price presentation allowance.
For SHIB-sized prices that would accept nearly nine units of return error.
Instead the armed presentation and intrinsic-display receipt checks use the
actual price rounding quantum. Disarmed presentation checks remain identical.
Tests demonstrate both acceptance of proved rounding noise and rejection of
an unrelated 99% return against a 20% tiny-price forecast.

Validation uses real modules without provider credentials or network calls:

- Gate vocabulary and health-state readback; exact legacy rounding across
  positive/negative prices from 1e-8 through 1e6 at four and six decimals.
- Engine synthesis, both provider-target horizon branches, local fallback,
  canonical scorer aliases, provider cap, intrinsic model and display receipts.
- Pair rewrite idempotence, tiny analyst-target publication and bounded
  copy-only presentation checks.
- `tests/test_de_price_precision.py`, `tests/test_data_engine_v2.py`,
  `tests/test_de_f7_scoring_settle.py`, `tests/test_sheet_presentation_contract.py`,
  `tests/test_de_f7_intrinsic_display_basis.py` and
  `tests/test_de_fc_tuple_coherence_p102.py` pass in the pinned Python 3.11
  environment.

Module versions: engine 5.152.0, scorer 5.11.4, presentation 1.0.2;
new leaf `core.price_precision` 1.0.0. Readback is
`/health.engine_gates.price_precision` and the GUARDS boot banner.
Deployment and flag arming remain operator actions. After arming on Render,
inspect a fresh export for nonzero tiny-price forecasts and zero forecast/ROI
pair disagreements; unset the flag to restore legacy precision.
