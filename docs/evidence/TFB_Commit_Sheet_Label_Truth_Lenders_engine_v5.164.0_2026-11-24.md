# Commit sheet: the leverage label leaves lenders out (data_engine_v2 5.164.0)

Date: 2026-11-24. Owner, 22:47 Riyadh: is `TFB_LABEL_TRUTH=enforce` needed from me now? At 22:56 he sent the Render
log and the workbook export and asked for a full review, with any updates the review shows are required.

## What the workbook shows (the observe tags of 5.163.0)

| Tag | Global_Markets | Market_Leaders | My_Portfolio | What enforce would do |
|---|---|---|---|---|
| `label_truth_horizon` | 6,596 of 6,612 | 255 of 349 | 8 of 8 | Show 3M / 90 days, the horizon the scorer used, instead of 1Y / 365. Display only; the weights do not change. |
| `label_truth_leverage:would_flag` | 719 | 25 | 0 | Add "High leverage" to Top Risks. That cuts conviction_score by 8 points, a 20 % weight of the Top 10 composite. |
| `label_truth_percent` | 0 | 0 | 0 | Nothing |
| `label_truth_float` | 0 | 0 | 0 | Nothing |

Of the 719 leverage flags, 187 fall on lenders:

| Group | Flags |
|---|---|
| Financial Services sector (banks, credit, capital markets, insurers) | 153 |
| Mortgage REITs | 34 |

Three of those lenders are on today's Top 10:

| Symbol | Industry | D/E |
|---|---|---|
| BLX.US | Banks - Regional | 3.5 |
| SAN.MC | Banks - Diversified | 4.3 |
| LADR.US | REIT - Mortgage | 3.0 |

For a lender, debt against equity is the business, because deposits and borrowings fund the loan book. Standard
analysis does not read the ratio for them. Under enforce, these three seats would move on a label that does not apply
to them.

## Change

`_label_truth_high_leverage(de, mode, sector, industry)`:

- A lender is a row whose sector names finance, or whose industry is "REIT - Mortgage" or "Mortgage Finance".
  The ratio rule never flags a lender.
- In observe, a lender is tagged `label_truth_leverage:lender:observe`, but only where the legacy >= 100 test flags it.
- Every other row behaves exactly as in 5.163.0.
- With `TFB_LABEL_TRUTH=off`, nothing changes: the legacy test runs and the sector is not read.

## Tests

- `tests/test_de_label_truth.py`: lender cases for all three modes, plus a bank row and a non-bank row through the
  engine.
- The engine version pins are bumped in their suites.
- All 22 `test_de_*` suites pass: 533 passed.

## Owner decision after merge and deploy

With this change in, `TFB_LABEL_TRUTH=enforce` shows the true horizon on every row. It also flags high leverage on
about 557 non-lenders: 532 on Global_Markets and 25 on Market_Leaders, none of them a holding or a Top 10 name today.
It is safe to turn on after the next sync shows the lender tags.

This is not investment advice.
