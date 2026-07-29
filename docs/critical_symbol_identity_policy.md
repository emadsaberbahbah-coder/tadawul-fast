# Critical Symbol Identity Policy

Status date: 2026-07-29

This registry exists because a provider can return a valid-looking but different
instrument under a requested symbol, and the sheet read-back plus KEEP-LAST-GOOD
path can make that contamination persistent.

## Active US mappings

| Input identifier | Provider-safe current identifier | Required issuer identity | Evidence |
|---|---|---|---|
| `BK` | `BK.US` | The Bank of New York Mellon Corporation | SEC 2026 filing lists common stock symbol `BK` on NYSE: https://www.sec.gov/Archives/edgar/data/1390777/000139077726000060/R1.htm ; EODHD identifies `BK.US` as The Bank of New York Mellon Corporation: https://eodhd.com/financial-summary/BK.US |
| `BRK-B` | `BRK-B.US` | Berkshire Hathaway Inc., Class B | SEC 2026 filing lists Class B symbol `BRK.B` on NYSE: https://www.sec.gov/Archives/edgar/data/1067983/000119312526092557/d82599d8k.htm ; EODHD identifies `BRK-B.US` as Berkshire Hathaway Inc.: https://eodhd.com/financial-summary/BRK-B.US |
| `FI` / `FI.US` | `FISV.US` | Fiserv, Inc. | Fiserv's 2025 Form 10-K states that it moved to NASDAQ on 2025-11-11 and changed its ticker from `FI` to `FISV`: https://www.sec.gov/Archives/edgar/data/798354/000079835426000009/fi-20251231.htm ; EODHD identifies `FISV.US` as Fiserv, Inc.: https://eodhd.com/financial-summary/FISV.US |

## Removed from the active Saudi refresh universe

| Identifier | Status | Evidence |
|---|---|---|
| `3001.SR` | Delisted | Saudi Exchange announced Hail Cement's delisting effective after 2024-06-12: https://www.saudiexchange.sa/wps/portal/saudiexchange/newsandreports/issuer-news/news-detail-wcm/?locale=en&newsId=8295 |
| `8270.SR` | Inactive merger/delisting case | Saudi Exchange records Buruj shareholder approval, trading suspension until delisting, and dissolution through the MEDGULF merger: https://www.saudiexchange.sa/wps/portal/saudiexchange/newsandreports/issuer-news/issuer-announcements/issuer-announcements-details/?anCat=1&anId=90920&cs=8270&locale=en |
| `4328.SR` | Unsupported / unverified active listing | No authoritative current Saudi Exchange issuer mapping was located. It stays outside the active universe until an official issuer record is supplied. This is deliberately not described as a confirmed delisting. |

## Fail-closed rules

1. Canonicalize the three US collision identifiers before provider requests.
2. Fetch each critical identifier in a one-symbol batch before normal batches.
3. Validate exact issuer name and available currency/country/exchange metadata.
4. A mismatch writes a tagged symbol-only stub to purge poisoned predecessor data.
5. The page result must be `failed` even when that protective stub write succeeds.
6. `runtime_enabled=false` remains unchanged; this policy contains no scoring,
   recommendation, portfolio, or trading behavior.
