# Market publication follow-up

The primary dashboard sync has its own Sheets writer. Late preservation can restore older margin values, return tuples and horizon labels after the API has presented them. Google Sheets Values updates also skip null cells, so withholding a value as `None` does not clear an older incorrect display.

The final writer boundary validates the four market pages against the exact canonical 115-column matrix, applies the shared copy-only presentation rule after preservation, and sends an explicit empty string for withheld presentation cells. The API continues to return JSON null for unknown values. Portfolio, ledger, account and cash pages retain their existing contracts. Source prices, model prices, scores and acquisition evidence are not repaired by guessing.

Intraday refresh has a separate partial-write contract. It requires successful source acquisition with a fresh actual quote timestamp and matching native currency. It reads raw cells, consumes the real API's object aliases alongside its matrix envelope, rejects contradictory quote aliases, and rechecks the complete current header and row before each bounded cell batch. A price-only refresh marks the carried model as preserved and clears return displays that conflict with the new price; it does not certify older forecasts as fresh.

Regression coverage uses actual writers and the actual intraday main path with fake HTTP and Sheets transports. Seeded existing cells model Google's null-skip behavior. Existing preservation, non-market-page, source-evidence and schema invariants remain checked. The new suites participate in the required lean CI job, and the deployment verifier pins both publication writers and the intraday script.

## Operational acceptance

Publish only after the required checks pass on the integrated commit. Verify the deployed commit and authenticated decision readback, retain a workbook backup, then refresh one existing market page and compare its raw values and membership. A successful refresh of the surviving 255 Market_Leaders symbols cannot establish completeness against the unchanged approved floor of 1,025.

This change does not reconstruct the missing approved roster, amend financial records, place broker orders, install the native Apps Script, or validate forecast accuracy. Those require their respective source evidence or installation/execution receipt.
