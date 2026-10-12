Market RAW write receipts now include a complete intended-cell SHA256, computed
after the Name, OHLC, ranking, presentation and final warning-publication seams.
This is the submitted header plus rectangular row matrix, not an independent
readback. `live_readback` remains false; the legacy SHA8 keeps its old meaning.

The contract is `tfb.sheets-raw-typed-binary64/v1`. Hash the UTF-8 encoding of
compact JSON with sorted object keys, `ensure_ascii=False`, `allow_nan=False`
and separators `(',', ':')`, with this exact structural envelope:

```json
{"contract":"tfb.sheets-raw-typed-binary64/v1","headers":[["string","exact header"]],"rows":[[["string","exact cell"]]],"width":1}
```

Header and row order, width and row boundaries are significant. Headers must be
strings; rows must be rectangular. A cell is exactly one of these typed arrays:

- `None` or `""`: `["blank"]`.
- Boolean: `["bool", true]` or `["bool", false]`.
- Nonempty string: `["string", exactString]`, without trimming or Unicode normalization.
- Finite binary64 number: `["number", float.hex()]`. Positive/negative zero share
  `"0x0.0p+0"`; every nonzero double remains exact, including subnormal prices.
  An integer is accepted only if its finite binary64 conversion is lossless.

Google documents supported values as bool/string/double and `numberValue` as a
double. It does not establish universal 15-significant-digit rounding. This
contract therefore uses exact numeric values and no tolerance or decimal rounding.
Sources: [ValueRange](https://developers.google.com/workspace/sheets/api/reference/rest/v4/spreadsheets.values#ValueRange),
[ExtendedValue](https://developers.google.com/workspace/sheets/api/reference/rest/v4/spreadsheets/other#ExtendedValue),
[RAW input](https://developers.google.com/workspace/sheets/api/reference/rest/v4/ValueInputOption).

The successful-update receipt exposes `written_payload_sha256`,
`canonical_method`, `canonical_version`, `canonical_width`, `canonical_rows`,
`canonical_status` and `canonical_verified`. Unsupported/nonfinite cells, unsafe
integers or invalid geometry produce `UNVERIFIABLE`, a bounded error code and no
full SHA256. Evidence failure does not alter the actual cells or abort an otherwise
successful RAW update. A failed update never publishes a successful-write receipt.

Blank equivalence is a state convention: RAW null skips a cell, whereas an empty
string clears it. Independent readback must detect any stale nonblank value left
behind. Native `UNFORMATTED_VALUE` preserves stored numeric/string/boolean types;
formatted values or an XLSX export may lose precision or types and cannot justify
a tolerance. Readback geometry must come from the complete expected native range,
because the values API omits trailing blank rows/columns. No automatic readback,
new API/provider/flag, financial-cell rewrite or native consumption claim is added.
