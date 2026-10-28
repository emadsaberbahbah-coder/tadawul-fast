# Commit Sheet - `scripts/provider_identity_probe.py` v1.0.0 + `provider_identity_probe.yml` - 2026-10-28

**Scope:** plan item P1-5 / WP-05 (crypto identity), widened to every Commodities_FX row that fails daily. A read-only tool that answers one question before anything is armed: which provider code returns the right instrument at a fresh price? Nothing is armed, written or deployed by this change.

**Files:**
- New: `scripts/provider_identity_probe.py`.
- New: `data/provider_probe_cfx_2026-10-28.json`, ASCII, 19 rows (17 failing rows and 2 controls).
- New: `.github/workflows/provider_identity_probe.yml`, dispatch only.
- New: `tests/test_provider_identity_probe.py`, 15 cases.
- New: this sheet.
- Changed: `scripts/verify_deployment.py` pins 1.0.0.
- Changed: `.github/workflows/ci.yml` 1.1.51 adds the test to the lean lane.

## S1 - WHY (28 Oct 2026, morning)
**The board.** `Top_10_Investments` B2 at 08:09 and 08:49 Riyadh read:
- `source readiness WITHHELD: … Commodities_FX acquired 430/453 below 95%; Commodities_FX acquisition not COMPLETE …`
- `stab: frozen`

So the 28 Oct stability day does not count.

**The sync.** In run `40604805750`, the Commodities_FX leg wrote 453 rows but only 430 were fresh. The inline recovery rewrote the page, and its audit stayed blocked at the same 430 (`TFB_AUDIT_MIN_FRESH_PCT` 95).

**The page itself** (gviz read, 28 Oct 08:55): 436 `success`, 7 `preserved`, 10 `unavailable`.

| Rows | What the page shows | Why |
|---|---|---|
| SUI-USD, GRT-USD, APT-USD, UNI-USD, COMP-USD, IMX-USD, STX-USD | "Salmonation", "Golden Ratio Token", "Apricot Finance", "UNICORN Token", "Compound Coin", "Impermax", "Stox"; price 0.00 (GRT 0.45); last written **23 Sep** | Short-ticker collisions on Yahoo / EODHD. WP-05 (11 Oct) added `TFB_EODHD_CRYPTO_MAP=1` (BASE-USD → EODHD `BASE-USD.CC`), default off: *"arm only after a provider read-back"*. |
| XAUUSD=X, BHDSAR=X, BRLBHD=X, AEDMYR=X, AEDPHP=X, AEDIDR=X, BHDPLN=X, AEDMXN=X | no price (`eodhd`, unavailable) | Gold spot and exotic crosses that no current route prices. |
| LBS=F, DX=F | no price (`yahoo_chart`, `history_or_fallback`) | CME replaced LBS with LBR; DX=F has no fresh quote. |

**The cost.** These 17 rows fail every day. The 95 % floor allows 22 non-fresh rows, so only 5 rows of slack remain for normal staleness. On 28 Oct six more rows were stale, and the board was withheld.

**What blocked a fix.** No read-back tool existed: the build container cannot reach the providers, and the backend is token-gated.

## S2 - WHAT
1. **Spec** (`data/provider_probe_cfx_2026-10-28.json`): explicit candidate codes per row, so nothing is inferred.

   | Row group | EODHD candidates | Yahoo candidates |
   |---|---|---|
   | Crypto | the WP-05 armed route (`.CC`) and today's legacy route; plus an EODHD search on the root | the short ticker and its CMC-suffixed id (e.g. `SUI20947-USD`) |
   | Gold | `XAUUSD.FOREX` | `XAUUSD=X`, and the front future `GC=F` as a cross-check |
   | Crosses | `.FOREX`, the code `TFB_EODHD_FX_MAP` would request | the page's `=X` ticker |
   | LBS=F | none | LBS=F, and its successor LBR=F |
   | DX=F | none | DX=F, and the index DX-Y.NYB |
   | Controls | `BTC-USD.CC`, `EURUSD.FOREX` | `BTC-USD`, `EURUSD=X` |

2. **Probe.** For each code it records the price, its age and the name, from EODHD real-time and search and from the Yahoo chart. It makes one retry on 429 or 5xx, uses a 15 s timeout, and scrubs the key from every message.
3. **Verdict per row:**
   - **CONFIRMED** when an EODHD and a Yahoo candidate agree within 3 %. For a curated crypto root, a returned name must also pass the WP-05 name screen (`core.symbols.crypto_identity.crypto_name_matches`).
   - **AVAILABLE** when some candidate prices fresh (≤ 72 h).
   - **NAME_MISMATCH** when the only fresh candidates are the wrong coin.
   - **DEAD** when nothing prices.

   A wrong-coin candidate is always listed. This is the case the read-back exists for: if EODHD's `SUI-USD.CC` is the same colliding coin, arming the WP-05 route would not fix the row, and the probe says so.
4. **Output:** a markdown table (log and job summary) and `provider_probe.json` (artifact, 30 days).
5. **Workflow:** dispatch only. Read-only `contents` permission. The only secret is `EODHD_API_KEY`. No workbook credentials.

## S3 - Tests (`tests/test_provider_identity_probe.py`, 15 passed; offline)
| Group | Proves |
|---|---|
| P1 | The default spec is ASCII and holds the 17 rows plus 2 controls. Bad items are refused. |
| P2 | Parsers on recorded shapes: EODHD real-time (price, `NA`, stale); search (`Code.Exchange`, name, previous close); Yahoo chart (price, *No data found*, stale). |
| P3 | `.CC` that is the right coin reads CONFIRMED. `.CC` that is the colliding coin reads AVAILABLE via the Yahoo CMC id, with the mismatch listed and never CONFIRMED. Exotic cross with no source reads DEAD. Gold EODHD vs GC=F (0.2 %) reads CONFIRMED. LBS=F reads AVAILABLE via LBR=F. A stale DX=F does not count. A crypto pair without any name is not CONFIRMED. |
| P4 | `main()` writes the table, the job summary and the evidence, and the key never appears. Without a key only Yahoo runs. Network down gives exit 1 (scrubbed). Bad spec gives exit 2. One retry on 429. |
| P5 | ASCII module, version and manifest pin. Dispatch-only, read-only workflow with no workbook credentials. |

Workflow audit: errors 0, warnings 0. Manifest pins pass.

### Review fix (Codex, PR #863, head `301be8c8`)
| Finding | Fix | Golden negative |
|---|---|---|
| A wrong-coin candidate that priced within 3 % of a correctly named candidate from the other provider was CONFIRMED: the passing name overrode the explicit mismatch. Example: EODHD `SUI-USD.CC` named *Salmonation* at 2.11 against Yahoo `SUI20947-USD` *Sui USD* at 2.12. | A pair is rejected when either side's name explicitly fails the screen. A curated root still needs at least one passing name. | `test_p3_named_wrong_coin_at_a_matching_price_is_not_confirmed` fails on `301be8c8`. |

## S4 - Rollback
Delete the workflow and the script. Nothing reads them, and nothing is armed.

## S5 - Operator steps
1. Merge.
2. Claude dispatches **🔎 Provider Identity Probe** once and reads the table.
3. Claude proposes the smallest fix per row group, and each one waits on the owner:

   | If the probe shows | Fix | Owner action |
   |---|---|---|
   | `.CC` rows CONFIRMED | `TFB_EODHD_CRYPTO_MAP=1` | Render env + restart, outside a sync |
   | `.CC` is the wrong coin | CMC-suffixed Yahoo ids | universe / identity decision (WP-05: an owner decision) |
   | `.FOREX` crosses AVAILABLE | `TFB_EODHD_FX_MAP` | review first: it reroutes every `=X` row sent to EODHD |
   | LBS=F DEAD, LBR=F AVAILABLE | replace LBS=F with LBR=F on the page | keeps the 453 row floor |

   The 95 % floor and the 453 row floor stay as they are.

Not a licensed financial adviser; the owner decides and executes every trade.
