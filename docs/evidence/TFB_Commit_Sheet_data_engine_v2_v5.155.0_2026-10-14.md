# TFB Commit Sheet - core/data_engine_v2.py v5.155.0 [suffix locale fallback, gate TFB_ENGINE_SUFFIX_LOCALE_FALLBACK] + tests/test_de_suffix_locale_fallback.py

Renumbered 2026-10-21: main shipped a different data_engine_v2 5.155.0 first (#810, PF-SUBUNIT: My_Portfolio SAR weights for subunit-quoted holdings). This build is now **v5.156.0** on top of it: its WHY block sits above main's, off is byte-identical to main's 5.155.0 (the harness's G1-G3 golden cases pass with `TFB_TEST_DE_BASE=<git show 55b47f24:core/data_engine_v2.py>`), and the verifier and the tiny-quote harness pin 5.156.0. The figures below are as measured on 2026-10-14 against 5.154.0.

Renumbered again 2026-10-22: main shipped a different data_engine_v2 5.156.0 first (#816, PF weight fail-closed fixes: PORTFOLIO_VALUE_INCOMPLETE and case-sensitive subunit FX keys). This build is now **v5.157.0** on top of it. Its docstring WHY block keeps its place and #816's comment block sits above this build's one-line note beside `__version__`. Off is byte-identical to main's 5.156.0: `tests/test_de_suffix_locale_fallback.py` G1-G3 now take `BASE_VERSION = "5.156.0"` (`TFB_TEST_DE_BASE` = `git show df99bcf3:core/data_engine_v2.py`) and pass, as does the rest of the suite. Pins moved in place: `scripts/verify_deployment.py` and `tests/test_core_repair_tiny_quotes.py` read 5.157.0. No behaviour changed in the renumbering.

Renumbered 2026-10-22 (second) to 2026-10-28: main #817 took 5.157.0 while open #818 claimed 5.158.0, so this build became **v5.159.0** (b08d01cc). Main #865 and #867 then shipped 5.159.0 and 5.159.1, so it became **v5.159.2** (9c96e453, 2026-10-28). Main #871 then shipped 5.159.2 (the BB-1 name rescue accepts the verified Yahoo alias), so this build is now **v5.159.3** (974bc418). Its WHY block sits above #871's v5.159.2 block, which is verbatim. Off is byte-identical to main's v5.159.2: `tests/test_de_suffix_locale_fallback.py` G1-G3 take `BASE_VERSION = "5.159.2"` (`TFB_TEST_DE_BASE` = `git show 729a316e:core/data_engine_v2.py`) and pass, 17 passed. In between, the base followed main: 5.157.0 (#817), 5.157.3 (#821-#829), 5.158.0 (#819) and 5.159.1 (#867). Pins: `scripts/verify_deployment.py` and `tests/test_core_repair_tiny_quotes.py` read 5.159.3. No behaviour changed in the renumbering.

Date: 2026-10-14 (Wednesday) - Lane: Render / Python (engine; backend ENV, read at call time) - Build: daily improvement 2026-10-14, priority (a) "a defect making a published recommendation or outcome number wrong" - File status: engine **REVISED** 5.154.0 -> 5.155.0 - harness **NEW** - verifier pin **REVISED** 1.0.61 -> 1.0.64 - tiny-quote harness pin **REVISED** - this sheet **NEW**

## S0 - Base pin
| Item | Value |
|---|---|
| Base commit | `030e22eb2d496a0d266536affb47ade7bcebb9f4` (main, "Fix Top-10 KPI readers and audit worksheet row references (#775)") |
| Base file | `core/data_engine_v2.py` sha256 `247e985bae49696d1befa49f5af7cf13e7cdc412a95c30f5196b0ff6881e451a`, 20,418 lines, `__version__ = "5.154.0"`, AST walk-all 559 defs / 10 classes |
| Pristine copy | `git show 030e22eb:core/data_engine_v2.py` in a scratch dir (the harness `TFB_TEST_DE_BASE`), and a scratch base tree from `git archive 030e22eb core` plus `tests/fixtures` for the golden-negative run |
| Delivered engine | sha256 `ad8547cf19f04afe5bdd2bbdcc2c1755f6e32980b89beec57317a77d14590a32`, 20,521 lines (+103), 2 lines edited in place (title, `__version__`), AST walk-all 561 / 10, **0 removals**, +2 defs (`_suffix_locale_fallback_enabled`, `_suffix_locale_nz_for`) |

## S1 - Evidence (why this item)
- **Published number.** Top_10_Investments export of 2026-10-13 20:37 Riyadh (footer: route v4.18.1, builder v1.26.1, pool body_rows/9786), audit row 433: `MWG.VN | Mobile World Investment Corporation | NASDAQ/NYSE | Consumer Discretionary | USD | 74,700.00 | 280,653.13 | ... | DO_NOT_INVEST | ... | Quote Freshness`. 74,700 x 3.75707 = 280,653.13. At VND 0.000148 the true Price SAR is about 11.06, so the published number is about 25,370x too large. It is the only row of the 500 whose dotted suffix the engine table does not know.
- **Freshness was the first fail.** The export's First Fail is Quote Freshness (10.1h old, no `.VN` venue calendar). The fail order does not show whether Blocked Identity also failed. In an offline replay (`build_opportunity_payload`, research stage, trust gate off, USD 3.7571) the same row with a live in-session quote and no BLOCKED stamp is INVEST and takes a research seat. Price SAR is 280,655.37 and the entry zone is 272,235.71-283,461.92 SAR. Labelled VND, it is 11.06 SAR.
- **Not new.** All 10 `.VN` rows in the 2026-10-10 Global_Markets fixture (`tests/fixtures/wp3ab_tuple_rows_2026-10-10.json`: FPT, ACB, VIC, MWG, MBB, PLX, BID, SSI, POW, MSN) are BLOCKED 'Identity unverified - re-fetch required'. Every one carries `market_cap_currency_suspect`, which is emitted only when currency == "USD" (`_sanitize_cross_currency_revenue`). The builder's v1.10.2 G-1 note records the same corruption on CTG/MBB/TCB/VIB/VPB.VN on 2026-08-09. Consumers were guarded; the producer was never fixed.
- **Not covered by open work.** Checked #770, #771, #773, #776, #750, #749, #720 and #721, and the 2026-10-14 review docs. #770's commit sheet names this defect under "Known limits" and defers it ("upstream identity defect ... reported, not fixed here"). No open PR changes the suffix currency or exchange inference.

## S2 - Root cause (traced offline; providers stubbed; no network)
1. Global_Markets and Top_10_Investments ask EODHD first (`eodhd`, `yahoo_chart`, `finnhub`). Its real-time payload has no exchange or currency, and `_GEO_BY_SUFFIX` lacks `.VN`, so the patch arrives with both blank (`quote_exchange_missing; quote_currency_missing`).
2. `_canonicalize_provider_row` fills blanks from `_infer_exchange_from_symbol` / `_infer_currency_from_symbol`. `_SUFFIX_TO_LOCALE` (99 entries) has no `.VN`, so they return `"NASDAQ/NYSE"` / `"USD"`.
3. `_infer_country_from_symbol` already falls back to `core.symbols.normalize` and returns Vietnam. That is why the row reads Vietnam / NASDAQ/NYSE / USD. `_nz_get_currency_from_symbol` has been imported since v5.68.0 and never called.
4. Every later merge and the Yahoo enrichment pass only fill blanks, so Yahoo's VND / HOSE never replaces the stamped USD.

The same mechanism hits 15 suffixes that normalize knows and the engine table lacks: `.VN .HOSE .SET .PS .PSE .AU .KO .BE .DU .DUB .FP .IM .VX .BU .CNQ`. On each one, base v5.154.0 names the right country and stamps USD.

## S3 - Change (anchored edits, count == 1 each)
| # | Site | Edit |
|---|---|---|
| 1 | title `- v5.154.0` | -> `- v5.155.0` |
| 2 | before `WHY v5.154.0` | insert `WHY v5.155.0 (2026-10-14)`: evidence, root cause, change, not-changed, off, rollback. Every prior WHY is verbatim. |
| 3 | terse lines above `__version__` | + 3 v5.155.0 lines; `__version__ = "5.155.0"` |
| 4 | before `def _infer_exchange_from_symbol` | + guarded import `get_exchange_display as _nz_get_exchange_display`, + `_SUFFIX_LOCALE_FALLBACK_ENV`, + `_suffix_locale_fallback_enabled()` ({1,true,yes,on}, default "0"), + `_suffix_locale_nz_for(symbol)` |
| 5 | `_infer_exchange_from_symbol`, before `return "NASDAQ/NYSE"` | `_nzl = _suffix_locale_nz_for(s)  # v5.155.0: off -> None`; return `_nzl[0]` if set |
| 6 | `_infer_currency_from_symbol`, before the final `return "USD"` | same; return `_nzl[1]` if set |

`_suffix_locale_nz_for` returns normalize's (exchange, currency, country) only when **all** of these hold:
- the gate is armed;
- the symbol is dotted;
- the local table has no entry for it;
- all three values are present;
- the currency is a 3-letter alphabetic code other than USD;
- the country is not a US token;
- the exchange is not NASDAQ/NYSE.

Otherwise it returns None. It never raises.

Table hits, 4-digit Tadawul codes, `=X`, `=F`, crypto pairs, undotted tickers and share classes (BRK.B) return before it is consulted.

**Not changed:**
- `_SUFFIX_TO_LOCALE`;
- the repair block in `_canonicalize_provider_row`;
- `_infer_country_from_symbol`;
- `surface_gate_states()`: no new `/health` key, so the payload is byte-identical;
- every provider, `identity_guard` and `opportunity_builder`.

Suffixes no table knows (`.PH .MA .ETR .LG .BH`) still fall back to USD.

## S4 - Audits (REAL module; offline; no credentials; no live Sheet)
| Battery | Command | Result |
|---|---|---|
| Compile | `python -m compileall -q main.py core routes scripts tests` | OK |
| Harness, with base | `TFB_TEST_DE_BASE=<base copy> python tests/test_de_suffix_locale_fallback.py` | **PASS 16/16** |
| Harness, pytest, with base | `TFB_TEST_DE_BASE=<base copy> python -m pytest -q tests/test_de_suffix_locale_fallback.py` | 17 passed (16 cases + the self-consistency test) |
| Harness, pytest, CI shape (no base) | `python -m pytest -q tests/test_de_suffix_locale_fallback.py` | 14 passed, 3 skipped (G1-G3 need the base) |
| **Golden negative** | the same harness run inside a scratch tree whose `core/` is `git archive 030e22eb core` | **PASS 1/13 (3 skipped)**. W1 fails with `('NASDAQ/NYSE', 'USD', 'Vietnam')`; W3 fails on Global_Markets; W5 fails with `{None: ('USD', 280653.13), '1': ('USD', 280653.13)}`, i.e. the armed base still publishes 280,653.13 |
| G2 in-process golden negative | `assert_vn_fixed(base)` under the armed env | raises AssertionError on base; passes on head |
| G1 OFF identity | the 9 OFF spellings x the 2,789-symbol census (both suffix tables, the class and untouched shapes, and 2,639 fixture symbols): `(exchange, currency, country)` head == base. Price-only canonicalization for the class plus the untouched shapes: head == base. `surface_gate_states()`: head == base | PASS |
| G3 zero removals | AST walk-all, base vs head | removed `[]`, added exactly the 2 helpers |
| CI lean list + engine suites, one process | the 106 test files of ci.yml's "Decision-layer suites (lean)" job + every `tests/test_de_*.py` + `tests/test_data_engine_v2.py` + `tests/test_core_repair_tiny_quotes.py` + this harness | **4157 passed, 24 skipped, 0 failed** |
| Remaining engine/verifier readers | the 15 further `tests/*.py` that name `verify_deployment`, `data_engine_v2` or `core_repair` | 280 passed, 8 skipped |
| Manifest pins | `python tests/test_verify_manifest_pins.py` | 3/3 PASS |
| Render ENV contract | `python scripts/render_env_contract.py --check` | OK (unchanged; no FLAGS entry added) |
| ASCII | added lines in `core/data_engine_v2.py` | 0 non-ASCII. The diff's only non-ASCII line is the pre-existing `SCRIPT_VERSION` line of `verify_deployment.py`, edited in place (`61` -> `64`) as every prior bump did |

### Measured, armed vs off (offline)
| Surface | Off (v5.154.0 behaviour) | Armed |
|---|---|---|
| `_infer_*("MWG.VN")` | NASDAQ/NYSE, USD, Vietnam | HOSE, VND, Vietnam |
| `_canonicalize_provider_row` (EODHD price-only patch) | NASDAQ/NYSE, USD, Vietnam | HOSE, VND, Vietnam; no other field moves |
| Full `_get_enriched_quote_impl` (real EODHD adapter over a mocked real-time payload; Yahoo fundamentals return VND/HOSE) on Global_Markets and Top_10_Investments | NASDAQ/NYSE, USD; `market_cap_currency_suspect` present | HOSE, VND; tag absent; price 74,700 and provider eodhd unchanged |
| `identity_guard.currency_is_consistent("MWG.VN", ccy)` | False | True |
| Builder `normalize_candidate` Price SAR | **280,653.13** (the published value) | **11.06** (builder default VND 0.000148) |
| Fixture census (2,639 symbols) | - | exactly the 10 `.VN` rows move; nothing else |

## S5 - Delivery and what is deliberately not done
Files:
- `core/data_engine_v2.py` (REVISED 5.155.0);
- `tests/test_de_suffix_locale_fallback.py` (NEW);
- `scripts/verify_deployment.py` 1.0.64: data engine pin 5.154.0 -> 5.155.0 only. `tests/test_verify_manifest_pins.py` makes the pin mandatory. The version is numbered after the open #773 (1.0.62) and #770 (1.0.63);
- `tests/test_core_repair_tiny_quotes.py`: `ENGINE` pin -> 5.155.0. Its CLI compares the pin with the source constant, so the pin moves in place;
- this sheet.

**Not done, on purpose**
- **Gate not armed.** The brief forbids arming. To arm: Render ENV `TFB_ENGINE_SUFFIX_LOCALE_FALLBACK=1`.
- **No `verify_deployment` FLAGS row and no `surface_gate_states` key.** Either would change the Render ENV contract doc or the `/health` payload, and #770 is also editing that doc. Proposed as a follow-up once #770 lands, following the v1.0.48 precedent.
- **No `ci.yml` edit.** The whole-suite job runs `pytest -q tests`, so the harness is already collected. Adding it to the lean list would collide with the `ci.yml` headers of #770 and #773.
- **`_SUFFIX_TO_LOCALE` and the repair block are untouched.** A provider that itself declared USD for `.VN` would not be corrected, and none does (EODHD sends no currency; both Yahoo providers resolve VND).
- **Unknown suffixes are not guessed.** `.PH`, `.MA` (Spain in the EODHD table, Morocco in `identity_guard`), `.ETR`, `.LG` and `.BH` are unknown to every table and keep USD. `.BH` has 4 fixture rows priced about 0.5, which looks like BHD; its label is not in the fixture. Proposed: add `.BH` to `core/symbols/normalize.py` with evidence.
- **`.AT` left alone.** Its label disagreement (Athens in the engine, Vienna in normalize, EUR either way) does not move Price SAR.
- **No consumer-side guard in `opportunity_builder`** (for example, cutting price_sar when the suffix currency disagrees). That file is open in #770, and fixing the producer removes the cause.

## S6 - Rollback
Unset `TFB_ENGINE_SUFFIX_LOCALE_FALLBACK`; no redeploy is needed. With the gate unset, v5.155.0 returns the same values as v5.154.0 for every surface G1 checks.
