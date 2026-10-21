# COMMIT SHEET — scripts/tfb_acceptance.py v1.0.6 (decision-integrity checks A1..A4)

Date: 2026-09-04 (Friday, evening session) · Builder: Claude · Executor: Emad (GitHub web UI)

## 1. Target
| Item | Value |
|---|---|
| Repo | `emadsaberbahbah-coder/tadawul-fast`, branch `main` |
| **Exact repo path** | `scripts/tfb_acceptance.py` (full-file replace) |
| Base pinned | HEAD `04ec928747a7b326402463cf742b064e5c80a494`, blob `1f8f971bc8715aa071f1b7b36b8dfe9cacd797d2`, 773 lines, 40,590 bytes, `VERSION = "1.0.5"` |
| Delivered | blob `a7c14007c9e242e6b5015557aae77482b8793b39`, 1,027 lines, 56,558 bytes, `VERSION = "1.0.6"` |
| Net diff | +254 lines; 3 lines replaced (version string, the `run_all` lambda list, the selftest tally line); 0 lines removed |
| Commit sheet path | `docs/evidence/TFB_Commit_Sheet_tfb_acceptance_v1.0.6_2026-09-04.md` (this file) |
| Commit message | `tfb_acceptance v1.0.6 — decision-integrity checks A1..A4 (venues, USD peg, identity collisions, Δ Shares)` |
| ENV | **none** — read-only instrument; `acceptance_check.yml` (06:45Z) unchanged |

## 2. What changed (additions only)
| # | Anchor (count == 1) | Edit |
|---|---|---|
| E1 | docstring "G1..G4 gates" block | A1..A4 documented |
| E2 | `VERSION = "1.0.5"` | → `1.0.6` |
| E3 | `RIYADH = …` | constants: frozen Derayah suffix set (19), peg band [3.74, 3.77], min rows 5, identity-collision regex, non-equity symbol regex |
| E4 | `def run_all(` | new `_derayah_suffixes` (live from `core.compliance_gate.DERAYAH_MARKETS` when importable, frozen fallback reported in evidence), `_symbol_suffix`, `_top10_sections` (board / qualified / audit), `_implied_fx`, `check_decision_integrity` |
| E5 | `run_all` lambda list | `check_decision_integrity` appended |
| E6/E7 | selftest | 7th fixture (good / bad / empty sources) + tally line |

Checks emitted:
| id | criterion | PASS / WARN / FAIL / NA |
|---|---|---|
| A1 | board seats on Derayah-tradable venues | FAIL if any seat's suffix ∉ DERAYAH set; NA if no board section |
| A2a | USD FX peg — Top_10 audit Price SAR / Price ∈ [3.74, 3.77] | FAIL if median outside; WARN if > 1% rows outside; NA if < 5 USD rows |
| A2b | USD FX peg — Portfolio_Decision USD rows | FAIL if any row outside |
| A3-Global_Markets / A3-Market_Leaders | equity identity collisions (names with "Futures", contract month + year, or trailing "USD" on rows whose suffix is a Derayah venue; `=F/=X/-USD/^` symbols excluded) | FAIL if ≥ 1 |
| A4 | Portfolio_Decision TRIM/EXIT rows carry Δ Shares | FAIL if any sell-side row has a blank/non-numeric Δ Shares |

## 3. Proofs
| Gate | Result |
|---|---|
| `py_compile` | OK |
| AST zero-removal | functions 31 → 37 (removed **none**); classes 5 → 5 |
| `--selftest` | **PASS 7/7 fixtures** × 3 runs (A1..A4 good/bad/empty asserted, incl. "Future FinTech Group" must NOT trip A3) |
| Re-execution on today's real export (W7-b) | A1 FAIL 4 (AEGN.AT, HDFCBANK.NS, BYMA.BA, BMA.BA) · A2a FAIL median 3.8090 (n = 406, 100% outside) · A2b FAIL 6/6 · A4 FAIL 1 (HCI.US) · A3-Global_Markets FAIL **6** · A3-Market_Leaders PASS |
| Parity | every pre-existing check id and verdict identical to v1.0.5 on the same export (23 PASS / 5 WARN / 0 FAIL / 2 NA → +1 PASS, +5 FAIL from the new ids only) |

New evidence surfaced by A3 on the real artifact — three collisions the manual audit missed: **SI.US → "Silver Dec 26", PL.US → "Platinum Oct 26", HG.US → "Copper Dec 26"** (in addition to KE.US wheat, LINK.US Chainlink, NG.US natural gas). Six `.US` equity rows carry commodity/crypto identities with blank sectors. Register: extend P-53 (bare-ticker collision class) from 3 to 6 symbols.

## 4. Post-commit verification (Emad → Claude)
- [ ] Exactly two changed files: `scripts/tfb_acceptance.py` + this sheet under `docs/evidence/`; no other file.
- [ ] Blob on main == `a7c14007c9e242e6b5015557aae77482b8793b39`.
- [ ] Next `acceptance_check.yml` run (06:45Z) prints A1..A4 with the live workbook; expected until the fixes are armed: A1 FAIL, A2a/A2b FAIL, A3-GM FAIL, A4 FAIL — these become the daily read-back for `TFB_T10_VENUE_ALLOWLIST`, `TFB_OPP_FX_SANITY` / the `_Lists_Config` USD cell, the enrichment fix, and the GAS PD writer fix respectively.
