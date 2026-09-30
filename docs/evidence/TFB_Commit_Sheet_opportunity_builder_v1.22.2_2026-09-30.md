# TFB Commit Sheet — core/analysis/opportunity_builder.py v1.22.2 [P-171 / P-149 NEAR-MISS TEXT TRUTH]

Date: 2026-09-30 (Wednesday) · Lane: Render/Python · Build #3 of the day · Protocol: One-Pass

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | `core/analysis/opportunity_builder.py` v1.22.1 at HEAD `6bcf7df` — sha `671c9b67484fc7ed…` (6,314 lines) = the 09-27 delivery (live on Render since 09-28 12:47, `/health` builder 1.22.1) |
| Production facts used | `TFB_OPP_VENUE_FLOORS=1` is armed (proof: the live 27,700 SAR floor on BBOX.L — `_VENUE_COSTS[".L"]` floor); operator floor 1,000 SAR; the 09-30 08:55 cockpit (req d9bf9ef769fb) |
| Environment golden (base, this workspace) | `test_opportunity_builder.py` 27/27 OK · `rel_cluster_tag` 8/8 OK · `test_ob_cash_floor_pct.py` FAIL 2 (t2 age-anchored fixtures dated 09-20; t8 stale pin "1.22.0") · `test_ob_price_xcheck.py` FAIL 5 (age-anchored fixtures) · `test_ob_f1b_plan_basis.py` / `test_ob_ann_roi_annualized.py` stale version pins (1.20.0 / 1.19.5) · `test_top10_selector.py` needs pytest — delivered must reproduce the same FAIL set |

## S2 — Root (pinned on the 09-30 board + source)
1. **P-171** — BBOX.L r27 deferral: "sized ticket 6,737 SAR below minimum ticket floor **27,700 SAR**"; NEAR MISS r50 Required: "fundable amount ≥ minimum ticket floor (**1,000 SAR**)". The sizing block raises the operator floor to the venue floor when `TFB_OPP_VENUE_FLOORS` is on (L5138–5141, v1.1.0 §18.5) and prints the raised value; `_near_miss_rows` prints `criteria["min_ticket_sar"]` (L5486) regardless. Two true numbers, one contradiction, and a CAPITAL_CALL of 27,661 SAR that the Required cell says is 26,661 too high.
2. **P-149 (builder half)** — DDI.US r52: verdict INVEST, `structural_block=True` (Portfolio gate: held, Include Portfolio Holdings = No). `_near_miss_rows` has branches for deferrals, then `elif verdict == INVEST` → "Capacity / rank beyond Max Selected / Max Selected = 10" (L5519) — for a rank-3 name. The cockpit's own "ranked below the Max Selected cut" text is the GAS half (16_Decision_Top10, blocked on the paste).

## S3 — Change (5 anchored edits, each `count == 1`)
| # | Site | Edit |
|---|---|---|
| E1 | header | v1.22.2 WHY block + `OPPORTUNITY_BUILDER_VERSION = "1.22.2"` |
| E2 | after `_venue_floor` | `_env_nearmiss_text_legacy()` (kill: `TFB_OPP_NEARMISS_TEXT_LEGACY=1`) and PURE `_effective_min_ticket(symbol, criteria) -> (floor_sar, "operator" \| "venue:.L")` |
| E3 | sizing floor deferral | the deferral string gains ` (.L venue floor; operator floor 1,000 SAR)` ONLY when the venue raised the floor and the kill is off; the `"minimum ticket floor"` token (substring contract with `_near_miss_rows` and the cockpit) and the number are untouched |
| E4 | `_near_miss_rows` floor branch | Required = the symbol's effective floor: `fundable amount ≥ .L venue floor (27,700 SAR; operator floor 1,000 SAR)` with a note that lowering the operator floor does not unblock it; operator-floor case and the legacy branch print the v1.22.1 strings byte-for-byte |
| E5 | `_near_miss_rows` | new branch before the Capacity branch: an INVEST row with `structural_block` and a `first_fail` is classified by that gate (`Portfolio / held / exclude holdings (Include Portfolio Holdings = No)`, note "held position — add or trim via the Portfolio page, not the board"); other structural gates get a generic structural note |

**DEFAULT ON** (the OFF state IS the defect — P-127 / P-142 / P-145 precedent), kill switch restores v1.22.1 text byte-for-byte. Display semantics only: selection, gates, sizing, deferral decisions, KPIs, alerts, funding plans and CAPITAL_CALL numbers are byte-untouched (harness N4/N6).

Deliberate cuts: the cockpit's "Why Not Selected" column text for held names (GAS, 16_Decision_Top10 v1.11.12 on the paste); whether a 27,700 SAR LSE floor is the right policy for a 92k SAR book (a Saturday parameter question — `TFB_OPP_VENUE_FLOORS` is an ENV, not code); P-108/P-141 KPI header (parked, un-park word owed).

## S4 — Audits (×3 identical)
| Check | Result |
|---|---|
| Delivered SHA-256 | `d53795d10e4658665bcbd64c9caa86ef7ad929e273d24370f9159d1058faf230` (6,420 lines) |
| `py_compile` | PASS |
| AST | functions 175 → 177 (+2, **0 removed**) |
| Line audit | 9 base lines not verbatim = version constant · the deferral closing line (extended) · the 7-line legacy floor branch re-indented under the kill switch (same text) |
| Non-ASCII | 0 new characters vs base; 0 smart quotes; `—` / `≥` kept as source escapes like the base |
| Harness `tests/test_ob_nearmiss_text_p171.py` (REAL module, real `build_opportunity_payload` on today-dated fixtures replaying the 09-30 shape: BBOX.L in GBX under `TFB_OPP_VENUE_FLOORS=1`, held DDI.US, cash 34,166.25; dual-tree vs the v1.22.1 file via `OB_BASE`) | **N1–N7 PASS ×3, digest `ed1df675515d6766` ×3** |
| N2 | BBOX.L deferral "below minimum ticket floor 27,700 SAR (.L venue floor; operator floor 1,000 SAR) \| CAPITAL_CALL …"; near-miss Funding / Required `fundable amount ≥ .L venue floor (27,700 SAR; operator floor 1,000 SAR)`; token kept |
| N3 | DDI.US near-miss = Portfolio / held / exclude holdings …, note "held position"; no Capacity label |
| N4 | kill switch: v1.22.1 strings reproduced (1,000 SAR Required; Capacity); selection / kpis / alerts identical to default |
| N5 | venue floors off: no venue text in either mode |
| N6 | **legacy payload == base payload** (version fields masked); default differs from base ONLY in `near_miss` and BBOX.L's `candidates_rows.deferral`; `selected` / `kpis` / `alerts` equal base |
| N7 | idempotent |
| Existing batteries | `test_opportunity_builder.py` 27/27 ×3; `test_opportunity_builder_rel_cluster_tag.py` 8/8 ×3 after one pin literal (1.22.1 → 1.22.2); cash_floor FAIL 2 / price_xcheck FAIL 5 / f1b / ann_roi / top10_selector = **the same FAIL set as base** (age-anchored fixtures, stale pins, pytest absent) |

## S5 — Delivery
| File | Destination |
|---|---|
| `core/analysis/opportunity_builder.py` | repo (full file) |
| `tests/test_ob_nearmiss_text_p171.py` | repo `tests/` (new; `OB_BASE=<v1.22.1 file>` enables N6) |
| `tests/test_opportunity_builder_rel_cluster_tag.py` | repo `tests/` (one pin literal; otherwise byte-identical) |
| `docs/evidence/TFB_Commit_Sheet_opportunity_builder_v1.22.2_2026-09-30.md` | repo `docs/evidence/` |

## S6 — Deploy / read-back (Render lane)
1. Commit the four files. **Do not deploy today only for this** — each Manual Deploy wipes the L1 fund cache (≈ 35–40 k EODHD calls on the next run); batch it with the next deploy that is needed anyway (or the Saturday sitting). No ENV change.
2. Read-back after the first deployed cockpit run: a sub-venue-floor deferral row prints the `(… venue floor; operator floor …)` parenthetical and its NEAR MISS Required names the venue floor; a held INVEST name (DDI.US today) appears under `Portfolio` in NEAR MISS. Zero change to Selected / KPIs / alerts on the same inputs.
3. Rollback: `TFB_OPP_NEARMISS_TEXT_LEGACY=1` on Render (call-time read, no redeploy) or `git revert`.
