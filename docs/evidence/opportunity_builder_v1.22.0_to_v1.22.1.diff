# TFB commit sheet — opportunity_builder v1.22.1 [B4d REL-CLUSTER TAG BASIS] — 2026-09-27

**Build #1 of 2026-09-27 (backend lane).** Owner Claude · approver/executor Emad · read-only session, nothing committed, no ENV touched.

## 1. What and why (one mechanism)
The 2026-09-27 evening audit (cockpit req 244024d0722e) showed the Reliability gate passing on **default** forecast confidence: 3,504 Global_Markets rows tagged `confidence_default_suspected` (2,297 at exactly 63.23%), six reliability values covering 495 of the 501 top-500 candidates, and all three seats (ITRN 71.5, ADAM 71.5, GOOGL 76.5) carrying the tag. The defence already exists — the v1.10.3 **Reliability Cluster** gate (`TFB_T10_EXCLUDE_REL_CLUSTER`, DEFAULT OFF, S-1 window law) — but it recognises the fingerprint by *value* (70.4/71.5/75.4/76.5, i.e. the 64.82/66.41 defaults). The 63.23 default lands on 74.3 / 69.3 / 54.3, outside the shipped set.

v1.22.1 adds a **basis switch** so the gate can use the source row's own provenance witness (the Warnings tag head) instead of chasing arithmetic:

| ENV (read at gate time, no restart) | Values | Effect |
|---|---|---|
| `TFB_T10_REL_CLUSTER_BASIS` | `values` (default) · `tag` · `both` | values = v1.22.0 byte-identical; tag = fail on the Warnings witness; both = either |
| `TFB_T10_CONF_DEFAULT_TOKENS` | csv, default `confidence_default_suspected` | tag heads that count as the witness (matched on `;`-split heads, never as a blob substring) |
| `TFB_T10_EXCLUDE_REL_CLUSTER` | unchanged, DEFAULT OFF | the gate itself; unarmed ⇒ no gate appended in any basis |

Gate name, GATE_ORDER, the default value tuple, every other gate: byte-untouched. Current text discloses the witness (`71.5 [confidence_default_suspected]`); required text names the basis so NEAR MISS / DATA GAPS read truthfully.

## 2. Pinned source (S1)
| Item | Value |
|---|---|
| Repo / branch | `emadsaberbahbah-coder/tadawul-fast` · `main` |
| HEAD at fetch | `2c1aef822e0437afc6f940332ad30dc60b3a2948` (2026-09-26 13:47 Riyadh, #605) |
| Source file | `core/analysis/opportunity_builder.py` v1.22.0 · 6,204 lines · sha256 `529a08644db49dbc43d103f8a3dcf9f8e16cf7d67bef44cb9770522b7b7c907b` |
| Live versions (cockpit 18:09:44) | route v4.16.0 · builder v1.22.0 · actions v1.12.2 |
| Runtime | python-3.11.9 (`runtime.txt`) |

## 3. Delivered files (S5)
| File | Lines | sha256 |
|---|---|---|
| `core/analysis/opportunity_builder.py` **v1.22.1** | 6,314 | `671c9b67484fc7edadf77bfc9ed3d5da004bfa253c55af26fccf8f19d30dde40` |
| `tests/test_opportunity_builder_rel_cluster_tag.py` (new, 8 tests) | — | `5cb3e6ad5443fb58f833b1033170a37cffaa1942c4f4765db732b980fd0b4b3e` |
| `opportunity_builder_v1.22.0_to_v1.22.1.diff` (review aid, not for commit) | 157 | — |

## 4. Build proof (S3/S4)
- **Anchored edits: 4, each `count == 1` asserted** — version constant; header WHY block appended after the v1.22.0 block; `_rel_cluster_values_text … _rel_cluster_assessment` span rewritten (helpers inserted, assessment extended); gate call site required-text via `_rel_cluster_required_text()`.
- `py_compile` OK · **AST defs 171 → 175: added `_env_rel_cluster_basis`, `_env_conf_default_tokens`, `_conf_default_tag_hit`, `_rel_cluster_required_text`; removed 0** · smart-quote scan on added lines: 0 · diff: 114 changed lines.
- **Real-module harness ×3** (both files imported as real modules; `make_criteria` on the live panel values; 6,864 rows = today's Market_Leaders + Global_Markets exports; `normalize_candidate` → `evaluate_gates`): digest `7d766cea1fafeced` identical on all three runs.

| Mode | v1.22.0 vs v1.22.1 | "Reliability Cluster" fails | of which first-fail | seats ITRN / ADAM / GOOGL |
|---|---|---|---|---|
| unarmed | **byte-identical** | 0 (gate absent) | 0 | untouched |
| armed, basis=values | **byte-identical** | 895 | 575 | 71.5 / 71.5 / 76.5 → fail |
| armed, values + `74.3,69.3` via csv | n/a | 991 | 641 | same |
| armed, basis=tag | n/a | **3,504** (= every tagged row) | 1,934 | fail, witness disclosed |
| armed, basis=both | n/a | 3,511 | 1,937 | fail, witness disclosed |

Harness caveat (disclosed): the harness runs the gate layer only, without Render's ENV set (softcaps, F-1b observe, xcheck…), so its other-gate outcomes (e.g. R/R) differ from the cockpit's; it proves version equivalence and the gate's own behaviour, not the board.
- **Tests:** new file 8/8 OK on v1.22.1; existing `tests/test_opportunity_builder.py` 27/27 OK on v1.22.1; the new file **fails on v1.22.0** (4 failures, 1 error) — it discriminates. `tests/test_top10_selector.py` needs pytest (absent here) — pre-existing, unrelated.

## 5. Operator steps (one action each; GitHub web UI)
1. Upload the delivered `opportunity_builder.py` over `core/analysis/opportunity_builder.py`: https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/core/analysis
2. Upload `test_opportunity_builder_rel_cluster_tag.py` into `tests/`: https://github.com/emadsaberbahbah-coder/tadawul-fast/upload/main/tests
3. Commit message: `opportunity_builder v1.22.1 [B4d] Reliability Cluster tag basis (default values = byte-identical) + tests`
4. After Render deploys from `main`, read-back = the next cockpit status line shows `builder v1.22.1` and **zero** "Reliability Cluster" rows in the audit grid (gate unarmed). Board, tickets, KPIs: unchanged.
5. Rollback: revert the commit (no ENV to unset).

## 6. Arming — a separate act, not part of this commit
`TFB_T10_EXCLUDE_REL_CLUSTER=1` + `TFB_T10_REL_CLUSTER_BASIS=both` on Render is the recommended arming. It is **recommendation-changing**: per the v1.10.0/v1.10.3 header law an armed selection-changing gate restarts the 28-day S-1 evidence clock (today 11/28 scored, 40 excluded-infra). Decision for the Saturday review; no default-on shortcut is built in.

## 7. Post-freeze findings → Register (vNEXT)
- `portfolio_actions` ADD path uses the same defaulted reliability (DDI 70.4 pending day 1/2) — same witness, that file's own build.
- The pages carry no `Sector Trend` / `News` columns → those two Top-10 gates are structurally inert (500/500 "Unknown"), independent of this build.
- `_DEFAULT_REL_CLUSTER_VALUES` deliberately not extended; if the values basis is kept, `TFB_T10_REL_CLUSTER_VALUES=70.4,71.5,75.4,76.5,74.3,69.3` covers the 63.23 family without code.
