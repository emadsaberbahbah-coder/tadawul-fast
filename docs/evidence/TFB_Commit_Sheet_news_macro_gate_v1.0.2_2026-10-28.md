# Commit Sheet - `scripts/news_macro_gate.py` v1.0.2 - 2026-10-28

**Scope:** a stable verdict. The first two runs of v1.0.1 on main (`2f8ead9f`) came eight minutes apart and disagreed, because the second fetch missed a wire story the first one had. This change adds:
1. a 24 h / 72 h memory of recorded headlines;
2. English Google-News for the wires and for US names;
3. INFO flags in the gate row.

**Files:**
- `scripts/news_macro_gate.py`: 1.0.1 -> 1.0.2; WHY block prepended, earlier text kept verbatim.
- `tests/test_news_macro_gate.py`: 74 cases.
- `scripts/verify_deployment.py`: pin 1.0.2.
- This sheet.

No workflow, tab, header or taxonomy change.

## S1 - Evidence (27 Oct UTC = 28 Oct Riyadh)
| Run | When (UTC) | Mode | GDELT | Google-News | Verdict |
|---|---|---|---|---|---|
| `40599453067` | 23:03:56-23:08:37 (4 m 38 s; v1.0.0 took 11 min) | dry run | 45 items; `central banks` HTTP 429; budget 240 s spent, 11 queries skipped | 380 | **NO_NEW_ENTRY** |
| `40600324126` | 23:11:02-23:14:45 | `write=true` | 35 items; `oil` HTTP 429; circuit open, 14 queries skipped | 400 | **CAUTION** |

Both runs:
- Scope: 15 names.
- EDGAR 19, Fed 0, econ 98, market 2 (S&P +0.60 %, VIX 17.5).
- Driver: FOMC today, 21:00 Riyadh.
- Names: FRST, OPBK and WPC each have an 8-K 2.02 (earnings released).

What separated the two verdicts:
- **Dry run.** A Hormuz cluster graded 1 from 5 domains, including a licensed wire: *"oil closes higher as attacks continue in the Strait of Hormuz"*.
- **Write run.** That story was not in its fetch. Its Hormuz cluster was graded 3: aljazeera.net (C) and three unknown sites (F), *"tanker ablaze in Hormuz; Washington accuses Tehran of economic war"*.
- **Write run, recorded:** `_News_Gate` 1 row, `_News_Items` 152 rows, `_Econ_Events` 15 rows.
- **Write run, Names Flagged cell:** empty. v1.0.1 left INFO flags out of the cell.

A hand check (web, 28 Oct 02:20 Riyadh) confirms that attacks on shipping in Hormuz are continuing ([Al-Monitor](https://www.al-monitor.com/originals/2026/10/ship-attacks-strait-hormuz-hit-record-high-october), [AP via Telemundo](https://este.telemundonebraska.com/story/332815416/iran-and-us-persist-in-their-attacks-as-pakistan-tries-to-keep-diplomacy-alive-1)). So the dry run's call was the right one, and the write run's CAUTION was an under-call caused by feed variance.

## S2 - WHAT
| # | Change | Why | Test (golden negative on `2f8ead9f`) |
|---|---|---|---|
| 1 | **Memory.** Before clustering, `read_memory()` reads the newest `_News_Items` rows (`TFB_NEWS_GATE_MEMORY_ROWS`, default 2500; 0 = off). It carries every GDELT or Google-News headline published within 24 h (macro) or 72 h (a name), the same windows `evaluate()` uses, and re-classifies each one with the current taxonomy. A fresh copy wins on the same Item ID. Nothing carried is written again. EDGAR and the Fed feed are re-read whole, so their rows are not carried. The gate row's Items cell shows `+N carried`. Dry runs read the memory too (read-only). A failed read is logged and the run goes on. | A situation confirmed by an earlier run within the window stays confirmed when the next fetch misses one story. | `test_g7_memory_keeps_a_confirmed_situation_across_runs`: run 2, with GDELT throttled, reads CAUTION on v1.0.1 and NO_NEW_ENTRY on v1.0.2.<br>`test_g7_memory_respects_the_verdict_windows`: macro ages out after 24 h; a name story stays 72 h; EDGAR and Fed rows are not carried; 0 turns it off. |
| 2 | **English Google-News (US edition).** Four macro queries (geopolitics, oil, rates, markets), plus one per non-Saudi name, beside GDELT. GDELT queries are unchanged. | GDELT throttles GitHub runners, so the wires and US names were mostly missing. | `test_g7_english_wires_confirm_without_gdelt`: Reuters / AP / BBC with GDELT down give CAUTION on v1.0.1 and NO_NEW_ENTRY on v1.0.2. |
| 3 | **Names Flagged lists INFO too** (e.g. earnings released): BLOCK first, then WATCH, then INFO. The digest uses the same order. | FRST's earnings filing is the fact the 26 Oct entry missed; the row should show it. | `test_g7_end_to_end_write_then_dedupe_and_upsert` (two new assertions) |

### Review fix (Codex, PR #862, head `1744b8c7`)
| Finding | Fix | Golden negative |
|---|---|---|
| Memory imported GDELT and Google-News headlines even when `TFB_NEWS_GATE_SOURCES` (or the dispatch `sources` input) left them out. A market-only dry run after a normal write still read NO_NEW_ENTRY from the remembered Hormuz news. | `read_memory()` takes the run's source selection and carries only GDELT / Google-News rows that are selected. With neither selected, it does not read the tab at all. Same pass: a name's rows are carried only while the name is in today's scope, so yesterday's board seat cannot raise a flag today. | `test_g7_memory_carries_only_the_selected_sources`: 6 items carried on `1744b8c7`, 0 now. The market-only run reads CAUTION (static FOMC schedule only).<br>`test_g7_memory_drops_a_name_that_left_the_scope` |

Unchanged:
- verdict rules and thresholds;
- grades and tiers;
- taxonomy;
- headers;
- schedule;
- observe-only mode.

## S3 - Tests
- `tests/test_news_macro_gate.py`: 74 passed (69 before).
- 8 fail on `2f8ead9f`: the four changes above, the two review-pass cases, and two version-string checks.
- Both review-pass cases fail on `1744b8c7`.
- Manifest pins, the workflow audit (errors 0, warnings 0) and the related suites pass (122). All files are ASCII.

## S4 - Known limits / next
- **Growth.** About 600 `_News_Items` rows a day at the 200-row cap. Memory reads only the newest 2500 rows. A trim or a BigQuery archive of the news tabs belongs with Phase 3.
- **Duplicates.** The same article reached through GDELT and through Google-News has two Item IDs (original URL vs redirect URL). Grades count domains, so the verdict is unaffected.
- **Classification.** Still keyword-based. English headlines such as *"US strikes Iran"* without a listed war term stay unclassified; Phase 3 scoring will measure such misses before the taxonomy grows.

## S5 - Rollback
Revert `scripts/news_macro_gate.py` to `2f8ead9f`, or set `TFB_NEWS_GATE_MEMORY_ROWS=0` to switch off only the memory.

## S6 - Operator steps
1. Merge; ideally before the 09:41 Riyadh scheduled run.
2. Claude dispatches one dry run on the merge commit and checks:
   - the `memory:` progress line shows carried items from the 02:11 write;
   - the English queries answered;
   - the verdict.

Not a licensed financial adviser; the owner decides and executes every trade.
