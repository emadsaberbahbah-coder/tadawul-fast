# TFB Commit Sheet — news_macro_gate 1.12.0 [AUDIT-GUARDS] (five findings of the 26 Nov engineering audit)

Date: 2026-11-27 (Friday) · Lane: GitHub Actions (📰 News & Macro Gate) · Gate: observe-only — it controls no buy · No tab or header changes · No Render deploy

## Why
The owner uploaded an outside engineering audit of the project on 26 Nov (23:55 Riyadh). Each finding below was reproduced on main (65c7864) before it was fixed. Standing word, 26 Nov 19:13 (in Arabic): *"if it is ready, build it now, so we lose no time"*.

## What
`scripts/news_macro_gate.py` 1.11.0 → **1.12.0**; `.github/workflows/news_macro_gate.yml` 1.1.6 → **1.1.7**.

| Audit | Reproduced on main | Fix |
|---|---|---|
| **A1** an undated item counts as news of this hour | three undated wires about Hormuz gave NO_NEW_ENTRY | `in_window()`: a member counts only with a date, no older than the window, at most one hour ahead of the run. Used for the 24-hour macro window and the 72-hour name window. |
| **A1** a future-stamped item counts | an item dated five hours ahead counted | same rule |
| **A3** three rumours make grade 1 (confirmed) | `information_grade` gave 1 to three rumour headlines from three sites | a cluster made only of rumours is grade 4 at best, unless one copy is tier A (an official source) |
| **A4** a copied headline votes several times on direction | two copies outvoted the wire they copied | direction, falling and calm counts run over origins, the same rule 1.10.0 applied to the source count |
| **A6** a stale quote is used, an empty input reads as calm | a VIX of 40 from five days ago raised NO_NEW_ENTRY; no quotes and no news gave a plain GO | a quote older than 96 hours (a long weekend fits) is left out and named; the digest has an **Inputs** line, and says when GO rests on missing inputs rather than on a calm market |
| **E1** the run log can name a holding | `TFB_NEWS_GATE_LOG_DETAIL=on` printed names on Actions; the extra symbols came from a repository Variable, which the log prints | on GitHub Actions the log is always public, whatever the setting; extra symbols come from a repository **secret** (masked); the manual input says it is public |

A quote that carries no time is used as before: the guard needs the quote's own time.

## Checked against the archive
- 0 undated and 0 future-stamped items in the recorded news: A1 changes no recorded verdict.
- 44 rumour items (8 of high severity), none in a cluster made only of rumours that raised a verdict: A3 changes no recorded verdict.
- A4 and A6 were not replayed (cluster directions and quote times are not archived). The first runs on main show their effect in the digest.
- The last scheduled run's log shows `TFB_NEWS_GATE_SYMBOLS` empty: the Variable is not in use, so moving it to a secret changes nothing today.

## Tests
- `tests/test_news_macro_gate.py` + `tests/test_news_llm_reader.py`: 353 pass, locally and with `GITHUB_ACTIONS=true`.
- New G21: undated and future members; the name window; three rumours vs an official source; a copy voting once; a stale quote left out and named; the Inputs line and the missing-input warning; the workflow reads a secret.
- Changed with a reason: `test_g14` (an undated member no longer raises), `test_g17` (on Actions the log is public even with `on`), the `RAISE_RULES_SHA` / `EVALUATE_SHA` pins (same thresholds).
- The `armed` fixture clears `GITHUB_ACTIONS`, so CI sees the same test log as a laptop.
- Ten hand mutants, one per guard: each fails a test.

## Owner
Nothing to set. To add extra symbols later, use a repository secret named `TFB_NEWS_GATE_SYMBOLS`, not a Variable.

## Not done here (needs the owner's decision)
- **C1** exit_plan tightens a stop on grade-3 negative name news.
- **A2** news on an issuer with no name in scope is kept.
- **B1** the baseline vs new escalation rule.
- **E4** pinning third-party actions to a commit.

## Rollback
Revert the script to 1.11.0 and the workflow to 1.1.6.

Not a licensed financial adviser; the owner decides and executes every trade.
