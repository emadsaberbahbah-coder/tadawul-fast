# TFB Commit Sheet — news_llm_reader 1.0.0 · news_macro_gate 1.8.0 [LLM-SHADOW] (a language model reads the same headlines beside the word lists)

Date: 2026-11-26 (Thursday) · Lane: GitHub Actions (📰 News & Macro Gate) · Gate: observe-only, unchanged — the reader controls nothing · No Render deploy · One new tab (`_News_LLM`); no existing tab or header changes

**Built on top of PR 1092 (Marketaux, 1.7.0): merge that one first.**

## Why
Owner, 26 Nov 15:28 and 15:34 Riyadh (in Arabic): can another AI he subscribes to (ChatGPT, Gemini) help the news reading or another part of the system — "let us start with this too", linked with BigQuery.

The gate reads headlines with word lists. Since 19 Nov it classified **56 %** of what it collected, and it cannot read a sentence: *"Iran attacks US bases in Gulf countries despite diplomatic efforts"* carries none of its fighting words. A language model can read the sentence. Whether its reading is **better** is not known — so it starts as a shadow, both readings recorded side by side, to be scored on what followed.

A chat subscription gives no API key; the API's free plan does. `gemini-3.5-flash-lite` is free of charge on that plan (Google's pricing page, checked 26 Nov; it publishes no request numbers — third-party readings of October give 10–15 requests a minute and 500 a day).

## What
`scripts/news_llm_reader.py` 1.0.0 (new) and a hook in `scripts/news_macro_gate.py` 1.7.0 → **1.8.0**.

After the verdict is computed **and the gate's own rows are written**, the run's fresh headlines that were not read before go to the model — names first, then market-wide, newest first, at most 120 a run in batches of 60 (two requests, 7 s apart, each asked once). One row per headline is appended to `_News_LLM`:

| Columns | |
|---|---|
| Item ID, Read At, Published, Scope, Headline | the headline |
| Rule Event, Rule Direction, Rule Severity | the word lists' reading (blank when they read nothing) |
| LLM Event, LLM Direction, LLM Severity, LLM Confidence | the model's reading |
| Agree | `same` · `direction differs` · `event differs` · `rule only` · `model only` · `neither` |
| Model, Run ID | |

The headlines the word lists leave unread are recorded here for the first time.

## What keeps it harmless
- **It controls nothing.** It runs after `evaluate()` and after the gate's rows; its labels go to their own tab, which nothing reads.
- **No failure of it can touch the gate.** Six faults are tested (refused, garbage reply, unreachable, no HTTP post, the tab write failing, a reply carrying instructions): in each the verdict, the exit code and the three gate tabs are identical, row for row, to a run without the reader.
- **Only the headline and its kind (MACRO / COMPANY) are sent.** No symbol, holding, quantity or price. On the free plan Google may use prompts to improve its products; the prompts hold public headlines only.
- **Headlines are untrusted text.** A reply is used only as labels checked against fixed lists — an event type of the headline's own kind or `none`, direction −1/0/1, severity H/M/L, confidence 0–100. Anything else is dropped.
- **The key** travels in the `x-goog-api-key` header and is scrubbed from every message.
- **Budget:** about 18–27 requests a day; 100 s at most per run; two failures stop it for the run.
- A dry run asks but records nothing. `TFB_NEWS_LLM=off` switches it off; without `GEMINI_API_KEY` nothing is asked and the digest says so.

## Not in this change — the BigQuery step
The archive copy of `_News_LLM` and the view that scores the two readings against what the market and each name did afterwards come **after the tab holds rows** (the archive creates a table from an existing tab; the archive's tab list is pinned to the view manifest). That is the next build.

## Tests
- `tests/test_news_llm_reader.py` (new): 22 pass — what is sent; what is kept; which headlines; the six comparisons; batches, spacing, no retry, a refusal named with the key scrubbed; the hook; the six faults; off / dry run / no key; pins.
- `tests/test_news_macro_gate.py`: 297 pass unchanged apart from the version pin.
- Ten mutants (an unknown event kept, a symbol sent, key not scrubbed, read-before ignored, filings sent, no stop after two failures, a dry run recording, a fault escaping, the switch ignored, direction unchecked) each fail a test.

## Owner
1. Merge PR 1092 first, then this one.
2. Create a free API key at Google AI Studio (aistudio.google.com → Get API key) and add it as the repository **secret** `GEMINI_API_KEY`. Until then the reader asks nothing and says so.

## Rollback
Repository Variable `TFB_NEWS_LLM` = `off`, or revert the gate script to 1.7.0.

Not a licensed financial adviser; the owner decides and executes every trade.
