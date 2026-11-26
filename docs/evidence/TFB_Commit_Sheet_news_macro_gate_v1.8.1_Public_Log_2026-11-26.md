# TFB Commit Sheet — news_macro_gate 1.8.1 [PUBLIC-LOG] (the public run log no longer names the holdings)

Date: 2026-11-26 (Thursday) · Lane: GitHub Actions (📰 News & Macro Gate) · Gate: observe-only, verdict unchanged · No tab, header or row changes · No Render deploy

## Why
The owner's standing rule: *"Repo logs are public: no amounts, quantities, account numbers or holdings' symbols in run logs."* This repository is public, so its Actions logs, step summaries and artifacts are public too. The news gate broke the rule in four places:

- **Since v1.0.0:** it printed its scope, which lists the holdings first (`scope 15: <the holdings, then the watch names>`).
- **The digest** (log, step summary and the `news-gate-evidence` artifact) listed the flagged names and every cluster about a name, with its headline.
- **The failure notes** added in v1.5.0 name the symbol asked (`<symbol>: HTTP 401`).
- **The evidence .json** carried the whole scope and the issuer-link examples, which are name headlines.

Found during the 26 Nov reviews of the Finnhub, Marketaux and Gemini dry runs; promised as a fix.

## What
`scripts/news_macro_gate.py` 1.8.0 → **1.8.1**, and the workflow 1.1.5 → **1.1.6**, which passes the Variable.

- **`log_detail()`** reads `TFB_NEWS_GATE_LOG_DETAIL` = `on` | `off` | `auto` (default `auto`: off on GitHub Actions, on elsewhere). This is the rule `exit_plan` 1.6.1 already uses.
- **When the detail is off:**
  - The scope line gives the count only: `scope 15: (names kept out of the public log)`.
  - The digest keeps the verdict, the drivers, the market-wide clusters, the item counts, the source table and the LLM line. The flagged names and the name clusters appear as counts, with a pointer to the tab that holds them.
  - The evidence .json is the same public cut: the scope and the flags as counts, the MACRO clusters only, and the issuer-link counts without examples.
  - Every line printed passes `mask_public()`. It replaces each scope symbol, its letter ticker and its company name with `[name]`, whole words only, so `name <symbol> finnhub: HTTP 429` reads `name [name] finnhub: HTTP 429`. The reason stays.
- **The workbook keeps every detail.** `_News_Gate`, `_News_Items` and `_Econ_Events` receive exactly the rows they did before; the test compares the whole workbook row for row.
- **The mask belongs to one run.** `main()` clears it on the way out, even on an error.

## Tests
- `tests/test_news_macro_gate.py`: 305 pass (297 earlier, one version line updated, plus 8 new). `tests/test_news_llm_reader.py`: 22 pass.
- **Codex P2, fixed:** the first version masked the serialised JSON, so a number equal to a symbol could become `[name]` and break the file. Two changes fix it:
  - The evidence .json now masks string values only; numbers stay numbers.
  - A numeric Saudi code alone is not a token. Its full symbol (`<code>.SR`) is, so counts and ages in the digest stay readable.
  
  A new test runs a scope with a `.SR` symbol and a bare code beside numbers equal to both. The old masking fails it.
- Both files also pass with `GITHUB_ACTIONS=true`. The test fixture pins the detail on, so CI and local runs read the same log.
- The new G17 tests:
  - the switch in five forms;
  - the masking rules: whole words only, so `MAX` and `TAXES` stay and `50230` stays;
  - one full `--write` run with the detail on and one on Actions. In the second, the log, the step summary and both evidence files contain none of ten scope secrets (symbols, tickers, company names) and no name-cluster headline. The verdict is the same, the workbook is identical, and the mask is empty afterwards.
- Six hand mutants each fail a test: no mask in `_out`, examples kept, name clusters shown, the mask left set, tickers unmasked, name clusters in the .json.

## Owner
Nothing to do. To see the names in a log again for one investigation, set the repository Variable `TFB_NEWS_GATE_LOG_DETAIL` = `on`, then set it back.

## Rollback
Variable `TFB_NEWS_GATE_LOG_DETAIL` = `on`, or revert the script to 1.8.0.

Not a licensed financial adviser; the owner decides and executes every trade.
