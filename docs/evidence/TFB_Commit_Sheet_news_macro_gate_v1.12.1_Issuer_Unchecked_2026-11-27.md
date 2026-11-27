# TFB Commit Sheet — news_macro_gate 1.12.1 [ISSUER-UNCHECKED] (audit A2: a name item whose issuer could not be checked is counted)

Date: 2026-11-27 (Friday) · Lane: GitHub Actions (📰 News & Macro Gate) · Gate: observe-only · No tab, column, verdict or count-rule changes · No Render deploy

## Why
Finding A2 of the outside engineering audit the owner uploaded on 26 Nov. For a symbol of the scope with no company name, `issuer_linked()` returns None (cannot judge) unless the ticker appears in the headline, and `apply_issuer_link()` kept the item without a word. It then reached the clusters, the `_News_Items` tab and the Exit Plan's news count as if it had been checked.

## What
`scripts/news_macro_gate.py` 1.12.0 → **1.12.1**.
- `apply_issuer_link()` counts those items (`unchecked`) and the symbols behind them that have no name (`unnamed`). `unnamed` is a count only, so the public log names no symbol.
- Each such item is marked `issuer_checked = False` in memory.
- The run log and the digest say *issuer not checked N (no company name for M symbol(s), kept)*.
- Nothing is dropped. The count shows how much of the name news rests on items nobody could tie to the company, and how many names the scope is missing.

## Tests
- `tests/test_news_macro_gate.py` + `tests/test_news_llm_reader.py`: 355 pass, also with `GITHUB_ACTIONS=true`.
- New G22:
  - counts and marks: a ticker in the headline still ties an item; mode off counts nothing;
  - the digest line;
  - the public report keeps the counts and no examples.
- Changed with a reason: the G8 dict literals gain the two counts. One of their fixtures has a symbol with no name.
- Four hand mutants each fail a test.

## Owner
Nothing to set. After merge, the first run says how many symbols lack a name. Filling them (from the pages or SEC) is the follow-up.

## Rollback
Revert the script to 1.12.0.

Not a licensed financial adviser; the owner decides and executes every trade.
