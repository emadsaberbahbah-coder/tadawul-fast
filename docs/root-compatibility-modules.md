# Root compatibility modules

The root `config.py`, `env.py` and `symbols_reader.py` remain available during
the WP-12 extraction. They are substantive compatibility implementations,
not transparent aliases to the similarly named core/integrations modules.
Replacing one with a re-export would change its API, defaults and import-time
behavior and requires a separate compatibility review.

Repository importer search on 11 October 2026 found active uses:

| Root module | Consumers | Migration prerequisite |
| --- | --- | --- |
| `env.py` | `scripts/track_performance.py`, `scripts/run_market_scan.py` | Compare the settings contract and fallback behavior before changing imports. |
| `symbols_reader.py` | `scripts/run_market_scan.py`; dashboard sync's optional dynamic reader | Compare page normalization, aliases and provider hints against both replacement implementations. |
| `config.py` | No direct production import found by static `rg` search | Check dynamic/external entry points and exported settings/auth contracts before removal. |

Preserve these modules and their versions. No deprecation date is declared
without an owner-approved migration plan. A complete importer audit must
include `importlib.import_module`, optional loader names, direct CLI imports
and deployed consumers outside this repository. The initial searches were
`rg 'from (config|env|symbols_reader)|import (config|env|symbols_reader)'`
and the dashboard sync header/optional-reader path inspection; absence of a
static importer alone does not establish that a compatibility API is unused.
