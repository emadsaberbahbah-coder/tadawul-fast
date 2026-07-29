# Project-Wide Execution and Continuous-Learning Policy v1

**Status:** Binding specification; runtime activation requires tests and explicit approval  
**Effective date:** 2026-07-29  
**Scope:** Every recommendation, ranking, portfolio action, report, API route, scheduled job, spreadsheet export, and historical review in the project.

## 1. Project objective

The project must maximize sustainable, risk-adjusted **net terminal wealth versus no action**. It must not optimize merely for a higher win rate, lower realized losses, or more frequent trading.

Every action must compare:

1. `HOLD_DO_NOTHING`
2. `SELL_HOLD_CASH`
3. `SELL_REDEPLOY_BEST_ELIGIBLE`

Selling and buying are separate decisions. Proposed sale proceeds are not buying power. A rotation is executable only after settlement and only when the alternative's expected net advantage exceeds the current holding after all switching costs and an uncertainty buffer.

## 2. Project-wide default: avoid speculation

`SPECULATION` is excluded by default from:

- candidate universes;
- Top 10 lists;
- BUY/ACCUMULATE recommendations;
- portfolio redeployment alternatives;
- automated alerts that could be interpreted as executable.

A candidate is not made investable by a high recent return, social attention, a single headline, a technical pattern, or an analyst target.

### 2.1 Speculation gate

The gate must use market-specific thresholds and multiple independent facts. Critical unknowns are blocking, not passing.

Hard-block examples include:

- OTC/Pink or equivalent unregulated/weakly regulated venue;
- market capitalization below the configured market floor;
- median daily traded value below the configured liquidity floor;
- abnormal spread or insufficient actual trading days;
- recent reverse split;
- extreme one-day or five-day price jump without verified fundamental transmission;
- unresolved symbol/instrument identity;
- quote, volume, or corporate-action inconsistency;
- recommendation driven mainly by promotion, rumor, or unverified social content.

Price alone is not sufficient to call a stock speculative. A low price is a warning that must be combined with capitalization, traded value, spread, history, corporate actions, and business quality.

Manual inclusion of a speculative idea requires a documented exception and must remain `RESEARCH_ONLY`; it is not executable under the default owner mandate.

## 3. Best buy price is a range, not false precision

The project must output:

- `BUY_ZONE_LOW`
- `BUY_ZONE_HIGH`
- `MAX_ACCEPTABLE_PRICE`
- `PRICE_AS_OF`
- `PRICE_SOURCE`
- `ORDER_TYPE`
- `ORDER_VALIDITY`
- `FULL_BUY_COST`
- `ENTRY_INVALIDATION`
- `NEXT_REVIEW_DATE`

The buy zone must reconcile four independent dimensions:

1. fundamental value with a margin of safety;
2. market structure/support based on verified price history;
3. volatility and gap risk;
4. executable liquidity, spread, order participation, and transaction cost.

A single exact “best price” is prohibited unless it is an actual executable limit price with a timestamp and a stated validity window.

Do not chase a price above `MAX_ACCEPTABLE_PRICE`. If the price never enters the zone, `NO_TRADE` is a valid and preferred outcome.

Staged entry is allowed only when every tranche independently satisfies minimum economic transaction size and cost limits.

## 4. Best sell price and winner protection

The project must output:

- `SELL_ZONE_LOW`
- `SELL_ZONE_HIGH`
- `MIN_ACCEPTABLE_EXIT_PRICE`
- `THESIS_EXIT`
- `RISK_EXIT`
- `CONCENTRATION_TRIM_ZONE`
- `EXPECTED_HOLD_RETURN_RANGE`
- `EXPECTED_REDEPLOY_RETURN_RANGE`
- `FULL_SWITCH_COST`
- `NET_ROTATION_ALPHA`

A profitable position must not be sold merely because it is profitable. The engine must explicitly test whether selling a winner early creates more regret than holding it.

A sell or trim is justified only by one or more of:

- thesis deterioration;
- verified valuation excess;
- materially better alternative after costs;
- concentration risk;
- time-stop without a valid extension;
- credit, liquidity, governance, or sustainability deterioration;
- portfolio constraint that cannot be solved more cheaply.

The target is a zone, not a promise. Limit orders must reflect actual liquidity and spread. Illiquid positions must not receive market-order language.

## 5. Stop-loss policy by instrument and position class

### 5.1 Investment equities

- The maximum planned loss from entry is 20%.
- The trade is invalid if a defensible thesis/volatility stop requires more than the maximum loss.
- Stops may tighten but may never be widened after entry to avoid realizing a loss.
- A price alert is not automatically a market stop. Gap and liquidity risk must be stated.
- The recommendation must include both a price-based risk level and a thesis invalidation condition.
- A 90-day negative position requires either `EXIT` or a written `EXTEND` decision with reason and review date.

### 5.2 Income instruments and sukuk

Equity-style price stops are prohibited. Review must be based on coupon/yield, maturity, callability, credit quality, covenant or payment deterioration, liquidity, and expected cash flows. A price decline alone is not an automatic exit.

### 5.3 REITs

Use AFFO, dividend coverage, balance-sheet/refinancing risk, asset quality, and relevant property metrics. GAAP payout alone is not an exit model.

### 5.4 Speculation

The owner mandate blocks new speculative positions. If a legacy position is explicitly classified `SPECULATION`, the maximum size is 5%, maximum planned loss is 8%, and the plan requires a short review horizon. It must never be reclassified as `INVESTMENT` merely to avoid the stop.

## 6. EODHD and external-data use

The project should use provider data by purpose rather than treating one field as truth:

- Screener: initial investability, capitalization, liquidity, and universe filters.
- Fundamentals: business quality, financial statements, valuation, dividends, holders, insiders, and instrument identity.
- End-of-day history: point-in-time price history, adjusted returns, gaps, drawdowns, and counterfactual outcomes.
- Intraday history: execution-zone, spread proxy, volume profile, and order-timing study; never as a substitute for live executable quotes.
- News: claims and event context subject to provenance and scenario analysis.
- Economic events and macro indicators: regime/context variables, not direct BUY/SELL triggers.
- Splits and corporate actions: return normalization and speculation/data-integrity gates.

Provider capability discovery is required at runtime. Missing API entitlement or stale/partial data must be surfaced as `UNKNOWN_BLOCK`, not silently replaced.

## 7. Continuous learning from all recommendations

Every recommendation must be stored as an immutable point-in-time record containing:

- recommendation ID and policy/model version;
- market snapshot ID and source timestamps;
- instrument and position class;
- action alternatives;
- buy/sell/stop ranges;
- expected return and downside ranges;
- full cost assumptions;
- confidence and uncertainty;
- news/event state and scenarios;
- final operator decision and actual execution;
- explicit reasons and invalidation conditions.

### 7.1 Outcome horizons

Measure at 1, 5, 20, 60, 90, 180, and 365 trading-day horizons where data exists.

### 7.2 Required counterfactuals

For every recommendation, calculate net-of-cost outcomes for:

- recommended action;
- no action;
- cash;
- best eligible alternative known at the time;
- actual executed action.

### 7.3 Required metrics

- net decision alpha versus no action;
- net rotation alpha;
- realized and unrealized P/L;
- maximum favorable excursion;
- maximum adverse excursion;
- execution slippage;
- forecast calibration and interval coverage;
- premature-winner-exit regret;
- long-loser-retention regret;
- source and model error attribution;
- process quality separate from outcome quality.

A profitable outcome can still be a poor process, and a losing outcome can still be a sound process. Both labels must be retained.

### 7.4 Learning safety

- No look-ahead data.
- No survivorship bias.
- No retroactive attribution to a model that did not exist.
- Learn separately by market, instrument class, horizon, signal strength, and event regime.
- Do not update production thresholds from a small sample.
- Minimum cohort size is configurable and defaults to 30.
- Proposed parameter changes run in shadow mode.
- Production changes require human approval, before/after validation, versioning, and rollback.
- The learner must not optimize for trading frequency or raw hit rate.

## 8. Mandatory recommendation card

No action is executable unless the card contains:

- classification and signal strength;
- verified instrument facts;
- buy zone, sell zone, and stop/invalidation logic where applicable;
- no-action result;
- sell-and-cash result;
- sell-and-redeploy result;
- best use of proceeds;
- all transaction, spread, slippage, FX, tax/VAT, and settlement costs;
- liquidity and order feasibility;
- news/source provenance and contradiction log;
- expected return/downside ranges;
- error condition;
- mandatory review date;
- unknown fields;
- policy and model version.

Missing critical fields produce:

`INCOMPLETE — NOT EXECUTABLE`

## 9. Required integration points

The central policy must be loaded by:

- `core/analysis/opportunity_builder.py`
- `core/analysis/portfolio_actions.py`
- `core/investment_advisor_engine.py`
- `core/news_intelligence.py`
- `routes/advanced_analysis.py`
- `scripts/track_performance.py`
- every spreadsheet/export writer that emits recommendations

Runtime activation must be one explicit project-wide flag. Module-level bypasses are prohibited in production.

## 10. Acceptance tests

At minimum, tests must prove:

1. a speculative candidate is excluded even when recent return is high;
2. missing critical liquidity data blocks execution;
3. an exact price without timestamp/validity is rejected;
4. sell proceeds cannot fund a new recommendation before settlement;
5. a rotation with negative net alpha is rejected;
6. an investment stop cannot be widened after entry;
7. income instruments do not receive equity-style stops;
8. recommendation outcomes are measured against no action after costs;
9. winner-exit and loser-retention regrets are both measured;
10. learning changes remain shadow-only until approved.
