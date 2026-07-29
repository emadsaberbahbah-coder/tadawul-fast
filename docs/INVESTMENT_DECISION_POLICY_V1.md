# Investment Decision Policy v1

**Status:** Binding project decision contract  
**Effective:** 2026-07-29  
**Owner objective:** Maximize sustainable, risk-adjusted net wealth versus doing nothing. The objective is not merely to reduce realized losses.

## 1. Scope and non-retroactivity

1. This policy governs future recommendations after its implementation and validation.
2. Historical trades must not be attributed to the current Google Sheet model or current recommendation engine when those components were incomplete or were not used to make the trade.
3. Historical reviews must distinguish:
   - actual decision process used at the time;
   - information genuinely available at the time;
   - later model output;
   - hindsight-only information.
4. A historical simulation is valid only when it uses point-in-time inputs and the policy version that existed on that date.

## 2. Role of the Google Sheet

The Google Sheet is a source for positions, quantities, purchase cost, cash, account, settlement state, and documented transaction costs. It is not, by itself, a source of market price, liquidity, news truth, instrument facts, or a final investment decision.

For every recommendation, the engine must explicitly label each field as one of:

- `SHEET_RECORDED`
- `MARKET_VERIFIED`
- `PUBLIC_SOURCE_VERIFIED`
- `INFERRED`
- `UNKNOWN`

A blank value is `UNKNOWN`, never zero.

## 3. Decision objective

Every proposed action must be evaluated against at least three mutually exclusive alternatives:

1. **HOLD / DO NOTHING**
2. **SELL AND HOLD CASH**
3. **SELL AND REDEPLOY** into the best eligible alternative available in the same currency and account context

The chosen action must maximize expected net terminal wealth subject to risk, liquidity, sustainability, Shariah constraints where applicable, account constraints, execution feasibility, and the owner's trading rules.

A loss-reduction argument is insufficient. A sale is justified only when the expected net advantage of selling exceeds the expected net advantage of holding after all costs and uncertainty.

## 4. Sell, hold, and rotation are separate decisions

### 4.1 Sell decision

The sell decision evaluates the current position independently. It must answer:

- Is the thesis broken?
- Is the signal a verified exit signal or only weak noise?
- Is the position classification still correct?
- Is the time-stop breached?
- Is the risk concentration unacceptable?
- Is the instrument executable at the intended order size?
- What is the expected return distribution from holding through the review horizon?

### 4.2 Redeployment decision

The redeployment decision is separate. Sale proceeds do not automatically authorize a purchase.

The engine must identify the best eligible use of proceeds:

- cash in the existing currency;
- another instrument already owned;
- a new eligible instrument;
- debt/income instrument when appropriate;
- no action.

US sale proceeds remain in USD unless an explicit currency decision passes the full FX-cost test. No automatic round-trip currency conversion is permitted.

### 4.3 Rotation gate

A rotation from `CURRENT` to `ALTERNATIVE` is permitted only when:

`ExpectedNetAlpha = ExpectedNetReturn(ALTERNATIVE) - ExpectedNetReturn(CURRENT) - FullSwitchCost - UncertaintyBuffer`

is positive by a material margin.

The material margin must be calibrated and versioned. Until calibration is complete, the engine must not present a rotation as executable; it may present it as `RESEARCH_REQUIRED`.

A lower-scoring name may not receive capital from a higher-scoring name unless a documented risk, liquidity, diversification, or mandate constraint explains the exception.

## 5. Full cost model

Every sell, buy, reduce, add, or rotate recommendation must calculate or mark unknown:

- sell commission;
- buy commission;
- transaction tax/VAT where applicable;
- bid-ask spread estimate;
- expected slippage at the proposed size;
- FX conversion cost;
- settlement delay and frozen-cash effect;
- opportunity cost during settlement;
- dividend/coupon lost before the horizon;
- tax or withholding impact where applicable;
- minimum-ticket efficiency.

Known broker costs must use the documented owner schedule. Unknown broker or FX costs must block a precise recommendation rather than be estimated silently.

The transaction-efficiency rule remains:

`Total explicit transaction cost <= 0.5% of trade value`

For US trades, approximately USD 500 remains the practical minimum unless the actual cost calculation demonstrates compliance.

## 6. Instrument-first gate

Before any recommendation, identify and verify:

- instrument type;
- market and currency;
- primary income source;
- coupon/dividend and frequency;
- maturity, callability, convertibility, and seniority where relevant;
- last actual trade date;
- average traded volume and average transaction size when available;
- current bid/ask or best available executable proxy;
- next earnings, ex-dividend, coupon, maturity, and material corporate-action dates.

Unknown critical fields invalidate the recommendation.

Income securities are assessed primarily by yield, credit, cash-flow coverage, duration, call risk, and maturity value. Equity valuation models must not be applied to sukuk or bonds.

## 7. Position classification before action

Every position must have a current `POSITION_CLASS` before an action is considered:

- `INCOME`
- `INVESTMENT`
- `SPECULATION`

The action must use the rules of that class, including position limits, time horizon, stop discipline, and review cadence.

A position must not be retrospectively reclassified merely to justify a loss or avoid a stop.

## 8. Opportunity comparison

The engine must compare the current position with an opportunity set, not with one hand-picked replacement.

The opportunity set must be filtered for:

- investability;
- minimum data quality;
- minimum conviction score;
- account and currency compatibility;
- minimum trade size;
- earnings blackout;
- Federal Reserve day rule;
- liquidity and execution capacity;
- portfolio position limits;
- sector and factor concentration;
- sustainability and owner constraints.

At least the following must be reported:

- best eligible alternative;
- second-best eligible alternative;
- cash/no-action alternative;
- expected return range, not only point estimate;
- downside range;
- probability-weighted outcome;
- full switching cost;
- break-even excess return required to justify the switch.

## 9. Source hierarchy and evidence

### Tier A — primary and official

- company investor-relations releases;
- exchange announcements;
- SEC EDGAR and equivalent regulators;
- CMA, Tadawul, Saudi official disclosures;
- central banks and official statistical agencies;
- prospectuses, audited financial statements, and formal credit documents.

### Tier B — reputable institutional research

- bank and investment-house research that is legally and technically accessible;
- rating-agency reports;
- exchange-approved research platforms;
- institutional consensus datasets with clear timestamp and methodology.

### Tier C — reputable financial media and specialist sources

Used for context, event discovery, interviews, and market reaction. They may not override primary filings on factual matters.

### Tier D — investor activity and public commentary

- Form 4 insider transactions;
- 13F filings with the mandatory 45-day-delay warning;
- published shareholder letters;
- disclosed fund commentary;
- public interviews and verified social accounts.

This tier is contextual evidence, not automatic instruction.

### Tier E — unverified or weak sources

Anonymous accounts, reposts, unattributed screenshots, promotional newsletters, and unsourced claims are not decision evidence.

## 10. Source truth and conflict handling

For each material claim, store:

- source name;
- source tier;
- URL or document identifier;
- publication time;
- event time;
- retrieval time;
- direct fact versus interpretation;
- freshness gap;
- corroborating sources;
- contradictory sources;
- confidence.

When reliable sources conflict, show the conflict. Do not average incompatible facts.

A recommendation may not cite a source for a different asset, commodity, company, or time period.

## 11. Bank and investment-center recommendations

External recommendations are inputs, not decisions.

For each recommendation, capture:

- institution;
- analyst/team where available;
- recommendation date;
- rating vocabulary;
- target price and horizon;
- model assumptions;
- previous rating and target;
- price at publication;
- conflicts/disclosures;
- historical hit rate when available;
- whether the report is original, summarized, or second-hand.

The project must normalize external rating vocabularies but preserve the original rating. Analyst targets must carry the project warning that target-price realization is historically uncertain; they cannot be used as deterministic fair value.

## 12. Major-investor and insider evidence

Major-investor activity must be interpreted according to filing type and latency:

- Form 4 purchases can carry information after checking transaction type, size, ownership change, and whether the trade was discretionary.
- Insider sales are weak evidence, especially under 10b5-1 plans, option exercises, tax sales, and diversification.
- 13F is delayed and does not represent the current portfolio. It must never be described as a live position.
- Public investor commentary must be separated from disclosed holdings and from inferred exposure.

No recommendation may be based solely on copying a famous investor.

## 13. News intelligence: from headline to event model

News sentiment alone must not alter a recommendation. Every material event must pass the following pipeline:

1. **Entity match:** confirm the event concerns the exact issuer, instrument, sector, commodity, country, or portfolio exposure.
2. **Fact verification:** obtain primary confirmation or at least two independent high-quality sources when primary evidence is unavailable.
3. **Event timestamping:** distinguish when the event occurred from when it was reported.
4. **Novelty test:** determine whether the information is new, repeated, leaked earlier, or already priced.
5. **Transmission map:** identify how the event affects revenue, cost, volume, price, financing, FX, credit, regulation, supply chain, or risk premium.
6. **Duration estimate:** classify the effect as transient, cyclical, structural, or unknown.
7. **Market-reaction check:** assess whether the asset and related instruments already moved.
8. **Scenario tree:** model what can plausibly happen next.
9. **Reversal and recurrence check:** detect repeated escalation/de-escalation patterns.
10. **Action threshold:** permit an action only when the event changes the expected net distribution enough to beat no action after costs.

## 14. Event-state machine and recurrence

Geopolitical and policy events must be represented as states, not one-time positive/negative labels.

Minimum states:

- `RUMOR`
- `CONFIRMED_ESCALATION`
- `ACTIVE_CONFLICT`
- `CEASEFIRE_OR_PAUSE`
- `NEGOTIATION`
- `RENEWED_ESCALATION`
- `RESOLUTION`
- `STALE_OR_UNVERIFIED`

The engine must store prior transitions and estimate recurrence. For example, repeated escalation followed by temporary pauses must reduce the confidence of a simple “buy on outbreak” rule. The next-state probability matters more than the current headline label.

For each event, calculate or mark unknown:

- base-case next state;
- upside and downside next states;
- probability ranges;
- expected duration;
- asset sensitivity;
- lag between event and financial impact;
- indicators that would confirm or reject each path.

## 15. Second-order and anticipatory analysis

The project must answer not only “What happened?” but also:

- What is the most likely next response by governments, companies, consumers, and capital markets?
- Which actors have incentives to reverse, extend, or exploit the event?
- What part of the expected reaction is already priced?
- What secondary beneficiaries and losers exist?
- Which recommendation would be invalidated if the expected next step does not occur?

Forecasts must be expressed as scenarios and probabilities, not certainty.

## 16. News-to-trade safeguards

A news-driven trade is permitted only when:

- the position is classified as `SPECULATION` or the news materially changes an `INVESTMENT` thesis;
- the evidence gate passes;
- the event is not stale;
- the event is not merely repeated with no incremental information;
- the expected next-state scenario is documented;
- liquidity and execution are verified;
- the trade satisfies the class-specific size and stop rules;
- the recommendation states the predeclared error condition;
- the review date is set.

For speculative positions, the existing maximum weight, stop, and weekly profit-taking discipline remain controlling.

## 17. Required decision card

No recommendation is valid unless the output contains:

1. `POSITION_CLASS`
2. proposed action and signal strength
3. verified instrument facts
4. current thesis and thesis status
5. no-action outcome
6. sell-and-cash outcome
7. sell-and-redeploy outcome
8. best eligible use of proceeds
9. full cost breakdown
10. liquidity/execution assessment
11. source ledger and conflicts
12. event state and next-state scenarios when news is relevant
13. external research summary with provenance
14. major-investor/insider evidence with latency warning
15. expected return range and downside range
16. break-even excess return for rotation
17. predeclared error condition
18. mandatory review date
19. missing/unknown fields
20. final decision versus no action

A missing required field invalidates the recommendation.

## 18. Decision-quality measurement

Every completed decision must be scored twice:

### 18.1 Outcome score

Did the action outperform no action after all costs over the declared review horizon?

### 18.2 Process score

Did the decision comply with the policy and use valid point-in-time evidence?

A profitable rule-breaking trade remains a poor decision. A compliant decision can have a negative outcome without being retrospectively relabeled as irrational.

Performance attribution must separate:

- market movement;
- selection effect;
- timing effect;
- sizing effect;
- transaction cost;
- FX effect;
- income received or forgone;
- decision-policy compliance.

## 19. Audit trail and versioning

Store for every recommendation:

- policy version;
- model version;
- data snapshot ID;
- market-data timestamp;
- news snapshot ID;
- source bundle hash;
- opportunity-set snapshot;
- cost schedule version;
- scenario assumptions;
- generated decision card;
- operator action and execution details;
- subsequent review outcome.

No current model output may overwrite the historical snapshot used for an earlier decision.

## 20. Implementation mapping

Primary integration points:

- `core/news_intelligence.py`: source provenance, event verification, event state, novelty, recurrence, and scenario output. News remains context-only until the event linkage gate passes.
- `core/investment_advisor_engine.py`: three-alternative decision comparison and decision-card assembly.
- `core/analysis/criteria_model.py`: policy switches, minimum evidence, opportunity comparison, cost gates, and scenario requirements.
- `core/analysis/opportunity_builder.py`: eligible opportunity set and best-use-of-proceeds ranking.
- `core/analysis/insights_builder.py`: surface assumptions, source conflicts, no-action comparison, and invalidation conditions.
- `core/analysis/top10_selector.py`: candidate eligibility only; it must not be treated as a forecast or automatic funding list.
- `scripts/track_performance.py`: net-of-cost benchmark against no action and process-compliance scoring.

## 21. Minimum acceptance tests

The implementation is not complete until tests prove that:

1. A sale is rejected when the replacement does not cover switching costs and uncertainty.
2. A sale may still be accepted with no replacement when thesis break or risk control dominates.
3. A profitable small US trade is flagged when commission makes it economically negative.
4. A blank price, liquidity, coupon, or cost field is `UNKNOWN`, not zero.
5. Conflicting reliable sources are displayed, not averaged.
6. A repeated geopolitical escalation does not trigger the same trade without a changed next-state distribution.
7. A ceasefire/pause scenario is evaluated before a war-sensitive purchase.
8. A stale 13F filing cannot be described as a current holding.
9. Form 4 purchases and insider sales receive different evidentiary weight.
10. The decision card is invalid when any mandatory field is missing.
11. Historical trades are not attributed to a model version that did not exist at the time.
12. The final result reports whether the action beat no action after full costs.

## 22. Current limitation

This document establishes the binding requirements and project memory. It does not, by itself, activate automated trading or change recommendation outputs. Runtime activation requires code implementation, tests, historical point-in-time validation, and an explicit production flag.