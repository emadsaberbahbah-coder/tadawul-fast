"""Synthetic, attributable price observations for decision contract tests.

The HTTP acquisition boundary has separate tests. These fixtures represent
its successful output without calling a provider or disabling ticket guards.
"""
from datetime import datetime, timezone
import copy
import json

_PORTFOLIO_CAPTURES = {}


def observed_price_fields():
    instant = datetime.now(timezone.utc).isoformat()
    return {"data_provider": "yahoo", "last_updated_utc": instant,
            "acquisition_status": "success", "acquisition_provider": "yahoo",
            "acquisition_acquired_at": instant, "acquisition_quote_asof": instant}


def observed_portfolio(portfolio, fx_rates=None):
    """Complete synthetic custody/cash/valuation state for allocator tests.

    Reuse one immutable capture for an identical research/allocation input.
    Supplied evidence is never repaired, so adversarial packet tests remain
    adversarial. These fixtures do not represent a connected account.
    """
    if portfolio.get("reconciliation_evidence") is not None:
        return copy.deepcopy(portfolio)
    rates = {"SAR": 1, **(fx_rates or {})}
    key = json.dumps([portfolio, rates], sort_keys=True, default=str)
    if key in _PORTFOLIO_CAPTURES:
        return copy.deepcopy(_PORTFOLIO_CAPTURES[key])
    out = copy.deepcopy(portfolio)
    out["holdings_input_incomplete"] = False
    holdings = out.setdefault("holdings", [])
    remainder = out.get("portfolio_value_sar", 0) - sum(h.get("value_sar", 0) for h in holdings)
    if remainder > 0:
        holdings.append({"symbol": "SYNTHHELD.SR", "sector": "Unknown",
                         "currency": "SAR", "quantity": 1,
                         "value_sar": remainder})
    instant = datetime.now(timezone.utc).isoformat()
    positions, links = [], []
    for index, holding in enumerate(holdings):
        holding.setdefault("currency", "SAR")
        holding.setdefault("quantity", 1)
        holding.setdefault("current_price", holding["value_sar"] / holding["quantity"] / float(rates[holding["currency"]]))
        holding.update(observed_price_fields())
        positions.append({"instrument_id": "synthetic-" + str(index),
                          "symbol": holding["symbol"], "currency": holding["currency"],
                          "quantity": holding["quantity"]})
        links.append({"row_index": index, "account_id": "synthetic-account",
                      "instrument_id": "synthetic-" + str(index), "position_symbol": holding["symbol"],
                      "symbol": holding["symbol"], "currency": holding["currency"]})
    # Price fixture creation can occur just after the first instant. Capture
    # the complete packet after its component observations.
    instant = datetime.now(timezone.utc).isoformat()
    out["reconciliation_evidence"] = {
        "schema_version": 1, "source_ref": "synthetic://allocator-test",
        "captured_at": instant,
        "accounts": [{"account_id": "synthetic-account", "positions_asof": instant,
                      "positions_complete": True, "positions": positions,
                      "cash_asof": instant, "cash_complete": True,
                      "cash": [{"currency": "SAR", "settled_cash": out.get("cash_available_sar", 0),
                                "reserved_cash": 0, "reservations_complete": True}]}],
        "holding_links": links, "funding_accounts": ["synthetic-account"],
        "fx_rates": [{"currency": currency, "rate_to_sar": rate,
                      "asof": instant, "source_ref": "synthetic://allocator-FX"}
                     for currency, rate in rates.items() if currency != "SAR"],
    }
    _PORTFOLIO_CAPTURES[key] = copy.deepcopy(out)
    return out


def build_with_observed_inputs(builder, rows, criteria=None, portfolio=None,
                               fx_rates=None, upstream_meta=None):
    return builder.build_opportunity_payload(
        rows, criteria=criteria,
        portfolio=observed_portfolio(portfolio or {}, fx_rates),
        fx_rates=fx_rates, upstream_meta=upstream_meta)
