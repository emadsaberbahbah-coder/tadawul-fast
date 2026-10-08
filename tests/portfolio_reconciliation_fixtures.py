"""Declared broker evidence for synthetic portfolio policy fixtures only.

These fixtures describe an invented account, not captured broker data. Missing
or malformed holding fields and FX are retained so the real certification
guard rejects them. No policy gate or certification function is mocked.
"""
from __future__ import annotations

import datetime as dt
import re


def synthetic_quote_receipt():
    """Declare a current invented quote using the production token contract.

    EODHD is a supported provider label in this fictional receipt. The fixture
    performs no provider I/O; the real parser and freshness guard consume it.
    Its clock is independent from portfolio confirmation calendar test clocks.
    """
    stamp = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    return {"Data Provider": "EODHD", "Last Updated (UTC)": stamp,
            "Warnings": "; ".join(("acquisition_status:success", "acquisition_provider:EODHD",
                                    "acquisition_acquired_at:" + stamp,
                                    "acquisition_quote_asof:" + stamp))}


def _supplied(row, names):
    for key, value in row.items():
        if re.sub(r"[^a-z0-9]", "", str(key).lower()) in names:
            return value
    return None


def synthetic_reconciliation_evidence(rows, *, cash_available_sar, fx_rates):
    """Bind supplied synthetic rows to explicit positions and settled funds.

    A missing quantity/currency is never replaced, and no FX rate is guessed.
    Positions, cash and reservations all declare complete coverage for this
    invented account at the same fresh UTC timestamp.
    """
    stamp = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    account_id = "synthetic-policy-account"
    positions, links = [], []
    for index, row in enumerate(rows):
        symbol = _supplied(row, {"symbol", "ticker"})
        currency = _supplied(row, {"currency", "nativecurrency", "currencycode"})
        quantity = _supplied(row, {"quantity", "qty", "shares", "units", "holdingqty", "positionqty"})
        instrument_id = "synthetic-policy-instrument-%d" % index
        positions.append({"instrument_id": instrument_id, "symbol": symbol,
                          "currency": currency, "quantity": quantity})
        links.append({"row_index": index, "account_id": account_id,
                      "instrument_id": instrument_id, "symbol": symbol,
                      "currency": currency, "position_symbol": symbol})
    return {
        "schema_version": 1, "source_ref": "synthetic://portfolio-policy-capture",
        "captured_at": stamp,
        "accounts": [{
            "account_id": account_id, "positions_complete": True,
            "positions_asof": stamp, "positions": positions,
            "cash_complete": True, "cash_asof": stamp,
            "cash": [{"currency": "SAR", "settled_cash": cash_available_sar,
                      "reserved_cash": 0, "reservations_complete": True}],
        }],
        "holding_links": links, "funding_accounts": [account_id],
        "fx_rates": [{"currency": currency, "rate_to_sar": rate, "asof": stamp,
                      "source_ref": "synthetic://portfolio-policy-fx"}
                     for currency, rate in fx_rates.items() if currency != "SAR"],
    }


def build_certified_portfolio_actions(module, rows, controls=None, fx_rates=None,
                                      upstream_meta=None):
    """Run the public builder with complete evidence for synthetic test data."""
    rates = {} if fx_rates is None else fx_rates
    declared_cash = module.make_controls(controls)["cash_available_sar"]
    meta = dict(upstream_meta or {})
    meta["reconciliation_evidence"] = synthetic_reconciliation_evidence(
        rows, cash_available_sar=declared_cash, fx_rates=rates)
    return module.build_portfolio_actions(rows, controls, rates, meta)
