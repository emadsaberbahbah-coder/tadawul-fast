#!/usr/bin/env python3
# core/corporate_offers.py
"""
================================================================================
TFB Corporate Offers - v1.0.0 (a cash offer standing on a listed share)
================================================================================
v1.0.0 (26 Nov 2026) - NEW. WHY (owner, 26 Nov 2026 01:07 and 01:15 Riyadh, in
Arabic): the stop on a holding under a cash tender was removed on purpose -
"the fear is that the price falls during the wave and a sell order is executed
while a purchase offer stands at a fixed price, so we lose for no reason ...
the system must have reason, wisdom and flexibility" - and "the sale here will
mostly be a manual decision, waiting for the best price before the final
offer". Read on the 00:42 run of that night: the Exit Plan trailed that
holding's stop to 1.8 % under the price (its ATR had shrunk as the price pinned
to the offer), its target stood above the offer price, and the Today Action
Plan asked to place the stop the owner had removed. Holding_Read (1.0.2) knows
an offer only while the news of the day carries it; the day after, the pages
forgot it.

WHAT. One registry, config/corporate_offers.json (schema
tfb.corporate-offers.v1). Merging a change to it is the confirmation, as with
config/corporate_actions_confirmed.json. Each offer:
    symbol     the listing, as the pages write it (DDI.US)
    kind       cash_tender | cash_merger
    price      the cash a share (or ADS) the offer pays; currency of the listing
    announced  the day it was made public (optional): the close before it is the
               price the share falls back to if the offer fails
    expires    the last day the offer is open (its own calendar day)
    sale       manual - the owner sells by hand: no stop order is asked for
               rule   - the Exit Plan's price rule stays in force
    bidder, conditions, source, notes   words for the page
The file lists public offers, never holdings: a page applies an entry only to a
share it finds in the book.

load() never raises: a missing file, bad JSON, another schema or a bad entry
leaves that entry out (an offer that cannot be read changes nothing).
standing() is the entry open on a day; lapsed() the entry whose last day has
passed (the page then says the registry needs its outcome); reading() the three
figures a page shows - the gap to the offer, the days left, and what the price
implies about completion against the close before the offer:
    implied % = (last - before) / (price - before), kept inside 0..100
A rough reading: it leaves out the time value of the wait, a raised bid and
anything that changed in the company since.

This module is pure apart from load(), decides nothing and writes nothing.
================================================================================
"""
from __future__ import annotations

import datetime as _dt
import json
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

__version__ = "1.0.0"

SCHEMA = "tfb.corporate-offers.v1"
KINDS = ("cash_tender", "cash_merger")
KIND_WORDS = {"cash_tender": "cash tender", "cash_merger": "cash merger"}
SALES = ("manual", "rule")
DEFAULT_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config",
                            "corporate_offers.json")


def _text(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _day(v: Any) -> Optional[_dt.date]:
    s = _text(v)[:10]
    try:
        return _dt.date.fromisoformat(s) if s else None
    except ValueError:
        return None


def _price(v: Any) -> Optional[float]:
    if isinstance(v, bool):
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if x == x and 0 < x < float("inf") else None


def parse(doc: Any) -> List[Dict[str, Any]]:
    """The valid offers of a registry document, in file order; [] for another schema. An entry without a symbol,
    a positive price, a known kind or an expiry day is left out; an `announced` after `expires` drops `announced`."""
    if not isinstance(doc, dict) or doc.get("schema") != SCHEMA or not isinstance(doc.get("offers"), list):
        return []
    out: List[Dict[str, Any]] = []
    for raw in doc["offers"]:
        if not isinstance(raw, dict):
            continue
        sym, kind = _text(raw.get("symbol")).upper(), _text(raw.get("kind")).lower()
        price, expires = _price(raw.get("price")), _day(raw.get("expires"))
        if not sym or " " in sym or kind not in KINDS or price is None or expires is None:
            continue
        announced = _day(raw.get("announced"))
        if announced is not None and announced > expires:
            announced = None
        sale = _text(raw.get("sale")).lower()
        out.append({"symbol": sym, "kind": kind, "price": price, "expires": expires, "announced": announced,
                    "sale": sale if sale in SALES else "rule", "bidder": _text(raw.get("bidder")),
                    "conditions": _text(raw.get("conditions")), "expires_note": _text(raw.get("expires_note")),
                    "source": _text(raw.get("source")), "notes": _text(raw.get("notes"))})
    return out


def load(path: Optional[str] = None) -> List[Dict[str, Any]]:
    """parse() of the registry file (TFB_CORPORATE_OFFERS_FILE, else config/corporate_offers.json); [] on any fault."""
    p = path or os.environ.get("TFB_CORPORATE_OFFERS_FILE") or DEFAULT_PATH
    try:
        with open(p, "r", encoding="utf-8") as fh:
            return parse(json.load(fh))
    except Exception:  # noqa: BLE001 - an unreadable registry is an empty one
        return []


def _of(offers: Sequence[Dict[str, Any]], symbol: Any) -> List[Dict[str, Any]]:
    sym = _text(symbol).upper()
    return [o for o in offers or () if o.get("symbol") == sym]


def standing(offers: Sequence[Dict[str, Any]], symbol: Any, today: _dt.date) -> Optional[Dict[str, Any]]:
    """The offer open on `today` for the symbol (announced, when given, on or before it; its last day not passed);
    the one that expires last when the registry carries two."""
    live = [o for o in _of(offers, symbol)
            if o["expires"] >= today and (o.get("announced") is None or o["announced"] <= today)]
    return max(live, key=lambda o: o["expires"]) if live else None


def lapsed(offers: Sequence[Dict[str, Any]], symbol: Any, today: _dt.date) -> Optional[Dict[str, Any]]:
    """The symbol's newest offer whose last day has passed, when none stands (its outcome is not in the registry)."""
    if standing(offers, symbol, today) is not None:
        return None
    gone = [o for o in _of(offers, symbol) if o["expires"] < today]
    return max(gone, key=lambda o: o["expires"]) if gone else None


def close_before(offer: Dict[str, Any], closes: Sequence[Tuple[_dt.date, float]]) -> Optional[float]:
    """The last close dated before the day the offer was announced; None without that day or such a close."""
    day = offer.get("announced")
    if day is None:
        return None
    prior = [c for d, c in closes if d < day and c and c > 0]
    return prior[-1] if prior else None


def reading(offer: Dict[str, Any], last: Optional[float], today: _dt.date,
            before: Optional[float] = None) -> Dict[str, Any]:
    """{"days_left", "gap_pct", "implied_pct", "before"}: days to the last day (0 on it), how far the last price is
    under the offer in % of the last price (negative when above it), and the completion the price implies against
    the close before the offer (None when that close is unknown or not under the offer price)."""
    price = offer["price"]
    out: Dict[str, Any] = {"days_left": (offer["expires"] - today).days, "gap_pct": None, "implied_pct": None,
                           "before": before}
    if last and last > 0:
        out["gap_pct"] = (price / last - 1.0) * 100.0
        if before and 0 < before < price:
            out["implied_pct"] = max(0.0, min(100.0, (last - before) / (price - before) * 100.0))
    return out


def label(offer: Dict[str, Any]) -> str:
    """'cash tender by <bidder>, conditional' - the words a page puts after the price."""
    words = KIND_WORDS.get(offer.get("kind"), "cash offer")
    if offer.get("bidder"):
        words += " by %s" % offer["bidder"]
    if offer.get("conditions"):
        words += ", conditional"
    return words
