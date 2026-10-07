"""Bounded instrument routing for the provider symbols currently supported.

Yahoo-native futures/indices and Yahoo Milan/NZX suffixes have no verified
EODHD translation in this release. Do not guess another exchange or strip a
suffix. Explicit provider configuration remains authoritative: an omitted
Yahoo provider is not enabled by this resolver.
"""
from __future__ import annotations

from collections.abc import Sequence

_YAHOO_NAMES = frozenset({"yahoo_chart", "yahoo", "yfinance"})


def yahoo_primary_reason(symbol: str) -> str:
    value = str(symbol or "").strip().upper()
    if value.endswith("=F"):
        return "yahoo_native_future"
    if value.startswith("^"):
        return "yahoo_native_index"
    if value.endswith((".MI", ".NZ")):
        return "unverified_eodhd_exchange_mapping"
    return ""


def provider_supports_instrument(provider: str, symbol: str) -> bool:
    """Only the known Yahoo path serves instruments with no verified mapping."""
    return not yahoo_primary_reason(symbol) or provider.lower() in _YAHOO_NAMES


def providers_for_instrument(symbol: str, providers: Sequence[str]) -> list[str]:
    """Keep ordinary provider order; special routes use configured Yahoo only."""
    return [provider for provider in providers
            if provider_supports_instrument(provider, symbol)]
