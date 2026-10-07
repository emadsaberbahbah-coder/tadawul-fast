"""Pure successful-price acquisition evidence shared by refresh and audits.

Last Updated is the established retrieval/engine stamp, never quote as-of.
Legacy rows can prove bounded acquisition from that stamp and valid price
provenance; actual quote age stays separately UNKNOWN when not supplied.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import math
import re
from typing import Any, Callable, Iterable, Mapping, Sequence
from core.provider_capabilities import provider_supports_instrument

MAX_CLOCK_SKEW_SECONDS = 900  # Match the decision audit's existing 15 min allowance.
# Fundamentals-only margin quarantine is retained for its own controls; it
# does not invalidate a successfully acquired price or its provider evidence.
_INVALID_WARNINGS = re.compile(
    r"fetch_failed|empty_row_no_provider_data|identity_quarantined|"
    r"kept_last_good|no_data_stub|placeholder_stub|price_unverified_live|"
    r"price_bar_stale|operator_quarantine|pl1_quarantined|"
    r"persist_sanity_quarantined|xprovider_price_conflict", re.I,
)
_NONLIVE_PROVIDER = re.compile(
    r"fallback_error|placeholder|unavailable|history|snapshot|cache|last_good", re.I,
)
_TOKEN_KEYS = frozenset({
    "acquisition_status", "acquisition_acquired_at", "acquisition_quote_asof",
    "acquisition_provider",
})
_SYMBOL_DOMAIN_RE = re.compile(r"^[A-Z0-9^][A-Z0-9.\-=^&/]{0,23}$")


def symbol_domain_ok(value: Any) -> bool:
    return bool(_SYMBOL_DOMAIN_RE.fullmatch(_text(value).upper()))


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", _text(value).lower())


def _warning_text(value: Any) -> str:
    if isinstance(value,(list,tuple)):
        return "; ".join(_text(item) for item in value)
    return _text(value)


def acquisition_tokens(warnings: Any) -> dict[str, str]:
    """Read semicolon-delimited typed tokens, preserving ISO colons/offsets.

    Conflicting repeated tokens invalidate the proof instead of selecting
    the last token. Direct internal fields are handled by row_acquisition.
    """
    out: dict[str, str] = {}
    for token in _warning_text(warnings).split(";"):
        name, sep, value = token.strip().partition(":")
        name = name.lower()
        if sep and name in _TOKEN_KEYS:
            if name in out and out[name] != value.strip():
                out["acquisition_status"] = "conflict"
                return out
            out[name] = value.strip()
    return out


def precise_utc(value: Any) -> datetime | None:
    """Acquisition evidence must carry an instant and an explicit timezone."""
    try:
        if isinstance(value, datetime):
            stamp = value
        else:
            raw = _text(value)
            if ":" not in raw:
                return None
            stamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            return None
        return stamp.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return None


def retrieval_timestamp(row: Mapping[str, Any]) -> tuple[datetime | None, str]:
    """Read the established retrieval stamp using its declared UTC/Riyadh basis."""
    grouped: dict[str, list[Any]] = {}
    for name,value in row.items():
        grouped.setdefault(_key(name), []).append(value)
    for name,values in grouped.items():
        if name.startswith("lastupdated") and len({_text(value) for value in values}) > 1:
            return None,"none"
    normalized = {_key(name): value for name, value in row.items()}
    for key, tz in (("lastupdatedutc", timezone.utc),
                    ("lastupdatedriyadh", timezone(timedelta(hours=3))),
                    ("lastupdated", timezone(timedelta(hours=3)))):
        raw = normalized.get(key)
        if not _text(raw):
            continue
        try:
            if isinstance(raw, datetime):
                stamp, precision = raw, "datetime"
            elif isinstance(raw, (int, float)) and not isinstance(raw, bool):
                if not math.isfinite(raw) or not 20000 < raw < 80000:
                    continue
                stamp = datetime(1899, 12, 30) + timedelta(days=raw)
                precision = "date" if raw == int(raw) else "datetime"
            else:
                stamp = datetime.fromisoformat(_text(raw).replace("Z", "+00:00"))
                precision = "datetime" if ":" in _text(raw) else "date"
            return (stamp.replace(tzinfo=tz) if stamp.tzinfo is None else stamp).astimezone(timezone.utc), precision
        except (ValueError, TypeError, OverflowError):
            return None, "none"
    return None, "none"


def timestamp_freshness(
    timestamp: datetime | None, now: datetime, *, precision: str = "datetime",
    max_age_seconds: float, clock_skew_seconds: float = MAX_CLOCK_SKEW_SECONDS,
) -> tuple[bool, float | None, str]:
    if timestamp is None or precision != "datetime":
        return False, None, "timestamp_precision_unknown"
    try:
        if (not math.isfinite(max_age_seconds) or max_age_seconds < 0
                or not math.isfinite(clock_skew_seconds) or clock_skew_seconds < 0):
            return False, None, "timestamp_policy_invalid"
        age = (now - timestamp).total_seconds()
    except (TypeError, ValueError, AttributeError):
        return False, None, "timestamp_basis_invalid"
    if age < -clock_skew_seconds:
        return False, age, "timestamp_future"
    if age > max_age_seconds:
        return False, age, "timestamp_stale"
    return True, age, ""


@dataclass(frozen=True)
class CoverageValidity:
    requested: int
    fresh: int | None
    valid: bool
    reasons: tuple[str, ...]

    @property
    def percent(self) -> float | None:
        if (type(self.requested) is not int or self.requested <= 0
                or type(self.fresh) is not int or not 0 <= self.fresh <= self.requested):
            return None
        return float(Decimal(self.fresh) * 100 / Decimal(self.requested))


def coverage_validity(requested: int, fresh: int | None, minimum_percent: Any = 95) -> CoverageValidity:
    reasons: list[str] = []
    try:
        minimum = Decimal(str(minimum_percent))
        if not minimum.is_finite() or not 0 <= minimum <= 100:
            raise ValueError("invalid threshold")
    except (ValueError, InvalidOperation):
        minimum = Decimal(95)
        reasons.append("invalid_threshold")
    if type(requested) is not int or requested <= 0:
        return CoverageValidity(0, None, False, tuple(reasons + ["requested_unknown"]))
    if fresh is None:
        reasons.append("acquisition_unknown")
    elif type(fresh) is not int or not 0 <= fresh <= requested:
        reasons.append("invalid_fresh_count")
    elif Decimal(fresh) * 100 < minimum * requested:
        reasons.append("coverage_below_minimum")
    return CoverageValidity(requested, fresh, not reasons, tuple(reasons))


@dataclass(frozen=True)
class AcquisitionValidity:
    status: str
    reason: str = ""
    acquired_at: datetime | None = None
    quote_asof: datetime | None = None

    @property
    def successful(self) -> bool:
        return self.status == "SUCCESS"


def row_acquisition(row: Mapping[str, Any], now: datetime, max_age_seconds: float) -> AcquisitionValidity:
    """Prove a live successful price acquisition; failures override success tags."""
    grouped: dict[str, list[Any]] = {}
    for name,value in row.items():
        grouped.setdefault(_key(name), []).append(value)
    warning_values = [value for name,values in grouped.items()
                      if name in {"warnings", "warning", "flags", "rowwarnings"} for value in values]
    warning_values += [value for name,values in grouped.items()
                      if name in {"error", "errors", "errormessage"} for value in values]
    warnings = "; ".join(_warning_text(value) for value in warning_values)
    if _INVALID_WARNINGS.search(warnings):
        return AcquisitionValidity("INVALID", "failed_or_quarantined_or_preserved")
    for name, values in grouped.items():
        if name in {"dataprovider", "currentprice", "symbol", "ticker", "lastupdatedutc", "lastupdatedriyadh", "lastupdated"} | {_key(key) for key in _TOKEN_KEYS}:
            if len({_text(value) for value in values}) > 1:
                return AcquisitionValidity("INVALID", "column_alias_conflict")
    normalized = {_key(name): value for name, value in row.items()}
    identity_values={_text(value).upper() for name,values in grouped.items()
                     if name in {"symbol","ticker"} for value in values}
    if len(identity_values)>1:
        return AcquisitionValidity("INVALID", "symbol_alias_conflict")
    symbol = normalized.get("symbol", normalized.get("ticker", ""))
    if _text(symbol) and not symbol_domain_ok(symbol):
        return AcquisitionValidity("INVALID", "symbol_domain_invalid")
    providers = [_text(value) for name,values in grouped.items()
                 if name in {"dataprovider", "provider", "datasource", "source"} for value in values if _text(value)]
    if any(_NONLIVE_PROVIDER.search(value) or value.lower() in {"none", "error", "unknown"} for value in providers):
        return AcquisitionValidity("INVALID", "nonlive_provider")
    if len({_key(value) for value in providers}) > 1:
        return AcquisitionValidity("INVALID", "column_alias_conflict")
    provider = providers[0] if providers else ""
    if provider and (_NONLIVE_PROVIDER.search(provider) or provider.lower() in {"none", "error", "unknown"}):
        return AcquisitionValidity("INVALID", "nonlive_provider")
    try:
        raw_prices = [value for name,values in grouped.items()
                      if name in {"currentprice", "price", "lastprice"} for value in values if _text(value)]
        prices = [float(_text(value).replace(",", "")) if not isinstance(value,bool) else float("nan") for value in raw_prices]
        if not prices or any(not math.isfinite(price) or price <= 0 for price in prices):
            raise ValueError("invalid price")
        if len(set(prices)) > 1:
            return AcquisitionValidity("INVALID", "column_alias_conflict")
    except (TypeError, ValueError):
        return AcquisitionValidity("INVALID", "price_missing_or_invalid")
    if not provider:
        return AcquisitionValidity("UNKNOWN", "provider_unknown")
    if _text(symbol) and not provider_supports_instrument(provider.lower(),_text(symbol)):
        return AcquisitionValidity("INVALID", "provider_instrument_unsupported")
    tokens = acquisition_tokens(warnings)
    for name in _TOKEN_KEYS:
        if _key(name) in normalized:
            value = _text(normalized[_key(name)])
            if name in tokens and tokens[name] != value:
                return AcquisitionValidity("INVALID", "acquisition_proof_conflict")
            tokens[name] = value
    status = tokens.get("acquisition_status", "").lower()
    if status in {"preserved", "unavailable", "failed", "conflict"}:
        return AcquisitionValidity("INVALID", "acquisition_" + status)
    if status and status != "success":
        return AcquisitionValidity("UNKNOWN", "acquisition_proof_missing")
    source = tokens.get("acquisition_provider", "")
    if status == "success" and not source:
        return AcquisitionValidity("UNKNOWN", "acquisition_provider_unknown")
    if _NONLIVE_PROVIDER.search(source) or source.lower() in {"none", "error", "unknown"}:
        return AcquisitionValidity("INVALID", "acquisition_provider_invalid")
    if source and _key(source) != _key(provider):
        return AcquisitionValidity("INVALID", "acquisition_provider_conflict")
    if status == "success":
        acquired = precise_utc(tokens.get("acquisition_acquired_at"))
        precision = "datetime" if acquired is not None else "none"
    else:
        acquired, precision = retrieval_timestamp(row)
    quote = precise_utc(tokens.get("acquisition_quote_asof"))
    if acquired is None or precision != "datetime":
        return AcquisitionValidity("UNKNOWN", "acquisition_time_unknown", acquired, quote)
    ok, _age, reason = timestamp_freshness(acquired, now, max_age_seconds=max_age_seconds)
    if not ok:
        return AcquisitionValidity("INVALID", "acquired_" + reason, acquired, quote)
    # Quote age is separate evidence; absent quote-asof cannot be inferred
    # from retrieval. Existing explicit stale/unverified guards remain binding.
    if quote is not None and quote > acquired + timedelta(seconds=MAX_CLOCK_SKEW_SECONDS):
        return AcquisitionValidity("INVALID", "quote_after_acquisition", acquired, quote)
    return AcquisitionValidity("SUCCESS", "", acquired, quote)


@dataclass
class AcquisitionCensus:
    requested: set[str] = field(default_factory=set)
    returned: set[str] = field(default_factory=set)
    successful: set[str] = field(default_factory=set)
    unknown: set[str] = field(default_factory=set)
    invalid: set[str] = field(default_factory=set)
    reasons: dict[str, str] = field(default_factory=dict)
    duplicates: set[str] = field(default_factory=set)
    timestamp_fresh: set[str] = field(default_factory=set)
    quote_asof_known: set[str] = field(default_factory=set)

    @property
    def reason_counts(self) -> dict[str, int]:
        return dict(sorted(Counter(self.reasons.values()).items()))


def acquisition_census(
    headers: Sequence[Any], rows: Iterable[Sequence[Any]], *, now: datetime,
    max_age_seconds: float, requested: Iterable[str] | None = None,
    symbol_key: Callable[[Any], str] = lambda value: _text(value).upper(),
) -> AcquisitionCensus:
    """Count identities once; conflicting failed duplicate cannot be hidden."""
    out = AcquisitionCensus()
    indexes = {_key(name): i for i, name in enumerate(headers)}
    symbol_indexes = [i for i,name in enumerate(headers) if _key(name) in {"symbol","ticker"}]
    si = symbol_indexes[0] if symbol_indexes else -1
    out.requested = {symbol_key(symbol) for symbol in requested or () if _text(symbol)}
    if si < 0:
        return out
    evidence: dict[str, list[AcquisitionValidity]] = {}
    for row in rows:
        if si >= len(row) or not _text(row[si]):
            continue
        symbol = symbol_key(row[si])
        identities = {symbol_key(row[i]) for i in symbol_indexes if i<len(row) and _text(row[i])}
        identity_conflict = len(identities)>1
        if identity_conflict and requested is not None:
            matching = sorted(identities & out.requested)
            if matching:
                symbol = matching[0]
        if requested is not None and symbol not in out.requested:
            continue
        out.returned.add(symbol)
        mapped: dict[str, Any] = {}
        # Preserve even identical duplicate headers for conservative alias
        # checking; an ordinary dict would hide the earlier cell.
        for i,name in enumerate(headers):
            label=str(name)
            while label in mapped:
                label += " "
            mapped[label]=row[i] if i<len(row) else None
        verdict = (AcquisitionValidity("INVALID","symbol_alias_conflict") if identity_conflict
                   else row_acquisition(mapped, now, max_age_seconds))
        evidence.setdefault(symbol, []).append(verdict)
        stamp, precision = retrieval_timestamp(mapped)
        if timestamp_freshness(stamp, now, precision=precision, max_age_seconds=max_age_seconds)[0]:
            out.timestamp_fresh.add(symbol)
        if verdict.quote_asof is not None:
            out.quote_asof_known.add(symbol)
    if requested is None:
        out.requested = set(out.returned)
    for symbol, verdicts in evidence.items():
        if len(verdicts) > 1:
            out.duplicates.add(symbol)
        failed = next((verdict for verdict in verdicts if verdict.status=="INVALID"),None)
        failed = failed or next((verdict for verdict in verdicts if not verdict.successful), None)
        if failed is None:
            out.successful.add(symbol)
        else:
            (out.unknown if failed.status == "UNKNOWN" else out.invalid).add(symbol)
            out.reasons[symbol] = failed.reason
    for symbol in out.requested - out.returned:
        out.invalid.add(symbol)
        out.reasons[symbol] = "response_missing"
    return out
