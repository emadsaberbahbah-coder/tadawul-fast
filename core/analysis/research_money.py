"""Strict numeric/native-unit inputs for research totals; no custody inference."""
from decimal import Decimal, InvalidOperation
import math
from typing import Any

from core.execution_accounting import AccountingError, _number
from core.portfolio_reconciliation import _native_currency


def research_amount(value: Any, *, positive: bool = False, signed: bool = False) -> float | None:
    """Use the accounting Decimal contract; percent/formatted money is unknown."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        if signed:
            number = Decimal(str(value).strip())
            _number(number.copy_abs(), "research amount", allow_zero=not positive)
        else:
            number = _number(value, "research amount", allow_zero=not positive)
        result = float(number)
        return result if math.isfinite(result) else None
    except (AccountingError, InvalidOperation, ValueError, TypeError, OverflowError):
        return None


def research_currency(value: Any) -> str | None:
    """Reject quote subunits and capitalization ambiguity rather than rescale."""
    try:
        # USX is a quote subunit even when the opportunity rollout gate is off.
        if isinstance(value, str) and value.strip() == "USX":
            return None
        return _native_currency(value)
    except AccountingError:
        return None
