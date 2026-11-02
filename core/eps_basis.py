"""Keep diagnostic EPS provenance bound to the value that actually landed.

These source observations do not certify eligibility, quote freshness, currency
or financial-report comparability. Missing evidence remains missing.
"""
from copy import deepcopy
from math import isfinite

FIELDS = ("eps_ttm", "eps_estimate_current_year", "eps_net_income_current_shares")
KEY = "_eps_basis"


def _matches(proof, value):
    if not isinstance(proof, dict) or isinstance(value, bool) or value is None:
        return False
    observed = proof.get("value")
    if isinstance(observed, bool) or not isinstance(observed, (int, float)):
        return False
    try:
        return isfinite(observed) and isfinite(float(value)) and observed == float(value)
    except (TypeError, ValueError, OverflowError):
        return False


def copy_basis(destination, source, landed_fields):
    """Copy only matching landed values; discard stale destination receipts."""
    prior = destination.get(KEY)
    proofs = {field: deepcopy(proof) for field, proof in prior.items()
              if field in FIELDS and _matches(proof, destination.get(field))} \
        if isinstance(prior, dict) else {}
    incoming = source.get(KEY)
    for field in landed_fields:
        if field not in FIELDS:
            continue
        proofs.pop(field, None)
        proof = incoming.get(field) if isinstance(incoming, dict) else None
        if _matches(proof, destination.get(field)):
            proofs[field] = deepcopy(proof)
    if proofs:
        destination[KEY] = proofs
    else:
        destination.pop(KEY, None)
