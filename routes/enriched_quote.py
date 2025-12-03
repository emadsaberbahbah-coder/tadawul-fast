"""
routes/enriched_quote.py
-------------------------------------------------------
ENRICHED QUOTE ROUTES – v1.0

- Exposes a clean endpoint that uses the Unified Data & Analysis Engine
  from core.data_engine.get_enriched_quote.

- Output:
    * Normalized quote (KSA + Global)
    * data_quality, data_gaps
    * opportunity_score, risk_flag, notes
    * sources[] (which providers were used)

NOTE:
- Adjust prefix "/v1" or auth logic to match your existing project.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from core.data_engine import get_enriched_quote, UnifiedQuote

# If you already have a shared auth dependency, import it instead:
# from core.security import get_current_api_user  # example

router = APIRouter(
    prefix="/v1",
    tags=["enriched_quote"],
)


def validate_api_token(token: str = Query(..., description="API access token")) -> str:
    """
    SIMPLE TOKEN VALIDATION PLACEHOLDER.

    IMPORTANT:
    - Replace this with your existing auth logic if you already have:
        * a dependency like get_current_user
        * or a token verification function.

    For now:
    - Accepts any non-empty token string, but keeps the interface consistent.
    """
    if not token or not token.strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or empty token",
        )
    return token


@router.get(
    "/enriched-quote",
    response_model=UnifiedQuote,
    summary="Get enriched quote with data quality & opportunity score",
    response_description="Unified quote with merged providers and analysis.",
)
async def enriched_quote_endpoint(
    symbol: str = Query(..., description="Ticker symbol, e.g. 1120.SR or MSFT"),
    token: str = Depends(validate_api_token),
):
    """
    Get a fully-enriched quote for a single symbol.

    QUERY PARAMS:
    - symbol: ticker, e.g. 1120.SR, 2222.SR, MSFT, AAPL
    - token:  your API token (placeholder validation for now)

    RETURNS:
    - UnifiedQuote model (see core.data_engine.UnifiedQuote)
    """

    try:
        quote = await get_enriched_quote(symbol)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating enriched quote: {exc}",
        ) from exc

    return quote
