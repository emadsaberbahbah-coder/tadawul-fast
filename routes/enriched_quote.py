# ----------------------------------------------------------------------
# ROUTES
# ----------------------------------------------------------------------


@router.get("/health")
async def enriched_health() -> Dict[str, Any]:
    """
    Simple health check for this module.
    """
    return {
        "status": "ok",
        "module": "enriched_quote",
        "version": "2.0",
    }


@router.get("/quote", response_model=EnrichedQuoteResponse)
async def get_enriched_quote_route(
    symbol: str = Query(..., alias="symbol"),
) -> EnrichedQuoteResponse:
    """
    Get enriched quote for a single symbol.

    Example:
        GET /v1/enriched/quote?symbol=AAPL
        GET /v1/enriched/quote?symbol=1120.SR
    """
    ticker = (symbol or "").strip()
    if not ticker:
        raise HTTPException(status_code=400, detail="Symbol is required")

    try:
        quote = await get_enriched_quote(ticker)
        enriched = _quote_to_enriched(quote)
        if enriched.data_quality == "MISSING":
            enriched.error = "No data available from providers"
        return enriched
    except HTTPException:
        raise
    except Exception as exc:
        # Never break Google Sheets – always return a valid response
        return EnrichedQuoteResponse(
            symbol=ticker.upper(),
            data_quality="MISSING",
            error=f"Exception in enriched quote: {exc}",
        )


@router.post("/quotes", response_model=BatchEnrichedResponse)
async def get_enriched_quotes_route(
    body: BatchEnrichedRequest,
) -> BatchEnrichedResponse:
    """
    Get enriched quotes for multiple symbols.

    Body:
        {
          "tickers": ["AAPL", "MSFT", "1120.SR"]
        }
    """
    tickers = [t.strip() for t in (body.tickers or []) if t and t.strip()]
    if not tickers:
        raise HTTPException(status_code=400, detail="At least one symbol is required")

    try:
        unified_quotes = await get_enriched_quotes(tickers)
    except Exception as exc:
        # Complete failure – build placeholder entries for all
        return BatchEnrichedResponse(
            results=[
                EnrichedQuoteResponse(
                    symbol=t.upper(),
                    data_quality="MISSING",
                    error=f"Batch enriched quotes failed: {exc}",
                )
                for t in tickers
            ]
        )

    results: List[EnrichedQuoteResponse] = []
    for t, q in zip(tickers, unified_quotes):
        try:
            enriched = _quote_to_enriched(q)
            if enriched.data_quality == "MISSING":
                enriched.error = "No data available from providers"
            results.append(enriched)
        except Exception as exc:
            results.append(
                EnrichedQuoteResponse(
                    symbol=t.upper(),
                    data_quality="MISSING",
                    error=f"Exception building enriched quote: {exc}",
                )
            )

    return BatchEnrichedResponse(results=results)


@router.post("/sheet-rows", response_model=SheetEnrichedResponse)
async def get_enriched_sheet_rows(
    body: BatchEnrichedRequest,
) -> SheetEnrichedResponse:
    """
    Google Sheets–friendly endpoint.

    Returns:
        {
          "headers": [...],
          "rows": [ [row for t1], [row for t2], ... ]
        }

    Apps Script usage pattern:
        - First row = headers
        - Following rows = values
    """
    batch = await get_enriched_quotes_route(body)
    headers = _build_sheet_headers()
    rows: List[List[Any]] = [_enriched_to_sheet_row(e) for e in batch.results]
    return SheetEnrichedResponse(headers=headers, rows=rows)
