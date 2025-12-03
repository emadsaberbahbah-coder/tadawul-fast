def read_ksa_tadawul_market(
    self, use_cache: bool = True, limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Enhanced KSA Tadawul Market data reading with caching."""
    sheet_key = "KSA_TADAWUL"

    if use_cache:
        cached_data = self._load_from_cache(sheet_key)
        if cached_data is not None:
            return cached_data[:limit] if limit else cached_data

    try:
        ws = self.get_sheet_by_key(sheet_key)
        if not ws:
            return []

        self._throttle_requests()
        rows = ws.get_all_records()
        results: List[Dict[str, Any]] = []

        for row in rows:
            ticker = str(
                row.get("Symbol")
                or row.get("Ticker")
                or row.get("Code")
                or ""
            ).strip()
            if not ticker:
                continue

            entry = {
                "ticker": ticker,
                "custom_tag": str(
                    row.get("Custom Tag")
                    or row.get("Custom Tag / Watchlist")
                    or row.get("Watchlist")
                    or ""
                ).strip(),
                "company_name": str(
                    row.get("Company Name")
                    or row.get("Instrument Name")
                    or ""
                ).strip(),
                "sector": str(row.get("Sector", "")).strip(),
                "sub_sector": str(row.get("Sub-Sector", "")).strip(),
                "trading_market": str(
                    row.get("Trading Market")
                    or row.get("Market")
                    or ""
                ).strip(),
                "currency": str(row.get("Currency", "")).strip(),
                "listing_date": str(row.get("Listing Date", "")).strip(),

                "shares_outstanding": self._safe_float(
                    row.get("Shares Outstanding")
                ),
                "free_float": self._safe_float(
                    row.get("Free Float")
                ),
                "market_cap": self._safe_float(
                    row.get("Market Cap")
                ),

                "last_price": self._safe_float(
                    row.get("Last Price")
                    or row.get("Price")
                ),
                "day_high": self._safe_float(
                    row.get("Day High")
                    or row.get("High")
                ),
                "day_low": self._safe_float(
                    row.get("Day Low")
                    or row.get("Low")
                ),
                "previous_close": self._safe_float(
                    row.get("Previous Close")
                ),
                "open_price": self._safe_float(
                    row.get("Open")
                    or row.get("Open Price")
                ),
                "change_value": self._safe_float(
                    row.get("Change Value")
                    or row.get("Change")
                ),
                "change_pct": self._safe_float(
                    row.get("Change %")
                    or row.get("Change Percent")
                ),
                "high_52w": self._safe_float(
                    row.get("52 Week High")
                    or row.get("52W High")
                ),
                "low_52w": self._safe_float(
                    row.get("52 Week Low")
                    or row.get("52W Low")
                ),
                "avg_price_50d": self._safe_float(
                    row.get("Average Price (50D)")
                ),

                "volume": self._safe_int(row.get("Volume")),
                "avg_volume_30d": self._safe_int(
                    row.get("Average Volume (30D)")
                ),
                "value_traded": self._safe_float(
                    row.get("Value Traded")
                ),
                "turnover_rate": self._safe_float(
                    row.get("Turnover Rate")
                ),
                "bid_price": self._safe_float(
                    row.get("Bid Price")
                ),
                "ask_price": self._safe_float(
                    row.get("Ask Price")
                ),
                "bid_size": self._safe_int(row.get("Bid Size")),
                "ask_size": self._safe_int(row.get("Ask Size")),
                "spread_pct": self._safe_float(
                    row.get("Spread %")
                ),
                "liquidity_score": self._safe_float(
                    row.get("Liquidity Score")
                ),

                "eps": self._safe_float(row.get("EPS")),
                "pe": self._safe_float(
                    row.get("P/E")
                    or row.get("P/E Ratio")
                ),
                "pb": self._safe_float(
                    row.get("P/B")
                    or row.get("P/B Ratio")
                ),
                "dividend_yield": self._safe_float(
                    row.get("Dividend Yield")
                ),
                "dividend_payout": self._safe_float(
                    row.get("Dividend Payout")
                ),
                "roe": self._safe_float(row.get("ROE")),
                "roa": self._safe_float(row.get("ROA")),
                "debt_equity": self._safe_float(
                    row.get("Debt/Equity")
                ),
                "current_ratio": self._safe_float(
                    row.get("Current Ratio")
                ),
                "quick_ratio": self._safe_float(
                    row.get("Quick Ratio")
                ),

                "revenue_growth": self._safe_float(
                    row.get("Revenue Growth")
                ),
                "net_income_growth": self._safe_float(
                    row.get("Net Income Growth")
                ),
                "ebitda_margin": self._safe_float(
                    row.get("EBITDA Margin")
                ),
                "operating_margin": self._safe_float(
                    row.get("Operating Margin")
                ),
                "net_margin": self._safe_float(
                    row.get("Net Margin")
                ),
                "ev_ebitda": self._safe_float(
                    row.get("EV/EBITDA")
                ),
                "price_sales": self._safe_float(
                    row.get("Price/Sales")
                ),
                "price_cash_flow": self._safe_float(
                    row.get("Price/Cash Flow")
                ),
                "peg_ratio": self._safe_float(
                    row.get("PEG Ratio")
                ),
                "opportunity_score": self._safe_float(
                    row.get("Opportunity Score")
                ),

                "rsi_14": self._safe_float(
                    row.get("RSI (14)")
                    or row.get("RSI14")
                ),
                "macd": self._safe_float(
                    row.get("MACD")
                ),
                "ma_20d": self._safe_float(
                    row.get("Moving Avg (20D)")
                ),
                "ma_50d": self._safe_float(
                    row.get("Moving Avg (50D)")
                ),
                "volatility": self._safe_float(
                    row.get("Volatility")
                ),

                "last_updated": str(
                    row.get("Last Updated") or ""
                ).strip(),
                "last_updated_riyadh": str(
                    row.get("Last Updated (Riyadh)") or ""
                ).strip(),
                "data_source": str(
                    row.get("Data Source") or ""
                ).strip(),
                "data_quality": str(
                    row.get("Data Quality") or ""
                ).strip(),

                "timestamp": datetime.now().isoformat(),
            }
            results.append(entry)

            if limit and len(results) >= limit:
                break

        logger.info(f"📈 Read {len(results)} KSA Tadawul rows")
        self.request_count += 1
        self.success_count += 1

        if use_cache and results:
            self._save_to_cache(sheet_key, results)

        return results

    except Exception as e:
        logger.error(f"❌ Failed to read KSA Tadawul Market: {e}")
        self.error_count += 1
        return []
