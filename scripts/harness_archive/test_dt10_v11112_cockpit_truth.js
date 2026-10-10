#!/usr/bin/env node
/* tests/test_dt10_v11112_cockpit_truth.js — dual-tree real-module harness for
 * 16_Decision_Top10.gs v1.11.12 [P-181b / P-149 / P-195 / SYNC-INFLIGHT].
 * Loads the REAL .gs source (base v1.11.11 and delivered v1.11.12) into a vm
 * context with minimal Apps Script service stubs (the P-144 harness shape)
 * and drives the REAL dt10MapHeaderCols_ / dt10PoolRowFromSheetRow_ /
 * dt10QualToRow_ / dt10SeatTruthKpi_ / dt10SyncInflightCore_ /
 * dt10SyncInflight_ / dt10RunMarkerCore_ / dt10RunMarkerCheck_ /
 * refreshDecisionTop10 (enforce + observe paths) with the real 2026-10-04
 * Global_Markets header + rows (RDN / NVDA / DDI / BHF) and the real
 * 2026-10-03 13:05 race stamps.
 * Usage: node test_dt10_v11112_cockpit_truth.js [--base p] [--delivered p]
 * Prints N/N PASS and a cases-digest; exit 1 on any FAIL. */
'use strict';
var fs = require('fs'), vm = require('vm'), crypto = require('crypto');
var args = process.argv.slice(2);
function arg(k, d) { var i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; }
var BASE = arg('--base', '16_Decision_Top10_base.gs');
var DELIV = arg('--delivered', '16_Decision_Top10.gs');
var FIX = {"header": ["Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country", "Sector", "Industry", "Current Price", "Previous Close", "Open", "Day High", "Day Low", "52W High", "52W Low", "Price Change", "Percent Change", "52W Position %", "Volume", "Avg Volume 10D", "Avg Volume 30D", "Market Cap", "Float Shares", "Beta (5Y)", "P/E (TTM)", "P/E (Forward)", "EPS (TTM)", "Dividend Yield", "Payout Ratio", "Revenue (TTM)", "Revenue Growth YoY", "Gross Margin", "Operating Margin", "Profit Margin", "Debt/Equity", "Free Cash Flow (TTM)", "RSI (14)", "Volatility 30D", "Volatility 90D", "Max Drawdown 1Y", "VaR 95% (1D)", "Sharpe (1Y)", "Risk Score", "Risk Bucket", "P/B", "P/S", "EV/EBITDA", "PEG", "Intrinsic Value", "Upside %", "Valuation Score", "Forecast Price 1M", "Forecast Price 3M", "Forecast Price 12M", "Expected ROI 1M", "Expected ROI 3M", "Expected ROI 12M", "Forecast Confidence", "Confidence Score", "Confidence Bucket", "Value Score", "Quality Score", "Momentum Score", "Growth Score", "Overall Score", "Opportunity Score", "Rank (Overall)", "Analyst Rating", "Target Price", "Upside/Downside %", "Signal", "Trend 1M", "Trend 3M", "Trend 12M", "ST Signal", "Recommendation", "Recommendation Detail", "Recommendation Reason", "Reco Priority", "Priority Band", "Recommendation Source", "Horizon Days", "Invest Period Label", "Sector-Adj Score", "Conviction Score", "Top Factors", "Top Risks", "Position Size Hint", "Candle Pattern", "Candle Signal", "Candle Strength", "Candle Confidence", "Recent Patterns (5D)", "Provider Rating", "Scoring Reco Source", "Scoring Schema Version", "Scoring Errors", "Opportunity Source", "Overall Score (Raw)", "Overall Penalty Factor", "Forecast Source", "Data Quality Score", "Forecast Reliability Score", "Provider/Engine Conflict", "Conflict Type", "Final Decision Basis", "Investability Status", "Final Action", "Block Reason", "Data Provider", "Provider Secondary", "Last Updated (UTC)", "Last Updated (Riyadh)", "Row Source", "Warnings"], "rows": {"DDI.US": ["DDI.US", "DoubleDown Interactive Co., Ltd.", "Equity", "NYSE/NASDAQ", "USD", "USA", "Communication Services", "Electronic Gaming & Multimedia", 13.31, 13.16, 13.24, 13.34, 13.177, 13.4, 8.1, 0.15, 0.01139818, 98.301887, 217580.0, 180560.0, 170561.67, 659556307.1934509, 16074640.0, 1.028, 5.2817464, 5.460513, 2.52, "", 0.0, 380043008.0, 0.112, 0.73363996, 0.38715, 32.908, 0.03702, 97248496.0, 77.95, 0.14488, 0.212863, -0.149485, -0.02604, 1.0847, 19.78, "LOW", 0.6522592, 1.735478, 0.94, "", "", "", 97.96, 13.7892, 14.7075, 17.7519, 0.036003, 0.104996, 0.333727, 0.6164, 61.64, "MODERATE", 97.96, 83.98, 42.68, 68.67, 76.81, 78.61, 2.0, "", 18.05, 0.356123, "BUY", "UP", "UP", "UP", "OVERBOUGHT", "BUY", "BUY", "BUY: Positive 3M expected return with acceptable confidence and risk. | overall=76.8 risk=19.8 conf=61.6 roi1m=3.6% roi3m=10.5% roi12m=33.4% horizon=month", 2.0, "P2", "engine", 365.0, "3M", 81.81, 58.0, "Strong fundamentals; Revenue growth", "Overbought (RSI)", "Standard position", "", "NEUTRAL", "", 0.0, "Bullish Engulfing | Doji", "", "scoring.py v5.11.2", "5.11.2", "", "both_present_fallback", 76.81, 1.0, "provider_target", 100.0, 53.1, "FALSE", "", "Engine", "INVESTABLE", "INVEST", "", "eodhd", "", "2026-10-03T23:32:49.385764+00:00", "2026-10-04T02:32:49.385779+03:00", "", "quote_exchange_from_suffix; quote_currency_from_suffix; name_from_chart_meta; fund_cache:hit:2h:19:nt; analyst_lkg:2h; provider_target_capped_for_short_horizon_derivation; eq_roi_backfill:expected_roi_12m:t12:observe; eq_roi_backfill:expected_roi_3m:t12:observe; eq_roi_backfill:expected_roi_1m:t12:observe; f7_settle:observe:st3:p2:ov=76.81>69.93:op=78.61>66.17:va=97.96>79.19:os=bpf>rb; analyst_trend_block_applied; confidence_default_suspected; rel_path:b=B:fc=61.6:dq=100.0:pen=SC5,OS:os=both_present_fb:fs=pt:raw=53.1:cf=none:fin=53.1"], "BHF.US": ["BHF.US", "Brighthouse Financial, Inc.", "Equity", "NYSE/NASDAQ", "USD", "USA", "Financial Services", "Insurance - Life", 50.02, 50.05, 50.02, 50.67, 49.57, 66.8, 44.51, -0.03, -0.0005994, 24.719605, 407795.0, 1073310.0, 1007170.67, 2876728407.5867043, 56656366.0, 0.84, 3.9666932, 2.4836147, 12.61, "", 0.0, 6837000192.0, 0.867, 0.35439998, 0.77778, "", 1.78382, 19879124992.0, 50.1, 0.274644, 0.219938, -0.267527, -0.026617, 0.3985, 30.07, "LOW", 0.43916872, 0.42075887, "", "", 70.028, 0.4, 78.72, 52.7164, 56.3116, 65.0, 0.053906, 0.125782, 0.29948, 0.6482, 64.82, "MODERATE", 78.72, 77.66, 51.91, 100.0, 72.41, 67.19, 1.0, "HOLD", 65.0, 0.29948, "BUY", "UP", "UP", "UP", "BULLISH", "BUY", "BUY", "BUY: Positive 3M expected return with acceptable confidence and risk. | overall=72.4 risk=30.1 conf=64.8 roi1m=5.4% roi3m=12.6% roi12m=29.9% horizon=month", 2.0, "P2", "engine", 365.0, "3M", 77.41, 74.0, "Attractive valuation; Strong fundamentals; Solid growth", "Limited downside signals", "Add gradually / accumulate", "Doji", "DOJI", "STRONG", 95.0, "Evening Star | Shooting Star | Doji | Doji", "HOLD", "scoring.py v5.11.2", "5.11.2", "", "roi_based", 74.61, 0.9705, "provider_target", 100.0, 75.4, "FALSE", "Aligned", "Engine", "INVESTABLE", "INVEST", "", "eodhd", "", "2026-10-03T23:38:21.228285+00:00", "2026-10-04T02:38:21.228296+03:00", "", "quote_exchange_from_suffix; quote_currency_from_suffix; yahoo_enrichment_applied; fund_unit_contract:yahoo:debt_to_equity; fund_coherence_quarantined:profit_margin; intrinsic_soft_ceiling_applied; f7_settle:observe:stx:p3:ov=72.41>73.64:va=78.72>82.24; analyst_trend_block_applied; confidence_default_suspected; rel_path:b=B:fc=64.8:dq=100.0:pen=none:os=ri_based:fs=pt:raw=75.4:cf=none:fin=75.4"], "NVDA.US": ["NVDA.US", "NVIDIA Corporation", "Equity", "NYSE/NASDAQ", "USD", "USA", "Technology", "Semiconductors", 233.95, 230.86, 236.055, 237.88, 233.6, 237.88, 164.27, 3.09, 0.01338474, 94.661051, 134350957.0, 106184832.4, 125195201.73, 5649190576309.204, 23132626000.0, 2.217, 29.17082, 14.905945, 8.02, "", 0.0354, 302970011648.0, 1.059, 0.74674004, 0.66236997, 63.663, 0.16971, 41809874944.0, 82.97, 0.391139, 0.396073, -0.202231, -0.037043, 0.781, 39.86, "MODERATE", 24.670464, 18.646038, 27.896, 0.28, 256.64, 0.096987, 24.32, 242.3722, 258.5147, 314.2723, 0.036, 0.105, 0.343331, 0.6164, 61.64, "MODERATE", 24.32, 84.34, 42.01, 100.0, 49.11, 30.51, 17.0, "", 327.7, 0.400727, "SELL", "UP", "UP", "UP", "OVERBOUGHT", "SELL", "SELL", "SELL: Weak score profile does not support holding the position. | overall=49.1 risk=39.9 conf=61.6 roi1m=3.6% roi3m=10.5% roi12m=34.3% horizon=month", 5.0, "P5", "engine", 365.0, "3M", 54.11, 66.0, "Strong fundamentals; Solid growth; Revenue growth", "Overbought (RSI)", "Exit position", "", "NEUTRAL", "", 0.0, "Shooting Star | Shooting Star", "", "scoring.py v5.11.2", "5.11.2", "", "both_present_fallback", 52.14, 0.9419, "provider_target", 100.0, 53.1, "FALSE", "", "Engine", "WATCHLIST", "DO_NOT_INVEST", "Engine recommends SELL", "eodhd", "", "2026-10-03T23:53:11.271799+00:00", "2026-10-04T02:53:11.271815+03:00", "", "quote_exchange_from_suffix; quote_currency_from_suffix; name_from_chart_meta; fund_cache:hit:6h:21:nt; sanitized:dividend_yield_out_of_range; analyst_lkg:6h; provider_target_capped_for_short_horizon_derivation; eq_roi_backfill:expected_roi_12m:t12:observe; eq_roi_backfill:expected_roi_3m:t12:observe; eq_roi_backfill:expected_roi_1m:t12:observe; f7_settle:observe:st3:p2:ov=49.11>63.8:op=30.51>66.41:va=24.32>56.3:rc=sell>accumulate:os=bpf>rb; analyst_trend_block_applied; bearish_reco_high_modeled_upside; confidence_default_suspected; rel_path:b=B:fc=61.6:dq=100.0:pen=SC5,OS:os=both_present_fb:fs=pt:raw=53.1:cf=none:fin=53.1"], "RDN.US": ["RDN.US", "Radian Group Inc.", "Equity", "NYSE/NASDAQ", "USD", "USA", "Financial Services", "Insurance - Specialty", 32.57, 31.22, 31.25, 32.74, 31.235, 41.05, 30.53, 1.35, 0.04324151, 19.391635, 2732653.0, 2015310.0, 1414296.67, 4309053886.884842, 129982076.0, 0.699, 8.041975, 6.161174, 4.05, 0.0313, 0.25190002, 1649537024.0, 0.935, 0.69302005, 0.31627, 32.512, 0.27967, 308735264.0, 30.38, 0.289492, 0.267653, -0.229161, -0.020917, -0.042, 29.93, "LOW", 0.9046469, 2.6122808, 5.472, 0.76, 45.5107, 0.397321, 80.24, 34.5441, 37.1761, 43.537, 0.06061, 0.141423, 0.336721, 0.6641, 66.41, "MODERATE", 80.24, 81.78, 33.71, 100.0, 70.46, 69.33, 1.0, "BUY", 44.5, 0.366288, "BUY", "UP", "UP", "UP", "BULLISH", "BUY", "BUY", "BUY: Positive 3M expected return with acceptable confidence and risk. | overall=70.5 risk=29.9 conf=66.4 roi1m=6.1% roi3m=14.1% roi12m=33.7% horizon=month", 2.0, "P2", "engine", 365.0, "3M", 75.46, 82.0, "Attractive valuation; Strong fundamentals; Solid growth", "Limited downside signals", "Add gradually / accumulate", "", "NEUTRAL", "", 0.0, "Hammer", "BUY", "scoring.py v5.11.2", "5.11.2", "", "roi_based", 72.57, 0.971, "provider_target", 100.0, 71.5, "FALSE", "Aligned", "Engine", "INVESTABLE", "INVEST", "", "eodhd", "", "2026-10-03T23:53:22.010436+00:00", "2026-10-04T02:53:22.010448+03:00", "", "quote_exchange_from_suffix; quote_currency_from_suffix; yahoo_enrichment_applied; fund_unit_contract:yahoo:debt_to_equity; fund_coherence_repaired:profit_margin:x100; provider_target_12m_capped_to_phase_ii_ceiling; provider_target_capped_for_short_horizon_derivation; intrinsic_soft_ceiling_applied; f7_settle:observe:stx:p3:ov=70.46>71.28:va=80.24>82.36; analyst_trend_block_applied; confidence_default_suspected; rel_path:b=B:fc=66.4:dq=100.0:pen=SC5:os=ri_based:fs=pt:raw=71.5:cf=none:fin=71.5"]}};

/* ---- stubs: a sheet whose ranges answer with a grid of '' unless seeded ---- */
function mkRange(sheet, r, c, nr, nc, a1) {
  var self = {
    getValue: function () { return sheet._val(r, c); },
    getValues: function () {
      if (a1 && sheet._a1 && sheet._a1[a1]) return sheet._a1[a1];
      var out = [];
      for (var i = 0; i < (nr || 1); i++) {
        var row = [];
        for (var j = 0; j < (nc || 1); j++) row.push(sheet._val(r + i, c + j));
        out.push(row);
      }
      return out;
    },
    setValue: function (v) { sheet._set(r, c, v); return self; },
    setValues: function (vv) { for (var i = 0; i < vv.length; i++) for (var j = 0; j < vv[i].length; j++) sheet._set(r + i, c + j, vv[i][j]); return self; },
    setNumberFormat: function () { return self; }, setNumberFormats: function () { return self; },
    setWrap: function () { return self; }, setFontSize: function () { return self; },
    setBackground: function () { return self; }, setBackgrounds: function () { return self; },
    setFontColor: function () { return self; }, setFontColors: function () { return self; },
    setFontWeight: function () { return self; }, setFontWeights: function () { return self; },
    setHorizontalAlignment: function () { return self; }, setBorder: function () { return self; },
    clear: function () { return self; }, clearContent: function () { return self; },
    clearFormat: function () { return self; }, merge: function () { return self; },
    breakApart: function () { return self; }, setNote: function () { return self; },
    getNumRows: function () { return nr || 1; }, getNumColumns: function () { return nc || 1; },
    getRow: function () { return r; }, getColumn: function () { return c; }
  };
  return self;
}
function mkSheet(name, seedVals, a1) {
  var cells = {};
  var sh = {
    _name: name, _a1: a1 || {}, _appended: [],
    _val: function (r, c) { var k = r + ':' + c; return cells.hasOwnProperty(k) ? cells[k] : ''; },
    _set: function (r, c, v) { cells[r + ':' + c] = v; },
    getName: function () { return name; },
    getRange: function (a, b, nr, nc) {
      if (typeof a === 'string') return mkRange(sh, 1, 1, 60, 2, a);
      return mkRange(sh, a, b, nr, nc, null);
    },
    getDataRange: function () { return mkRange(sh, 1, 1, 1, 1, null); },
    getLastRow: function () { return 20; }, getLastColumn: function () { return 30; },
    getMaxRows: function () { return 200; }, getMaxColumns: function () { return 40; },
    appendRow: function (row) { sh._appended.push(row); return sh; },
    insertRows: function () { return sh; }, deleteRows: function () { return sh; },
    setColumnWidth: function () { return sh; }, setFrozenRows: function () { return sh; },
    autoResizeColumns: function () { return sh; }, hideSheet: function () { return sh; },
    clear: function () { return sh; }, clearContents: function () { return sh; },
    getSheetId: function () { return 1; }
  };
  if (seedVals) for (var k in seedVals) if (seedVals.hasOwnProperty(k)) { var p = k.split(':'); cells[k] = seedVals[k]; }
  return sh;
}
function stubs(opts) {
  opts = opts || {};
  var props = opts.props || {};
  var statusRows = opts.statusRows || [];
  var top10 = mkSheet('Top_10_Investments', { '4:1': 'CONTROL PANEL', '14:1': 'KPIs' });
  var status = mkSheet('_Status', null, { 'L1:M60': statusRows });
  var runlog = mkSheet('_Run_Log');
  var sheets = { 'Top_10_Investments': top10, '_Status': status, '_Run_Log': runlog };
  var ss = { getSheetByName: function (n) { return sheets[n] || null; },
             getRangeByName: function () { return null; }, getSheets: function () { return [top10, status, runlog]; },
             getId: function () { return 'stub' }, getSpreadsheetTimeZone: function () { return 'Asia/Riyadh'; },
             insertSheet: function (n) { sheets[n] = mkSheet(n); return sheets[n]; } };
  var fetchCalls = [];
  return {
    console: console, Logger: { log: function () {} },
    Utilities: { formatDate: function (d) { return new Date(d).toISOString().replace('T', ' ').slice(0, 19); },
                 sleep: function () {}, computeDigest: function () { return []; }, base64Encode: function () { return ''; },
                 DigestAlgorithm: { MD5: 'MD5' } },
    PropertiesService: { getScriptProperties: function () { return {
      getProperty: function (k) { return props.hasOwnProperty(k) ? props[k] : null; },
      setProperty: function (k, v) { props[k] = String(v); return this; },
      deleteProperty: function (k) { delete props[k]; return this; },
      getProperties: function () { return props; } }; },
      getUserProperties: function () { return this.getScriptProperties(); } },
    SpreadsheetApp: { getActiveSpreadsheet: function () { return ss; }, getActive: function () { return ss; },
                      getUi: function () { throw new Error('no UI'); }, openById: function () { return ss; },
                      flush: function () {} },
    UrlFetchApp: { fetch: function (url) { fetchCalls.push(url); throw new Error('no network'); } },
    Session: { getScriptTimeZone: function () { return 'Asia/Riyadh'; } },
    CacheService: { getScriptCache: function () { return { get: function () { return null; }, put: function () {} }; } },
    LockService: { getScriptLock: function () { return { tryLock: function () { return true; }, releaseLock: function () {} }; } },
    _props: props, _ss: ss, _sheets: sheets, _fetchCalls: fetchCalls
  };
}
function loadTree(file, opts) {
  opts = opts || {};
  var env = stubs(opts);
  var ctx = vm.createContext(env);
  if (opts.nowMs) {   /* freeze the context clock for replayed stamps */
    vm.runInContext('Date.now = function () { return ' + opts.nowMs + '; };', ctx);
  }
  vm.runInContext(fs.readFileSync(file, 'utf8'), ctx, { filename: file });
  ctx._env = env;
  return ctx;
}
var RACE_NOW = Date.UTC(2026, 9, 3, 10, 5, 0);   /* 13:05 Riyadh, 2026-10-03 */
function json(x) { return JSON.stringify(x); }
function deepEq(a, b) { return json(a) === json(b); }

/* real 2026-10-03 13:05 race (run 37114669145 wrote ML at 12:59; GM still run 37079471147 from 03:43) */
var ML_RACE = 'OK | cov=100.0 | run=37114669145 | 2026-10-03 12:59:12+03:00';
var GM_RACE = 'OK | cov=99.1 | run=37079471147 | 2026-10-03 03:43:37+03:00';
/* real 2026-10-04 clean state (run 37160795404 complete) */
var ML_CLEAN = 'OK | cov=100.0 | run=37160795404 | 2026-10-04 02:12:58+03:00';
var GM_CLEAN = 'OK | cov=99.8 | run=37160795404 | 2026-10-04 03:07:34+03:00';
var FEED_OK = 'EXECUTABLE | run=37160795404 | 2026-10-04 03:07:35+03:00 | ML:OK GM:OK CFX:OK MF:OK';
function statusRows(ml, gm) {
  return [['TFB Feed Market_Leaders', ml], ['TFB Feed Global_Markets', gm],
          ['TFB Decision Feed', FEED_OK], ['TFB Feed Commodities_FX', ml], ['TFB Feed Mutual_Funds', ml]];
}
/* the AER.US qualified candidate on the 2026-10-04 board (builder v1.23.0 shape) */
function aerCand() {
  return { symbol: 'AER.US', name: 'AerCap Holdings N.V.', market: 'NYSE/NASDAQ', sector: 'Industrials',
           verdict: 'INVEST', selected: false, deferral: null, structural_block: true,
           first_fail: { gate: 'Portfolio', current: 'held', required: 'exclude holdings (Include Portfolio Holdings = No)' },
           failure_reason: 'Portfolio: held vs exclude holdings (Include Portfolio Holdings = No)',
           engine_roi_pct: 24.2, ann_roi_pct: 57.9, rr: 2.92, reliability: 75.4, dq: 100, risk_level: 'Low',
           opportunity_score: 74.7 };
}
function boardPayload() {   /* 10-04 08:09 board: 4 FT suspended + 2 grace, max 10 */
  return { status: 'ok', selected: [
    { symbol: 'ITRN.US', rank: 1, _sizing_suspended: true, _fast_track: true, suggested_shares: 0, detail: {} },
    { symbol: 'HCI.US', rank: 2, _grace_hold: true, detail: {} },
    { symbol: 'BHF.US', rank: 3, _sizing_suspended: true, _fast_track: true, suggested_shares: 0, detail: {} },
    { symbol: 'PINE.US', rank: 4, _sizing_suspended: true, _fast_track: true, suggested_shares: 0, detail: {} },
    { symbol: 'DVN.US', rank: 5, _sizing_suspended: true, _fast_track: true, suggested_shares: 0, detail: {} },
    { symbol: 'VINP.US', rank: 6, _grace_hold: true, detail: {} } ],
    kpis: { selected_count: 4, max_selected: 10 }, meta: { stability: { audit: { pending: [] } } } };
}

function runOnce() {
  var log = [], fails = 0, cases = [];
  function check(name, ok, detail) { cases.push(name + '=' + (ok ? 1 : 0)); log.push((ok ? 'PASS ' : 'FAIL ') + name + (detail ? ' :: ' + detail : '')); if (!ok) fails++; }
  var B = loadTree(BASE, { statusRows: statusRows(ML_CLEAN, GM_CLEAN) });
  var D = loadTree(DELIV, { statusRows: statusRows(ML_CLEAN, GM_CLEAN) });
  var DL = loadTree(DELIV, { statusRows: statusRows(ML_CLEAN, GM_CLEAN),
                             props: { DT10_P181B_W52_LEGACY: '1', DT10_SEAT_TRUTH_LEGACY: '1', DT10_P195_MARKER_LEGACY: '1', DT10_SYNC_INFLIGHT: 'off' } });

  /* ---------- T1 P-181b: projection on the REAL 10-04 Global_Markets header ---------- */
  var hdr = FIX.header;
  var mapB = B.dt10MapHeaderCols_(hdr), mapD = D.dt10MapHeaderCols_(hdr), mapL = DL.dt10MapHeaderCols_(hdr);
  check('T1a delivered maps the four W52 fields from the real GM header',
        mapD['52W High'] !== null && mapD['52W High'] !== undefined && mapD['52W Low'] >= 0 &&
        mapD['52W Position %'] >= 0 && mapD['Percent Change'] >= 0,
        json({ hi: mapD['52W High'], lo: mapD['52W Low'], pos: mapD['52W Position %'], chg: mapD['Percent Change'] }));
  check('T1b base never mapped them (the P-181b blindness)',
        !mapB.hasOwnProperty('52W High') && !mapB.hasOwnProperty('Percent Change'));
  check('T1c legacy kill restores the base map byte-for-byte', deepEq(mapL, mapB));
  var syms = ['RDN.US', 'NVDA.US', 'DDI.US', 'BHF.US'], superset = true, legacyEq = true, vals = {};
  for (var i = 0; i < syms.length; i++) {
    var rowB = B.dt10PoolRowFromSheetRow_(FIX.rows[syms[i]], mapB, 'Global_Markets');
    var rowD = D.dt10PoolRowFromSheetRow_(FIX.rows[syms[i]], mapD, 'Global_Markets');
    var rowL = DL.dt10PoolRowFromSheetRow_(FIX.rows[syms[i]], mapL, 'Global_Markets');
    for (var k in rowB) if (rowB.hasOwnProperty(k) && !deepEq(rowB[k], rowD[k])) superset = false;
    var extra = Object.keys(rowD).filter(function (k2) { return !rowB.hasOwnProperty(k2); }).sort();
    if (!deepEq(extra, ['52W High', '52W Low', '52W Position %', 'Percent Change'])) superset = false;
    if (!deepEq(rowL, rowB)) legacyEq = false;
    vals[syms[i]] = { pos: rowD['52W Position %'], chg: rowD['Percent Change'], hi: rowD['52W High'], lo: rowD['52W Low'] };
  }
  check('T1d delivered pool rows = base rows + exactly the four fields (RDN/NVDA/DDI/BHF)', superset, json(vals));
  check('T1e real values travel (RDN 19.39 % / NVDA 94.66 % / DDI 98.30 % / BHF 24.72 %)',
        Math.abs(vals['RDN.US'].pos - 19.391635) < 1e-6 && Math.abs(vals['NVDA.US'].pos - 94.661051) < 1e-6 &&
        Math.abs(vals['DDI.US'].pos - 98.301887) < 1e-6 && Math.abs(vals['BHF.US'].pos - 24.719605) < 1e-6 &&
        Math.abs(vals['RDN.US'].chg - 0.04324151) < 1e-9);
  check('T1f legacy kill: pool rows byte-identical to base', legacyEq);
  fs.writeFileSync('/tmp/dt10_rdn_pool_row.json', json(D.dt10PoolRowFromSheetRow_(FIX.rows['RDN.US'], mapD, 'Global_Markets')));

  /* ---------- T2 P-149: structural reason + seat-truth KPI ---------- */
  var whyB = B.dt10QualToRow_(aerCand(), 2, {}, 14, 4, {});
  var whyD = D.dt10QualToRow_(aerCand(), 2, {}, 14, 4, {});
  var whyL = DL.dt10QualToRow_(aerCand(), 2, {}, 14, 4, {});
  check('T2a base prints the rank-cut lie for held AER.US', whyB[14] === 'ranked below the Max Selected cut', whyB[14]);
  check('T2b delivered prints the structural reason', whyD[14] === 'Portfolio: held vs exclude holdings (Include Portfolio Holdings = No)', whyD[14]);
  check('T2c legacy kill restores the base text', deepEq(whyL, whyB));
  check('T2d non-structural rows unchanged (deferral text precedence)',
        deepEq(B.dt10QualToRow_({ symbol: 'AUB.US', deferral: 'Diversification: sector cap 2/2 (Financials)' }, 5, {}, 14, 4, {}),
               D.dt10QualToRow_({ symbol: 'AUB.US', deferral: 'Diversification: sector cap 2/2 (Financials)' }, 5, {}, 14, 4, {})));
  var kpi = D.dt10SeatTruthKpi_(boardPayload());
  check('T2e seat-truth KPI cell for the 10-04 board reads exec/grace truth', /^0 exec \+ 6 grace \/ 10$/.test(kpi) || /^0 exec/.test(kpi), kpi);
  check('T2f seat truth ON in delivered, OFF under the kill', D.dt10SeatTruthOn_() === true && DL.dt10SeatTruthOn_() === false);

  /* ---------- T3 SYNC-INFLIGHT core + I/O ---------- */
  var now1305 = Date.UTC(2026, 9, 3, 10, 5, 0);
  var a = D.dt10SyncInflightCore_(ML_RACE, GM_RACE, now1305, 90);
  var b = D.dt10SyncInflightCore_(ML_CLEAN, GM_CLEAN, Date.UTC(2026, 9, 4, 5, 9, 0), 90);
  var c = D.dt10SyncInflightCore_(ML_RACE, GM_RACE, Date.UTC(2026, 9, 3, 12, 0, 0), 90);
  var d = D.dt10SyncInflightCore_(GM_RACE, ML_RACE, now1305, 90);
  var e = D.dt10SyncInflightCore_('', GM_RACE, now1305, 90);
  var f = D.dt10SyncInflightCore_('SKIPPED | run=37114669145 | 2026-10-03 12:59:12+03:00', 'OK | run=37114669145 | 2026-10-03 03:43:37+03:00', now1305, 90);
  check('T3a 13:05 race => active, run 37114669145, 6 min', a.active && a.run === '37114669145' && a.firstAgeMin === 6 && a.lastRun === '37079471147', a.detail);
  check('T3b clean 10-04 => inactive (same run)', !b.active && b.detail === 'run 37160795404 complete');
  check('T3c 121 min old first leg => inactive (stale bound)', !c.active && /stale/.test(c.detail), c.detail);
  check('T3d last leg newer than first => inactive', !d.active && d.detail === 'last leg newer');
  check('T3e blank first leg => inactive, fail-open', !e.active && e.detail === 'stamps unreadable');
  check('T3f same run id across states => inactive', !f.active);
  var Dr = loadTree(DELIV, { statusRows: statusRows(ML_RACE, GM_RACE), nowMs: RACE_NOW });
  var liveObs = Dr.dt10SyncInflight_(Dr._env._ss);
  check('T3g I/O read of the race _Status (default observe) => active', liveObs.active && liveObs.mode === 'observe', json(liveObs));
  var Doff = loadTree(DELIV, { statusRows: statusRows(ML_RACE, GM_RACE), props: { DT10_SYNC_INFLIGHT: 'off' }, nowMs: RACE_NOW });
  check('T3h mode off short-circuits', !Doff.dt10SyncInflight_(Doff._env._ss).active && Doff.dt10SyncInflightMode_() === 'off');
  check('T3i stamp parser: naive = Riyadh, Z = UTC', D.dt10FeedStampParse_('OK | run=5 | 2026-10-03 03:00:00').ms === Date.UTC(2026, 9, 3, 0, 0, 0) &&
        D.dt10FeedStampParse_('OK | run=5 | 2026-10-03T00:00:00Z').ms === Date.UTC(2026, 9, 3, 0, 0, 0) && D.dt10FeedStampParse_('OK | run=5 | 2026-10-03T00:00:00Z').run === '5');

  /* ---------- T4 P-195 marker core + I/O ---------- */
  var nowM = Date.UTC(2026, 9, 3, 18, 10, 9);
  var m1 = D.dt10RunMarkerCore_('{"started":"2026-10-03T14:06:29.000Z","v":"1.11.12"}', nowM, 7);
  var m2 = D.dt10RunMarkerCore_('{"started":"2026-10-03T18:08:00.000Z","v":"1.11.12"}', nowM, 7);
  check('T4a Saturday 17:06 marker judged ABORTED at 21:10 (244 min)', m1.aborted && m1.ageMin === 244);
  check('T4b 2-minute-old marker = concurrent, not aborted', !m2.aborted && m2.ageMin === 2);
  check('T4c blank / junk => nothing', !D.dt10RunMarkerCore_('', nowM, 7).aborted && !D.dt10RunMarkerCore_('junk', nowM, 7).aborted);
  var Dm = loadTree(DELIV, { statusRows: statusRows(ML_CLEAN, GM_CLEAN), props: { DT10_RUN_MARKER: '{"started":"2026-10-03T14:06:29.000Z","v":"1.11.11"}' } });
  var judged = Dm.dt10RunMarkerCheck_('refreshDecisionTop10');
  var rl = Dm._env._sheets['_Run_Log']._appended;
  check('T4d stale marker at entry => ONE _Run_Log ABORTED_PREV row, new marker stored',
        judged && judged.aborted && rl.length === 1 && rl[0][4] === 'ABORTED_PREV' && rl[0][1] === 'WARN' &&
        /P-195/.test(rl[0][5]) && /"started"/.test(Dm._env._props.DT10_RUN_MARKER || ''), rl.length ? rl[0][5] : 'no row');
  Dm.dt10RunMarkerClear_();
  check('T4e clear removes the marker', !Dm._env._props.hasOwnProperty('DT10_RUN_MARKER'));
  var Dm2 = loadTree(DELIV, { statusRows: statusRows(ML_CLEAN, GM_CLEAN), props: { DT10_RUN_MARKER: '{"started":"2026-10-03T14:06:29.000Z"}', DT10_P195_MARKER_LEGACY: '1' } });
  check('T4f legacy kill: no row, no property writes', Dm2.dt10RunMarkerCheck_() === null && Dm2._env._sheets['_Run_Log']._appended.length === 0 &&
        Dm2._env._props.DT10_RUN_MARKER === '{"started":"2026-10-03T14:06:29.000Z"}');

  /* ---------- T5 REAL orchestrator: enforce holds before any POST; observe proceeds ---------- */
  var De = loadTree(DELIV, { statusRows: statusRows(ML_RACE, GM_RACE), props: { DT10_SYNC_INFLIGHT: 'enforce' }, nowMs: RACE_NOW });
  var threw = null; try { De.refreshDecisionTop10(); } catch (ex) { threw = String(ex); }
  var statusCell = De._env._sheets['Top_10_Investments']._val(2, 2);
  var rle = De._env._sheets['_Run_Log']._appended;
  check('T5a enforce + race: no POST, status HELD, one HELD row, marker cleared',
        threw === null && De._env._fetchCalls.length === 0 && /status: HELD/.test(statusCell) && /sync in flight/.test(statusCell) &&
        rle.length === 1 && rle[0][4] === 'HELD' && !De._env._props.hasOwnProperty('DT10_RUN_MARKER'), statusCell.slice(0, 160));
  var Do = loadTree(DELIV, { statusRows: statusRows(ML_RACE, GM_RACE), nowMs: RACE_NOW });
  threw = null; try { Do.refreshDecisionTop10(); } catch (ex) { threw = String(ex); }
  var statusO = Do._env._sheets['Top_10_Investments']._val(2, 2);
  check('T5b observe + race: run proceeds to the POST (network stub) and the marker is cleared on the error path',
        threw === null && Do._env._fetchCalls.length === 1 && /NETWORK ERROR/.test(statusO) && !Do._env._props.hasOwnProperty('DT10_RUN_MARKER'), statusO.slice(0, 120));
  var Dc = loadTree(DELIV, { statusRows: statusRows(ML_CLEAN, GM_CLEAN), props: { DT10_SYNC_INFLIGHT: 'enforce' } });
  threw = null; try { Dc.refreshDecisionTop10(); } catch (ex) { threw = String(ex); }
  var statusC = Dc._env._sheets['Top_10_Investments']._val(2, 2);
  check('T5c enforce + clean stamps: not held, proceeds to the POST', threw === null && Dc._env._fetchCalls.length === 1 && /NETWORK ERROR/.test(statusC));
  var Bo = loadTree(BASE, { statusRows: statusRows(ML_RACE, GM_RACE), nowMs: RACE_NOW });
  threw = null; try { Bo.refreshDecisionTop10(); } catch (ex) { threw = String(ex); }
  check('T5d base on the same race: posts regardless (the 13:05 defect)', threw === null && Bo._env._fetchCalls.length === 1);

  /* ---------- T6 defaults + regression ---------- */
  var defB = null, defD = null;
  for (var p = 0; p < B.DT10_PANEL.length; p++) if (B.DT10_PANEL[p].label === 'T10: Max Per Sector') defB = B.DT10_PANEL[p].def;
  for (var q = 0; q < D.DT10_PANEL.length; q++) if (D.DT10_PANEL[q].label === 'T10: Max Per Sector') defD = D.DT10_PANEL[q].def;
  check('T6a Max Per Sector built-in default 2 -> 3', defB === 2 && defD === 3);
  check('T6b DT10_VERSION 1.11.12', D.DT10_VERSION === '1.11.12' && B.DT10_VERSION === '1.11.11');
  var r1 = D.dt10StabResolveEpoch_('OK | cov=97.5 | run=36199188352 | 2026-09-26 04:18:18+03:00', '2026-09-25', '2026-09-26');
  var r2 = D.dt10StabResolveEpoch_('OK | cov=100.0 | run=36199188352 | 2026-09-26 01:59:00+03:00', '2026-09-25', '2026-09-26');
  check('T6c P-144 epoch resolver unchanged (fresh advances, aged frozen)', r1.key === '2026-09-26' && !r1.held && r2.key === '2026-09-25' && r2.held);
  check('T6d base/delivered agree on dt10StabStampUtcDate_', B.dt10StabStampUtcDate_('2026-09-26 02:59:59+03:00') === D.dt10StabStampUtcDate_('2026-09-26 02:59:59+03:00'));
  var fnRe = /^function\s+([A-Za-z0-9_]+)\s*\(/mg, fB = {}, fD = {}, mm;
  var sB = fs.readFileSync(BASE, 'utf8'), sD = fs.readFileSync(DELIV, 'utf8');
  while ((mm = fnRe.exec(sB))) fB[mm[1]] = 1;
  fnRe.lastIndex = 0;
  while ((mm = fnRe.exec(sD))) fD[mm[1]] = 1;
  var removed = Object.keys(fB).filter(function (k) { return !fD[k]; });
  check('T6e zero functions removed; twelve added', removed.length === 0 && (Object.keys(fD).length - Object.keys(fB).length) === 12, 'removed=' + json(removed));
  check('T6f ES5 only in the delta (no let/const/arrow/template code)', !/^\s*(let|const)\s/m.test(sD.split('\n').filter(function (l) { return sB.indexOf(l) === -1 && !/^\s*(\*|\/\/|\/\*)/.test(l); }).join('\n')));

  return { log: log, fails: fails, digest: crypto.createHash('sha256').update(cases.join('|')).digest('hex').slice(0, 16) };
}
var res = runOnce();
res.log.forEach(function (l) { console.log(l); });
var total = res.log.length;
console.log('[DT10 v1.11.12 HARNESS] ' + (total - res.fails) + '/' + total + ' PASS  cases-digest=' + res.digest);
process.exit(res.fails ? 1 : 0);
