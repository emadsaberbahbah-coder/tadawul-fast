// TADAWUL FAST BRIDGE - IBKR HISTORY PLUMBING v1.0.0  [Decision Pages v2 plan, package B2]
//
// Purpose
// -------
// Turn every fresh IBKR Flex capture (the JSON file 10_My_Portfolio's capture
// watcher already reads, schema tfb.ibkr-capture/v1) into two durable history
// tabs, so the Portfolio_Decision page can show a time-weighted return line
// and a trade-record bar chart that are computed from broker data, not from
// the workbook's own estimates:
//   _IBKR_NAV_History  one row per capture DATE (idempotent on the date):
//                      date, NAV in the base currency, base ccy, net
//                      deposits/withdrawals that day (Cash Report), daily
//                      return r = NAV1 / (NAV0 - flows) - 1, and the
//                      chain-linked TWR (1+TWR1) = (1+TWR0) * (1+r) - 1.
//   _IBKR_Trades       one row per FILL (idempotent on the trade id): id,
//                      date, symbol, side, qty, price, commission, realized
//                      P&L, currency, order type, capture, plus two helper
//                      columns Gain / Loss that feed the bar chart.
// Both tabs are append-only facts. Nothing here touches the request body,
// the decision render, any gate or any Script Property; the only caller is a
// 3-line hook in 10_My_Portfolio pdIbkrCaptureWatch (v1.5.17) that runs
// after a NEW capture file is seen and before the decision refresh.
//
// Capture sections (plan C3 adds them to the Flex query; absent sections are
// simply skipped and the status line says so):
//   positions / balances / orders   already present (v1.5.12 contract)
//   net_asset_value | nav           {total|nav, currency|base_currency,
//                                    report_date|date|asof}  or an array of
//                                    such rows (the latest report date wins)
//   cash_report | cash              [{currency, deposits_withdrawals |
//                                    depositWithdrawals, report_date}]
//   trades                          [{trade_id|tradeID|exec_id,
//                                    trade_date|tradeDate|date, symbol,
//                                    side|buySell, quantity|qty,
//                                    price|tradePrice, commission|
//                                    ibCommission, realized_pnl|
//                                    fifoPnlRealized|pnl, currency,
//                                    order_type|orderType}]
// Field names accept both the snake_case the capture tool writes and the
// camelCase names of the IBKR Flex XML/JSON, so the C3 query change needs no
// renaming step.
//
// Statistics (ibkrHistoryStats_, PURE, 90-day window by default): fills are
// grouped into CLOSED TRADES by symbol + sell date (several partial fills on
// one day = one closed trade, the PFLT case); a closed trade is a win when its
// realized P&L > 0; stop-out = any fill of the trade carries order type STP /
// STP LMT / TRAIL. Reported: n, wins, losses, win rate %, avg win, avg loss,
// payoff (avg win / |avg loss|), expectancy per trade, stop-out count and
// share, net P&L. Validation data: tests/fixtures/
// ibkr_closed_trades_90d_2026-10-17.csv (15 closed names, net -809.08 USD,
// 3 wins / 12 losses, avg win 278.61, avg loss -137.08, payoff 2.03,
// expectancy -53.94, 6 stop-outs) - pinned by ibkrHistorySelfTest_ and
// tests/js/ibkr_history_v100.test.js.
//
// Charts (for the Portfolio_Decision v2 renderer, plan A3, to call):
//   ibkrHistoryTwrChart_(sheet, row, col)    line chart, TWR % by date,
//                                            1 series #2a78d6, 2 px
//   ibkrHistoryTradeChart_(sheet, row, col)  column chart, Gain / Loss per
//                                            fill, #2a78d6 / #e34948
// Both are idempotent per anchor (an existing chart of this module at the same
// anchor is replaced) and fail-open: a chart fault never breaks a render.
//
// Entry points
//   ibkrHistoryRecord_(cap, captureId)   the hook target; never throws
//   ibkrHistoryStatus_()                 one status line (pdSelfTest / menu)
//   ibkrHistorySelfTest_()               pure checks + the live tab summary
// ES5 only (V8 features stay out of this file, per the project rule).
// -----------------------------------------------------------------------------

var IH_VERSION = '1.0.0';
var IH_NAV_SHEET = '_IBKR_NAV_History';
var IH_TRADES_SHEET = '_IBKR_Trades';
var IH_WINDOW_DAYS = 90;
var IH_MAX_TRADES = 5000;        /* fills read per capture (bounded) */
var IH_COLOR_GAIN = '#2a78d6';
var IH_COLOR_LOSS = '#e34948';
var IH_CHART_TAG = 'TFB_IBKR_HISTORY';
var IH_STOP_RE = /^(STP|STOP|TRAIL)/i;
var IH_NAV_HEADERS = ['Date', 'NAV', 'Base Ccy', 'Deposits/Withdrawals',
  'Daily Return', 'TWR (chain)', 'Capture', 'Recorded At', 'Source'];
var IH_TRADE_HEADERS = ['Trade ID', 'Date', 'Symbol', 'Side', 'Qty', 'Price',
  'Commission', 'Realized P&L', 'Currency', 'Order Type', 'Capture', 'Gain',
  'Loss'];

/* ----- pure helpers ---------------------------------------------------------- */

function ihIsArray_(v) {
  return Object.prototype.toString.call(v) === '[object Array]';
}
function ihStr_(v) {
  return String(v === null || v === undefined ? '' : v).replace(/^\s+|\s+$/g, '');
}
/** First present key of the keys list on obj -> its value, else undefined. */
function ihPick_(obj, keys) {
  if (!obj || typeof obj !== 'object') return undefined;
  for (var i = 0; i < keys.length; i++) {
    if (Object.prototype.hasOwnProperty.call(obj, keys[i]) &&
        obj[keys[i]] !== null && obj[keys[i]] !== undefined &&
        String(obj[keys[i]]) !== '') return obj[keys[i]];
  }
  return undefined;
}
/** Finite number from a number or a plain decimal string ('1,234.5' ok). */
function ihNum_(v) {
  if (typeof v === 'boolean' || v === null || v === undefined) return null;
  if (typeof v === 'number') return isFinite(v) ? v : null;
  var s = String(v).replace(/,/g, '').replace(/^\s+|\s+$/g, '');
  if (!s || !/^[+-]?\d*\.?\d+(e[+-]?\d+)?$/i.test(s)) return null;
  var n = Number(s);
  return isFinite(n) ? n : null;
}
/** 'yyyy-MM-dd' from a Date, an ISO stamp, 'yyyyMMdd' (Flex) or
 * 'yyyyMMdd;HHmmss'; '' when unreadable. Dates keep their UTC calendar day. */
function ihDay_(v) {
  if (Object.prototype.toString.call(v) === '[object Date]') {
    return isFinite(v.getTime()) ? v.toISOString().slice(0, 10) : '';
  }
  var s = ihStr_(v);
  var m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) m = s.match(/^(\d{4})(\d{2})(\d{2})(?:;|T|$)/);
  if (!m) return '';
  var d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
  if (!isFinite(d.getTime()) || d.toISOString().slice(0, 10) !== m[1] + '-' + m[2] + '-' + m[3]) return '';
  return m[1] + '-' + m[2] + '-' + m[3];
}
function ihDayMs_(day) {
  var m = String(day || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  return m ? Date.UTC(+m[1], +m[2] - 1, +m[3]) : NaN;
}
function ihRound_(v, digits) {
  var p = Math.pow(10, digits);
  return Math.round(v * p) / p;
}
function ihList_(cap, keys, inner) {
  var v = ihPick_(cap, keys);
  if (v === undefined) return null;
  if (ihIsArray_(v)) return v;
  if (v && typeof v === 'object') {
    for (var i = 0; i < inner.length; i++) {
      if (ihIsArray_(v[inner[i]])) return v[inner[i]];
    }
    return [v];
  }
  return null;
}

/** PURE: the NAV entry of a capture -> {day, nav, ccy, flows} or null with a
 * reason. The latest report date wins; flows are the Cash Report
 * deposits/withdrawals for that day in the base currency (BASE_SUMMARY rows
 * or the NAV currency), summed; absent Cash Report -> flows 0 and a note. */
function ihExtractNav_(cap) {
  var out = { ok: false, reason: '', day: '', nav: null, ccy: '', flows: 0,
              flowsKnown: false };
  if (!cap || typeof cap !== 'object') { out.reason = 'no capture'; return out; }
  var navRows = ihList_(cap, ['net_asset_value', 'nav', 'equity_summary'],
                        ['rows', 'items', 'EquitySummaryByReportDateInBase']);
  if (!navRows || !navRows.length) { out.reason = 'no NAV section'; return out; }
  var best = null;
  for (var i = 0; i < navRows.length; i++) {
    var r = navRows[i] || {};
    var day = ihDay_(ihPick_(r, ['report_date', 'reportDate', 'date', 'asof', 'as_of']) ||
                     ihPick_(cap, ['captured_at']));
    var nav = ihNum_(ihPick_(r, ['total', 'nav', 'net_asset_value', 'total_nav', 'totalNav', 'value']));
    if (!day || nav === null || nav < 0) continue;
    if (!best || day > best.day) {
      best = { day: day, nav: nav,
               ccy: ihStr_(ihPick_(r, ['currency', 'base_currency', 'baseCurrency']) ||
                           ihPick_(cap, ['base_currency', 'baseCurrency']) || 'USD').toUpperCase() };
    }
  }
  if (!best) { out.reason = 'NAV rows unreadable'; return out; }
  out.day = best.day; out.nav = best.nav; out.ccy = best.ccy;
  var cashRows = ihList_(cap, ['cash_report', 'cash', 'cash_transactions'],
                         ['rows', 'items', 'CashReportCurrency']);
  if (cashRows) {
    var sum = 0, seen = false;
    for (var c = 0; c < cashRows.length; c++) {
      var cr = cashRows[c] || {};
      var ccy = ihStr_(ihPick_(cr, ['currency']) || '').toUpperCase();
      if (ccy && ccy !== 'BASE_SUMMARY' && ccy !== 'BASE' && ccy !== out.ccy) continue;
      var d = ihDay_(ihPick_(cr, ['report_date', 'reportDate', 'date']));
      if (d && d !== out.day) continue;
      var f = ihNum_(ihPick_(cr, ['deposits_withdrawals', 'depositWithdrawals',
                                  'deposits_and_withdrawals', 'net_flows', 'flows']));
      if (f === null) continue;
      sum += f; seen = true;
    }
    out.flows = seen ? sum : 0;
    out.flowsKnown = seen;
  }
  out.ok = true;
  return out;
}

/** PURE: the fills of a capture -> [{id, day, symbol, side, qty, price,
 * commission, pnl, ccy, orderType}] (unreadable fills skipped, bounded). */
function ihExtractTrades_(cap) {
  var rows = ihList_(cap, ['trades', 'fills', 'executions'], ['rows', 'items', 'Trade']);
  var out = [];
  if (!rows) return out;
  for (var i = 0; i < rows.length && out.length < IH_MAX_TRADES; i++) {
    var t = rows[i] || {};
    var id = ihStr_(ihPick_(t, ['trade_id', 'tradeID', 'tradeId', 'exec_id', 'execId', 'ibExecID', 'id']));
    var day = ihDay_(ihPick_(t, ['trade_date', 'tradeDate', 'date', 'dateTime', 'reportDate']));
    var sym = ihStr_(ihPick_(t, ['symbol', 'contract_description', 'description'])).toUpperCase();
    if (!id || !day || !sym) continue;
    var side = ihStr_(ihPick_(t, ['side', 'buySell', 'buy_sell']) || '').toUpperCase();
    if (side !== 'BUY' && side !== 'SELL') {
      var q0 = ihNum_(ihPick_(t, ['quantity', 'qty']));
      side = (q0 !== null && q0 < 0) ? 'SELL' : (q0 !== null && q0 > 0 ? 'BUY' : '');
    }
    var qty = ihNum_(ihPick_(t, ['quantity', 'qty', 'shares']));
    out.push({
      id: id, day: day, symbol: sym, side: side,
      qty: qty === null ? null : Math.abs(qty),
      price: ihNum_(ihPick_(t, ['price', 'tradePrice', 'trade_price'])),
      commission: ihNum_(ihPick_(t, ['commission', 'ibCommission', 'ib_commission'])),
      pnl: ihNum_(ihPick_(t, ['realized_pnl', 'fifoPnlRealized', 'fifo_pnl_realized', 'pnl', 'realizedPnl'])),
      ccy: ihStr_(ihPick_(t, ['currency']) || '').toUpperCase(),
      orderType: ihStr_(ihPick_(t, ['order_type', 'orderType']) || '').toUpperCase()
    });
  }
  return out;
}

/** PURE: the next NAV row given the existing rows (header excluded, oldest
 * first) and the new entry. {row, skipped, reason}. Idempotent on the day:
 * an existing row for the same day is kept as is. Daily return uses the
 * previous row's NAV adjusted for this day's flows; the first row is the
 * chain's base (r = 0, TWR = 0). */
function ihNavAppend_(existingRows, entry, captureId, nowIso) {
  var out = { row: null, skipped: false, reason: '' };
  if (!entry || !entry.ok) { out.skipped = true; out.reason = (entry && entry.reason) || 'no NAV'; return out; }
  var prev = null;
  for (var i = 0; i < (existingRows || []).length; i++) {
    var r = existingRows[i] || [];
    var d = ihDay_(r[0]);
    if (!d) continue;
    if (d === entry.day) { out.skipped = true; out.reason = 'day already recorded'; return out; }
    if (d < entry.day && (!prev || d > prev.day)) {
      prev = { day: d, nav: ihNum_(r[1]), twr: ihNum_(r[5]) };
    }
  }
  var ret = 0, twr = 0;
  if (prev && prev.nav !== null) {
    var base = prev.nav + (entry.flows || 0);
    ret = base > 0 ? entry.nav / base - 1 : 0;
    twr = (1 + (prev.twr || 0)) * (1 + ret) - 1;
  }
  out.row = [entry.day, entry.nav, entry.ccy, entry.flowsKnown ? entry.flows : '',
             ihRound_(ret, 6), ihRound_(twr, 6), String(captureId || ''),
             String(nowIso || ''), 'ibkr-capture'];
  return out;
}

/** PURE: rows for fills not yet recorded (by trade id). */
function ihTradesAppend_(existingIds, trades, captureId) {
  var seen = {};
  for (var i = 0; i < (existingIds || []).length; i++) seen[String(existingIds[i])] = true;
  var rows = [];
  for (var t = 0; t < (trades || []).length; t++) {
    var tr = trades[t];
    if (!tr || !tr.id || seen[tr.id]) continue;
    seen[tr.id] = true;
    var pnl = tr.pnl === null || tr.pnl === undefined ? null : tr.pnl;
    rows.push([tr.id, tr.day, tr.symbol, tr.side,
               tr.qty === null ? '' : tr.qty, tr.price === null ? '' : tr.price,
               tr.commission === null ? '' : tr.commission, pnl === null ? '' : pnl,
               tr.ccy, tr.orderType, String(captureId || ''),
               pnl !== null && pnl > 0 ? pnl : '', pnl !== null && pnl < 0 ? pnl : '']);
  }
  return rows;
}

/** PURE: closed trades from fill records [{day, symbol, side, pnl,
 * orderType}] -> [{symbol, day, pnl, stop}] grouped by symbol + day over
 * fills that carry a realized P&L (sell side or non-zero P&L). */
function ihClosedTrades_(records) {
  var groups = {}, order = [];
  for (var i = 0; i < (records || []).length; i++) {
    var r = records[i] || {};
    var pnl = ihNum_(r.pnl);
    if (pnl === null) continue;
    if (String(r.side || '').toUpperCase() !== 'SELL' && pnl === 0) continue;
    var key = String(r.symbol || '').toUpperCase() + '|' + ihDay_(r.day);
    if (!groups[key]) {
      groups[key] = { symbol: String(r.symbol || '').toUpperCase(), day: ihDay_(r.day), pnl: 0, stop: false };
      order.push(key);
    }
    groups[key].pnl += pnl;
    if (IH_STOP_RE.test(String(r.orderType || ''))) groups[key].stop = true;
  }
  var out = [];
  for (var k = 0; k < order.length; k++) out.push(groups[order[k]]);
  return out;
}

/** PURE: trade-record statistics over the window ending nowMs (inclusive). */
function ihStats_(records, nowMs, windowDays) {
  var days = windowDays > 0 ? windowDays : IH_WINDOW_DAYS;
  var endMs = typeof nowMs === 'number' && isFinite(nowMs) ? nowMs : Date.now();
  var startMs = endMs - days * 86400000;
  var closed = ihClosedTrades_(records);
  var st = { n: 0, wins: 0, losses: 0, winRatePct: null, avgWin: null, avgLoss: null,
             payoff: null, expectancy: null, stopOuts: 0, stopSharePct: null, netPnl: 0,
             windowDays: days };
  var winSum = 0, lossSum = 0;
  for (var i = 0; i < closed.length; i++) {
    var ms = ihDayMs_(closed[i].day);
    if (!isFinite(ms) || ms < startMs || ms > endMs) continue;
    st.n++;
    st.netPnl += closed[i].pnl;
    if (closed[i].pnl > 0) { st.wins++; winSum += closed[i].pnl; }
    else { st.losses++; lossSum += closed[i].pnl; }
    if (closed[i].stop) st.stopOuts++;
  }
  if (st.n) {
    st.winRatePct = ihRound_(100 * st.wins / st.n, 1);
    st.expectancy = ihRound_(st.netPnl / st.n, 2);
    st.stopSharePct = ihRound_(100 * st.stopOuts / st.n, 1);
  }
  if (st.wins) st.avgWin = ihRound_(winSum / st.wins, 2);
  if (st.losses) st.avgLoss = ihRound_(lossSum / st.losses, 2);
  if (st.avgWin !== null && st.avgLoss !== null && st.avgLoss !== 0) {
    st.payoff = ihRound_(st.avgWin / Math.abs(st.avgLoss), 2);
  }
  st.netPnl = ihRound_(st.netPnl, 2);
  return st;
}

/** PURE: one line of statistics text. */
function ihStatsText_(st) {
  if (!st || !st.n) return 'no closed trades in the last ' + ((st && st.windowDays) || IH_WINDOW_DAYS) + 'd';
  return st.n + ' closed (' + st.wins + 'W/' + st.losses + 'L) · win rate ' + st.winRatePct +
      '% · avg win ' + (st.avgWin === null ? 'n/a' : (st.avgWin > 0 ? '+' : '') + st.avgWin) +
      ' · avg loss ' + (st.avgLoss === null ? 'n/a' : st.avgLoss) +
      ' · payoff ' + (st.payoff === null ? 'n/a' : st.payoff) +
      ' · expectancy ' + (st.expectancy > 0 ? '+' : '') + st.expectancy + '/trade' +
      ' · ' + st.stopOuts + ' stop-outs (' + st.stopSharePct + '%)' +
      ' · net ' + (st.netPnl > 0 ? '+' : '') + st.netPnl;
}

/* ----- sheet I/O (never throws to the caller) --------------------------------- */

function ihSheet_(ss, name, headers) {
  var sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
    sh.getRange(1, 1, 1, headers.length).setValues([headers]).setFontWeight('bold');
    sh.setFrozenRows(1);
    try { sh.hideSheet(); } catch (eHide) {}
    return sh;
  }
  if (sh.getMaxColumns() < headers.length) {
    sh.insertColumnsAfter(sh.getMaxColumns(), headers.length - sh.getMaxColumns());
  }
  var hdr = sh.getRange(1, 1, 1, headers.length).getValues()[0];
  for (var i = 0; i < headers.length; i++) {
    if (ihStr_(hdr[i]) === '') sh.getRange(1, i + 1).setValue(headers[i]).setFontWeight('bold');
  }
  return sh;
}
function ihRows_(sh, width) {
  var last = sh.getLastRow();
  if (last < 2) return [];
  return sh.getRange(2, 1, last - 1, width).getValues();
}
/** Fill records from the _IBKR_Trades rows (for the statistics). */
function ihRecordsFromRows_(rows) {
  var out = [];
  for (var i = 0; i < (rows || []).length; i++) {
    var r = rows[i] || [];
    if (!ihStr_(r[0])) continue;
    out.push({ id: ihStr_(r[0]), day: ihDay_(r[1]), symbol: ihStr_(r[2]), side: ihStr_(r[3]),
               pnl: ihNum_(r[7]), orderType: ihStr_(r[9]) });
  }
  return out;
}

/** The hook target: record a capture's NAV day and new fills. Returns a
 * short note ('nav +1, trades +12' / 'nav skipped: day already recorded,
 * trades +0' / 'error: <message>'); never throws. */
function ibkrHistoryRecord_(cap, captureId) {
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var nowIso = new Date().toISOString();
    var navSh = ihSheet_(ss, IH_NAV_SHEET, IH_NAV_HEADERS);
    var navRes = ihNavAppend_(ihRows_(navSh, IH_NAV_HEADERS.length), ihExtractNav_(cap), captureId, nowIso);
    if (navRes.row) {
      navSh.getRange(navSh.getLastRow() + 1, 1, 1, IH_NAV_HEADERS.length).setValues([navRes.row]);
      navSh.getRange(navSh.getLastRow(), 5, 1, 2).setNumberFormat('0.0000%');
    }
    var trSh = ihSheet_(ss, IH_TRADES_SHEET, IH_TRADE_HEADERS);
    var existing = ihRows_(trSh, 1), ids = [];
    for (var i = 0; i < existing.length; i++) ids.push(ihStr_(existing[i][0]));
    var trRows = ihTradesAppend_(ids, ihExtractTrades_(cap), captureId);
    if (trRows.length) {
      trSh.getRange(trSh.getLastRow() + 1, 1, trRows.length, IH_TRADE_HEADERS.length).setValues(trRows);
    }
    var note = 'nav ' + (navRes.row ? '+1 (' + navRes.row[0] + ')' : 'skipped: ' + navRes.reason) +
        ', trades +' + trRows.length;
    Logger.log('[IBKR-HIST v' + IH_VERSION + '] ' + note);
    return note;
  } catch (e) {
    var msg = 'error: ' + String(e && e.message ? e.message : e).slice(0, 120);
    try { Logger.log('[IBKR-HIST v' + IH_VERSION + '] ' + msg); } catch (eL) {}
    return msg;
  }
}

/** One status line for pdSelfTest / the menu. Never throws. */
function ibkrHistoryStatus_() {
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var navSh = ss.getSheetByName(IH_NAV_SHEET), trSh = ss.getSheetByName(IH_TRADES_SHEET);
    var navRows = navSh ? ihRows_(navSh, IH_NAV_HEADERS.length) : [];
    var last = navRows.length ? navRows[navRows.length - 1] : null;
    var trRows = trSh ? ihRows_(trSh, IH_TRADE_HEADERS.length) : [];
    var st = ihStats_(ihRecordsFromRows_(trRows), Date.now(), IH_WINDOW_DAYS);
    return 'IBKR history v' + IH_VERSION + ': NAV rows ' + navRows.length +
        (last ? ' (last ' + ihDay_(last[0]) + ', TWR ' + ihRound_(100 * (ihNum_(last[5]) || 0), 2) + '%)' : '') +
        ' · fills ' + trRows.length + ' · ' + ihStatsText_(st);
  } catch (e) {
    return 'IBKR history v' + IH_VERSION + ': unavailable (' +
        String(e && e.message ? e.message : e).slice(0, 80) + ')';
  }
}

/* ----- charts (for the Portfolio_Decision v2 renderer) ------------------------ */

function ihRemoveCharts_(sheet, tag) {
  try {
    var charts = sheet.getCharts();
    for (var i = 0; i < charts.length; i++) {
      var opt = '';
      try { opt = String(charts[i].getOptions().get('title') || ''); } catch (eO) {}
      if (opt.indexOf(tag) === 0) sheet.removeChart(charts[i]);
    }
  } catch (e) {}
}
/** TWR line chart anchored at (row, col) of the given sheet; data from
 * _IBKR_NAV_History A:F. Returns true when a chart was inserted. */
function ibkrHistoryTwrChart_(sheet, row, col) {
  try {
    var ss = (sheet && typeof sheet.getParent === 'function') ? sheet.getParent()
        : SpreadsheetApp.getActiveSpreadsheet();
    var nav = ss.getSheetByName(IH_NAV_SHEET);
    if (!nav || nav.getLastRow() < 2) return false;
    var title = IH_CHART_TAG + ' TWR — chain-linked, ' + IH_WINDOW_DAYS + 'd';
    ihRemoveCharts_(sheet, title);
    var n = nav.getLastRow();
    var chart = sheet.newChart().asLineChart()
        .addRange(nav.getRange(1, 1, n, 1))
        .addRange(nav.getRange(1, 6, n, 1))
        .setNumHeaders(1)
        .setOption('title', title)
        .setOption('colors', [IH_COLOR_GAIN])
        .setOption('lineWidth', 2)
        .setOption('legend', { position: 'none' })
        .setOption('vAxis', { format: '0.0%' })
        .setPosition(row, col, 0, 0)
        .build();
    sheet.insertChart(chart);
    return true;
  } catch (e) {
    try { Logger.log('[IBKR-HIST v' + IH_VERSION + '] TWR chart skipped: ' + e); } catch (eL) {}
    return false;
  }
}
/** Gain / Loss column chart anchored at (row, col); data from _IBKR_Trades
 * B (date), L (gain), M (loss). Returns true when inserted. */
function ibkrHistoryTradeChart_(sheet, row, col) {
  try {
    var ss = (sheet && typeof sheet.getParent === 'function') ? sheet.getParent()
        : SpreadsheetApp.getActiveSpreadsheet();
    var tr = ss.getSheetByName(IH_TRADES_SHEET);
    if (!tr || tr.getLastRow() < 2) return false;
    var title = IH_CHART_TAG + ' Trade record — realized P&L per fill';
    ihRemoveCharts_(sheet, title);
    var n = tr.getLastRow();
    var chart = sheet.newChart().asColumnChart()
        .addRange(tr.getRange(1, 2, n, 1))
        .addRange(tr.getRange(1, 12, n, 2))
        .setNumHeaders(1)
        .setOption('title', title)
        .setOption('colors', [IH_COLOR_GAIN, IH_COLOR_LOSS])
        .setOption('isStacked', true)
        .setOption('legend', { position: 'bottom' })
        .setPosition(row, col, 0, 0)
        .build();
    sheet.insertChart(chart);
    return true;
  } catch (e) {
    try { Logger.log('[IBKR-HIST v' + IH_VERSION + '] trade chart skipped: ' + e); } catch (eL) {}
    return false;
  }
}

/* ----- self-test --------------------------------------------------------------- */

/** The 15 closed names of IBKR_Closed_Trades_90d_2026-10-17.csv as fill
 * records (one SELL fill per closed name; 'stop' rows carry order type STP). */
function ihFixtureRecords_() {
  var rows = [
    ['SHG', '2026-09-03', 693.32, 'LMT'], ['T82U', '2026-08-21', 82.90, 'LMT'],
    ['PFLT', '2026-08-10', 59.60, 'LMT'], ['CTO', '2026-08-06', -6.28, 'MKT'],
    ['PFS', '2026-09-08', -6.65, 'MKT'], ['VEL', '2026-09-16', -29.30, 'STP'],
    ['ARCO', '2026-08-07', -76.37, 'MKT'], ['AER', '2026-10-08', -111.54, 'STP'],
    ['HCI', '2026-09-08', -156.38, 'MKT'], ['EPRT', '2026-09-11', -168.08, 'STP'],
    ['UVV', '2026-08-07', -188.78, 'MKT'], ['CARE', '2026-09-29', -192.58, 'STP'],
    ['OTIS', '2026-09-10', -208.63, 'LMT'], ['YUM', '2026-10-01', -210.98, 'STP'],
    ['SBAC', '2026-09-24', -289.33, 'STP']];
  var out = [];
  for (var i = 0; i < rows.length; i++) {
    out.push({ id: 'fx' + i, day: rows[i][1], symbol: rows[i][0], side: 'SELL',
               pnl: rows[i][2], orderType: rows[i][3] });
  }
  return out;
}
/** PURE core checks; returns the list of failures (empty = ok). */
function ihCoreChecks_() {
  var bad = [];
  var st = ihStats_(ihFixtureRecords_(), Date.UTC(2026, 9, 17), 90);
  if (!(st.n === 15 && st.wins === 3 && st.losses === 12 && st.winRatePct === 20 &&
        st.avgWin === 278.61 && st.avgLoss === -137.08 && st.payoff === 2.03 &&
        st.expectancy === -53.94 && st.stopOuts === 6 && st.netPnl === -809.08)) {
    bad.push('stats vs CSV: ' + ihStatsText_(st));
  }
  var split = ihClosedTrades_([
    { day: '2026-08-10', symbol: 'PFLT', side: 'SELL', pnl: 30, orderType: 'LMT' },
    { day: '2026-08-10', symbol: 'PFLT', side: 'SELL', pnl: 29.6, orderType: 'LMT' },
    { day: '2026-08-06', symbol: 'PFLT', side: 'BUY', pnl: 0, orderType: 'LMT' }]);
  if (!(split.length === 1 && Math.abs(split[0].pnl - 59.6) < 1e-9)) bad.push('partial fills not grouped');
  var nav1 = ihNavAppend_([], { ok: true, day: '2026-10-16', nav: 10000, ccy: 'USD', flows: 0, flowsKnown: true }, 'c1', 'now');
  var nav2 = ihNavAppend_([nav1.row], { ok: true, day: '2026-10-17', nav: 10600, ccy: 'USD', flows: 500, flowsKnown: true }, 'c2', 'now');
  var nav3 = ihNavAppend_([nav1.row, nav2.row], { ok: true, day: '2026-10-17', nav: 1, ccy: 'USD', flows: 0, flowsKnown: true }, 'c3', 'now');
  if (!(nav1.row && nav1.row[4] === 0 && nav1.row[5] === 0 &&
        nav2.row && Math.abs(nav2.row[4] - 0.009524) < 1e-6 && Math.abs(nav2.row[5] - 0.009524) < 1e-6 &&
        nav3.skipped && nav3.reason === 'day already recorded')) {
    bad.push('nav chain');
  }
  var ex = ihExtractNav_({ captured_at: '2026-10-17T06:00:00Z',
    net_asset_value: { total: '46296.26', currency: 'USD', report_date: '20261016' },
    cash_report: [{ currency: 'BASE_SUMMARY', depositWithdrawals: '1000', reportDate: '20261016' },
                  { currency: 'SGD', depositWithdrawals: '5', reportDate: '20261016' }] });
  if (!(ex.ok && ex.day === '2026-10-16' && ex.nav === 46296.26 && ex.ccy === 'USD' && ex.flows === 1000 && ex.flowsKnown)) {
    bad.push('nav extract');
  }
  var tr = ihExtractTrades_({ trades: [
    { tradeID: '1', tradeDate: '20260903', symbol: 'SHG', buySell: 'SELL', quantity: '-70', tradePrice: '84.34',
      ibCommission: '-1.0', fifoPnlRealized: '693.32', currency: 'USD', orderType: 'LMT' },
    { trade_id: '2', trade_date: '2026-09-16', symbol: 'VEL', side: 'SELL', quantity: -24, price: 16.97,
      commission: -1, realized_pnl: -29.3, currency: 'USD', order_type: 'STP' },
    { tradeID: '', tradeDate: '20260903', symbol: 'X' }] });
  if (!(tr.length === 2 && tr[0].qty === 70 && tr[0].pnl === 693.32 && tr[0].day === '2026-09-03' &&
        tr[1].orderType === 'STP' && tr[1].side === 'SELL')) {
    bad.push('trade extract');
  }
  var app = ihTradesAppend_(['1'], tr, 'cap');
  if (!(app.length === 1 && app[0][0] === '2' && app[0][12] === -29.3 && app[0][11] === '')) bad.push('trade append dedupe');
  return bad;
}
/** Editor-run: pure checks + the live tabs' summary. */
function ibkrHistorySelfTest_() {
  var bad = ihCoreChecks_();
  var msg = '[IBKR-HIST v' + IH_VERSION + '] core: ' + (bad.length ? 'FAIL ' + bad.join(' | ') : 'ok (CSV 2026-10-17 stats, chain, extract, dedupe)') +
      '\n' + ibkrHistoryStatus_();
  Logger.log(msg);
  try { SpreadsheetApp.getUi().alert(msg); } catch (e) {}
  return msg;
}
