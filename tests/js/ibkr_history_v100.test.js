/* 26_IBKR_History.gs v1.0.0 + 10_My_Portfolio.gs v1.5.17 hook (2026-10-17) --
 * Decision Pages v2 plan, package B2. Validates the closed-trade statistics
 * against tests/fixtures/ibkr_closed_trades_90d_2026-10-17.csv (parsed here,
 * not the module's embedded fixture), the NAV / TWR chain, the capture-section
 * extractors (snake_case and Flex camelCase), idempotence, the hook placement
 * and the ES5 guard. Run: node tests/js/ibkr_history_v100.test.js */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const ROOT = path.join(__dirname, '../..');
const source = fs.readFileSync(path.join(ROOT, 'apps_script/26_IBKR_History.gs'), 'utf8');
const pfSource = fs.readFileSync(path.join(ROOT, 'apps_script/10_My_Portfolio.gs'), 'utf8');
const plain = v => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
const csv = fs.readFileSync(path.join(ROOT, 'tests/fixtures/ibkr_closed_trades_90d_2026-10-17.csv'), 'utf8');
let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + (e && e.stack || e)); }
}

/* Minimal sheet stub: rows[] of arrays, getRange(r,c,n,w) over them. */
function fakeSheet(name, initialRows) {
  const rows = initialRows || [];
  const charts = [];
  const sh = {
    name, rows, charts, hidden: false, frozen: 0,
    getName() { return name; }, getLastRow() { return rows.length; }, getMaxColumns() { return 20; },
    insertColumnsAfter() {}, setFrozenRows(n) { sh.frozen = n; }, hideSheet() { sh.hidden = true; },
    getCharts() { return charts.slice(); }, removeChart(c) { const i = charts.indexOf(c); if (i >= 0) charts.splice(i, 1); },
    insertChart(c) { charts.push(c); },
    newChart() {
      const opts = {}; const b = {
        asLineChart() { b.kind = 'line'; return b; }, asColumnChart() { b.kind = 'column'; return b; },
        addRange(r) { (b.ranges = b.ranges || []).push(r.a1); return b; }, setNumHeaders() { return b; },
        setOption(k, v) { opts[k] = v; return b; }, setPosition(r, c) { b.pos = [r, c]; return b; },
        build() { return { kind: b.kind, ranges: b.ranges, pos: b.pos, getOptions() { return { get(k) { return opts[k]; } }; } }; }
      }; return b;
    },
    getRange(r, c, n, w) {
      n = n || 1; w = w || 1;
      return {
        a1: name + '!' + r + ',' + c + ',' + n + ',' + w,
        getValues() { const out = []; for (let i = 0; i < n; i++) { const row = rows[r - 1 + i] || []; out.push(Array.from({ length: w }, (_, j) => (row[c - 1 + j] === undefined ? '' : row[c - 1 + j]))); } return out; },
        setValues(vals) { for (let i = 0; i < vals.length; i++) { rows[r - 1 + i] = rows[r - 1 + i] || []; for (let j = 0; j < vals[i].length; j++) rows[r - 1 + i][c - 1 + j] = vals[i][j]; } return this; },
        setValue(v) { return this.setValues([[v]]); }, setFontWeight() { return this; }, setNumberFormat() { return this; }
      };
    }
  };
  return sh;
}
function load(sheets) {
  const book = sheets || {};
  const ss = {
    getSheetByName(n) { return book[n] || null; },
    insertSheet(n) { book[n] = fakeSheet(n, []); return book[n]; }
  };
  const ctx = { console, Logger: { log() {} }, Date, SpreadsheetApp: { getActiveSpreadsheet() { return ss; }, getUi() { throw new Error('no ui'); } } };
  vm.createContext(ctx);
  vm.runInContext(source, ctx, { filename: '26_IBKR_History.gs' });
  ctx.__book = book; ctx.__ss = ss;
  return ctx;
}
function csvRecords() {
  const lines = csv.trim().split('\n'); const head = lines[0].split(',');
  const recs = [];
  for (const line of lines.slice(1)) {
    const cells = line.split(','); const r = {}; head.forEach((h, i) => { r[h] = cells[i]; });
    if (r.symbol === 'TOTAL' || r.symbol === 'OPEN') continue;
    recs.push({ id: 'csv-' + r.symbol, day: r.sell_date, symbol: r.symbol, side: 'SELL', pnl: Number(r.realized_pnl_usd), orderType: r.exit_type === 'stop' ? 'STP' : 'LMT' });
  }
  return recs;
}

test('V1 version, headers, entry points and the module self-check pass', () => {
  const ctx = load();
  assert.equal(ctx.IH_VERSION, '1.0.0');
  assert.equal(ctx.IH_NAV_HEADERS.length, 9); assert.equal(ctx.IH_TRADE_HEADERS.length, 13);
  for (const f of ['ibkrHistoryRecord_', 'ibkrHistoryStatus_', 'ibkrHistorySelfTest_', 'ibkrHistoryTwrChart_', 'ibkrHistoryTradeChart_', 'ihStats_', 'ihNavAppend_']) assert.equal(typeof ctx[f], 'function', f);
  assert.deepEqual(plain(ctx.ihCoreChecks_()), []);
});
test('S1 statistics over the real CSV (15 closed names, 90-day window to 2026-10-17) match the CSV TOTAL row', () => {
  const ctx = load();
  const recs = csvRecords(); assert.equal(recs.length, 15);
  const st = ctx.ihStats_(recs, Date.UTC(2026, 9, 17), 90);
  assert.equal(st.n, 15); assert.equal(st.wins, 3); assert.equal(st.losses, 12);
  assert.equal(st.winRatePct, 20); assert.equal(st.avgWin, 278.61); assert.equal(st.avgLoss, -137.08);
  assert.equal(st.payoff, 2.03); assert.equal(st.expectancy, -53.94); assert.equal(st.stopOuts, 6); assert.equal(st.stopSharePct, 40);
  assert.equal(st.netPnl, -809.08);
  assert.equal(ctx.ihStatsText_(st), '15 closed (3W/12L) · win rate 20% · avg win +278.61 · avg loss -137.08 · payoff 2.03 · expectancy -53.94/trade · 6 stop-outs (40%) · net -809.08');
  const narrow = ctx.ihStats_(recs, Date.UTC(2026, 9, 17), 30);   // window: 17 Sep .. 17 Oct
  assert.equal(narrow.n, 4); assert.equal(narrow.stopOuts, 4);     // AER, CARE, YUM, SBAC
  assert.equal(ctx.ihStatsText_(ctx.ihStats_([], Date.UTC(2026, 9, 17), 90)), 'no closed trades in the last 90d');
});
test('S2 closed trades group partial fills by symbol + day; BUY fills with zero P&L are ignored; a STP fill marks the trade a stop-out', () => {
  const ctx = load();
  const closed = ctx.ihClosedTrades_([
    { day: '2026-08-10', symbol: 'PFLT', side: 'SELL', pnl: 10, orderType: 'LMT' },
    { day: '2026-08-10', symbol: 'pflt', side: 'SELL', pnl: 49.6, orderType: 'LMT' },
    { day: '2026-08-06', symbol: 'PFLT', side: 'BUY', pnl: 0, orderType: 'LMT' },
    { day: '2026-09-16', symbol: 'VEL', side: 'SELL', pnl: -20, orderType: 'LMT' },
    { day: '2026-09-16', symbol: 'VEL', side: 'SELL', pnl: -9.3, orderType: 'STP LMT' }]);
  assert.equal(closed.length, 2);
  assert.equal(closed[0].symbol, 'PFLT'); assert.ok(Math.abs(closed[0].pnl - 59.6) < 1e-9); assert.equal(closed[0].stop, false);
  assert.equal(closed[1].symbol, 'VEL'); assert.ok(Math.abs(closed[1].pnl + 29.3) < 1e-9); assert.equal(closed[1].stop, true);
});
test('N1 NAV chain: first row is the base, r = NAV1/(NAV0 - flows) - 1 with deposits removed, TWR chain-links, a repeated day is skipped', () => {
  const ctx = load();
  const d1 = ctx.ihNavAppend_([], { ok: true, day: '2026-10-15', nav: 10000, ccy: 'USD', flows: 0, flowsKnown: true }, 'c1', 't');
  assert.deepEqual(plain(d1.row.slice(0, 6)), ['2026-10-15', 10000, 'USD', 0, 0, 0]);
  const d2 = ctx.ihNavAppend_([d1.row], { ok: true, day: '2026-10-16', nav: 10600, ccy: 'USD', flows: 500, flowsKnown: true }, 'c2', 't');
  assert.ok(Math.abs(d2.row[4] - (10600 / 10500 - 1)) < 1e-6); assert.ok(Math.abs(d2.row[5] - d2.row[4]) < 1e-9);
  const d3 = ctx.ihNavAppend_([d1.row, d2.row], { ok: true, day: '2026-10-17', nav: 10494, ccy: 'USD', flows: 0, flowsKnown: true }, 'c3', 't');
  assert.ok(Math.abs(d3.row[4] - (10494 / 10600 - 1)) < 1e-6);
  assert.ok(Math.abs(d3.row[5] - ((1 + d2.row[5]) * (1 + d3.row[4]) - 1)) < 2e-6, 'stored values are rounded to 6 digits');
  const dup = ctx.ihNavAppend_([d1.row, d2.row, d3.row], { ok: true, day: '2026-10-17', nav: 1, ccy: 'USD', flows: 0, flowsKnown: true }, 'c4', 't');
  assert.equal(dup.skipped, true); assert.equal(dup.reason, 'day already recorded');
  const none = ctx.ihNavAppend_([], { ok: false, reason: 'no NAV section' }, 'c', 't');
  assert.equal(none.skipped, true); assert.equal(none.reason, 'no NAV section');
  const unknownFlows = ctx.ihNavAppend_([], { ok: true, day: '2026-10-15', nav: 1, ccy: 'USD', flows: 0, flowsKnown: false }, 'c', 't');
  assert.equal(unknownFlows.row[3], '');
});
test('N2 extractors accept snake_case and Flex camelCase; latest NAV report date wins; flows only from the base-currency Cash Report row of that day', () => {
  const ctx = load();
  const ex = ctx.ihExtractNav_({ captured_at: '2026-10-17T06:00:00Z', base_currency: 'USD',
    net_asset_value: [{ total: '46000', reportDate: '20261015' }, { total: '46296.26', reportDate: '20261016' }],
    cash_report: [{ currency: 'BASE_SUMMARY', depositWithdrawals: '1000', reportDate: '20261016' },
                  { currency: 'BASE_SUMMARY', depositWithdrawals: '77', reportDate: '20261015' },
                  { currency: 'SGD', depositWithdrawals: '5', reportDate: '20261016' }] });
  assert.equal(ex.ok, true); assert.equal(ex.day, '2026-10-16'); assert.equal(ex.nav, 46296.26); assert.equal(ex.ccy, 'USD'); assert.equal(ex.flows, 1000); assert.equal(ex.flowsKnown, true);
  const noNav = ctx.ihExtractNav_({ positions: [], balances: [] });
  assert.equal(noNav.ok, false); assert.equal(noNav.reason, 'no NAV section');
  const noCash = ctx.ihExtractNav_({ nav: { nav: 100, currency: 'usd', date: '2026-10-17' } });
  assert.equal(noCash.ok, true); assert.equal(noCash.flowsKnown, false); assert.equal(noCash.flows, 0); assert.equal(noCash.ccy, 'USD');
  const tr = ctx.ihExtractTrades_({ trades: [
    { tradeID: '1', tradeDate: '20260903;153000', symbol: 'SHG', buySell: 'SELL', quantity: '-70', tradePrice: '84.34', ibCommission: '-1', fifoPnlRealized: '693.32', currency: 'USD', orderType: 'LMT' },
    { trade_id: '2', trade_date: '2026-09-16', symbol: 'VEL', quantity: -24, price: 16.97, realized_pnl: -29.3, currency: 'USD', order_type: 'STP' },
    { tradeID: '3', tradeDate: 'garbage', symbol: 'X' }, null] });
  assert.equal(tr.length, 2);
  assert.deepEqual(plain([tr[0].id, tr[0].day, tr[0].symbol, tr[0].side, tr[0].qty, tr[0].price, tr[0].commission, tr[0].pnl, tr[0].ccy, tr[0].orderType]), ['1', '2026-09-03', 'SHG', 'SELL', 70, 84.34, -1, 693.32, 'USD', 'LMT']);
  assert.equal(tr[1].side, 'SELL', 'side inferred from the negative quantity'); assert.equal(tr[1].orderType, 'STP');
  assert.equal(ctx.ihExtractTrades_({}).length, 0);
});
test('I1 ibkrHistoryRecord_ creates both hidden tabs with headers, appends NAV and fills, then is idempotent for the same capture; status line reads back', () => {
  const ctx = load();
  const cap = { captured_at: '2026-10-17T06:00:00Z', net_asset_value: { total: 46296.26, currency: 'USD', report_date: '2026-10-16' },
    cash_report: [{ currency: 'BASE_SUMMARY', deposits_withdrawals: 0, report_date: '2026-10-16' }],
    trades: csvRecords().map(r => ({ trade_id: r.id, trade_date: r.day, symbol: r.symbol, side: 'SELL', quantity: -1, price: 1, commission: 0, realized_pnl: r.pnl, currency: 'USD', order_type: r.orderType })) };
  const n1 = ctx.ibkrHistoryRecord_(cap, 'cap-1');
  assert.equal(n1, 'nav +1 (2026-10-16), trades +15');
  const nav = ctx.__book['_IBKR_NAV_History'], tr = ctx.__book['_IBKR_Trades'];
  assert.equal(nav.hidden, true); assert.equal(tr.hidden, true);
  assert.deepEqual(plain(nav.rows[0]), plain(ctx.IH_NAV_HEADERS)); assert.equal(nav.rows.length, 2); assert.equal(nav.rows[1][6], 'cap-1');
  assert.deepEqual(plain(tr.rows[0]), plain(ctx.IH_TRADE_HEADERS)); assert.equal(tr.rows.length, 16);
  assert.equal(tr.rows[1][11], 693.32); assert.equal(tr.rows[1][12], ''); assert.equal(tr.rows[6][12], -29.3); assert.equal(tr.rows[6][9], 'STP');
  const n2 = ctx.ibkrHistoryRecord_(cap, 'cap-1b');
  assert.equal(n2, 'nav skipped: day already recorded, trades +0');
  assert.equal(nav.rows.length, 2); assert.equal(tr.rows.length, 16);
  const cap2 = { captured_at: '2026-10-18T06:00:00Z', nav: { total: 46000, currency: 'USD', report_date: '2026-10-17' }, trades: [] };
  assert.equal(ctx.ibkrHistoryRecord_(cap2, 'cap-2'), 'nav +1 (2026-10-17), trades +0');
  assert.ok(Math.abs(nav.rows[2][4] - (46000 / 46296.26 - 1)) < 1e-6);
  const status = ctx.ibkrHistoryStatus_();
  assert.ok(status.indexOf('IBKR history v1.0.0: NAV rows 2 (last 2026-10-17, TWR -0.64%) · fills 15 · ') === 0, status);
  assert.ok(status.indexOf('15 closed (3W/12L)') > 0 || status.indexOf('closed') > 0, status);
  assert.equal(ctx.ibkrHistoryRecord_(null, 'x'), 'nav skipped: no capture, trades +0');
});
test('I2 charts: TWR line (#2a78d6, 2 px) over A:F and the stacked Gain/Loss column chart (#2a78d6/#e34948); replaced per anchor; absent tabs -> false', () => {
  const ctx = load();
  const pd = fakeSheet('Portfolio_Decision', []);
  assert.equal(ctx.ibkrHistoryTwrChart_(pd, 20, 1), false);
  ctx.ibkrHistoryRecord_({ captured_at: '2026-10-17T06:00:00Z', nav: { total: 1, currency: 'USD', date: '2026-10-17' },
    trades: [{ trade_id: 'a', trade_date: '2026-10-17', symbol: 'A', side: 'SELL', quantity: -1, price: 1, realized_pnl: 5 }] }, 'c');
  assert.equal(ctx.ibkrHistoryTwrChart_(pd, 20, 1), true);
  assert.equal(ctx.ibkrHistoryTwrChart_(pd, 20, 1), true);
  assert.equal(ctx.ibkrHistoryTradeChart_(pd, 40, 1), true);
  assert.equal(pd.charts.length, 2);
  const twr = pd.charts[0], bar = pd.charts[1];
  assert.equal(twr.kind, 'line'); assert.deepEqual(plain(twr.getOptions().get('colors')), ['#2a78d6']); assert.equal(twr.getOptions().get('lineWidth'), 2);
  assert.deepEqual(plain(twr.ranges), ['_IBKR_NAV_History!1,1,2,1', '_IBKR_NAV_History!1,6,2,1']); assert.deepEqual(plain(twr.pos), [20, 1]);
  assert.equal(bar.kind, 'column'); assert.deepEqual(plain(bar.getOptions().get('colors')), ['#2a78d6', '#e34948']);
  assert.deepEqual(plain(bar.ranges), ['_IBKR_Trades!1,2,2,1', '_IBKR_Trades!1,12,2,2']);
});
test('H1 10_My_Portfolio v1.5.17: the 3-line hook sits after the future-clock check and before the 10-minute decision return; selftest line; ES5 guard on every added line of both files', () => {
  assert.ok(pfSource.includes("var PD_VERSION = '1.5.17';"));
  const hook = pfSource.indexOf("    if (typeof ibkrHistoryRecord_ === 'function') {   /* v1.5.17 [B2]: history before the decision gate */");
  const future = pfSource.indexOf("    if (capMs !== null && capMs > nowMs) return 'capture time ahead of the clock';");
  const stale = pfSource.indexOf('    if (capMs === null || nowMs - capMs > PD_IBKR_WATCH_MAX_AGE_MS) {');
  assert.ok(future > 0 && future < hook && hook < stale, [future, hook, stale].join(' '));
  assert.ok(pfSource.includes('      try { ibkrHistoryRecord_(cap.json, cap.id); } catch (eIh) {}'));
  assert.ok(pfSource.includes("  report.push(typeof ibkrHistoryStatus_ === 'function' ? ibkrHistoryStatus_() :   /* v1.5.17 [B2] */"));
  const bad = /\b(let|const)\b|=>|`|\bclass\b|\.\.\.|\.includes\(/;
  for (const line of source.split('\n')) { assert.ok(!bad.test(line), 'ES5 26: ' + line); assert.ok(!/[‘’“”\r]/.test(line), 'quotes/CR 26: ' + line); }
  for (const line of pfSource.split('\n').filter(l => /v1\.5\.17|ibkrHistory/.test(l))) assert.ok(!bad.test(line), 'ES5 10: ' + line);
});

console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
