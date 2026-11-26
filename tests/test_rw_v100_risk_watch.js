#!/usr/bin/env node
/* tests/test_rw_v100_risk_watch.js
 * Harness for apps_script/32_Risk_Watch.gs v1.0.0 (a new file): an e-mail within minutes when a holding nears its stop
 * or its target, falls sharply, or sits exposed at the open. WHY: IBOC (25 Nov 2026) and NMM (20 Nov) were sold by their
 * stop orders in the first second of a session; nothing had warned that the stop sat inside one ordinary day's range.
 * The REAL .gs runs in a vm with shims for the workbook, UrlFetchApp.fetchAll (Yahoo charts built per scenario),
 * MailApp, LockService, PropertiesService, ScriptApp and a real time-zone clock (Intl).
 *   W0  static: compile, version lockstep, ES5 and ASCII, the only web host is Yahoo, no broker or order call
 *   W1  gate: off / absent / junk -> one property read, no sheet, no fetch, no e-mail
 *   W2  idle hours and weekends -> no sheet, no fetch
 *   W3  the IBOC morning of 25 Nov: one e-mail before the open, once
 *   W4  a pre-market trade at or below the stop
 *   W5  a holiday: nothing is sent, before the open (the calendar) or in the session (nothing is fresh); before the
 *       open the rule needs no price feed
 *   W6  near, then crossed, once each; a new day starts clean
 *   W7  target reached: the first and the next
 *   W8  a sharp fall, and each further step
 *   W9  the market's own fall
 *   W10 a holding under a cash offer
 *   W11 an old price fires nothing; a listing outside the US is shown, not watched
 *   W12 its own health: prices, levels
 *   W13 observe; a failed e-mail is tried again; the daily cap; no recipient; the lock
 *   W14 owner functions; summer time
 *   W15 mutants: each guard removed is caught
 * The last line is 'PASS k/k' (exit 1 on any failure).
 */
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const SRC_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '32_Risk_Watch.gs');
const SRC = fs.readFileSync(SRC_PATH, 'utf8');

let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n  ')); }
}
function ok(c, m) { if (!c) throw new Error(m || 'assertion failed'); }
function eq(a, b, m) { if (a !== b) throw new Error((m || 'not equal') + ': ' + JSON.stringify(a) + ' !== ' + JSON.stringify(b)); }

/* Utilities.formatDate with a real time zone (the three patterns the file uses). */
function formatDate(d, tz, pattern) {
  const f = new Intl.DateTimeFormat('en-CA', { timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' });
  const p = {}; f.formatToParts(d).forEach(x => { p[x.type] = x.value; });
  return String(pattern).replace('yyyy', p.year).replace('MM', p.month).replace('dd', p.day).replace('HH', p.hour).replace('mm', p.minute);
}
function fakeDate(nowMs) {
  const RealDate = Date;
  return class extends RealDate { constructor(...a) { if (a.length === 0) super(nowMs); else super(...a); } static now() { return nowMs; } };
}
const Z = (d, h, m) => Date.UTC(2026, 10, d, h, m || 0);            // a November 2026 moment, UTC
const OPEN25 = Z(25, 14, 30), PRE25 = Z(25, 9, 0);                   // 09:30 and 04:00 New York (EST)

/* the Exit_Plan page as scripts/exit_plan.py page_rows writes it (tests/test_rw_v100_risk_watch.py pins the layout) */
const PAGE_HDR = ['Symbol', 'Name', 'Qty', 'Entry', 'Last close', 'Live', 'P/L %', 'Highest close', 'ATR %', 'Gain at high (R)', 'Stop', 'Was', 'Target', 'Action', 'Why', 'Broker stop'];
function page(holdings, opts) {
  opts = opts || {};
  const rows = [['EXIT PLAN - stop and target per holding, reviewed after every close'],
    ['As of ' + (opts.asOf || '2026-11-25 08:35') + ' Riyadh · daily bars to 2026-11-24 · ' + (opts.broker || 'broker orders from the IBKR capture of 2026-11-25 08:05 Riyadh')],
    ['Rule: initial stop 3xATR ...'], ['Today: HOLD ' + holdings.length], [], ['HOLDINGS (' + holdings.length + ')'], opts.hdr || PAGE_HDR];
  holdings.forEach(h => rows.push([h.symbol, h.name || '', String(h.qty || 10), h.entry.toFixed(2), h.close.toFixed(2), '—', '+0.0', h.close.toFixed(2),
    (h.atrPct === undefined ? 2.0 : h.atrPct).toFixed(1), '+0.0', h.stop.toFixed(2), '—', h.target.toFixed(2), h.action || 'HOLD', 'why', h.broker === undefined ? 'unknown' : h.broker.toFixed(2)]));
  rows.push([], ["TODAY'S PLAN PICKS - per-stock levels (0)"], ['Symbol', 'Entry']);
  return rows.map(r => { const o = r.slice(); while (o.length < 16) o.push(''); return o; });
}
const LEDGER_HDR = ['Date', 'Run At (Riyadh)', 'Symbol', 'Lot', 'Entry', 'Shares', 'Last', 'Highest Close', 'ATR', 'R', 'Stop', 'Target', 'TP1 Reached', 'Action', 'Tighten', 'Bars To', 'Basis', 'Market', 'Judgment'];
function ledger(holdings, day) {
  const rows = [LEDGER_HDR, ['2026-11-20', '2026-11-20 08:35', 'OLD.US', 'x', 1, 1, 1, 1, 1, 1, 1, 1, 'yes', 'HOLD', '', '', 1, '', 'offer:manual']];
  holdings.forEach(h => rows.push([day || '2026-11-25', '08:35', h.symbol, 'lot', h.entry, h.qty || 10, h.close, h.close, 1, 1, h.stop, h.target, h.tp1 ? 'yes' : '', h.action || 'HOLD', '', '', 1, '', h.judgment || '']));
  return rows;
}
/* one Yahoo chart: the last regular trade, the previous close, today's periods and pre-market bars */
function chart(o) {
  const reg = o.regStart === undefined ? OPEN25 : o.regStart, pre = o.preStart === undefined ? reg - 5.5 * 3600000 : o.preStart;
  const bars = o.pre || [];
  return JSON.stringify({ chart: { result: [{ meta: { regularMarketPrice: o.price, regularMarketTime: Math.floor(o.at / 1000), previousClose: o.prev,
    currentTradingPeriod: { pre: { start: pre / 1000, end: reg / 1000 }, regular: { start: reg / 1000, end: (reg + 6.5 * 3600000) / 1000 } } },
    timestamp: bars.map(b => b[0] / 1000), indicators: { quote: [{ close: bars.map(b => b[1]), volume: bars.map(b => b[2] === undefined ? 0 : b[2]) }] } }] } });
}
const YESTERDAY = OPEN25 - 17.5 * 3600000;                            // 24 Nov 16:00 New York

function world(props, sheets) {
  return { props: Object.assign({}, props || {}), sheets: sheets || {}, charts: {}, fetches: [], mails: [], logs: [], toasts: [], triggers: [],
           reads: [], sheetReads: 0, mailThrows: '', fetchThrows: false, http: {}, lockBusy: false, owner: 'owner@example.com', quota: 90 };
}
function sheetOf(w, name) {
  if (!w.sheets.hasOwnProperty(name)) return null;
  const s = w.sheets[name];
  return {
    getLastRow: () => s.length, getLastColumn: () => s.reduce((m, r) => Math.max(m, r.length), 0),
    getRange: (r, c, nr, nc) => ({
      getValues: () => { w.sheetReads++; const out = []; for (let i = 0; i < nr; i++) { const row = s[r - 1 + i] || [], line = []; for (let j = 0; j < nc; j++) line.push(row[c - 1 + j] === undefined ? '' : row[c - 1 + j]); out.push(line); } return out; },
      setValues: (vals) => { for (let i = 0; i < nr; i++) { s[r - 1 + i] = s[r - 1 + i] || []; for (let j = 0; j < nc; j++) s[r - 1 + i][c - 1 + j] = vals[i][j]; } }
    }),
    appendRow: (row) => { s.push(row.slice()); }
  };
}
function env(src, w, ms, opts) {
  opts = opts || {};
  const ctx = {
    Date: fakeDate(ms), JSON, Math, Number, String, parseFloat, isFinite, isNaN, encodeURIComponent, Object, Array, RegExp, Error,
    PropertiesService: { getScriptProperties() {
      if (opts.propsThrow) throw new Error('synthetic property outage');
      return { getProperty(k) { w.reads.push(k); return Object.prototype.hasOwnProperty.call(w.props, k) ? w.props[k] : null; }, setProperty(k, v) { w.props[k] = String(v); } }; } },
    Utilities: { formatDate },
    UrlFetchApp: { fetchAll(reqs) {
      if (w.fetchThrows) { w.fetches.push({ threw: true, n: reqs.length }); throw new Error('Address unavailable'); }
      return reqs.map(rq => {
        const host = rq.url.split('/')[2], sym = decodeURIComponent(rq.url.split('/chart/')[1].split('?')[0]);
        w.fetches.push({ host, sym, url: rq.url, mute: rq.muteHttpExceptions, method: rq.method });
        const code = w.http[host] || (Object.prototype.hasOwnProperty.call(w.charts, sym) ? 200 : 404);
        return { getResponseCode: () => code, getContentText: () => (code === 200 ? w.charts[sym] : '{"chart":{"result":null}}') };
      });
    } },
    MailApp: { sendEmail(m) { if (w.mailThrows) throw new Error(w.mailThrows); w.mails.push(m); }, getRemainingDailyQuota: () => w.quota },
    LockService: { getScriptLock: () => ({ tryLock: () => !w.lockBusy, releaseLock() {} }) },
    Session: { getEffectiveUser: () => ({ getEmail: () => w.owner }) },
    Logger: { log(m) { w.logs.push(String(m)); } },
    SpreadsheetApp: { getActive: () => ({ getSheetByName: (n) => sheetOf(w, n), insertSheet: (n) => { w.sheets[n] = []; return sheetOf(w, n); }, toast: (m) => { w.toasts.push(String(m)); } }) },
    ScriptApp: {
      getProjectTriggers() { return w.triggers.map(t => ({ getHandlerFunction: () => t.h, _t: t })); },
      deleteTrigger(tr) { w.triggers = w.triggers.filter(t => t !== tr._t); },
      newTrigger(h) { const t = { h, every: 0 }; const b = { timeBased() { return b; }, everyMinutes(n) { t.every = n; return b; }, create() { w.triggers.push(t); return t; } }; return b; }
    }
  };
  vm.createContext(ctx);
  vm.runInContext(src, ctx, { filename: '32_Risk_Watch.gs' });
  return ctx;
}
function tick(w, ms, opts) {
  const before = { mails: w.mails.length, fetches: w.fetches.length, reads: w.sheetReads };
  w.reads = [];
  const ret = env((opts && opts.src) || SRC, w, ms, opts).rwTick();
  return { ret, mails: w.mails.slice(before.mails), fetches: w.fetches.slice(before.fetches), sheetReads: w.sheetReads - before.reads, propReads: w.reads.slice() };
}
const ON = { TFB_RISK_WATCH: 'on' };
const IBOC = { symbol: 'IBOC.US', qty: 52, entry: 68.74, close: 63.77, atrPct: 1.9, stop: 62.67, target: 74.81, broker: 62.67 };
const GLPI = { symbol: 'GLPI.US', qty: 79, entry: 38.88, close: 39.01, atrPct: 1.6, stop: 35.74, target: 42.02, broker: 35.74 };
const DDI = { symbol: 'DDI.US', qty: 240, entry: 13.44, close: 15.50, atrPct: 0.6, stop: 15.22, target: 16.25, judgment: 'offer:manual' };
function book(holdings, opts) { return { Exit_Plan: page(holdings, opts), _Exit_Plan: ledger(holdings, opts && opts.ledgerDay) }; }
const spyPre = (at) => chart({ price: 590.0, at: YESTERDAY, prev: 588.0, pre: [[at - 600000, 590.5, 1000], [at - 120000, 590.8, 2000]] });
const spyLive = (at, px) => chart({ price: px === undefined ? 590.0 : px, at: at - 20000, prev: 590.0 });
const closed = (px, prev) => chart({ price: px, at: YESTERDAY, prev: prev === undefined ? px : prev });
const live = (at, px, prev) => chart({ price: px, at: at - 60000, prev });
const statusRows = (w) => (w.sheets.Risk_Watch || []).slice(5).filter(r => r[0] !== '');
const statusOf = (w, sym) => statusRows(w).find(r => r[0] === sym);
const logRows = (w) => (w.sheets._Risk_Watch_Log || []).slice(1);
const state = (w) => JSON.parse(w.props.TFB_RISK_WATCH_STATE);

// ------------------------------------------------------------------ W0 static
test('W0 static: compile, version, ES5, ASCII, Yahoo only, no broker or order call', () => {
  new vm.Script(SRC, { filename: '32_Risk_Watch.gs' });
  const code = SRC.split('\n').filter(l => !/^\s*(\/\/|\/?\*)/.test(l)).join('\n');
  ok(/^ \* Version: 1\.0\.0 \(2026-11-26\)$/m.test(SRC) && /^var RW_VERSION = '1\.0\.0';/m.test(SRC), 'version lockstep');
  ok(!/\b(let|const)\s|=>|`/.test(code), 'ES5 only');
  ok(/^[\x00-\x7F]*$/.test(SRC) && SRC.indexOf('\r') < 0, 'ASCII and LF');
  eq((code.match(/https?:\/\//g) || []).length, 1, 'one URL literal');
  ok(/'https:\/\/' \+ host \+ '\/v8\/finance\/chart\/'/.test(code) && /HOSTS: \['query1\.finance\.yahoo\.com', 'query2\.finance\.yahoo\.com'\]/.test(code), 'the URL is Yahoo\'s chart');
  ok(!/ibkr|interactivebrokers|placeOrder|cancelOrder|modifyOrder/i.test(code), 'no broker call');
  ok(!/method: '(post|put|delete|patch)'/i.test(code) && (code.match(/method: 'get'/g) || []).length === 1, 'reads only');
  // the tabs it writes are its own two; it never writes the Exit Plan
  eq((code.match(/setValues\(/g) || []).length, 1, 'one setValues');
  ok(/getSheetByName\(RW_\.TAB_STATUS\) \|\| ss\.insertSheet\(RW_\.TAB_STATUS\)/.test(code) && /ss\.insertSheet\(RW_\.TAB_LOG\)/.test(code), 'its own tabs');
  const e = env(SRC, world(), Z(25, 13, 20));
  ['rwTick', 'tfbRiskWatchNow', 'tfbInstallRiskWatchTrigger', 'tfbRemoveRiskWatchTrigger', 'tfbRiskWatchStatus', 'tfbRiskWatchTestMail', 'tfbRiskWatchSelfTest']
    .forEach(n => ok(typeof e[n] === 'function', n));
  eq(e.tfbRiskWatchSelfTest(), 'risk watch core: ok');
});

// ------------------------------------------------------------------ W1 gate
test('W1 gate: off / absent / junk reads one property and nothing else', () => {
  ['', 'off', 'junk', '0'].forEach(v => {
    const w = world(v === '' ? {} : { TFB_RISK_WATCH: v }, book([IBOC]));
    const t = tick(w, Z(25, 15, 0));
    eq(t.ret, 'off'); eq(t.propReads.join(','), 'TFB_RISK_WATCH'); eq(t.sheetReads, 0); eq(t.fetches.length, 0); eq(t.mails.length, 0);
    ok(!w.sheets.Risk_Watch && !w.props.TFB_RISK_WATCH_STATE);
  });
  const w = world(ON, book([IBOC]));
  eq(tick(w, Z(25, 15, 0), { propsThrow: true }).ret, 'off (properties not read)');
});

// ------------------------------------------------------------------ W2 idle
test('W2 idle hours and weekends: no sheet, no fetch', () => {
  // Wed 25 Nov 03:00 and 08:14 New York; 16:11; Sat 28 Nov 11:00; Sun 29 Nov
  [Z(25, 8, 0), Z(25, 13, 14), Z(25, 21, 11), Z(28, 16, 0), Z(29, 16, 0)].forEach(ms => {
    const w = world(ON, book([IBOC])); w.charts = { SPY: spyLive(ms), IBOC: live(ms, 62.0, 63.77) };
    const t = tick(w, ms);
    eq(t.ret, 'idle'); eq(t.sheetReads, 0); eq(t.fetches.length, 0); eq(t.mails.length, 0);
  });
  // the edges that work: 08:15 (pre), 09:30 and 16:10 (session)
  eq(tick(world(ON, book([])), Z(25, 13, 15)).ret.split(':')[0], 'pre');
  eq(tick(world(ON, book([])), Z(25, 14, 30)).ret.split(':')[0], 'session');
  eq(tick(world(ON, book([])), Z(25, 21, 10)).ret.split(':')[0], 'session');
});

// ------------------------------------------------------------------ W3 the IBOC morning
test('W3 the IBOC morning of 25 Nov: one e-mail before the open, once', () => {
  const now = Z(25, 13, 20);                                           // 08:20 New York, 16:20 Riyadh
  const w = world(ON, book([IBOC, GLPI, DDI]));
  w.charts = { SPY: spyPre(now), IBOC: closed(63.77, 63.85), GLPI: closed(39.01), DDI: closed(15.50) };
  const t = tick(w, now);
  eq(t.ret, 'pre: 1 alert(s) sent'); eq(t.mails.length, 1);
  const m = t.mails[0];
  eq(m.to, 'owner@example.com');
  eq(m.subject, 'TFB risk watch: IBOC stop 1.7% under the last close');
  ok(m.body.indexOf('- IBOC.US: the stop working at the broker, 62.67, is 1.7% under the last close 63.77 - inside one ordinary day\'s range (ATR 1.9%). ' +
    'A plain stop order can be reached in the first seconds of the open (17:30 Riyadh). No pre-market trade yet.') === 0, m.body);
  ok(/Levels: the Exit Plan of 2026-11-25 08:35 Riyadh\. Prices: Yahoo, read 2026-11-25 16:20 Riyadh\./.test(m.body), 'provenance');
  ok(/places, changes and cancels no order/.test(m.body) && /Not investment advice\. Risk watch 1\.0\.0\.$/.test(m.body), 'footer');
  ok(m.body.indexOf('GLPI') < 0 && m.body.indexOf('DDI') < 0, 'a stop 8% away and an offer holding are not in it');
  // one chart per US holding and one for the fund, GET, muted
  eq(t.fetches.map(f => f.sym).join(','), 'SPY,IBOC,GLPI,DDI'); ok(t.fetches.every(f => f.host === 'query1.finance.yahoo.com' && f.mute === true && f.method === 'get'));
  ok(/interval=5m&range=1d&includePrePost=true$/.test(t.fetches[0].url));
  // the tab and the log
  const st = statusOf(w, 'IBOC.US');
  eq(st[1], 63.77); eq(st[4], 62.67); eq(st[5], 'working at the broker'); eq(st[6], 1.8); eq(st[9], 'OPEN_RISK'); eq(st[10], 'OPEN_RISK');
  eq(statusOf(w, 'DDI.US')[5], 'none: cash offer, manual sale'); eq(statusOf(w, 'GLPI.US')[9], 'before the open: last close');
  ok(/^Read 2026-11-25 16:20 Riyadh - mode on - before the open - market: trading - levels: Exit Plan of 2026-11-25 08:35 - e-mails today 1 of 12 - 1 alert\(s\) sent$/.test(w.sheets.Risk_Watch[1][0]), w.sheets.Risk_Watch[1][0]);
  eq(logRows(w).length, 1); eq(logRows(w)[0].slice(1, 5).join('|'), 'on|pre|OPEN_RISK|IBOC.US'); eq(logRows(w)[0][6], 'sent');
  eq(state(w).day, '2026-11-25'); ok(state(w).sent['IBOC.US|OPEN_RISK'] > 0); eq(state(w).mails, 1);
  // five minutes later: nothing new
  w.charts.SPY = spyPre(now + 300000);
  const t2 = tick(w, now + 300000);
  eq(t2.ret, 'pre: nothing new'); eq(t2.mails.length, 0); eq(logRows(w).length, 1); eq(statusOf(w, 'IBOC.US')[10], 'OPEN_RISK');
});

// ------------------------------------------------------------------ W4 pre-market below the stop
test('W4 a pre-market trade at or below the stop comes first', () => {
  const now = Z(25, 14, 0);
  const NMM = { symbol: 'NMM.US', qty: 24, entry: 88.0, close: 82.32, atrPct: 2.4, stop: 81.22, target: 95.69, broker: 81.22 };
  const w = world(ON, book([NMM]));
  w.charts = { SPY: spyPre(now), NMM: chart({ price: 82.32, at: YESTERDAY, prev: 83.26, pre: [[now - 1500000, 81.6, 0], [now - 900000, 80.9, 300]] }) };
  const t = tick(w, now);
  eq(t.mails.length, 1);
  eq(t.mails[0].subject, 'TFB risk watch: NMM pre-market at or below its stop; NMM stop 1.3% under the last close');
  const lines = t.mails[0].body.split('\n');
  ok(/^- NMM\.US: a pre-market trade at 80\.90 \(16:45 Riyadh\) is at or below the stop working at the broker, 81\.22\. If the price stays there, the order sells at the open \(17:30 Riyadh\)\. Last close 82\.32\.$/.test(lines[0]), lines[0]);
  ok(/Pre-market: 80\.90 at 16:45 Riyadh on 300 shares \(thin trading\)\.$/.test(lines[1]), lines[1]);
  // a pre-market trade above the stop: the open-risk line carries it, no PRE_BELOW
  const w2 = world(ON, book([NMM]));
  w2.charts = { SPY: spyPre(now), NMM: chart({ price: 82.32, at: YESTERDAY, prev: 83.26, pre: [[now - 900000, 82.0]] }) };
  const t2 = tick(w2, now);
  eq(t2.mails[0].subject, 'TFB risk watch: NMM stop 1.3% under the last close'); ok(/Pre-market: 82\.00 at 16:45 Riyadh \(thin trading\)\.$/.test(t2.mails[0].body.split('\n')[0]));
  // a chart that still holds the prior session's pre-market bars is not this morning's trade (review of PR #937, pre_open_check)
  const w4 = world(ON, book([NMM]));
  w4.charts = { SPY: spyPre(now), NMM: chart({ price: 82.32, at: YESTERDAY, prev: 83.26, regStart: OPEN25 - 86400000, pre: [[now - 86400000 - 900000, 80.9, 300]] }) };
  const t4 = tick(w4, now);
  eq(t4.mails[0].subject, 'TFB risk watch: NMM stop 1.3% under the last close'); ok(/No pre-market trade yet\.$/.test(t4.mails[0].body.split('\n')[0]), t4.mails[0].body);
  // without a pre-market period in the answer, the 6 hours before the open count; the previous close is read in the provider's order
  const e4 = env(SRC, world(), now);
  const bare = JSON.parse(chart({ price: 82.32, at: YESTERDAY, prev: 83.26, pre: [[now - 900000, 80.9, 300]] }));
  delete bare.chart.result[0].meta.currentTradingPeriod.pre;
  bare.chart.result[0].meta.regularMarketPreviousClose = 83.0; bare.chart.result[0].meta.chartPreviousClose = 70.0;
  const pb = e4.rwParseChartCore_(JSON.stringify(bare));
  eq(pb.preLast, 80.9); eq(pb.preStartMs, OPEN25 - 6 * 3600000); eq(pb.prevClose, 83.0);
  delete bare.chart.result[0].meta.regularMarketPreviousClose; eq(e4.rwParseChartCore_(JSON.stringify(bare)).prevClose, 83.26);
  delete bare.chart.result[0].meta.previousClose; eq(e4.rwParseChartCore_(JSON.stringify(bare)).prevClose, 70.0);
  ['', 'not json', '{"chart":{"result":null}}', '{"chart":{"result":[{"meta":{}}]}}'].forEach(b => eq(e4.rwParseChartCore_(b).ok, false));
  // a holding with no working stop at the broker has nothing that can fire at the open
  const w3 = world(ON, book([Object.assign({}, NMM, { broker: undefined })]));
  w3.charts = { SPY: spyPre(now), NMM: closed(82.32) };
  eq(tick(w3, now).mails.length, 0);
});

// ------------------------------------------------------------------ W5 holiday
test('W5 a holiday: nothing is fresh, nothing is sent', () => {
  // Thu 26 Nov 2026 (Thanksgiving): Yahoo still answers, with Wednesday's last trade and no pre-market bar
  const hol = (h, m) => Z(26, h, m), w = world(ON, book([IBOC]));
  const stale = (px) => chart({ price: px, at: Z(25, 21, 0), prev: 63.77, regStart: Z(26, 14, 30) });
  w.charts = { SPY: stale(590), IBOC: stale(62.0) };                    // IBOC under its stop on the last trade
  const pre = tick(w, hol(13, 30)), ses = tick(w, hol(16, 0));
  eq(pre.ret, 'pre: nothing new'); eq(w.mails.length, 0);
  // before the open the holiday is known from the list; in the session from the data
  const wp = world(ON, book([IBOC])); wp.charts = w.charts; tick(wp, hol(13, 30));
  ok(/before the open - market: not trading \(a listed market holiday\)/.test(wp.sheets.Risk_Watch[1][0]), wp.sheets.Risk_Watch[1][0]);
  eq(ses.ret, 'session: nothing new'); eq(w.mails.length, 0); eq(logRows(w).length, 0);
  ok(/market: not trading \(no regular trade in the last 10 min\)/.test(w.sheets.Risk_Watch[1][0]), w.sheets.Risk_Watch[1][0]);
  eq(statusOf(w, 'IBOC.US')[9], 'no fresh trade: last price only'); eq(state(w).fail, 0);
  // a closure the list does not hold, added by the owner
  const x = world({ TFB_RISK_WATCH: 'on', TFB_RISK_WATCH_HOLIDAYS: '2026-12-24, 2026-11-25' }, book([IBOC])); x.charts = { SPY: spyPre(Z(25, 13, 20)), IBOC: closed(63.77) };
  eq(tick(x, Z(25, 13, 20)).mails.length, 0);
  // before the open the rule needs no price feed: with Yahoo down the page's last close and the broker's stop are enough
  const y = world(ON, book([IBOC, GLPI])); y.http = { 'query1.finance.yahoo.com': 503, 'query2.finance.yahoo.com': 503 };
  const ty = tick(y, Z(25, 13, 20));
  eq(ty.mails.length, 1); eq(ty.mails[0].subject, 'TFB risk watch: IBOC stop 1.7% under the last close');
  ok(/is 1\.7% under the last close 63\.77 - inside one ordinary day's range \(ATR 1\.9%\)\. A plain stop order can be reached in the first seconds of the open \(17:30 Riyadh\)\. The pre-market price could not be read\.$/m.test(ty.mails[0].body), ty.mails[0].body);
  eq(statusOf(y, 'IBOC.US')[1], 63.77); eq(statusOf(y, 'IBOC.US')[2], 'last close (Exit Plan)'); eq(statusOf(y, 'IBOC.US')[9], 'OPEN_RISK'); eq(statusOf(y, 'GLPI.US')[9], 'no price (HTTP 503)');
  eq(state(y).fail, 1);
});

// ------------------------------------------------------------------ W6 near, crossed
test('W6 near, then crossed, once each; a new day starts clean', () => {
  const w = world(ON, book([IBOC]));
  const at = (min, px, spy) => { const ms = OPEN25 + min * 60000; w.charts = { SPY: spyLive(ms, spy), IBOC: live(ms, px, 63.77) }; return tick(w, ms); };
  eq(at(5, 63.40).mails.length, 0);                                     // 1.2% above the stop: not near
  const n = at(10, 63.20, 587.64);
  eq(n.mails.length, 1); eq(n.mails[0].subject, 'TFB risk watch: IBOC 0.8% above its stop');
  ok(/^- IBOC\.US: 63\.20 now, 0\.8% above the stop working at the broker, 62\.67\. Today -0\.9%; S&P 500 fund -0\.4%\./.test(n.mails[0].body), n.mails[0].body);
  eq(at(15, 63.10).mails.length, 0);                                    // still near: once
  const c = at(20, 62.60);
  eq(c.mails.length, 1); eq(c.mails[0].subject, 'TFB risk watch: IBOC at or below its stop');
  ok(/62\.60 now - at or below the stop working at the broker, 62\.67\. The order has probably sold: check the broker\./.test(c.mails[0].body));
  eq(at(25, 63.00).mails.length, 0); eq(at(30, 62.50).mails.length, 0); // back and forth: nothing more today
  eq(state(w).mails, 2); eq(logRows(w).map(r => r[3]).join(','), 'NEAR,CROSSED');
  // crossed first: no NEAR afterwards
  const w2 = world(ON, book([IBOC])); const o2 = OPEN25 + 600000;
  w2.charts = { SPY: spyLive(o2), IBOC: live(o2, 62.60, 63.77) }; eq(tick(w2, o2).mails.length, 1);
  w2.charts = { SPY: spyLive(o2 + 300000), IBOC: live(o2 + 300000, 63.00, 63.77) }; eq(tick(w2, o2 + 300000).mails.length, 0);
  // the next session: a new day
  const next = OPEN25 + 2 * 86400000 + 600000;                          // Fri 27 Nov
  w.charts = { SPY: chart({ price: 590, at: next - 20000, prev: 590, regStart: OPEN25 + 2 * 86400000 }), IBOC: chart({ price: 63.10, at: next - 60000, prev: 63.56, regStart: OPEN25 + 2 * 86400000 }) };
  const d2 = tick(w, next);
  eq(d2.mails.length, 1); eq(state(w).day, '2026-11-27'); eq(state(w).mails, 1); eq(Object.keys(state(w).sent).join(','), 'IBOC.US|NEAR');
  // the Exit Plan's own stop is named when it differs from the broker's; with no broker stop the wording says so
  const split = Object.assign({}, IBOC, { stop: 62.10 });
  const w3 = world(ON, book([split])); w3.charts = { SPY: spyLive(o2), IBOC: live(o2, 63.00, 63.77) };
  ok(/above the stop working at the broker, 62\.67 \(Exit Plan stop 62\.10\)\./.test(tick(w3, o2).mails[0].body));
  const w4 = world(ON, book([Object.assign({}, IBOC, { broker: undefined })])); w4.charts = { SPY: spyLive(o2), IBOC: live(o2, 62.60, 63.77) };
  const b4 = tick(w4, o2).mails[0].body;
  ok(/at or below the Exit Plan stop 62\.67, and no working stop is known at the broker\. The plan reads the close: at or below the stop at the close it says SELL\./.test(b4), b4);
  eq(statusOf(w4, 'IBOC.US')[5], 'Exit Plan (no working stop at the broker)');
  const w5 = world(ON, book([IBOC], { broker: 'broker orders unknown (last read 2026-10-17 09:00, 940 h old)' })); w5.charts = { SPY: spyLive(o2), IBOC: live(o2, 63.00, 63.77) };
  ok(/above the Exit Plan stop 62\.67; no working stop is known at the broker\./.test(tick(w5, o2).mails[0].body)); eq(statusOf(w5, 'IBOC.US')[5], 'Exit Plan (broker orders not read)');
});

// ------------------------------------------------------------------ W7 target
test('W7 target reached: the first and the next', () => {
  const ms = OPEN25 + 3600000;
  const w = world(ON, book([GLPI])); w.charts = { SPY: spyLive(ms), GLPI: live(ms, 42.10, 41.50) };
  const t = tick(w, ms);
  eq(t.mails[0].subject, 'TFB risk watch: GLPI target reached');
  ok(/^- GLPI\.US: 42\.10 now - the first target 42\.02 \(\+1R\) is reached\. The plan's rule: sell half, and the stop rises to at least the entry 38\.88\. The Exit Plan confirms it on the close\. Today \+1\.4%; S&P 500 fund \+0\.0%\./.test(t.mails[0].body), t.mails[0].body);
  const w2 = world(ON, book([Object.assign({}, GLPI, { tp1: true, target: 45.16 })])); w2.charts = { SPY: spyLive(ms), GLPI: live(ms, 45.20, 44.9) };
  ok(/45\.20 now - the target of record 45\.16 \(the next \+1R step\) is reached\./.test(tick(w2, ms).mails[0].body));
  const w3 = world(ON, book([GLPI])); w3.charts = { SPY: spyLive(ms), GLPI: live(ms, 42.01, 41.50) };
  eq(tick(w3, ms).mails.length, 0);
});

// ------------------------------------------------------------------ W8 drop
test('W8 a sharp fall, and each further step', () => {
  const SUPN = { symbol: 'SUPN.US', qty: 95, entry: 29.89, close: 30.74, atrPct: 3.0, stop: 26.40, target: 34.2, broker: 26.40 };
  const w = world(ON, book([SUPN]));
  const at = (min, px, spy) => { const ms = OPEN25 + min * 60000; w.charts = { SPY: spyLive(ms, spy), SUPN: live(ms, px, 30.74) }; return tick(w, ms); };
  eq(at(10, 29.60).mails.length, 0);                                    // -3.7%
  const d = at(15, 29.33, 588.23);
  eq(d.mails[0].subject, 'TFB risk watch: SUPN -4.6% today');
  ok(/^- SUPN\.US: -4\.6% today \(29\.33 now, 30\.74 at the last close\); S&P 500 fund -0\.3%\. Its stop 26\.40 is 10\.0% under the price\./.test(d.mails[0].body), d.mails[0].body);
  eq(at(20, 28.90).mails.length, 0);                                    // -6.0%: the same step
  const d8 = at(25, 28.20);
  eq(d8.mails[0].subject, 'TFB risk watch: SUPN -8.3% today'); eq(at(30, 28.00).mails.length, 0);
  eq(logRows(w).map(r => r[3]).join(','), 'DROP4,DROP8');
});

// ------------------------------------------------------------------ W9 market
test('W9 the market\'s own fall', () => {
  const AX = { symbol: 'AX.US', qty: 10, entry: 91.63, close: 87.05, atrPct: 2.8, stop: 83.88, target: 99.0, broker: 83.88 };
  const w = world(ON, book([GLPI, AX]));
  const at = (min, spy) => { const ms = OPEN25 + min * 60000; w.charts = { SPY: spyLive(ms, spy), GLPI: live(ms, 38.60, 39.01), AX: live(ms, 85.90, 87.05) }; return tick(w, ms); };
  eq(at(10, 579.38).mails.length, 0);                                   // -1.8%
  const m = at(15, 576.43);
  eq(m.mails[0].subject, 'TFB risk watch: market -2.3%');
  ok(/^- The S&P 500 fund is -2\.3% today\. Holdings nearest to their stops: AX 2\.4%, GLPI 8\.0%\./.test(m.mails[0].body), m.mails[0].body);
  eq(at(20, 575.0).mails.length, 0);
  eq(at(25, 571.71).mails[0].subject, 'TFB risk watch: market -3.1%');
  eq(logRows(w).map(r => r[3] + ':' + r[4]).join(','), 'MKT2:MARKET,MKT3:MARKET');
});

// ------------------------------------------------------------------ W10 offer
test('W10 a holding under a cash offer: no stop rule, the offer\'s own', () => {
  const ms = OPEN25 + 1800000;
  const w = world(ON, book([DDI])); w.charts = { SPY: spyLive(ms), DDI: live(ms, 14.90, 15.50) };   // under its ledger stop 15.22, 8.3% under the offer
  const t = tick(w, ms);
  eq(t.mails.length, 1); eq(t.mails[0].subject, 'TFB risk watch: DDI 8.3% under the offer');
  ok(/^- DDI\.US \(cash offer 16\.25, the sale is the owner's decision\): 14\.90 now, 8\.3% under the offer price\. A widening gap can mean doubt about the offer; no stop order is asked for on this holding\. Today -3\.9%; S&P 500 fund \+0\.0%\./.test(t.mails[0].body), t.mails[0].body);
  ok(!/stop working|Exit Plan stop/.test(t.mails[0].body), 'no stop line for it');
  // a 3% fall alone is enough; a small one is not; near the offer price is its own alert
  const one = (px, prev) => { const x = world(ON, book([DDI])); x.charts = { SPY: spyLive(ms), DDI: live(ms, px, prev) }; return tick(x, ms).mails; };
  eq(one(15.00, 15.50)[0].subject, 'TFB risk watch: DDI 7.7% under the offer'); eq(one(15.30, 15.50).length, 0);
  eq(one(16.20, 15.50)[0].subject, 'TFB risk watch: DDI at the offer price'); ok(/16\.20 now, within 0\.3% of the offer price\./.test(one(16.20, 15.50)[0].body));
  ok(/16\.30 now, at or above the offer price\./.test(one(16.30, 15.50)[0].body));
  // before the open an offer holding has no open-risk line, even with a stop listed
  const w2 = world(ON, book([Object.assign({}, DDI, { broker: 15.22 })])); w2.charts = { SPY: spyPre(Z(25, 13, 20)), DDI: closed(15.50) };
  eq(tick(w2, Z(25, 13, 20)).mails.length, 0);
});

// ------------------------------------------------------------------ W11 old price; other venues
test('W11 an old price fires nothing; a listing outside the US is shown, not watched', () => {
  const ms = OPEN25 + 3600000;
  const w = world(ON, book([IBOC])); w.charts = { SPY: spyLive(ms), IBOC: chart({ price: 62.0, at: ms - 40 * 60000, prev: 63.77 }) };
  eq(tick(w, ms).mails.length, 0); eq(statusOf(w, 'IBOC.US')[9], 'no fresh trade: last price only'); eq(statusOf(w, 'IBOC.US')[3], '');
  // a price from before today's open is not a trade of this session
  const w1 = world(ON, book([IBOC])); w1.charts = { SPY: spyLive(OPEN25 + 120000), IBOC: closed(62.0, 63.77) };
  eq(tick(w1, OPEN25 + 120000).mails.length, 0);
  const SR = { symbol: '2222.SR', qty: 100, entry: 27.0, close: 26.0, atrPct: 1.0, stop: 25.9, target: 29.0, broker: 25.9 };
  const w2 = world(ON, book([SR, IBOC])); w2.charts = { SPY: spyLive(ms), IBOC: live(ms, 64.5, 63.77) };
  const t = tick(w2, ms);
  eq(t.fetches.map(f => f.sym).join(','), 'SPY,IBOC'); eq(statusOf(w2, '2222.SR')[9], 'not watched: listed outside the US'); eq(statusOf(w2, 'IBOC.US')[9], 'watched'); eq(t.mails.length, 0);
});

// ------------------------------------------------------------------ W12 health
test('W12 its own health: prices and levels', () => {
  const ms = OPEN25 + 600000;
  // the second host answers when the first refuses
  const w = world(ON, book([IBOC])); w.charts = { SPY: spyLive(ms), IBOC: live(ms, 64.5, 63.77) }; w.http = { 'query1.finance.yahoo.com': 429 };
  const t = tick(w, ms);
  eq(t.fetches.map(f => f.host.slice(0, 6) + ':' + f.sym).join(','), 'query1:SPY,query1:IBOC,query2:SPY,query2:IBOC'); eq(statusOf(w, 'IBOC.US')[9], 'watched'); eq(state(w).fail, 0);
  // no host answers: the third run in a row says so, once
  const x = world(ON, book([IBOC])); x.http = { 'query1.finance.yahoo.com': 429, 'query2.finance.yahoo.com': 429 };
  eq(tick(x, ms).mails.length, 0); eq(tick(x, ms + 300000).mails.length, 0);
  const h3 = tick(x, ms + 600000);
  eq(h3.mails.length, 1); eq(h3.mails[0].subject, 'TFB risk watch: prices are not readable');
  ok(/^- Risk watch cannot read prices \(3 runs in a row; last answer: HTTP 429\)\. No alert can be raised until it can\./.test(h3.mails[0].body), h3.mails[0].body);
  eq(tick(x, ms + 900000).mails.length, 0); eq(state(x).fail, 4); eq(statusOf(x, 'IBOC.US')[9], 'no price (HTTP 429)');
  x.http = {}; x.charts = { SPY: spyLive(ms + 1200000), IBOC: live(ms + 1200000, 64.5, 63.77) };
  tick(x, ms + 1200000); eq(state(x).fail, 0);
  const y = world(ON, book([IBOC])); y.fetchThrows = true;
  tick(y, ms); tick(y, ms + 300000); ok(/last answer: no answer/.test(tick(y, ms + 600000).mails[0].body));
  // the Exit Plan page: missing, without its table, old
  const a = world(ON, {}); a.charts = { SPY: spyLive(ms) };
  const ta = tick(a, ms);
  eq(ta.mails[0].subject, 'TFB risk watch: the Exit Plan levels are not readable'); ok(/Exit_Plan tab missing or empty/.test(ta.mails[0].body)); eq(tick(a, ms + 300000).mails.length, 0);
  const b = world(ON, { Exit_Plan: page([IBOC], { hdr: ['Sym', 'Level'] }), _Exit_Plan: ledger([IBOC]) }); b.charts = { SPY: spyLive(ms) };
  ok(/Exit_Plan HOLDINGS header not recognised/.test(tick(b, ms).mails[0].body));
  const c = world(ON, book([IBOC], { asOf: '2026-11-19 08:35' })); c.charts = { SPY: spyLive(ms), IBOC: live(ms, 64.5, 63.77) };
  const tc = tick(c, ms);
  eq(tc.mails[0].subject, 'TFB risk watch: the Exit Plan levels are old'); ok(/was last written 2026-11-19 08:35 Riyadh \(6 days ago\)/.test(tc.mails[0].body), tc.mails[0].body);
  const d = world(ON, book([IBOC], { asOf: '2026-11-21 08:35' })); d.charts = { SPY: spyLive(ms), IBOC: live(ms, 64.5, 63.77) };
  eq(tick(d, ms).mails.length, 0);                                      // 4.4 days: a long weekend is not old
  // an all-cash book is not a fault
  const e = world(ON, book([])); e.charts = { SPY: spyLive(ms) };
  const te = tick(e, ms); eq(te.mails.length, 0); eq(te.ret, 'session: nothing new'); eq(statusRows(e).length, 0);
});

// ------------------------------------------------------------------ W13 observe, failures, cap, recipient, lock
test('W13 observe; a failed e-mail is tried again; the daily cap; no recipient; the lock', () => {
  const ms = OPEN25 + 600000, charts = (t) => ({ SPY: spyLive(t), IBOC: live(t, 63.20, 63.77) });
  const o = world({ TFB_RISK_WATCH: 'observe' }, book([IBOC])); o.charts = charts(ms);
  const to = tick(o, ms);
  eq(to.ret, 'session: 1 alert(s) recorded (observe)'); eq(to.mails.length, 0); eq(logRows(o)[0][6], 'observe: not sent'); ok(state(o).sent['IBOC.US|NEAR'] > 0);
  o.charts = charts(ms + 300000); eq(tick(o, ms + 300000).ret, 'session: nothing new'); eq(logRows(o).length, 1);
  // the e-mail service refuses: logged once, tried again, sent when it works
  const f = world(ON, book([IBOC])); f.charts = charts(ms); f.mailThrows = 'Service invoked too many times for one day: email.';
  eq(tick(f, ms).ret, 'session: 1 alert(s) NOT sent: e-mail failed: Service invoked too many times for one day: email.');
  ok(!state(f).sent['IBOC.US|NEAR'] && state(f).mails === 0); eq(logRows(f).length, 1);
  f.charts = charts(ms + 300000); tick(f, ms + 300000); eq(logRows(f).length, 1);
  f.mailThrows = ''; f.charts = charts(ms + 600000);
  const tf = tick(f, ms + 600000);
  eq(tf.mails.length, 1); ok(state(f).sent['IBOC.US|NEAR'] > 0); eq(logRows(f).length, 2); eq(logRows(f)[1][6], 'sent');
  // the cap: the last e-mail says so; the next alert is on the tab and the log only
  const c = world({ TFB_RISK_WATCH: 'on', TFB_RISK_WATCH_CFG: '{"mail_cap":1}' }, book([IBOC, GLPI]));
  c.charts = { SPY: spyLive(ms), IBOC: live(ms, 63.20, 63.77), GLPI: live(ms, 39.5, 39.01) };
  const c1 = tick(c, ms);
  eq(c1.mails.length, 1); ok(/This is e-mail 1 of 1 today: further alerts today are on the Risk_Watch tab only\./.test(c1.mails[0].body), c1.mails[0].body);
  c.charts = { SPY: spyLive(ms + 300000), IBOC: live(ms + 300000, 63.20, 63.77), GLPI: live(ms + 300000, 42.5, 39.01) };
  const c2 = tick(c, ms + 300000);
  eq(c2.mails.length, 0); eq(c2.ret, 'session: 1 alert(s) NOT sent: daily cap of 1 e-mails reached'); ok(state(c).sent['GLPI.US|TARGET'] > 0);
  eq(logRows(c)[1][6], 'NOT sent: daily cap of 1 e-mails reached');
  c.charts = { SPY: spyLive(ms + 600000), IBOC: live(ms + 600000, 63.20, 63.77), GLPI: live(ms + 600000, 42.5, 39.01) };
  eq(tick(c, ms + 600000).ret, 'session: nothing new');
  // recipient: the watch's own property, then the page's, then the owner; nobody -> said, not thrown
  const r = world({ TFB_RISK_WATCH: 'on', TFB_RISK_WATCH_TO: 'a@x.com', DP2_EXIT_ALERT_TO: 'b@x.com' }, book([IBOC])); r.charts = charts(ms); eq(tick(r, ms).mails[0].to, 'a@x.com');
  const r2 = world({ TFB_RISK_WATCH: 'on', DP2_EXIT_ALERT_TO: 'b@x.com' }, book([IBOC])); r2.charts = charts(ms); eq(tick(r2, ms).mails[0].to, 'b@x.com');
  const r3 = world(ON, book([IBOC])); r3.charts = charts(ms); r3.owner = '';
  eq(tick(r3, ms).ret, 'session: 1 alert(s) NOT sent: no recipient - set TFB_RISK_WATCH_TO'); ok(!state(r3).sent['IBOC.US|NEAR']);
  // another run holds the lock
  const l = world(ON, book([IBOC])); l.charts = charts(ms); l.lockBusy = true;
  const tl = tick(l, ms); eq(tl.ret, 'busy'); eq(tl.fetches.length, 0); eq(tl.mails.length, 0);
  // limits from the property: a wider near band
  const n = world({ TFB_RISK_WATCH: 'on', TFB_RISK_WATCH_CFG: '{"near_pct":3}' }, book([IBOC])); n.charts = { SPY: spyLive(ms), IBOC: live(ms, 64.20, 63.77) };
  eq(tick(n, ms).mails[0].subject, 'TFB risk watch: IBOC 2.4% above its stop');
});

// ------------------------------------------------------------------ W14 owner functions; summer time
test('W14 owner functions; summer time', () => {
  const w = world({}, book([IBOC, GLPI]));
  const e = env(SRC, w, Z(26, 7, 0));
  const msg = e.tfbInstallRiskWatchTrigger();
  eq(w.triggers.length, 1); eq(w.triggers[0].h, 'rwTick'); eq(w.triggers[0].every, 5); eq(w.props.TFB_RISK_WATCH, 'on');
  ok(/^Risk watch installed: rwTick every 5 min\. TFB_RISK_WATCH = on \(set to on\)\. E-mails go to owner@example\.com\./.test(msg), msg);
  e.tfbInstallRiskWatchTrigger(); eq(w.triggers.length, 1);                                  // replaced, not doubled
  const w10 = world({ TFB_RISK_WATCH_EVERY_MIN: '10' }, {}); ok(/every 10 min/.test(env(SRC, w10, Z(26, 7, 0)).tfbInstallRiskWatchTrigger())); eq(w10.triggers[0].every, 10);
  const w7 = world({ TFB_RISK_WATCH_EVERY_MIN: '7' }, {}); env(SRC, w7, Z(26, 7, 0)).tfbInstallRiskWatchTrigger(); eq(w7.triggers[0].every, 5);   // not a value Apps Script accepts
  const w2 = world({ TFB_RISK_WATCH: 'observe' }, {}); env(SRC, w2, Z(26, 7, 0)).tfbInstallRiskWatchTrigger(); eq(w2.props.TFB_RISK_WATCH, 'observe');
  ok(/^RISK WATCH v1\.0\.0 \| mode=on \| triggers=1 \| to=owner@example\.com \| phase now=idle \| e-mails today=0 \| alerts today=none \| runs without a price=0 \| mail quota=90$/.test(e.tfbRiskWatchStatus()), e.tfbRiskWatchStatus());
  ok(/^Removed 1 risk-watch trigger/.test(e.tfbRemoveRiskWatchTrigger())); eq(w.triggers.length, 0); ok(/^No risk-watch trigger/.test(e.tfbRemoveRiskWatchTrigger()));
  // a manual run on the holiday morning: the tab is filled, nothing is sent
  w.charts = { SPY: chart({ price: 590, at: Z(25, 21, 0), prev: 588 }), IBOC: chart({ price: 63.56, at: Z(25, 21, 0), prev: 63.77 }), GLPI: chart({ price: 39.01, at: Z(25, 21, 0), prev: 39.0 }) };
  const r = env(SRC, w, Z(26, 7, 0)).tfbRiskWatchNow();
  eq(r, 'manual: nothing new'); eq(w.mails.length, 0); eq(statusRows(w).length, 2); eq(statusOf(w, 'IBOC.US')[1], 63.56); eq(statusOf(w, 'IBOC.US')[6], 1.4);
  ok(/outside market hours \(manual run\) - market: not trading/.test(w.sheets.Risk_Watch[1][0]));
  // with the gate off a manual run looks and sends nothing
  const off = world({}, book([IBOC])); off.charts = { SPY: spyLive(OPEN25 + 600000), IBOC: live(OPEN25 + 600000, 62.60, 63.77) };
  eq(env(SRC, off, OPEN25 + 600000).tfbRiskWatchNow(), 'session: 1 alert(s) recorded (observe)'); eq(off.mails.length, 0); eq(logRows(off)[0][1], 'observe');
  // the test e-mail
  const tm = world({}, {}); ok(/^Test e-mail sent to owner@example\.com\.$/.test(env(SRC, tm, Z(26, 7, 0)).tfbRiskWatchTestMail())); eq(tm.mails[0].subject, 'TFB risk watch: TEST');
  tm.owner = ''; ok(/^No recipient/.test(env(SRC, tm, Z(26, 7, 0)).tfbRiskWatchTestMail())); eq(tm.mails.length, 1);
  // summer time: Mon 15 Jun 2026, New York is UTC-4 - the open is 13:30 UTC, the watch starts 12:15 UTC
  const J = (h, m) => Date.UTC(2026, 5, 15, h, m);
  eq(tick(world(ON, book([])), J(12, 14)).ret, 'idle'); eq(tick(world(ON, book([])), J(12, 15)).ret.split(':')[0], 'pre');
  eq(tick(world(ON, book([])), J(13, 30)).ret.split(':')[0], 'session'); eq(tick(world(ON, book([])), J(20, 11)).ret, 'idle');
});

// ------------------------------------------------------------------ W15 mutants
test('W15 mutants: each guard removed is caught', () => {
  const mutate = (from, to) => { ok(SRC.split(from).length === 2, 'mutation site not unique: ' + from.slice(0, 50)); return SRC.replace(from, to); };
  const ms = OPEN25 + 600000;
  // (a) the trading-day proof: without it a holiday's stale price fires
  const a = mutate("if (!h || !mkt || !mkt.tradingDay) return out;", "if (!h || !mkt) return out;");
  const wa = world(ON, book([IBOC])); wa.charts = { SPY: closed(590), IBOC: closed(63.77) };                // Thanksgiving morning
  eq(tick(wa, Z(26, 13, 20)).mails.length, 0);
  const wa2 = world(ON, book([IBOC])); wa2.charts = wa.charts;
  eq(tick(wa2, Z(26, 13, 20), { src: a }).mails.length, 1, 'mutant a not caught');
  // (b) once a day: without the sent check every run mails
  const b = mutate("if (st.sent.hasOwnProperty(a.key)) continue;", "");
  const wb = world(ON, book([IBOC])); wb.charts = { SPY: spyLive(ms), IBOC: live(ms, 63.20, 63.77) };
  tick(wb, ms, { src: b }); wb.charts = { SPY: spyLive(ms + 300000), IBOC: live(ms + 300000, 63.20, 63.77) };
  eq(tick(wb, ms + 300000, { src: b }).mails.length, 1, 'mutant b not caught');
  // (c) freshness: without it a 40-minute-old price fires
  const c = mutate("if (age > lim.fresh_min * 60000 || age < -60000 || q.priceMs < q.regStartMs) return out;", "");
  const wc = world(ON, book([IBOC])); wc.charts = { SPY: spyLive(ms + 3600000), IBOC: chart({ price: 62.0, at: ms + 3600000 - 40 * 60000, prev: 63.77 }) };
  eq(tick(wc, ms + 3600000, { src: c }).mails.length, 1, 'mutant c not caught');
  // (d) an offer holding keeps out of the stop rules
  const d = mutate("  if (h.offer) {\n    if (h.target !== null && h.target > 0) {", "  if (false) {\n    if (h.target !== null && h.target > 0) {");
  const wd = world(ON, book([Object.assign({}, DDI, { broker: 15.22 })])); wd.charts = { SPY: spyLive(ms), DDI: live(ms, 15.20, 15.50) };
  eq(tick(wd, ms).mails.length, 0);
  const wd2 = world(ON, book([Object.assign({}, DDI, { broker: 15.22 })])); wd2.charts = wd.charts;
  ok(/DDI at or below its stop/.test(tick(wd2, ms, { src: d }).mails[0].subject), 'mutant d not caught');
  // (e) the gate: without it an unset property would run
  const e = mutate("    if (!force) return 'off';", "");
  const we = world({}, book([IBOC])); we.charts = { SPY: spyLive(ms), IBOC: live(ms, 63.20, 63.77) };
  ok(tick(we, ms, { src: e }).ret !== 'off', 'mutant e not caught');
});

console.log((failed === 0 ? 'PASS ' : 'FAIL ') + passed + '/' + (passed + failed));
process.exit(failed === 0 ? 0 : 1);
