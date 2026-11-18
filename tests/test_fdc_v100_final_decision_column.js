#!/usr/bin/env node
/* tests/test_fdc_v100_final_decision_column.js
 * Harness for apps_script/31_Final_Decision_Column.gs v1.0.0: the FINAL DECISION column on Top_10_Investments,
 * Portfolio_Decision and My_Portfolio (owner, Mon 16 Nov 2026: one final decision per stock on every page).
 * The REAL file runs in a vm with a small Sheets shim. Every symbol and row below is synthetic.
 *   S1  Top_10_Investments: the column sits one column right of the seats table; a formula on every seat row and
 *       nowhere else; every other cell is unchanged; outside the range the Today Action Plan reads (A1:AE60)
 *   S2  Portfolio_Decision: only the ACTIONS table rows; outside A1:AA150
 *   S3  My_Portfolio: no column (its refresh and validator count the columns), a note on its engine decision headers
 *   S4  a second tick reuses the column (no second header), keeps the page's own colour rules, replaces its own
 *   S5  the header: the time, OLD past FDC_STALE_H, "not read" without the tab; formulas look up their own row
 *   S6  the lock: a busy lock writes nothing; install / status / remove trigger; fdcRemoveColumn rolls back
 *   S7  the self-test core, ES5 / ASCII, version lockstep
 *   S11 (1.3.0) Portfolio_Decision: the two Holding_Read columns directly right of FINAL DECISION, a formula pair on
 *       every holding row and nowhere else; the page's own cells unchanged; Top_10_Investments gets neither
 *   S12 (1.3.0) the Holding_Read columns are found by header name; the header carries the session, OLD past 5 days,
 *       "not read" without the tab or the table; a holding not on the page reads n/a
 *   S13 (1.3.0) a second tick reuses the columns; colour rules; a header left elsewhere is cleared; rebuild; rollback
 * The last line is 'PASS k/k' (exit 1 on any failure).
 */
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const SRC = fs.readFileSync(process.argv[2] || path.join(ROOT, 'apps_script', '31_Final_Decision_Column.gs'), 'utf8');
const NOW = Date.UTC(2026, 10, 16, 6, 0);   // 09:00 Riyadh

let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n  ')); }
}
function ok(c, m) { if (!c) throw new Error(m || 'assertion failed'); }
function eq(a, b, m) { if (a !== b) throw new Error((m || 'not equal') + ': ' + JSON.stringify(a) + ' !== ' + JSON.stringify(b)); }

function makeSheet(name, grid) {
  const cells = new Map(), key = (r, c) => r + ':' + c;
  let maxCols = Math.max(26, ...grid.map(r => r.length)), rules = [];
  const widths = {};
  grid.forEach((row, i) => row.forEach((v, j) => { if (v !== '' && v !== null && v !== undefined) cells.set(key(i + 1, j + 1), { v }); }));
  const cell = (r, c) => { let x = cells.get(key(r, c)); if (!x) { x = {}; cells.set(key(r, c), x); } return x; };
  const has = x => x && ((x.v !== undefined && x.v !== '') || x.f);
  const sh = {
    _cells: cells, _rules: () => rules, _widths: widths,
    getName: () => name,
    getLastRow() { let m = 0; for (const [k, x] of cells) if (has(x)) m = Math.max(m, +k.split(':')[0]); return m; },
    getLastColumn() { let m = 0; for (const [k, x] of cells) if (has(x)) m = Math.max(m, +k.split(':')[1]); return m; },
    getMaxColumns: () => maxCols,
    insertColumnsAfter(a, n) { maxCols += n; },
    setColumnWidth(c, w) { widths[c] = w; },
    getConditionalFormatRules: () => rules.slice(),
    setConditionalFormatRules(rs) { rules = rs.slice(); },
    getRange(r, c, nr, nc) {
      nr = nr || 1; nc = nc || 1;
      if (c + nc - 1 > maxCols) throw new Error('beyond the sheet: column ' + (c + nc - 1) + ' > ' + maxCols);
      const self = {
        getRow: () => r, getColumn: () => c, getNumRows: () => nr, getNumColumns: () => nc,
        getValues() { return Array.from({ length: nr }, (_, i) => Array.from({ length: nc }, (_, j) => { const x = cells.get(key(r + i, c + j)); return x && x.v !== undefined ? x.v : ''; })); },
        setValue(v) { const x = cell(r, c); x.v = v; delete x.f; return self; },
        setFormulas(g) { if (g.length !== nr || g.some(row => row.length !== nc)) throw new Error('shape'); g.forEach((row, i) => row.forEach((f, j) => { const x = cell(r + i, c + j); if (f) { x.f = f; x.v = ''; } else { delete x.f; x.v = ''; } })); return self; },
        clearContent() { for (let i = 0; i < nr; i++) for (let j = 0; j < nc; j++) { const x = cells.get(key(r + i, c + j)); if (x) { delete x.f; x.v = ''; } } return self; },
        clear() { for (let i = 0; i < nr; i++) for (let j = 0; j < nc; j++) cells.delete(key(r + i, c + j)); return self; },
        setNote(t) { const x = cell(r, c); if (t) x.note = t; else delete x.note; return self; }
      };
      ['setFontWeight', 'setFontColor', 'setBackground', 'setWrap', 'setVerticalAlignment', 'setFontSize'].forEach(m => { self[m] = v => { for (let i = 0; i < nr; i++) for (let j = 0; j < nc; j++) cell(r + i, c + j)[m] = v; return self; }; });
      return self;
    }
  };
  return sh;
}
function makeEnv(sheets, opts) {
  opts = opts || {};
  const book = { _s: new Map(), toasts: [], getSheetByName(n) { return this._s.get(n) || null; }, toast(m) { this.toasts.push(String(m)); } };
  for (const n of Object.keys(sheets)) book._s.set(n, makeSheet(n, sheets[n]));
  const props = Object.assign({}, opts.props || {}), triggers = [], logs = [];
  class FakeDate extends Date { constructor(...a) { if (a.length === 0) super(NOW); else super(...a); } static now() { return NOW; } }
  const ruleBuilder = () => { const spec = {}; const b = { whenTextStartsWith(t) { spec.text = t; return b; }, setBackground(v) { spec.bg = v; return b; }, setFontColor(v) { spec.fg = v; return b; }, setRanges(rs) { spec.ranges = rs; return b; }, build() { return { spec, getRanges: () => spec.ranges }; } }; return b; };
  const ctx = {
    Date: FakeDate,
    SpreadsheetApp: { getActive: () => book, newConditionalFormatRule: ruleBuilder },
    PropertiesService: { getScriptProperties: () => ({ getProperty: k => (k in props ? props[k] : null), setProperty: (k, v) => { props[k] = String(v); }, deleteProperty: k => { delete props[k]; } }) },
    LockService: { getScriptLock: () => ({ tryLock: () => !opts.busy, releaseLock() {} }) },
    ScriptApp: {
      getProjectTriggers: () => triggers.slice(),
      deleteTrigger(t) { const i = triggers.indexOf(t); if (i >= 0) triggers.splice(i, 1); },
      newTrigger(h) { const t = { h, every: null, after: null, getHandlerFunction: () => h }; const b = { timeBased() { return b; }, everyMinutes(n) { t.every = n; return b; }, after(ms) { t.after = ms; return b; }, create() { triggers.push(t); return t; } }; return b; }
    },
    Logger: { log(m) { logs.push(String(m)); } }
  };
  vm.createContext(ctx);
  vm.runInContext(SRC, ctx, { filename: '31_Final_Decision_Column.gs' });
  return { ctx, book, props, triggers, logs };
}
const pad = (row, n) => { const r = row.slice(); while (r.length < n) r.push(''); return r; };
const T10_HEAD = ['Rank', 'Symbol', 'Name', 'Market', 'Sector', 'Ccy', 'FX', 'Price', 'Price SAR', 'Entry Zone', 'Ticket SAR', 'Shares', 'Stop SAR', 'TP1 SAR', 'TP2 SAR',
  'ROI % (TP1)', 'Engine ROI % (12M)', 'Ann ROI %', 'Gain 12M SAR', 'Rel', 'DQ', 'Conf', 'Funds From', 'Review By', 'Stability', 'Days', 'Since', 'Sm Score', 'Trend', 'Advisor Note'];
function top10() {
  const g = [['TOP 10 INVESTMENTS'], ['Status', 'Last run 2026-11-16 08:30'], [''], ['CONTROL PANEL'], ['Cash Available (SAR)', 17547]];
  while (g.length < 17) g.push(['']);
  g.push(['FEED NOT ACTIONABLE banner']); g.push(T10_HEAD);
  [['AX.US', 0, 0], ['MBIN.US', 0, 0], ['TEN.US', 5117, 29]].forEach((s, i) => g.push(pad([i + 1, s[0], 'Name ' + i, 'NYSE', 'Fin', 'USD', 3.75, 90, 337.5, '88-92', s[1], s[2]], 30)));
  g.push(['']); g.push(['ALERTS']); g.push(['Type', 'Count', 'Required action']); g.push(['STALE', 1, 'refresh']);
  return g.map(r => pad(r, 30));
}
const PD_HEAD = ['Action', 'Symbol', 'Name', 'Sector', 'Ccy', 'Qty', 'Avg Cost', 'Price', 'Price SAR', 'MV SAR', 'Cost SAR', 'P&L SAR', 'P&L %', 'Weight %', 'D SAR', 'D Shares',
  'Funds From', 'Stop SAR', 'TP1 SAR', 'TP2 SAR', 'ROI %', 'Engine ROI %', 'Rel', 'DQ', 'Conf', 'Advisor Note'];
function pd() {
  return [['MY PORTFOLIO - DECISION'], ['Status:', 'Last run 2026-11-16 08:19'], [''], ['ACTIONS - one per holding (3)'], PD_HEAD,
    pad(['TRIM', 'DDI.US', 'DoubleDown'], 26), pad(['HOLD', 'SUPN.US', 'Supernus'], 26), pad(['HOLD', 'GLPI.US', 'Gaming'], 26), [''],
    ['SECTOR SUMMARY (2)'], ['Sector', 'Weight'], ['Fin', 0.2]].map(r => pad(r, 26));
}
function mp() {
  const head = ['Symbol', 'Name', 'Recommendation', 'Position Qty', 'Final Action', 'User Notes'];
  return [head, ['SUPN.US', 'Supernus', 'BUY', 95, 'ACCUMULATE', ''], ['DDI.US', 'DoubleDown', 'HOLD', 240, 'HOLD', ''], ['', '', '', '', '', ''],
    ['1120.SR', 'Al Rajhi', 'WATCH', '', 'WATCH', 'mine'], ['TOTAL ROW', '', '', '', '', '']];
}
function finals(at) {
  return [['Symbol', 'Decision', 'Do today', 'Qty', 'Limit', 'Stop', 'Cost SAR', 'Why', 'Rule of record', 'Other', 'Analysis differs on', 'Decided At (Riyadh)', 'Version'],
    ['AX.US', 'BUY', 'Buy 8 AX.US ...', 8, '', 83.88, 2735, '', 'Daily Plan', '', 'none', at || '2026-11-16 08:41', '1.6.1'],
    ['SUPN.US', 'HOLD', 'Hold; stop 24.28.', 95, '', 24.28, '', '', 'Exit Plan', '', 'none', at || '2026-11-16 08:41', '1.6.1']];
}
const world = o => Object.assign({ Top_10_Investments: top10(), Portfolio_Decision: pd(), My_Portfolio: mp(), Final_Decisions: finals() }, o || {});
const snapshot = (sh, maxCol) => { const out = {}; for (const [k, x] of sh._cells) if (+k.split(':')[1] <= maxCol) out[k] = JSON.stringify(x); return out; };
const colOf = (sh, re) => { for (const [k, x] of sh._cells) if (typeof x.v === 'string' && re.test(x.v)) return k.split(':').map(Number); return null; };
const formulaRows = (sh, col) => { const out = []; for (const [k, x] of sh._cells) { const [r, c] = k.split(':').map(Number); if (c === col && x.f) out.push(r); } return out.sort((a, b) => a - b); };

test('S1 Top_10_Investments: one column right of the seats table, a formula per seat, nothing else changes', () => {
  const env = makeEnv(world());
  const t10 = env.book.getSheetByName('Top_10_Investments');
  const before = snapshot(t10, 30);
  const line = env.ctx.fdcTick();
  ok(/Top_10_Investments: 3 row\(s\) in column AF/.test(line), line);
  const [hr, hc] = colOf(t10, /^FINAL DECISION/);
  eq(hr, 19, 'the header row'); eq(hc, 32, 'column AF: one empty column after the 30-column table');
  eq(formulaRows(t10, 32).join(','), '20,21,22', 'the three seat rows');
  ok(t10._cells.get('20:32').f.indexOf('INDIRECT("R"&ROW()&"C2",FALSE)') > 0, 'the formula reads its own row\'s Symbol (column B)');
  const after = snapshot(t10, 30); delete after['3:1'];   // 1.1.0: A3 carries the today line (S8)
  eq(JSON.stringify(after), JSON.stringify(before), 'every other cell of the page itself is unchanged');
  ok(hc > 31, 'outside A1:AE60, the range the Today Action Plan reads');
  eq(t10._widths[32], 330);
});

test('S2 Portfolio_Decision: only the ACTIONS rows, outside A1:AA150', () => {
  const env = makeEnv(world());
  env.ctx.fdcTick();
  const sh = env.book.getSheetByName('Portfolio_Decision');
  const [hr, hc] = colOf(sh, /^FINAL DECISION/);
  eq(hr, 5); eq(hc, 28, 'column AB'); ok(hc > 27, 'outside A1:AA150');
  eq(formulaRows(sh, 28).join(','), '6,7,8', 'the three action rows, not the sector table');
});

test('S3 My_Portfolio: no column (schema-checked width), a note on its engine decision headers (Codex P2 x2, #1001)', () => {
  const env = makeEnv(world());
  const sh = env.book.getSheetByName('My_Portfolio');
  const before = JSON.stringify(Array.from(sh._cells.entries()).map(([k, x]) => [k, x.v, x.f]));
  const line = env.ctx.fdcTick();
  ok(/My_Portfolio: 2 header note\(s\), no column/.test(line), line);
  eq(JSON.stringify(Array.from(sh._cells.entries()).map(([k, x]) => [k, x.v, x.f])), before, 'no value, formula or header changes');
  eq(sh.getLastColumn(), 6, 'the width the validator expects');
  ok(/^Engine field \(analysis\), not the decision/.test(sh._cells.get('1:3').note) && sh._cells.get('1:5').note === sh._cells.get('1:3').note, 'Recommendation and Final Action');
  ok(!sh._cells.get('1:1').note && !sh._cells.get('1:4').note, 'no other header');
  // the finding's two writers: a refresh that rewrites every row by its values, then a sort, cannot misplace anything
  env.ctx.fdcTick();
  eq(sh.getLastColumn(), 6, 'a second tick adds nothing');
});

test('S4 a second tick reuses the column and replaces only its own colour rules', () => {
  const env = makeEnv(world());
  const sh = env.book.getSheetByName('Top_10_Investments');
  const other = { spec: { text: 'theirs' }, getRanges: () => [sh.getRange(20, 11, 3, 1)] };
  sh.setConditionalFormatRules([other]);
  env.ctx.fdcTick(); env.ctx.fdcTick();
  const heads = []; for (const [k, x] of sh._cells) if (typeof x.v === 'string' && /^FINAL DECISION/.test(x.v)) heads.push(k);
  eq(heads.join(','), '19:32', 'one header');
  const rules = sh._rules();
  eq(rules.length, 7, 'the page\'s own rule + six of ours');
  ok(rules[0] === other, 'the page\'s rule kept first');
  eq(rules.slice(1).map(r => r.spec.text).join('|'), 'SELL|BUY|HOLD|WATCH|DO NOT INVEST|n/a');
  // a rebuilt page (cleared) gets the column back
  const fresh = makeEnv(world());
  fresh.ctx.fdcTick();
  const t = fresh.book.getSheetByName('Top_10_Investments');
  t.getRange(1, 1, 40, 32).clear();
  top10().forEach((row, i) => row.forEach((v, j) => { if (v !== '') t.getRange(i + 1, j + 1).setValue(v); }));
  fresh.ctx.fdcTick();
  eq(formulaRows(t, 32).join(','), '20,21,22', 'back after a rebuild');
});

test('S5 the header: time, OLD past the limit, not read without the tab', () => {
  let env = makeEnv(world());
  env.ctx.fdcTick();
  let sh = env.book.getSheetByName('Top_10_Investments');
  eq(sh._cells.get('19:32').v, 'FINAL DECISION · Today Action Plan 2026-11-16 08:41');
  eq(sh._cells.get('19:32').setBackground, '#0f5c63');
  env = makeEnv(world({ Final_Decisions: finals('2026-11-16 01:00') }));
  env.ctx.fdcTick();
  sh = env.book.getSheetByName('Top_10_Investments');
  eq(sh._cells.get('19:32').v, 'FINAL DECISION · Today Action Plan 2026-11-16 01:00 · OLD (8 h): take orders from Today_Action_Plan');
  env = makeEnv(world({ Final_Decisions: finals('2026-11-16 01:00') }), { props: { FDC_STALE_H: '12' } });
  env.ctx.fdcTick();
  ok(!/OLD/.test(env.book.getSheetByName('Top_10_Investments')._cells.get('19:32').v), 'FDC_STALE_H widens the limit');
  const w = world(); delete w.Final_Decisions;
  env = makeEnv(w);
  const line = env.ctx.fdcTick();
  ok(/Final_Decisions not read/.test(line), line);
  eq(env.book.getSheetByName('Top_10_Investments')._cells.get('19:32').v, 'FINAL DECISION · Final_Decisions not read (tab missing or empty): take orders from Today_Action_Plan');
  eq(formulaRows(env.book.getSheetByName('Top_10_Investments'), 32).length, 3, 'the formulas still go in: they read n/a until the tab exists');
  eq(env.ctx.fdcFormula_(2), '=IFERROR(VLOOKUP(TRIM(INDIRECT("R"&ROW()&"C2",FALSE)),Final_Decisions!$A:$C,2,FALSE)&" — "&VLOOKUP(TRIM(INDIRECT("R"&ROW()&"C2",FALSE)),Final_Decisions!$A:$C,3,FALSE),"n/a — not on Final_Decisions (no order)")');
});

test('S6 the lock, the trigger and the rollback', () => {
  let env = makeEnv(world(), { busy: true });
  ok(/^busy/.test(env.ctx.fdcTick()), 'busy');
  ok(!colOf(env.book.getSheetByName('Top_10_Investments'), /^FINAL DECISION/), 'nothing written under a busy lock');
  env = makeEnv(world());
  env.ctx.fdcInstallTrigger(); env.ctx.fdcInstallTrigger();
  eq(env.triggers.length, 1, 'one trigger'); eq(env.triggers[0].h, 'fdcTick'); eq(env.triggers[0].every, 10);
  ok(colOf(env.book.getSheetByName('Portfolio_Decision'), /^FINAL DECISION/), 'install stamps the pages now');
  ok(/trigger installed .* self-test ok .* last tick: 2026-11-16T06:00Z/.test(env.ctx.fdcStatus()), env.ctx.fdcStatus());
  const out = env.ctx.fdcRemoveColumn();
  eq(out.join(' | '), 'Top_10_Investments: removed from column AF | Portfolio_Decision: removed from column AB and 2 Holding Read column(s) | My_Portfolio: 2 note(s) removed | A3 today line removed');
  ok(!colOf(env.book.getSheetByName('Portfolio_Decision'), /^(LAST SESSION|NEXT SESSION READ)/), 'the Holding Read columns are gone too');
  ok(!env.book.getSheetByName('My_Portfolio')._cells.get('1:3').note, 'the note is gone');
  eq(env.book.getSheetByName('Top_10_Investments')._cells.get('3:1').v, '', 'the today line is gone');
  ok(!colOf(env.book.getSheetByName('Top_10_Investments'), /^FINAL DECISION/) && formulaRows(env.book.getSheetByName('Top_10_Investments'), 32).length === 0, 'gone');
  eq(env.ctx.fdcRemoveTrigger(), 1); eq(env.triggers.length, 0);
  // a missing page or table is reported, the others are still stamped
  const w = world(); delete w.Portfolio_Decision; w.My_Portfolio = [['no table here'], ['x']];
  const line = makeEnv(w).ctx.fdcTick();
  ok(/Portfolio_Decision: tab missing/.test(line) && /My_Portfolio: 0 header note\(s\), no column/.test(line) && /Top_10_Investments: 3 row/.test(line), line);
});

test('S7 the self-test core, ES5 / ASCII, version lockstep', () => {
  const env = makeEnv(world());
  ok(env.ctx.fdcCoreOk_() === true, 'core');
  ok(!/[^\x00-\x7F]/.test(SRC), 'ASCII');
  ok(!/\b(let|const)\s|=>|`/.test(SRC), 'ES5');
  ok(/ \* Version: 1\.3\.0 \(2026-11-18\)/.test(SRC) && /var FDC_VERSION = '1\.3\.0';/.test(SRC), 'version lockstep');
});

test('S8 (1.1.0) the today line in Top_10_Investments!A3: written when empty, kept current, never over other text', () => {
  const env = makeEnv(world());
  env.ctx.fdcTick();
  const t10 = env.book.getSheetByName('Top_10_Investments');
  eq(t10._cells.get('3:1').v, 'TODAY (Today Action Plan 2026-11-16 08:41): BUY AX.US \u00b7 SELL none. The FINAL DECISION of every seat is in the FINAL DECISION column at the right; orders come from Today_Action_Plan - this page is the engine\'s research.');
  eq(t10._cells.get('3:1').setFontWeight, 'bold');
  env.book.getSheetByName('Final_Decisions').getRange(2, 2).setValue('WATCH');   // the plan changes
  env.ctx.fdcTick();
  ok(/BUY none/.test(t10._cells.get('3:1').v), 'kept current');
  const w = world(); w.Top_10_Investments[2][0] = 'something of the page';
  const e2 = makeEnv(w); const line = e2.ctx.fdcTick();
  eq(e2.book.getSheetByName('Top_10_Investments')._cells.get('3:1').v, 'something of the page', 'never over other text');
  ok(/A3: not empty, left/.test(line), line);
  const w3 = world(); delete w3.Final_Decisions;
  const e3 = makeEnv(w3); e3.ctx.fdcTick();
  ok(!e3.book.getSheetByName('Top_10_Investments')._cells.get('3:1'), 'nothing without the decisions');
  // a written line goes when Final_Decisions is removed or renamed (Codex P2, #1003)
  env.book._s.delete('Final_Decisions');
  ok(/A3: cleared \(Final_Decisions missing\)/.test(env.ctx.fdcTick()), 'reported');
  eq(t10._cells.get('3:1').v, '', 'no old decisions left on screen');
  // the readers' landmarks stay: row 2 status, row 4 panel, the seats header
  eq(t10._cells.get('2:2').v, 'Last run 2026-11-16 08:30'); eq(t10._cells.get('4:1').v, 'CONTROL PANEL');
});

test('S9 (1.1.0) a render stopped by Google is re-run once, in its own execution, in hours', () => {
  const stuck = 'Last run 2026-11-16 08:30:00 | status: running…';   // NOW = 09:00 Riyadh: 30 min
  const mk = (status, props, busy) => { const w = world(); w.Top_10_Investments[1][1] = status; return makeEnv(w, { props: props || {}, busy }); };
  const reruns = e => e.triggers.filter(t => t.h === 'fdcRerunTop10');
  let e = mk(stuck);
  ok(/re-run of the stopped 2026-11-16 08:30:00 render started/.test(e.ctx.fdcTick()), 'due');
  eq(reruns(e).length, 1, 'one one-off trigger'); eq(reruns(e)[0].after, 60000, 'a minute on'); eq(e.props.FDC_RERUN_FOR, '2026-11-16 08:30:00');
  e.ctx.fdcTick(); eq(reruns(e).length, 1, 'never twice for the same stopped run');
  // the handler removes its trigger, then runs the refresh
  let calls = 0; e.ctx.refreshDecisionTop10 = () => { calls++; };
  eq(e.ctx.fdcRerunTop10(), 'ran'); eq(calls, 1); eq(reruns(e).length, 0, 'removed itself');
  const e0 = mk(stuck); eq(e0.ctx.fdcRerunTop10(), 'missing', 'no refresh function in the project: reported, nothing thrown');
  for (const [status, props, busy, why] of [['Last run 2026-11-16 08:50:00 | status: running…', {}, false, '10 minutes is a render still going'],
      ['Last run 2026-11-16 08:00:00 | status: ok', {}, false, 'a finished render'], [stuck, { FDC_RERUN: 'off' }, false, 'off'], [stuck, {}, true, 'a busy lock decides nothing']]) {
    const x = mk(status, props, busy); x.ctx.fdcTick(); eq(reruns(x).length, 0, why);
  }
  const c = makeEnv(world()).ctx;
  ok(!c.fdcRerunDue_({ run: 'r', minutes: 90 }, '', 6, true) && !c.fdcRerunDue_({ run: 'r', minutes: 90 }, '', 23, true) && c.fdcRerunDue_({ run: 'r', minutes: 90 }, '', 22, true), 'only 07:00-23:00 Riyadh');
  // a scheduling failure clears the reservation, so the next tick tries again (Codex P2, #1003)
  const f = mk(stuck); let fail = true; const realNew = f.ctx.ScriptApp.newTrigger;
  f.ctx.ScriptApp.newTrigger = h => { if (fail) throw new Error('quota'); return realNew(h); };
  f.ctx.fdcTick();
  ok(!('FDC_RERUN_FOR' in f.props) && reruns(f).length === 0 && f.book.toasts.some(t => /could not be scheduled \(the next tick tries again\): quota/.test(t)), 'released');
  fail = false; f.ctx.fdcTick();
  eq(reruns(f).length, 1, 'scheduled on the next tick'); eq(f.props.FDC_RERUN_FOR, '2026-11-16 08:30:00');
  // the 10-minute trigger is never touched by the re-run bookkeeping
  const t = mk(stuck); t.ctx.fdcInstallTrigger(); eq(t.triggers.filter(x => x.h === 'fdcTick').length, 1); eq(reruns(t).length, 1);
});

test('S10 (1.2.0) a SKIPPED render (16 v1.30.0 [PAINT-BUDGET]) is re-run on the next tick, once, at most 4 a Riyadh day', () => {
  const PREV = 'Last run 2026-11-16 08:10:08 | status: ok | output: EXECUTABLE';
  const skipped = PREV + ' | SKIPPED render 2026-11-16 08:55:00 (out of time: t+331.9s, paint budget 300s) - the board below is the previous render, unchanged; nothing was cleared';
  const mk = (status, props) => { const w = world(); w.Top_10_Investments[1][1] = status; return makeEnv(w, { props: props || {} }); };
  const reruns = e => e.triggers.filter(t => t.h === 'fdcRerunTop10');
  const e = mk(skipped);   // NOW = 09:00 Riyadh: 5 minutes after the skip, no 20-minute wait
  ok(/re-run of the skipped 2026-11-16 08:55:00 render started/.test(e.ctx.fdcTick()), 'due at once');
  eq(reruns(e).length, 1, 'one one-off trigger'); eq(e.props.FDC_RERUN_FOR, 'skipped 2026-11-16 08:55:00'); eq(e.props.FDC_RERUN_DAY, '2026-11-16|1', 'counted');
  e.ctx.fdcTick(); eq(reruns(e).length, 1, 'never twice for the same skipped run'); eq(e.props.FDC_RERUN_DAY, '2026-11-16|1');
  const n = mk(skipped, { FDC_RERUN_DAY: '2026-11-16|3' }); n.ctx.fdcTick(); eq(reruns(n).length, 1, 'the 4th of the day runs'); eq(n.props.FDC_RERUN_DAY, '2026-11-16|4');
  for (const [props, why] of [[{ FDC_RERUN_DAY: '2026-11-16|4' }, 'the day cap'], [{ FDC_RERUN: 'off' }, 'off'], [{ FDC_RERUN_FOR: 'skipped 2026-11-16 08:55:00' }, 'already done']]) {
    const x = mk(skipped, props); x.ctx.fdcTick(); eq(reruns(x).length, 0, why);
  }
  const y = mk(skipped, { FDC_RERUN_DAY: '2026-11-15|4' }); y.ctx.fdcTick(); eq(reruns(y).length, 1, 'yesterday\'s count does not hold today'); eq(y.props.FDC_RERUN_DAY, '2026-11-16|1');
  // the stopped-run rule counts against the same cap
  const st = mk('Last run 2026-11-16 08:30:00 | status: running\u2026', { FDC_RERUN_DAY: '2026-11-16|4' }); st.ctx.fdcTick(); eq(reruns(st).length, 0, 'cap covers stopped runs');
  // a scheduling failure is not counted
  const f = mk(skipped); f.ctx.ScriptApp.newTrigger = () => { throw new Error('quota'); }; f.ctx.fdcTick();
  ok(!('FDC_RERUN_DAY' in f.props) && !('FDC_RERUN_FOR' in f.props), 'not counted, released');
  ok(f.book.toasts.some(t => /re-run of the skipped 2026-11-16 08:55:00 render could not be scheduled/.test(t)), 'toast names the skipped run');
  const c = makeEnv(world()).ctx;
  eq(c.fdcStuck_('Last run 2026-11-16 08:44:11 | status: running\u2026 | SKIPPED render 2026-11-16 08:50:00 (x)', Date.UTC(2026, 10, 16, 6, 0)).run, 'skipped 2026-11-16 08:50:00', 'the tail wins over running');
  ok(!c.fdcRerunDue_({ run: 's', minutes: 0, skipped: true }, '', 23, true), 'still 07:00-23:00 only');
  ok(/re-runs today 1\/4/.test(e.ctx.fdcStatus()), 'fdcStatus shows the count');
});

const HR_HEAD = ['Symbol', 'Name', 'Last session', 'Stock %', 'Market %', 'Sector %', 'Driver', 'Main news (last session)', 'Next session read', 'Up after similar days', 'Why', 'After the close'];
function hread(session, head) {
  head = head || HR_HEAD;
  const row = o => head.map(h => (h in o ? o[h] : ''));
  return [['HOLDING READ - what moved each holding last session, and the read for the next one'], ['Written 2026-11-18 00:40 Riyadh'], ['How to read: ...'], ['Score: 0 of 0'], [],
    head.slice(),
    row({ Symbol: 'DDI.US', Name: 'DoubleDown', 'Last session': session || '2026-11-17', 'Stock %': '+0.06%', 'Market %': '-1.00%', 'Sector %': '-0.80%', Driver: 'NORMAL RANGE',
          'Main news (last session)': 'No company item; market -1.00%', 'Next session read': 'PINNED BY AN OFFER', 'Up after similar days': '' }),
    row({ Symbol: 'GLPI.US', Name: 'Gaming', 'Last session': session || '2026-11-17', 'Stock %': '-1.84%', 'Market %': '-1.00%', 'Sector %': '-1.50%', Driver: 'SECTOR',
          'Main news (last session)': 'No company item; real estate -1.50%', 'Next session read': 'FURTHER FALL MORE LIKELY', 'Up after similar days': '39% of 77 (any day 52%)' })];
}
const worldR = o => world(Object.assign({ Holding_Read: hread() }, o || {}));
const KEY = 'TRIM(INDIRECT("R"&ROW()&"C2",FALSE))';
const at = L => 'INDEX(Holding_Read!$' + L + ':$' + L + ',MATCH(' + KEY + ',Holding_Read!$A:$A,0))';

test('S11 (1.3.0) Portfolio_Decision: the two Holding_Read columns right of FINAL DECISION, one formula pair per holding row', () => {
  const env = makeEnv(worldR());
  const sh = env.book.getSheetByName('Portfolio_Decision');
  const before = snapshot(sh, 27);
  const line = env.ctx.fdcTick();
  ok(/Portfolio_Decision: 3 row\(s\) in column AB · read: session 2026-11-17 in columns AC-AD/.test(line), line);
  eq(sh._cells.get('5:28').v, 'FINAL DECISION · Today Action Plan 2026-11-16 08:41', 'the FINAL DECISION column is where it was');
  eq(sh._cells.get('5:29').v, 'LAST SESSION · what moved it · Holding Read, session 2026-11-17 · analysis, not an order');
  eq(sh._cells.get('5:30').v, 'NEXT SESSION READ · after the session of 2026-11-17 · a count from its own history, not a forecast of size');
  eq(sh._cells.get('5:29').setBackground, '#1b2a41');
  eq(formulaRows(sh, 29).join(','), '6,7,8', 'what moved it: the three holding rows, not the sector table');
  eq(formulaRows(sh, 30).join(','), '6,7,8', 'next session read: the same rows');
  eq(sh._cells.get('6:29').f, '=IFERROR(' + at('D') + '&" · "&' + at('G') + '&" — "&' + at('H') + ',"n/a — not on Holding_Read")');
  eq(sh._cells.get('6:30').f, '=IFERROR(' + at('I') + '&IF(' + at('J') + '="",""," — closed up after similar days: "&' + at('J') + '),"n/a — not on Holding_Read")');
  eq(sh._cells.get('7:29').f, sh._cells.get('6:29').f, 'every row carries the same formula: it reads its own row\'s symbol');
  eq(JSON.stringify(snapshot(sh, 27)), JSON.stringify(before), 'every cell of the page itself (A:AA) is unchanged');
  eq(sh._widths[29], 430); eq(sh._widths[30], 300); eq(sh._cells.get('6:29').setFontSize, 9); ok(sh._cells.get('6:30').setWrap === true, 'wrapped');
  // Top_10_Investments holds candidates, not holdings: no Holding Read column there
  const t10 = env.book.getSheetByName('Top_10_Investments');
  ok(!colOf(t10, /^(LAST SESSION|NEXT SESSION READ)/) && formulaRows(t10, 33).length === 0 && formulaRows(t10, 34).length === 0, 'none on the Top 10');
  ok(/Top_10_Investments: 3 row\(s\) in column AF(?! · read)/.test(line), line);
  ok(!colOf(env.book.getSheetByName('My_Portfolio'), /^(LAST SESSION|NEXT SESSION READ)/) && env.book.getSheetByName('My_Portfolio').getLastColumn() === 6, 'My_Portfolio keeps its width');
});

test('S12 (1.3.0) columns by header name; session, OLD, not read; never a stale formula', () => {
  // a column added on the left of Holding_Read moves every letter; the tick follows the names
  const moved = ['Rank'].concat(HR_HEAD);
  let env = makeEnv(worldR({ Holding_Read: hread('2026-11-17', moved).map((r, i) => (i < 5 ? r : r)) }));
  env.ctx.fdcTick();
  let sh = env.book.getSheetByName('Portfolio_Decision');
  const at2 = L => 'INDEX(Holding_Read!$' + L + ':$' + L + ',MATCH(' + KEY + ',Holding_Read!$B:$B,0))';
  eq(sh._cells.get('6:29').f, '=IFERROR(' + at2('E') + '&" · "&' + at2('H') + '&" — "&' + at2('I') + ',"n/a — not on Holding_Read")', 'one column to the right');
  // NOW is 16 Nov 09:00 Riyadh. A session 6 days back is OLD; 5 days is not (a long weekend)
  env = makeEnv(worldR({ Holding_Read: hread('2026-11-10') })); env.ctx.fdcTick();
  sh = env.book.getSheetByName('Portfolio_Decision');
  eq(sh._cells.get('5:29').v, 'LAST SESSION · what moved it · Holding Read, session 2026-11-10 · OLD (6 days) · analysis, not an order');
  eq(sh._cells.get('5:29').setBackground, '#b45309'); ok(/OLD \(6 days\)/.test(sh._cells.get('5:30').v), 'both headers say so');
  eq(formulaRows(sh, 29).length, 3, 'the cells still read the page: the header is the warning');
  env = makeEnv(worldR({ Holding_Read: hread('2026-11-11') })); env.ctx.fdcTick();
  ok(!/OLD/.test(env.book.getSheetByName('Portfolio_Decision')._cells.get('5:29').v), '5 days is not old');
  // a date cell instead of text (a hand edit) is read as its day
  const dated = hread(); dated[6][2] = new Date(Date.UTC(2026, 10, 12, 21, 0)); dated[7][2] = dated[6][2];   // 13 Nov 00:00 Riyadh
  env = makeEnv(worldR({ Holding_Read: dated })); env.ctx.fdcTick();
  ok(/session 2026-11-13 /.test(env.book.getSheetByName('Portfolio_Decision')._cells.get('5:29').v), env.book.getSheetByName('Portfolio_Decision')._cells.get('5:29').v);
  // no tab: headers say so, no formula is written, and formulas of an earlier tick are removed
  env = makeEnv(worldR()); env.ctx.fdcTick();
  sh = env.book.getSheetByName('Portfolio_Decision');
  eq(formulaRows(sh, 29).length, 3);
  env.book._s.delete('Holding_Read');
  const line = env.ctx.fdcTick();
  ok(/Portfolio_Decision: 3 row\(s\) in column AB · read: headers only \(tab missing or empty\)/.test(line), line);
  eq(sh._cells.get('5:29').v, 'LAST SESSION · what moved it · Holding_Read not read (tab missing or empty)');
  eq(sh._cells.get('5:30').v, 'NEXT SESSION READ · Holding_Read not read (tab missing or empty)');
  eq(formulaRows(sh, 29).length + formulaRows(sh, 30).length, 0, 'no formula pointing at a missing tab');
  eq(formulaRows(sh, 28).join(','), '6,7,8', 'the FINAL DECISION column is not affected');
  // a page without the read table, or with no holding rows
  for (const [grid, why] of [[[['HOLDING READ'], ['Symbol', 'Name']], 'no read table on the page'], [hread().slice(0, 6), 'no holding rows']]) {
    const e = makeEnv(worldR({ Holding_Read: grid }));
    ok(e.ctx.fdcTick().indexOf('read: headers only (' + why + ')') > 0, why);
    eq(formulaRows(e.book.getSheetByName('Portfolio_Decision'), 29).length, 0, why);
  }
  // pure: the day count is in Riyadh calendar days
  const c = makeEnv(worldR()).ctx;
  eq(c.fdcSessionAgeD_('2026-11-17', Date.UTC(2026, 10, 17, 21, 30)), 1, '00:30 Riyadh on the 18th');
  eq(c.fdcSessionAgeD_('2026-11-17', Date.UTC(2026, 10, 17, 20, 30)), 0, '23:30 Riyadh on the 17th');
  eq(c.fdcSessionAgeD_('17 Nov', 0), null);
});

test('S13 (1.3.0) a second tick reuses the columns; colours; a header left elsewhere is cleared; rebuild; rollback', () => {
  const env = makeEnv(worldR());
  const sh = env.book.getSheetByName('Portfolio_Decision');
  const other = { spec: { text: 'theirs' }, getRanges: () => [sh.getRange(6, 1, 3, 1)] };
  sh.setConditionalFormatRules([other]);
  env.ctx.fdcTick(); env.ctx.fdcTick();
  const heads = []; for (const [k, x] of sh._cells) if (typeof x.v === 'string' && /^(FINAL DECISION|LAST SESSION|NEXT SESSION READ)/.test(x.v)) heads.push(k);
  eq(heads.sort().join(','), '5:28,5:29,5:30', 'one header each after two ticks');
  const rules = sh._rules();
  eq(rules.length, 1 + 6 + 11, 'the page\'s own rule, six for FINAL DECISION, eleven for the read');
  ok(rules[0] === other, 'the page\'s rule kept first');
  eq(rules.slice(7).map(r => r.spec.text).join('|'), 'REBOUND MORE LIKELY|RISE MORE LIKELY|UP MORE LIKELY|FURTHER FALL MORE LIKELY|PULLBACK MORE LIKELY|DOWN MORE LIKELY|PINNED BY AN OFFER|EARNINGS DUE|NO EDGE|NOT ENOUGH HISTORY|n/a');
  ok(rules.slice(7).every(r => r.spec.ranges[0].getColumn() === 30 && r.spec.ranges[0].getRow() === 6 && r.spec.ranges[0].getNumRows() === 3), 'on the NEXT SESSION READ cells only');
  ok(rules.slice(1, 7).every(r => r.spec.ranges[0].getColumn() === 28), 'the FINAL DECISION rules stay on their column');
  // a Holding Read header left further right by another layout is cleared, with its cells
  sh.insertColumnsAfter(sh.getMaxColumns(), 6);
  sh.getRange(5, 33).setValue('NEXT SESSION READ · old place'); sh.getRange(6, 33).setValue('stale text');
  env.ctx.fdcTick();
  ok(!sh._cells.get('5:33') && !sh._cells.get('6:33'), 'cleared');
  eq(sh._cells.get('5:28').v.indexOf('FINAL DECISION'), 0, 'our own headers never push FINAL DECISION to the right');
  // a rebuilt page (cleared by its own script) gets all three columns back in the same place
  sh.getRange(1, 1, 40, 34).clear();
  pd().forEach((row, i) => row.forEach((v, j) => { if (v !== '') sh.getRange(i + 1, j + 1).setValue(v); }));
  env.ctx.fdcTick();
  eq(formulaRows(sh, 28).join(',') + ';' + formulaRows(sh, 29).join(',') + ';' + formulaRows(sh, 30).join(','), '6,7,8;6,7,8;6,7,8', 'back after a rebuild');
  // a page rebuilt with only the read headers left behind: FINAL DECISION goes back to its own column
  const e2 = makeEnv(worldR()); e2.ctx.fdcTick();
  const s2 = e2.book.getSheetByName('Portfolio_Decision');
  s2.getRange(5, 28, 4, 1).clear();
  e2.ctx.fdcTick();
  eq(colOf(s2, /^FINAL DECISION/).join(':'), '5:28'); eq(colOf(s2, /^LAST SESSION/).join(':'), '5:29');
  // rollback
  eq(env.ctx.fdcRemoveColumn()[1], 'Portfolio_Decision: removed from column AB and 2 Holding Read column(s)');
  eq(formulaRows(sh, 29).length + formulaRows(sh, 30).length + formulaRows(sh, 28).length, 0, 'all three gone');
  // a holding table with no rows: headers only
  const w = worldR(); w.Portfolio_Decision = [['MY PORTFOLIO - DECISION'], [''], PD_HEAD, [''], ['SECTOR SUMMARY']].map(r => pad(r, 26));
  const e3 = makeEnv(w);
  ok(/Portfolio_Decision: header only \(no rows\) · read: headers only \(no rows\)/.test(e3.ctx.fdcTick()), 'no rows');
});

console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
