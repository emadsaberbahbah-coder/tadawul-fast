/* 10_My_Portfolio v1.5.15 (authored as v1.5.14; merge note 2026-10-21 (#780): renumbered v1.5.15 over #810's v1.5.14):
 * the Portfolio_Decision layout builder keeps the
 * operator's panel inputs (P-76 port from 16_Decision_Top10 v1.11.3), gated
 * by Script Property PD_PANEL_PRESERVE = off (default) | observe | enforce.
 * Runs the REAL source in a vm against a cell-level sheet model that records
 * every mutating call; holdings and backend responses are synthetic.
 *
 *   node tests/js/pf_panel_preserve.test.js
 *   PF_GAS=<file>          source under test (default apps_script/10_My_Portfolio.gs)
 *   PF_BASE_GAS=<file>     optional v1.5.13 copy, compared live (off == base)
 *   PF_WRITE_GOLDEN=1      with PF_GAS=<v1.5.13 copy>: rewrite the golden fixture
 *   PF_DUMP=<file>         write the produced panels, controls and logs as JSON
 *
 * WORKS  W1-W7  enforce keeps typed inputs, re-seeds blanks, survives the
 *               auto-build inside refreshPortfolioDecision, seeds a fresh
 *               sheet exactly as before; observe only logs (also when the
 *               snapshot read faults).
 * NEVER  N1-N7  off and every unknown spelling are v1.5.13 byte-identical
 *               and read no panel cell; a moved or renamed label keeps
 *               nothing; the pure seed and the snapshot never throw; a
 *               snapshot read fault under enforce aborts BEFORE the clear
 *               (sheet untouched, nothing posted); off ignores read faults.
 * GOLD   G1-G2  the v1.5.13 golden (and PF_BASE_GAS live) reset cash to 0
 *               and Max Position to 15 under the same inputs - the defect.
 */
'use strict';
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const GAS = process.env.PF_GAS || path.join(__dirname, '../../apps_script/10_My_Portfolio.gs');
const GOLDEN = path.join(__dirname, '../fixtures/pf_panel_v1_5_13_golden.json');
const SOURCE = fs.readFileSync(GAS, 'utf8');
const START = Date.parse('2026-10-15T05:50:11Z');            // 08:50:11 Riyadh
const TOTAL = 16;

const LABELS = ['PF: Cash Available SAR', 'PF: Target Cash %', 'PF: Max Position %',
  'PF: Max Sector %', 'PF: Min Reliability to Add', 'PF: Min DQ to Add', 'PF: Rebalance Mode'];
/* _Lists_Config TFB_PANEL_DEFAULTS: cash 0 / Max Position 15 as the 10-15
 * export reads them. Max Sector (25) and Min Reliability (65) deliberately
 * differ from the built-in PD_PANEL defaults (30 / 70) so a seed that skips
 * _Lists_Config is visible; 'PF: Min DQ to Add' is left out so the built-in
 * default (80) is exercised too. */
const DEFAULTS = [['PF: Cash Available SAR', 0], ['PF: Target Cash %', 10],
  ['PF: Max Position %', 15], ['PF: Max Sector %', 25],
  ['PF: Min Reliability to Add', 65], ['PF: Rebalance Mode', 'Advisory Only']];
const SEEDED = {'PF: Cash Available SAR': 0, 'PF: Target Cash %': 10, 'PF: Max Position %': 15,
  'PF: Max Sector %': 25, 'PF: Min Reliability to Add': 65, 'PF: Min DQ to Add': 80,
  'PF: Rebalance Mode': 'Advisory Only'};
/* The operator panel of the _Portfolio_Action_Log snapshot, 2026-10-14 04:45:58. */
const OPERATOR = {'PF: Cash Available SAR': 53769.2625, 'PF: Target Cash %': 10,
  'PF: Max Position %': 20, 'PF: Max Sector %': 30, 'PF: Min Reliability to Add': 70,
  'PF: Min DQ to Add': 80, 'PF: Rebalance Mode': 'Advisory'};
const RESET = ['PF: Cash Available SAR', 'PF: Max Position %', 'PF: Max Sector %',
  'PF: Min Reliability to Add', 'PF: Rebalance Mode'];

const HOLD_HEADERS = ['Symbol', 'Name', 'Sector', 'Currency', 'Position Qty', 'Avg Cost',
  'Current Price', 'Last Updated (UTC)'];
const HOLD_ROWS = [['SYNTH1.US', 'Synthetic One', 'Energy', 'USD', 10, 20.5, 21.25,
  '2026-10-15T05:40:10Z'], ['SYNTH2.SR', 'Synthetic Two', 'Financials', 'SAR', 5, 100, 99.5,
  '2026-10-15T05:40:11Z']];
const RESPONSE = {status: 'ok', kpis: {}, sector_summary: [], alerts: [],
  actions: [{action: 'BLOCK', symbol: 'SYNTH1.US', name: 'Synthetic One'},
            {action: 'BLOCK', symbol: 'SYNTH2.SR', name: 'Synthetic Two'}],
  meta: {route: {version: '4.18.2', request_id: 'req-synthetic'},
         versions: {portfolio_actions: '1.17.0'}}};

let passed = 0;
const dump = {};
function check(label, fn) { fn(); passed++; console.log('PASS ' + label); }
function has(o, k) { return Object.prototype.hasOwnProperty.call(o, k); }
function pos(i) { return {row: 5 + Math.floor(i / 3), labelCol: 1 + (i % 3) * 3, valueCol: 2 + (i % 3) * 3}; }
function sha(x) { return crypto.createHash('sha256').update(JSON.stringify(x)).digest('hex'); }
/* A golden trace with only the 7 panel seed values swapped: every other write
 * (formats, colours, borders, order) must stay v1.5.13's. */
function withValues(trace, values) {
  const at = {};
  LABELS.forEach((l, i) => {
    const p = pos(i);
    at['Portfolio_Decision!' + p.row + ',' + p.valueCol + ',1,1 setValue '] = l;
  });
  let swapped = 0;
  const out = trace.map((t) => {
    for (const prefix of Object.keys(at)) {
      if (t.startsWith(prefix)) { swapped++; return prefix + JSON.stringify([values[at[prefix]]]); }
    }
    return t;
  });
  assert.equal(swapped, 7, 'each panel seed appears exactly once in the golden trace');
  return out;
}

/* ----- cell-level sheet model -------------------------------------------- */
const SKIP = new Set(['then', 'toJSON', 'inspect', 'constructor']);
function makeSheet(name, cells0, opts) {
  opts = opts || {};
  const cells = new Map(cells0 || []);
  const trace = [];
  const reads = [];
  let cleared = false;
  let maxCols = 26;
  const key = (r, c) => r + ',' + c;
  const get = (r, c) => { const v = cells.get(key(r, c)); return v === undefined ? '' : v; };
  function range(r, c, nr, nc) {
    nr = nr || 1; nc = nc || 1;
    if (opts.throwOnRead) throw new Error('synthetic getRange fault');
    const rng = new Proxy({}, {get(_t, prop) {
      if (typeof prop === 'symbol' || SKIP.has(prop)) return undefined;
      if (prop === 'getValue') {
        return () => {
          reads.push([r, c, cleared]);
          /* a transient fault on the panel rows (5-7) before the clear */
          if (opts.faultBeforeClear && !cleared && r >= 5 && r <= 7) {
            throw new Error('Service Spreadsheets timed out');
          }
          return get(r, c);
        };
      }
      if (prop === 'getValues') {
        return () => Array.from({length: nr}, (_x, i) =>
          Array.from({length: nc}, (_y, j) => get(r + i, c + j)));
      }
      return (...args) => {
        trace.push(name + '!' + r + ',' + c + ',' + nr + ',' + nc + ' ' + prop + ' ' +
                   JSON.stringify(args));
        if (prop === 'setValue') {
          for (let i = 0; i < nr; i++) for (let j = 0; j < nc; j++) cells.set(key(r + i, c + j), args[0]);
        } else if (prop === 'setValues') {
          args[0].forEach((row, i) => row.forEach((v, j) => cells.set(key(r + i, c + j), v)));
        } else if (prop === 'clear' || prop === 'clearContent') {
          for (let i = 0; i < nr; i++) for (let j = 0; j < nc; j++) cells.delete(key(r + i, c + j));
        }
        return rng;
      };
    }});
    return rng;
  }
  const sheet = new Proxy({}, {get(_t, prop) {
    if (typeof prop === 'symbol' || SKIP.has(prop)) return undefined;
    if (prop === '__cells') return cells;
    if (prop === '__trace') return trace;
    if (prop === '__reads') return reads;
    if (prop === '__headers') return opts.headers;
    if (prop === 'getRange') return range;
    if (prop === 'getName') return () => name;
    if (prop === 'getMaxRows') return () => 1000;
    if (prop === 'getMaxColumns') return () => maxCols;
    if (prop === 'getLastRow' || prop === 'getLastColumn') {
      return () => {
        let m = 0;
        for (const [k, v] of cells) {
          if (v === '' || v === null || v === undefined) continue;
          m = Math.max(m, +k.split(',')[prop === 'getLastRow' ? 0 : 1]);
        }
        return m;
      };
    }
    return (...args) => {
      trace.push(name + ' ' + prop + ' ' + JSON.stringify(args));
      if (prop === 'clear') { cells.clear(); cleared = true; }
      if (prop === 'insertColumnsAfter') maxCols += args[1];
      return sheet;
    };
  }});
  return sheet;
}
function holdingsSheet() {
  const cells = [];
  HOLD_HEADERS.forEach((h, j) => cells.push(['1,' + (j + 1), h]));
  HOLD_ROWS.forEach((row, i) => row.forEach((v, j) => cells.push([(i + 2) + ',' + (j + 1), v])));
  return makeSheet('My_Portfolio', cells, {headers: HOLD_HEADERS});
}
function panelOf(sheet) {
  const out = {};
  LABELS.forEach((l, i) => {
    const p = pos(i);
    const v = sheet.__cells.get(p.row + ',' + p.valueCol);
    out[l] = v === undefined ? '' : v;
  });
  return out;
}

/* ----- one Apps Script world ---------------------------------------------- */
function world(opts) {
  opts = opts || {};
  const clock = {now: START};
  class FixedDate extends Date {
    constructor(...a) { if (a.length) super(...a); else super(clock.now); }
    static now() { return clock.now; }
  }
  const props = Object.assign({PD_ACTION_LOG: 'off'}, opts.props || {});
  const sheets = {My_Portfolio: holdingsSheet()};
  if (opts.pd) sheets.Portfolio_Decision = opts.pd;
  const logs = [];
  const posts = [];
  const ss = {
    getSheetByName(n) { return has(sheets, n) ? sheets[n] : null; },
    insertSheet(n) { sheets[n] = makeSheet(n); return sheets[n]; },
    getRangeByName(n) {
      if (n !== 'TFB_PANEL_DEFAULTS' || opts.noDefaults) return null;
      return {getValues() { return DEFAULTS.map((r) => r.slice()); }};
    }
  };
  const ctx = {
    SHEET_NAMES: {MY_PORTFOLIO: 'My_Portfolio'},
    Date: FixedDate,
    Logger: {log(m) { logs.push(String(m)); }},
    Session: {getScriptTimeZone() { return 'Asia/Riyadh'; }},
    Utilities: {formatDate(d) {
      return new Date(d.getTime() + 3 * 3600000).toISOString().slice(0, 19).replace('T', ' ');
    }},
    PropertiesService: {
      getScriptProperties() {
        if (opts.propsThrow) throw new Error('synthetic properties fault');
        return {getProperty(k) { return has(props, k) ? props[k] : null; },
                setProperty(k, v) { props[k] = v; }};
      },
      getDocumentProperties() { return {getProperty() { return null; }}; }
    },
    SpreadsheetApp: {getActiveSpreadsheet() { return ss; }, getActive() { return ss; }, flush() {},
      BorderStyle: {SOLID: 'SOLID', SOLID_THICK: 'SOLID_THICK'}, getUi() { return {alert() {}}; }},
    UrlFetchApp: {fetch(url, options) {
      posts.push(JSON.parse(options.payload));
      return {getContentText() { return JSON.stringify(RESPONSE); },
              getResponseCode() { return 200; }};
    }},
    ensureSheetExists_(name) { return ss.getSheetByName(name); },
    readCurrentSheetHeaders_(sheet) { return sheet.__headers || []; }
  };
  vm.createContext(ctx);
  vm.runInContext(opts.source || SOURCE, ctx, {filename: '10_My_Portfolio.gs'});
  return {ctx: ctx, sheets: sheets, logs: logs, posts: posts};
}

/* The layout exactly as the builder writes it, then the operator's typing. */
function operatorCells(source, edit) {
  const w = world({source: source});
  w.ctx.buildPortfolioDecisionLayout();
  const cells = new Map(w.sheets.Portfolio_Decision.__cells);
  LABELS.forEach((l, i) => cells.set(pos(i).row + ',' + pos(i).valueCol, OPERATOR[l]));
  if (edit) edit(cells);
  return cells;
}
function build(o) {
  o = o || {};
  const pd = o.fresh ? null : makeSheet('Portfolio_Decision', operatorCells(o.source, o.edit), o.sheetOpts);
  const w = world({source: o.source, props: o.props, pd: pd, noDefaults: o.noDefaults});
  w.ctx.buildPortfolioDecisionLayout();
  const sheet = w.sheets.Portfolio_Decision;
  return {sheet: sheet, panel: panelOf(sheet), trace: sheet.__trace.slice(), logs: w.logs,
          reads: sheet.__reads.slice(), ctx: w.ctx};
}
/* refreshPortfolioDecision with A4 blank: the auto-build branch runs first. */
function refresh(o) {
  o = o || {};
  const cells = operatorCells(o.source);
  cells.delete('4,1');
  const pd = makeSheet('Portfolio_Decision', cells, o.sheetOpts);
  const w = world({source: o.source, props: o.props, pd: pd});
  let error = null;
  try { w.ctx.refreshPortfolioDecision(); } catch (e) { error = e; }
  if (error && !o.allowThrow) throw error;
  return {controls: w.posts.length ? w.posts[0].controls : null, posts: w.posts.length,
          panel: panelOf(pd), trace: pd.__trace.slice(), logs: w.logs, error: error};
}

const fresh = build({fresh: true});
const offOp = build({});
const offRefresh = refresh({});

if (process.env.PF_WRITE_GOLDEN) {            // regenerate from a v1.5.13 PF_GAS only
  fs.writeFileSync(GOLDEN, JSON.stringify({
    note: 'buildPortfolioDecisionLayout / refreshPortfolioDecision output of ' +
          '10_My_Portfolio.gs v1.5.13 on the synthetic panel scenario',
    source_sha256: crypto.createHash('sha256').update(SOURCE).digest('hex'),
    fresh_trace: fresh.trace, operator_trace: offOp.trace, operator_panel: offOp.panel,
    refresh_controls: offRefresh.controls, refresh_trace_sha256: sha(offRefresh.trace)
  }, null, 1).replace(/[\u0080-\uffff]/g,
      (ch) => '\\u' + ch.charCodeAt(0).toString(16).padStart(4, '0')) + '\n');
  console.log('golden written from ' + GAS);
  process.exit(0);
}
const golden = JSON.parse(fs.readFileSync(GOLDEN, 'utf8'));
const ctx0 = build({fresh: true}).ctx;
dump.off = {panel: offOp.panel, controls: offRefresh.controls};

/* ----------------------------------------------------------------- WORKS -- */
check('W1 enforce: the typed panel survives the rebuild; the log names what differs', () => {
  const r = build({props: {PD_PANEL_PRESERVE: 'enforce'}});
  assert.deepEqual(r.panel, OPERATOR);
  assert.deepEqual(r.trace, withValues(golden.operator_trace, OPERATOR));
  const line = r.logs.filter((l) => l.indexOf('layout: preserved') >= 0);
  assert.equal(line.length, 1);
  assert.ok(line[0].endsWith('layout: preserved 7 operator input(s) (5 differ from defaults: ' +
                             RESET.join(', ') + ')'), line[0]);
  dump.enforce = {panel: r.panel, log: line[0]};
});

check('W2 enforce: blank and dash cells re-seed from _Lists_Config, then the built-in', () => {
  const r = build({props: {PD_PANEL_PRESERVE: 'enforce'}, edit(cells) {
    cells.set('6,5', '');                 // PF: Min Reliability to Add -> _Lists_Config 70
    cells.set('6,8', '\u2014');           // PF: Min DQ to Add -> built-in 80
    cells.set('5,5', 12.5);               // PF: Target Cash % typed
  }});
  const want = Object.assign({}, OPERATOR,
      {'PF: Target Cash %': 12.5, 'PF: Min Reliability to Add': 65, 'PF: Min DQ to Add': 80});
  assert.deepEqual(r.panel, want);
  assert.deepEqual(r.trace, withValues(golden.operator_trace, want));
  assert.ok(r.logs.some((l) => /layout: preserved 5 operator input\(s\) \(5 differ/.test(l)));
});

check('W3 enforce: the auto-build inside refreshPortfolioDecision posts the typed controls', () => {
  const r = refresh({props: {PD_PANEL_PRESERVE: 'enforce'}});
  assert.equal(r.controls['PF: Cash Available SAR'], 53769.2625);
  assert.equal(r.controls['PF: Max Position %'], 20);
  assert.equal(r.controls['PF: Rebalance Mode'], 'Advisory');
  assert.deepEqual(r.panel, OPERATOR);
  dump.enforce_controls = r.controls;
});

check('W4 enforce on a fresh workbook seeds exactly as v1.5.13 (nothing to keep)', () => {
  const r = build({fresh: true, props: {PD_PANEL_PRESERVE: 'enforce'}});
  assert.deepEqual(r.trace, golden.fresh_trace);
  assert.deepEqual(r.panel, SEEDED);                      // _Lists_Config > built-in
  assert.ok(r.logs.some((l) => /layout: preserved 0 operator input\(s\) \(0 differ from defaults\)$/.test(l)));
});

check('W5 observe: every sheet write is v1.5.13\'s; the log names the inputs it reset', () => {
  const r = build({props: {PD_PANEL_PRESERVE: 'observe'}});
  assert.deepEqual(r.trace, golden.operator_trace);
  assert.deepEqual(r.panel, golden.operator_panel);
  const line = r.logs.filter((l) => l.indexOf('layout: observe') >= 0);
  assert.equal(line.length, 1);
  assert.ok(line[0].endsWith('layout: observe -- 5 operator input(s) reset to defaults: ' +
                             RESET.join(', ')), line[0]);
  const rr = refresh({props: {PD_PANEL_PRESERVE: 'observe'}});
  assert.deepEqual(rr.controls, golden.refresh_controls);
  assert.equal(sha(rr.trace), golden.refresh_trace_sha256);
  dump.observe = line[0];
});

check('W6 the mode tolerates case and surrounding blanks like the other PD properties', () => {
  for (const raw of ['ENFORCE', ' enforce ', 'Enforce\t']) {
    assert.deepEqual(build({props: {PD_PANEL_PRESERVE: raw}}).panel, OPERATOR, JSON.stringify(raw));
  }
  assert.equal(ctx0.PD_VERSION, '1.5.15');   // merge note 2026-10-21 (#780): renumbered v1.5.15 over #810's v1.5.14
  assert.ok(/\n \* Version: 1\.5\.15\n/.test(SOURCE));
});

check('W7 observe: a snapshot read fault is logged and the build carries on as v1.5.13', () => {
  const r = build({props: {PD_PANEL_PRESERVE: 'observe'}, sheetOpts: {faultBeforeClear: true}});
  assert.deepEqual(r.trace, golden.operator_trace);
  assert.deepEqual(r.panel, golden.operator_panel);
  assert.ok(r.logs.some((l) => l.endsWith('layout: panel snapshot failed (Service Spreadsheets ' +
                                          'timed out); observe continues with the defaults')), r.logs.join('\n'));
});

/* ----------------------------------------------------------------- NEVER -- */
check('N1 off and every unknown spelling: v1.5.13 byte-identical, no panel cell read', () => {
  for (const raw of [undefined, '', ' ', 'off', 'on', 'true', '1', 'yes', 'preserve',
                     'enforced', 'observe-ish', 'keep']) {
    const props = raw === undefined ? {} : {PD_PANEL_PRESERVE: raw};
    const r = build({props: props});
    assert.deepEqual(r.trace, golden.operator_trace, 'mode ' + JSON.stringify(raw));
    assert.deepEqual(r.panel, golden.operator_panel);
    assert.equal(r.reads.filter((x) => !x[2]).length, 0, 'read before clear: ' + JSON.stringify(raw));
    assert.equal(r.logs.filter((l) => l.indexOf('layout: ') >= 0).length, 0);
  }
  assert.deepEqual(fresh.trace, golden.fresh_trace);
  assert.deepEqual(golden.operator_panel, SEEDED);        // v1.5.13 honours _Lists_Config
  assert.deepEqual(offRefresh.controls, golden.refresh_controls);
  assert.equal(sha(offRefresh.trace), golden.refresh_trace_sha256);
  /* a PropertiesService fault reads off, never enforce */
  const w = world({props: {PD_PANEL_PRESERVE: 'enforce'}, propsThrow: true});
  assert.equal(w.ctx.pdPanelPreserveMode_(), 'off');
});

check('N2 a renamed label keeps nothing for that cell; a moved panel keeps nothing at all', () => {
  const renamed = build({props: {PD_PANEL_PRESERVE: 'enforce'}, edit(cells) {
    cells.set('5,1', 'PF: Cash Available (SAR)');
  }});
  assert.equal(renamed.panel['PF: Cash Available SAR'], 0);          // _Lists_Config default
  assert.equal(renamed.panel['PF: Max Position %'], 20);              // still kept
  const moved = build({props: {PD_PANEL_PRESERVE: 'enforce'}, edit(cells) {
    const shifted = new Map();
    for (const [k, v] of cells) {
      const [r, c] = k.split(',').map(Number);
      shifted.set((r >= 4 ? r + 1 : r) + ',' + c, v);
    }
    cells.clear();
    for (const [k, v] of shifted) cells.set(k, v);
  }});
  assert.deepEqual(moved.panel, golden.operator_panel);
  assert.ok(moved.logs.some((l) => /layout: preserved 0 operator input\(s\)/.test(l)));
});

check('N3 pure seed: preserved > _Lists_Config > built-in; no map is the v1.5.13 expression', () => {
  const s = ctx0.pdPanelSeedValue_;
  const item = {label: 'PF: Cash Available SAR', def: 0, kind: 'sar'};
  assert.equal(s(item, {}, {'PF: Cash Available SAR': 53769.2625}), 53769.2625);
  assert.equal(s(item, {'PF: Cash Available SAR': 5000}, {}), 5000);
  assert.equal(s(item, {}, {}), 0);
  assert.equal(s(item, {'PF: Cash Available SAR': 5000}, {'PF: Cash Available SAR': 7}), 7);
  for (const blank of ['', ' ', '\u2014', '-', 'n/a', null, undefined, NaN, Infinity]) {
    assert.equal(s(item, {'PF: Cash Available SAR': 5000}, {'PF: Cash Available SAR': blank}), 5000,
                 String(blank));
    assert.equal(s(item, {'PF: Cash Available SAR': blank}, null), 0, String(blank));
  }
  assert.equal(s(item, null, null), 0);
  assert.equal(s(item, {'PF: Cash Available SAR': 0}, {'PF: Cash Available SAR': 0}), 0);
  const defaults = {};
  DEFAULTS.forEach((r) => { defaults[r[0]] = r[1]; });
  for (const it of ctx0.PD_PANEL) {
    const old = defaults.hasOwnProperty(it.label) && ctx0.pdHasValue_(defaults[it.label])
        ? defaults[it.label] : it.def;
    assert.equal(s(it, defaults, null), old, it.label);
    assert.equal(s(it, defaults, {}), old, it.label);
  }
});

check('N4 the snapshot never throws: no sheet, an empty sheet, a faulting sheet -> {}', () => {
  const snap = ctx0.pdPanelPreserve_;
  assert.equal(JSON.stringify(snap(null)), '{}');
  assert.equal(JSON.stringify(snap(makeSheet('x'))), '{}');
  const faulty = makeSheet('Portfolio_Decision', operatorCells(), {throwOnRead: true});
  assert.equal(JSON.stringify(snap(faulty)), '{}');
  const faults = [];
  assert.equal(JSON.stringify(snap(faulty, faults)), '{}');
  assert.deepEqual(faults, ['synthetic getRange fault']);
  const clean = [];
  snap(makeSheet('Portfolio_Decision', operatorCells()), clean);
  assert.deepEqual(clean, []);                            // an ordinary read records nothing
  const ok = snap(makeSheet('Portfolio_Decision', operatorCells()));
  assert.deepEqual(JSON.parse(JSON.stringify(ok)), OPERATOR);
});

check('N5 the v1.5.15 header block and the added functions are ASCII', () => {
  const a = SOURCE.indexOf(' * v1.5.15 MAIN CHANGES');
  const b = SOURCE.indexOf(' * v1.5.14 (2026-10-20)  (LEDGER READER');   // merge note 2026-10-21 (#780): renumbered v1.5.15 over #810's v1.5.14
  const c = SOURCE.indexOf('/* ----- v1.5.15 operator inputs survive');
  const d = SOURCE.indexOf('/* ----- holdings pool (reads the 122-col');
  assert.ok(a > 0 && b > a && c > 0 && d > c);
  for (const block of [SOURCE.slice(a, b), SOURCE.slice(c, d)]) {
    assert.ok(/^[\x00-\x7f]*$/.test(block));
  }
  const e = SOURCE.indexOf('/* v1.5.15: snapshot operator inputs BEFORE the clear');
  const f = SOURCE.indexOf("Logger.log('[MP-DEC v' + PD_VERSION + '] layout built');");
  assert.ok(e > 0 && f > e && /^[\x00-\x7f]*$/.test(SOURCE.slice(e, f).replace(/\\u2014/g, '')));
});

check('N6 enforce: a snapshot read fault aborts BEFORE the clear; the panel is untouched', () => {
  let thrown = null;
  let r = null;
  try {
    r = build({props: {PD_PANEL_PRESERVE: 'enforce'}, sheetOpts: {faultBeforeClear: true}});
  } catch (e) { thrown = e; }
  assert.ok(thrown && /panel snapshot failed \(Service Spreadsheets timed out\); rebuild aborted, sheet left untouched/
      .test(String(thrown.message)), String(thrown));
  const pd = makeSheet('Portfolio_Decision', operatorCells(), {faultBeforeClear: true});
  const w = world({props: {PD_PANEL_PRESERVE: 'enforce'}, pd: pd});
  assert.throws(() => w.ctx.buildPortfolioDecisionLayout(), /rebuild aborted/);
  assert.deepEqual(pd.__trace, []);                       // not one write, no clear
  assert.deepEqual(panelOf(pd), OPERATOR);
  const rr = refresh({props: {PD_PANEL_PRESERVE: 'enforce'}, sheetOpts: {faultBeforeClear: true},
                      allowThrow: true});
  assert.ok(rr.error && /rebuild aborted/.test(String(rr.error.message)));
  assert.equal(rr.posts, 0);                              // nothing sized on defaults
  assert.deepEqual(rr.trace, []);
  assert.deepEqual(rr.panel, OPERATOR);
});

check('N7 off: read faults are irrelevant (no snapshot), v1.5.13 byte-identical', () => {
  const r = build({sheetOpts: {faultBeforeClear: true}});
  assert.deepEqual(r.trace, golden.operator_trace);
  assert.equal(r.logs.filter((l) => l.indexOf('layout: ') >= 0).length, 0);
  const rr = refresh({sheetOpts: {faultBeforeClear: true}, allowThrow: true});
  assert.equal(rr.error, null);
  assert.deepEqual(rr.controls, golden.refresh_controls);
});

/* ------------------------------------------------------------------ GOLD -- */
check('G1 golden negative: v1.5.13 resets cash to 0 and Max Position to 15; enforce does not', () => {
  assert.equal(golden.operator_panel['PF: Cash Available SAR'], 0);
  assert.equal(golden.operator_panel['PF: Max Position %'], 15);
  assert.equal(golden.refresh_controls['PF: Cash Available SAR'], 0);
  assert.equal(golden.refresh_controls['PF: Max Position %'], 15);
  const r = build({props: {PD_PANEL_PRESERVE: 'enforce'}});
  assert.notDeepEqual(r.panel, golden.operator_panel);
  assert.notDeepEqual(r.trace, golden.operator_trace);
});

check('G2 live base (PF_BASE_GAS): off == base byte for byte; enforce on base still resets', () => {
  if (!process.env.PF_BASE_GAS) { console.log('  (PF_BASE_GAS unset: golden fixture only)'); return; }
  const base = fs.readFileSync(process.env.PF_BASE_GAS, 'utf8');
  // merge note 2026-10-21 (#780): the base is the v1.5.13 golden source or main's v1.5.14
  // (#810 changed only _myPortfolioLedgerMap_, outside the panel path).
  const base13 = /var PD_VERSION = '1\.5\.13';/.test(base);
  assert.ok(base13 || /var PD_VERSION = '1\.5\.14';/.test(base));
  if (base13) assert.equal(crypto.createHash('sha256').update(base).digest('hex'), golden.source_sha256);
  const b = build({source: base});
  assert.deepEqual(offOp.trace, b.trace);
  assert.deepEqual(fresh.trace, build({source: base, fresh: true}).trace);
  const be = build({source: base, props: {PD_PANEL_PRESERVE: 'enforce'}});
  assert.equal(be.panel['PF: Cash Available SAR'], 0);
  assert.equal(be.panel['PF: Max Position %'], 15);
  const br = refresh({source: base, props: {PD_PANEL_PRESERVE: 'enforce'}});
  assert.equal(br.controls['PF: Cash Available SAR'], 0);
  assert.equal(sha(br.trace), sha(offRefresh.trace));
});

assert.equal(passed, TOTAL);
const digest = sha(dump).slice(0, 16);
if (process.env.PF_DUMP) fs.writeFileSync(process.env.PF_DUMP, JSON.stringify(dump, null, 1));
console.log('PASS ' + passed + '/' + TOTAL + ' digest ' + digest);
