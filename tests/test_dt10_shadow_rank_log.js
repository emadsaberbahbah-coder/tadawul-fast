#!/usr/bin/env node
/* tests/test_dt10_shadow_rank_log.js
 * Harness for apps_script/16_Decision_Top10.gs v1.17.0 [SHADOW-RANK-LOG].
 * Extracts the REAL functions from the delivered file (never a copy) and runs
 * them in node with PropertiesService / Utilities / Session / Logger stubs and
 * an in-memory spreadsheet that records every call. The payload is the REAL
 * opportunity_builder v1.28.0 shadow output for the A2 harness pool
 * (tests/fixtures/rank_challenger_payload_v1280.json), so the reader is
 * checked against the writer's actual keys. Sections:
 *   S1 mode reader                 S2 rows core on the builder fixture
 *   S3 nothing to log              S4 kill identity (property absent / off)
 *   S5 orchestrator on / all: create (header, hidden, view back, unused
 *      columns dropped), daily cadence (on) and on-change cadence (all), new
 *      day, narrow / empty / cleared / foreign-layout tab, grid growth
 *   S6 never throws                S7 embedded self-test core lines
 *   S8 wiring, version lockstep, ES5/ASCII lint, inventory
 *   S9 golden negative: the v1.16.0 base rebuilt by reversing the v1.17.0
 *      edits (tests/js/dt10_edits_v1170.json, SHA-256 pinned)
 * Run: node tests/test_dt10_shadow_rank_log.js   (exit 0 = all PASS)
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const history = require('./js/dt10_cash_utc_history');

const SRC = process.argv[2] || path.join(__dirname, '..', 'apps_script', '16_Decision_Top10.gs');
const src = fs.readFileSync(SRC, 'utf8');
// merge note 2026-10-27 (#778): the delivered file is main's v1.17.1 plus #778's
// held-seat regions (v1.17.2; tests/js/dt10_audit_keep_regions.js). Behaviour
// runs on the delivered file; the base oracle, the lint and the inventory
// compare main's v1.17.1 layer: the delivered file with exactly those regions
// removed (stripDesigned throws on any other shape).
// merge note 2026-10-28 (#778): the delivered file is now main's v1.18.0 plus the
// v1.18.1 held-seat regions, so the layer is main's v1.18.0 byte for byte.
const layerSrc = require('./js/dt10_audit_keep_regions.js').stripDesigned(src);
const dt10Floor = (v, f) => { const x = String(v).split('.').map(Number); for (let k = 0; k < f.length; k++) { if (x[k] !== f[k]) return x[k] > f[k]; } return true; };
const deliveredVersion = (src.match(/^var DT10_VERSION = '(\d+\.\d+\.\d+)';$/m) || [])[1];
const FIX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', 'rank_challenger_payload_v1280.json'), 'utf8'));

function extract(source, name) {
  const i = source.indexOf('\nfunction ' + name + '(');
  if (i < 0) throw new Error('missing ' + name);
  let d = 0;
  const j = source.indexOf('{', i);
  for (let k = j; k < source.length; k++) {
    if (source[k] === '{') d++;
    else if (source[k] === '}') { d--; if (!d) return source.slice(i + 1, k + 1); }
  }
  throw new Error('unbalanced ' + name);
}
function extractVar(source, name) {
  const m = source.match(new RegExp('^var ' + name + ' = [^;]+;', 'm'));
  if (!m) throw new Error('missing var ' + name);
  return m[0];
}

// ------------------------------------------------------------------ stubs
let PROPS = {};
let PROP_READS = [];
let PROP_WRITES = [];
let PROPS_THROW = false;
let LOGS = [];
let NOW_ISO = '2026-10-27T05:47:00Z';
global.PropertiesService = {
  getScriptProperties: () => {
    if (PROPS_THROW) throw new Error('property service down');
    return {
      getProperty: (k) => { PROP_READS.push(k); return Object.prototype.hasOwnProperty.call(PROPS, k) ? PROPS[k] : null; },
      setProperty: (k, v) => { PROP_WRITES.push([k, v]); PROPS[k] = String(v); }
    };
  }
};
global.Logger = { log: (m) => LOGS.push(String(m)) };
global.Session = { getScriptTimeZone: () => 'Asia/Riyadh' };
global.Utilities = {
  formatDate: (date, zone, pattern) => {
    const off = (zone === 'Etc/UTC' || zone === 'UTC') ? 0 : 3 * 3600000;
    const iso = new Date(Date.parse(NOW_ISO) + off).toISOString();
    if (pattern === 'yyyy-MM-dd') return iso.slice(0, 10);
    return iso.replace('T', ' ').slice(0, 19);
  }
};

function makeBook(opts) {
  opts = opts || {};
  const calls = [];
  const sheets = {};
  function mkSheet(name, cols) {
    const sh = {
      _name: name, _rows: [], _cols: cols || 26, _maxRows: 1000, _hidden: false, _frozen: 0,
      getRange(r, c, nr, nc) {
        calls.push(['getRange', name, r, c, nr, nc]);
        if (opts.rangeThrows) throw new Error('range write failed');
        if (r < 1 || c < 1 || c - 1 + nc > sh._cols || r - 1 + nr > sh._maxRows) {
          throw new Error('The coordinates of the range are outside the dimensions of the sheet.');
        }
        const rng = {
          getValues() {
            calls.push(['getValues', name, r, c, nr, nc]);
            const out = [];
            for (let i = 0; i < nr; i++) {
              const row = sh._rows[r - 1 + i] || [];
              const vals = [];
              for (let j = 0; j < nc; j++) vals.push(row[c - 1 + j] === undefined ? '' : row[c - 1 + j]);
              out.push(vals);
            }
            return out;
          },
          setValues(v) {
            calls.push(['setValues', name, r, c, nr, nc, JSON.stringify(v)]);
            if (v.length !== nr || v.some((row) => row.length !== nc)) throw new Error('dimension mismatch');
            for (let i = 0; i < v.length; i++) sh._rows[r - 1 + i] = v[i].slice();
            return rng;
          },
          setFontWeight(w) { calls.push(['setFontWeight', name, w]); return rng; }
        };
        return rng;
      },
      setFrozenRows(n) { calls.push(['setFrozenRows', name, n]); sh._frozen = n; },
      hideSheet() { calls.push(['hideSheet', name]); sh._hidden = true; },
      getMaxColumns() { return sh._cols; },
      getMaxRows() { return sh._maxRows; },
      insertColumnsAfter(after, n) { calls.push(['insertColumnsAfter', name, after, n]); sh._cols += n; },
      deleteColumns(at, n) { calls.push(['deleteColumns', name, at, n]); sh._cols -= n; },
      insertRowsAfter(after, n) { calls.push(['insertRowsAfter', name, after, n]); sh._maxRows += n; },
      getLastRow() { return sh._rows.length; }
    };
    return sh;
  }
  const ss = {
    getSheetByName(n) { calls.push(['getSheetByName', n]); return sheets[n] || null; },
    insertSheet(n) { calls.push(['insertSheet', n]); sheets[n] = mkSheet(n, 26); return sheets[n]; },
    setActiveSheet(sh) { calls.push(['setActiveSheet', sh ? sh._name : null]); return sh; }
  };
  sheets.Top_10_Investments = mkSheet('Top_10_Investments', 40);
  if (opts.existing) {
    const ex = mkSheet(opts.existing.name, opts.existing.cols);
    ex._rows = opts.existing.rows === undefined ? [['hdr']] : opts.existing.rows;
    if (opts.existing.maxRows) ex._maxRows = opts.existing.maxRows;
    sheets[opts.existing.name] = ex;
  }
  return { ss, calls, sheets };
}

const VARS = ['DT10_VERSION', 'DT10_SHADOW_LOG_PROP', 'DT10_SHADOW_LOG_SHEET', 'DT10_SHADOW_LOG_STATE_PROP',
  'DT10_SHADOW_LOG_HEADERS'];
const NEW_FUNCS = ['dt10ShadowLogMode_', 'dt10ShadowLogRows_', 'dt10AppendShadowRankLog_'];
const FUNCS = ['dt10NormSym_', 'dt10Cell_', 'dt10StabToday_'].concat(NEW_FUNCS);
const geval = eval;   // indirect eval: global scope, so the real functions are callable here
for (const v of VARS) geval(extractVar(src, v));
geval(extractVar(src, 'DT10_SHEET'));
for (const f of FUNCS) geval(extract(src, f));

let pass = 0;
function ok(cond, label) {
  if (!cond) { console.error('FAIL ' + label); process.exit(1); }
  pass++;
}
function deep(a, b) { return JSON.stringify(a) === JSON.stringify(b); }
function payload(over) { return Object.assign(JSON.parse(JSON.stringify(FIX)), over || {}); }

// ---------------------------------------------------------------- S1 mode
PROPS = {}; ok(dt10ShadowLogMode_() === 'off', 'S1 absent -> off');
PROPS = { DT10_SHADOW_RANK_LOG: '' }; ok(dt10ShadowLogMode_() === 'off', 'S1 blank -> off');
PROPS = { DT10_SHADOW_RANK_LOG: ' ON ' }; ok(dt10ShadowLogMode_() === 'on', 'S1 on (trim, case)');
PROPS = { DT10_SHADOW_RANK_LOG: ' All ' }; ok(dt10ShadowLogMode_() === 'all', 'S1 all (trim, case)');
for (const junk of ['observe', '1', 'true', 'enforce', 'onn', 'always', 'al']) {
  PROPS = { DT10_SHADOW_RANK_LOG: junk }; ok(dt10ShadowLogMode_() === 'off', 'S1 ' + junk + ' -> off');
}
PROPS = { DT10_SHADOW_RANK_LOG: 'on' }; PROPS_THROW = true;
ok(dt10ShadowLogMode_() === 'off', 'S1 service fault -> off'); PROPS_THROW = false;
ok(DT10_SHADOW_LOG_PROP === 'DT10_SHADOW_RANK_LOG' && DT10_SHADOW_LOG_SHEET === '_Shadow_Rank_Log' &&
   DT10_SHADOW_LOG_STATE_PROP === 'DT10_SHADOW_LOG_STATE_V1', 'S1 names');
console.log('S1 PASS mode reader');

// ------------------------------------------------------------ S2 rows core
const H = DT10_SHADOW_LOG_HEADERS;
ok(deep(H, ['Logged At', 'Day', 'Cohort', 'Order', 'Rank', 'Symbol', 'Market', 'Sector', 'Seat', 'Live Rank', 'Shadow Rank',
  'Engine ROI %', 'Rel', 'Conf', 'Momentum', 'Strength', 'Strength Ref', 'Cap', 'Floor OK', 'Mode', 'Rule', 'Builder', 'Pool',
  'Overlap Top5']), 'S2 header (24 columns)');
const col = (name) => H.indexOf(name);
const p2 = payload();
// live-board decorations the cockpit adds after stability: a seat class, a grace
// hold outside today's INVEST pool, and a seat whose ticket lost its shadow_rank
p2.selected[0]._stab_status = 'ACTIVE (3/3)';
p2.selected[1]._stab_status = 'FAST-TRACK 1/3';
delete p2.selected[2].shadow_rank;                       // F.SR: falls back to the audit row (1)
p2.selected.push({ rank: 6, symbol: 'GHOST.US', market: 'NYSE', sector: 'Financials', _stab_status: 'GRACE 1/3',
                   engine_roi_pct: 21.4, reliability: 74, confidence_band: 'Medium' });
const b2 = dt10ShadowLogRows_(p2, '2026-10-27 08:47:00', '2026-10-27', '40418101374');
ok(b2.rows.length === 6 + 6 + 6 && b2.reason === '', 'S2 6 champion + 6 challenger + 6 board rows');
ok(b2.rows.every((r) => r.length === H.length), 'S2 every row is header-wide');
const byOrder = (o) => b2.rows.filter((r) => r[col('Order')] === o);
ok(deep(byOrder('CHAMPION').map((r) => r[col('Symbol')]), ['A.SR', 'D.SR', 'F.SR', 'G.SR', 'B.SR', 'E.SR']), 'S2 champion = the live order');
ok(deep(byOrder('CHAMPION').map((r) => r[col('Rank')]), [1, 2, 3, 4, 5, 6]), 'S2 champion Rank = live rank');
ok(deep(byOrder('CHALLENGER').map((r) => r[col('Symbol')]), ['F.SR', 'B.SR', 'G.SR', 'A.SR', 'D.SR', 'E.SR']), 'S2 challenger = the shadow order');
ok(deep(byOrder('CHALLENGER').map((r) => r[col('Rank')]), [1, 2, 3, 4, 5, 6]), 'S2 challenger Rank = shadow rank');
const a0 = byOrder('CHAMPION')[0];
ok(deep(a0, ['2026-10-27 08:47:00', '2026-10-27', '40418101374', 'CHAMPION', 1, 'A.SR', 'Tadawul', 'Banks', '', 1, 4, 35, 88, 'High',
  60, 7.5, 'pool', 'Y', 'Y', 'shadow', 'v1.0', 'v1.28.0', 6, 5]), 'S2 champion row A.SR (cap Y, floor Y, strength +7.5 vs pool; Rule / Builder as text) ' + JSON.stringify(a0));
const noVer = payload(); delete noVer.meta.rank_challenger.version; delete noVer.meta.rank_challenger.builder;
ok(dt10ShadowLogRows_(noVer, 's', 'd', 'c').rows.every((r) => r[col('Rule')] === '' && r[col('Builder')] === ''), 'S2 missing version -> blank, never a bare "v"');
const e5 = byOrder('CHALLENGER')[5];
ok(e5[col('Symbol')] === 'E.SR' && e5[col('Floor OK')] === 'N' && e5[col('Cap')] === 'N' && e5[col('Engine ROI %')] === 10 && e5[col('Live Rank')] === 6,
   'S2 challenger row E.SR (below floor -> Floor OK N)');
const g = byOrder('CHALLENGER')[2];
ok(g[col('Symbol')] === 'G.SR' && g[col('Momentum')] === '' && g[col('Strength')] === '' && g[col('Strength Ref')] === '', 'S2 no momentum -> blank cells');
const board = byOrder('BOARD');
ok(deep(board.map((r) => [r[col('Rank')], r[col('Symbol')], r[col('Seat')], r[col('Live Rank')], r[col('Shadow Rank')]]),
  [[1, 'A.SR', 'ACTIVE', 1, 4], [2, 'D.SR', 'FAST-TRACK', 2, 5], [3, 'F.SR', '', 3, 1], [4, 'G.SR', '', 4, 3], [5, 'B.SR', '', 5, 2],
   [6, 'GHOST.US', 'GRACE', '', '']]), 'S2 board rows: seat class, live rank from the lists, shadow rank (ticket, else audit row, else blank)');
const ghost = board[5];
ok(ghost[col('Engine ROI %')] === 21.4 && ghost[col('Rel')] === 74 && ghost[col('Conf')] === 'Medium' && ghost[col('Cap')] === '' &&
   ghost[col('Market')] === 'NYSE', 'S2 grace seat keeps its own ticket figures; cap/floor unknown -> blank');
ok(b2.sig === '2026-10-27|C:A.SR,D.SR,F.SR,G.SR,B.SR,E.SR|S:F.SR,B.SR,G.SR,A.SR,D.SR,E.SR|B:1:A.SR,2:D.SR,3:F.SR,4:G.SR,5:B.SR,6:GHOST.US', 'S2 signature ' + b2.sig);
const noRank = payload(); noRank.selected.forEach((t) => { delete t.rank; });
ok(deep(dt10ShadowLogRows_(noRank, 's', 'd', 'c').rows.filter((r) => r[3] === 'BOARD').map((r) => r[4]), [1, 2, 3, 4, 5]), 'S2 missing board rank -> position');
ok(JSON.stringify(b2.rows).indexOf('suggested') < 0 && !b2.rows.some((r) => r.some((v) => /SAR\b|price/i.test(String(v)))), 'S2 no ticket, price or amount in the log');
console.log('S2 PASS rows core on the builder fixture');

// ------------------------------------------------------------- S3 nothing
ok(deep(dt10ShadowLogRows_({}, 's', 'd', 'c'), { rows: [], sig: '', reason: 'no challenger block' }), 'S3 no meta');
ok(dt10ShadowLogRows_({ meta: { rank_challenger: 'x' } }, 's', 'd', 'c').reason === 'no challenger block', 'S3 non-object block');
ok(dt10ShadowLogRows_({ meta: { rank_challenger: { version: '1.0', mode: 'shadow', error: 'ValueError' } } }, 's', 'd', 'c').reason === 'challenger error ValueError', 'S3 error block');
ok(dt10ShadowLogRows_({ meta: { rank_challenger: { champion: [], challenger: [] } } }, 's', 'd', 'c').reason === 'empty challenger lists', 'S3 empty lists');
ok(dt10ShadowLogRows_(null, 's', 'd', 'c').rows.length === 0, 'S3 null payload');
console.log('S3 PASS nothing to log');

// ------------------------------------------------------------ S4 kill identity
for (const value of [undefined, '', 'off', 'observe']) {
  PROPS = value === undefined ? {} : { DT10_SHADOW_RANK_LOG: value }; PROP_READS = []; PROP_WRITES = []; LOGS = [];
  const bk = makeBook();
  const before = JSON.stringify(FIX);
  const note = dt10AppendShadowRankLog_(bk.ss, payload(), '40418101374');
  ok(note === '' && bk.calls.length === 0 && deep(PROP_READS, ['DT10_SHADOW_RANK_LOG']) && PROP_WRITES.length === 0 && LOGS.length === 0 &&
     JSON.stringify(FIX) === before, 'S4 property ' + JSON.stringify(value) + ': no token, no sheet call, one read, no write');
}
console.log('S4 PASS kill identity');

// ------------------------------------------------------------ S5 orchestrator on / all
PROPS = { DT10_SHADOW_RANK_LOG: 'on' }; PROP_WRITES = []; NOW_ISO = '2026-10-27T05:47:00Z';
const bk = makeBook();
let n = dt10AppendShadowRankLog_(bk.ss, payload(), '40418101374');
const tab = bk.sheets._Shadow_Rank_Log;
ok(n === 'ShadowLog: +17' && tab && tab._hidden === true && tab._frozen === 1, 'S5 first event: tab created hidden + frozen, 17 rows: ' + n);
ok(deep(tab._rows[0], H) && bk.calls.some((c) => c[0] === 'setFontWeight' && c[2] === 'bold'), 'S5 bold header row');
ok(tab._cols === 24 && bk.calls.some((c) => c[0] === 'deleteColumns' && c[2] === 25 && c[3] === 2), 'S5 the two unused columns dropped at creation');
ok(bk.calls.some((c) => c[0] === 'setActiveSheet' && c[1] === 'Top_10_Investments') &&
   bk.calls.findIndex((c) => c[0] === 'setActiveSheet') > bk.calls.findIndex((c) => c[0] === 'hideSheet'), 'S5 creation returns the view to Top_10_Investments');
ok(tab._rows.length === 18 && tab._rows[1][0] === '2026-10-27 08:47:00' && tab._rows[1][1] === '2026-10-27' && tab._rows[1][2] === '40418101374', 'S5 rows at 2..18; Logged At Riyadh, Day UTC, Cohort');
const st1 = JSON.parse(PROPS.DT10_SHADOW_LOG_STATE_V1);
ok(st1.date === '2026-10-27' && st1.sig.indexOf('2026-10-27|C:A.SR,D.SR') === 0, 'S5 state saved');
let calls0 = bk.calls.length;
n = dt10AppendShadowRankLog_(bk.ss, payload(), '40418101374');
ok(n === 'ShadowLog: logged today' && bk.calls.length === calls0 && tab._rows.length === 18, 'S5 on: same day -> logged today, no sheet call');
const moved = payload();
moved.meta.rank_challenger.challenger = moved.meta.rank_challenger.challenger.slice().reverse();
n = dt10AppendShadowRankLog_(bk.ss, moved, '40418101374');
ok(n === 'ShadowLog: logged today' && bk.calls.length === calls0 && tab._rows.length === 18, 'S5 on: a changed order the same day is NOT logged (daily cadence)');
PROPS.DT10_SHADOW_RANK_LOG = 'all';
n = dt10AppendShadowRankLog_(bk.ss, moved, '40418101374');
ok(n === 'ShadowLog: +17' && tab._rows.length === 35 && bk.calls.filter((c) => c[0] === 'insertSheet').length === 1, 'S5 all: a changed order appends again (no second tab)');
calls0 = bk.calls.length;
n = dt10AppendShadowRankLog_(bk.ss, moved, '40418101374');
ok(n === 'ShadowLog: unchanged' && bk.calls.length === calls0 && tab._rows.length === 35, 'S5 all: same orders -> unchanged, no sheet call');
PROPS.DT10_SHADOW_RANK_LOG = 'on'; NOW_ISO = '2026-10-28T05:47:00Z';
n = dt10AppendShadowRankLog_(bk.ss, moved, '40418999999');
ok(n === 'ShadowLog: +17' && tab._rows.length === 52 && tab._rows[51][1] === '2026-10-28' && JSON.parse(PROPS.DT10_SHADOW_LOG_STATE_V1).date === '2026-10-28', 'S5 on: new UTC day -> appended with the new Day key');
ok(bk.calls.filter((c) => c[0] === 'setFontWeight').length === 1, 'S5 the header is written once, not per event');
n = dt10AppendShadowRankLog_(bk.ss, { meta: {} }, 'x');
ok(n === 'ShadowLog: none (no challenger block)' && tab._rows.length === 52, 'S5 backend at legacy -> none, nothing written');
NOW_ISO = '2026-10-27T05:47:00Z';
function fresh(existing) { PROPS = { DT10_SHADOW_RANK_LOG: 'on' }; return makeBook({ existing: existing }); }
const narrow = fresh({ name: '_Shadow_Rank_Log', cols: 20, rows: [H.slice(0, 20)] });
n = dt10AppendShadowRankLog_(narrow.ss, payload(), 'c');
ok(n === 'ShadowLog: +17' && narrow.calls.some((c) => c[0] === 'insertColumnsAfter' && c[2] === 20 && c[3] === 4) &&
   !narrow.calls.some((c) => c[0] === 'insertSheet') && deep(narrow.sheets._Shadow_Rank_Log._rows[0], H) &&
   narrow.sheets._Shadow_Rank_Log._rows.length === 18, 'S5 an existing narrower tab gains the missing columns and the full header');
const empty = fresh({ name: '_Shadow_Rank_Log', cols: 26, rows: [] });
n = dt10AppendShadowRankLog_(empty.ss, payload(), 'c');
const et = empty.sheets._Shadow_Rank_Log;
ok(n === 'ShadowLog: +17' && deep(et._rows[0], H) && et._rows.length === 18 && et._frozen === 1 && !empty.calls.some((c) => c[0] === 'insertSheet' || c[0] === 'deleteColumns'),
   'S5 an existing EMPTY tab (first run cut short, or emptied by hand) gets the header before the rows');
const cleared = fresh({ name: '_Shadow_Rank_Log', cols: 24, rows: [new Array(24).fill(''), ['old'].concat(new Array(23).fill(''))] });
n = dt10AppendShadowRankLog_(cleared.ss, payload(), 'c');
const ct = cleared.sheets._Shadow_Rank_Log;
ok(n === 'ShadowLog: +17' && deep(ct._rows[0], H) && ct._rows[1][0] === 'old' && ct._rows.length === 19, 'S5 a cleared header row is restored; existing rows kept, new rows below them');
const foreign = fresh({ name: '_Shadow_Rank_Log', cols: 24, rows: [['Date', 'Note']] });
n = dt10AppendShadowRankLog_(foreign.ss, payload(), 'c');
ok(n === 'ShadowLog: ERROR header mismatch at column 1' && !foreign.calls.some((c) => c[0] === 'setValues') &&
   !PROPS.DT10_SHADOW_LOG_STATE_V1, 'S5 a tab with another layout is never written; state not saved');
const shifted = fresh({ name: '_Shadow_Rank_Log', cols: 24, rows: [H.slice(0, 5).concat(['Seat'], H.slice(6))] });
ok(dt10AppendShadowRankLog_(shifted.ss, payload(), 'c') === 'ShadowLog: ERROR header mismatch at column 6', 'S5 one wrong header cell names its column');
const small = fresh({ name: '_Shadow_Rank_Log', cols: 24, rows: [H.slice()], maxRows: 5 });
n = dt10AppendShadowRankLog_(small.ss, payload(), 'c');
ok(n === 'ShadowLog: +17' && small.calls.some((c) => c[0] === 'insertRowsAfter' && c[2] === 5 && c[3] === 13) &&
   small.sheets._Shadow_Rank_Log._rows.length === 18, 'S5 rows are added before a write that would pass the grid');
PROPS = { DT10_SHADOW_RANK_LOG: 'on', DT10_SHADOW_LOG_STATE_V1: '{not json' };
const healed = makeBook();
ok(dt10AppendShadowRankLog_(healed.ss, payload(), 'c') === 'ShadowLog: +17', 'S5 unreadable state -> treated as no state');
console.log('S5 PASS orchestrator on / all');

// --------------------------------------------------------------- S6 never throws
PROPS = { DT10_SHADOW_RANK_LOG: 'on' }; LOGS = [];
const broken = makeBook({ rangeThrows: true });
n = dt10AppendShadowRankLog_(broken.ss, payload(), 'c');
ok(/^ShadowLog: ERROR Error: range write failed/.test(n) && LOGS.some((l) => /ShadowLog ERROR/.test(l)) && !PROPS.DT10_SHADOW_LOG_STATE_V1, 'S6 a write fault -> ERROR token, logged, state not saved: ' + n);
ok(dt10AppendShadowRankLog_(null, payload(), 'c').indexOf('ShadowLog: ERROR') === 0, 'S6 no spreadsheet -> ERROR token');
console.log('S6 PASS never throws');

// ----------------------------------------------------------- S7 self-test core
const s0 = src.indexOf('  // v1.17.0 [SHADOW-RANK-LOG]: pure rows core (nothing is written).');
const s1 = src.indexOf('  // v1.4.0: page-status dry check', s0);
ok(s0 > 0 && s1 > s0, 'S7 self-test block located');
const report = [];
PROPS = {};
new Function('report', src.slice(s0, s1))(report);
ok(deep(report, ['shadow rank log (DT10_SHADOW_RANK_LOG): off', 'shadow rank log core: ok']), 'S7 self-test lines: ' + JSON.stringify(report));
console.log('S7 PASS embedded self-test core');

// ---------------------------------------------------------- S8 wiring + lint
// 2026-10-27: v1.17.1 (shadow momentum) stacked on this change; pins moved (S9 checks the v1.17.0 file itself)
// 2026-10-27: v1.18.0 (post-earnings tag) stacked as well; pins moved
ok((layerSrc.match(/^var DT10_VERSION = '1\.18\.0';$/m) || []).length === 1 && /^ \* Version: 1\.18\.0 \(see DT10_VERSION;/.test(layerSrc.split('\n')[3]), 'S8 version lockstep 1.18.0 (layer)');
ok(dt10Floor(deliveredVersion, [1, 18, 0]) && (src.match(/^var DT10_VERSION = '/mg) || []).length === 1 &&
   src.split('\n')[3].indexOf(' * Version: ' + deliveredVersion + ' (see DT10_VERSION;') === 0,
   'S8 delivered version floor 1.18.0 in lockstep: ' + deliveredVersion);   // merge note 2026-10-28 (#778): held seats are v1.18.1 over #855's v1.18.0
const orchStart = src.indexOf('  var exitNote = dt10AppendExitLog_(ss,');
ok(orchStart > 0 && src.lastIndexOf('\nfunction ', orchStart) === src.indexOf('\nfunction dt10RefreshDecisionTop10Run_(run) {'),
   'S8 exit-log call located inside dt10RefreshDecisionTop10Run_ (the refreshDecisionTop10 body)');
const wiring = src.slice(orchStart, src.indexOf('  statusCell.setValue(finalStatusLine);', orchStart));
ok(wiring.indexOf("  var shadowNote = dt10AppendShadowRankLog_(ss, payload,\n      finalSource && finalSource.ready ? finalSource.cohort : '');") > 0, 'S8 one call after the exit log, cohort from the final source readiness');
ok(wiring.indexOf("  if (exitNote) finalStatusLine += ' | ' + exitNote;\n  if (shadowNote) finalStatusLine += ' | ' + shadowNote;   // v1.17.0") > 0, 'S8 token appended only when non-empty');
ok((src.match(/dt10AppendShadowRankLog_\(/g) || []).length === 2, 'S8 exactly one call site (+ the definition)');
const b0 = src.indexOf('// v1.17.0 [SHADOW-RANK-LOG] -- the backend');
const b1 = src.indexOf('/**\n * Append the rendered board to _Selection_Log. Modes', b0);
const block = src.slice(b0, b1);
ok(b0 > 0 && b1 > b0, 'S8 code block located');
ok(!/\b(let|const|class)\s|=>|`|\.\.\./.test(block), 'S8 ES5 only');
ok(!Array.from(block).some((ch) => ch.charCodeAt(0) > 126), 'S8 ASCII only (code block)');
const why = src.slice(src.indexOf(' * v1.17.0 (2026-10-27) [SHADOW-RANK-LOG]'), src.indexOf(' * v1.16.0 (2026-10-26) [FUNDING-EVIDENCE]'));
ok(why.length > 2000 && /^[\x09\x0a\x20-\x7e]*$/.test(why), 'S8 WHY block present and ASCII');
const selfBlock = src.slice(s0, s1);
ok(!/\b(let|const|class)\s|=>|`/.test(selfBlock) && /^[\x09\x0a\x20-\x7e]*$/.test(selfBlock), 'S8 self-test block ES5 + ASCII');
for (const f of NEW_FUNCS) ok(block.indexOf('\nfunction ' + f + '(') > 0, 'S8 inventory ' + f);
for (const v of VARS.slice(1)) ok(block.indexOf('\nvar ' + v + ' = ') > 0, 'S8 inventory ' + v);
new vm.Script(src, { filename: '16_Decision_Top10.gs' });
console.log('S8 PASS wiring, lockstep, lint, inventory');

// -------------------------------------------------------- S9 golden negative
const base = history.v1160Source(layerSrc);   // merge note 2026-10-27 (#778): held seats are v1.17.2 over #852's v1.17.1; v1.18.1 over #855's v1.18.0 since 2026-10-28
const sha = crypto.createHash('sha256').update(base).digest('hex');
ok(sha === history.V1160_SHA256, 'S9 base is the reviewed v1.16.0 file (SHA-256 pinned)');
ok(/var DT10_VERSION = '1\.16\.0';/.test(base) && base.indexOf('ShadowLog') < 0 && base.indexOf('DT10_SHADOW') < 0 &&
   NEW_FUNCS.every((f) => base.indexOf('function ' + f + '(') < 0), 'S9 the base lacks every added name and the status token');
function census(s, re) { const out = new Set(); let m; while ((m = re.exec(s))) out.add(m[1]); return out; }
// 2026-10-27: the census compares v1.16.0 with the v1.17.0 file rebuilt from the delivered one (v1.17.1 edits reversed, SHA-256 pinned)
const s1170 = history.v1170Source(layerSrc);   // merge note 2026-10-27 (#778): held seats are v1.17.2 over #852's v1.17.1; v1.18.1 over #855's v1.18.0 since 2026-10-28
ok(crypto.createHash('sha256').update(s1170).digest('hex') === history.V1170_SHA256, 'S9 the v1.17.0 file is rebuilt exactly (SHA-256 pinned)');
const fb = census(base, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = census(s1170, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
const vb = census(base, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = census(s1170, /^var\s+([A-Za-z_$][\w$]*)/mg);
ok([...fb].every((f) => fn.has(f)) && [...vb].every((v) => vn.has(v)), 'S9 zero functions / vars removed');
ok(deep([...fn].filter((f) => !fb.has(f)).sort(), NEW_FUNCS.slice().sort()) && deep([...vn].filter((v) => !vb.has(v)).sort(), VARS.slice(1).sort()),
   'S9 exactly the designed names added');
ok(fb.size === 214 && fn.size === 217 && vb.size === 112 && vn.size === 116, 'S9 census 214 -> 217 functions, 112 -> 116 vars');
const edits = require('./js/dt10_edits_v1170.json');
ok(edits.length === 7 && edits.every((e) => e[0].indexOf(e[1].replace(/\n$/, '')) >= 0 || e[1].indexOf('1.16.0') >= 0), 'S9 seven hunks; every insertion keeps its anchor');
console.log('S9 PASS golden negative on the v1.16.0 base ' + sha.slice(0, 12));

const digest = crypto.createHash('sha256').update(src).digest('hex').slice(0, 12);
console.log('ALL PASS (' + pass + ' checks) on ' + path.basename(SRC) + ' sha256 ' + digest);
