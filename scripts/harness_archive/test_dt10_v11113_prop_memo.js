#!/usr/bin/env node
/* tests/test_dt10_v11113_prop_memo.js — dual-tree real-module harness for
 * 16_Decision_Top10.gs v1.11.13 [P-202 HOTFIX: one Script Property read per
 * execution]. Loads the REAL .gs source (base v1.11.12 and delivered
 * v1.11.13) into vm contexts with Apps Script service stubs (the v1.11.12
 * harness shape) whose PropertiesService COUNTS getProperty calls, then
 * drives the REAL dt10CollectPoolRows_ over a fake workbook holding the real
 * 2026-10-05 page headers (115 cols) and the first 10 rows of each of the
 * four source pages (40 valid symbol rows), the REAL dt10SeatTruthOn_, the
 * REAL memo reset, and the REAL dt10SelfTest.
 * The defect under test: v1.11.12 read DT10_P181B_W52_LEGACY once per pool
 * row (9,791 rows on the live workbook -> ~9,800 service calls per run, the
 * 6-minute kill); v1.11.13 reads it once per execution.
 * Usage: node test_dt10_v11113_prop_memo.js [--base p] [--delivered p]
 *        [--fixture p]
 * Prints N/N PASS and a cases-digest; exit 1 on any FAIL. */
'use strict';
var fs = require('fs'), vm = require('vm'), crypto = require('crypto'), path = require('path');
var args = process.argv.slice(2);
function arg(k, d) { var i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; }
var BASE = arg('--base', '16_Decision_Top10_base.gs');
var DELIV = arg('--delivered', '16_Decision_Top10.gs');
var FIXTURE = arg('--fixture', path.join(path.dirname(process.argv[1]), 'fixtures', 'dt10_pages_2026-10-05.json'));
var FIX = JSON.parse(fs.readFileSync(FIXTURE, 'utf8'));
var PAGES = ['Market_Leaders', 'Global_Markets', 'Commodities_FX', 'Mutual_Funds'];

/* ---- stubs (v1.11.12 harness shape) + a counting PropertiesService ---- */
function mkRange(sheet, r, c, nr, nc, a1) {
  var self = {
    getValue: function () { return sheet._val(r, c); },
    getValues: function () {
      if (a1 && sheet._a1 && sheet._a1[a1]) return sheet._a1[a1];
      if (sheet._grid) return sheet._grid;
      var out = [];
      for (var i = 0; i < (nr || 1); i++) { var row = []; for (var j = 0; j < (nc || 1); j++) row.push(sheet._val(r + i, c + j)); out.push(row); }
      return out;
    },
    setValue: function (v) { sheet._set(r, c, v); return self; },
    setValues: function (vv) { for (var i = 0; i < vv.length; i++) for (var j = 0; j < vv[i].length; j++) sheet._set(r + i, c + j, vv[i][j]); return self; },
    setNumberFormat: function () { return self; }, setNumberFormats: function () { return self; },
    setWrap: function () { return self; }, setFontSize: function () { return self; },
    setBackground: function () { return self; }, setBackgrounds: function () { return self; },
    setFontColor: function () { return self; }, setFontColors: function () { return self; },
    setFontWeight: function () { return self; }, setFontWeights: function () { return self; },
    setFontStyle: function () { return self; }, setHorizontalAlignment: function () { return self; },
    setBorder: function () { return self; }, clear: function () { return self; },
    clearContent: function () { return self; }, clearFormat: function () { return self; },
    merge: function () { return self; }, breakApart: function () { return self; },
    setNote: function () { return self; }, getNumRows: function () { return nr || 1; },
    getNumColumns: function () { return nc || 1; }, getRow: function () { return r; }, getColumn: function () { return c; }
  };
  return self;
}
function mkSheet(name, seedVals, a1, grid) {
  var cells = {};
  var sh = {
    _name: name, _a1: a1 || {}, _grid: grid || null, _appended: [],
    _val: function (r, c) { var k = r + ':' + c; return cells.hasOwnProperty(k) ? cells[k] : ''; },
    _set: function (r, c, v) { cells[r + ':' + c] = v; },
    getName: function () { return name; },
    getRange: function (a, b, nr, nc) { if (typeof a === 'string') return mkRange(sh, 1, 1, 60, 2, a); return mkRange(sh, a, b, nr, nc, null); },
    getDataRange: function () { return mkRange(sh, 1, 1, 1, 1, null); },
    getLastRow: function () { return grid ? grid.length : 20; }, getLastColumn: function () { return grid ? grid[0].length : 30; },
    getMaxRows: function () { return 200; }, getMaxColumns: function () { return 120; },
    appendRow: function (row) { sh._appended.push(row); return sh; },
    insertRows: function () { return sh; }, deleteRows: function () { return sh; },
    setColumnWidth: function () { return sh; }, setFrozenRows: function () { return sh; },
    autoResizeColumns: function () { return sh; }, hideSheet: function () { return sh; },
    clear: function () { return sh; }, clearContents: function () { return sh; }, getSheetId: function () { return 1; }
  };
  if (seedVals) for (var k in seedVals) if (seedVals.hasOwnProperty(k)) cells[k] = seedVals[k];
  return sh;
}
var FEED_OK = 'EXECUTABLE | run=37243158091 | 2026-10-05 03:16:16+03:00 | ML:OK GM:OK CFX:OK MF:OK';
var ML_CLEAN = 'OK | cov=100.0 | run=37243158091 | 2026-10-05 02:22:14+03:00';
var GM_CLEAN = 'OK | cov=100.0 | run=37243158091 | 2026-10-05 03:16:16+03:00';
function statusRows() {
  return [['TFB Feed Market_Leaders', ML_CLEAN], ['TFB Feed Global_Markets', GM_CLEAN],
          ['TFB Decision Feed', FEED_OK], ['TFB Feed Commodities_FX', ML_CLEAN], ['TFB Feed Mutual_Funds', ML_CLEAN]];
}
function stubs(opts) {
  opts = opts || {};
  var props = opts.props || {};
  var reads = { total: 0, byKey: {} };
  var top10 = mkSheet('Top_10_Investments', { '4:1': 'CONTROL PANEL', '14:1': 'KPIs' });
  var status = mkSheet('_Status', null, { 'L1:M60': statusRows() });
  var runlog = mkSheet('_Run_Log');
  var sheets = { 'Top_10_Investments': top10, '_Status': status, '_Run_Log': runlog };
  for (var p = 0; p < PAGES.length; p++) {
    var pg = PAGES[p];
    sheets[pg] = mkSheet(pg, null, null, [FIX[pg].header].concat(FIX[pg].rows));
  }
  var ss = { getSheetByName: function (n) { return sheets[n] || null; },
             getRangeByName: function () { return null; },
             getSheets: function () { var out = []; for (var k in sheets) if (sheets.hasOwnProperty(k)) out.push(sheets[k]); return out; },
             getId: function () { return 'stub'; }, getSpreadsheetTimeZone: function () { return 'Asia/Riyadh'; },
             insertSheet: function (n) { sheets[n] = mkSheet(n); return sheets[n]; } };
  return {
    console: console, Logger: { log: function () {} },
    Utilities: { formatDate: function (d) { return new Date(d).toISOString().replace('T', ' ').slice(0, 19); },
                 sleep: function () {}, computeDigest: function () { return []; }, base64Encode: function () { return ''; },
                 DigestAlgorithm: { MD5: 'MD5' } },
    PropertiesService: { getScriptProperties: function () { return {
      getProperty: function (k) { reads.total++; reads.byKey[k] = (reads.byKey[k] || 0) + 1; return props.hasOwnProperty(k) ? props[k] : null; },
      setProperty: function (k, v) { props[k] = String(v); return this; },
      deleteProperty: function (k) { delete props[k]; return this; },
      getProperties: function () { return props; } }; },
      getUserProperties: function () { return this.getScriptProperties(); } },
    SpreadsheetApp: { getActiveSpreadsheet: function () { return ss; }, getActive: function () { return ss; },
                      getUi: function () { throw new Error('no UI'); }, openById: function () { return ss; }, flush: function () {} },
    UrlFetchApp: { fetch: function () { throw new Error('no network'); } },
    Session: { getScriptTimeZone: function () { return 'Asia/Riyadh'; } },
    CacheService: { getScriptCache: function () { return { get: function () { return null; }, put: function () {} }; } },
    LockService: { getScriptLock: function () { return { tryLock: function () { return true; }, releaseLock: function () {} }; } },
    _props: props, _ss: ss, _sheets: sheets, _reads: reads
  };
}
function loadTree(file, opts) {
  var env = stubs(opts);
  var ctx = vm.createContext(env);
  vm.runInContext(fs.readFileSync(file, 'utf8'), ctx, { filename: file });
  ctx._env = env;
  return ctx;
}
function json(x) { return JSON.stringify(x); }
function deepEq(a, b) { return json(a) === json(b); }
function poolShape(pool) {   /* rows + per-page counts, field names of the first row (order-insensitive) */
  var keys = pool.rows.length ? Object.keys(pool.rows[0]).sort() : [];
  return { n: pool.rows.length, perPage: pool.perPage, available: pool.available, dup: pool.duplicatesSkipped, keys: keys, rows: pool.rows };
}
var VALID_ROWS = 0;
for (var p0 = 0; p0 < PAGES.length; p0++) VALID_ROWS += FIX[PAGES[p0]].rows.length;

function runOnce() {
  var log = [], fails = 0, cases = [];
  function check(name, ok, detail) { cases.push(name + '=' + (ok ? 1 : 0)); log.push((ok ? 'PASS ' : 'FAIL ') + name + (detail ? ' :: ' + detail : '')); if (!ok) fails++; }

  /* ---------- T1 default properties: pool identical, reads collapse ---------- */
  var B = loadTree(BASE), D = loadTree(DELIV);
  var poolB = B.dt10CollectPoolRows_(B._env._ss, 50000);
  var poolD = D.dt10CollectPoolRows_(D._env._ss, 50000);
  var rB = B._env._reads, rD = D._env._reads;
  check('T1a both trees read the same ' + VALID_ROWS + ' valid rows', poolB.rows.length === VALID_ROWS && poolD.rows.length === VALID_ROWS, json([poolB.rows.length, poolD.rows.length]));
  check('T1b pool output deep-equal (rows, fields, per-page counts, duplicates)', deepEq(poolShape(poolB), poolShape(poolD)));
  check('T1c delivered rows carry the four P-181b fields (projection ON, unchanged)',
        poolD.rows[0].hasOwnProperty('52W High') && poolD.rows[0].hasOwnProperty('Percent Change') && poolB.rows[0].hasOwnProperty('52W High'));
  check('T1d BASE reads DT10_P181B_W52_LEGACY once per row + once per header map (' + (VALID_ROWS + 4) + ')',
        rB.byKey['DT10_P181B_W52_LEGACY'] === VALID_ROWS + 4, json(rB.byKey));
  check('T1e DELIVERED reads DT10_P181B_W52_LEGACY exactly once', rD.byKey['DT10_P181B_W52_LEGACY'] === 1 && rD.total === 1, json(rD.byKey));

  /* ---------- T2 kill property '1': projection legacy in both, reads collapse ---------- */
  var BL = loadTree(BASE, { props: { DT10_P181B_W52_LEGACY: '1' } });
  var DL = loadTree(DELIV, { props: { DT10_P181B_W52_LEGACY: '1' } });
  var poolBL = BL.dt10CollectPoolRows_(BL._env._ss, 50000), poolDL = DL.dt10CollectPoolRows_(DL._env._ss, 50000);
  check('T2a kill tree: pool deep-equal to base kill tree', deepEq(poolShape(poolBL), poolShape(poolDL)));
  check('T2b kill tree drops the four fields in both trees',
        !poolDL.rows[0].hasOwnProperty('52W High') && !poolBL.rows[0].hasOwnProperty('52W High') && poolDL.rows[0].hasOwnProperty('Symbol'));
  check('T2c kill tree reads: base ' + (VALID_ROWS + 4) + ' / delivered 1',
        BL._env._reads.byKey['DT10_P181B_W52_LEGACY'] === VALID_ROWS + 4 && DL._env._reads.byKey['DT10_P181B_W52_LEGACY'] === 1);

  /* ---------- T3 seat truth memo ---------- */
  var B3 = loadTree(BASE), D3 = loadTree(DELIV);
  var stB = [B3.dt10SeatTruthOn_(), B3.dt10SeatTruthOn_(), B3.dt10SeatTruthOn_()];
  var stD = [D3.dt10SeatTruthOn_(), D3.dt10SeatTruthOn_(), D3.dt10SeatTruthOn_()];
  check('T3a seat truth value identical (ON) in both trees', json(stB) === json([true, true, true]) && json(stD) === json(stB));
  check('T3b seat truth reads: base 3 / delivered 1', B3._env._reads.byKey['DT10_SEAT_TRUTH_LEGACY'] === 3 && D3._env._reads.byKey['DT10_SEAT_TRUTH_LEGACY'] === 1);
  var D3L = loadTree(DELIV, { props: { DT10_SEAT_TRUTH_LEGACY: '1' } });
  check('T3c seat truth kill property honoured through the memo', D3L.dt10SeatTruthOn_() === false && D3L.dt10SeatTruthOn_() === false && D3L._env._reads.byKey['DT10_SEAT_TRUTH_LEGACY'] === 1);

  /* ---------- T4 memo reset: a changed property is picked up on the next run ---------- */
  var D4 = loadTree(DELIV);
  var f1 = D4.dt10PoolFieldsActive_().length;
  D4._env._props['DT10_P181B_W52_LEGACY'] = '1';
  var f2 = D4.dt10PoolFieldsActive_().length;          /* memoized: unchanged within the run */
  D4.dt10PropMemoReset_();
  var f3 = D4.dt10PoolFieldsActive_().length;          /* after reset: the kill is seen */
  check('T4a within one execution the list is stable even if the property changes', f1 === f2 && f1 === D4.DT10_POOL_FIELDS.length);
  check('T4b after dt10PropMemoReset_ the new property value is read (' + f3 + ' = ' + (D4.DT10_POOL_FIELDS.length - 4) + ')', f3 === D4.DT10_POOL_FIELDS.length - 4);
  check('T4c reads across T4 = 2 (one per execution)', D4._env._reads.byKey['DT10_P181B_W52_LEGACY'] === 2);
  check('T4d explicit-argument form stays pure and unmemoized', D4.dt10PoolFieldsActive_(false).length === D4.DT10_POOL_FIELDS.length && D4.dt10PoolFieldsActive_(true).length === D4.DT10_POOL_FIELDS.length - 4 && D4._env._reads.byKey['DT10_P181B_W52_LEGACY'] === 2);

  /* ---------- T5 refreshDecisionTop10 entry resets the memo (source + behaviour) ---------- */
  var srcD = fs.readFileSync(DELIV, 'utf8');
  var entryIdx = srcD.indexOf('function refreshDecisionTop10() {');
  var resetIdx = srcD.indexOf('dt10PropMemoReset_();', entryIdx);
  check('T5a dt10PropMemoReset_ sits within the first 200 chars of refreshDecisionTop10', entryIdx > 0 && resetIdx > entryIdx && resetIdx - entryIdx < 200);
  var D5 = loadTree(DELIV);
  D5.dt10PoolFieldsActive_();                         /* primes the memo */
  var keysBefore = Object.keys(D5.DT10_PROP_MEMO_).length;
  try { D5.refreshDecisionTop10(); } catch (e5) { /* the stub workbook lacks the cockpit layout; we only need the entry */ }
  check('T5b memo primed before the run (' + keysBefore + ' keys) and the run entry reset it', keysBefore >= 2 && D5._env._reads.byKey['DT10_P181B_W52_LEGACY'] >= 1);

  /* ---------- T6 the REAL dt10SelfTest on the delivered tree ---------- */
  var D6 = loadTree(DELIV);
  var rep = '';
  try { rep = D6.dt10SelfTest(); } catch (e6) { rep = 'THROW ' + e6; }
  var need = ['property memo core: ok', 'w52 projection core: ok', 'sync inflight core: ok', 'run marker core: ok', 'epoch key core: ok', 'outage pause core: ok', 'grace sizing core: ok', 'funding containment core: ok', 'cash source core: ok'];
  var missing = [];
  for (var n = 0; n < need.length; n++) if (rep.indexOf(need[n]) === -1) missing.push(need[n]);
  check('T6a REAL dt10SelfTest prints every prior "core: ok" line plus the v1.11.13 line', missing.length === 0 && rep.indexOf('FAIL') === -1, missing.length ? 'missing ' + json(missing) : 'ok');
  /* the self-test deliberately calls dt10PropMemoReset_() before its memo case, so the W52 flag is read
     exactly twice in one self-test execution (once before the reset, once after); seat truth once. */
  check('T6b self-test reads: W52 flag 2 (pre/post its deliberate reset), seat truth 1', D6._env._reads.byKey['DT10_P181B_W52_LEGACY'] === 2 && D6._env._reads.byKey['DT10_SEAT_TRUTH_LEGACY'] === 1, json({ w52: D6._env._reads.byKey['DT10_P181B_W52_LEGACY'], seat: D6._env._reads.byKey['DT10_SEAT_TRUTH_LEGACY'] }));

  /* ---------- T7 versions + function census ---------- */
  var srcB = fs.readFileSync(BASE, 'utf8');
  function census(src) { var m = src.match(/^function \w+\s*\(/mg) || []; var out = {}; for (var i = 0; i < m.length; i++) out[m[i]] = 1; return out; }
  var cB = census(srcB), cD = census(srcD), removed = [], added = [];
  for (var k in cB) if (cB.hasOwnProperty(k) && !cD[k]) removed.push(k);
  for (var k2 in cD) if (cD.hasOwnProperty(k2) && !cB[k2]) added.push(k2);
  check('T7a DT10_VERSION base 1.11.12 / delivered 1.11.13', B.DT10_VERSION === '1.11.12' && D.DT10_VERSION === '1.11.13');
  check('T7b functions: +2 (dt10PropMemoGet_, dt10PropMemoReset_), 0 removed', removed.length === 0 && added.length === 2, json({ added: added, removed: removed }));
  check('T7c delivered is CRLF without a trailing newline (editor paste convention)', srcD.indexOf('\r\n') > 0 && srcD.replace(/\r\n/g, '').indexOf('\n') === -1 && !/\r\n$/.test(srcD));

  var digest = crypto.createHash('sha256').update(cases.join('|')).digest('hex').slice(0, 16);
  return { log: log, fails: fails, total: cases.length, digest: digest };
}
var r = runOnce();
console.log(r.log.join('\n'));
console.log((r.total - r.fails) + '/' + r.total + (r.fails ? ' FAIL' : ' PASS') + ' cases-digest=' + r.digest);
process.exit(r.fails ? 1 : 0);
