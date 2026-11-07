/* REENTRY-COOLDOWN (16_Decision_Top10.gs v1.23.0) -- a name sold inside the
 * cool-down (the lot ledger's sell date, last N calendar days) cannot be
 * seated on the Top-10 board (Script Property TFB_T10_REENTRY_COOLDOWN_DAYS,
 * default absent/0 = the v1.22.0 board byte for byte; 1..60 arms).
 *
 * Dual-tree node harness over the REAL file: loads the .gs source in a vm
 * context with minimal GAS shims, replays the confirm-basis board through the
 * REAL dt10StabCore_ (8th argument) and the REAL dt10ApplyStability_ (service
 * read -> knob -> ledger read -> argument), and proves:
 *   R0 base oracle: the v1.22.0 file rebuilt by reversing the enumerated
 *      v1.23.0 edits (tests/js/dt10_edits_v1230.json) carries the pinned
 *      SHA-256; versions in lockstep; both files compile.
 *   R1 GOLDEN NEGATIVE on v1.22.0: the ITRN pattern -- a raw ticket for a name
 *      the ledger sold 2 days ago is seated FAST-TRACK day 1; the base ignores
 *      an unknown cooldown_days knob and an 8th argument.
 *   R2 KILL absent/0/junk/fault: every fixture, every day deep-equals the base
 *      through the pure core (map absent / null / knob 0) AND through
 *      dt10ApplyStability_ (saved blob, selected, audit, note, knobs; the
 *      ledger tab is never read while unarmed).
 *   R3 armed: the sold raw ticket is not seated, not counted, not in the
 *      state, named in the note and audit; a sold member hard-exits and is
 *      named; the held name is non-qualified for the basis / near list; the
 *      other seats, clocks, labels, co path and ticket sizing are v1.22.0.
 *   R4 parser: header anywhere in the first rows ('Sell Date' / 'Sold Date' /
 *      'Exit Date'), titles above it ignored, Date cells (Riyadh instants)
 *      and ISO text, 0..N days kept, future / older / blank skipped, most
 *      recent sale per symbol wins, repeated header skipped, grid without
 *      both headers or N = 0 -> {}.
 *   R5 service read: '1'..'60' -> n, '99' -> 60, '', '0', '-1', '2.5',
 *      'ten', absent and a fault -> 0; the panel snapshot never carries the
 *      knob; the knobs object carries it only while armed; the live tab is
 *      read once per armed run, bounded.
 *   R6 embedded dt10SelfTest: 'reentry cooldown core: ok', the property line
 *      follows the property, every base line kept, zero FAIL.
 *   R7 ES5/ASCII lint of the diff; inventory: 3 functions and 3 vars added,
 *      0 removed; 14 hunks whose reversal is the reviewed v1.22.0 file; WHY
 *      block newest-first; LF only.
 *   R8 mutants: three one-line mutations of the delivered file are each
 *      caught.
 *
 * The whole battery runs three times and the three digests must match; the
 * last line is 'PASS k/k'.
 *
 * Usage: node tests/test_dt10_v1230_reentry_cooldown.js [new.gs] [base.gs]
 */
'use strict';
var fs = require('fs'), vm = require('vm'), path = require('path'), crypto = require('crypto');
var ROOT = path.join(__dirname, '..');
var NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '16_Decision_Top10.gs');
var BASE_PATH = process.argv[3] || '';
var history = require(path.join(ROOT, 'tests', 'js', 'dt10_cash_utc_history.js'));
var EDITS = require(path.join(ROOT, 'tests', 'js', 'dt10_edits_v1230.json'));
var PROP = 'TFB_T10_REENTRY_COOLDOWN_DAYS';

// 2026-11-08: the delivered file is v1.23.1 [CONFIRM-WINDOW-FIX]; this harness
// proves the v1.23.0 diff, so it reads the v1.23.0 file rebuilt from the
// delivered one (tests/js/dt10_edits_v1231.json reversed, SHA-256 pinned).
var newSource = history.v1230Source(fs.readFileSync(NEW_PATH, 'utf8'));
function sha256(s) { return crypto.createHash('sha256').update(s).digest('hex'); }
var baseSource;
try {
  baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : history.v1220Source(newSource);
} catch (eBase) {
  console.log('FAIL R0 base oracle: ' + String(eBase && eBase.message || eBase).split('\n')[0]);
  console.log('FAIL 0/1');
  process.exit(1);
}

var passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n  ')); }
}
function assert(c, m) { if (!c) throw new Error(m || 'assertion failed'); }
function eq(a, b, m) { if (a !== b) throw new Error((m || 'not equal') + ': ' + JSON.stringify(a) + ' !== ' + JSON.stringify(b)); }
function deq(a, b, m) {
  var ja = JSON.stringify(a), jb = JSON.stringify(b);
  if (ja !== jb) {
    var i = 0; while (i < ja.length && ja[i] === jb[i]) i++;
    throw new Error((m || 'deep-equal') + ' differs at ' + i + ': ...' + ja.slice(Math.max(0, i - 80), i + 120) + ' | ...' + jb.slice(Math.max(0, i - 80), i + 120));
  }
}
function plain(v) { return JSON.parse(JSON.stringify(v)); }

/* ------------------------------------------------------------ GAS shims (+ a ledger tab) */
function ctx(props, options) {
  options = options || {};
  var store = {}; for (var k in props) if (props.hasOwnProperty(k)) store[k] = props[k];
  var logs = [], reads = {}, sheetReads = {};
  var statusRows = [];
  for (var r = 0; r < 60; r++) statusRows.push(['', '']);
  statusRows[0] = ['TFB Feed Global_Markets', 'OK | cov=99.0 | run=40145487350 | 2026-10-25 04:09:22+03:00'];
  var statusSheet = { getRange: function () { return { getValues: function () { return plain(statusRows); } }; } };
  var ledgerRows = options.ledger || null;
  var ledgerSheet = ledgerRows ? {
    getLastRow: function () { return ledgerRows.length; },
    getLastColumn: function () { return ledgerRows[0].length; },
    getRange: function (r1, c1, nr, nc) {
      sheetReads['_Portfolio_CostBasis'] = (sheetReads['_Portfolio_CostBasis'] || 0) + 1;
      var out = [];
      for (var i = r1 - 1; i < r1 - 1 + nr; i++) {
        var row = ledgerRows[i] || [];
        var cells = [];
        for (var j = c1 - 1; j < c1 - 1 + nc; j++) cells.push(row[j] === undefined ? '' : row[j]);
        out.push(cells);
      }
      return { getValues: function () { return out; } };
    }
  } : null;
  var ssStub = { getSheetByName: function (n) {
                   if (n === '_Status') return statusSheet;
                   if (n === '_Portfolio_CostBasis') { sheetReads.lookup = (sheetReads.lookup || 0) + 1; return ledgerSheet; }
                   return null; },
                 getRangeByName: function () { return null; } };
  var context = {
    PropertiesService: { getScriptProperties: function () {
      if (options.propsThrow) throw new Error('synthetic property outage');
      return { getProperty: function (key) { reads[key] = (reads[key] || 0) + 1; return store.hasOwnProperty(key) ? store[key] : null; },
               setProperty: function (key, v) { store[key] = String(v); },
               deleteProperty: function (key) { delete store[key]; } }; } },
    Logger: { log: function (m) { logs.push(String(m)); } },
    SpreadsheetApp: { getActiveSpreadsheet: function () { return ssStub; }, getActive: function () { return ssStub; },
                      getUi: function () { throw new Error('no ui'); } },
    Utilities: { formatDate: function (d, tz, pattern) { return pattern === 'yyyy-MM-dd' ? '2026-10-25' : '2026-10-25 00:00:00'; } },
    Session: { getScriptTimeZone: function () { return 'Asia/Riyadh'; } },
    UrlFetchApp: { fetch: function () { throw new Error('no net'); } }
  };
  vm.createContext(context);
  context.__store = store; context.__logs = logs; context.__reads = reads; context.__sheetReads = sheetReads;
  return context;
}
function load(source, props, options) {
  var c = ctx(props || {}, options);
  vm.runInContext(source, c, { filename: '16_Decision_Top10.gs' });
  return c;
}

/* ------------------------------------------------------------ fixtures */
var DAYS = ['2026-10-21', '2026-10-22', '2026-10-23', '2026-10-24'];
function knobs(cd, buf) {
  var k = { enabled: true, confirm_days: 3, exit_days: 3, rank_buffer: (buf === undefined ? 15 : buf),
            smooth_days: 5, hard_strict: true, confirm_basis: 'qualified' };
  if (cd !== undefined) k.cooldown_days = cd;
  return k;
}
function cand(sym, sc, verdict, extra) {
  var c = { symbol: sym, name: sym, market: 'NYSE/NASDAQ', sector: 'Financial Services', currency: 'USD',
            opportunity_score: sc, verdict: (verdict === undefined ? 'INVEST' : verdict), price: 10, price_sar: 37.5,
            roi_pct: 9, engine_roi_pct: 18, ann_roi_pct: 12, reliability: 75, dq: 100, confidence_band: 'High' };
  if (extra) for (var k in extra) if (extra.hasOwnProperty(k)) c[k] = extra[k];
  return c;
}
function ticket(sym, sc) {
  return { symbol: sym, name: sym, market: 'NYSE/NASDAQ', sector: 'Financial Services', currency: 'USD', fx_to_sar: 3.75,
           price: 10, price_sar: 37.5, entry_zone: '36-39 SAR', suggested_sar: 5000, suggested_shares: 133,
           stop_sar: 33, tp1_sar: 44, tp2_sar: 48, roi_pct: 9, engine_roi_pct: 18, ann_roi_pct: 12,
           exp_gain_12m_sar: 900, reliability: 75, dq: 100, confidence_band: 'High', opportunity_score: sc,
           advisor_note: 'INVEST - 5,000 SAR (133 sh)', detail: { funds_from: 'Cash 5,000 SAR', review_date: '2026-11-24' } };
}
function grid(chalScore, chalVerdict) {
  var g = [cand('NEWC.US', 97), cand('A.US', 95)];
  for (var i = 1; i <= 12; i++) g.push(cand('F' + i + '.US', 95.5 - i));
  g.push(cand('B.US', 84));
  g.push(cand('CHAL.US', chalScore === undefined ? 86 : chalScore, chalVerdict));
  return g;
}
function seed() {
  return { v: 1, date: '2026-10-20', symbols: {
    'A.US': { ci: 3, co: 0, member: true, since: '2026-10-15', ls: '2026-10-20', hist: [95], ft: false },
    'B.US': { ci: 3, co: 1, member: true, since: '2026-10-15', ls: '2026-10-20', hist: [84], ft: false } } };
}
function raws() {
  return [[ticket('A.US', 95), ticket('CHAL.US', 86)],
          [ticket('A.US', 95), ticket('CHAL.US', 86)],
          [ticket('A.US', 95), ticket('NEWC.US', 97)],
          [ticket('A.US', 95), ticket('CHAL.US', 86)]];
}
/* Ledger values as the sheet returns them: titles, header row 4, lots.
 * The live reader measures age against the REAL calendar day (the ledger holds
 * real sell dates), so the end-to-end fixtures date their sales relative to now. */
function isoDaysAgo(n) { return new Date(Date.now() - n * 86400000).toISOString().slice(0, 10); }
function ledger(rows) {
  var base = [['Portfolio ledger v2', '', '', ''], ['', '', '', ''], ['', '', '', ''],
              ['Symbol', 'Status', 'Buy Date', 'Sell Date']];
  return base.concat(rows || []);
}
var LEDGER_CHAL2 = ledger([['CHAL.US', 'Sold', isoDaysAgo(40), isoDaysAgo(2)]]);   // sold 2 (or 3, by the hour) days ago
function replay(g, k, seedObj, rawsArr, gridsArr, maps) {
  var st = g.dt10StabParseState_(plain(seedObj));
  var outs = [];
  for (var d = 0; d < DAYS.length; d++) {
    var m = maps ? maps[d] : undefined;
    var o = g.dt10StabCore_(plain(rawsArr[d]), plain(gridsArr[d]), st, plain(k), 3, DAYS[d], undefined, m);
    outs.push(plain({ tickets: o.tickets, state: o.state, audit: o.audit, note: o.note }));
    st = g.dt10StabParseState_(plain(o.state));
  }
  return outs;
}
var G = grid(), SOFT = grid(86, 'WATCH');
function grids(d3) { return [plain(G), plain(G), plain(d3 || G), plain(G)]; }
function bySym(out, s) { for (var i = 0; i < out.tickets.length; i++) if (out.tickets[i].symbol === s) return out.tickets[i]; return null; }
function sym(out, s) { return out.state.symbols[s]; }
function scenarios(g, cd, maps) {
  return {
    plain: replay(g, knobs(cd), seed(), raws(), grids(), maps),
    soft: replay(g, knobs(cd), seed(), raws(), grids(SOFT), maps),
    buf0: replay(g, knobs(cd, 0), seed(), raws(), grids(), maps)
  };
}
function applyE2E(source, props, options) {
  var g = load(source, props, options);
  var seedObj = seed(); seedObj.date = '2026-10-24';
  seedObj.symbols['CHAL.US'] = { ci: 2, co: 0, member: false, since: '', ls: '2026-10-24', hist: [86, 86], ft: false };
  g.__store.DT10_STAB_STATE_V1 = JSON.stringify(seedObj);
  var payload = { selected: [ticket('A.US', 95), ticket('CHAL.US', 86)], candidates_rows: grid(86), meta: {} };
  var panel = { 'T10: Max Selected': 2, 'T10: Stability Enabled': 'Yes', 'T10: Stability Confirm Days': 3,
                'T10: Stability Exit Days': 3, 'T10: Stability Rank Buffer': 15, 'T10: Stability Smooth Days': 5 };
  var out = g.dt10ApplyStability_(payload, panel, null);
  return { g: g, note: out.note, selected: plain(payload.selected), audit: plain(payload.meta.stability.audit),
           knobs: plain(payload.meta.stability.knobs),
           saved: JSON.parse(g.__store.DT10_STAB_STATE_V1), reads: plain(g.__reads), sheetReads: plain(g.__sheetReads) };
}
function names(src, re) { var out = {}, m; while ((m = re.exec(src)) !== null) out[m[1]] = true; return out; }

/* ------------------------------------------------------------ battery */
function battery() {
  var N = load(newSource, {});
  var B = load(baseSource, {});
  var bb = null, nn = null;
  var chalMap = { 'CHAL.US': { days: 2, date: '2026-10-19' } };
  var bMap = { 'B.US': { days: 1, date: '2026-10-20' } };

  test('R0 base oracle is the reviewed v1.22.0 file (SHA-256 pinned); versions in lockstep; both files compile', function () {
    eq(sha256(baseSource), history.V1220_SHA256, 'base SHA-256');
    eq(B.DT10_VERSION, '1.22.0', 'base version');
    eq(N.DT10_VERSION, '1.23.0', 'delivered version');
    assert(/^ \* Version: 1\.23\.0 /.test(newSource.split('\n')[3]), 'header line 4 lockstep');
    assert(newSource.indexOf("var DT10_VERSION = '1.23.0';") > 0, 'DT10_VERSION literal');
    eq(N.DT10_T10_COOLDOWN_PROP, PROP); eq(N.DT10_T10_COOLDOWN_MAX, 60); eq(N.DT10_LEDGER_SHEET, '_Portfolio_CostBasis');
    assert(B.DT10_T10_COOLDOWN_PROP === undefined && typeof B.dt10CooldownMap_ !== 'function', 'base lacks the change');
    new vm.Script(newSource, { filename: 'new.gs' }); new vm.Script(baseSource, { filename: 'base.gs' });
  });

  test('R1 GOLDEN NEGATIVE (v1.22.0): a name sold 2 days ago is seated FAST-TRACK day 1 from the raw ticket; the base ignores the knob and the 8th argument', function () {
    bb = scenarios(B, undefined);
    var t = bySym(bb.plain[0], 'CHAL.US');
    assert(t !== null && /^FAST-TRACK \(day 1/.test(t._stab_status), 'base seats CHAL: ' + (t && t._stab_status));
    deq(scenarios(B, 10, [chalMap, chalMap, chalMap, chalMap]), bb, 'base ignores knob + map');
    eq(bb.plain[0].note.indexOf('cooldown'), -1, 'no token'); eq(bb.plain[0].audit.cooldown_held, undefined, 'no audit key');
  });

  test('R2 KILL absent/0/junk/fault: every fixture, every day deep-equals the base; dt10ApplyStability_ end to end; the ledger is never read unarmed', function () {
    nn = scenarios(N, undefined);
    deq(nn, bb, 'knob absent, map absent');
    deq(scenarios(N, 0, [chalMap, chalMap, chalMap, chalMap]), bb, 'knob 0 with a map');
    deq(scenarios(N, 10), bb, 'knob 10 without a map');
    deq(scenarios(N, 10, [null, null, null, null]), bb, 'knob 10 with null maps');
    deq(scenarios(N, 'abc', [chalMap, chalMap, chalMap, chalMap]), bb, 'knob junk');
    var e2eN = applyE2E(newSource, {}, { ledger: LEDGER_CHAL2 }), e2eB = applyE2E(baseSource, {}, { ledger: LEDGER_CHAL2 });
    deq([e2eN.note, e2eN.selected, e2eN.audit, e2eN.saved, e2eN.knobs], [e2eB.note, e2eB.selected, e2eB.audit, e2eB.saved, e2eB.knobs], 'e2e unarmed vs base');
    eq(e2eN.reads[PROP], 1, 'the new file reads the property once'); eq(e2eB.reads[PROP], undefined, 'base never reads it');
    eq(e2eN.sheetReads.lookup, undefined, 'ledger tab not looked up while unarmed');
    var e2e0 = applyE2E(newSource, { TFB_T10_REENTRY_COOLDOWN_DAYS: '0' }, { ledger: LEDGER_CHAL2 });
    deq([e2e0.note, e2e0.selected, e2e0.audit, e2e0.saved, e2e0.knobs], [e2eB.note, e2eB.selected, e2eB.audit, e2eB.saved, e2eB.knobs], 'e2e property 0 vs base');
    var e2eF = applyE2E(newSource, { TFB_T10_REENTRY_COOLDOWN_DAYS: '10' }, { ledger: LEDGER_CHAL2, propsThrow: true });
    var e2eBF = applyE2E(baseSource, {}, { ledger: LEDGER_CHAL2, propsThrow: true });
    deq([e2eF.note, e2eF.selected, e2eF.audit, e2eF.knobs], [e2eBF.note, e2eBF.selected, e2eBF.audit, e2eBF.knobs], 'e2e property fault vs base fault');
  });

  test('R3 armed: the sold raw ticket is not seated / counted / stored and is named; a sold member hard-exits and is named; the rest is v1.22.0', function () {
    var on = replay(N, knobs(10), seed(), raws(), grids(), [chalMap, chalMap, chalMap, chalMap]);
    for (var d = 0; d < 4; d++) {
      eq(bySym(on[d], 'CHAL.US'), null, 'CHAL never seated (day ' + (d + 1) + ')');
      eq(sym(on[d], 'CHAL.US'), undefined, 'CHAL never enters the state (day ' + (d + 1) + ')');
    }
    deq(on[0].audit.cooldown_held, [{ symbol: 'CHAL.US', days: 2, date: '2026-10-19', member: false }], 'audit day 1');
    assert(on[0].note.indexOf(', cooldown=10d held 1 CHAL.US(sold 2d ago)') > 0, 'note: ' + on[0].note);
    deq(on[2].audit.cooldown_held, [], 'day 3: CHAL not in raw -> nothing held');
    assert(on[2].note.indexOf(', cooldown=10d') > 0 && on[2].note.indexOf('held') < 0, 'armed token without names');
    // the other seats, clocks and labels equal the base replay with CHAL stripped from raw
    var stripped = raws().map(function (r) { return r.filter(function (t) { return t.symbol !== 'CHAL.US'; }); });
    var ref = replay(B, knobs(), seed(), stripped, grids());
    for (var e = 0; e < 4; e++) {
      deq(on[e].tickets, ref[e].tickets, 'tickets equal the stripped-raw base (day ' + (e + 1) + ')');
      deq(on[e].state, ref[e].state, 'state equal the stripped-raw base (day ' + (e + 1) + ')');
      eq(on[e].note.replace(/, cooldown=10d.*$/, ''), ref[e].note, 'note equal apart from the token');
    }
    // the held name is non-qualified for the basis / near list (hardOut) -> no 'near' token for CHAL
    assert(on[0].note.indexOf('CHAL.US(') === on[0].note.indexOf('CHAL.US(sold'), 'no near token for a cooled name');
    // a sold member hard-exits and is named (B sold yesterday relative to DAYS[0])
    var mb = N.dt10StabCore_(plain(raws()[0]), plain(G), N.dt10StabParseState_(plain(seed())), knobs(10), 3, DAYS[0], undefined, bMap);
    deq(mb.audit.exited_hard, ['B.US'], 'B hard-exits');
    deq(mb.audit.cooldown_held, [{ symbol: 'B.US', days: 1, date: '2026-10-20', member: true }], 'B named as a member');
    eq(mb.state.symbols['B.US'].member, false, 'B out'); eq(mb.state.symbols['B.US'].ci, 0, 'B ci 0');
    assert(bySym(mb, 'CHAL.US') !== null, 'CHAL (not in this map) fast-tracks into the freed seat');
    // ticket sizing of the seated names is untouched
    eq(bySym(on[0], 'A.US').suggested_sar, 5000, 'A sizing');
  });

  test('R4 parser: header row found below titles, three header spellings, Date + ISO cells, 0..N kept, future/older/blank skipped, most recent wins, repeated header skipped, no headers -> {}', function () {
    var asOf = '2026-10-21';
    var rows = ledger([
      ['CHAL.US', 'Sold', '2026-09-01', new Date(Date.UTC(2026, 9, 17, 21, 0, 0))],   // 2026-10-18 Riyadh -> 3d
      ['CHAL.US', 'Sold', '2026-08-01', '2026-10-12'],                                 // 9d, older -> loses
      ['B.US', 'Sold', '2026-09-01', '2026-10-20'],                                    // 1d
      ['OLD.US', 'Sold', '2026-08-01', '2026-09-11'],                                  // 40d -> out
      ['FUT.US', 'Active', '2026-10-01', '2026-10-22'],                                // tomorrow -> out
      ['SAME.US', 'Sold', '2026-10-01', '2026-10-21'],                                 // today -> 0d kept
      ['', 'Sold', '2026-09-01', '2026-10-19'],
      ['Symbol', 'Status', 'Buy Date', 'Sell Date'],
      ['A.US', 'Active', '2026-09-01', ''],
      ['BAD.US', 'Sold', '2026-09-01', 'yesterday'],
      ['SER.US', 'Sold', '2026-09-01', 46000]]);
    var m = N.dt10CooldownMapFromValues_(rows, asOf, 10);
    deq(Object.keys(m).sort(), ['B.US', 'CHAL.US', 'SAME.US'], 'members');
    deq(m['CHAL.US'], { days: 3, date: '2026-10-18' }, 'most recent sale wins, Date cell read as a Riyadh instant');
    deq(m['B.US'], { days: 1, date: '2026-10-20' }, 'ISO text');
    deq(m['SAME.US'], { days: 0, date: '2026-10-21' }, 'today counts');
    deq(Object.keys(N.dt10CooldownMapFromValues_(rows, asOf, 2)), ['B.US', 'SAME.US'].sort(), 'N = 2');
    deq(N.dt10CooldownMapFromValues_(rows, asOf, 0), {}, 'N = 0 -> {}');
    deq(N.dt10CooldownMapFromValues_([['Symbol', 'Status', 'Buy Date']], asOf, 10), {}, 'no sell-date header');
    deq(N.dt10CooldownMapFromValues_([['Sell Date']], asOf, 10), {}, 'no symbol header');
    deq(N.dt10CooldownMapFromValues_([], asOf, 10), {}, 'empty grid');
    deq(N.dt10CooldownMapFromValues_(null, asOf, 10), {}, 'null grid');
    deq(N.dt10CooldownMapFromValues_([['symbol', 'SOLD DATE'], ['X.US', '2026-10-20']], asOf, 10), { 'X.US': { days: 1, date: '2026-10-20' } }, 'Sold Date, any case');
    deq(N.dt10CooldownMapFromValues_([['Symbol', 'Exit Date'], ['Y.US', '2026-10-15']], asOf, 10), { 'Y.US': { days: 6, date: '2026-10-15' } }, 'Exit Date');
    deq(N.dt10CooldownMapFromValues_(rows, 'not a date', 10), {}, 'unparseable as-of -> {}');
    // the live reader: bounded, fail-soft
    var g = load(newSource, {}, { ledger: rows });
    var live = g.dt10CooldownMap_(g.SpreadsheetApp.getActiveSpreadsheet(), 10, asOf);
    deq(Object.keys(live).sort(), ['B.US', 'CHAL.US', 'SAME.US'], 'live read');
    deq(load(newSource, {}).dt10CooldownMap_(load(newSource, {}).SpreadsheetApp.getActiveSpreadsheet(), 10, asOf), {}, 'absent tab -> {}');
    deq(g.dt10CooldownMap_(null, 10, asOf), {}, 'null spreadsheet -> {}');
    var big = []; for (var i = 0; i < 5002; i++) big.push(['Symbol', 'Sell Date']);
    deq(load(newSource, {}, { ledger: big }).dt10CooldownMap_(load(newSource, {}, { ledger: big }).SpreadsheetApp.getActiveSpreadsheet(), 10, asOf), {}, 'oversized grid -> {}');
  });

  test('R5 service read: 1..60 -> n, 99 -> 60, blank/0/-1/2.5/junk/absent/fault -> 0; panel snapshot never carries it; knobs object only while armed; one bounded tab read per armed run', function () {
    [['1', 1], [' 10 ', 10], ['60', 60], ['99', 60], ['07', 7], ['', 0], ['0', 0], ['-1', 0], ['2.5', 0], ['ten', 0]].forEach(function (pair) {
      var p = {}; p[PROP] = pair[0];
      eq(load(newSource, p).dt10StabCooldownDays_(), pair[1], 'property ' + JSON.stringify(pair[0]));
    });
    eq(load(newSource, {}).dt10StabCooldownDays_(), 0, 'absent');
    eq(load(newSource, {}, { propsThrow: true }).dt10StabCooldownDays_(), 0, 'service fault');
    var panel = { 'T10: Max Selected': 2, 'T10: Stability Enabled': 'Yes', 'T10: Stability Confirm Days': 3,
                  'T10: Stability Exit Days': 3, 'T10: Stability Rank Buffer': 15, 'T10: Stability Smooth Days': 5 };
    eq(load(newSource, { TFB_T10_REENTRY_COOLDOWN_DAYS: '10' }).dt10StabKnobs_(panel).cooldown_days, undefined, 'dt10StabKnobs_ unchanged');
    var e = applyE2E(newSource, { TFB_T10_REENTRY_COOLDOWN_DAYS: '10' }, { ledger: LEDGER_CHAL2 });
    eq(e.knobs.cooldown_days, 10, 'armed knob');
    eq(e.sheetReads.lookup, 1, 'ledger looked up once'); eq(e.sheetReads['_Portfolio_CostBasis'], 1, 'ledger read once');
    assert(/, cooldown=10d held 1 CHAL\.US\(sold [23]d ago\)/.test(e.note), 'armed note: ' + e.note);
    deq(e.audit.cooldown_held.map(function (h) { return h.symbol; }), ['CHAL.US'], 'armed audit');
    eq(e.selected.filter(function (t) { return t.symbol === 'CHAL.US'; }).length, 0, 'CHAL not published');
    eq(e.saved.symbols['CHAL.US'].ci, 0, 'the seeded pending clock (ci 2) resets while cooled -> re-earns its days after the cool-down');
    eq(e.saved.symbols['CHAL.US'].member, false, 'CHAL stays a non-member');
    var eB1 = applyE2E(newSource, { TFB_T10_REENTRY_COOLDOWN_DAYS: '1' }, { ledger: LEDGER_CHAL2 });
    eq(eB1.audit.cooldown_held.length, 0, 'sold 2-3 days ago is outside a 1-day cool-down');
    assert(eB1.selected.some(function (t) { return t.symbol === 'CHAL.US'; }), 'CHAL seats under a 1-day cool-down');
  });

  test('R6 embedded dt10SelfTest: reentry cooldown battery ok, property line follows the property, every base line kept, zero FAIL', function () {
    var rep = String(N.dt10SelfTest()), repB = String(B.dt10SelfTest());
    assert(rep.indexOf('reentry cooldown core: ok') > 0, 'battery line: ' + rep.split('\n').filter(function (l) { return /reentry cooldown/.test(l); }).join(' / '));
    assert(rep.indexOf('reentry cooldown (TFB_T10_REENTRY_COOLDOWN_DAYS): 0 (') > 0, 'property line default');
    assert(rep.indexOf('[DT10 v1.23.0] self-test') === 0, 'version line');
    assert(rep.indexOf(': FAIL') < 0 && rep.indexOf('FAIL [') < 0, 'no FAIL');
    var baseLines = repB.split('\n'), newLines = rep.split('\n');
    deq(baseLines.filter(function (l) { return l.indexOf('[DT10 v1.22.0]') !== 0 && newLines.indexOf(l) < 0; }), [], 'every base self-test line present');
    eq(newLines.length, baseLines.length + 2, 'exactly two lines added');
    assert(repB.indexOf('reentry cooldown') < 0, 'GOLDEN NEGATIVE: the base lacks the lines');
    var armed = String(load(newSource, { TFB_T10_REENTRY_COOLDOWN_DAYS: ' 10 ' }).dt10SelfTest());
    assert(armed.indexOf('reentry cooldown (TFB_T10_REENTRY_COOLDOWN_DAYS): 10 (') > 0 && armed.indexOf('reentry cooldown core: ok') > 0 && armed.indexOf('FAIL') < 0, 'armed property line');
  });

  test('R7 ES5/ASCII lint of the diff; inventory 3 functions + 3 vars added, 0 removed; 14 hunks reverse to the pinned base; WHY block newest-first; LF only', function () {
    var baseLines = {}, bl = baseSource.split('\n'), nl = newSource.split('\n');
    for (var i = 0; i < bl.length; i++) baseLines[bl[i]] = true;
    var added = nl.filter(function (l) { return !baseLines[l]; });
    assert(added.length >= 150 && added.length <= 230, 'added lines ' + added.length);
    added.forEach(function (l) {
      assert(!/\b(let|const|class)\b|=>|`/.test(l), 'ES5 violation: ' + l);
      if (!/^[\x09\x20-\x7e]*$/.test(l)) assert(baseLines[l.replace('1.23.0', '1.22.0')] === true, 'non-ASCII added line: ' + l);
    });
    assert(newSource.indexOf('\r') < 0, 'LF only');
    var tb = names(baseSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), tn = names(newSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    deq(Object.keys(tb).filter(function (k) { return !tn[k]; }), [], 'functions removed');
    deq(Object.keys(tn).filter(function (k) { return !tb[k]; }).sort(), ['dt10CooldownMapFromValues_', 'dt10CooldownMap_', 'dt10StabCooldownDays_'], 'functions added');
    var vb = names(baseSource, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(newSource, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(function (k) { return !vn[k]; }), [], 'vars removed');
    deq(Object.keys(vn).filter(function (k) { return !vb[k]; }).sort(), ['DT10_LEDGER_SHEET', 'DT10_T10_COOLDOWN_MAX', 'DT10_T10_COOLDOWN_PROP'], 'vars added');
    eq(EDITS.length, 14, 'hunk count');
    for (var e = 0; e < EDITS.length; e++) eq(newSource.split(EDITS[e][0]).length - 1, 1, 'each current is unique: ' + EDITS[e][0].split('\n')[0]);
    eq(sha256(history.v1220Source(newSource)), history.V1220_SHA256, 'reversal of the delivered file');
    var why = newSource.indexOf(' * v1.23.0 (2026-11-08) [REENTRY-COOLDOWN]'), prev = newSource.indexOf(' * v1.22.0 (2026-11-08) [CONFIRM-WINDOW]');
    assert(why > 0 && prev > why, 'WHY block newest-first');
    assert(/^[\x09\x0a\x20-\x7e]*$/.test(newSource.slice(why, prev)), 'WHY block ASCII');
    assert(/^function dt10StabCore_\(rawTickets, cands, state, knobs, limit, today, outage, cooldown\) \{$/m.test(newSource), 'signature');
  });

  test('R8 mutants: three one-line mutations are each caught', function () {
    var mutants = [
      ['cooled name still seated', "      if (cooledDown(ts)) {\n        // v1.23.0", "      if (false) {\n        // v1.23.0"],
      ['token leaks unarmed', "  if (cdMap !== null) {\n    note += ', cooldown='", "  if (true) {\n    note += ', cooldown='"],
      ['future sale counts', "    if (age < 0 || age > lim) continue;", "    if (age > lim) continue;"]
    ];
    mutants.forEach(function (mu) {
      eq(newSource.split(mu[1]).length - 1, 1, 'mutant anchor unique: ' + mu[0]);
      var src = newSource.replace(mu[1], mu[2]);
      var M = load(src, {});
      var caught = false;
      try {
        var m0 = scenarios(M, undefined);
        var ok1 = JSON.stringify(m0) === JSON.stringify(bb);
        var on = replay(M, knobs(10), seed(), raws(), grids(), [chalMap, chalMap, chalMap, chalMap]);
        var ok2 = bySym(on[0], 'CHAL.US') === null;
        var fut = M.dt10CooldownMapFromValues_(ledger([['FUT.US', 'Active', '2026-10-01', '2026-10-22']]), '2026-10-21', 10);
        var ok3 = Object.keys(fut).length === 0;
        caught = !(ok1 && ok2 && ok3);
      } catch (eM) { caught = true; }
      assert(caught, 'mutant not caught: ' + mu[0]);
    });
  });

  return sha256(JSON.stringify([bb, nn]));
}

var digests = [];
for (var run = 0; run < 3; run++) {
  passed = 0; failed = 0;
  if (run > 0) console.log('--- run ' + (run + 1));
  digests.push(battery());
}
var same = digests[0] === digests[1] && digests[1] === digests[2];
if (!same) { failed++; console.log('FAIL digests differ across runs: ' + digests.join(' ')); }
console.log('digest ' + digests[0].slice(0, 16) + (same ? ' x3 identical' : ''));
console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
