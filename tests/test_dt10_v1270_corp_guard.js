/* CORP-EVENT-GUARD (16_Decision_Top10.gs v1.27.0) -- a ticket on a name with a filed corporate event is held.
 * DT10_GUARD_CORP_DAYS = N (owner, 8 Nov 2026, after INDV.US was seated and bought the day its merger closed): the
 * ticket guard reads the Corporate_Events tab (scripts/run_corporate_events.py: SEC EDGAR 8-K items 2.01 / 5.01 /
 * 5.03 / 3.03 / 1.03 / 3.01 / 4.02, newest first) and holds an executable ticket whose event day lies within
 * -N..+N days. Absent / junk / a fault = the v1.26.0 guard byte for byte.
 *
 * Dual-tree node harness over the REAL file; the base is the v1.26.0 file rebuilt by reversing the enumerated
 * v1.27.0 edits (SHA-256 pinned).
 *   G0 base oracle; versions in lockstep; both compile; census +3 functions +3 vars, 0 removed; 11 hunks reverse.
 *   G1 GOLDEN NEGATIVE (the 2 Nov trace): INDV.US executable, 8-K 2.01 + 5.03 filed 2 Nov, render 8 Nov ->
 *      v1.26.0 (and v1.26.0 handed the knob) keeps the ticket; v1.27.0 with the knob at 10 holds it: observe names
 *      it, enforce routes it through the research path and frees its cash.
 *   G2 pure rules: the window is symmetric (-N..+N); a filing N+1 days back passes; a future dated event holds;
 *      the first row per symbol wins (newest first); a malformed date is skipped; earnings and ex-dividend win
 *      first; owner rule and cap still apply after; the map reads a header anywhere in the first rows.
 *   G3 KILL: no knob -> core deep-equals v1.26.0 on every board; config: absent / blank / junk / 61 / a fault = no
 *      corpDays key; the tab is read once per render and only while the knob is set; an absent or oversized tab
 *      reads {}.
 *   G4 embedded dt10SelfTest: the v1.26.0 report while the knob is unset; one core-ok line while it is set and the
 *      guard is armed; zero FAIL.
 *   G5 lint: ES5/ASCII diff, LF only, WHY block newest-first.
 *   G6 mutants: five one-line mutations are each caught.
 * The whole battery runs three times and the three digests must match.
 * Usage: node tests/test_dt10_v1270_corp_guard.js [new.gs] [base.gs]
 */
'use strict';
var fs = require('fs'), vm = require('vm'), path = require('path'), crypto = require('crypto');
var ROOT = path.join(__dirname, '..');
var NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '16_Decision_Top10.gs');
var BASE_PATH = process.argv[3] || '';
var history = require(path.join(ROOT, 'tests', 'js', 'dt10_cash_utc_history.js'));
var EDITS = require(path.join(ROOT, 'tests', 'js', 'dt10_edits_v1270.json'));
var GUARD = 'DT10_TICKET_GUARD', KNOB = 'DT10_GUARD_CORP_DAYS';

// 2026-11-09: the delivered file is v1.28.0 [RENDER-TIMING]; this harness proves the v1.27.0 diff, so it reads
// the v1.27.0 file rebuilt from the delivered one (tests/js/dt10_edits_v1280.json reversed, SHA-256 pinned).
var newSource = history.v1270Source(fs.readFileSync(NEW_PATH, 'utf8'));
function sha256(s) { return crypto.createHash('sha256').update(s).digest('hex'); }
var baseSource;
try {
  baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : history.v1260Source(newSource);
} catch (eBase) {
  console.log('FAIL G0 base oracle: ' + String(eBase && eBase.message || eBase).split('\n')[0]);
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
  if (ja !== jb) throw new Error((m || 'deep-equal') + ': ' + ja.slice(0, 300) + ' | ' + jb.slice(0, 300));
}
function plain(v) { return JSON.parse(JSON.stringify(v)); }
function names(src, re) { var out = {}, m; while ((m = re.exec(src)) !== null) out[m[1]] = true; return out; }

/* The Corporate_Events grid (run_corporate_events.py HEADERS), event days relative to the Riyadh day 2026-11-08. */
var TODAY = '2026-11-08';
/* The context's Date is pinned to 12:00 Riyadh on TODAY (09:00 UTC): new Date() and Date.now() return it, every
 * other constructor form and Date.parse / Date.UTC are the real ones. */
var FROZEN_CLOCK = "(function () { var RealDate = Date, FIXED = RealDate.parse('" + TODAY + "T09:00:00Z');" +
  " function FrozenDate() { var a = Array.prototype.slice.call(arguments);" +
  " if (!(this instanceof FrozenDate)) return new RealDate(FIXED).toString();" +
  " if (!a.length) return new RealDate(FIXED);" +
  " return new (Function.prototype.bind.apply(RealDate, [null].concat(a)))(); }" +
  " FrozenDate.prototype = RealDate.prototype; FrozenDate.now = function () { return FIXED; };" +
  " FrozenDate.parse = RealDate.parse; FrozenDate.UTC = RealDate.UTC; Date = FrozenDate; })();";
var GRID = [
  ['Symbol', 'Event', 'Event Date', 'Filed At (UTC)', 'Form', 'Items', 'Source', 'URL', 'Updated At (Riyadh)'],
  ['FUT.US', 'rights_modified', '2026-11-12', '2026-11-10 21:00', '8-K', '3.03', 'sec.gov EDGAR submissions', '', '2026-11-08 08:37'],
  ['INDV.US', 'merger_completed+charter_change', '2026-11-02', '2026-11-02 21:05', '8-K', '2.01,5.03', 'sec.gov EDGAR submissions', 'https://www.sec.gov/x', '2026-11-08 08:37'],
  ['INDV.US', 'change_in_control', '2026-10-01', '2026-10-01 21:00', '8-K', '5.01', 'sec.gov EDGAR submissions', '', '2026-11-08 08:37'],
  ['OLD.US', 'restatement', '2026-09-28', '2026-09-28 20:00', '8-K/A', '4.02', 'sec.gov EDGAR submissions', '', '2026-11-08 08:37'],
  ['EDGE.US', 'delisting_notice', '2026-10-25', '2026-10-25 20:00', '8-K', '3.01', 'sec.gov EDGAR submissions', '', '2026-11-08 08:37'],
  ['BAD.US', 'bankruptcy', 'n/a', '', '8-K', '1.03', 'sec.gov EDGAR submissions', '', '2026-11-08 08:37']
];

/* A context over the REAL file with stubs: Script Properties, a _Status sheet, a Calendar_Events tab, a
 * Corporate_Events tab (counted reads) and Logger capture. */
function load(source, props, options) {
  options = options || {};
  var store = {}; for (var k in props) if (props.hasOwnProperty(k)) store[k] = props[k];
  var statusRows = []; for (var r = 0; r < 60; r++) statusRows.push(['', '']);
  var logs = [], corpReads = [];
  var grid = options.corpGrid === undefined ? GRID : options.corpGrid;
  function tab(values) {
    return { getLastRow: function () { return values.length; }, getLastColumn: function () { return values[0].length; },
             getRange: function (r1, c1, nr, nc) { return { getValues: function () {
               var out = []; for (var i = r1 - 1; i < r1 - 1 + nr; i++) { var row = values[i] || []; out.push(row.slice(c1 - 1, c1 - 1 + nc)); } return out; } }; } };
  }
  var corpTab = grid === null ? null : tab(grid);
  var statusSheet = { getRange: function () { return { getValues: function () { return plain(statusRows); } }; } };
  var ssStub = { getSheetByName: function (n) {
      if (n === '_Status') return statusSheet;
      if (n === 'Corporate_Events') { corpReads.push(n); if (options.corpThrow) throw new Error('synthetic read fault'); return corpTab; }
      return null; }, getRangeByName: function () { return null; } };
  var c = {
    PropertiesService: { getScriptProperties: function () {
      if (options.propsThrow) throw new Error('synthetic property outage');
      return { getProperty: function (key) { return store.hasOwnProperty(key) ? store[key] : null; },
               setProperty: function (key, v) { store[key] = String(v); }, deleteProperty: function (key) { delete store[key]; } }; } },
    Logger: { log: function (m) { logs.push(String(m)); } },
    SpreadsheetApp: { getActiveSpreadsheet: function () { return ssStub; }, getActive: function () { return ssStub; },
                      getUi: function () { throw new Error('no ui'); } },
    Utilities: { formatDate: function (d, tz, pattern) { return pattern === 'yyyy-MM-dd' ? TODAY : TODAY + ' 00:00:00'; } },
    Session: { getScriptTimeZone: function () { return 'Asia/Riyadh'; } },
    UrlFetchApp: { fetch: function () { throw new Error('no net'); } }
  };
  vm.createContext(c);
  // 2026-11-09: freeze the context clock at the trace day. The fixture's event days are relative to TODAY and
  // Utilities.formatDate returns TODAY, but the guard also reads new Date(); on the real clock the '6d ago'
  // expectations turned into '7d' on 9 Nov and the blocking lean lane went red for every PR.
  vm.runInContext(FROZEN_CLOCK, c, { filename: 'frozen_clock.js' });
  vm.runInContext(source, c, { filename: '16_Decision_Top10.gs' });
  c.__logs = logs; c.__corpReads = corpReads; c.__ss = ssStub;
  return c;
}

/* ------------------------------------------------------------ fixtures */
function tk(sym, extra) {
  var t = { symbol: sym, name: sym, price: 20, currency: 'USD', suggested_sar: 1000, suggested_shares: 10,
            opportunity_score: 80, advisor_note: 'INVEST ' + sym, detail: { funds_from: 'Cash 1,000 SAR' },
            confidence: 'High', engine_roi_pct: 20 };
  for (var x in (extra || {})) if (extra.hasOwnProperty(x)) t[x] = extra[x];
  return t;
}
function board() { return [tk('INDV.US'), tk('OLD.US'), tk('FUT.US'), tk('BAD.US'), tk('NEW.US'), tk('EDGE.US')]; }
function syms(res) {
  var h = [], i; for (i = 0; i < res.held.length; i++) h.push(res.held[i].symbol + ':' + res.held[i].kind);
  return h.join(',') + '/' + res.checked + '/' + res.kept;
}
function corpMap(M, grid) { return plain(M.dt10CorpEventMapFromValues_(grid === undefined ? GRID : grid, TODAY)); }

function battery() {
  var N = load(newSource, {}), B = load(baseSource, {});
  var parts = [];

  test('G0 base oracle is the reviewed v1.26.0 file (SHA-256 pinned); versions in lockstep; both compile; census +3 functions +3 vars, 0 removed; 11 hunks reverse to the base', function () {
    eq(sha256(baseSource), history.V1260_SHA256, 'base SHA-256');
    eq(B.DT10_VERSION, '1.26.0'); eq(N.DT10_VERSION, '1.27.0');
    assert(/^ \* Version: 1\.27\.0 /.test(newSource.split('\n')[3]), 'header line 4 lockstep');
    assert(newSource.indexOf("var DT10_VERSION = '1.27.0';") > 0, 'DT10_VERSION literal');
    new vm.Script(newSource); new vm.Script(baseSource);
    var fb = names(baseSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = names(newSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    deq(Object.keys(fb).filter(function (k) { return !fn[k]; }), [], 'functions removed');
    deq(Object.keys(fn).filter(function (k) { return !fb[k]; }).sort(), ['dt10CorpEventHold_', 'dt10CorpEventMapFromValues_', 'dt10CorpEventMap_'], 'functions added');
    var vb = names(baseSource, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(newSource, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(function (k) { return !vn[k]; }), [], 'vars removed');
    deq(Object.keys(vn).filter(function (k) { return !vb[k]; }).sort(), ['DT10_CORP_EVENTS_SHEET', 'DT10_GUARD_CORP_DAYS_MAX', 'DT10_GUARD_CORP_DAYS_PROP'], 'vars added');
    deq([N.DT10_GUARD_CORP_DAYS_PROP, N.DT10_GUARD_CORP_DAYS_MAX, N.DT10_CORP_EVENTS_SHEET], [KNOB, 60, 'Corporate_Events'], 'constants');
    ['DT10_GUARD_CORP_DAYS_PROP', 'DT10_CORP_EVENTS_SHEET', 'dt10CorpEventHold_', 'dt10CorpEventMap_', 'dt10CorpEventMapFromValues_'].forEach(function (k) { eq(typeof B[k], 'undefined', 'the base lacks ' + k); });
    eq(EDITS.length, 11, 'hunk count');
    for (var e = 0; e < EDITS.length; e++) eq(newSource.split(EDITS[e][0]).length - 1, 1, 'each current is unique: ' + EDITS[e][0].split('\n')[0]);
    eq(sha256(history.v1260Source(newSource)), history.V1260_SHA256, 'reversal of the delivered file');
  });

  test('G1 GOLDEN NEGATIVE (the 2 Nov trace): v1.26.0 keeps the INDV.US ticket whatever it is handed; v1.27.0 with the knob at 10 holds it (6d ago) - observe names it, enforce routes it to research and frees the cash', function () {
    var m = corpMap(N);
    eq(m['INDV.US'].days, -6, 'the merger filing is 6 days back'); eq(m['INDV.US'].event, 'merger_completed+charter_change', 'the newest row wins');
    var b0 = B.dt10TicketGuardCore_(board(), true, {}, { earnDays: 3 }, null);
    var b1 = B.dt10TicketGuardCore_(board(), true, {}, { earnDays: 3, corpDays: 10 }, null, m);
    eq(syms(b0), '/6/6', 'base: nothing held'); eq(syms(b1), '/6/6', 'base ignores the knob and the map');
    var n1 = N.dt10TicketGuardCore_(board(), true, {}, { earnDays: 3, corpDays: 10 }, null, m);
    eq(syms(n1), 'INDV.US:corp,FUT.US:corp/6/4', 'armed: the merger name and the dated future event are held');
    eq(n1.held[0].reason, 'corporate event: merger_completed+charter_change (6d ago)', 'reason');
    eq(n1.held[0].tag, 'corp merger_completed+charter_change 6d', 'tag');
    eq(N.dt10TicketGuardToken_(n1, 'observe'), 'tguard: would hold INDV.US(corp merger_completed+charter_change 6d) FUT.US(corp rights_modified 4d)', 'observe token');
    eq(N.dt10TicketGuardToken_(n1, 'enforce'), 'tguard: held 2 (INDV.US corp merger_completed+charter_change 6d, FUT.US corp rights_modified 4d)', 'enforce token');
    // the REAL guard entry point with the knob: one Corporate_Events read, the Logger suffix, the token
    var props = {}; props[GUARD] = 'observe'; props[KNOB] = '10';
    var O = load(newSource, props);
    var cfg = O.dt10TicketGuardConfig_();
    eq(cfg.corpDays, 10, 'config reads the knob');
    var payload = { selected: board(), kpis: { capital_unallocated_sar: 0 }, _dt10_tguard: cfg, _dt10_uv: { state: 'EXECUTABLE' } };
    var token = O.dt10TicketGuard_(payload, {}, O.__ss);
    eq(token, 'tguard: would hold INDV.US(corp merger_completed+charter_change 6d) FUT.US(corp rights_modified 4d)', 'observe token via the guard');
    eq(O.__corpReads.length, 1, 'one Corporate_Events read per render');
    var gl = O.__logs.filter(function (l) { return /ticket guard/.test(l); });
    deq(gl, ['[DT10 v1.27.0] ticket guard (observe): would hold 2 of 6 executable ticket(s): INDV.US(corp merger_completed+charter_change 6d) FUT.US(corp rights_modified 4d) | earnings <= 3d, owner rule off, cap none, corp events <= 10d'], 'one Logger line');
    deq(plain(payload.selected), board(), 'observe changes no ticket');
    // enforce: the held tickets take the research path (Ticket SAR 0) and the freed cash reads in the Logger tail
    var propsE = {}; propsE[GUARD] = 'enforce'; propsE[KNOB] = '10';
    var E = load(newSource, propsE);
    var payloadE = { selected: board(), kpis: { capital_unallocated_sar: 0 }, _dt10_tguard: E.dt10TicketGuardConfig_(), _dt10_uv: { state: 'EXECUTABLE' } };
    var tokenE = E.dt10TicketGuard_(payloadE, {}, E.__ss);
    eq(tokenE, 'tguard: held 2 (INDV.US corp merger_completed+charter_change 6d, FUT.US corp rights_modified 4d)', 'enforce token via the guard');
    // the shared v1.20.0 apply path (dt10TicketGuardApply_ -> dt10FinalizeBoard_) marks the held tickets; the
    // research-path rewrite itself is proven by the v1.20.0 / v1.21.0 REAL-refresh harnesses on the same path
    eq(E.dt10TicketGuardHeldCount_(payloadE.selected), 2, 'two _tguard_held markers');
    eq(payloadE.selected[0]._tguard_held, 'corporate event: merger_completed+charter_change (6d ago)', 'INDV.US marked');
    eq(payloadE.selected[2]._tguard_held, 'corporate event: rights_modified (in 4d)', 'FUT.US marked');
    eq(payloadE.selected[1]._tguard_held, undefined, 'OLD.US (41d back, outside 10) not marked');
    assert(E.__logs.some(function (l) { return l.indexOf('[DT10 v1.27.0] ticket guard (enforce): held 2 of 6 executable ticket(s): INDV.US(corp merger_completed+charter_change 6d) FUT.US(corp rights_modified 4d) | earnings <= 3d, owner rule off, cap none, corp events <= 10d | executable 6 -> ') === 0; }), 'enforce Logger line: ' + E.__logs.join(' || '));
    parts.push(token, tokenE, gl, m);
  });

  test('G2 pure rules: symmetric window; N+1 days back passes; a future event holds; first row per symbol; malformed date skipped; earnings and ex-div first; owner rule and cap after; header anywhere', function () {
    var m = corpMap(N);
    deq(Object.keys(m).sort(), ['EDGE.US', 'FUT.US', 'INDV.US', 'OLD.US'], 'BAD.US (n/a date) is skipped');
    deq([m['OLD.US'].days, m['EDGE.US'].days, m['FUT.US'].days], [-41, -14, 4], 'days');
    eq(m['INDV.US'].form, '8-K', 'form kept');
    var a14 = N.dt10TicketGuardCore_(board(), true, {}, { earnDays: 3, corpDays: 14 }, null, m);
    eq(syms(a14), 'INDV.US:corp,FUT.US:corp,EDGE.US:corp/6/3', 'EDGE at exactly -14 holds (inclusive)');
    var a13 = N.dt10TicketGuardCore_(board(), true, {}, { earnDays: 3, corpDays: 13 }, null, m);
    eq(syms(a13), 'INDV.US:corp,FUT.US:corp/6/4', 'EDGE at -14 passes at 13');
    var a3 = N.dt10TicketGuardCore_(board(), true, {}, { earnDays: 3, corpDays: 3 }, null, m);
    eq(syms(a3), '/6/6', 'at 3 nothing is within the window (INDV -6, FUT +4)');
    var a60 = N.dt10TicketGuardCore_(board(), true, {}, { earnDays: 3, corpDays: 60 }, null, m);
    eq(syms(a60), 'INDV.US:corp,OLD.US:corp,FUT.US:corp,EDGE.US:corp/6/2', 'at 60 the 41d-old restatement holds too');
    eq(N.dt10CorpEventHold_(tk('OLD.US'), m, 40), null, 'N+1 days back passes');
    deq(plain(N.dt10CorpEventHold_(tk('OLD.US'), m, 41)), { kind: 'corp', reason: 'corporate event: restatement (41d ago)', tag: 'corp restatement 41d' }, 'N days back holds');
    deq(plain(N.dt10CorpEventHold_(tk('FUT.US'), m, 4)), { kind: 'corp', reason: 'corporate event: rights_modified (in 4d)', tag: 'corp rights_modified 4d' }, 'a dated future event holds');
    eq(N.dt10CorpEventHold_(tk('NEW.US'), m, 60), null, 'no row: no hold'); eq(N.dt10CorpEventHold_(tk('NEW.US'), null, 60), null, 'no map: no hold');
    var today = N.dt10CorpEventMapFromValues_([['Symbol', 'Event', 'Event Date'], ['T.US', 'merger_completed', TODAY]], TODAY);
    deq(plain(N.dt10CorpEventHold_(tk('T.US'), today, 0)), { kind: 'corp', reason: 'corporate event: merger_completed (today)', tag: 'corp merger_completed 0d' }, 'today at window 0');
    // earnings first, then ex-dividend, then the corporate event, then the owner rule and the cap
    var e1 = N.dt10TicketGuardCore_(board(), true, { 'INDV.US': 1 }, { earnDays: 3, corpDays: 10 }, null, m);
    eq(syms(e1), 'INDV.US:earn,FUT.US:corp/6/4', 'earnings wins');
    var dm = N.dt10ExDivMapFromValues_([['Symbol', 'Ex-Div Date', 'Amount', 'Currency'], ['INDV.US', '2026-11-09', 8.13, 'USD']], TODAY);
    var e2 = N.dt10TicketGuardCore_(board(), true, {}, { earnDays: 3, exdivDays: 3, exdivPct: 2, corpDays: 10 }, dm, m);
    eq(syms(e2), 'INDV.US:exdiv,FUT.US:corp/6/4', 'ex-dividend wins over the corporate event');
    var e3 = N.dt10TicketGuardCore_(board(), true, {}, { earnDays: 3, corpDays: 10, maxTickets: 1 }, null, m);
    eq(syms(e3), 'INDV.US:corp,FUT.US:corp,BAD.US:cap,NEW.US:cap,EDGE.US:cap/6/1', 'the cap applies to the survivors');
    var low = board(); low[1].confidence = 'Low';
    var e4 = N.dt10TicketGuardCore_(low, true, {}, { earnDays: 3, corpDays: 10, ownerRule: true }, null, m);
    eq(e4.held.filter(function (h) { return h.symbol === 'OLD.US'; })[0].kind, 'rule', 'the owner rule still holds OLD.US after the corporate filter');
    // a non-executable seat is never checked; feed not EXECUTABLE: nothing
    var nx = board(); nx[0].suggested_sar = 0;
    eq(syms(N.dt10TicketGuardCore_(nx, true, {}, { earnDays: 3, corpDays: 10 }, null, m)), 'FUT.US:corp/5/4', 'a research seat is not checked');
    eq(syms(N.dt10TicketGuardCore_(board(), false, {}, { earnDays: 3, corpDays: 10 }, null, m)), '/0/0', 'no hold on a non-executable feed');
    // the header may sit below a title row; lower-case / spaced tokens; a repeated symbol keeps the first row
    var g2 = [['Corporate events', '', ''], ['symbol', 'event', 'event date'], ['x.us', 'bankruptcy', '2026-11-07'], ['X.US', 'restatement', '2026-10-01']];
    var m2 = corpMap(N, g2);
    deq(plain(m2), { 'X.US': { days: -1, event: 'bankruptcy', form: '' } }, 'header on row 2, first row per symbol');
    deq(plain(corpMap(N, [['Symbol', 'Event']])), {}, 'no Event Date column: empty map');
    deq(plain(corpMap(N, [])), {}, 'empty grid');
    deq(plain(N.dt10CorpEventMapFromValues_(GRID, 'junk')), {}, 'an unparseable as-of day: empty map');
    parts.push(syms(a14), syms(a60), syms(e2), syms(e3), m2);
  });

  test('G3 KILL: no knob -> the core deep-equals v1.26.0; config absent / blank / junk / 61 / a fault = no corpDays key; the tab is read once and only while the knob is set; absent or oversized tab = {}', function () {
    var m = corpMap(N);
    [[{ earnDays: 3 }, {}], [{ earnDays: 3 }, { 'INDV.US': 1 }], [{ earnDays: 3, ownerRule: true, maxTickets: 1 }, {}]].forEach(function (c) {
      deq(plain(N.dt10TicketGuardCore_(board(), true, c[1], c[0], null, m)), plain(B.dt10TicketGuardCore_(board(), true, c[1], c[0], null)), 'core ' + JSON.stringify(c[0]));
    });
    ['', '  ', 'junk', '61', '-1', '1.5'].forEach(function (raw) {
      var p = {}; p[GUARD] = 'observe'; p[KNOB] = raw;
      var cfg = load(newSource, p).dt10TicketGuardConfig_();
      eq(cfg.hasOwnProperty('corpDays'), false, 'no corpDays for ' + JSON.stringify(raw));
    });
    var pg = {}; pg[GUARD] = 'observe';
    deq(plain(load(newSource, pg).dt10TicketGuardConfig_()), plain(load(baseSource, pg).dt10TicketGuardConfig_()), 'config object = the v1.26.0 object without the knob');
    var p60 = {}; p60[GUARD] = 'observe'; p60[KNOB] = '60'; eq(load(newSource, p60).dt10TicketGuardConfig_().corpDays, 60, 'the clamp admits 60');
    var p0 = {}; p0[GUARD] = 'observe'; p0[KNOB] = '0'; eq(load(newSource, p0).dt10TicketGuardConfig_().corpDays, 0, '0 = the event day only');
    // tab reads: none without the knob; one with it; {} on an absent tab, a fault or an oversized grid
    var payload = function (ctx) { return { selected: board(), kpis: { capital_unallocated_sar: 0 }, _dt10_tguard: ctx.dt10TicketGuardConfig_(), _dt10_uv: { state: 'EXECUTABLE' } }; };
    var Off = load(newSource, pg); Off.dt10TicketGuard_(payload(Off), {}, Off.__ss); eq(Off.__corpReads.length, 0, 'no read without the knob');
    var pk = {}; pk[GUARD] = 'observe'; pk[KNOB] = '10';
    var NoTab = load(newSource, pk, { corpGrid: null }); eq(NoTab.dt10TicketGuard_(payload(NoTab), {}, NoTab.__ss), 'tguard: would hold none', 'absent tab: no hold');
    var Flt = load(newSource, pk, { corpThrow: true }); eq(Flt.dt10TicketGuard_(payload(Flt), {}, Flt.__ss), 'tguard: would hold none', 'a read fault: no hold');
    var big = [GRID[0]]; for (var i = 0; i < 5001; i++) big.push(['INDV.US', 'merger_completed', '2026-11-02']);
    var Big = load(newSource, pk, { corpGrid: big }); eq(Big.dt10TicketGuard_(payload(Big), {}, Big.__ss), 'tguard: would hold none', 'an oversized tab: no hold');
    deq(plain(Big.dt10CorpEventMap_(Big.__ss)), {}, 'dt10CorpEventMap_ reads {} on an oversized tab');
    deq(plain(N.dt10CorpEventMap_(null)), {}, 'no spreadsheet: {}');
    parts.push(plain(load(newSource, p60).dt10TicketGuardConfig_()));
  });

  test('G4 embedded dt10SelfTest: the v1.26.0 report while the knob is unset; one core-ok line while it is set and the guard is armed; zero FAIL', function () {
    var vAll = function (s) { return String(s).replace(/1\.27\.0/g, '1.26.0'); };
    var rep = String(N.dt10SelfTest()), repB = String(B.dt10SelfTest());
    assert(rep.indexOf('[DT10 v1.27.0] self-test') === 0, 'version line');
    eq(vAll(rep), repB, 'the v1.26.0 report without the knob');
    assert(rep.indexOf('corp events') < 0, 'no corp line without the knob');
    var p = {}; p[GUARD] = 'observe'; p[KNOB] = '10';
    var armed = String(load(newSource, p).dt10SelfTest());
    var line = armed.split('\n').filter(function (l) { return l.indexOf('ticket guard corp events (DT10_GUARD_CORP_DAYS): <= 10d either side, tab Corporate_Events | core: ok (') === 0; });
    eq(line.length, 1, 'one armed line: ' + armed.split('\n').filter(function (l) { return /corp events/.test(l); }).join(' || '));
    assert(armed.indexOf(': FAIL') < 0 && armed.indexOf('FAIL [') < 0, 'no FAIL');
    eq(armed.split('\n').length, rep.split('\n').length + 1, 'exactly one line added when armed');
    var pb = {}; pb[GUARD] = 'observe'; pb[KNOB] = '10';
    eq(String(load(baseSource, pb).dt10SelfTest()).indexOf('corp events'), -1, 'GOLDEN NEGATIVE: the base never writes the line');
    parts.push(line[0]);
  });

  test('G5 lint: ES5/ASCII diff, LF only, WHY block newest-first', function () {
    var baseLines = {}, bl = baseSource.split('\n'), nl = newSource.split('\n');
    for (var i = 0; i < bl.length; i++) baseLines[bl[i]] = true;
    var added = nl.filter(function (l) { return !baseLines[l]; });
    assert(added.length >= 100 && added.length <= 160, 'added lines ' + added.length);
    added.forEach(function (l) {
      var code = l.replace(/'(?:[^'\\]|\\.)*'/g, "''");
      assert(!/\b(let|const|class)\b|=>|`/.test(code), 'ES5 violation: ' + l);
      if (!/^[\x09\x20-\x7e]*$/.test(l)) assert(baseLines[l.replace('1.27.0', '1.26.0')] === true, 'non-ASCII added line: ' + l);
    });
    assert(newSource.indexOf('\r') < 0, 'LF only');
    var why = newSource.indexOf(' * v1.27.0 (2026-11-08) [CORP-EVENT-GUARD]'), prev = newSource.indexOf(' * v1.26.0 (2026-11-08) [FUNDABLE-SEATS]');
    assert(why > 0 && prev > why, 'WHY block newest-first');
  });

  test('G6 mutants: five one-line mutations are each caught', function () {
    var mutants = [
      ['window not symmetric (past filings ignored)', "e.days < -maxDays || e.days > maxDays) {", "e.days < 0 || e.days > maxDays) {"],
      ['window exclusive', "e.days < -maxDays || e.days > maxDays) {", "e.days <= -maxDays || e.days >= maxDays) {"],
      ['last row per symbol wins', "    if (!sym || sym === 'SYMBOL' || map.hasOwnProperty(sym)) continue;\n    var day = dt10CalendarDay_(cells[iDate]);\n    if (day === null) continue;\n    var ev =", "    if (!sym || sym === 'SYMBOL') continue;\n    var day = dt10CalendarDay_(cells[iDate]);\n    if (day === null) continue;\n    var ev ="],
      ['filter runs before ex-dividend', "    } else if (typeof cfg.corpDays === 'number' &&\n               (h = dt10CorpEventHold_(t, corpMap, cfg.corpDays)) !== null) {\n      // v1.27.0 [CORP-EVENT-GUARD]: h set by dt10CorpEventHold_.\n    } else if (cfg.ownerRule === true", "    } else if (cfg.ownerRule === true"],
      ['knob clamp dropped', "  if (/^\\d{1,2}$/.test(cpRaw) && Number(cpRaw) <= DT10_GUARD_CORP_DAYS_MAX) {", "  if (/^\\d{1,2}$/.test(cpRaw)) {"]
    ];
    mutants.forEach(function (mu) {
      eq(newSource.split(mu[1]).length - 1, 1, 'mutant anchor unique: ' + mu[0]);
      var caught = false;
      try {
        var M = load(newSource.replace(mu[1], mu[2]), {});
        var m = corpMap(M);
        var ok1 = syms(M.dt10TicketGuardCore_(board(), true, {}, { earnDays: 3, corpDays: 14 }, null, m)) === 'INDV.US:corp,FUT.US:corp,EDGE.US:corp/6/3';
        var ok2 = M.dt10CorpEventHold_(tk('OLD.US'), m, 40) === null;
        var ok3 = m['INDV.US'] && m['INDV.US'].event === 'merger_completed+charter_change';
        var p61 = {}; p61[GUARD] = 'observe'; p61[KNOB] = '61';
        var ok4 = load(newSource.replace(mu[1], mu[2]), p61).dt10TicketGuardConfig_().hasOwnProperty('corpDays') === false;
        caught = !(ok1 && ok2 && ok3 && ok4);
      } catch (eM) { caught = true; }
      assert(caught, 'mutant not caught: ' + mu[0]);
    });
  });

  return sha256(JSON.stringify(parts));
}

var digests = [];
for (var rn = 0; rn < 3; rn++) {
  passed = 0; failed = 0;
  if (rn > 0) console.log('--- run ' + (rn + 1));
  digests.push(battery());
}
var same = digests[0] === digests[1] && digests[1] === digests[2];
if (!same) { failed++; console.log('FAIL digests differ across runs: ' + digests.join(' ')); }
console.log('digest ' + digests[0].slice(0, 16) + (same ? ' x3 identical' : ''));
console.log((failed ? 'FAIL ' : 'PASS ') + passed + '/' + (passed + failed));
process.exit(failed ? 1 : 0);
