/* FUNDABLE-SEATS (16_Decision_Top10.gs v1.26.0) -- a name IBKR cannot fund never takes a seat from one it can.
 * TFB_T10_FUNDABLE_CCY = 'USD,SAR' (owner decision C, 8 Nov 2026; Codex review P1 on #929: a backend re-sort is
 * undone by this core, which seats by smoothed score): listed confirmed challengers and fast-track fills go first;
 * a listed challenger takes the seat guard's non-qualifying pick, else the weakest unlisted seat without the score
 * test ('nf'); an unlisted challenger may only take an unlisted seat. Absent / junk / a fault = the v1.25.0 seats.
 *
 * Dual-tree node harness over the REAL file; the base is the v1.25.0 file rebuilt by reversing the enumerated
 * v1.26.0 edits (SHA-256 pinned).
 *   F0 base oracle; versions in lockstep; both compile; census +3 functions +1 var, 0 removed; 14 hunks reverse.
 *   F1 GOLDEN NEGATIVE (the #929 trace): limit 1, S.SR seated at 60, E.PA (EUR) confirms at 90 -> v1.25.0 (and
 *      v1.25.0 handed the armed knob) displaces S.SR; v1.26.0 armed keeps S.SR and E.PA waits at 3/3.
 *   F2 armed rules: a listed challenger takes an unlisted seat with no score test (nf); the fill takes listed names
 *      first; with the seat guard the guard's non-qualifying pick stands; an unlisted challenger takes an unlisted
 *      seat only by the score test (u) and otherwise waits; an outage-paused seat is never force-taken; the raw
 *      ticket's currency stands in for a candidate row without one; a missing currency is unlisted.
 *   F3 KILL: no knob -> the core deep-equals v1.25.0 on every scenario; parser and service read (absent, blank,
 *      junk, a fault = off).
 *   F4 embedded dt10SelfTest: two new lines ok, zero FAIL, base + 2 lines; the base lacks them; armed line.
 *   F5 lint: ES5/ASCII diff, LF only, WHY block newest-first.
 *   F6 mutants: four one-line mutations are each caught.
 * The whole battery runs three times and the three digests must match.
 * Usage: node tests/test_dt10_v1260_fundable_seats.js [new.gs] [base.gs]
 */
'use strict';
var fs = require('fs'), vm = require('vm'), path = require('path'), crypto = require('crypto');
var ROOT = path.join(__dirname, '..');
var NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '16_Decision_Top10.gs');
var BASE_PATH = process.argv[3] || '';
var history = require(path.join(ROOT, 'tests', 'js', 'dt10_cash_utc_history.js'));
var EDITS = require(path.join(ROOT, 'tests', 'js', 'dt10_edits_v1260.json'));
var FD = 'TFB_T10_FUNDABLE_CCY';

var newSource = fs.readFileSync(NEW_PATH, 'utf8');
function sha256(s) { return crypto.createHash('sha256').update(s).digest('hex'); }
var baseSource;
try {
  baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : history.v1250Source(newSource);
} catch (eBase) {
  console.log('FAIL F0 base oracle: ' + String(eBase && eBase.message || eBase).split('\n')[0]);
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

function load(source, props, options) {
  options = options || {};
  var store = {}; for (var k in props) if (props.hasOwnProperty(k)) store[k] = props[k];
  var statusRows = [];
  for (var r = 0; r < 60; r++) statusRows.push(['', '']);
  var statusSheet = { getRange: function () { return { getValues: function () { return plain(statusRows); } }; } };
  var ssStub = { getSheetByName: function (n) { return n === '_Status' ? statusSheet : null; }, getRangeByName: function () { return null; } };
  var c = {
    PropertiesService: { getScriptProperties: function () {
      if (options.propsThrow) throw new Error('synthetic property outage');
      return { getProperty: function (key) { return store.hasOwnProperty(key) ? store[key] : null; },
               setProperty: function (key, v) { store[key] = String(v); }, deleteProperty: function (key) { delete store[key]; } }; } },
    Logger: { log: function () {} },
    SpreadsheetApp: { getActiveSpreadsheet: function () { return ssStub; }, getActive: function () { return ssStub; },
                      getUi: function () { throw new Error('no ui'); } },
    Utilities: { formatDate: function (d, tz, pattern) { return pattern === 'yyyy-MM-dd' ? '2026-10-25' : '2026-10-25 00:00:00'; } },
    Session: { getScriptTimeZone: function () { return 'Asia/Riyadh'; } },
    UrlFetchApp: { fetch: function () { throw new Error('no net'); } }
  };
  vm.createContext(c);
  vm.runInContext(source, c, { filename: '16_Decision_Top10.gs' });
  return c;
}

/* ------------------------------------------------------------ fixtures */
var DAY = '2026-10-21';
function knobs(fund, extra) {
  var k = { enabled: true, confirm_days: 3, exit_days: 3, rank_buffer: 15, smooth_days: 5, hard_strict: true, confirm_basis: 'qualified' };
  if (fund) k.fundable = { USD: true, SAR: true };
  for (var x in (extra || {})) if (extra.hasOwnProperty(x)) k[x] = extra[x];
  return k;
}
function cand(sym, sc, ccy, verdict) {
  var c = { symbol: sym, name: sym, opportunity_score: sc, verdict: verdict || 'INVEST', price: 10 };
  if (ccy !== undefined) c.currency = ccy;
  return c;
}
function tk(sym, sc, ccy) {
  var t = { symbol: sym, name: sym, opportunity_score: sc, suggested_sar: 5000, suggested_shares: 50, entry_zone: '9-11',
            stop_sar: 8, tp1_sar: 12, tp2_sar: 14, exp_gain_12m_sar: 900, advisor_note: 'INVEST', detail: { funds_from: 'Cash 5,000 SAR' } };
  if (ccy !== undefined) t.currency = ccy;
  return t;
}
function seat(hist, extra) {
  var s = { ci: 3, co: 0, member: true, since: '2026-10-01', ls: '2026-10-20', hist: hist, ft: false };
  for (var x in (extra || {})) if (extra.hasOwnProperty(x)) s[x] = extra[x];
  return s;
}
function chal(hist) { return { ci: 2, co: 0, member: false, since: '', ls: '2026-10-20', hist: hist, ft: false }; }
function seed(symbols) { return { v: 1, date: '2026-10-20', symbols: symbols }; }
function core(M, raw, grid, sd, k, limit, outage) {
  return plain(M.dt10StabCore_(raw, grid, M.dt10StabParseState_(plain(sd)), k, limit, DAY, outage));
}
var SC = {
  A: { raw: [tk('E.PA', 90, 'EUR'), tk('S.SR', 60, 'SAR')], grid: [cand('E.PA', 90, 'EUR'), cand('S.SR', 60, 'SAR')],
       seed: seed({ 'S.SR': seat([60, 60]), 'E.PA': chal([90, 90]) }), limit: 1 },
  B: { raw: [tk('E.PA', 95, 'EUR'), tk('U.US', 70, 'USD')], grid: [cand('E.PA', 95, 'EUR'), cand('U.US', 70, 'USD')],
       seed: seed({ 'E.PA': seat([95, 95]), 'U.US': chal([70, 70]) }), limit: 1 },
  C: { raw: [tk('E.PA', 95, 'EUR'), tk('U.US', 70, 'USD')], grid: [cand('E.PA', 95, 'EUR'), cand('U.US', 70, 'USD')],
       seed: seed({}), limit: 1 },
  /* guard 3: G.US listed but WATCH today (non-qualifying), H.PA unlisted and qualifying, C.US listed challenger */
  D: { raw: [tk('H.PA', 95, 'EUR'), tk('C.US', 88, 'USD')], grid: [cand('H.PA', 95, 'EUR'), cand('C.US', 88, 'USD'), cand('G.US', 70, 'USD', 'WATCH')],
       seed: seed({ 'H.PA': seat([95, 95]), 'G.US': seat([99, 99], { co: 1 }), 'C.US': chal([88, 88]) }), limit: 2 },
  /* unlisted challenger E2.PA at 92 vs an unlisted seat E1.PA at 80 and a listed seat U.US at 50 */
  E: { raw: [tk('E2.PA', 92, 'EUR'), tk('U.US', 50, 'USD'), tk('E1.PA', 80, 'EUR')],
       grid: [cand('E2.PA', 92, 'EUR'), cand('U.US', 50, 'USD'), cand('E1.PA', 80, 'EUR')],
       seed: seed({ 'U.US': seat([50, 50]), 'E1.PA': seat([80, 80]), 'E2.PA': chal([92, 92]) }), limit: 2 },
  /* the candidate row has no currency; the raw ticket says USD */
  G: { raw: [tk('E.PA', 95, 'EUR'), tk('R.US', 70, 'USD')], grid: [cand('E.PA', 95, 'EUR'), cand('R.US', 70)],
       seed: seed({ 'E.PA': seat([95, 95]), 'R.US': chal([70, 70]) }), limit: 1 },
  /* Codex P2 on #933: guard 3, an old listed seat O.US at 50, a FRESH confirmed unlisted seat F.PA at 80, an unlisted
     challenger X.PA confirming at 92: the guard picks O.US; the unlisted path must keep F.PA's age protection */
  I: { raw: [tk('X.PA', 92, 'EUR'), tk('F.PA', 80, 'EUR'), tk('O.US', 50, 'USD')],
       grid: [cand('X.PA', 92, 'EUR'), cand('F.PA', 80, 'EUR'), cand('O.US', 50, 'USD')],
       seed: seed({ 'O.US': seat([50, 50]), 'F.PA': seat([80, 80], { since: '2026-10-20' }), 'X.PA': chal([92, 92]) }), limit: 2 },
  /* the same with F.PA past the guard: the unlisted challenger takes it by the score test */
  J: { raw: [tk('X.PA', 92, 'EUR'), tk('F.PA', 80, 'EUR'), tk('O.US', 50, 'USD')],
       grid: [cand('X.PA', 92, 'EUR'), cand('F.PA', 80, 'EUR'), cand('O.US', 50, 'USD')],
       seed: seed({ 'O.US': seat([50, 50]), 'F.PA': seat([80, 80]), 'X.PA': chal([92, 92]) }), limit: 2 },
  /* no currency anywhere: unlisted */
  H: { raw: [tk('E.PA', 95, 'EUR'), tk('X.US', 70)], grid: [cand('E.PA', 95, 'EUR'), cand('X.US', 70)],
       seed: seed({ 'E.PA': seat([95, 95]), 'X.US': chal([70, 70]) }), limit: 1 }
};
function run(M, key, fund, extra, outage) { var s = SC[key]; return core(M, s.raw, s.grid, s.seed, knobs(fund, extra), s.limit, outage); }

function battery() {
  var N = load(newSource, {}), B = load(baseSource, {});
  var parts = [];

  test('F0 base oracle is the reviewed v1.25.0 file (SHA-256 pinned); versions in lockstep; both compile; census +3 functions +1 var, 0 removed; 15 hunks reverse to the base', function () {
    eq(sha256(baseSource), history.V1250_SHA256, 'base SHA-256');
    eq(B.DT10_VERSION, '1.25.0'); eq(N.DT10_VERSION, '1.26.0');
    assert(/^ \* Version: 1\.26\.0 /.test(newSource.split('\n')[3]), 'header line 4 lockstep');
    new vm.Script(newSource); new vm.Script(baseSource);
    var fb = names(baseSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = names(newSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    deq(Object.keys(fb).filter(function (k) { return !fn[k]; }), [], 'functions removed');
    deq(Object.keys(fn).filter(function (k) { return !fb[k]; }).sort(), ['dt10StabFundableParse_', 'dt10StabFundableText_', 'dt10StabFundable_'], 'functions added');
    var vb = names(baseSource, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(newSource, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(function (k) { return !vn[k]; }), [], 'vars removed');
    deq(Object.keys(vn).filter(function (k) { return !vb[k]; }).sort(), ['DT10_T10_FUNDABLE_PROP'], 'vars added');
    eq(N.DT10_T10_FUNDABLE_PROP, FD);
    eq(EDITS.length, 15, 'hunk count');
    for (var e = 0; e < EDITS.length; e++) eq(newSource.split(EDITS[e][0]).length - 1, 1, 'each current is unique: ' + EDITS[e][0].split('\n')[0]);
    eq(sha256(history.v1250Source(newSource)), history.V1250_SHA256, 'reversal of the delivered file');
  });

  test('F1 GOLDEN NEGATIVE (#929 trace): v1.25.0 lets the confirmed EUR challenger at 90 displace the SAR seat at 60, even handed the knob; v1.26.0 armed keeps S.SR and E.PA waits at 3/3', function () {
    var b0 = run(B, 'A', false), b1 = run(B, 'A', true), n1 = run(N, 'A', true);
    eq(b0.audit.exited_displaced.join(','), 'S.SR', 'base displaces'); eq(b1.audit.exited_displaced.join(','), 'S.SR', 'base ignores the knob');
    eq(n1.audit.exited_displaced.length, 0, 'armed: no displacement');
    eq(n1.state.symbols['S.SR'].member, true); eq(n1.state.symbols['E.PA'].member, false);
    assert(n1.note.indexOf(', fundable=SAR/USD') > 0 && n1.note.indexOf('E.PA(3/3)') > 0, 'note: ' + n1.note);
    deq(n1.audit.fundable, { ccys: 'SAR/USD', took: [] }, 'audit');
    parts.push(n1.note, n1.audit);
  });

  test('F2 armed rules: nf without the score test; listed fill first; the guard nq pick stands; an unlisted challenger needs an unlisted seat and the score test; a paused seat is never force-taken; raw-ticket currency fallback; a missing currency is unlisted', function () {
    var b = run(N, 'B', true), b0 = run(B, 'B', false);
    eq(b0.audit.exited_displaced.length, 0, 'base: the score test keeps E.PA');
    eq(b.audit.exited_displaced.join(','), 'E.PA', 'armed: U.US takes the unlisted seat'); eq(b.state.symbols['U.US'].member, true);
    assert(b.note.indexOf('fundable=SAR/USD took E.PA(nf)') > 0, 'note: ' + b.note);
    var c = run(N, 'C', true), c0 = run(B, 'C', false);
    eq(c0.state.symbols['E.PA'].member, true, 'base fills raw order'); eq(c.state.symbols['U.US'].member, true, 'armed fills U.US');
    var d = run(N, 'D', true, { seat_guard_days: 3 });
    eq(d.audit.exited_displaced.join(','), 'G.US', 'the guard non-qualifying pick stands');
    assert(d.note.indexOf('guard=3d took G.US(nq)') > 0 && d.note.indexOf('fundable=SAR/USD') > 0 && d.note.indexOf('fundable=SAR/USD took') < 0, 'note: ' + d.note);
    eq(d.state.symbols['H.PA'].member, true, 'the unlisted qualifying seat keeps its seat this time');
    var e = run(N, 'E', true);
    eq(e.audit.exited_displaced.join(','), 'E1.PA', 'unlisted challenger takes the unlisted seat by score');
    assert(e.note.indexOf('took E1.PA(u)') > 0 && e.state.symbols['U.US'].member === true, 'note: ' + e.note);
    var e0 = run(B, 'E', false);
    eq(e0.audit.exited_displaced.join(','), 'U.US', 'base takes the weakest seat (listed)');
    var pz = core(N, [tk('U.US', 70, 'USD')], [cand('U.US', 70, 'USD')], SC.B.seed, knobs(true), 1, { 'E.PA': true });
    eq(pz.audit.exited_displaced.length, 0, 'an outage-paused unlisted seat (row missing in an outage) is never force-taken');
    eq(pz.state.symbols['E.PA'].member, true, 'the paused seat keeps its seat');
    var i0 = run(B, 'I', false, { seat_guard_days: 3 }), i1 = run(N, 'I', true, { seat_guard_days: 3 });
    eq(i0.audit.exited_displaced.join(','), 'O.US', 'base: the guard takes the old listed seat');
    eq(i1.audit.exited_displaced.length, 0, 'armed: the fresh unlisted seat keeps its age protection, X.PA waits');
    assert(i1.state.symbols['F.PA'].member === true && i1.state.symbols['O.US'].member === true && i1.note.indexOf('X.PA(3/3)') > 0, 'note: ' + i1.note);
    var j1 = run(N, 'J', true, { seat_guard_days: 3 });
    eq(j1.audit.exited_displaced.join(','), 'F.PA', 'past the guard the unlisted seat goes by the score test');
    assert(j1.note.indexOf('took F.PA(u)') > 0, 'note: ' + j1.note);
    parts.push(i1.note, j1.note);
    var g = run(N, 'G', true);
    eq(g.audit.exited_displaced.join(','), 'E.PA', 'raw-ticket USD stands in for the row');
    var h = run(N, 'H', true);
    eq(h.audit.exited_displaced.length, 0, 'no currency anywhere: unlisted, and E.PA wins the score test');
    parts.push(b.note, c.note, d.note, e.note, g.note, h.note);
  });

  test('F3 KILL: no knob -> the core deep-equals v1.25.0 on every scenario (and with the seat guard); parser and service read', function () {
    Object.keys(SC).forEach(function (key) {
      deq(run(N, key, false), run(B, key, false), 'scenario ' + key);
      deq(run(N, key, false, { seat_guard_days: 3 }), run(B, key, false, { seat_guard_days: 3 }), 'scenario ' + key + ' with the guard');
    });
    var P = N.dt10StabFundableParse_;
    [null, undefined, '', '  ', 'junk', 'USD,EURO', 'US', 'USD,1', 'USD;S'].forEach(function (raw) { eq(P(raw), null, 'off: ' + JSON.stringify(raw)); });
    deq(plain(P('usd sar')), { USD: true, SAR: true }); deq(plain(P('USD; SAR / usd + GBX')), { USD: true, SAR: true, GBX: true });
    eq(N.dt10StabFundableText_(P('usd,sar')), 'SAR/USD');
    eq(load(newSource, {}).dt10StabFundable_(), null, 'absent');
    var pj = {}; pj[FD] = 'junk'; eq(load(newSource, pj).dt10StabFundable_(), null, 'junk');
    var pa = {}; pa[FD] = 'USD,SAR'; deq(plain(load(newSource, pa).dt10StabFundable_()), { USD: true, SAR: true }, 'armed');
    eq(load(newSource, pa, { propsThrow: true }).dt10StabFundable_(), null, 'a fault reads off');
  });

  test('F4 embedded dt10SelfTest: two new lines ok, zero FAIL, base + 2 lines; the base lacks them; armed line', function () {
    var rep = String(N.dt10SelfTest()), repB = String(B.dt10SelfTest());
    assert(rep.indexOf('[DT10 v1.26.0] self-test') === 0, 'version line');
    ['fundable seats (TFB_T10_FUNDABLE_CCY): off (every currency)', 'fundable seats core: ok'].forEach(function (l) {
      assert(rep.indexOf(l) > 0, 'line: ' + l); assert(repB.indexOf('fundable seats') < 0, 'GOLDEN NEGATIVE: the base lacks ' + l);
    });
    assert(rep.indexOf(': FAIL') < 0 && rep.indexOf('FAIL [') < 0, 'no FAIL');
    eq(rep.split('\n').length, repB.split('\n').length + 2, 'two lines added');
    var p = {}; p[FD] = 'USD,SAR';
    var armed = String(load(newSource, p).dt10SelfTest());
    assert(armed.indexOf('fundable seats (TFB_T10_FUNDABLE_CCY): SAR/USD') > 0 && armed.indexOf('FAIL') < 0, 'armed line');
  });

  test('F5 lint: ES5/ASCII diff, LF only, WHY block newest-first', function () {
    var baseLines = {}, bl = baseSource.split('\n'), nl = newSource.split('\n');
    for (var i = 0; i < bl.length; i++) baseLines[bl[i]] = true;
    var added = nl.filter(function (l) { return !baseLines[l]; });
    assert(added.length >= 120 && added.length <= 200, 'added lines ' + added.length);
    added.forEach(function (l) {
      var code = l.replace(/'(?:[^'\\]|\\.)*'/g, "''");
      assert(!/\b(let|const|class)\b|=>|`/.test(code), 'ES5 violation: ' + l);
      if (!/^[\x09\x20-\x7e]*$/.test(l)) assert(baseLines[l.replace('1.26.0', '1.25.0')] === true, 'non-ASCII added line: ' + l);
    });
    assert(newSource.indexOf('\r') < 0, 'LF only');
    var why = newSource.indexOf(' * v1.26.0 (2026-11-08) [FUNDABLE-SEATS]'), prev = newSource.indexOf(' * v1.25.0 (2026-11-08) [OWNER-BANDS]');
    assert(why > 0 && prev > why, 'WHY block newest-first');
  });

  test('F6 mutants: five one-line mutations are each caught', function () {
    var mutants = [
      ['fill in raw order', "  var ftSyms = fundFirst(rawSyms);   // v1.26.0: listed names fill first (armed only)", "  var ftSyms = rawSyms;"],
      ['nf needs the score test', "          return { sym: vs, force: true, tag: 'nf' };", "          return { sym: vs, force: false, tag: 'nf' };"],
      ['unlisted challenger may take a listed seat', "    if (cur && !fundOk(cur)) return null;", "    if (cur) return null;"],
      ['no raw-ticket fallback', "    if ((fccy === null || fccy === undefined || fccy === '') && rawBySym[sym]) fccy = rawBySym[sym].currency;", "    // (mutant: no fallback)"],
      ['unlisted path ignores the seat guard', "    if (sgDays > 0) {   // the seat guard's rules, restricted to unlisted seats", "    if (false) {"]
    ];
    mutants.forEach(function (mu) {
      eq(newSource.split(mu[1]).length - 1, 1, 'mutant anchor unique: ' + mu[0]);
      var caught = false;
      try {
        var M = load(newSource.replace(mu[1], mu[2]), {});
        var ok1 = run(M, 'C', true).state.symbols['U.US'].member === true;
        var ok2 = run(M, 'B', true).audit.exited_displaced.join(',') === 'E.PA';
        var ok3 = run(M, 'A', true).audit.exited_displaced.length === 0;
        var ok4 = run(M, 'G', true).audit.exited_displaced.join(',') === 'E.PA';
        var ok5 = run(M, 'I', true, { seat_guard_days: 3 }).audit.exited_displaced.length === 0;
        caught = !(ok1 && ok2 && ok3 && ok4 && ok5);
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
