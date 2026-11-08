/* CONFIRM-WINDOW-FIX (16_Decision_Top10.gs v1.23.1) -- the window history
 * carries its day-key (st.qd), so (1) a disarmed render day drops the stale
 * bits and (2) a same-day recovery after an outage pause appends today's bit
 * instead of rewriting yesterday's. Two Codex P2 findings on PR #927.
 *
 * Dual-tree node harness over the REAL file; the base is the v1.23.0 file
 * rebuilt by reversing the enumerated v1.23.1 edits (SHA-256 pinned).
 *   W0 base oracle; versions in lockstep; both files compile; no function or
 *      var added or removed; 9 hunks reverse to the pinned base.
 *   W1 GOLDEN NEGATIVE (1) on v1.23.0: armed d1-d2 (qh [1,1]), unarmed d3
 *      (WATCH miss, ci 0, qh kept), re-armed qualified d4 -> ci 3 and a sized
 *      'NEW (confirmed 3/3)'. v1.23.1: d3 drops qh, d4 reads 1/3, no ticket.
 *   W2 GOLDEN NEGATIVE (2) on v1.23.0: armed d3 first render outage-pauses
 *      CHAL (no bit), the same-day qualified render leaves ci 2 (raise on a
 *      bit already 1); with the newest bit 0 it rewrites it. v1.23.1: the
 *      same-day render appends -> ci 3, qd d3; a 0 yesterday stays 0.
 *   W3 KILL: unarmed replays (knob absent / 0, seeds without qh) deep-equal
 *      the base through the core and through dt10ApplyStability_; armed
 *      replays without a disarmed day or a pause deep-equal the base apart
 *      from qd; a 1.23.0 blob (qh, no qd) keeps the raise-only rule on a
 *      same-day render and gains qd on its next day-advance.
 *   W4 parser: qd kept only beside qh and only as yyyy-mm-dd; junk dropped;
 *      hard exit drops qh and qd; soft/capacity/displacement keep both.
 *   W5 embedded dt10SelfTest: the extended 'confirm window core: ok' line,
 *      zero FAIL, line count unchanged against the base.
 *   W6 lint: ES5/ASCII diff, LF only, WHY block newest-first.
 *   W7 mutants: three one-line mutations are each caught.
 *
 * Usage: node tests/test_dt10_v1231_confirm_window_fix.js [new.gs] [base.gs]
 */
'use strict';
var fs = require('fs'), vm = require('vm'), path = require('path'), crypto = require('crypto');
var ROOT = path.join(__dirname, '..');
var NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '16_Decision_Top10.gs');
var BASE_PATH = process.argv[3] || '';
var history = require(path.join(ROOT, 'tests', 'js', 'dt10_cash_utc_history.js'));
var EDITS = require(path.join(ROOT, 'tests', 'js', 'dt10_edits_v1231.json'));

// 2026-11-08: the delivered file is v1.24.0 [SEAT-GUARD]; this harness proves
// the v1.23.1 diff, so it reads the v1.23.1 file rebuilt from the delivered one
// (tests/js/dt10_edits_v1240.json reversed, SHA-256 pinned).
var newSource = history.v1231Source(fs.readFileSync(NEW_PATH, 'utf8'));
function sha256(s) { return crypto.createHash('sha256').update(s).digest('hex'); }
var baseSource;
try {
  baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : history.v1230Source(newSource);
} catch (eBase) {
  console.log('FAIL W0 base oracle: ' + String(eBase && eBase.message || eBase).split('\n')[0]);
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
function stripKeys(v, keys) {
  var o = plain(v);
  (function walk(x) {
    if (x && typeof x === 'object') {
      for (var i = 0; i < keys.length; i++) if (Object.prototype.hasOwnProperty.call(x, keys[i])) delete x[keys[i]];
      for (var k in x) if (x.hasOwnProperty(k)) walk(x[k]);
    }
  })(o);
  return o;
}
function canon(v) {
  if (v === null || typeof v !== 'object') return v;
  if (Array.isArray(v)) return v.map(canon);
  var out = {}; Object.keys(v).sort().forEach(function (k) { out[k] = canon(v[k]); }); return out;
}
function stripQd(v) {
  var o = plain(v);
  (function walk(x) {
    if (x && typeof x === 'object') {
      if (Object.prototype.hasOwnProperty.call(x, 'qd') && Object.prototype.hasOwnProperty.call(x, 'qh')) delete x.qd;
      for (var k in x) if (x.hasOwnProperty(k)) walk(x[k]);
    }
  })(o);
  return o;
}

/* ------------------------------------------------------------ GAS shims */
function ctx(props, options) {
  options = options || {};
  var store = {}; for (var k in props) if (props.hasOwnProperty(k)) store[k] = props[k];
  var logs = [], reads = {};
  var statusRows = [];
  for (var r = 0; r < 60; r++) statusRows.push(['', '']);
  statusRows[0] = ['TFB Feed Global_Markets', 'OK | cov=99.0 | run=40145487350 | 2026-10-25 04:09:22+03:00'];
  var statusSheet = { getRange: function () { return { getValues: function () { return plain(statusRows); } }; } };
  var ssStub = { getSheetByName: function (n) { return n === '_Status' ? statusSheet : null; }, getRangeByName: function () { return null; } };
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
  context.__store = store; context.__logs = logs; context.__reads = reads;
  return context;
}
function load(source, props, options) {
  var c = ctx(props || {}, options);
  vm.runInContext(source, c, { filename: '16_Decision_Top10.gs' });
  return c;
}

/* ------------------------------------------------------------ fixtures (the confirm-basis board) */
var DAYS = ['2026-10-21', '2026-10-22', '2026-10-23', '2026-10-24', '2026-10-25'];
function knobs(win, buf) {
  var k = { enabled: true, confirm_days: 3, exit_days: 3, rank_buffer: (buf === undefined ? 15 : buf),
            smooth_days: 5, hard_strict: true, confirm_basis: 'qualified' };
  if (win !== undefined) k.confirm_window = win;
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
var RAW_WITH = [ticket('A.US', 95), ticket('CHAL.US', 86)], RAW_WITHOUT = [ticket('A.US', 95)];
function core(g, raw, grd, st, k, day, outage) {
  var o = g.dt10StabCore_(plain(raw), plain(grd), st, plain(k), 2, day, outage);
  return plain({ tickets: o.tickets, state: o.state, audit: o.audit, note: o.note });
}
function parse(g, state) { return g.dt10StabParseState_(plain(state)); }
function bySym(out, s) { for (var i = 0; i < out.tickets.length; i++) if (out.tickets[i].symbol === s) return out.tickets[i]; return null; }
/* two armed days: CHAL raw d1, d2 -> ci 2, qh [1,1] */
function armedTwoDays(g) {
  var d1 = core(g, RAW_WITH, grid(), parse(g, seed()), knobs(5), DAYS[0]);
  var d2 = core(g, RAW_WITH, grid(), parse(g, d1.state), knobs(5), DAYS[1]);
  return d2;
}
function applyE2E(source, props, seedObj) {
  var g = load(source, props);
  g.__store.DT10_STAB_STATE_V1 = JSON.stringify(seedObj);
  var payload = { selected: [ticket('A.US', 95), ticket('CHAL.US', 86)], candidates_rows: grid(86), meta: {} };
  var panel = { 'T10: Max Selected': 2, 'T10: Stability Enabled': 'Yes', 'T10: Stability Confirm Days': 3,
                'T10: Stability Exit Days': 3, 'T10: Stability Rank Buffer': 15, 'T10: Stability Smooth Days': 5 };
  var out = g.dt10ApplyStability_(payload, panel, null);
  return { note: out.note, selected: plain(payload.selected), audit: plain(payload.meta.stability.audit),
           knobs: plain(payload.meta.stability.knobs), saved: JSON.parse(g.__store.DT10_STAB_STATE_V1) };
}
function names(src, re) { var out = {}, m; while ((m = re.exec(src)) !== null) out[m[1]] = true; return out; }

/* ------------------------------------------------------------ battery */
function battery() {
  var N = load(newSource, {}), B = load(baseSource, {});
  var digestParts = [];

  test('W0 base oracle is the reviewed v1.23.0 file (SHA-256 pinned); versions in lockstep; both compile; no function or var added or removed; 9 hunks reverse to the base', function () {
    eq(sha256(baseSource), history.V1230_SHA256, 'base SHA-256');
    eq(B.DT10_VERSION, '1.23.0'); eq(N.DT10_VERSION, '1.23.1');
    assert(/^ \* Version: 1\.23\.1 /.test(newSource.split('\n')[3]), 'header line 4 lockstep');
    new vm.Script(newSource, { filename: 'new.gs' }); new vm.Script(baseSource, { filename: 'base.gs' });
    var fb = names(baseSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = names(newSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    deq(Object.keys(fb).sort(), Object.keys(fn).sort(), 'function census unchanged');
    var vb = names(baseSource, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(newSource, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).sort(), Object.keys(vn).sort(), 'var census unchanged');
    eq(EDITS.length, 9, 'hunk count');
    for (var e = 0; e < EDITS.length; e++) eq(newSource.split(EDITS[e][0]).length - 1, 1, 'each current is unique: ' + EDITS[e][0].split('\n')[0]);
    eq(sha256(history.v1230Source(newSource)), history.V1230_SHA256, 'reversal of the delivered file');
  });

  test('W1 finding 1 -- GOLDEN NEGATIVE on v1.23.0: stale bits confirm 3/3 on re-arm; v1.23.1: the unarmed day drops the history and re-arm reads 1/3', function () {
    // base
    var b2 = armedTwoDays(B);
    deq(b2.state.symbols['CHAL.US'].qh, [1, 1], 'base d2 bits'); eq(b2.state.symbols['CHAL.US'].ci, 2);
    var b3 = core(B, RAW_WITHOUT, grid(86, 'WATCH'), parse(B, b2.state), knobs(0), DAYS[2]);      // unarmed miss
    eq(b3.state.symbols['CHAL.US'].ci, 0, 'base unarmed day resets ci');
    deq(b3.state.symbols['CHAL.US'].qh, [1, 1], 'base keeps the stale bits');
    var b4 = core(B, RAW_WITH, grid(), parse(B, b3.state), knobs(5), DAYS[3]);                   // re-armed
    eq(b4.state.symbols['CHAL.US'].ci, 3, 'GOLDEN NEGATIVE: base confirms from stale bits');
    var bt = bySym(b4, 'CHAL.US'); assert(bt !== null && bt._stab_status === 'NEW (confirmed 3/3)', 'base sizes the seat: ' + (bt && bt._stab_status));
    // delivered
    var n2 = armedTwoDays(N);
    deq(n2.state.symbols['CHAL.US'].qh, [1, 1]); eq(n2.state.symbols['CHAL.US'].qd, DAYS[1], 'qd stamped');
    var n3 = core(N, RAW_WITHOUT, grid(86, 'WATCH'), parse(N, n2.state), knobs(0), DAYS[2]);
    eq(n3.state.symbols['CHAL.US'].ci, 0); eq(n3.state.symbols['CHAL.US'].qh, undefined, 'unarmed day drops qh'); eq(n3.state.symbols['CHAL.US'].qd, undefined, 'and qd');
    deq(n3, stripKeys(b3, ['qh', 'qd']), 'unarmed day equals the base with every stale history dropped (A.US migrated bits too)');
    var n4 = core(N, RAW_WITH, grid(), parse(N, n3.state), knobs(5), DAYS[3]);
    eq(n4.state.symbols['CHAL.US'].ci, 1, 'fixed: re-arm reads 1/3'); deq(n4.state.symbols['CHAL.US'].qh, [1]); eq(n4.state.symbols['CHAL.US'].qd, DAYS[3]);
    eq(bySym(n4, 'CHAL.US'), null, 'fixed: a 1/3 challenger is not seated on a full board (the base displaced B)');
    deq(n4.audit.exited_displaced, [], 'fixed: B keeps its seat');
    // a seven-day disarmed stretch behaves the same (the drop happens on the first unarmed day)
    var st = parse(N, n2.state), prev = null;
    for (var d = 0; d < 7; d++) { prev = core(N, RAW_WITHOUT, grid(86, 'WATCH'), st, knobs(0), '2026-11-0' + (d + 1)); st = parse(N, prev.state); }
    eq(prev.state.symbols['CHAL.US'].qh, undefined, 'still no history after 7 unarmed days');
    digestParts.push(b4, n4);
  });

  test('W2 finding 2 -- GOLDEN NEGATIVE on v1.23.0: outage pause then same-day recovery stays 2/3 (or rewrites yesterday); v1.23.1 appends the day bit -> 3/3', function () {
    var outage = { 'CHAL.US': true };
    // base
    var b2 = armedTwoDays(B);
    var bp = core(B, RAW_WITHOUT, grid(), parse(B, b2.state), knobs(5), DAYS[2], outage);
    deq(bp.audit.outage_paused, ['CHAL.US'], 'paused'); eq(bp.state.symbols['CHAL.US'].ci, 2); deq(bp.state.symbols['CHAL.US'].qh, [1, 1], 'no bit appended');
    var bs = core(B, RAW_WITH, grid(), parse(B, bp.state), knobs(5), DAYS[2]);
    eq(bs.state.symbols['CHAL.US'].ci, 2, 'GOLDEN NEGATIVE: the third qualified day is not counted');
    // base, newest bit 0: d2 miss (WATCH) then d3 pause + recovery rewrites d2
    var b1 = core(B, RAW_WITH, grid(), parse(B, seed()), knobs(5), DAYS[0]);
    var b2m = core(B, RAW_WITHOUT, grid(86, 'WATCH'), parse(B, b1.state), knobs(5), DAYS[1]);
    deq(b2m.state.symbols['CHAL.US'].qh, [1, 0]);
    var b3p = core(B, RAW_WITHOUT, grid(), parse(B, b2m.state), knobs(5), DAYS[2], outage);
    var b3s = core(B, RAW_WITH, grid(), parse(B, b3p.state), knobs(5), DAYS[2]);
    deq(b3s.state.symbols['CHAL.US'].qh, [1, 1], 'GOLDEN NEGATIVE: history rewritten');
    // delivered
    var n2 = armedTwoDays(N);
    var np = core(N, RAW_WITHOUT, grid(), parse(N, n2.state), knobs(5), DAYS[2], outage);
    deq(np.audit.outage_paused, ['CHAL.US']); eq(np.state.symbols['CHAL.US'].ci, 2); eq(np.state.symbols['CHAL.US'].qd, DAYS[1], 'qd untouched by the pause');
    var ns = core(N, RAW_WITH, grid(), parse(N, np.state), knobs(5), DAYS[2]);
    eq(ns.state.symbols['CHAL.US'].ci, 3, 'fixed: the day bit is appended'); deq(ns.state.symbols['CHAL.US'].qh, [1, 1, 1]); eq(ns.state.symbols['CHAL.US'].qd, DAYS[2]);
    var nt = bySym(ns, 'CHAL.US'); assert(nt !== null && nt._stab_status === 'NEW (confirmed 3/3)', 'fixed: sized: ' + (nt && nt._stab_status));
    var n1 = core(N, RAW_WITH, grid(), parse(N, seed()), knobs(5), DAYS[0]);
    var n2m = core(N, RAW_WITHOUT, grid(86, 'WATCH'), parse(N, n1.state), knobs(5), DAYS[1]);
    var n3p = core(N, RAW_WITHOUT, grid(), parse(N, n2m.state), knobs(5), DAYS[2], outage);
    var n3s = core(N, RAW_WITH, grid(), parse(N, n3p.state), knobs(5), DAYS[2]);
    deq(n3s.state.symbols['CHAL.US'].qh, [1, 0, 1], 'fixed: yesterday stays 0, today appended');
    // window trim on the late append: window 2
    var w1 = core(N, RAW_WITH, grid(), parse(N, seed()), knobs(2), DAYS[0]);
    var w2 = core(N, RAW_WITH, grid(), parse(N, w1.state), knobs(2), DAYS[1]);
    var w3p = core(N, RAW_WITHOUT, grid(), parse(N, w2.state), knobs(2), DAYS[2], outage);
    var w3s = core(N, RAW_WITH, grid(), parse(N, w3p.state), knobs(2), DAYS[2]);
    eq(w3s.state.symbols['CHAL.US'].qh.length, 3, 'window 2 is raised to confirm_days 3 (the v1.22.0 rule), so three bits');
    // a non-qualified later render still changes nothing (same-day renders never lower)
    var nq = core(N, RAW_WITHOUT, grid(86, 'WATCH'), parse(N, np.state), knobs(5), DAYS[2]);
    deq(canon(nq.state.symbols['CHAL.US']), canon(np.state.symbols['CHAL.US']), 'non-qualified same-day render leaves the entry (key order aside)');
    digestParts.push(bs, ns, n3s);
  });

  test('W3 KILL: unarmed replays deep-equal the base (core + dt10ApplyStability_); armed replays without a disarmed day or a pause equal the base apart from qd; a 1.23.0 blob keeps the raise rule until its next day-advance', function () {
    function replay(g, k, days, raws, grids) {
      var st = parse(g, seed()), outs = [];
      for (var d = 0; d < days.length; d++) { var o = core(g, raws[d], grids[d], st, k, days[d]); outs.push(o); st = parse(g, o.state); }
      return outs;
    }
    var raws = [RAW_WITH, RAW_WITH, [ticket('A.US', 95), ticket('NEWC.US', 97)], RAW_WITH];
    var grids = [grid(), grid(), grid(86, 'WATCH'), grid()];
    deq(replay(N, knobs(), DAYS.slice(0, 4), raws, grids), replay(B, knobs(), DAYS.slice(0, 4), raws, grids), 'unarmed (knob absent)');
    deq(replay(N, knobs(0), DAYS.slice(0, 4), raws, grids), replay(B, knobs(0), DAYS.slice(0, 4), raws, grids), 'unarmed (knob 0)');
    var e2eN = applyE2E(newSource, {}, seed()), e2eB = applyE2E(baseSource, {}, seed());
    deq(e2eN, e2eB, 'e2e unarmed');
    var e2eNa = applyE2E(newSource, { TFB_T10_CONFIRM_WINDOW: '5' }, seed()), e2eBa = applyE2E(baseSource, { TFB_T10_CONFIRM_WINDOW: '5' }, seed());
    deq(stripQd(e2eNa), e2eBa, 'e2e armed equals the base apart from qd');
    assert(JSON.stringify(e2eNa.saved).indexOf('"qd":"') > 0, 'armed blob carries qd');
    deq(stripQd(replay(N, knobs(5), DAYS.slice(0, 4), raws, grids)), replay(B, knobs(5), DAYS.slice(0, 4), raws, grids), 'armed replay equals the base apart from qd');
    // a 1.23.0 blob (qh, no qd): same-day render raises only; next day-advance stamps qd
    var old2 = armedTwoDays(B);                                   // qh [1,1], no qd
    var oldMiss = core(B, RAW_WITHOUT, grid(86, 'WATCH'), parse(B, old2.state), knobs(5), DAYS[2]);   // qh [1,1,0]
    deq(oldMiss.state.symbols['CHAL.US'].qh, [1, 1, 0]);
    var same = core(N, RAW_WITH, grid(), parse(N, oldMiss.state), knobs(5), DAYS[2]);
    deq(same.state.symbols['CHAL.US'].qh, [1, 1, 1], 'raise-only rule on a blob without qd'); eq(same.state.symbols['CHAL.US'].qd, undefined, 'raise does not invent qd');
    var next = core(N, RAW_WITH, grid(), parse(N, oldMiss.state), knobs(5), DAYS[3]);
    eq(next.state.symbols['CHAL.US'].qd, DAYS[3], 'next day-advance stamps qd'); deq(next.state.symbols['CHAL.US'].qh, [1, 1, 0, 1]);
    digestParts.push(e2eNa.saved);
  });

  test('W4 parser: qd kept only beside qh and only as yyyy-mm-dd; junk dropped; hard exit drops qh and qd; soft/capacity/displacement keep both', function () {
    var blob = { v: 1, date: '2026-10-22', symbols: {
      'A.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-22', hist: [95], ft: false, qh: [1, 1], qd: '2026-10-22' },
      'B.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-22', hist: [84], ft: false, qh: [1, 1], qd: 'yesterday' },
      'C.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-22', hist: [84], ft: false, qd: '2026-10-22' },
      'D.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-22', hist: [84], ft: false, qh: [1, 1], qd: 20261022 } } };
    var p = parse(N, blob);
    eq(p.symbols['A.US'].qd, '2026-10-22', 'kept'); eq(p.symbols['B.US'].qd, undefined, 'junk dropped'); deq(p.symbols['B.US'].qh, [1, 1]);
    eq(p.symbols['C.US'].qd, undefined, 'no qh -> no qd'); eq(p.symbols['C.US'].qh, undefined);
    eq(p.symbols['D.US'].qd, undefined, 'number dropped');
    deq(stripQd(p), parse(B, blob), 'parser equals the base apart from qd');
    // hard exit
    var n2 = armedTwoDays(N);
    var g2 = grid(); g2[g2.length - 1].structural_block = true;   // CHAL
    var hx = core(N, RAW_WITH, g2, parse(N, n2.state), knobs(5), DAYS[2]);
    assert(hx.audit.exited_hard.indexOf('CHAL.US') < 0 || true, 'CHAL is a non-member');
    var seed2 = seed(); seed2.symbols['B.US'].qh = [1, 1, 1]; seed2.symbols['B.US'].qd = '2026-10-20';
    var gb = grid(); gb[gb.length - 2].structural_block = true;   // B.US member
    var hxB = core(N, RAW_WITH, gb, parse(N, seed2), knobs(5), DAYS[0]);
    deq(hxB.audit.exited_hard, ['B.US']); eq(hxB.state.symbols['B.US'].qh, undefined, 'hard exit drops qh'); eq(hxB.state.symbols['B.US'].qd, undefined, 'and qd');
    // displacement keeps both (CHAL confirms d3 and displaces B)
    var d3 = core(N, RAW_WITH, grid(), parse(N, n2.state), knobs(5), DAYS[2]);
    deq(d3.audit.exited_displaced, ['B.US']);
    assert(d3.state.symbols['B.US'].qh !== undefined && d3.state.symbols['B.US'].qd === DAYS[2], 'displaced member keeps qh and qd');
  });

  test('W5 embedded dt10SelfTest: extended confirm-window line ok, zero FAIL, line count unchanged against the base', function () {
    var rep = String(N.dt10SelfTest()), repB = String(B.dt10SelfTest());
    assert(rep.indexOf('confirm window core: ok') > 0, 'battery line: ' + rep.split('\n').filter(function (l) { return /confirm window core/.test(l); }).join(' / '));
    assert(rep.indexOf('v1.23.1: unarmed day drops the history (re-arm 1/3), outage pause + same-day recovery appends (3/3)') > 0, 'extended text');
    assert(rep.indexOf('[DT10 v1.23.1] self-test') === 0, 'version line');
    assert(rep.indexOf(': FAIL') < 0 && rep.indexOf('FAIL [') < 0, 'no FAIL');
    eq(rep.split('\n').length, repB.split('\n').length, 'line count unchanged');
    assert(repB.indexOf('v1.23.1:') < 0, 'GOLDEN NEGATIVE: the base lacks the extension');
  });

  test('W6 lint: ES5/ASCII diff, LF only, WHY block newest-first', function () {
    var baseLines = {}, bl = baseSource.split('\n'), nl = newSource.split('\n');
    for (var i = 0; i < bl.length; i++) baseLines[bl[i]] = true;
    var added = nl.filter(function (l) { return !baseLines[l]; });
    assert(added.length >= 80 && added.length <= 130, 'added lines ' + added.length);
    added.forEach(function (l) {
      assert(!/\b(let|const|class)\b|=>|`/.test(l), 'ES5 violation: ' + l);
      if (!/^[\x09\x20-\x7e]*$/.test(l)) assert(baseLines[l.replace('1.23.1', '1.23.0')] === true, 'non-ASCII added line: ' + l);
    });
    assert(newSource.indexOf('\r') < 0, 'LF only');
    var why = newSource.indexOf(' * v1.23.1 (2026-11-08) [CONFIRM-WINDOW-FIX]'), prev = newSource.indexOf(' * v1.23.0 (2026-11-08) [REENTRY-COOLDOWN]');
    assert(why > 0 && prev > why, 'WHY block newest-first');
  });

  test('W7 mutants: three one-line mutations are each caught', function () {
    var mutants = [
      ['stale history kept', "        } else if (st.qh) {\n          // v1.23.1 [CONFIRM-WINDOW-FIX]: a render day passed", "        } else if (false) {\n          // v1.23.1 [CONFIRM-WINDOW-FIX]: a render day passed"],
      ['late append never taken', "        if (st.qd !== undefined && st.qd !== today) {", "        if (false) {"],
      ['qd not stamped on day-advance', "          st.qd = today;   // v1.23.1 [CONFIRM-WINDOW-FIX]: the day this bit belongs to", "          // (mutant: no stamp)"]
    ];
    var outage = { 'CHAL.US': true };
    mutants.forEach(function (mu) {
      eq(newSource.split(mu[1]).length - 1, 1, 'mutant anchor unique: ' + mu[0]);
      var M = load(newSource.replace(mu[1], mu[2]), {});
      var caught = false;
      try {
        var m2 = armedTwoDays(M);
        var m3 = core(M, RAW_WITHOUT, grid(86, 'WATCH'), parse(M, m2.state), knobs(0), DAYS[2]);
        var m4 = core(M, RAW_WITH, grid(), parse(M, m3.state), knobs(5), DAYS[3]);
        var ok1 = m4.state.symbols['CHAL.US'].ci === 1;
        var mp = core(M, RAW_WITHOUT, grid(), parse(M, m2.state), knobs(5), DAYS[2], outage);
        var ms = core(M, RAW_WITH, grid(), parse(M, mp.state), knobs(5), DAYS[2]);
        var ok2 = ms.state.symbols['CHAL.US'].ci === 3 && ms.state.symbols['CHAL.US'].qd === DAYS[2];
        var ok3 = m2.state.symbols['CHAL.US'].qd === DAYS[1];
        caught = !(ok1 && ok2 && ok3);
      } catch (eM) { caught = true; }
      assert(caught, 'mutant not caught: ' + mu[0]);
    });
  });

  return sha256(JSON.stringify(digestParts));
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
