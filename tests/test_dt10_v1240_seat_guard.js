/* SEAT-GUARD (16_Decision_Top10.gs v1.24.0) -- fresh seats stop churning.
 * (a) [HARD-EXIT-CONFIRM] TFB_T10_HARD_EXIT_CONFIRM=on: a data-side hard exit
 *     (structural block; strict DO_NOT_INVEST / BLOCKED) needs a second
 *     day-key; sold (cool-down) and SELL/AVOID/EXIT stay immediate.
 * (b) [SEAT-GUARD] TFB_T10_SEAT_GUARD_DAYS=N: displacement takes the weakest
 *     non-qualifying seat, then the weakest unconfirmed fast-track seat (no
 *     score test), else a confirmed seat past its first N day-keys (score
 *     test); a fresh confirmed seat is never taken.
 * (c) [COOLDOWN-NOTE-FIX] a seated name that is also in the raw list is named
 *     once in the cool-down token / audit.
 *
 * Dual-tree node harness over the REAL file; the base is the v1.23.1 file
 * rebuilt by reversing the enumerated v1.24.0 edits (SHA-256 pinned).
 *   G0 base oracle; versions in lockstep; both compile; census +2 functions
 *      +3 vars, 0 removed; 19 hunks reverse to the pinned base.
 *   G1 GOLDEN NEGATIVE (the 8 Nov 07:33 pattern) on v1.23.1: a confirmed
 *      challenger displaces the fresh confirmed seat while a non-qualifying
 *      seat with a high smoothed score keeps its seat. Guard 3: the
 *      non-qualifying seat goes; a qualifying board with a fresh seat keeps
 *      every seat (challenger waits 3/3); past the guard the score test
 *      applies; an unconfirmed fast-track seat goes before a confirmed one.
 *   G2 GOLDEN NEGATIVE on v1.23.1: one blocked day-key is an exit. Armed: the
 *      seat is kept ('GRACE (c/n missed) - hard-exit pending', no sizing even
 *      with a raw ticket), a same-day re-run keeps it, the next blocked
 *      day-key exits, a clean day clears hx; strict DO_NOT_INVEST is held;
 *      SELL and a sold name stay immediate; an outage pause is untouched.
 *   G3 GOLDEN NEGATIVE on v1.23.1: a sold seat also in the raw list is named
 *      twice; v1.24.0 names it once (member true).
 *   G4 KILL: knobs absent / 0 / false and properties absent / off / junk /
 *      faulted deep-equal the base through the core and dt10ApplyStability_
 *      (note, tickets, audit, knobs, saved blob).
 *   G5 service reads and the knobs object (set only when armed).
 *   G6 parser: hx kept only as yyyy-mm-dd; a blob without hx round-trips.
 *   G7 embedded dt10SelfTest: four new lines ok, the extended cool-down line,
 *      zero FAIL, base + 4 lines, the base lacks them.
 *   G8 lint: ES5/ASCII diff, LF only, WHY block newest-first.
 *   G9 mutants: four one-line mutations are each caught.
 *
 * The whole battery runs three times and the three digests must match.
 * Usage: node tests/test_dt10_v1240_seat_guard.js [new.gs] [base.gs]
 */
'use strict';
var fs = require('fs'), vm = require('vm'), path = require('path'), crypto = require('crypto');
var ROOT = path.join(__dirname, '..');
var NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '16_Decision_Top10.gs');
var BASE_PATH = process.argv[3] || '';
var history = require(path.join(ROOT, 'tests', 'js', 'dt10_cash_utc_history.js'));
var EDITS = require(path.join(ROOT, 'tests', 'js', 'dt10_edits_v1240.json'));
var HX = 'TFB_T10_HARD_EXIT_CONFIRM', SG = 'TFB_T10_SEAT_GUARD_DAYS';

var newSource = fs.readFileSync(NEW_PATH, 'utf8');
function sha256(s) { return crypto.createHash('sha256').update(s).digest('hex'); }
var baseSource;
try {
  baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : history.v1231Source(newSource);
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
  if (ja !== jb) {
    var i = 0; while (i < ja.length && ja[i] === jb[i]) i++;
    throw new Error((m || 'deep-equal') + ' differs at ' + i + ': ...' + ja.slice(Math.max(0, i - 80), i + 120) + ' | ...' + jb.slice(Math.max(0, i - 80), i + 120));
  }
}
function plain(v) { return JSON.parse(JSON.stringify(v)); }
function names(src, re) { var out = {}, m; while ((m = re.exec(src)) !== null) out[m[1]] = true; return out; }

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

/* ------------------------------------------------------------ fixtures */
function knobs(extra) {
  var k = { enabled: true, confirm_days: 3, exit_days: 3, rank_buffer: 15, smooth_days: 5, hard_strict: true,
            confirm_basis: 'qualified', confirm_window: 5 };
  for (var x in (extra || {})) if (extra.hasOwnProperty(x)) k[x] = extra[x];
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
function core(g, raw, grd, st, k, limit, day, outage, cd) {
  var o = g.dt10StabCore_(plain(raw), plain(grd), st, plain(k), limit, day, outage, cd);
  return plain({ tickets: o.tickets, state: o.state, audit: o.audit, note: o.note });
}
function parse(g, state) { return g.dt10StabParseState_(plain(state)); }
function bySym(out, s) { for (var i = 0; i < out.tickets.length; i++) if (out.tickets[i].symbol === s) return out.tickets[i]; return null; }
var D1 = '2026-10-21', D2 = '2026-10-22';
/* seat-guard board (limit 3): A old seat; G seated, WATCH today (non-qualifying), smoothed 99;
 * N confirmed + seated since nSince; C challenger at 2 that today's raw confirms. */
function sgSeed(nSince, withFt) {
  var s = { v: 1, date: '2026-10-20', symbols: {
    'A.US': { ci: 3, co: 0, member: true, since: '2026-10-01', ls: '2026-10-20', hist: [95, 95], ft: false },
    'G.US': { ci: 3, co: 1, member: true, since: '2026-10-01', ls: '2026-10-20', hist: [99, 99], ft: false },
    'N.US': { ci: 3, co: 0, member: true, since: nSince, ls: '2026-10-20', hist: [80], ft: false },
    'C.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-20', hist: [88, 88], ft: false } } };
  if (withFt) {
    delete s.symbols['A.US'];
    s.symbols['FT.US'] = { ci: 1, co: 0, member: true, since: '2026-10-20', ls: '2026-10-20', hist: [97], ft: true };
  }
  return s;
}
function sgGrid(gVerdict, gScore, withFt) {
  return [cand(withFt ? 'FT.US' : 'A.US', withFt ? 97 : 95), cand('C.US', 88), cand('N.US', 80), cand('G.US', gScore, gVerdict)];
}
var SG_RAW = [ticket('A.US', 95), ticket('C.US', 88), ticket('N.US', 80)];
var SG_RAW_FT = [ticket('FT.US', 97), ticket('C.US', 88), ticket('N.US', 80)];
/* hard-exit board (limit 2): A + B seated; B's audited row varies */
function hxSeed() {
  return { v: 1, date: '2026-10-20', symbols: {
    'A.US': { ci: 3, co: 0, member: true, since: '2026-10-15', ls: '2026-10-20', hist: [95], ft: false },
    'B.US': { ci: 3, co: 0, member: true, since: '2026-10-15', ls: '2026-10-20', hist: [84], ft: false } } };
}
function hxGrid(bVerdict, bExtra) {
  var gr = [cand('A.US', 95)];
  for (var i = 1; i <= 6; i++) gr.push(cand('F' + i + '.US', 94 - i));
  gr.push(cand('B.US', 84, bVerdict, bExtra));
  return gr;
}
var HX_RAW = [ticket('A.US', 95)];
var BLOCK = { structural_block: true };
function applyE2E(source, props, seedObj, payloadIn) {
  var g = load(source, props);
  g.__store.DT10_STAB_STATE_V1 = JSON.stringify(seedObj);
  var payload = payloadIn ? plain(payloadIn) : { selected: plain(SG_RAW), candidates_rows: sgGrid('WATCH', 70), meta: {} };
  var panel = { 'T10: Max Selected': 3, 'T10: Stability Enabled': 'Yes', 'T10: Stability Confirm Days': 3,
                'T10: Stability Exit Days': 3, 'T10: Stability Rank Buffer': 15, 'T10: Stability Smooth Days': 5 };
  var out = g.dt10ApplyStability_(payload, panel, null);
  return { note: out.note, selected: plain(payload.selected), audit: plain(payload.meta.stability.audit),
           knobs: plain(payload.meta.stability.knobs), saved: JSON.parse(g.__store.DT10_STAB_STATE_V1) };
}

/* ------------------------------------------------------------ battery */
function battery() {
  var N = load(newSource, {}), B = load(baseSource, {});
  var digestParts = [];

  test('G0 base oracle is the reviewed v1.23.1 file (SHA-256 pinned); versions in lockstep; both compile; census +2 functions +3 vars, 0 removed; 19 hunks reverse to the base', function () {
    eq(sha256(baseSource), history.V1231_SHA256, 'base SHA-256');
    eq(B.DT10_VERSION, '1.23.1'); eq(N.DT10_VERSION, '1.24.0');
    assert(/^ \* Version: 1\.24\.0 /.test(newSource.split('\n')[3]), 'header line 4 lockstep');
    new vm.Script(newSource, { filename: 'new.gs' }); new vm.Script(baseSource, { filename: 'base.gs' });
    var fb = names(baseSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), fn = names(newSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    deq(Object.keys(fb).filter(function (k) { return !fn[k]; }), [], 'functions removed');
    deq(Object.keys(fn).filter(function (k) { return !fb[k]; }).sort(), ['dt10StabHardExitConfirm_', 'dt10StabSeatGuardDays_'], 'functions added');
    var vb = names(baseSource, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(newSource, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(function (k) { return !vn[k]; }), [], 'vars removed');
    deq(Object.keys(vn).filter(function (k) { return !vb[k]; }).sort(), ['DT10_T10_HARD_EXIT_CONFIRM_PROP', 'DT10_T10_SEAT_GUARD_MAX', 'DT10_T10_SEAT_GUARD_PROP'], 'vars added');
    eq(N.DT10_T10_HARD_EXIT_CONFIRM_PROP, HX); eq(N.DT10_T10_SEAT_GUARD_PROP, SG); eq(N.DT10_T10_SEAT_GUARD_MAX, 10);
    eq(EDITS.length, 19, 'hunk count');
    for (var e = 0; e < EDITS.length; e++) eq(newSource.split(EDITS[e][0]).length - 1, 1, 'each current is unique: ' + EDITS[e][0].split('\n')[0]);
    eq(sha256(history.v1231Source(newSource)), history.V1231_SHA256, 'reversal of the delivered file');
  });

  test('G1 GOLDEN NEGATIVE on v1.23.1: the fresh confirmed seat is displaced while a non-qualifying seat keeps its seat; guard 3 takes the non-qualifying seat, never a fresh confirmed one; score test past the guard; fast-track first', function () {
    // base: the 8 Nov 07:33 pattern
    var b = core(B, SG_RAW, sgGrid('WATCH', 70), parse(B, sgSeed('2026-10-20')), knobs(), 3, D1);
    deq(b.audit.exited_displaced, ['N.US'], 'GOLDEN NEGATIVE: the base displaces the fresh seat');
    eq(b.state.symbols['G.US'].member, true, 'GOLDEN NEGATIVE: the non-qualifying seat stays');
    var bg = bySym(b, 'G.US'); assert(bg && /^GRACE /.test(bg._stab_status), 'G is a grace seat on the base');
    // delivered, guard 3
    var n = core(N, SG_RAW, sgGrid('WATCH', 70), parse(N, sgSeed('2026-10-20')), knobs({ seat_guard_days: 3 }), 3, D1);
    deq(n.audit.exited_displaced, ['G.US'], 'guard: the non-qualifying seat goes');
    eq(n.state.symbols['N.US'].member, true, 'guard: the fresh seat stays');
    var nc = bySym(n, 'C.US'); assert(nc && nc._stab_status === 'NEW (confirmed 3/3)' && nc.suggested_sar === 5000, 'C seated and sized');
    var nn = bySym(n, 'N.US'); assert(nn && /^ACTIVE \(day 2\)$/.test(nn._stab_status), 'N ACTIVE day 2: ' + (nn && nn._stab_status));
    assert(n.note.indexOf(', guard=3d took G.US(nq)') > 0, 'token: ' + n.note);
    deq(n.audit.seat_guard, { days: 3, took: ['G.US(nq)'] }, 'audit');
    assert(n.state.symbols['G.US'].qh !== undefined, 'the displaced seat keeps its window bits (v1.3.0 displacement exit)');
    // a fully qualifying board with a fresh seat: nothing is taken, C waits at 3/3
    var q = core(N, SG_RAW, sgGrid('INVEST', 90), parse(N, sgSeed('2026-10-20')), knobs({ seat_guard_days: 3 }), 3, D1);
    deq(q.audit.exited_displaced, [], 'no seat taken'); eq(q.state.symbols['C.US'].member, false);
    assert(q.note.indexOf('pending C.US(3/3)') > 0 && q.note.indexOf(', guard=3d') > 0 && q.note.indexOf('took') < 0, 'token: ' + q.note);
    var qb = core(B, SG_RAW, sgGrid('INVEST', 90), parse(B, sgSeed('2026-10-20')), knobs(), 3, D1);
    deq(qb.audit.exited_displaced, ['N.US'], 'GOLDEN NEGATIVE: the base takes the fresh seat here too');
    // past the guard: the score test takes N (88 > 80)
    var o = core(N, SG_RAW, sgGrid('INVEST', 90), parse(N, sgSeed('2026-10-01')), knobs({ seat_guard_days: 3 }), 3, D1);
    deq(o.audit.exited_displaced, ['N.US'], 'past the guard');
    assert(o.note.indexOf('took N.US') > 0 && o.note.indexOf('took N.US(') < 0, 'untagged score-test take: ' + o.note);
    // guard 25 clamps to 10 in the core; N since 10-14 (day 8) is protected, since 10-01 is not
    var c10 = core(N, SG_RAW, sgGrid('INVEST', 90), parse(N, sgSeed('2026-10-14')), knobs({ seat_guard_days: 25 }), 3, D1);
    deq(c10.audit.exited_displaced, [], 'day 8 protected under the clamped guard 10'); eq(c10.audit.seat_guard.days, 10);
    // an unconfirmed fast-track seat goes before any confirmed seat
    var f = core(N, SG_RAW_FT, sgGrid('INVEST', 90, true), parse(N, sgSeed('2026-10-20', true)), knobs({ seat_guard_days: 3 }), 3, D1);
    deq(f.audit.exited_displaced, ['FT.US'], 'fast-track first'); assert(f.note.indexOf('took FT.US(ft)') > 0, f.note);
    var fb = core(B, SG_RAW_FT, sgGrid('INVEST', 90, true), parse(B, sgSeed('2026-10-20', true)), knobs(), 3, D1);
    deq(fb.audit.exited_displaced, ['N.US'], 'GOLDEN NEGATIVE: the base keeps the fast-track seat and takes N');
    // an outage-paused non-qualifying seat is not taken without the score test
    var p = core(N, [ticket('A.US', 95), ticket('C.US', 88), ticket('N.US', 80)], sgGrid('WATCH', 70),
                 parse(N, sgSeed('2026-10-20')), knobs({ seat_guard_days: 3 }), 3, D1, { 'G.US': true });
    deq(p.audit.outage_paused, ['G.US'], 'G paused'); deq(p.audit.exited_displaced, [], 'paused G is not taken without the score test, N is protected, A (95) beats C (88)');
    digestParts.push(b, n, q, o, f, p);
  });

  test('G2 GOLDEN NEGATIVE on v1.23.1: one blocked day-key is an exit; armed: kept one day-key with no sizing, exits on the next blocked day-key, clears when clean; SELL and a sold name stay immediate', function () {
    var kOn = knobs({ hard_exit_confirm: true });
    var b = core(B, HX_RAW, hxGrid('INVEST', BLOCK), parse(B, hxSeed()), knobs(), 2, D1);
    deq(b.audit.exited_hard, ['B.US'], 'GOLDEN NEGATIVE: the base exits at once');
    var bd = core(B, HX_RAW, hxGrid('DO_NOT_INVEST'), parse(B, hxSeed()), knobs(), 2, D1);
    deq(bd.audit.exited_hard, ['B.US'], 'GOLDEN NEGATIVE: strict DO_NOT_INVEST exits at once on the base');
    var n1 = core(N, HX_RAW, hxGrid('INVEST', BLOCK), parse(N, hxSeed()), kOn, 2, D1);
    deq(n1.audit.exited_hard, [], 'kept'); deq(n1.audit.hard_exit_held, ['B.US'], 'audit');
    var t1 = bySym(n1, 'B.US');
    assert(t1 && t1._stab_status === 'GRACE (0/3 missed) - hard-exit pending', 'label: ' + (t1 && t1._stab_status));
    assert(t1._grace_hold === true && t1.suggested_sar === '—' && t1.suggested_shares === '—', 'no sizing');
    eq(n1.state.symbols['B.US'].hx, D1, 'hx stamped'); assert(n1.note.indexOf(', hxconfirm held 1 B.US') > 0, n1.note);
    var n1b = core(N, HX_RAW, hxGrid('INVEST', BLOCK), parse(N, n1.state), kOn, 2, D1);
    deq(n1b.audit.exited_hard, [], 'same-day re-run keeps'); eq(n1b.state.symbols['B.US'].hx, D1);
    var n2 = core(N, HX_RAW, hxGrid('INVEST', BLOCK), parse(N, n1.state), kOn, 2, D2);
    deq(n2.audit.exited_hard, ['B.US'], 'next blocked day-key exits'); eq(n2.state.symbols['B.US'].hx, undefined, 'hx gone');
    assert(n2.note.indexOf(', hxconfirm') > 0 && n2.note.indexOf('hxconfirm held') < 0, n2.note);
    var n2c = core(N, HX_RAW, hxGrid('INVEST'), parse(N, n1.state), kOn, 2, D2);
    deq(n2c.audit.exited_hard, [], 'clean day keeps'); eq(n2c.state.symbols['B.US'].hx, undefined, 'clean day clears hx');
    eq(n2c.state.symbols['B.US'].member, true);
    // a blocked row that still carries a raw ticket: sizing suspended
    var nr = core(N, [ticket('A.US', 95), ticket('B.US', 84)], hxGrid('INVEST', BLOCK), parse(N, hxSeed()), kOn, 2, D1);
    var tr = bySym(nr, 'B.US'); assert(tr && /hard-exit pending$/.test(tr._stab_status) && tr.suggested_sar === '—' && tr._grace_hold === true, 'raw ticket suspended');
    // strict DO_NOT_INVEST and BLOCKED verdicts are held
    var nd = core(N, HX_RAW, hxGrid('DO_NOT_INVEST'), parse(N, hxSeed()), kOn, 2, D1);
    deq(nd.audit.hard_exit_held, ['B.US'], 'DO_NOT_INVEST held');
    var nbk = core(N, HX_RAW, hxGrid('BLOCKED'), parse(N, hxSeed()), kOn, 2, D1);
    deq(nbk.audit.hard_exit_held, ['B.US'], 'BLOCKED held');
    // SELL / AVOID stay immediate; a sold name stays immediate
    ['SELL', 'AVOID', 'EXIT_NOW'].forEach(function (v) {
      var ns = core(N, HX_RAW, hxGrid(v), parse(N, hxSeed()), kOn, 2, D1);
      deq(ns.audit.exited_hard, ['B.US'], v + ' immediate'); deq(ns.audit.hard_exit_held, [], v + ' not held');
    });
    var cd = { 'B.US': { days: 1, date: '2026-10-20' } };
    var nsold = core(N, HX_RAW, hxGrid('INVEST'), parse(N, hxSeed()), knobs({ hard_exit_confirm: true, cooldown_days: 10 }), 2, D1, undefined, cd);
    deq(nsold.audit.exited_hard, ['B.US'], 'sold immediate'); deq(nsold.audit.hard_exit_held, [], 'sold not held');
    // an outage-paused member is untouched (no hx, no exit)
    var np = core(N, HX_RAW, hxGrid('INVEST', BLOCK), parse(N, hxSeed()), kOn, 2, D1, { 'B.US': true });
    deq(np.audit.outage_paused, ['B.US']); deq(np.audit.exited_hard, []); eq(np.state.symbols['B.US'].hx, undefined, 'pause writes no hx');
    // disarming drops a pending mark on the next render
    var nd2 = core(N, HX_RAW, hxGrid('INVEST', BLOCK), parse(N, n1.state), knobs(), 2, D1);
    deq(nd2.audit.exited_hard, ['B.US'], 'disarmed: the v1.23.1 exit'); eq(nd2.state.symbols['B.US'].hx, undefined);
    digestParts.push(b, n1, n2, n2c, nr, nsold, np);
  });

  test('G3 GOLDEN NEGATIVE on v1.23.1: a sold seat also in the raw list is named twice; v1.24.0 names it once (member true)', function () {
    var cd = { 'B.US': { days: 1, date: '2026-10-20' } };
    var raw = [ticket('A.US', 95), ticket('B.US', 84)];
    var b = core(B, raw, hxGrid('INVEST'), parse(B, hxSeed()), knobs({ cooldown_days: 10 }), 2, D1, undefined, cd);
    eq(b.audit.cooldown_held.length, 2, 'GOLDEN NEGATIVE: two entries');
    assert(b.note.indexOf('held 2 B.US(sold 1d ago) B.US(sold 1d ago)') > 0, 'GOLDEN NEGATIVE token: ' + b.note);
    var n = core(N, raw, hxGrid('INVEST'), parse(N, hxSeed()), knobs({ cooldown_days: 10 }), 2, D1, undefined, cd);
    deq(n.audit.cooldown_held, [{ symbol: 'B.US', days: 1, date: '2026-10-20', member: true }], 'one entry, member');
    assert(n.note.indexOf('held 1 B.US(sold 1d ago)') > 0, n.note);
    // everything else in the run is the base's
    var strip = function (o) { var c = plain(o); delete c.audit.cooldown_held; c.note = c.note.replace(/ held \d+ .*$/, ''); return c; };
    deq(strip(n), strip(b), 'only the token and the audit list differ');
    digestParts.push(n);
  });

  test('G4 KILL: knobs absent / 0 / false and properties absent / off / junk / faulted deep-equal the base (core + dt10ApplyStability_)', function () {
    function replay(g, k) {
      var outs = [];
      outs.push(core(g, SG_RAW, sgGrid('WATCH', 70), parse(g, sgSeed('2026-10-20')), k, 3, D1));
      outs.push(core(g, SG_RAW_FT, sgGrid('INVEST', 90, true), parse(g, sgSeed('2026-10-20', true)), k, 3, D1));
      var s1 = core(g, HX_RAW, hxGrid('INVEST', BLOCK), parse(g, hxSeed()), k, 2, D1); outs.push(s1);
      outs.push(core(g, HX_RAW, hxGrid('DO_NOT_INVEST'), parse(g, s1.state), k, 2, D2));
      return outs;
    }
    var base = replay(B, knobs());
    deq(replay(N, knobs()), base, 'knobs absent');
    deq(replay(N, knobs({ seat_guard_days: 0, hard_exit_confirm: false })), base, 'knobs 0 / false');
    deq(replay(N, knobs({ seat_guard_days: 'junk', hard_exit_confirm: 'on' })), base, 'junk knob values (the core reads only true / an integer)');
    var props = [{}, { [HX]: 'off', [SG]: '0' }, { [HX]: '', [SG]: '' }, { [HX]: 'yes', [SG]: 'abc' }, { [HX]: '1', [SG]: '-3' }];
    var eB = applyE2E(baseSource, {}, sgSeed('2026-10-20'));
    props.forEach(function (pr) {
      deq(applyE2E(newSource, pr, sgSeed('2026-10-20')), eB, 'e2e ' + JSON.stringify(pr));
    });
    var gF = load(newSource, {}, { propsThrow: true }), gBF = load(baseSource, {}, { propsThrow: true });
    eq(gF.dt10StabHardExitConfirm_(), false, 'fault -> off'); eq(gF.dt10StabSeatGuardDays_(), 0, 'fault -> 0');
    eq(typeof gBF.dt10StabSeatGuardDays_, 'undefined', 'the base has no reader');
    digestParts.push(base, eB);
  });

  test('G5 service reads; the knobs object carries the new keys only when armed', function () {
    var cases = [[' On ', true], ['ON', true], ['on', true], ['off', false], ['', false], ['yes', false], ['1', false], ['enforce', false]];
    cases.forEach(function (c) { eq(load(newSource, { [HX]: c[0] }).dt10StabHardExitConfirm_(), c[1], 'HX ' + JSON.stringify(c[0])); });
    eq(load(newSource, {}).dt10StabHardExitConfirm_(), false, 'HX absent');
    var sg = [['3', 3], [' 5 ', 5], ['10', 10], ['15', 10], ['0', 0], ['-1', 0], ['abc', 0], ['', 0], ['2.5', 0]];
    sg.forEach(function (c) { eq(load(newSource, { [SG]: c[0] }).dt10StabSeatGuardDays_(), c[1], 'SG ' + JSON.stringify(c[0])); });
    var armed = applyE2E(newSource, { [HX]: 'on', [SG]: '3' }, sgSeed('2026-10-20'));
    eq(armed.knobs.hard_exit_confirm, true); eq(armed.knobs.seat_guard_days, 3);
    deq(armed.audit.exited_displaced, ['G.US'], 'armed e2e: the guard acts');
    assert(armed.note.indexOf(', hxconfirm') > 0 && armed.note.indexOf(', guard=3d took G.US(nq)') > 0, armed.note);
    var off = applyE2E(newSource, {}, sgSeed('2026-10-20'));
    assert(!('hard_exit_confirm' in off.knobs) && !('seat_guard_days' in off.knobs), 'unarmed knobs carry no new key');
    digestParts.push(armed);
  });

  test('G6 parser: hx kept only as yyyy-mm-dd; junk dropped; a blob without hx round-trips byte for byte', function () {
    var blob = { v: 1, date: '2026-10-22', symbols: {
      'A.US': { ci: 3, co: 0, member: true, since: '2026-10-01', ls: '2026-10-22', hist: [95], ft: false, hx: '2026-10-22' },
      'B.US': { ci: 3, co: 0, member: true, since: '2026-10-01', ls: '2026-10-22', hist: [84], ft: false, hx: 'today' },
      'C.US': { ci: 3, co: 0, member: true, since: '2026-10-01', ls: '2026-10-22', hist: [84], ft: false, hx: 20261022 } } };
    var p = parse(N, blob);
    eq(p.symbols['A.US'].hx, '2026-10-22'); eq(p.symbols['B.US'].hx, undefined); eq(p.symbols['C.US'].hx, undefined);
    var noHx = hxSeed();
    deq(parse(N, noHx), parse(B, noHx), 'blob without hx: identical parse');
    var pb = parse(B, blob);
    var pn = plain(p); delete pn.symbols['A.US'].hx;
    deq(pn, pb, 'the base parse drops hx; everything else equal');
  });

  test('G7 embedded dt10SelfTest: four new lines ok, the extended cool-down line, zero FAIL, base + 4 lines; the base lacks them', function () {
    var rep = String(N.dt10SelfTest()), repB = String(B.dt10SelfTest());
    assert(rep.indexOf('[DT10 v1.24.0] self-test') === 0, 'version line');
    ['hard-exit confirm (TFB_T10_HARD_EXIT_CONFIRM): off (off = v1.23.1 exits)', 'hard-exit confirm core: ok',
     'seat guard (TFB_T10_SEAT_GUARD_DAYS): 0 (0 = v1.23.1 displacement; 1..10 arms)', 'seat guard core: ok'].forEach(function (l) {
      assert(rep.indexOf(l) > 0, 'line: ' + l); assert(repB.indexOf(l.split(' (')[0].split(':')[0]) < 0, 'GOLDEN NEGATIVE: the base lacks ' + l);
    });
    assert(rep.indexOf('v1.24.0: a seated name in the raw list is named once)') > 0, 'extended cool-down line');
    assert(rep.indexOf(': FAIL') < 0 && rep.indexOf('FAIL [') < 0, 'no FAIL');
    eq(rep.split('\n').length, repB.split('\n').length + 4, 'four lines added');
    var armed = String(load(newSource, { [HX]: 'on', [SG]: '3' }).dt10SelfTest());
    assert(armed.indexOf('hard-exit confirm (TFB_T10_HARD_EXIT_CONFIRM): on') > 0 && armed.indexOf('seat guard (TFB_T10_SEAT_GUARD_DAYS): 3') > 0 && armed.indexOf('FAIL') < 0, 'armed lines');
  });

  test('G8 lint: ES5/ASCII diff, LF only, WHY block newest-first', function () {
    var baseLines = {}, bl = baseSource.split('\n'), nl = newSource.split('\n');
    for (var i = 0; i < bl.length; i++) baseLines[bl[i]] = true;
    var added = nl.filter(function (l) { return !baseLines[l]; });
    assert(added.length >= 250 && added.length <= 320, 'added lines ' + added.length);
    added.forEach(function (l) {
      assert(!/\b(let|const|class)\b|=>|`/.test(l), 'ES5 violation: ' + l);
      if (!/^[\x09\x20-\x7e]*$/.test(l)) assert(baseLines[l.replace('1.24.0', '1.23.1')] === true, 'non-ASCII added line: ' + l);
    });
    assert(newSource.indexOf('\r') < 0, 'LF only');
    var why = newSource.indexOf(' * v1.24.0 (2026-11-08) [SEAT-GUARD]'), prev = newSource.indexOf(' * v1.23.1 (2026-11-08) [CONFIRM-WINDOW-FIX]');
    assert(why > 0 && prev > why, 'WHY block newest-first');
  });

  test('G9 mutants: four one-line mutations are each caught', function () {
    var mutants = [
      ['safety verdict held', "    if (hxOn && hardOut(ms) && !hardImmediate(ms) &&", "    if (hxOn && hardOut(ms) &&"],
      ['grace seat needs the score test', "        return { sym: gs, force: true, tag: 'nq' };", "        return { sym: gs, force: false, tag: 'nq' };"],
      ['no tenure protection', "      if (dt10StabDays_(gst.since || today, today) + 1 <= sgDays) continue;", "      // (mutant: no tenure guard)"],
      ['cool-down named twice', "          if (cooldownHeld[cds].symbol === ms) { cooldownHeld[cds].member = true; cdSeen = true; }", "          // (mutant: no dedupe)"]
    ];
    mutants.forEach(function (mu) {
      eq(newSource.split(mu[1]).length - 1, 1, 'mutant anchor unique: ' + mu[0]);
      var M = load(newSource.replace(mu[1], mu[2]), {});
      var caught = false;
      try {
        var kOn = knobs({ hard_exit_confirm: true });
        var s = core(M, HX_RAW, hxGrid('SELL'), parse(M, hxSeed()), kOn, 2, D1);
        var ok1 = s.audit.exited_hard.length === 1;
        var g = core(M, SG_RAW, sgGrid('WATCH', 70), parse(M, sgSeed('2026-10-20')), knobs({ seat_guard_days: 3 }), 3, D1);
        var ok2 = g.audit.exited_displaced.join(',') === 'G.US';
        var q = core(M, SG_RAW, sgGrid('INVEST', 90), parse(M, sgSeed('2026-10-20')), knobs({ seat_guard_days: 3 }), 3, D1);
        var ok3 = q.audit.exited_displaced.length === 0;
        var cd = { 'B.US': { days: 1, date: '2026-10-20' } };
        var c = core(M, [ticket('A.US', 95), ticket('B.US', 84)], hxGrid('INVEST'), parse(M, hxSeed()), knobs({ cooldown_days: 10 }), 2, D1, undefined, cd);
        var ok4 = c.audit.cooldown_held.length === 1;
        caught = !(ok1 && ok2 && ok3 && ok4);
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
