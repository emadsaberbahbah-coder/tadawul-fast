/* CONFIRM-WINDOW (16_Decision_Top10.gs v1.22.0) -- entry confirmation counts the
 * qualified day-keys inside a rolling window instead of a consecutive run
 * (Script Property TFB_T10_CONFIRM_WINDOW, default absent/0/1 = the v1.21.1
 * clocks byte for byte; an integer >= 2 arms, raised to confirm_days, capped 30).
 *
 * Dual-tree node harness over the REAL file: loads the .gs source in a vm
 * context with minimal GAS shims, replays a 2-seat board through the REAL
 * dt10StabCore_ and the REAL dt10ApplyStability_ (service read -> knob), and
 * proves:
 *   K0 base oracle: the v1.21.1 file rebuilt by reversing the enumerated
 *      v1.22.0 edits (tests/js/dt10_edits_v1220.json) carries the pinned
 *      SHA-256; versions in lockstep; both files compile.
 *   K1 GOLDEN NEGATIVE on the v1.21.1 base: CHAL confirmed on days 1-2, WATCH
 *      at rank 12 on day 3 -> ci 0, day 4 -> ci 1, never seated; the base
 *      ignores an unknown confirm_window knob and writes no qh.
 *   K2 KILL absent/0/1/junk/fault: every fixture, every day deep-equals the
 *      base (tickets, state, audit, note) through the pure core AND through
 *      dt10ApplyStability_ (saved blob, payload.selected, audit, note, knobs).
 *   K3 armed (window 5): ci 1,2,2,3; day 4 'NEW (confirmed 3/3)' with the raw
 *      ticket's sizing and B displaced; the seeded consecutive count migrates
 *      to ones; ', cwin=5' and audit.confirm_window; qh in the blob; pending
 *      shows min(ci, confirm_days).
 *   K4 semantics: a window below confirm_days is raised to it; 99 clamps to
 *      30; a same day-key re-run raises today's bit and never lowers it; a
 *      blob without a window entry waits for the day-advance; a hard exit
 *      deletes qh; soft, capacity and displacement exits keep qh and set ci to
 *      its sum; an outage-paused day pushes no bit; exit clocks unchanged.
 *   K5 service read: '2'..'30' -> n, '99' -> 30, '', '0', '1', '-1', '2.5',
 *      'five', absent and a fault -> 0; the panel snapshot never carries the
 *      knob; the knobs object carries it only while armed.
 *   K6 state sanitizer: bits 1/0/'1'/2/'x' -> 1/0/1/0/0; newest 30 of 40 kept;
 *      absent or empty -> no key.
 *   K7 embedded dt10SelfTest: 'confirm window core: ok', the property line
 *      follows the property, every base line kept, zero FAIL.
 *   K8 ES5/ASCII lint of the diff; inventory: 4 functions and 2 vars added,
 *      0 removed; 19 hunks whose reversal is the reviewed v1.21.1 file; WHY
 *      block newest-first; LF only.
 *   K9 mutants: four one-line mutations of the delivered file are each
 *      caught by K2, K3, K4 or K5.
 *
 * The whole battery runs three times and the three digests must match; the
 * last line is 'PASS k/k'.
 *
 * Usage: node tests/test_dt10_v1220_confirm_window.js [new.gs] [base.gs]
 */
'use strict';
var fs = require('fs'), vm = require('vm'), path = require('path'), crypto = require('crypto');
var ROOT = path.join(__dirname, '..');
var NEW_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '16_Decision_Top10.gs');
var BASE_PATH = process.argv[3] || '';
var history = require(path.join(ROOT, 'tests', 'js', 'dt10_cash_utc_history.js'));
var EDITS = require(path.join(ROOT, 'tests', 'js', 'dt10_edits_v1220.json'));
var PROP = 'TFB_T10_CONFIRM_WINDOW';

// 2026-11-08: v1.23.0 (re-entry cool-down) is stacked on this change; this harness keeps proving
// v1.22.0 = v1.21.1 apart from the window, so it reads the v1.22.0 file rebuilt from the delivered
// one (the v1.23.0 edits reversed, SHA-256 pinned); tests/test_dt10_v1230_reentry_cooldown.js owns v1.23.0.
var newSource = history.v1220Source(fs.readFileSync(NEW_PATH, 'utf8'));
function sha256(s) { return crypto.createHash('sha256').update(s).digest('hex'); }
var baseSource;
try {
  baseSource = BASE_PATH ? fs.readFileSync(BASE_PATH, 'utf8') : history.v1211Source(newSource);
} catch (eBase) {
  console.log('FAIL K0 base oracle: ' + String(eBase && eBase.message || eBase).split('\n')[0]);
  console.log('FAIL 0/1');
  process.exit(1);
}

/* ------------------------------------------------------------ runner */
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

/* ------------------------------------------------------------ GAS shims */
function ctx(props, options) {
  options = options || {};
  var store = {}; for (var k in props) if (props.hasOwnProperty(k)) store[k] = props[k];
  var logs = [], reads = {};
  var statusRows = [];
  for (var r = 0; r < 60; r++) statusRows.push(['', '']);
  statusRows[0] = ['TFB Feed Global_Markets', 'OK | cov=99.0 | run=40145487350 | 2026-10-25 04:09:22+03:00'];
  var statusSheet = { getRange: function () { return { getValues: function () { return plain(statusRows); } }; } };
  var ssStub = { getSheetByName: function (n) { return n === '_Status' ? statusSheet : null; },
                 getRangeByName: function () { return null; } };
  var context = {
    PropertiesService: { getScriptProperties: function () {
      if (options.propsThrow) throw new Error('synthetic property outage');
      return { getProperty: function (key) { reads[key] = (reads[key] || 0) + 1; return store.hasOwnProperty(key) ? store[key] : null; },
               setProperty: function (key, v) { store[key] = String(v); },
               deleteProperty: function (key) { delete store[key]; } }; } },
    Logger: { log: function (m) { logs.push(String(m)); } },
    SpreadsheetApp: { getActiveSpreadsheet: function () { return ssStub; },
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
/* Board of 2 seats. Audited grid ranks (opportunity_score desc): NEWC 97 (1),
 * A 95 (2), F1..F12 94.5..83.5, CHAL 86 (12), B 84 (15 = buffer boundary).
 * Raw (sized top-2): d1 [A, CHAL], d2 [A, CHAL], d3 [A, NEWC] (CHAL out),
 * d4 [A, CHAL]. */
var DAYS = ['2026-10-21', '2026-10-22', '2026-10-23', '2026-10-24'];
function knobs(cw, buf) {
  var k = { enabled: true, confirm_days: 3, exit_days: 3, rank_buffer: (buf === undefined ? 15 : buf),
            smooth_days: 5, hard_strict: true, confirm_basis: 'qualified' };
  if (cw !== undefined) k.confirm_window = cw;
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
function grid(chalScore, chalVerdict, chalExtra, withoutChal, bExtra) {
  var g = [cand('NEWC.US', 97), cand('A.US', 95)];
  for (var i = 1; i <= 12; i++) g.push(cand('F' + i + '.US', 95.5 - i));
  g.push(cand('B.US', 84, undefined, bExtra));
  if (!withoutChal) g.push(cand('CHAL.US', chalScore === undefined ? 86 : chalScore, chalVerdict, chalExtra));
  return g;
}
function seed() {
  return { v: 1, date: '2026-10-20', symbols: {
    'A.US': { ci: 3, co: 0, member: true, since: '2026-10-15', ls: '2026-10-20', hist: [95], ft: false },
    'B.US': { ci: 3, co: 1, member: true, since: '2026-10-15', ls: '2026-10-20', hist: [84], ft: false } } };
}
function raws(day4Chal) {
  return [[ticket('A.US', 95), ticket('CHAL.US', 86)],
          [ticket('A.US', 95), ticket('CHAL.US', 86)],
          [ticket('A.US', 95), ticket('NEWC.US', 97)],
          day4Chal === false ? [ticket('A.US', 95), ticket('NEWC.US', 97)] : [ticket('A.US', 95), ticket('CHAL.US', 86)]];
}
function replay(g, k, seedObj, rawsArr, gridsArr, outages) {
  var st = g.dt10StabParseState_(plain(seedObj));
  var outs = [];
  for (var d = 0; d < DAYS.length; d++) {
    var o = g.dt10StabCore_(plain(rawsArr[d]), plain(gridsArr[d]), st, plain(k), 2, DAYS[d],
                            outages ? outages[d] : undefined);
    outs.push(plain({ tickets: o.tickets, state: o.state, audit: o.audit, note: o.note }));
    st = g.dt10StabParseState_(plain(o.state));
  }
  return outs;
}
var G = grid(), SOFT = grid(86, 'WATCH'), DNI = grid(86, 'DO_NOT_INVEST'), BLK = grid(86, 'WATCH', { structural_block: true }),
    HARD = grid(86, 'SELL'), OUT16 = grid(83, 'WATCH'), ABSENT = grid(undefined, undefined, undefined, true),
    BBLK = grid(86, undefined, undefined, false, { structural_block: true });
function grids(d3, d4) { return [plain(G), plain(G), plain(d3 || G), plain(d4 || G)]; }
function bySym(out, s) { for (var i = 0; i < out.tickets.length; i++) if (out.tickets[i].symbol === s) return out.tickets[i]; return null; }
function sym(out, s) { return out.state.symbols[s]; }

function scenarios(g, cw) {
  return {
    soft: replay(g, knobs(cw), seed(), raws(), grids(SOFT)),
    dni: replay(g, knobs(cw), seed(), raws(), grids(DNI)),
    soft2: replay(g, knobs(cw), seed(), raws(false), grids(SOFT, SOFT)),
    blocked: replay(g, knobs(cw), seed(), raws(), grids(BLK)),
    hard: replay(g, knobs(cw), seed(), raws(), grids(HARD)),
    out16: replay(g, knobs(cw), seed(), raws(), grids(OUT16)),
    buf0: replay(g, knobs(cw, 0), seed(), raws(), grids(SOFT)),
    absent: replay(g, knobs(cw), seed(), raws(), grids(ABSENT)),
    bblk: replay(g, knobs(cw), seed(), raws(), [plain(BBLK), plain(G), plain(G), plain(G)]),
    plain: replay(g, knobs(cw), seed(), raws(), grids())
  };
}
/* End to end through the REAL dt10ApplyStability_ (service read -> knob,
 * state load/save, epoch from the _Status feed stamp = 2026-10-25). */
function applyE2E(source, props, options) {
  var g = load(source, props, options);
  var seedObj = seed(); seedObj.date = '2026-10-24';
  seedObj.symbols['CHAL.US'] = { ci: 2, co: 0, member: false, since: '', ls: '2026-10-24', hist: [86, 86], ft: false };
  g.__store.DT10_STAB_STATE_V1 = JSON.stringify(seedObj);
  var payload = { selected: [ticket('A.US', 95), ticket('NEWC.US', 97)], candidates_rows: grid(86, 'WATCH'), meta: {} };
  var panel = { 'T10: Max Selected': 2, 'T10: Stability Enabled': 'Yes', 'T10: Stability Confirm Days': 3,
                'T10: Stability Exit Days': 3, 'T10: Stability Rank Buffer': 15, 'T10: Stability Smooth Days': 5 };
  var out = g.dt10ApplyStability_(payload, panel, null);
  return { g: g, note: out.note, selected: plain(payload.selected), audit: plain(payload.meta.stability.audit),
           knobs: plain(payload.meta.stability.knobs),
           saved: JSON.parse(g.__store.DT10_STAB_STATE_V1), reads: plain(g.__reads) };
}
function names(src, re) { var out = {}, m; while ((m = re.exec(src)) !== null) out[m[1]] = true; return out; }

/* ------------------------------------------------------------ battery */
function battery() {
  var N = load(newSource, {});
  var B = load(baseSource, {});
  var bb = null, nn = null, n0 = null, n5 = null;

  test('K0 base oracle is the reviewed v1.21.1 file (SHA-256 pinned); versions in lockstep; both files compile', function () {
    eq(sha256(baseSource), history.V1211_SHA256, 'base SHA-256');
    eq(B.DT10_VERSION, '1.21.1', 'base version');
    eq(N.DT10_VERSION, '1.22.0', 'delivered version');
    assert(/^ \* Version: 1\.22\.0 /.test(newSource.split('\n')[3]), 'header line 4 lockstep');
    assert(/^ \* Version: 1\.21\.1 /.test(baseSource.split('\n')[3]), 'base header line 4');
    assert(newSource.indexOf("var DT10_VERSION = '1.22.0';") > 0, 'DT10_VERSION literal');
    eq(N.DT10_T10_CONFIRM_WINDOW_PROP, PROP, 'property name');
    eq(N.DT10_T10_CONFIRM_WINDOW_MAX, 30, 'cap');
    assert(B.DT10_T10_CONFIRM_WINDOW_PROP === undefined && typeof B.dt10StabConfirmWindow_ !== 'function', 'base lacks the change');
    new vm.Script(newSource, { filename: 'new.gs' }); new vm.Script(baseSource, { filename: 'base.gs' });
  });

  test('K1 GOLDEN NEGATIVE (v1.21.1): a WATCH day resets the challenger to 0; day 4 reads 1 and seats nothing; the base ignores the knob and writes no qh', function () {
    bb = scenarios(B, undefined);
    var s = bb.soft;
    deq([sym(s[0], 'CHAL.US').ci, sym(s[1], 'CHAL.US').ci, sym(s[2], 'CHAL.US').ci, sym(s[3], 'CHAL.US').ci], [1, 2, 0, 1], 'base ci path');
    eq(bySym(s[3], 'CHAL.US'), null, 'base never seats CHAL');
    eq(JSON.stringify(s[3].state).indexOf('"qh"'), -1, 'base writes no qh');
    deq(scenarios(B, 5), bb, 'base ignores confirm_window');
    eq(s[3].note.indexOf('cwin='), -1, 'base has no token');
    eq(s[3].audit.confirm_window, undefined, 'base has no audit key');
  });

  test('K2 KILL absent/0/1/junk/fault: every fixture, every day deep-equals the v1.21.1 base; dt10ApplyStability_ end to end (knobs, saved blob, note, reads)', function () {
    nn = scenarios(N, undefined); n0 = scenarios(N, 0);
    deq(nn, bb, 'knob absent vs base');
    deq(n0, bb, 'knob 0 vs base');
    deq(scenarios(N, 1), bb, 'knob 1 vs base (below 2 = off)');
    deq(scenarios(N, 'abc'), bb, 'knob junk vs base');
    var e2eN = applyE2E(newSource, {}), e2eB = applyE2E(baseSource, {});
    deq([e2eN.note, e2eN.selected, e2eN.audit, e2eN.saved, e2eN.knobs], [e2eB.note, e2eB.selected, e2eB.audit, e2eB.saved, e2eB.knobs], 'e2e unarmed vs base');
    eq(e2eN.reads[PROP], 1, 'the new file reads the property once');
    eq(e2eB.reads[PROP], undefined, 'base never reads the property');
    eq(e2eN.knobs.confirm_window, undefined, 'unarmed knobs object carries no confirm_window');
    var e2e0 = applyE2E(newSource, { TFB_T10_CONFIRM_WINDOW: '0' });
    deq([e2e0.note, e2e0.selected, e2e0.audit, e2e0.saved, e2e0.knobs], [e2eB.note, e2eB.selected, e2eB.audit, e2eB.saved, e2eB.knobs], 'e2e property 0 vs base');
    var e2e1 = applyE2E(newSource, { TFB_T10_CONFIRM_WINDOW: '1' });
    deq([e2e1.note, e2e1.selected, e2e1.audit, e2e1.saved, e2e1.knobs], [e2eB.note, e2eB.selected, e2eB.audit, e2eB.saved, e2eB.knobs], 'e2e property 1 vs base');
    var e2eF = applyE2E(newSource, { TFB_T10_CONFIRM_WINDOW: '5' }, { propsThrow: true });
    var e2eBF = applyE2E(baseSource, {}, { propsThrow: true });
    deq([e2eF.note, e2eF.selected, e2eF.audit, e2eF.knobs], [e2eBF.note, e2eBF.selected, e2eBF.audit, e2eBF.knobs], 'e2e property fault vs base fault');
  });

  test('K3 armed (window 5): ci 1,2,2,3; day 4 NEW 3/3 sized and B displaced; migration; token; audit; blob; pending cap', function () {
    n5 = scenarios(N, 5);
    var s = n5.soft;
    deq([sym(s[0], 'CHAL.US').ci, sym(s[1], 'CHAL.US').ci, sym(s[2], 'CHAL.US').ci, sym(s[3], 'CHAL.US').ci], [1, 2, 2, 3], 'window ci path');
    deq(sym(s[2], 'CHAL.US').qh, [1, 1, 0], 'bits after the WATCH day');
    deq(sym(s[3], 'CHAL.US').qh, [1, 1, 0, 1], 'bits after day 4');
    var t4 = bySym(s[3], 'CHAL.US');
    assert(t4 !== null, 'CHAL seated on day 4');
    eq(t4._stab_status, 'NEW (confirmed 3/3)', 'status');
    eq(t4.suggested_sar, 5000, 'raw ticket sizing kept');
    assert(t4._grace_hold !== true && t4._ft_suspended !== true, 'not withheld');
    deq(s[3].audit.entered, ['CHAL.US'], 'entered');
    deq(s[3].audit.exited_displaced, ['B.US'], 'B displaced');
    deq(sym(s[0], 'A.US').qh, [1, 1, 1, 1], 'A migrates its consecutive 3 and adds today');
    eq(sym(s[0], 'A.US').ci, 4, 'A ci');
    assert(s[0].note.indexOf(', cwin=5') > 0, 'token');
    eq(s[0].audit.confirm_window, 5, 'audit key');
    assert(JSON.stringify(s[1].state.symbols['CHAL.US']).indexOf('"qh":[1,1]') > 0, 'blob carries qh');
    // the soft-miss branch is unchanged: a pending challenger still reads cp absent (no pending grace armed)
    eq(sym(s[2], 'CHAL.US').cp, undefined, 'no cp without pending grace');
    // pending cap: a raw challenger with four qualified bits and no free seat reads 3/3, not 4/3
    var st = N.dt10StabParseState_(plain(seed()));
    st.symbols['CHAL.US'] = { ci: 4, co: 0, member: false, since: '', ls: '2026-10-20', hist: [86], ft: false, qh: [1, 1, 1, 1] };
    var full = N.dt10StabCore_([ticket('A.US', 95), ticket('B.US', 84), ticket('CHAL.US', 86)], plain(G), st, knobs(5), 2, DAYS[0]);
    var pend = null; for (var i = 0; i < full.audit.pending.length; i++) if (full.audit.pending[i].symbol === 'CHAL.US') pend = full.audit.pending[i];
    // CHAL (86 smoothed) beats B (84) -> displacement, so it is seated; make it weaker to stay pending
    st = N.dt10StabParseState_(plain(seed()));
    st.symbols['CHAL.US'] = { ci: 4, co: 0, member: false, since: '', ls: '2026-10-20', hist: [80, 80, 80], ft: false, qh: [1, 1, 1, 1] };
    var g80 = plain(G); g80[g80.length - 1].opportunity_score = 80;
    full = N.dt10StabCore_([ticket('A.US', 95), ticket('B.US', 84), ticket('CHAL.US', 80)], g80, st, knobs(5), 2, DAYS[0]);
    pend = null; for (var j = 0; j < full.audit.pending.length; j++) if (full.audit.pending[j].symbol === 'CHAL.US') pend = full.audit.pending[j];
    assert(pend !== null, 'CHAL pending (' + full.note + ')');
    eq(pend.confirmed_days, 3, 'pending shows min(ci, confirm_days)');
    eq(full.state.symbols['CHAL.US'].ci, 5, 'the real count is 5');
  });

  test('K4 semantics: raise to confirm_days; cap 30; same-day raise never lowers; no entry waits; hard exit deletes qh; soft/capacity/displacement keep; outage pause pushes no bit', function () {
    var s2 = scenarios(N, 2).soft;
    eq(s2[0].audit.confirm_window, 3, 'window 2 raised to confirm_days 3');
    deq([sym(s2[2], 'CHAL.US').ci, sym(s2[3], 'CHAL.US').ci], [2, 2], 'window 3: 1,1,0 then 1,0,1');
    eq(bySym(s2[3], 'CHAL.US'), null, 'not seated under window 3');
    var s99 = replay(N, knobs(99), seed(), raws(), grids(SOFT));
    eq(s99[0].audit.confirm_window, 30, 'core caps at 30');
    deq(sym(s99[3], 'CHAL.US').qh, [1, 1, 0, 1], 'same bits under a wide window');
    // same-day raise: after the WATCH day (bits 1,1,0) a later render with CHAL INVEST at rank 12 raises the bit
    var st3 = N.dt10StabParseState_(plain(n5.soft[2].state));
    var re = N.dt10StabCore_(plain(raws()[2]), plain(G), st3, knobs(5), 2, DAYS[2]);
    deq(re.state.symbols['CHAL.US'].qh, [1, 1, 1], 'bit raised');
    eq(re.state.symbols['CHAL.US'].ci, 3, 'ci recomputed');
    assert(re.note.indexOf('near 1 CHAL.US(3/3)') > 0, 'near token reads the window count: ' + re.note);
    eq(bySym(re, 'CHAL.US'), null, 'no raw ticket on day 3 -> not seated by a re-run');
    // never lowers: a re-run that finds CHAL WATCH again keeps the raised bit
    var st3b = N.dt10StabParseState_(plain(re.state));
    var re2 = N.dt10StabCore_(plain(raws()[2]), plain(SOFT), st3b, knobs(5), 2, DAYS[2]);
    deq(re2.state.symbols['CHAL.US'].qh, [1, 1, 1], 'bit kept');
    // a blob with no window entry waits for the day-advance (no qh created by a same-day run)
    var stW = N.dt10StabParseState_(plain(seed())); stW.date = DAYS[0];
    var reW = N.dt10StabCore_(plain(raws()[0]), plain(G), stW, knobs(5), 2, DAYS[0]);
    eq(reW.state.symbols['A.US'].qh, undefined, 'no qh on a same-day run without an entry');
    eq(reW.state.symbols['A.US'].ci, 3, 'ci untouched');
    // hard exit (B structural_block on day 1) deletes qh and zeroes ci
    var hb = n5.bblk[0];
    deq(hb.audit.exited_hard, ['B.US'], 'B hard-exited');
    eq(sym(hb, 'B.US').ci, 0, 'ci zeroed'); eq(sym(hb, 'B.US').qh, undefined, 'qh deleted');
    // displacement keeps qh and sets ci to its sum (B displaced on day 4 of the soft scenario)
    var b4 = sym(n5.soft[3], 'B.US');
    eq(b4.member, false, 'B out');
    deq(b4.qh, [1, 1, 1, 1, 1], 'B keeps its bits (seeded 3 migrated + INVEST at rank 15 on days 1-4, window 5)');
    eq(b4.ci, 5, 'B ci = sum of bits');
    // soft exit keeps qh: a member outside the buffer for exit_days misses
    var stS = N.dt10StabParseState_(plain(seed()));
    stS.symbols['B.US'] = { ci: 3, co: 2, member: true, since: '2026-10-15', ls: '2026-10-20', hist: [84], ft: false, qh: [1, 1, 1] };
    var gOut = plain(G); gOut[gOut.length - 2].opportunity_score = 50;   // B falls to rank 16
    var so = N.dt10StabCore_(plain(raws()[2]), gOut, stS, knobs(5), 2, DAYS[0]);
    deq(so.audit.exited_soft, ['B.US'], 'soft exit');
    deq(so.state.symbols['B.US'].qh, [1, 1, 1, 0], 'bits kept');
    eq(so.state.symbols['B.US'].ci, 3, 'ci = sum');
    // capacity exit keeps qh
    var stC = N.dt10StabParseState_(plain(seed()));
    stC.symbols['A.US'].qh = [1, 1, 1]; stC.symbols['B.US'].qh = [1, 1, 1];
    var cap = N.dt10StabCore_([ticket('A.US', 95)], plain(G), stC, knobs(5), 1, DAYS[0]);
    deq(cap.audit.exited_capacity, ['B.US'], 'capacity exit');
    deq(cap.state.symbols['B.US'].qh, [1, 1, 1, 1], 'bits kept'); eq(cap.state.symbols['B.US'].ci, 4, 'ci = sum');
    // outage pause: a paused member pushes no bit
    var stO = N.dt10StabParseState_(plain(seed())); stO.symbols['B.US'].qh = [1, 1, 1];
    var outage = { 'B.US': true };
    var op = N.dt10StabCore_(plain(raws()[2]), plain(G), stO, knobs(5), 2, DAYS[0], outage);
    deq(op.audit.outage_paused, ['B.US'], 'paused');
    deq(op.state.symbols['B.US'].qh, [1, 1, 1], 'no bit pushed'); eq(op.state.symbols['B.US'].ci, 3, 'ci untouched');
    // exit clocks (co) unchanged by the window: soft scenario B co path equals the base
    var coN = [], coB = [];
    for (var d = 0; d < 4; d++) { coN.push(sym(n5.soft[d], 'B.US').co); coB.push(sym(bb.soft[d], 'B.US').co); }
    deq(coN, coB, 'co path identical');
  });

  test('K5 service read: 2..30 -> n, 99 -> 30, blank/0/1/-1/2.5/junk/absent/fault -> 0; panel snapshot never carries the knob; knobs object carries it only while armed', function () {
    [['2', 2], [' 5 ', 5], ['30', 30], ['99', 30], ['02', 2], ['', 0], ['0', 0], ['1', 0], ['-1', 0], ['2.5', 0], ['five', 0]].forEach(function (pair) {
      var p = {}; p[PROP] = pair[0];
      eq(load(newSource, p).dt10StabConfirmWindow_(), pair[1], 'property ' + JSON.stringify(pair[0]));
    });
    eq(load(newSource, {}).dt10StabConfirmWindow_(), 0, 'absent');
    eq(load(newSource, {}, { propsThrow: true }).dt10StabConfirmWindow_(), 0, 'service fault');
    var panel = { 'T10: Max Selected': 2, 'T10: Stability Enabled': 'Yes', 'T10: Stability Confirm Days': 3,
                  'T10: Stability Exit Days': 3, 'T10: Stability Rank Buffer': 15, 'T10: Stability Smooth Days': 5 };
    var k = load(newSource, { TFB_T10_CONFIRM_WINDOW: '5' }).dt10StabKnobs_(panel);
    eq(k.confirm_window, undefined, 'dt10StabKnobs_ unchanged (panel snapshot)');
    var e5 = applyE2E(newSource, { TFB_T10_CONFIRM_WINDOW: '5' });
    eq(e5.knobs.confirm_window, 5, 'armed knobs object');
    eq(e5.audit.confirm_window, 5, 'armed audit');
    assert(e5.note.indexOf('cwin=5') > 0, 'armed note');
    assert(JSON.stringify(e5.saved).indexOf('"qh"') > 0, 'armed blob carries qh');
    var e5b = applyE2E(newSource, { TFB_T10_CONFIRM_WINDOW: '99' });
    eq(e5b.knobs.confirm_window, 30, 'clamped knob');
  });

  test('K6 state sanitizer: bits 1/0/"1"/2/"x" -> 1/0/1/0/0; newest 30 of 40 kept; absent or empty -> no key', function () {
    var parsed = N.dt10StabParseState_({ v: 1, date: '2026-10-20', symbols: {
      'A.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-20', hist: [1], ft: false, qh: [1, 0, '1', 2, 'x'] },
      'B.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-20', hist: [1], ft: false, qh: [] },
      'C.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-20', hist: [1], ft: false } } });
    deq(parsed.symbols['A.US'].qh, [1, 0, 1, 0, 0], 'bits');
    eq(parsed.symbols['B.US'].qh, undefined, 'empty -> absent');
    eq(parsed.symbols['C.US'].qh, undefined, 'absent -> absent');
    var long = []; for (var i = 0; i < 40; i++) long.push(i < 10 ? 0 : 1);
    var p2 = N.dt10StabParseState_({ v: 1, date: '2026-10-20', symbols: { 'A.US': { ci: 30, co: 0, member: false, since: '', ls: '2026-10-20', hist: [1], ft: false, qh: long } } });
    eq(p2.symbols['A.US'].qh.length, 30, 'newest 30'); eq(p2.symbols['A.US'].qh[0], 1, 'the zeros (oldest) fell off');
    // base parse of a blob with qh: the base keeps no qh key (unknown field)
    var pb = B.dt10StabParseState_({ v: 1, date: '2026-10-20', symbols: { 'A.US': { ci: 2, co: 0, member: false, since: '', ls: '2026-10-20', hist: [1], ft: false, qh: [1, 1] } } });
    eq(pb.symbols['A.US'].qh, undefined, 'base drops qh (rollback safe)');
  });

  test('K7 embedded dt10SelfTest: confirm window battery ok, property line follows the property, every base line kept, zero FAIL', function () {
    var rep = String(N.dt10SelfTest()), repB = String(B.dt10SelfTest());
    assert(rep.indexOf('confirm window core: ok') > 0, 'battery line: ' + rep.split('\n').filter(function (l) { return /confirm window/.test(l); }).join(' / '));
    assert(rep.indexOf('confirm window (TFB_T10_CONFIRM_WINDOW): 0 (') > 0, 'property line default');
    assert(rep.indexOf('[DT10 v1.22.0] self-test') === 0, 'version line');
    assert(rep.indexOf(': FAIL') < 0 && rep.indexOf('FAIL [') < 0, 'no FAIL');
    var baseLines = repB.split('\n'), newLines = rep.split('\n');
    var missing = baseLines.filter(function (l) { return l.indexOf('[DT10 v1.21.1]') !== 0 && newLines.indexOf(l) < 0; });
    deq(missing, [], 'every base self-test line present');
    eq(newLines.length, baseLines.length + 2, 'exactly two lines added');
    assert(repB.indexOf('confirm window') < 0, 'GOLDEN NEGATIVE: the base lacks the lines');
    var armed = String(load(newSource, { TFB_T10_CONFIRM_WINDOW: ' 5 ' }).dt10SelfTest());
    assert(armed.indexOf('confirm window (TFB_T10_CONFIRM_WINDOW): 5 (') > 0 && armed.indexOf('confirm window core: ok') > 0 && armed.indexOf('FAIL') < 0, 'armed property line');
  });

  test('K8 ES5/ASCII lint of the diff; inventory 4 functions + 2 vars added, 0 removed; 19 hunks reverse to the pinned base; WHY block newest-first; LF only', function () {
    var baseLines = {}, bl = baseSource.split('\n'), nl = newSource.split('\n');
    for (var i = 0; i < bl.length; i++) baseLines[bl[i]] = true;
    var added = nl.filter(function (l) { return !baseLines[l]; });
    assert(added.length >= 180 && added.length <= 260, 'added lines ' + added.length);
    added.forEach(function (l) {
      assert(!/\b(let|const|class)\b|=>|`/.test(l), 'ES5 violation: ' + l);
      if (!/^[\x09\x20-\x7e]*$/.test(l)) assert(baseLines[l.replace('1.22.0', '1.21.1')] === true, 'non-ASCII added line: ' + l);
    });
    assert(newSource.indexOf('\r') < 0, 'LF only');
    var tb = names(baseSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg), tn = names(newSource, /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg);
    deq(Object.keys(tb).filter(function (k) { return !tn[k]; }), [], 'functions removed');
    deq(Object.keys(tn).filter(function (k) { return !tb[k]; }).sort(), ['dt10StabConfirmWindowLen_', 'dt10StabConfirmWindow_', 'dt10StabQhBits_', 'dt10StabQhSum_'], 'functions added');
    var vb = names(baseSource, /^var\s+([A-Za-z_$][\w$]*)/mg), vn = names(newSource, /^var\s+([A-Za-z_$][\w$]*)/mg);
    deq(Object.keys(vb).filter(function (k) { return !vn[k]; }), [], 'vars removed');
    deq(Object.keys(vn).filter(function (k) { return !vb[k]; }).sort(), ['DT10_T10_CONFIRM_WINDOW_MAX', 'DT10_T10_CONFIRM_WINDOW_PROP'], 'vars added');
    eq(EDITS.length, 19, 'hunk count');
    for (var e = 0; e < EDITS.length; e++) eq(newSource.split(EDITS[e][0]).length - 1, 1, 'each current is unique: ' + EDITS[e][0].split('\n')[0]);
    eq(sha256(history.v1211Source(newSource)), history.V1211_SHA256, 'reversal of the delivered file');
    var why = newSource.indexOf(' * v1.22.0 (2026-11-08) [CONFIRM-WINDOW]'), prev = newSource.indexOf(' * v1.21.1 (2026-11-05) [MF-SCOPE-2422]');
    assert(why > 0 && prev > why, 'WHY block newest-first');
    assert(/^[\x09\x0a\x20-\x7e]*$/.test(newSource.slice(why, prev)), 'WHY block ASCII');
    var core = newSource.slice(newSource.indexOf('function dt10StabCore_('), newSource.indexOf('\n// ----', newSource.indexOf('function dt10StabCore_(')));
    ['outageOf', 'hardOut', 'scoreOf', 'smoothed', 'boardSort', 'inFinal', 'inList', 'qualifiedWithin', 'softMiss'].forEach(function (f) {
      assert(core.indexOf('function ' + f + '(') > 0, 'nested ' + f);
    });
  });

  test('K9 mutants: four one-line mutations are each caught', function () {
    var mutants = [
      ['bit always 1', "cwBits.push(cwQual ? 1 : 0);", "cwBits.push(1);"],
      ['token leaks unarmed', "if (cwLen > 0) note += ', cwin=' + cwLen;", "note += ', cwin=' + cwLen;"],
      ['service read arms 1', "if (!(n >= 2)) return 0;\n    return Math.min(DT10_T10_CONFIRM_WINDOW_MAX, n);\n  } catch (eCw) {}", "if (!(n >= 1)) return 0;\n    return Math.min(DT10_T10_CONFIRM_WINDOW_MAX, n);\n  } catch (eCw) {}"],
      ['hard exit keeps qh', "if (cwLen > 0 && stm.qh) delete stm.qh;   // v1.22.0 [CONFIRM-WINDOW]", "// mutant: qh kept"]
    ];
    mutants.forEach(function (mu) {
      eq(newSource.split(mu[1]).length - 1, 1, 'mutant anchor unique: ' + mu[0]);
      var src = newSource.replace(mu[1], mu[2]);
      var M = load(src, {});
      var caught = false;
      try {
        var m5 = scenarios(M, 5).soft, m0 = scenarios(M, undefined);
        var ok1 = JSON.stringify(m0) === JSON.stringify(bb);
        var ok2 = sym(m5[2], 'CHAL.US').ci === 2 && sym(m5[3], 'CHAL.US').ci === 3;
        var mb = scenarios(M, 5).bblk[0];
        var ok3 = sym(mb, 'B.US').qh === undefined;
        var ok4 = load(src, { TFB_T10_CONFIRM_WINDOW: '1' }).dt10StabConfirmWindow_() === 0;
        caught = !(ok1 && ok2 && ok3 && ok4);
      } catch (eM) { caught = true; }
      assert(caught, 'mutant not caught: ' + mu[0]);
    });
  });

  return sha256(JSON.stringify([bb, nn, n0, n5]));
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
