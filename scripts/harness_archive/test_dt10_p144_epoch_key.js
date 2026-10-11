#!/usr/bin/env node
/* tests/test_dt10_p144_epoch_key.js — dual-tree real-module harness for
 * 16_Decision_Top10.gs v1.11.11 [P-144 EPOCH-KEYED STABILITY CLOCKS].
 * Loads the REAL .gs source (base v1.11.10 and delivered v1.11.11) into a vm
 * context with minimal Apps Script service stubs (the P-168 harness shape)
 * and drives the REAL dt10StabResolveEpoch_ / dt10StabStampUtcDate_ /
 * dt10StabCore_ / dt10ApplyStability_ with the 2026-09-26 feed stamps.
 * Usage: node test_dt10_p144_epoch_key.js [--base p] [--delivered p] */
'use strict';
var fs = require('fs'), vm = require('vm'), crypto = require('crypto');
var args = process.argv.slice(2);
function arg(k, d) { var i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; }
var BASE = arg('--base', '16_Decision_Top10_base.gs');
var DELIV = arg('--delivered', '16_Decision_Top10.gs');

function stubs(statusRows) {
  var props = {};
  var statusSheet = {
    getRange: function () { return { getValues: function () { return statusRows || []; },
                                     getValue: function () { return ''; }, setValue: function () { return this; },
                                     setValues: function () { return this; } }; },
    getDataRange: function () { return { getValues: function () { return [[]]; } }; },
    getLastRow: function () { return 0; }, getLastColumn: function () { return 0; }, getName: function () { return '_Status'; }
  };
  var ss = { getSheetByName: function (n) { return n === '_Status' ? statusSheet : null; },
             getRangeByName: function () { return null; }, getSheets: function () { return []; }, getId: function () { return 'stub'; } };
  return {
    console: console, Logger: { log: function () {} },
    Utilities: { formatDate: function (d) { return d.toISOString().slice(0, 10); }, sleep: function () {},
                 computeDigest: function () { return []; }, base64Encode: function () { return ''; } },
    PropertiesService: { getScriptProperties: function () { return {
      getProperty: function (k) { return props.hasOwnProperty(k) ? props[k] : null; },
      setProperty: function (k, v) { props[k] = String(v); return this; },
      deleteProperty: function (k) { delete props[k]; return this; } }; },
      getUserProperties: function () { return this.getScriptProperties(); } },
    SpreadsheetApp: { getActiveSpreadsheet: function () { return ss; }, getUi: function () { throw new Error('no UI'); }, openById: function () { return ss; } },
    UrlFetchApp: { fetch: function () { throw new Error('no network'); } },
    Session: { getScriptTimeZone: function () { return 'Etc/UTC'; } },
    CacheService: { getScriptCache: function () { return { get: function () { return null; }, put: function () {} }; } },
    _props: props, _setStatus: function (rows) { statusRows = rows; }
  };
}
function loadTree(file, statusRows) {
  var ctx = vm.createContext(stubs(statusRows));
  vm.runInContext(fs.readFileSync(file, 'utf8'), ctx, { filename: file });
  return ctx;
}
var KNOBS = { enabled: true, confirm_days: 3, exit_days: 3, rank_buffer: 15, smooth_days: 5, hard_strict: true };
function seed() {
  return { v: 1, date: '2026-09-25', symbols: {
    'ITRN.US':   { ci: 3, co: 0, member: true, since: '2026-09-19', ls: '2026-09-25', hist: [79, 78.6], ft: false },
    'PINFRA.MX': { ci: 3, co: 1, member: true, since: '2026-09-15', ls: '2026-09-24', hist: [75, 74.8], ft: false },
    'NVDA.US':   { ci: 1, co: 0, member: true, since: '2026-09-25', ls: '2026-09-25', hist: [69.8], ft: true },
    '2222.SR':   { ci: 3, co: 1, member: true, since: '2026-09-12', ls: '2026-09-24', hist: [67, 66.7], ft: false } } };
}
var FEED_FRESH = 'OK | cov=97.5 | run=36199188352 | 2026-09-26 04:18:18+03:00';   // real 09-26 GM stamp (01:18Z)
var FEED_AGED  = 'OK | cov=100.0 | run=36199188352 | 2026-09-26 01:59:00+03:00';  // 22:59Z 09-25: the 20Z epoch
var FEED_SKIP  = 'SKIPPED | run=36231358321 | 2026-09-26 12:09:00+03:00';          // enforce-armed future
var OUTAGE = { 'ITRN.US': true, 'PINFRA.MX': true, 'NVDA.US': true };                // today's fetch-failed seats
function json(x) { return JSON.stringify(x); }
function co(res, s) { return res.state.symbols[s].co; }

function runOnce() {
  var B = loadTree(BASE), D = loadTree(DELIV);
  var log = [], fails = 0;
  function check(name, ok, detail) { log.push((ok ? 'PASS ' : 'FAIL ') + name + (detail ? ' :: ' + detail : '')); if (!ok) fails++; }
  // T1 pure resolver + stamp parser
  var r = function (f, sd, tk) { return D.dt10StabResolveEpoch_(f, sd, tk); };
  var t1 = [
    [r(FEED_FRESH, '2026-09-25', '2026-09-26'), '2026-09-26', 'feed', false, 'fresh epoch advances'],
    [r(FEED_AGED, '2026-09-25', '2026-09-26'), '2026-09-25', 'feed', true, 'aged 20Z epoch (22:59Z) stays on 09-25: frozen'],
    [r('STALE_COV | cov=89.0 | run=1 | 2026-09-26 01:30:00', '2026-09-25', '2026-09-26'), '2026-09-25', 'feed', true, 'naive stamp read as Riyadh'],
    [r(FEED_SKIP, '2026-09-25', '2026-09-26'), '2026-09-25', 'held', true, 'SKIPPED leg = no evidence: held'],
    [r('FAILED | run=9 | 2026-09-26 05:00:00+03:00', '2026-09-25', '2026-09-26'), '2026-09-25', 'held', true, 'FAILED leg held'],
    [r('', '2026-09-25', '2026-09-26'), '2026-09-25', 'held', true, 'missing key held'],
    [r('', '', '2026-09-26'), '2026-09-26', 'clock', false, 'bootstrap (no state date) -> wall-clock'],
    [r('OK | cov=97.5 | run=3 | 2026-09-24 10:00:00+03:00', '2026-09-26', '2026-09-26'), '2026-09-26', 'held', true, 'never backwards'],
    [r('OK | cov=97.5 | run=4 | 2026-09-27 03:30:00+03:00', '2026-09-25', '2026-09-26'), '2026-09-26', 'feed', false, 'never in the future (clamped)'],
    [r('garbage', '2026-09-25', '2026-09-26'), '2026-09-25', 'held', true, 'unreadable held'],
    [r('PARTIAL_FRESH | cov=88.0 | run=5 | 2026-09-26 03:00:00+03:00', '2026-09-25', '2026-09-26'), '2026-09-26', 'feed', false, 'PARTIAL_FRESH counts as an epoch (00:00Z boundary)'],
    [r('OK | cov=97.5 | run=6 | 2026-09-26 02:59:59+03:00', '2026-09-25', '2026-09-26'), '2026-09-25', 'feed', true, '02:59:59+03 is still 09-25 UTC']
  ];
  t1.forEach(function (c, i) { check('T1.' + (i + 1) + ' ' + c[4], c[0].key === c[1] && c[0].src === c[2] && c[0].held === c[3], json(c[0])); });
  check('T1.13 stamp parser: Z / offset / naive / garbage',
        D.dt10StabStampUtcDate_('2026-09-26T00:17:04Z') === '2026-09-26' && D.dt10StabStampUtcDate_('2026-09-25 21:00:00-05:00') === '2026-09-26' &&
        D.dt10StabStampUtcDate_('2026-09-26 02:00:00') === '2026-09-25' && D.dt10StabStampUtcDate_('nope') === '' && D.dt10StabStampUtcDate_(null) === '');

  // T2 the aged morning: base advances on stale evidence, delivered freezes
  var rb = B.dt10StabCore_([], [], B.dt10StabParseState_(seed()), KNOBS, 10, '2026-09-26');
  var ekA = D.dt10StabResolveEpoch_(FEED_AGED, '2026-09-25', '2026-09-26');
  var rd = D.dt10StabCore_([], [], D.dt10StabParseState_(seed()), KNOBS, 10, ekA.key, OUTAGE);
  check('T2 base (wall-clock key) advances on the 22:59Z epoch: PINFRA 1->2, 2222 1->2', co(rb, 'PINFRA.MX') === 2 && co(rb, '2222.SR') === 2, rb.note);
  check('T2 delivered (epoch key 09-25) freezes: counters, hist, state.date unchanged, no outage token',
        co(rd, 'PINFRA.MX') === 1 && co(rd, '2222.SR') === 1 && co(rd, 'ITRN.US') === 0 && rd.state.date === '2026-09-25' &&
        !rd.state.symbols['ITRN.US'].op && rd.audit.outage_paused.length === 0 && json(rd.state.symbols['ITRN.US'].hist) === json([79, 78.6]) &&
        rd.audit.exited_soft.length === 0, rd.note);
  // T3 the fresh epoch arrives: one advance (P-168 pauses the poisoned seats), then frozen again on the same epoch
  var ekF = D.dt10StabResolveEpoch_(FEED_FRESH, rd.state.date, '2026-09-26');
  var rd2 = D.dt10StabCore_([], [], D.dt10StabParseState_(rd.state), KNOBS, 10, ekF.key, OUTAGE);
  check('T3 fresh epoch: ITRN/PINFRA/NVDA paused (P-168), 2222.SR 1->2 on merit, state.date 09-26',
        ekF.key === '2026-09-26' && ekF.src === 'feed' && !ekF.held && co(rd2, 'PINFRA.MX') === 1 && rd2.state.symbols['PINFRA.MX'].op === 1 &&
        co(rd2, '2222.SR') === 2 && rd2.state.date === '2026-09-26' && rd2.audit.outage_paused.length === 3, rd2.note);
  var ekF2 = D.dt10StabResolveEpoch_(FEED_FRESH, rd2.state.date, '2026-09-26');
  var rd3 = D.dt10StabCore_([], [], D.dt10StabParseState_(rd2.state), KNOBS, 10, ekF2.key, OUTAGE);
  check('T3b second run on the same epoch: frozen (three legs a day still advance once)',
        ekF2.held === true && json(rd3.state.symbols) === json(rd2.state.symbols) && rd3.audit.outage_paused.length === 0);
  // T4 enforce-armed future: a SKIPPED leg brings no evidence
  var ekS = D.dt10StabResolveEpoch_(FEED_SKIP, '2026-09-25', '2026-09-26');
  var rd4 = D.dt10StabCore_([], [], D.dt10StabParseState_(seed()), KNOBS, 10, ekS.key, {});
  check('T4 SKIPPED feed: key held on 09-25, nothing moves', ekS.held && co(rd4, 'PINFRA.MX') === 1 && rd4.state.date === '2026-09-25');

  // T5 REAL dt10ApplyStability_ end to end (real state blob in the property store, real _Status L:M rows)
  function apply(T, feedVal, legacy) {
    T._setStatus([['Backend URL', 'x'], ['TFB Feed Global_Markets', feedVal], ['TFB Decision Feed', 'EXECUTABLE | run=1 | 2026-09-26 04:25:05+03:00']]);
    T._props['DT10_STAB_STATE_V1'] = json(seed());
    if (legacy) T._props['DT10_P144_EPOCH_KEY_LEGACY'] = '1'; else delete T._props['DT10_P144_EPOCH_KEY_LEGACY'];
    var payload = { selected: [], candidates_rows: [], meta: {} };
    var out = T.dt10ApplyStability_(payload, { 'T10: Max Selected': 10 }, null);
    return { note: out.note, state: T.dt10StabParseState_(T._props['DT10_STAB_STATE_V1']), tickets: payload.selected };
  }
  var todayKey = D.dt10StabToday_();                       // the stub's wall-clock UTC date
  var agedForToday = 'OK | cov=100.0 | run=1 | 2026-09-25 23:30:00+03:00';   // 20:30Z 09-25 < any later wall-clock
  var aB = apply(B, agedForToday, false), aD = apply(D, agedForToday, false), aK = apply(D, agedForToday, true);
  check('T5 real apply — base advances on the aged feed (co 2), delivered freezes (co 1) and prints epoch=2026-09-25/feed(frozen)',
        co(aB, 'PINFRA.MX') === 2 && co(aD, 'PINFRA.MX') === 1 && aD.note.indexOf('epoch=2026-09-25/feed(frozen)') > 0 && aB.note.indexOf('epoch=') < 0,
        aD.note);
  check('T5b kill property: delivered == base (tickets + state blob + note, wall-clock key, no epoch token)',
        json(aK.tickets) === json(aB.tickets) && json(aK.state) === json(aB.state) && aK.note === aB.note && todayKey === aK.state.date);
  var freshForToday = 'OK | cov=97.5 | run=2 | ' + todayKey + ' 12:00:00+00:00';
  var aF = apply(D, freshForToday, false);
  check('T5c real apply — fresh epoch (today) advances once: epoch=<today>/feed, co 2', co(aF, 'PINFRA.MX') === 2 && aF.note.indexOf('epoch=' + todayKey + '/feed') > 0 && aF.note.indexOf('(frozen)') < 0, aF.note);
  var aM = apply(D, '', false);
  check('T5d real apply — missing feed key: held on the state date, frozen token', co(aM, 'PINFRA.MX') === 1 && aM.note.indexOf('epoch=2026-09-25/held(frozen)') > 0, aM.note);

  // T6 embedded self-test replay on both trees
  function selftest(T) { try { return T.dt10SelfTest(); } catch (e) { return 'THREW ' + e; } }
  var stB = selftest(B), stD = selftest(D);
  var prior = ['outage pause core: ok', 'grace sizing core: ok', 'funding containment core: ok', 'cash source core: ok'];
  check('T6 delivered dt10SelfTest prints "epoch key core: ok" + the prior ok lines', stD.indexOf('epoch key core: ok') >= 0 && prior.every(function (l) { return stD.indexOf(l) >= 0; }),
        stD.split('\n').filter(function (l) { return /core: /.test(l); }).join(' | '));
  check('T6b base has no epoch line; versions 1.11.10 -> 1.11.11', stB.indexOf('epoch key core') < 0 && B.DT10_VERSION === '1.11.10' && D.DT10_VERSION === '1.11.11');

  var digest = crypto.createHash('sha256').update(json({ t1: t1.map(function (c) { return c[0]; }), rd: rd.note, rd2: rd2.note, aD: aD.note, aF: aF.note.replace(todayKey, 'TODAY'), aM: aM.note,
                                                             st: stD.split('\n').filter(function (l) { return /core: /.test(l); }) })).digest('hex').slice(0, 12);
  return { log: log, fails: fails, digest: digest };
}
var runs = [];
for (var i = 0; i < 3; i++) runs.push(runOnce());
runs[0].log.forEach(function (l) { console.log(l); });
console.log('---');
console.log('runs x3 digests:', runs.map(function (r) { return r.digest; }).join(' '), 'identical=' + (runs[0].digest === runs[1].digest && runs[1].digest === runs[2].digest));
console.log('result:', runs.every(function (r) { return r.fails === 0; }) ? 'ALL PASS' : 'FAILURES ' + runs[0].fails);
process.exit(runs.every(function (r) { return r.fails === 0; }) ? 0 : 1);
