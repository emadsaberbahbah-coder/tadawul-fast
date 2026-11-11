#!/usr/bin/env node
/* tests/test_prd_v100_page_dispatch.js
 * Harness for apps_script/29_Page_Refresh_Dispatch.gs v1.0.0 (a new file): prdTick starts the Daily Plan, the Exit Plan
 * (with the e-mail at 07:30) and the Consistency Check once per Riyadh hour through GitHub workflow_dispatch, because
 * GitHub's own schedule did not start them (11 Nov 2026, 14:00-18:00 Riyadh: 0 of about 12 runs).
 *   Q0 compile; version lockstep; inventory; ES5 and ASCII in the whole file; LF
 *   Q1 GATE: PRD_MODE off / absent / junk -> one property read, no fetch, no write; a property fault -> off
 *   Q2 pure: mode, skip, state, due lanes, the request, redaction
 *   Q3 observe: the lanes it would start, no fetch, no state
 *   Q4 on: the right lanes at 07:31, 08:36, 00:51, 23:21; the request (URL, payload, headers); 204 marks done; a refusal
 *      is tried at the next tick, 3 times at most; no token; skip; a new hour starts a new state; the token never logged
 *   Q5 owner functions: install (one trigger every 10 min, PRD_MODE on when unset, kept when set), remove, status
 *   Q6 mutants
 * The battery runs twice and the two digests must match; the last line is 'PASS k/k' (exit 1 on any failure).
 */
'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const vm = require('vm');
const ROOT = path.join(__dirname, '..');
const SRC_PATH = process.argv[2] || path.join(ROOT, 'apps_script', '29_Page_Refresh_Dispatch.gs');
const SRC = fs.readFileSync(SRC_PATH, 'utf8');
const sha256 = s => crypto.createHash('sha256').update(s).digest('hex');
const plain = v => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
const TOKEN = 'ghp_TESTTOKEN0123456789';

let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('PASS ' + name); }
  catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n  ')); }
}
function ok(c, m) { if (!c) throw new Error(m || 'assertion failed'); }
function eq(a, b, m) { if (a !== b) throw new Error((m || 'not equal') + ': ' + JSON.stringify(a) + ' !== ' + JSON.stringify(b)); }
function deq(a, b, m) { const ja = JSON.stringify(a), jb = JSON.stringify(b); if (ja !== jb) throw new Error((m || 'deep-equal') + ': ' + ja.slice(0, 900) + ' !== ' + jb.slice(0, 900)); }

const at = (h, m, d) => Date.UTC(2026, 10, d || 12, h - 3, m, 0);   // Riyadh h:m on 12 Nov 2026 (h may be 24 for 00:xx next day)
function formatDate(d, tz, pattern) {
  const ms = d.getTime(), off = /Riyadh/.test(String(tz)) ? 3 : 0;
  const t = new Date(ms + off * 3600000), p = n => (n < 10 ? '0' : '') + n;
  return String(pattern).replace('yyyy', t.getUTCFullYear()).replace('MM', p(t.getUTCMonth() + 1)).replace('dd', p(t.getUTCDate()))
    .replace('HH', p(t.getUTCHours())).replace('mm', p(t.getUTCMinutes()));
}
function fakeDate(nowMs) {
  const RealDate = Date;
  return class extends RealDate { constructor(...a) { if (a.length === 0) super(nowMs); else super(...a); } static now() { return nowMs; } };
}
/* one world: a property store, a trigger store and the GitHub answers across ticks */
function world(props, answers) {
  return { props: Object.assign({}, props || {}), triggers: [], answers: answers || {}, fetches: [], logs: [], toasts: [], reads: [], writes: [] };
}
function env(src, w, ms, opts) {
  opts = opts || {};
  const ctx = {
    Date: fakeDate(ms),
    PropertiesService: { getScriptProperties() {
      if (opts.propsThrow) throw new Error('synthetic property outage');
      return { getProperty(k) { w.reads.push(k); return Object.prototype.hasOwnProperty.call(w.props, k) ? w.props[k] : null; },
               setProperty(k, v) { w.writes.push(k); w.props[k] = String(v); }, getProperties() { return Object.assign({}, w.props); } }; } },
    Utilities: { formatDate },
    UrlFetchApp: { fetch(url, o) {
      w.fetches.push({ url, method: o.method, payload: JSON.parse(o.payload), headers: o.headers, mute: o.muteHttpExceptions, ct: o.contentType });
      const wf = url.split('/workflows/')[1].split('/')[0], a = w.answers[wf];
      if (a === 'throw') throw new Error('Address unavailable: ' + url + ' ' + o.headers.Authorization);
      const code = typeof a === 'number' ? a : 204;
      return { getResponseCode: () => code, getContentText: () => (code === 204 ? '' : '{"message":"Bad credentials ' + TOKEN + '"}') };
    } },
    Logger: { log(m) { w.logs.push(String(m)); } },
    SpreadsheetApp: { getActive() { return { toast(m, t, s) { w.toasts.push([String(m), String(t), s]); } }; } },
    ScriptApp: {
      getProjectTriggers() { return w.triggers.map(t => ({ getHandlerFunction: () => t.h, _t: t })); },
      deleteTrigger(tr) { w.triggers = w.triggers.filter(t => t !== tr._t); },
      newTrigger(h) { const t = { h, every: 0 }; const b = { timeBased() { return b; }, everyMinutes(n) { t.every = n; return b; }, create() { w.triggers.push(t); return t; } }; return b; }
    }
  };
  vm.createContext(ctx);
  vm.runInContext(src, ctx, { filename: '29_Page_Refresh_Dispatch.gs' });
  return ctx;
}
function tick(src, w, ms, opts) { const f = w.fetches.length, l = w.logs.length; w.reads = []; w.writes = []; const r = plain(env(src, w, ms, opts).prdTick()); return { ret: r, fetches: w.fetches.slice(f), logs: w.logs.slice(l), reads: w.reads.slice(), writes: w.writes.slice() }; }
const ON = { PRD_MODE: 'on', TFB_GH_DISPATCH_TOKEN: TOKEN };
const ids = fs_ => fs_.map(f => f.url.split('/workflows/')[1].split('/')[0] + ':' + (f.payload.inputs.send_mail || '-'));

function scenarios(src) {
  const out = {};
  const e = env(src, world(), at(9, 0));
  out.pure = [
    ['', null, 'off', 'OFF', 'observe', ' On ', 'yes', 'true', '1', 'junk'].map(v => e.prdModeCore_(v)),
    plain(e.prdSkipCore_(' mail, cons;mail ')), plain(e.prdSkipCore_('')),
    plain(e.prdStateCore_('', '2026-11-12 08')), plain(e.prdStateCore_('{"hour":"2026-11-12 07","done":{"plan":1}}', '2026-11-12 08')),
    plain(e.prdStateCore_('{"hour":"2026-11-12 08","done":{"plan":1},"tries":{"plan":1}}', '2026-11-12 08')), plain(e.prdStateCore_('junk', 'k')),
    plain(e.prdRequestCore_('o/r', 'exit_plan.yml', 'main', { dry_run: 'false', send_mail: 'true' }, 'T')),
    e.prdRedact_('a T b T', 'T'), e.prdRedact_(null, 'T'), e.prdRedact_('x', '')
  ];
  const due = (h, m, st, skip) => e.prdDueCore_(e.PRD_LANES_, h, m, e.prdStateCore_(st || '', 'k'), skip || []).map(l => l.id);
  out.due = [due(7, 19), due(7, 20), due(7, 30), due(7, 34), due(7, 50), due(8, 35), due(8, 30), due(0, 50), due(0, 20), due(1, 55), due(6, 59),
             due(23, 59), due(7, 55, '{"hour":"k","done":{"plan":1,"mail":1}}'), due(7, 55, '{"hour":"k","tries":{"plan":3,"mail":2}}'),
             due(7, 55, '', ['mail', 'cons'])];
  // Q1 gate
  const g = ['', 'off', 'junk'].map(v => { const w = world(v === '' ? {} : { PRD_MODE: v }); const t = tick(src, w, at(7, 31)); return [t.ret, t.fetches.length, t.reads, t.writes]; });
  const gf = (() => { const w = world(ON); const t = tick(src, w, at(7, 31), { propsThrow: true }); return [t.ret, t.fetches.length]; })();
  out.gate = [g, gf];
  // Q3 observe
  const wo = world({ PRD_MODE: 'observe', TFB_GH_DISPATCH_TOKEN: TOKEN });
  const o1 = tick(src, wo, at(7, 31));
  out.observe = [o1.ret, o1.fetches.length, o1.logs, wo.props.PRD_STATE || null];
  // Q4 on
  const w = world(ON);
  const a1 = tick(src, w, at(7, 31)), s1 = JSON.parse(w.props.PRD_STATE);
  const a2 = tick(src, w, at(7, 41));
  const a3 = tick(src, w, at(7, 51));
  const a4 = tick(src, w, at(8, 36));
  const a5 = tick(src, w, at(24, 51));                                     // 00:51 on 13 Nov
  out.on = [a1.ret, ids(a1.fetches), a1.fetches[0], s1, a1.logs, a2.ret, a2.fetches.length, a3.ret, ids(a3.fetches), a4.ret, ids(a4.fetches),
            a5.ret, ids(a5.fetches), JSON.parse(w.props.PRD_STATE)];
  const wr = world(Object.assign({ TFB_GH_DISPATCH_REPO: ' me/other ', TFB_GH_DISPATCH_REF: 'dev' }, ON));
  const r1 = tick(src, wr, at(23, 21));
  out.repo = [r1.fetches[0].url, r1.fetches[0].payload.ref, r1.ret];
  const wf = world(ON, { 'exit_plan.yml': 401 });
  const f1 = tick(src, wf, at(7, 31)), f2 = tick(src, wf, at(7, 41)), f3 = tick(src, wf, at(7, 51)), f4 = tick(src, wf, at(7, 58));
  out.refused = [f1.ret, f2.ret, f3.ret, f4.ret, ids(f4.fetches), JSON.parse(wf.props.PRD_STATE), [].concat(f1.logs, f2.logs, f3.logs).some(l => l.indexOf(TOKEN) >= 0)];
  const wt = world(ON, { 'daily_plan.yml': 'throw' });
  const t1 = tick(src, wt, at(9, 21));
  out.thrown = [t1.ret.started, t1.ret.failed.length, t1.ret.failed[0].indexOf(TOKEN) < 0, JSON.parse(wt.props.PRD_STATE)];
  const wn = world({ PRD_MODE: 'on' });
  const n1 = tick(src, wn, at(7, 31));
  out.notoken = [n1.ret, n1.fetches.length, wn.props.PRD_STATE || null, n1.logs];
  const ws = world(Object.assign({ PRD_SKIP: 'mail' }, ON));
  const k1 = tick(src, ws, at(7, 31));
  out.skip = [k1.ret.started, ids(k1.fetches)];
  // Q5 owner functions
  const wi = world({}); wi.triggers = [{ h: 'prdTick', every: 5 }, { h: 'tfbDispatchDailySync', every: 10 }];
  const ie = env(src, wi, at(9, 0));
  const m1 = ie.prdInstallTrigger(), tr1 = plain(wi.triggers), mode1 = wi.props.PRD_MODE;
  wi.props.PRD_MODE = 'observe';
  const m2 = ie.prdInstallTrigger(), mode2 = wi.props.PRD_MODE;
  wi.props.TFB_GH_DISPATCH_TOKEN = TOKEN; wi.props.PRD_STATE = '{"hour":"2026-11-12 09","done":{"plan":1},"tries":{"plan":1}}';
  const st1 = ie.prdStatus();
  const rm1 = ie.prdRemoveTrigger(), rm2 = ie.prdRemoveTrigger(), tr2 = plain(wi.triggers);
  out.owner = [m1, tr1, mode1, m2, mode2, st1, rm1, rm2, tr2, wi.toasts.map(t => t[1]), st1.indexOf(TOKEN) < 0];
  return out;
}

const MUTANTS = [
  ['default on', "  return v === 'observe' ? 'observe' : 'off';", "  return v === 'observe' ? 'observe' : 'on';"],
  ['no gate', "  if (mode === 'off') return out;\n", ""],
  ['the e-mail every hour', "minute: 30, hours: [7],", "minute: 30, hours: PRD_DAY_,"],
  ['minute ignored', "minute < l.minute ||", ""],
  ['hours ignored', "if (l.hours.indexOf(hour) < 0 || minute", "if (minute"],
  ['done ignored', " || st.done[l.id] ||", " ||"],
  ['one more try', "(+st.tries[l.id] || 0) >= PRD_.MAX_TRIES) continue;", "(+st.tries[l.id] || 0) > PRD_.MAX_TRIES) continue;"],
  ['skip ignored', "    if (skip.indexOf(l.id) >= 0) continue;\n", ""],
  ['state of another hour kept', "st.hour !== key) st = ", "false) st = "],
  ['observe starts', "  if (mode === 'observe') {", "  if (false) {"],
  ['no token check', "  if (!token) {", "  if (false) {"],
  ['any answer is done', "    if (code === 204) {", "    if (code) {"],
  ['the token in the log', "prdRedact_(body, token).slice(0, 120)", "String(body).slice(0, 120)"],
  ['tries not counted', "    st.tries[l.id] = (+st.tries[l.id] || 0) + 1;\n", ""],
  ['state not saved', "    try { p.setProperty(PRD_.PROP_STATE, JSON.stringify(st)); } catch (eS) {}\n", ""],
  ['the e-mail at every exit run', "inputs: { dry_run: 'false', send_mail: 'false' } }", "inputs: { dry_run: 'false', send_mail: 'true' } }"],
  ['exit at 07:35 too', "hours: PRD_DAY_.slice(1).concat([0])", "hours: PRD_DAY_.concat([0])"],
  ['the wrong ref', "prdTrim_(p.getProperty(PRD_.PROP_REF)) || PRD_.DEFAULT_REF", "PRD_.DEFAULT_REF"],
  ['every 5 minutes', "everyMinutes(PRD_.EVERY_MIN)", "everyMinutes(5)"],
  ['mode overwritten', "  if (!prdTrim_(p.getProperty(PRD_.PROP_MODE))) { p.setProperty(PRD_.PROP_MODE, 'on'); set = ' (set to on)'; }", "  p.setProperty(PRD_.PROP_MODE, 'on'); set = ' (set to on)';"],
  ['old triggers kept', "  var old = prdRemoveTrigger_();\n  ScriptApp.newTrigger", "  var old = 0;\n  ScriptApp.newTrigger"]
];

function battery() {
  const parts = [];
  let sc = null;
  test('Q0 compile; version lockstep 1.0.0; inventory; ES5 and ASCII in the whole file; LF', () => {
    new vm.Script(SRC);
    ok(SRC.indexOf("var PRD_VERSION = '1.0.0';   // lockstep with the header") > 0 && / \* Version: 1\.0\.0 \(2026-11-11\)/.test(SRC.split('\n')[3]), 'version lockstep');
    const fns = []; let m; const re = /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg; while ((m = re.exec(SRC)) !== null) fns.push(m[1]);
    deq(fns.sort(), ['prdDueCore_', 'prdInstallTrigger', 'prdModeCore_', 'prdNotify_', 'prdRedact_', 'prdRemoveTrigger', 'prdRemoveTrigger_', 'prdRequestCore_',
                     'prdSkipCore_', 'prdStateCore_', 'prdStatus', 'prdTick', 'prdTrim_'], 'functions');
    ok(SRC.indexOf('\r') < 0, 'LF');
    for (let i = 0; i < SRC.length; i++) ok(SRC.charCodeAt(i) < 128, 'ASCII: ' + JSON.stringify(SRC.slice(Math.max(0, i - 20), i + 5)));
    const code = SRC.split('\n').filter(l => !/^\s*(\*|\/\*)/.test(l)).join('\n');
    ok(!/=>|`|\blet\s|\bconst\s|\bclass\s|\.\.\.[A-Za-z_]/.test(code), 'ES5');
  });
  test('Q1 GATE: absent / off / junk -> one property read, no fetch, no write; a property fault -> off', () => {
    sc = scenarios(SRC);
    for (const [ret, n, reads, writes] of sc.gate[0]) { eq(ret.mode, 'off'); eq(n, 0); deq(reads, ['PRD_MODE']); deq(writes, []); }
    deq(sc.gate[1], [{ mode: 'off', started: [], failed: [], would: [] }, 0], 'property fault');
    parts.push(sc.gate);
  });
  test('Q2 pure: mode, skip, state, due lanes by hour and minute, the request, redaction', () => {
    const p = sc.pure;
    deq(p[0], ['off', 'off', 'off', 'off', 'observe', 'on', 'on', 'on', 'on', 'off'], 'mode');
    deq(p[1], ['mail', 'cons'], 'skip'); deq(p[2], [], 'no skip');
    deq(p[3], { hour: '2026-11-12 08', done: {}, tries: {} }); deq(p[4], { hour: '2026-11-12 08', done: {}, tries: {} }, 'another hour starts empty');
    deq(p[5], { hour: '2026-11-12 08', done: { plan: 1 }, tries: { plan: 1 } }); deq(p[6], { hour: 'k', done: {}, tries: {} });
    deq(p[7], { url: 'https://api.github.com/repos/o/r/actions/workflows/exit_plan.yml/dispatches',
                options: { method: 'post', contentType: 'application/json', payload: '{"ref":"main","inputs":{"dry_run":"false","send_mail":"true"}}',
                           muteHttpExceptions: true, headers: { Authorization: 'Bearer T', Accept: 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28' } } }, 'request');
    deq(p.slice(8), ['a [token] b [token]', '', 'x'], 'redact');
    deq(sc.due, [[], ['plan'], ['plan', 'mail'], ['plan', 'mail'], ['plan', 'mail', 'cons'], ['plan', 'exit'], ['plan'], ['exit', 'cons'], [], [], [],
                 ['plan', 'exit', 'cons'], ['cons'], ['mail', 'cons'], ['plan']], 'due lanes');
    parts.push(sc.pure, sc.due);
  });
  test('Q3 observe: the lanes it would start, logged; no fetch; no state', () => {
    deq(sc.observe, [{ mode: 'observe', hour: '2026-11-12 07', started: [], failed: [], would: ['plan', 'mail'] }, 0,
                     ['page refresh 2026-11-12 07: observe - would start plan, mail'], null]);
    parts.push(sc.observe);
  });
  test('Q4 on: lanes, request, 204 done, refusals retried 3 times, a thrown fetch, no token, skip, a new hour, the token never logged', () => {
    const [a1, f1, req, s1, log1, a2, n2, a3, f3, a4, f4, a5, f5, s5] = sc.on;
    deq([a1.started, a1.failed, f1], [['plan', 'mail'], [], ['daily_plan.yml:-', 'exit_plan.yml:true']], '07:31');
    deq(req, { url: 'https://api.github.com/repos/emadsaberbahbah-coder/tadawul-fast/actions/workflows/daily_plan.yml/dispatches', method: 'post',
               payload: { ref: 'main', inputs: { dry_run: 'false' } }, headers: { Authorization: 'Bearer ' + TOKEN, Accept: 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28' },
               mute: true, ct: 'application/json' }, 'the request');
    deq(s1, { hour: '2026-11-12 07', done: { plan: 1, mail: 1 }, tries: { plan: 1, mail: 1 } }, 'state');
    deq(log1, ['page refresh 2026-11-12 07: started plan, mail'], 'log');
    deq([a2.started, n2], [[], 0], '07:41 nothing new');
    deq([a3.started, f3], [['cons'], ['consistency_check.yml:-']], '07:51 the check');
    deq([a4.started, f4], [['plan', 'exit'], ['daily_plan.yml:-', 'exit_plan.yml:false']], '08:36 a new hour');
    deq([a5.hour, a5.started, f5], ['2026-11-13 00', ['exit', 'cons'], ['exit_plan.yml:false', 'consistency_check.yml:-']], '00:51');
    deq(s5, { hour: '2026-11-13 00', done: { exit: 1, cons: 1 }, tries: { exit: 1, cons: 1 } }, 'the 00 state');
    deq(sc.repo, ['https://api.github.com/repos/me/other/actions/workflows/daily_plan.yml/dispatches', 'dev',
                  { mode: 'on', hour: '2026-11-12 23', started: ['plan'], failed: [], would: [] }], 'repo and ref properties');
    const [r1, r2, r3, r4, rf4, rs, leaked] = sc.refused;
    deq([r1.started, r1.failed.length, r2.started, r2.failed.length, r3.started, r3.failed.length, r4.started, r4.failed.length, rf4],
        [['plan'], 1, [], 1, ['cons'], 1, [], 0, []], 'a refused lane is tried at the next tick, 3 times at most');
    ok(/^mail \(401: \{"message":"Bad credentials \[token\]"\}\)$/.test(r1.failed[0]), 'the refusal, redacted: ' + r1.failed[0]);
    deq(rs, { hour: '2026-11-12 07', done: { plan: 1, cons: 1 }, tries: { plan: 1, mail: 3, cons: 1 } }, 'tries');
    eq(leaked, false, 'the token never logged');
    deq(sc.thrown, [[], 1, true, { hour: '2026-11-12 09', done: {}, tries: { plan: 1 } }], 'a thrown fetch: not done, redacted, counted');
    deq(sc.notoken, [{ mode: 'on', hour: '2026-11-12 07', started: [], failed: [], would: [], error: 'no GitHub token (Script Property TFB_GH_DISPATCH_TOKEN)' }, 0, null,
                     ['page refresh 2026-11-12 07: no GitHub token (Script Property TFB_GH_DISPATCH_TOKEN)']], 'no token');
    deq(sc.skip, [['plan'], ['daily_plan.yml:-']], 'skip');
    parts.push(sc.on, sc.refused, sc.thrown, sc.notoken, sc.skip);
  });
  test('Q5 owner functions: install (one trigger, 10 min, PRD_MODE on only when unset), remove, status', () => {
    const [m1, tr1, mode1, m2, mode2, st1, rm1, rm2, tr2, titles, noTok] = sc.owner;
    eq(m1, 'Page refresh installed: prdTick every 10 min (replaced 1). PRD_MODE = on (set to on). GitHub token: MISSING - set TFB_GH_DISPATCH_TOKEN. ' +
       'Lanes (Riyadh): Daily Plan :20 07-23; Exit Plan :30 at 07 with the e-mail, :35 08-00; Consistency Check :50 07-00.', 'install notice');
    deq(tr1, [{ h: 'tfbDispatchDailySync', every: 10 }, { h: 'prdTick', every: 10 }], 'one trigger; the other kept');
    eq(mode1, 'on'); eq(mode2, 'observe', 'a mode already set is kept');
    ok(/PRD_MODE = observe\. GitHub token: MISSING/.test(m2), m2);
    eq(st1, 'Page refresh 1.0.0: 1 prdTick trigger(s); PRD_MODE = observe; token present; skip []; last hour {"hour":"2026-11-12 09","done":{"plan":1},"tries":{"plan":1}}; ' +
       'lanes plan daily_plan.yml :20 | mail exit_plan.yml :30 | exit exit_plan.yml :35 | cons consistency_check.yml :50', 'status');
    deq([rm1, rm2, tr2], ['Removed 1 page-refresh trigger(s); nothing starts the pages from here until it is installed again.', 'No page-refresh trigger was installed.',
                          [{ h: 'tfbDispatchDailySync', every: 10 }]], 'remove');
    ok(titles.every(t => t === 'Page refresh 1.0.0') && titles.length === 5, 'toasts'); eq(noTok, true, 'status never shows the token');
    parts.push(sc.owner);
  });
  test('Q6 mutants: ' + MUTANTS.length + ' single-line mutants are each caught', () => {
    const want = JSON.stringify(sc);
    for (const [label, from, to] of MUTANTS) {
      eq(SRC.split(from).length - 1, 1, 'mutant anchor unique: ' + label);
      const M = SRC.replace(from, () => to);
      let caught = false;
      try { caught = JSON.stringify(scenarios(M)) !== want; } catch (eM) { caught = true; }
      ok(caught, 'mutant not caught: ' + label);
    }
    parts.push(want);
  });
  return sha256(JSON.stringify(parts));
}

const digests = [];
let total = 0;
for (let run = 1; run <= 2; run++) {
  passed = 0; failed = 0;
  const d = battery();
  digests.push(d);
  total = passed + failed;
  console.log('run ' + run + ': ' + passed + '/' + total + ' digest ' + d);
  if (failed) { console.log('FAIL ' + passed + '/' + total); process.exit(1); }
}
if (digests[0] !== digests[1]) { console.log('FAIL x2 digest mismatch ' + digests.join(' ')); process.exit(1); }
console.log('PRD V100 Q0-Q6 x2 identical digest ' + digests[0].slice(0, 16) + ' | SHA256 ' + sha256(SRC).slice(0, 12));
console.log('PASS ' + passed + '/' + total);
