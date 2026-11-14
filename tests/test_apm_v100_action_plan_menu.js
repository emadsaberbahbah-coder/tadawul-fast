#!/usr/bin/env node
/* tests/test_apm_v100_action_plan_menu.js
 * Harness for apps_script/30_Action_Plan_Menu.gs v1.0.0 (a new file): its own menu for the Today Action Plan, whose
 * items start GitHub workflows with the sync start's token. The battery runs twice; the two digests must match; the
 * last line is 'PASS k/k'. */
'use strict';
const fs = require('fs'), path = require('path'), crypto = require('crypto'), vm = require('vm');
const SRC = fs.readFileSync(process.argv[2] || path.join(__dirname, '..', 'apps_script', '30_Action_Plan_Menu.gs'), 'utf8');
const sha256 = s => crypto.createHash('sha256').update(s).digest('hex');
const plain = v => (v === undefined ? undefined : JSON.parse(JSON.stringify(v)));
const TOKEN = 'ghp_TESTTOKEN0123456789';
let passed = 0, failed = 0;
function test(name, fn) { try { fn(); passed++; console.log('PASS ' + name); } catch (e) { failed++; console.log('FAIL ' + name + '\n  ' + String(e && e.stack || e).split('\n').slice(0, 5).join('\n  ')); } }
function ok(c, m) { if (!c) throw new Error(m || 'assertion failed'); }
function deq(a, b, m) { const ja = JSON.stringify(a), jb = JSON.stringify(b); if (ja !== jb) throw new Error((m || 'deep-equal') + ': ' + ja.slice(0, 700) + ' !== ' + jb.slice(0, 700)); }

function world(props, answers, sheets) { return { props: Object.assign({}, props || {}), answers: answers || {}, sheets: sheets || {}, triggers: [], fetches: [], logs: [], toasts: [], menus: [], active: null, uiThrows: false }; }
function env(src, w) {
  const ss = { toast(m, t, s) { w.toasts.push([String(m), String(t), s]); },
    getSheetByName(n) { return Object.prototype.hasOwnProperty.call(w.sheets, n) ? { name: n, getRange() { return { getDisplayValue: () => w.sheets[n] }; } } : null; },
    setActiveSheet(sh) { w.active = sh.name; } };
  const ctx = {
    PropertiesService: { getScriptProperties: () => ({ getProperty: k => (Object.prototype.hasOwnProperty.call(w.props, k) ? w.props[k] : null) }) },
    UrlFetchApp: { fetch(url, o) {
      w.fetches.push({ url, method: o.method, payload: JSON.parse(o.payload), auth: o.headers.Authorization, mute: o.muteHttpExceptions });
      const wf = url.split('/workflows/')[1].split('/')[0], a = w.answers[wf];
      if (a === 'throw') throw new Error('Address unavailable ' + o.headers.Authorization);
      const code = typeof a === 'number' ? a : 204;
      return { getResponseCode: () => code, getContentText: () => (code === 204 ? '' : '{"message":"Bad credentials ' + TOKEN + '"}') };
    } },
    Logger: { log(m) { w.logs.push(String(m)); } },
    SpreadsheetApp: { getActive: () => ss, getUi() {
      if (w.uiThrows) throw new Error('no UI in this context');
      return { createMenu(name) { const m = { name, items: [] }; const b = { addItem(l, f) { m.items.push([l, f]); return b; }, addSeparator() { m.items.push('---'); return b; }, addToUi() { w.menus.push(m); } }; return b; } };
    } },
    ScriptApp: {
      getProjectTriggers() { return w.triggers.map(t => ({ getHandlerFunction: () => t.h, _t: t })); },
      deleteTrigger(tr) { w.triggers = w.triggers.filter(t => t !== tr._t); },
      newTrigger(h) { const t = { h, kind: '' }; const b = { forSpreadsheet() { t.kind += 'ss'; return b; }, onOpen() { t.kind += ':open'; return b; }, create() { w.triggers.push(t); return t; } }; return b; }
    }
  };
  vm.createContext(ctx); vm.runInContext(src, ctx, { filename: '30_Action_Plan_Menu.gs' });
  return ctx;
}
const ON = { TFB_GH_DISPATCH_TOKEN: TOKEN };

function scenarios(src) {
  const out = {};
  let w = world(ON), e = env(src, w);
  out.pure = [plain(e.apmRequestCore_('o/r', 'today_action_plan.yml', 'main', { dry_run: 'false' }, 'T')), e.apmRedact_('a T b T', 'T'), e.apmRedact_(null, 'T'), e.apmTrim_('  x '), e.apmTrim_(null)];
  e.apmOnOpen(); out.menu = plain(w.menus);
  out.now = [e.apmRefreshNow(), plain(w.fetches), w.toasts.slice(-1)[0][1]];
  w = world(Object.assign({ TFB_GH_DISPATCH_REPO: ' me/other ', TFB_GH_DISPATCH_REF: 'dev' }, ON)); e = env(src, w);
  out.all = [e.apmRefreshAll(), w.fetches.map(f => [f.url, f.payload])];
  w = world({}); e = env(src, w);
  out.notoken = [e.apmRefreshNow(), e.apmRefreshAll(), w.fetches.length];
  w = world(ON, { 'today_action_plan.yml': 401, 'exit_plan.yml': 'throw' }); e = env(src, w);
  out.refused = [e.apmRefreshNow(), e.apmRefreshAll(), w.fetches.length, w.logs.concat(w.toasts.map(t => t[0])).some(l => l.indexOf(TOKEN) >= 0)];
  w = world(ON, {}, { Today_Action_Plan: 'Built 2026-11-12 09:00 Riyadh - today_action_plan 1.0.0 - copied from the plan pages and more text' }); e = env(src, w);
  w.triggers = [{ h: 'apmOnOpen', kind: 'old' }, { h: 'prdTick', kind: 'time' }];
  const m1 = e.apmInstallMenu(), tr1 = plain(w.triggers), menus1 = w.menus.length;
  const st = e.apmStatus(), op = e.apmOpenPage(), act = w.active;
  w.uiThrows = true; const m2 = e.apmInstallMenu(), tr2 = plain(w.triggers);
  const rm1 = e.apmRemoveMenu(), rm2 = e.apmRemoveMenu(), tr3 = plain(w.triggers);
  out.install = [m1, tr1, menus1, st, op, act, m2, tr2, rm1, rm2, tr3, st.indexOf(TOKEN) < 0];
  w = world({}); e = env(src, w);
  out.nopage = [e.apmOpenPage(), w.active, e.apmStatus(), e.apmInstallMenu()];
  return out;
}

const MUTANTS = [
  ['no token check', "  if (!token) return 'no GitHub token (Script Property ' + APM_.PROP_TOKEN + ')';\n", ''],
  ['any answer accepted', "  return code === 204 ? '' :", "  return code ? '' :"],
  ['token in the message', 'apmRedact_(body, token).slice(0, 120)', 'String(body).slice(0, 120)'],
  ['the e-mail on a menu refresh', "{ dry_run: 'false', send_mail: 'false' }", "{ dry_run: 'false', send_mail: 'true' }"],
  ['the wrong workflow', "WF_PAGE: 'today_action_plan.yml'", "WF_PAGE: 'daily_plan.yml'"],
  ['ref ignored', 'apmTrim_(p.getProperty(APM_.PROP_REF)) || APM_.DEFAULT_REF', 'APM_.DEFAULT_REF'],
  ['old triggers kept', '  var old = apmRemoveTrigger_();\n  ScriptApp.newTrigger', '  var old = 0;\n  ScriptApp.newTrigger'],
  ['a time trigger, not on open', '.forSpreadsheet(SpreadsheetApp.getActive()).onOpen().create();', '.forSpreadsheet(SpreadsheetApp.getActive()).create();'],
  ['removes every trigger', 'if (trs[i].getHandlerFunction() === APM_.HANDLER) { ScriptApp.deleteTrigger', 'if (true) { ScriptApp.deleteTrigger'],
  ['menu item dropped', "    .addItem('Status', 'apmStatus')\n", ''],
  ['dry run by default', "apmDispatch_(APM_.WF_PAGE, { dry_run: 'false' })", "apmDispatch_(APM_.WF_PAGE, { dry_run: 'true' })"]
];

function battery() {
  const parts = []; let sc = null;
  test('A0 compile; version lockstep; inventory; ES5 and ASCII in the whole file; LF', () => {
    new vm.Script(SRC);
    ok(SRC.indexOf("var APM_VERSION = '1.0.0';   // lockstep with the header") > 0 && / \* Version: 1\.0\.0 \(2026-11-12\)/.test(SRC.split('\n')[3]), 'lockstep');
    const fns = []; let m; const re = /^function\s+([A-Za-z_$][\w$]*)\s*\(/mg; while ((m = re.exec(SRC)) !== null) fns.push(m[1]);
    deq(fns.sort(), ['apmDispatch_', 'apmInstallMenu', 'apmNotify_', 'apmOnOpen', 'apmOpenPage', 'apmRedact_', 'apmRefreshAll', 'apmRefreshNow', 'apmRemoveMenu', 'apmRemoveTrigger_', 'apmRequestCore_', 'apmStatus', 'apmTrim_'], 'functions');
    ok(SRC.indexOf('\r') < 0, 'LF');
    for (let i = 0; i < SRC.length; i++) ok(SRC.charCodeAt(i) < 128, 'ASCII near ' + JSON.stringify(SRC.slice(Math.max(0, i - 20), i + 3)));
    const code = SRC.split('\n').filter(l => !/^\s*(\*|\/\*)/.test(l)).join('\n');
    ok(!/=>|`|\blet\s|\bconst\s|\bclass\s|\.\.\.[A-Za-z_]/.test(code), 'ES5');
    ok(SRC.indexOf('01_Menu') > 0 && !/function\s+onOpen\s*\(/.test(SRC), 'never defines onOpen: the existing menu is untouched');
  });
  test('A1 the menu: its own top-level menu, four items', () => {
    sc = scenarios(SRC);
    deq(sc.menu, [{ name: '✅ Today Action Plan', items: [['Build / refresh now', 'apmRefreshNow'], ['Refresh the plan pages first (Daily Plan + Exit Plan)', 'apmRefreshAll'], '---', ['Open the page', 'apmOpenPage'], ['Status', 'apmStatus']] }]);
    parts.push(sc.menu);
  });
  test('A2 requests: URL, payload, bearer; repo and ref properties; no e-mail from the menu', () => {
    deq(sc.pure[0], { url: 'https://api.github.com/repos/o/r/actions/workflows/today_action_plan.yml/dispatches', options: { method: 'post', contentType: 'application/json', payload: '{"ref":"main","inputs":{"dry_run":"false"}}', muteHttpExceptions: true, headers: { Authorization: 'Bearer T', Accept: 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28' } } });
    deq(sc.pure.slice(1), ['a [token] b [token]', '', 'x', '']);
    deq(sc.now, ['Today Action Plan: build started. The page updates in about one minute.', [{ url: 'https://api.github.com/repos/emadsaberbahbah-coder/tadawul-fast/actions/workflows/today_action_plan.yml/dispatches', method: 'post', payload: { ref: 'main', inputs: { dry_run: 'false' } }, auth: 'Bearer ' + TOKEN, mute: true }], 'Today Action Plan 1.0.0']);
    deq(sc.all, ['Daily Plan and Exit Plan started. The Today Action Plan rebuilds by itself when they finish (about 2 minutes).', [['https://api.github.com/repos/me/other/actions/workflows/daily_plan.yml/dispatches', { ref: 'dev', inputs: { dry_run: 'false' } }], ['https://api.github.com/repos/me/other/actions/workflows/exit_plan.yml/dispatches', { ref: 'dev', inputs: { dry_run: 'false', send_mail: 'false' } }]]]);
    parts.push(sc.pure, sc.now, sc.all);
  });
  test('A3 failures: no token (no fetch), a refusal and a thrown fetch, the token never shown', () => {
    deq(sc.notoken, ['Not started: no GitHub token (Script Property TFB_GH_DISPATCH_TOKEN)', 'Not all started: no GitHub token (Script Property TFB_GH_DISPATCH_TOKEN); no GitHub token (Script Property TFB_GH_DISPATCH_TOKEN)', 0]);
    deq(sc.refused, ['Not started: today_action_plan.yml was not accepted (401: {"message":"Bad credentials [token]"})', 'Not all started: exit_plan.yml was not accepted (no answer: Address unavailable Bearer [token])', 3, false]);
    parts.push(sc.notoken, sc.refused);
  });
  test('A4 install (one on-open trigger, others kept), status, open, remove', () => {
    const [m1, tr1, menus1, st, op, act, m2, tr2, rm1, rm2, tr3, noTok] = sc.install;
    deq([m1, tr1, menus1], ['Today Action Plan menu installed (replaced 1). It is in the menu bar now. GitHub token: present.', [{ h: 'prdTick', kind: 'time' }, { h: 'apmOnOpen', kind: 'ss:open' }], 1]);
    deq([st, op, act], ['Today Action Plan menu 1.0.0: 1 menu trigger(s); GitHub token present; page: Built 2026-11-12 09:00 Riyadh - today_action_plan 1.0.0 - co', 'Today_Action_Plan', 'Today_Action_Plan']);
    deq([m2, tr2.length], ['Today Action Plan menu installed (replaced 1). It appears when the workbook is opened again. GitHub token: present.', 2]);
    deq([rm1, rm2, tr3, noTok], ['Removed 1 menu trigger(s); the menu is gone the next time the workbook opens.', 'No Today Action Plan menu trigger was installed.', [{ h: 'prdTick', kind: 'time' }], true]);
    deq(sc.nopage, ['The Today_Action_Plan page does not exist yet: choose "Build / refresh now" first.', null, 'Today Action Plan menu 1.0.0: 0 menu trigger(s); GitHub token MISSING; page: the page does not exist yet', 'Today Action Plan menu installed. It is in the menu bar now. GitHub token: MISSING - set TFB_GH_DISPATCH_TOKEN.']);
    parts.push(sc.install, sc.nopage);
  });
  test('A5 mutants: ' + MUTANTS.length + ' single-line mutants are each caught', () => {
    const want = JSON.stringify(sc);
    for (const [label, from, to] of MUTANTS) {
      ok(SRC.split(from).length - 1 === 1, 'mutant anchor unique: ' + label);
      let caught = false;
      try { caught = JSON.stringify(scenarios(SRC.replace(from, () => to))) !== want; } catch (eM) { caught = true; }
      ok(caught, 'mutant not caught: ' + label);
    }
    parts.push(want);
  });
  return sha256(JSON.stringify(parts));
}
const digests = []; let total = 0;
for (let run = 1; run <= 2; run++) { passed = 0; failed = 0; digests.push(battery()); total = passed + failed; if (failed) { console.log('FAIL ' + passed + '/' + total); process.exit(1); } }
if (digests[0] !== digests[1]) { console.log('FAIL x2 digest mismatch'); process.exit(1); }
console.log('APM V100 A0-A5 x2 identical digest ' + digests[0].slice(0, 16) + ' | SHA256 ' + sha256(SRC).slice(0, 12));
console.log('PASS ' + passed + '/' + total);
