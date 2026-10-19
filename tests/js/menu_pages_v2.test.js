/* 01_Menu Decision Pages v2 block v1.0.0: the "🆕 Decision Pages v2" menu that
 * refreshes Portfolio_Decision_v2 and Top_10_Investments_v2 on demand. Runs the
 * REAL block in a vm (the shared Apps Script namespace) with service stubs and
 * a recording Ui. The v2 renderers are synthetic stand-ins: their real source
 * is not in the repository, which is why the block discovers and confirms them.
 *
 *   node tests/js/menu_pages_v2.test.js
 */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const FILE = path.join(__dirname, '../../apps_script/01_Menu_Decision_Pages_v2_append.gs');
const SRC = fs.readFileSync(FILE, 'utf8');
let passed = 0;
function check(label, fn) { fn(); passed++; console.log('PASS ' + label); }

/* Synthetic project functions loaded into the same namespace as the block. */
const PROJECT = `
var calls = [];
function dp2RefreshPortfolioDecisionV2() { calls.push('pd'); return 'OK'; }
function renderTop10InvestmentsV2() { calls.push('t10'); return {status: 'rendered 10 seats'}; }
function buildTop10V2Layout() { calls.push('layout'); }
function pdV2SelfTest() { calls.push('selftest'); }
function dt10V2RenderSeats_() { calls.push('private'); }
function refreshPortfolioDecision() { calls.push('v1-pd'); }
function refreshDecisionTop10() { calls.push('v1-t10'); }
function clearPortfolioDecisionV2() { calls.push('clear'); }
var DP2Lib = { render: function () { calls.push('lib:' + (this === DP2Lib)); return 'lib ok'; } };
`;

function world(o) {
  o = o || {};
  const props = Object.assign({}, o.props || {});
  const cache = {};
  const ui = {alerts: [], prompts: [], menus: [], promptReplies: (o.prompts || []).slice()};
  ui.ButtonSet = {OK: 'OK', OK_CANCEL: 'OK_CANCEL'};
  ui.Button = {OK: 'OK', CANCEL: 'CANCEL'};
  ui.alert = function (title, msg) { ui.alerts.push(String(msg === undefined ? title : msg)); return 'OK'; };
  ui.prompt = function (title, body) {
    ui.prompts.push(String(body));
    const r = ui.promptReplies.length ? ui.promptReplies.shift() : {button: 'CANCEL', text: ''};
    return {getSelectedButton() { return r.button; }, getResponseText() { return r.text; }};
  };
  ui.createMenu = function (title) {
    const m = {title, items: [], added: false};
    const api = {
      addItem(label, handler) { m.items.push({label, handler}); return api; },
      addSeparator() { m.items.push({sep: true}); return api; },
      addSubMenu(sub) { m.items.push({sub}); return api; },
      addToUi() { m.added = true; ui.menus.push(m); return api; },
      _m: m,
    };
    return api;
  };
  const sheets = {};
  (o.tabs || ['Portfolio_Decision_v2', 'Top_10_Investments_v2']).forEach(n => {
    sheets[n] = {name: n, getLastRow() { return 42; }};
  });
  const ss = {
    toasts: [], active: null,
    getSheetByName(n) { return sheets[n] || null; },
    setActiveSheet(sh) { ss.active = sh.name; return sh; },
    toast(msg) { ss.toasts.push(String(msg)); },
  };
  const ctx = {
    Math, JSON, Date, Object, String, Array, parseInt,
    Logger: {log() {}},
    PropertiesService: {getScriptProperties() { return {
      getProperty(k) { return Object.prototype.hasOwnProperty.call(props, k) ? props[k] : null; },
      setProperty(k, v) { props[k] = String(v); }, deleteProperty(k) { delete props[k]; }}; }},
    CacheService: {getDocumentCache() { return {
      get(k) { return Object.prototype.hasOwnProperty.call(cache, k) ? cache[k] : null; },
      put(k, v) { cache[k] = v; }, remove(k) { delete cache[k]; }}; }},
    LockService: {getDocumentLock() { return {tryLock() { return !o.lockBusy; }, releaseLock() {}}; }},
    SpreadsheetApp: {
      getUi() { if (o.noUi) throw new Error('no ui'); return ui; },
      getActiveSpreadsheet() { return ss; }, flush() {},
    },
  };
  vm.createContext(ctx);
  if (o.before) vm.runInContext(o.before, ctx, {filename: 'before.gs'});
  vm.runInContext(PROJECT, ctx, {filename: 'project.gs'});
  for (let i = 0; i < (o.loads || 1); i++) vm.runInContext(SRC, ctx, {filename: '01_Menu_Decision_Pages_v2_append.gs'});
  if (o.after) vm.runInContext(o.after, ctx, {filename: 'after.gs'});
  return {ctx, props, cache, ui, ss, calls: ctx.calls};
}

check('menu: one menu-bar entry, 7 items, every handler resolves and is public', () => {
  const w = world();
  assert.equal(w.ctx.tfbPagesV2AddMenu_(), true);
  assert.equal(w.ui.menus.length, 1);
  const m = w.ui.menus[0];
  assert.equal(m.title, '🆕 Decision Pages v2');
  const items = m.items.filter(i => i.handler);
  assert.equal(items.length, 7);
  assert.equal(m.items.filter(i => i.sep).length, 2);
  for (const it of items) {
    assert.equal(typeof w.ctx[it.handler], 'function', it.handler + ' must resolve');
    assert.ok(!/_$/.test(it.handler), it.handler + ' must be public');
  }
  assert.deepEqual(items.slice(0, 3).map(i => i.handler),
    ['tfbPagesV2RefreshPortfolioDecision', 'tfbPagesV2RefreshTop10', 'tfbPagesV2RefreshBoth']);
});

check('menu: building it without a Ui (trigger context) returns false and never throws', () => {
  const w = world({noUi: true});
  assert.equal(w.ctx.tfbPagesV2AddMenu_(), false);
});

check('menu: the submenu form returns a Menu the main builder can add', () => {
  const w = world();
  const sub = w.ctx.tfbPagesV2Menu_(w.ui);
  assert.equal(sub._m.title, '🆕 Decision Pages v2');
  assert.equal(sub._m.added, false);
});

check('discovery: page-specific renderer first; layout, self-test, clear, private, v1 and own names never suggested', () => {
  const w = world();
  const pd = Array.from(w.ctx.tfbPagesV2Candidates_(w.ctx.TFB_PAGES_V2_.pd));
  const t10 = Array.from(w.ctx.tfbPagesV2Candidates_(w.ctx.TFB_PAGES_V2_.t10));
  assert.equal(pd[0], 'dp2RefreshPortfolioDecisionV2');
  assert.equal(t10[0], 'renderTop10InvestmentsV2');
  for (const bad of ['buildTop10V2Layout', 'pdV2SelfTest', 'dt10V2RenderSeats_', 'refreshPortfolioDecision',
    'refreshDecisionTop10', 'clearPortfolioDecisionV2', 'tfbPagesV2RefreshBoth', 'tfbPagesV2Setup']) {
    assert.ok(!pd.includes(bad) && !t10.includes(bad), bad + ' must not be suggested');
  }
  assert.ok(!t10.includes('dp2RefreshPortfolioDecisionV2'), 'the PD renderer is not suggested for Top 10');
});

check('refresh: a saved function runs once, on its tab, with a done toast; nothing is prompted', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: 'dp2RefreshPortfolioDecisionV2'}});
  const r = w.ctx.tfbPagesV2RefreshPortfolioDecision();
  assert.equal(r.status, 'ok');
  assert.deepEqual(Array.from(w.calls), ['pd']);
  assert.equal(w.ui.prompts.length, 0);
  assert.equal(w.ss.active, 'Portfolio_Decision_v2');
  assert.match(w.ss.toasts[w.ss.toasts.length - 1], /Portfolio_Decision_v2: done in \d+ s - OK/);
  assert.deepEqual(Object.keys(w.cache), [], 'in-flight flag released');
});

check('first use: blank OK takes the top suggestion, saves it and runs it', () => {
  const w = world({prompts: [{button: 'OK', text: ''}]});
  const r = w.ctx.tfbPagesV2RefreshTop10();
  assert.equal(r.status, 'ok');
  assert.equal(w.props.TFB_V2_T10_REFRESH_FN, 'renderTop10InvestmentsV2');
  assert.deepEqual(Array.from(w.calls), ['t10']);
  assert.match(w.ui.prompts[0], /1\. renderTop10InvestmentsV2/);
  assert.match(w.ss.toasts[w.ss.toasts.length - 1], /rendered 10 seats/);
});

check('first use: Cancel saves nothing and runs nothing', () => {
  const w = world({prompts: [{button: 'CANCEL', text: ''}]});
  const r = w.ctx.tfbPagesV2RefreshPortfolioDecision();
  assert.equal(r.status, 'not_configured');
  assert.equal(w.props.TFB_V2_PD_REFRESH_FN, undefined);
  assert.deepEqual(Array.from(w.calls), []);
  assert.match(w.ss.toasts[w.ss.toasts.length - 1], /Nothing refreshed/);
});

check('setup: a number picks that suggestion; an unknown name or this block\'s own name is refused', () => {
  const w = world({prompts: [{button: 'OK', text: '1'}, {button: 'OK', text: 'noSuchRendererV2'}]});
  const out = w.ctx.tfbPagesV2Setup();
  assert.equal(out.pd, 'dp2RefreshPortfolioDecisionV2');
  assert.equal(out.t10, null);
  assert.equal(w.props.TFB_V2_T10_REFRESH_FN, undefined);
  assert.match(w.ui.alerts[0], /No function named "noSuchRendererV2"/);
  const w2 = world({prompts: [{button: 'OK', text: 'tfbPagesV2RefreshBoth'}]});
  assert.equal(w2.ctx.tfbPagesV2RefreshPortfolioDecision().status, 'not_configured');
  assert.match(w2.ui.alerts[0], /part of this menu block/);
  assert.deepEqual(Array.from(w2.calls), []);
});

check('setup: any existing function may be typed, including a library member called on its library', () => {
  const w = world({prompts: [{button: 'OK', text: 'DP2Lib.render'}]});
  const r = w.ctx.tfbPagesV2RefreshPortfolioDecision();
  assert.equal(r.status, 'ok');
  assert.equal(w.props.TFB_V2_PD_REFRESH_FN, 'DP2Lib.render');
  assert.deepEqual(Array.from(w.calls), ['lib:true']);
});

check('a saved name that no longer exists asks again instead of guessing', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: 'goneRendererV2'}, prompts: [{button: 'CANCEL', text: ''}]});
  const r = w.ctx.tfbPagesV2RefreshPortfolioDecision();
  assert.equal(r.status, 'not_configured');
  assert.match(w.ui.alerts[0], /goneRendererV2\) is not in the project/);
  assert.deepEqual(Array.from(w.calls), []);
  assert.equal(w.props.TFB_V2_PD_REFRESH_FN, 'goneRendererV2', 'nothing overwritten on cancel');
});

check('a renderer that throws: alert names the function and error; the flag is released for the next click', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: 'boomV2'},
    after: 'function boomV2() { calls.push("boom"); throw new Error("HTTP 503 from backend"); }'});
  const r = w.ctx.tfbPagesV2RefreshPortfolioDecision();
  assert.equal(r.status, 'partial');
  assert.match(w.ui.alerts[0], /FAILED in boomV2\(\) - HTTP 503 from backend/);
  assert.deepEqual(Object.keys(w.cache), []);
  w.ctx.tfbPagesV2RefreshPortfolioDecision();
  assert.deepEqual(Array.from(w.calls), ['boom', 'boom']);
});

check('a second click while the page is refreshing is refused without running it', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: 'dp2RefreshPortfolioDecisionV2'}});
  w.cache.TFB_PAGES_V2_INFLIGHT_pd = '1';
  const r = w.ctx.tfbPagesV2RefreshPortfolioDecision();
  assert.equal(r.results[0].status, 'busy');
  assert.deepEqual(Array.from(w.calls), []);
  assert.match(w.ui.alerts[0], /already refreshing/);
  assert.equal(w.cache.TFB_PAGES_V2_INFLIGHT_pd, '1', 'someone else\'s flag is left alone');
});

check('a busy document lock is not a running refresh: the click still runs', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: 'dp2RefreshPortfolioDecisionV2'}, lockBusy: true});
  assert.equal(w.ctx.tfbPagesV2RefreshPortfolioDecision().status, 'ok');
  assert.deepEqual(Array.from(w.calls), ['pd']);
});

check('the live manual-priority wrapper is used when present; a wrapper that does not run is reported, not "done"', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: 'dp2RefreshPortfolioDecisionV2'},
    after: 'var wrapped = []; function menuRunWithManualRefreshPriority_(label, fn) { wrapped.push(label); return fn(); }'});
  assert.equal(w.ctx.tfbPagesV2RefreshPortfolioDecision().status, 'ok');
  assert.deepEqual(Array.from(w.ctx.wrapped), ['Refresh Portfolio_Decision_v2']);
  const w2 = world({props: {TFB_V2_PD_REFRESH_FN: 'dp2RefreshPortfolioDecisionV2'},
    after: 'function menuRunWithManualRefreshPriority_(label, fn) { return {status: "queued behind auto refresh"}; }'});
  const r2 = w2.ctx.tfbPagesV2RefreshPortfolioDecision();
  assert.equal(r2.results[0].status, 'deferred');
  assert.deepEqual(Array.from(w2.calls), []);
  assert.match(w2.ui.alerts[0], /not run now .*queued behind auto refresh/);
});

check('refresh both: Portfolio first, then Top 10; one function saved for both runs once', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: 'dp2RefreshPortfolioDecisionV2', TFB_V2_T10_REFRESH_FN: 'renderTop10InvestmentsV2'}});
  assert.equal(w.ctx.tfbPagesV2RefreshBoth().status, 'ok');
  assert.deepEqual(Array.from(w.calls), ['pd', 't10']);
  const w2 = world({props: {TFB_V2_PD_REFRESH_FN: 'DP2Lib.render', TFB_V2_T10_REFRESH_FN: 'DP2Lib.render'}});
  assert.equal(w2.ctx.tfbPagesV2RefreshBoth().status, 'ok');
  assert.deepEqual(Array.from(w2.calls), ['lib:true']);
  assert.match(w2.ss.toasts[w2.ss.toasts.length - 1], /same function as Portfolio_Decision_v2/);
});

check('refresh both: if Top 10 is not configured and setup is cancelled, nothing runs', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: 'dp2RefreshPortfolioDecisionV2'}, prompts: [{button: 'CANCEL', text: ''}]});
  assert.equal(w.ctx.tfbPagesV2RefreshBoth().status, 'not_configured');
  assert.deepEqual(Array.from(w.calls), []);
});

check('open: existing tab is activated; a missing tab is reported', () => {
  const w = world({tabs: ['Portfolio_Decision_v2']});
  assert.equal(w.ctx.tfbPagesV2OpenPortfolioDecision(), true);
  assert.equal(w.ss.active, 'Portfolio_Decision_v2');
  assert.equal(w.ctx.tfbPagesV2OpenTop10(), false);
  assert.match(w.ui.alerts[0], /Top_10_Investments_v2 does not exist/);
});

check('status: tabs, saved functions, suggestions and wrapper presence in one alert', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: 'dp2RefreshPortfolioDecisionV2', TFB_V2_T10_REFRESH_FN: 'goneV2'}});
  const rep = w.ctx.tfbPagesV2Status();
  assert.equal(rep.pd.resolves, true);
  assert.equal(rep.t10.resolves, false);
  const msg = w.ui.alerts[0];
  assert.match(msg, /Portfolio_Decision_v2:\n {2}tab: present \(42 rows\)/);
  assert.match(msg, /refresh function: goneV2 \(NOT FOUND\)/);
  assert.match(msg, /suggestions: renderTop10InvestmentsV2/);
  assert.match(msg, /Manual-priority wrapper: not in this project/);
});

check('decision-owned: after the v2.19.0 block both tabs join MENU_DECISION_OWNED_EXTRA_ once; without it, a no-op', () => {
  const before = "var MENU_DECISION_OWNED_EXTRA_ = ['Portfolio_Decision'];" +
    'function menuIsDecisionOwnedPage_(p) { return MENU_DECISION_OWNED_EXTRA_.indexOf(String(p)) >= 0; }';
  const w = world({before, loads: 2});
  assert.deepEqual(Array.from(w.ctx.MENU_DECISION_OWNED_EXTRA_),
    ['Portfolio_Decision', 'Portfolio_Decision_v2', 'Top_10_Investments_v2']);
  assert.equal(w.ctx.tfbPagesV2DecisionOwned_(), true);
  const w2 = world();
  assert.equal(typeof w2.ctx.MENU_DECISION_OWNED_EXTRA_, 'undefined');
  assert.equal(w2.ctx.tfbPagesV2DecisionOwned_(), false);
});

check('conventions: ES5 source, no onOpen of its own, no trigger, no service call at load, version 1.0.0', () => {
  assert.doesNotMatch(SRC, /\blet\s|\bconst\s|=>|`/);
  assert.doesNotMatch(SRC, /function\s+onOpen\s*\(|function\s+onEdit\s*\(|function\s+onInstall\s*\(/);
  assert.doesNotMatch(SRC, /ScriptApp\s*\.\s*newTrigger/);
  const w = world();
  assert.equal(w.ctx.TFB_PAGES_V2_MENU_VERSION, '1.0.0');
  assert.equal(w.ui.menus.length + w.ui.alerts.length + w.ss.toasts.length, 0, 'loading the block does nothing visible');
});

console.log('menu_pages_v2: ' + passed + ' checks passed');
