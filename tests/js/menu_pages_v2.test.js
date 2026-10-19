/* 01_Menu Decision Pages v2 block v1.0.0: the "🆕 Decision Pages v2" menu that
 * refreshes Portfolio_Decision_v2 and Top_10_Investments_v2 on demand. Runs the
 * REAL block in a vm (the shared Apps Script namespace) with recording service
 * stubs and a recording Ui. The v2 renderers are synthetic stand-ins: their
 * real source is not in the repository, which is why the block discovers and
 * confirms them.
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

/* Independent copy of the never-suggest words: dropping one from the block fails. */
const EXCLUDED_WORDS = ['test', 'selftest', 'install', 'uninstall', 'remove', 'delete', 'clear', 'reset',
  'purge', 'trigger', 'triggers', 'status', 'diag', 'diagnose', 'diagnostics', 'debug', 'menu', 'onopen',
  'onedit', 'oninstall', 'setup', 'probe', 'audit', 'log', 'seed', 'layout'];
const excludedFns = EXCLUDED_WORDS.map(w => w + 'PortfolioDecisionV2');

/* Synthetic project functions loaded into the same namespace as the block.
 * Expected PD ranking: dp2RefreshPortfolioDecisionV2 (hint+strong verb),
 * buildPortfolioV2 (hint+weak verb), portfolioDecisionV2 (hint only), then the
 * page-neutral dp2Run and refreshAllPagesV2 (strong verb, name order). */
const PROJECT = `
var calls = [];
var seenDuringRun = [];
function dp2RefreshPortfolioDecisionV2() { calls.push('pd'); seenDuringRun.push(probe()); return 'OK'; }
function buildPortfolioV2() { calls.push('pd-build'); }
function portfolioDecisionV2() { calls.push('pd-bare'); }
function refreshAllPagesV2() { calls.push('all'); }
function dp2Run() { calls.push('dp2'); }
function renderTop10InvestmentsV2() { calls.push('t10'); seenDuringRun.push(probe()); return {status: 'rendered 10 seats'}; }
function buildTop10V2Layout() { calls.push('layout'); }
function pdV2SelfTest() { calls.push('selftest'); }
function dt10V2RenderSeats_() { calls.push('private'); }
function refreshPortfolioDecision() { calls.push('v1-pd'); }
function refreshDecisionTop10() { calls.push('v1-t10'); }
${excludedFns.map(n => 'function ' + n + '() { calls.push("' + n + '"); }').join('\n')}
var DP2Lib = { render: function () { calls.push('lib:' + (this === DP2Lib)); return 'lib ok'; } };
`;
const LEXICAL = 'const renderConstPortfolioDecisionV2 = function () { calls.push("const"); return "const ok"; };';

function world(o) {
  o = o || {};
  const props = Object.assign({}, o.props || {});
  const cache = {};
  const svc = {props: 0, cache: 0, lock: 0, sheets: 0, ui: 0};
  const lockLog = {tries: 0, releases: 0};
  const ttls = [];
  const clock = {now: Date.parse('2026-10-19T13:00:00Z')};
  class FixedDate extends Date {
    constructor(...a) { super(...(a.length ? a : [clock.now])); }
    static now() { return clock.now; }
  }
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
  const docCache = {
    get(k) { return Object.prototype.hasOwnProperty.call(cache, k) ? cache[k] : null; },
    put(k, v, ttl) { cache[k] = v; ttls.push(ttl); }, remove(k) { delete cache[k]; }};
  const ctx = {
    Math, JSON, Object, String, Array, parseInt, Date: FixedDate,
    Logger: {log() {}},
    PropertiesService: {getScriptProperties() { svc.props++; return {
      getProperty(k) { return Object.prototype.hasOwnProperty.call(props, k) ? props[k] : null; },
      setProperty(k, v) { props[k] = String(v); }, deleteProperty(k) { delete props[k]; }}; }},
    CacheService: {getDocumentCache() { svc.cache++; if (o.noCache) throw new Error('cache unavailable'); return docCache; },
      getScriptCache() { return docCache; }},
    LockService: {getDocumentLock() { svc.lock++; return {
      tryLock() { lockLog.tries++; return !o.lockBusy; }, releaseLock() { lockLog.releases++; }}; }},
    SpreadsheetApp: {
      getUi() { svc.ui++; if (o.noUi) throw new Error('no ui'); return ui; },
      getActiveSpreadsheet() { svc.sheets++; return ss; }, flush() {},
    },
  };
  vm.createContext(ctx);
  if (o.before) vm.runInContext(o.before, ctx, {filename: 'before.gs'});
  vm.runInContext(PROJECT, ctx, {filename: 'project.gs'});
  vm.runInContext(LEXICAL, ctx, {filename: 'lexical.gs'});
  const globalsBefore = Object.keys(ctx).sort();
  vm.runInContext(SRC, ctx, {filename: '01_Menu_Decision_Pages_v2_append.gs'});
  const svcAtLoad = Object.assign({}, svc);
  ctx.probe = () => ({active: ss.active, flags: Object.keys(cache)});
  if (o.after) vm.runInContext(o.after, ctx, {filename: 'after.gs'});
  return {ctx, props, cache, ui, ss, calls: ctx.calls, svcAtLoad, lockLog, ttls, clock, globalsBefore};
}
const arr = (x) => Array.from(x);
const lastToast = (w) => w.ss.toasts[w.ss.toasts.length - 1];
const PD_FN = 'dp2RefreshPortfolioDecisionV2';
const T10_FN = 'renderTop10InvestmentsV2';
const BOTH = {TFB_V2_PD_REFRESH_FN: PD_FN, TFB_V2_T10_REFRESH_FN: T10_FN};

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
  assert.deepEqual(items.map(i => i.handler), ['tfbPagesV2RefreshPortfolioDecision', 'tfbPagesV2RefreshTop10',
    'tfbPagesV2RefreshBoth', 'tfbPagesV2OpenPortfolioDecision', 'tfbPagesV2OpenTop10', 'tfbPagesV2Setup',
    'tfbPagesV2Status']);
});

check('menu: without a Ui (trigger context) the hook returns false and never throws; the submenu form is not added', () => {
  assert.equal(world({noUi: true}).ctx.tfbPagesV2AddMenu_(), false);
  const w = world();
  const sub = w.ctx.tfbPagesV2Menu_(w.ui);
  assert.equal(sub._m.title, '🆕 Decision Pages v2');
  assert.equal(sub._m.added, false);
});

check('discovery: exact ranking, page-specific first; dp2-only names count; the other page\'s renderer is not suggested', () => {
  const w = world();
  assert.deepEqual(arr(w.ctx.tfbPagesV2Candidates_(w.ctx.TFB_PAGES_V2_.pd)),
    [PD_FN, 'buildPortfolioV2', 'portfolioDecisionV2', 'dp2Run', 'refreshAllPagesV2']);
  assert.deepEqual(arr(w.ctx.tfbPagesV2Candidates_(w.ctx.TFB_PAGES_V2_.t10)),
    [T10_FN, 'dp2Run', 'refreshAllPagesV2']);
});

check('discovery: no excluded word, private helper, v1 refresher or menu function is ever suggested', () => {
  const w = world();
  const all = arr(w.ctx.tfbPagesV2Candidates_(w.ctx.TFB_PAGES_V2_.pd))
    .concat(arr(w.ctx.tfbPagesV2Candidates_(w.ctx.TFB_PAGES_V2_.t10)));
  for (const bad of excludedFns.concat(['buildTop10V2Layout', 'pdV2SelfTest', 'dt10V2RenderSeats_',
    'refreshPortfolioDecision', 'refreshDecisionTop10', 'tfbPagesV2RefreshBoth', 'tfbPagesV2Setup'])) {
    assert.ok(!all.includes(bad), bad + ' must not be suggested');
  }
});

check('refresh: a saved function runs once, with its tab already active and its flag held while it runs', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: PD_FN}});
  const r = w.ctx.tfbPagesV2RefreshPortfolioDecision();
  assert.equal(r.status, 'ok');
  assert.deepEqual(arr(w.calls), ['pd']);
  assert.equal(w.ui.prompts.length, 0);
  assert.equal(w.ctx.seenDuringRun[0].active, 'Portfolio_Decision_v2', 'tab activated before the renderer runs');
  assert.deepEqual(arr(w.ctx.seenDuringRun[0].flags), ['TFB_PAGES_V2_INFLIGHT_' + PD_FN]);
  assert.deepEqual(Object.keys(w.cache), [], 'flag released afterwards');
  assert.deepEqual(w.ttls, [420], 'flag carries the 7-minute expiry');
  assert.equal(w.lockLog.releases, w.lockLog.tries, 'document lock released');
  assert.match(lastToast(w), /Portfolio_Decision_v2: done in \d+ s - OK/);
});

check('first use: blank OK takes the top suggestion, saves it and runs it; the result is shown', () => {
  const w = world({prompts: [{button: 'OK', text: ''}]});
  assert.equal(w.ctx.tfbPagesV2RefreshTop10().status, 'ok');
  assert.equal(w.props.TFB_V2_T10_REFRESH_FN, T10_FN);
  assert.deepEqual(arr(w.calls), ['t10']);
  assert.match(w.ui.prompts[0], /1\. renderTop10InvestmentsV2\n {2}2\. dp2Run/);
  assert.match(lastToast(w), /rendered 10 seats/);
});

check('first use: Cancel saves nothing and runs nothing', () => {
  const w = world({prompts: [{button: 'CANCEL', text: ''}]});
  assert.equal(w.ctx.tfbPagesV2RefreshPortfolioDecision().status, 'not_configured');
  assert.equal(w.props.TFB_V2_PD_REFRESH_FN, undefined);
  assert.deepEqual(arr(w.calls), []);
  assert.match(lastToast(w), /Nothing refreshed/);
});

check('setup: a number picks exactly that suggestion; 0 or past the list is refused, nothing saved or run', () => {
  const w = world({prompts: [{button: 'OK', text: '2'}, {button: 'OK', text: '3'}]});
  const out = w.ctx.tfbPagesV2Setup();
  assert.equal(out.pd, 'buildPortfolioV2');
  assert.equal(out.t10, 'refreshAllPagesV2');
  for (const bad of ['0', '6', '99']) {
    const w2 = world({prompts: [{button: 'OK', text: bad}]});
    assert.equal(w2.ctx.tfbPagesV2RefreshPortfolioDecision().status, 'not_configured');
    assert.match(w2.ui.alerts[0], new RegExp('no suggestion number ' + bad));
    assert.equal(w2.props.TFB_V2_PD_REFRESH_FN, undefined);
    assert.deepEqual(arr(w2.calls), []);
  }
});

check('setup: an unknown name is refused; the toast reports what is actually saved', () => {
  const w = world({props: {TFB_V2_T10_REFRESH_FN: T10_FN},
    prompts: [{button: 'OK', text: 'noSuchRendererV2'}, {button: 'CANCEL', text: ''}]});
  const out = w.ctx.tfbPagesV2Setup();
  assert.equal(out.pd, null);
  assert.equal(out.t10, T10_FN, 'cancel keeps the saved function and says so');
  assert.match(w.ui.alerts[0], /No function named "noSuchRendererV2"/);
  assert.match(lastToast(w), /Portfolio_Decision_v2 = \(none\) · Top_10_Investments_v2 = renderTop10InvestmentsV2/);
});

check('setup: a saved choice stays the default over the top suggestion; "-" removes it', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: 'portfolioDecisionV2'}, prompts: [{button: 'OK', text: ''}, {button: 'CANCEL', text: ''}]});
  w.ctx.tfbPagesV2Setup();
  assert.equal(w.props.TFB_V2_PD_REFRESH_FN, 'portfolioDecisionV2');
  assert.match(w.ui.prompts[0], /leave blank and press OK to use portfolioDecisionV2/);
  const w2 = world({props: {TFB_V2_PD_REFRESH_FN: PD_FN}, prompts: [{button: 'OK', text: '-'}, {button: 'CANCEL', text: ''}]});
  assert.equal(w2.ctx.tfbPagesV2Setup().pd, null);
  assert.equal(w2.props.TFB_V2_PD_REFRESH_FN, undefined);
});

check('setup: this block\'s own functions are refused, also through a dotted path, and never saved', () => {
  for (const own of ['tfbPagesV2RefreshBoth', 'globalThis.tfbPagesV2RefreshTop10', 'globalThis.globalThis.tfbPagesV2Setup']) {
    const w = world({props: {TFB_V2_PD_REFRESH_FN: 'goneV2'}, prompts: [{button: 'OK', text: own}]});
    assert.equal(w.ctx.tfbPagesV2RefreshPortfolioDecision().status, 'not_configured', own);
    assert.match(w.ui.alerts[1], /part of this menu block/, own);
    assert.equal(w.props.TFB_V2_PD_REFRESH_FN, 'goneV2', own + ' must not be saved');
    assert.deepEqual(arr(w.calls), []);
  }
});

check('setup: a library member runs on its library; a top-level const renderer can be typed in', () => {
  const w = world({prompts: [{button: 'OK', text: 'DP2Lib.render'}]});
  assert.equal(w.ctx.tfbPagesV2RefreshPortfolioDecision().status, 'ok');
  assert.equal(w.props.TFB_V2_PD_REFRESH_FN, 'DP2Lib.render');
  assert.deepEqual(arr(w.calls), ['lib:true']);
  const w2 = world({prompts: [{button: 'OK', text: 'renderConstPortfolioDecisionV2'}]});
  assert.equal(w2.ctx.tfbPagesV2RefreshPortfolioDecision().status, 'ok');
  assert.deepEqual(arr(w2.calls), ['const']);
  const w3 = world({prompts: [{button: 'OK', text: 'NoLib.render'}]});
  assert.equal(w3.ctx.tfbPagesV2RefreshPortfolioDecision().status, 'not_configured');
  assert.match(w3.ui.alerts[0], /No function named "NoLib.render"/);
});

check('a saved name that no longer exists asks again instead of guessing', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: 'goneRendererV2'}, prompts: [{button: 'CANCEL', text: ''}]});
  assert.equal(w.ctx.tfbPagesV2RefreshPortfolioDecision().status, 'not_configured');
  assert.match(w.ui.alerts[0], /goneRendererV2\) is not in the project/);
  assert.deepEqual(arr(w.calls), []);
  assert.equal(w.props.TFB_V2_PD_REFRESH_FN, 'goneRendererV2', 'nothing overwritten on cancel');
});

check('a renderer that throws: alert names the function and error; the flag is released for the next click', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: 'boomV2'},
    after: 'function boomV2() { calls.push("boom"); throw new Error("HTTP 503 from backend"); }'});
  assert.equal(w.ctx.tfbPagesV2RefreshPortfolioDecision().status, 'partial');
  assert.match(w.ui.alerts[0], /Portfolio_Decision_v2: FAILED - boomV2\(\): HTTP 503 from backend/);
  assert.deepEqual(Object.keys(w.cache), []);
  w.ctx.tfbPagesV2RefreshPortfolioDecision();
  assert.deepEqual(arr(w.calls), ['boom', 'boom']);
});

check('in-flight: a running function refuses a second click; another page\'s different function is not blocked', () => {
  const w = world({props: BOTH});
  w.cache['TFB_PAGES_V2_INFLIGHT_' + PD_FN] = '1';
  const r = w.ctx.tfbPagesV2RefreshPortfolioDecision();
  assert.equal(r.results[0].status, 'busy');
  assert.match(w.ui.alerts[0], /dp2RefreshPortfolioDecisionV2\(\) is already running/);
  assert.equal(w.cache['TFB_PAGES_V2_INFLIGHT_' + PD_FN], '1', 'someone else\'s flag is left alone');
  assert.equal(w.ctx.tfbPagesV2RefreshTop10().status, 'ok');
  assert.deepEqual(arr(w.calls), ['t10']);
  assert.equal(w.ctx.tfbPagesV2Status().pd.running, true);
});

check('in-flight: one function saved for both pages is guarded as one (the second page\'s click is refused)', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: 'refreshAllPagesV2', TFB_V2_T10_REFRESH_FN: 'refreshAllPagesV2'}});
  w.cache.TFB_PAGES_V2_INFLIGHT_refreshAllPagesV2 = '1';
  assert.equal(w.ctx.tfbPagesV2RefreshTop10().results[0].status, 'busy');
  assert.deepEqual(arr(w.calls), []);
});

check('a busy document lock is not a running refresh; a missing cache does not block either', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: PD_FN}, lockBusy: true});
  assert.equal(w.ctx.tfbPagesV2RefreshPortfolioDecision().status, 'ok');
  assert.equal(w.lockLog.releases, 0, 'a lock never acquired is never released');
  const w2 = world({props: {TFB_V2_PD_REFRESH_FN: PD_FN}, noCache: true});
  assert.equal(w2.ctx.tfbPagesV2RefreshPortfolioDecision().status, 'ok');
  assert.deepEqual(arr(w2.calls), ['pd']);
});

check('manual-priority wrapper: used when present; declining (any return) is "not run now", never "done"', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: PD_FN},
    after: 'var wrapped = []; function menuRunWithManualRefreshPriority_(label, fn) { wrapped.push(label); return fn(); }'});
  assert.equal(w.ctx.tfbPagesV2RefreshPortfolioDecision().status, 'ok');
  assert.deepEqual(arr(w.ctx.wrapped), ['Refresh Portfolio_Decision_v2']);
  for (const ret of ['{status: "queued behind auto refresh"}', 'undefined', 'false', 'null']) {
    const w2 = world({props: {TFB_V2_PD_REFRESH_FN: PD_FN},
      after: 'function menuRunWithManualRefreshPriority_(label, fn) { return ' + ret + '; }'});
    const r2 = w2.ctx.tfbPagesV2RefreshPortfolioDecision();
    assert.equal(r2.results[0].status, 'deferred', ret);
    assert.deepEqual(arr(w2.calls), []);
    assert.match(w2.ui.alerts[0], /not run now - the manual-refresh coordinator did not run dp2RefreshPortfolioDecisionV2\(\)/);
    assert.deepEqual(Object.keys(w2.cache), [], 'flag released after a deferral');
  }
  const w3 = world({props: {TFB_V2_PD_REFRESH_FN: PD_FN},
    after: 'function menuRunWithManualRefreshPriority_(label, fn) { throw new Error("coordinator state corrupt"); }'});
  w3.ctx.tfbPagesV2RefreshPortfolioDecision();
  assert.match(w3.ui.alerts[0], /coordinator failed before running dp2RefreshPortfolioDecisionV2\(\): coordinator state corrupt/);
});

check('refresh both: Portfolio first, then Top 10; one function saved for both runs once', () => {
  const w = world({props: BOTH});
  assert.equal(w.ctx.tfbPagesV2RefreshBoth().status, 'ok');
  assert.deepEqual(arr(w.calls), ['pd', 't10']);
  const w2 = world({props: {TFB_V2_PD_REFRESH_FN: 'DP2Lib.render', TFB_V2_T10_REFRESH_FN: 'DP2Lib.render'}});
  assert.equal(w2.ctx.tfbPagesV2RefreshBoth().status, 'ok');
  assert.deepEqual(arr(w2.calls), ['lib:true']);
  assert.match(lastToast(w2), /same function as Portfolio_Decision_v2, run once/);
});

check('refresh both: a Portfolio failure still runs a different Top 10 function; a shared failing function runs once', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: 'boomV2', TFB_V2_T10_REFRESH_FN: T10_FN},
    after: 'function boomV2() { calls.push("boom"); throw new Error("x"); }'});
  const r = w.ctx.tfbPagesV2RefreshBoth();
  assert.equal(r.status, 'partial');
  assert.deepEqual(arr(w.calls), ['boom', 't10']);
  const w2 = world({props: {TFB_V2_PD_REFRESH_FN: 'boomV2', TFB_V2_T10_REFRESH_FN: 'boomV2'},
    after: 'function boomV2() { calls.push("boom"); throw new Error("x"); }'});
  const r2 = w2.ctx.tfbPagesV2RefreshBoth();
  assert.deepEqual(arr(w2.calls), ['boom'], 'the failing shared function is not run a second time');
  assert.equal(r2.results[1].status, 'error');
  assert.match(w2.ui.alerts[0], /Top_10_Investments_v2: FAILED - boomV2\(\): x \(same function as Portfolio_Decision_v2, run once\)/);
});

check('refresh both: Top 10 is not started once Portfolio used 4 of the 6 minutes, and the alert says why', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: 'slowV2', TFB_V2_T10_REFRESH_FN: T10_FN}});
  vm.runInContext('function slowV2() { calls.push("slow"); }', w.ctx);
  const realSlow = w.ctx.slowV2;
  w.ctx.slowV2 = function () { w.clock.now += 250000; return realSlow(); };
  const r = w.ctx.tfbPagesV2RefreshBoth();
  assert.deepEqual(arr(w.calls), ['slow']);
  assert.equal(r.results[1].status, 'skipped_time');
  assert.match(w.ui.alerts[0], /Top_10_Investments_v2: not started, the first refresh used most of the 6-minute limit/);
});

check('refresh both: if Top 10 is not configured and setup is cancelled, nothing runs', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: PD_FN}, prompts: [{button: 'CANCEL', text: ''}]});
  assert.equal(w.ctx.tfbPagesV2RefreshBoth().status, 'not_configured');
  assert.deepEqual(arr(w.calls), []);
});

check('no Ui (run from a trigger or the editor without a sheet): unconfigured returns quietly, a failure never throws', () => {
  const w = world({noUi: true});
  assert.equal(w.ctx.tfbPagesV2RefreshPortfolioDecision().status, 'not_configured');
  assert.equal(w.ui.prompts.length, 0);
  const w2 = world({noUi: true, props: {TFB_V2_PD_REFRESH_FN: 'boomV2'},
    after: 'function boomV2() { throw new Error("x"); }'});
  assert.equal(w2.ctx.tfbPagesV2RefreshPortfolioDecision().status, 'partial');
  assert.equal(w2.ctx.tfbPagesV2Setup(), null);
});

check('open: existing tab is activated; a missing tab is reported', () => {
  const w = world({tabs: ['Portfolio_Decision_v2']});
  assert.equal(w.ctx.tfbPagesV2OpenPortfolioDecision(), true);
  assert.equal(w.ss.active, 'Portfolio_Decision_v2');
  assert.equal(w.ctx.tfbPagesV2OpenTop10(), false);
  assert.match(w.ui.alerts[0], /Top_10_Investments_v2 does not exist/);
});

check('status: tabs, saved functions, suggestions and wrapper presence in one alert and in the report', () => {
  const w = world({props: {TFB_V2_PD_REFRESH_FN: PD_FN, TFB_V2_T10_REFRESH_FN: 'goneV2'}});
  const rep = w.ctx.tfbPagesV2Status();
  assert.equal(rep.pd.resolves, true);
  assert.equal(rep.t10.resolves, false);
  assert.equal(rep.pd.running, false);
  assert.deepEqual(arr(rep.t10.candidates), [T10_FN, 'dp2Run', 'refreshAllPagesV2']);
  const msg = w.ui.alerts[0];
  assert.match(msg, /Portfolio_Decision_v2:\n {2}tab: present \(42 rows\)/);
  assert.match(msg, /refresh function: goneV2 \(NOT FOUND\)/);
  assert.match(msg, /suggestions: renderTop10InvestmentsV2, dp2Run, refreshAllPagesV2/);
  assert.match(msg, /Manual-priority wrapper: not in this project/);
  assert.equal(w.ctx.tfbPagesV2Summary_({message: 'm'}), 'm');
});

check('loading the block: no service call, no visible effect, no change to an existing global (MENU_DECISION_OWNED_EXTRA_)', () => {
  const before = "var MENU_DECISION_OWNED_EXTRA_ = ['Portfolio_Decision'];";
  const w = world({before});
  assert.deepEqual(w.svcAtLoad, {props: 0, cache: 0, lock: 0, sheets: 0, ui: 0});
  assert.equal(w.ui.menus.length + w.ui.alerts.length + w.ss.toasts.length, 0);
  assert.deepEqual(arr(w.ctx.MENU_DECISION_OWNED_EXTRA_), ['Portfolio_Decision']);
  const added = Object.keys(w.ctx).filter(k => !w.globalsBefore.includes(k) && k !== 'probe');
  assert.ok(added.length > 0);
  for (const k of added) assert.match(k, /^(tfbPagesV2|TFB_PAGES_V2)/, k + ' is outside the block\'s namespace');
});

check('conventions: ES5 source, no onOpen of its own, no trigger, version 1.0.0', () => {
  assert.doesNotMatch(SRC, /\blet\s|\bconst\s|=>|`/);
  assert.doesNotMatch(SRC, /function\s+onOpen\s*\(|function\s+onEdit\s*\(|function\s+onInstall\s*\(/);
  assert.doesNotMatch(SRC, /ScriptApp\s*\.\s*newTrigger/);
  assert.equal(world().ctx.TFB_PAGES_V2_MENU_VERSION, '1.0.0');
});

console.log('menu_pages_v2: ' + passed + ' checks passed');
