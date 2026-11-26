#!/usr/bin/env node
/* tests/test_gas_dispatch_retry_v120.js
 * Harness for apps_script/24_Sync_Dispatch.gs v1.2.0: a fault at GitHub is tried again.
 * WHY: 25 Nov 2026 18:47:58 Riyadh - the scorer's dispatch got HTTP 500, was sent once, no run started.
 * Loads the REAL .gs source into a vm context; UrlFetchApp answers from a scripted plan (one entry per
 * call, in order) and Utilities.sleep only records the wait. Every test calls the real functions.
 * Run: node tests/test_gas_dispatch_retry_v120.js   (exit 0 = all PASS; last line 'PASS n/n')
 */
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const SRC = path.join(__dirname, '..', 'apps_script', '24_Sync_Dispatch.gs');
const code = fs.readFileSync(SRC, 'utf8');
const TOKEN = 'github_pat_TESTTOKEN_0123456789abcdef';
const NOW = Date.parse('2026-11-25T15:47:58Z');          // 18:47:58 Riyadh, after the scorer's 15:20Z slot

function makeWorld(plan, opts) {
  const o = Object.assign({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, nowMs: NOW }, opts || {});
  const world = { fetches: [], logRows: [], logger: [], sleeps: [], props: Object.assign({}, o.props), nowMs: o.nowMs, plan: plan.slice() };
  const sheets = {
    '_Run_Log': { appendRow: (row) => { world.logRows.push(row); }, getDataRange: () => ({ getValues: () => [] }) },
    '_Sync_Control': { getDataRange: () => ({ getValues: () => [['Key', 'Value', '']] }) },
    '_Status': { getDataRange: () => ({ getValues: () => [['Page', 'Last Updated', 'Status']] }) }
  };
  const ctx = {
    Date, JSON, Math, Number, String, parseInt, isNaN, encodeURIComponent, Object, Array, RegExp, Error,
    PropertiesService: { getScriptProperties: () => ({
      getProperty: (k) => (Object.prototype.hasOwnProperty.call(world.props, k) ? world.props[k] : null),
      setProperty: (k, v) => { world.props[k] = String(v); },
      deleteProperty: (k) => { delete world.props[k]; },
      getProperties: () => Object.assign({}, world.props)
    }) },
    UrlFetchApp: { fetch: (url, options) => {
      let step = world.plan.shift() || { code: 599, body: 'plan exhausted' };
      if (typeof step === 'function') { step = step(world); }
      world.fetches.push({ url, method: options.method, options });
      if (step.throws) { throw new Error(step.throws); }
      return { getResponseCode: () => step.code, getContentText: () => step.body || '' };
    } },
    Utilities: { sleep: (ms) => { world.sleeps.push(ms); } },
    SpreadsheetApp: { getActiveSpreadsheet: () => ({ getSheetByName: (n) => sheets[n] || null }) },
    ScriptApp: { getProjectTriggers: () => [] },
    Logger: { log: (s) => { world.logger.push(String(s)); } }
  };
  vm.createContext(ctx);
  vm.runInContext(code + '\n;', ctx, { filename: '24_Sync_Dispatch.gs' });
  ctx.tfbSdNowMs_ = () => world.nowMs;
  world.ctx = ctx;
  return world;
}

// the id the dispatcher put on its first send, and a runs list as GitHub would give it
const idOf = (w) => JSON.parse(w.fetches[0].options.payload).inputs.dispatch_id;
const runsBody = (...titles) => JSON.stringify({ workflow_runs: titles.map((t) => ({ event: 'workflow_dispatch', display_title: t, created_at: '2026-11-25T15:48:03Z' })) });
const OTHERS = { code: 200, body: runsBody('TFB Shadow Scorer (S-1 Gate)', 'TFB Shadow Scorer (S-1 Gate) [gas-shadow-scorer-zzzzzz-0000]') };   // a manual run and another dispatch
// a plan step may be a function of the world, so a runs list can name the id that was really sent
const OURS = (w) => ({ code: 200, body: runsBody('TFB Shadow Scorer (S-1 Gate)', 'TFB Shadow Scorer (S-1 Gate) [' + idOf(w) + ']') });
const methods = (w) => w.fetches.map((f) => f.method).join(',');
const details = (w, i) => JSON.parse(String(w.logRows[i][9]));
const leak = (w) => (JSON.stringify(w.logRows) + JSON.stringify(w.logger) + JSON.stringify(w.props.TFB_SYNC_DISPATCH_LAST_EVENT || '')).indexOf(TOKEN) !== -1;
const ID_RE = /^gas-[a-z-]+-[0-9a-z]+-[0-9a-z]{4}$/;

let pass = 0;
let total = 0;
function T(name, cond, detail) {
  total++;
  if (cond) { pass++; }
  console.log((cond ? 'PASS ' : 'FAIL ') + name + (detail ? ' | ' + detail : ''));
}

// R1 the day of 25 Nov: HTTP 500, no run carries the id -> sent again -> accepted
{
  const w = makeWorld([{ code: 500, body: '{"message":"Server Error"}' }, OTHERS, { code: 204 }]);
  const r = w.ctx.tfbDispatchShadowScorer();
  T('R1 a 500 is sent again and accepted', r === 'OK:204' && methods(w) === 'post,get,post' && w.sleeps.join(',') === '5000', r + ' ' + methods(w));
  T('R1 the runs list is the scorer\'s own, dispatch events only',
    w.fetches[1].url === 'https://api.github.com/repos/emadsaberbahbah-coder/tadawul-fast/actions/workflows/shadow_scorer.yml/runs?event=workflow_dispatch&per_page=10');
  T('R1 both sends carry the same payload and the same id', w.fetches[0].options.payload === w.fetches[2].options.payload && w.fetches[0].url === w.fetches[2].url && ID_RE.test(idOf(w)) && /^gas-shadow-scorer-/.test(idOf(w)), idOf(w));
  T('R1 the other inputs are the lane\'s own', JSON.parse(w.fetches[0].options.payload).inputs.dry_run === 'false' && JSON.parse(w.fetches[0].options.payload).ref === 'main');
  T('R1 one OK row that says so', w.logRows.length === 1 && w.logRows[0][4] === 'OK' && w.logRows[0][7] === 204 &&
    / after 2 tries \(500, 204\)$/.test(String(w.logRows[0][5])) && JSON.stringify(details(w, 0).attempts) === '[500,204]' && details(w, 0).dispatch_id === idOf(w), String(w.logRows[0][5]));
  T('R1 LAST_OK set', /^\d{13}$/.test(String(w.props.TFB_S1_DISPATCH_LAST_OK_MS || '')));
}
// R2 the fault started the run all the same -> found BY ITS ID in the runs list -> NOT sent again
{
  const w = makeWorld([{ code: 500, body: 'x' }, OURS]);
  const r = w.ctx.tfbDispatchShadowScorer();
  T('R2 a run found by its id after the fault is not sent again', r === 'OK:run_started' && methods(w) === 'post,get', r + ' ' + methods(w));
  T('R2 row OK with the fault\'s code, LAST_OK set', w.logRows[0][4] === 'OK' && w.logRows[0][1] === 'INFO' && w.logRows[0][7] === 500 &&
    /answered HTTP 500 but its run started \(found by its dispatch id in the runs list\); not sent again/.test(String(w.logRows[0][5])) &&
    /^\d{13}$/.test(String(w.props.TFB_S1_DISPATCH_LAST_OK_MS || '')), String(w.logRows[0][5]));
  // the next trigger the same day is held by the min gap, as after a clean accept
  w.nowMs += 30 * 60000;
  T('R2 min gap holds the next trigger', w.ctx.tfbDispatchShadowScorer() === 'SKIPPED:min_gap' && w.fetches.length === 2);
}
// R3 three faults, the id never seen -> FAILED with the last code; three sends at most
{
  const w = makeWorld([{ code: 500 }, OTHERS, { throws: 'Address unavailable ' + TOKEN }, OTHERS, { code: 502, body: 'Bad Gateway' }, OTHERS]);
  const r = w.ctx.tfbDispatchShadowScorer();
  T('R3 three sends at most, then FAILED', r === 'FAILED:502' && methods(w) === 'post,get,post,get,post,get' && w.sleeps.join(',') === '5000,20000,5000', r + ' ' + methods(w) + ' ' + w.sleeps.join(','));
  T('R3 row FAILED lists the tries, LAST_OK not set', w.logRows.length === 1 && w.logRows[0][4] === 'FAILED' && w.logRows[0][1] === 'ERROR' && w.logRows[0][7] === 502 &&
    / after 3 tries \(500, -1, 502\)$/.test(String(w.logRows[0][5])) && !w.props.TFB_S1_DISPATCH_LAST_OK_MS, String(w.logRows[0][5]));
  T('R3 token never leaks', !leak(w));
}
// R4 an answer that is ours (401, 403, 404, a 422 about something else) is final: one send, no wait, no runs reading
for (const c of [401, 403, 404, 422]) {
  const w = makeWorld([{ code: c, body: '{"message":"Workflow does not have \'workflow_dispatch\' trigger"}' }]);
  const r = w.ctx.tfbDispatchShadowScorer();
  T('R4 HTTP ' + c + ' is not sent again', r === 'FAILED:' + c && methods(w) === 'post' && w.sleeps.length === 0 && !/tries/.test(String(w.logRows[0][5])) && details(w, 0).attempts === undefined, r);
}
// R5 a rate limit (429) and a transport error are tried again
{
  const w = makeWorld([{ code: 429, body: 'slow down' }, OTHERS, { code: 204 }]);
  T('R5 429 is sent again', w.ctx.tfbDispatchShadowBoardNow() === 'OK:204' && methods(w) === 'post,get,post' && /^gas-shadow-board-/.test(idOf(w)));
  const w2 = makeWorld([{ throws: 'DNS error' }, OTHERS, { code: 204 }]);
  T('R5 a transport error is sent again', w2.ctx.tfbDispatchShadowBoardNow() === 'OK:204' && JSON.stringify(details(w2, 0).attempts) === '[-1,204]');
}
// R6 the runs list cannot be read (GitHub still faulty) -> the send is tried again all the same
{
  const w = makeWorld([{ code: 503 }, { code: 503, body: 'x' }, { code: 500 }, { throws: 'timeout' }, { code: 204 }]);
  const r = w.ctx.tfbDispatchShadowScorer();
  T('R6 an unreadable runs list does not stop the next send', r === 'OK:204' && methods(w) === 'post,get,post,get,post' && w.sleeps.join(',') === '5000,20000', r + ' ' + methods(w));
  T('R6 row lists three tries', / after 3 tries \(503, 500, 204\)$/.test(String(w.logRows[0][5])));
}
// R7 the last send faults too, but its run is there on the final reading
{
  const w = makeWorld([{ code: 500 }, OTHERS, { code: 500 }, OTHERS, { code: 500 }, OURS]);
  const r = w.ctx.tfbDispatchShadowScorer();
  T('R7 a run seen on the final reading counts', r === 'OK:run_started' && methods(w) === 'post,get,post,get,post,get' && /^\d{13}$/.test(String(w.props.TFB_S1_DISPATCH_LAST_OK_MS || '')), r);
}
// R8 the daily sync goes the same way, with its own inputs, its own id and its own LAST_OK
{
  const w = makeWorld([{ code: 503, body: 'Service Unavailable' }, { code: 200, body: runsBody('Tadawul Dashboard Advanced Sync') }, { code: 204 }]);
  const r = w.ctx.tfbDispatchDailySyncNow();
  T('R8 daily_sync: a 503 is sent again', r === 'OK:204' && methods(w) === 'post,get,post', r + ' ' + methods(w));
  const p8 = JSON.parse(w.fetches[0].options.payload);
  T('R8 daily_sync inputs unchanged, plus the id, on both sends',
    p8.ref === 'main' && p8.inputs.run_mode === 'full_sync' && p8.inputs.include_portfolio === 'true' && /^gas-sync-[0-9a-z]+-[0-9a-z]{4}$/.test(p8.inputs.dispatch_id) &&
    Object.keys(p8.inputs).length === 3 && w.fetches[2].options.payload === w.fetches[0].options.payload, JSON.stringify(p8));
  T('R8 daily_sync runs list and LAST_OK are its own',
    /\/actions\/workflows\/daily_sync\.yml\/runs\?event=workflow_dispatch&per_page=10$/.test(w.fetches[1].url) &&
    /^\d{13}$/.test(String(w.props.TFB_SYNC_DISPATCH_LAST_OK_MS || '')) && !w.props.TFB_S1_DISPATCH_LAST_OK_MS);
  const w2 = makeWorld([{ code: 500 }, (x) => ({ code: 200, body: runsBody('Tadawul Dashboard Advanced Sync [' + idOf(x) + ']') })]);
  T('R8 daily_sync: its own started run is not sent twice', w2.ctx.tfbDispatchDailySyncNow() === 'OK:run_started' && methods(w2) === 'post,get' && /^\d{13}$/.test(String(w2.props.TFB_SYNC_DISPATCH_LAST_OK_MS || '')));
}
// R9 Codex P2 on #1087: a run is ours by its id, never by its time. A manual health_only sync, a dry-run board or
// another dispatch started in the same seconds is NOT adopted - the production dispatch is sent again.
{
  const w = makeWorld([{ code: 500 }, { code: 200, body: runsBody('Tadawul Dashboard Advanced Sync', 'Tadawul Dashboard Advanced Sync [gas-sync-aaaaaa-1111]') }, { code: 204 }]);
  T('R9 a concurrent manual sync is not adopted', w.ctx.tfbDispatchDailySyncNow() === 'OK:204' && methods(w) === 'post,get,post');
  const w2 = makeWorld([{ code: 500 }, { code: 200, body: runsBody('TFB Shadow Board') }, { code: 204 }]);
  T('R9 a concurrent dry-run board is not adopted', w2.ctx.tfbDispatchShadowBoardNow() === 'OK:204' && methods(w2) === 'post,get,post');
  const w3 = makeWorld([{ code: 500 }, (x) => ({ code: 200, body: JSON.stringify({ workflow_runs: [{ event: 'schedule', display_title: 'S [' + idOf(x) + ']' }] }) }), { code: 204 }]);
  T('R9 a scheduled run is never a dispatch', w3.ctx.tfbDispatchShadowScorer() === 'OK:204' && methods(w3) === 'post,get,post');
  const w4 = makeWorld([{ code: 500 }, (x) => ({ code: 200, body: runsBody('S [' + idOf(x).slice(0, -1) + ']') }), { code: 204 }]);
  T('R9 a nearly equal id is not this dispatch', w4.ctx.tfbDispatchShadowScorer() === 'OK:204' && methods(w4) === 'post,get,post');
  const a = makeWorld([{ code: 204 }]); a.ctx.tfbDispatchShadowScorer();
  const b = makeWorld([{ code: 204 }]); b.ctx.tfbDispatchShadowScorer();
  T('R9 two dispatches at the same instant carry different ids', idOf(a) !== idOf(b), idOf(a) + ' ' + idOf(b));
}
// R10 a clean first try: one send, no wait; the guards still stand before any send
{
  const w = makeWorld([{ code: 204 }]);
  const r = w.ctx.tfbDispatchShadowScorer();
  T('R10 clean accept: one send, no wait, the old message', r === 'OK:204' && methods(w) === 'post' && w.sleeps.length === 0 &&
    String(w.logRows[0][5]) === 'workflow_dispatch accepted (HTTP 204) ref=main inputs={"dry_run":"false"}' && details(w, 0).attempts === undefined &&
    details(w, 0).version === '1.2.0' && details(w, 0).dispatch_id === idOf(w), String(w.logRows[0][5]));
  const w2 = makeWorld([{ code: 204 }], { props: { TFB_GH_DISPATCH_TOKEN: TOKEN, TFB_S1_DISPATCH_DISABLED: 'true' } });
  T('R10 kill switch: nothing is sent', /^SKIPPED:/.test(w2.ctx.tfbDispatchShadowScorer()) && w2.fetches.length === 0);
  const w3 = makeWorld([{ code: 204 }], { nowMs: Date.parse('2026-11-25T15:05:00Z') });
  T('R10 before the slot: nothing is sent', w3.ctx.tfbDispatchShadowScorer() === 'SKIPPED:before_slot' && w3.fetches.length === 0);
}
// R11 the token is never written, also when a body echoes it
{
  const w = makeWorld([{ code: 500, body: 'Bearer ' + TOKEN }, OTHERS, { code: 500, body: TOKEN }, OTHERS, { code: 500, body: TOKEN }, OTHERS]);
  w.ctx.tfbDispatchShadowScorer();
  T('R11 token never leaks', !leak(w) && w.logRows.length === 1);
  T('R11 every request carries the bearer header and mutes HTTP exceptions', w.fetches.every((f) => f.options.headers.Authorization === 'Bearer ' + TOKEN && f.options.muteHttpExceptions === true));
}
// R12 a workflow that does not take dispatch_id yet (the file pasted before the workflows are on the ref): 422
// 'Unexpected inputs' -> the dispatch goes once without the id, as 1.1.1 sent it, and the row says so
{
  const refused = { code: 422, body: '{"message":"Unexpected inputs provided: [\\"dispatch_id\\"]"}' };
  const w = makeWorld([refused, { code: 204 }]);
  const r = w.ctx.tfbDispatchShadowScorer();
  const second = JSON.parse(w.fetches[1].options.payload);
  T('R12 the id refused: sent once without it', r === 'OK:204' && methods(w) === 'post,post' && w.sleeps.length === 0 &&
    second.inputs.dispatch_id === undefined && second.inputs.dry_run === 'false' && second.ref === 'main', r + ' ' + JSON.stringify(second));
  T('R12 the row says so', /\(sent without dispatch_id: the workflow on this ref does not take it yet\)$/.test(String(w.logRows[0][5])) && JSON.stringify(details(w, 0).attempts) === '[422,204]', String(w.logRows[0][5]));
  const w2 = makeWorld([refused, { code: 500, body: 'x' }]);
  T('R12 without an id a fault is not sent again (nothing to tell its run by)', w2.ctx.tfbDispatchShadowScorer() === 'FAILED:500' && methods(w2) === 'post,post' && w2.sleeps.length === 0);
  const w3 = makeWorld([refused, { code: 204 }]);
  T('R12 the daily sync falls back the same way', w3.ctx.tfbDispatchDailySyncNow() === 'OK:204' &&
    w3.fetches[1].options.payload === JSON.stringify({ ref: 'main', inputs: { run_mode: 'full_sync', include_portfolio: 'true' } }));
  const w4 = makeWorld([{ code: 500 }, OTHERS, refused]);
  T('R12 a 422 on a later send is final', w4.ctx.tfbDispatchShadowScorer() === 'FAILED:422' && methods(w4) === 'post,get,post');
}
// R13 the pure battery, the version, and the three workflows take and print the id
{
  const w = makeWorld([]);
  const v = w.ctx.tfbSyncDispatchSelfTest();
  T('R13 selftest both batteries', v === 'sync dispatch core: ok | s1 lane core: ok', v);
  T('R13 version 1.2.0, ES5 only', w.ctx.TFB_SYNC_DISPATCH_VERSION === '1.2.0' && !/\b(let|const)\s|=>/.test(code.split('\n').filter((l) => !/^\s*\/\//.test(l)).join('\n')));
  for (const f of ['daily_sync.yml', 'shadow_board.yml', 'shadow_scorer.yml']) {
    const y = fs.readFileSync(path.join(__dirname, '..', '.github', 'workflows', f), 'utf8');
    T('R13 ' + f + ' takes dispatch_id and prints it in the run name',
      /\n      dispatch_id:\n        description: "[^"\n]+"\n        required: false\n        default: ""\n(        type: string\n)?/.test(y) &&
      y.split('\n').filter((l) => l === "run-name: ${{ inputs.dispatch_id && format('{0} [{1}]', github.workflow, inputs.dispatch_id) || '' }}").length === 1);
  }
}

console.log((pass === total ? 'PASS ' : 'FAIL ') + pass + '/' + total);
process.exit(pass === total ? 0 : 1);
