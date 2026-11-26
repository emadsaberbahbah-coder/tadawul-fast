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
      const step = world.plan.shift() || { code: 599, body: 'plan exhausted' };
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

const runs = (...created) => JSON.stringify({ workflow_runs: created.map((c) => ({ event: 'workflow_dispatch', created_at: c })) });
const NONE = { code: 200, body: runs('2026-11-24T15:41:00Z') };              // yesterday's run only
const STARTED = { code: 200, body: runs('2026-11-25T15:48:03Z', '2026-11-24T15:41:00Z') };
const methods = (w) => w.fetches.map((f) => f.method).join(',');
const details = (w, i) => JSON.parse(String(w.logRows[i][9]));
const leak = (w) => (JSON.stringify(w.logRows) + JSON.stringify(w.logger) + JSON.stringify(w.props.TFB_SYNC_DISPATCH_LAST_EVENT || '')).indexOf(TOKEN) !== -1;

let pass = 0;
let total = 0;
function T(name, cond, detail) {
  total++;
  if (cond) { pass++; }
  console.log((cond ? 'PASS ' : 'FAIL ') + name + (detail ? ' | ' + detail : ''));
}

// R1 the day of 25 Nov: HTTP 500, no run started -> sent again -> accepted
{
  const w = makeWorld([{ code: 500, body: '{"message":"Server Error"}' }, NONE, { code: 204 }]);
  const r = w.ctx.tfbDispatchShadowScorer();
  T('R1 a 500 is sent again and accepted', r === 'OK:204' && methods(w) === 'post,get,post' && w.sleeps.join(',') === '5000', r + ' ' + methods(w));
  T('R1 the runs list is the scorer\'s own, dispatch events only',
    w.fetches[1].url === 'https://api.github.com/repos/emadsaberbahbah-coder/tadawul-fast/actions/workflows/shadow_scorer.yml/runs?event=workflow_dispatch&per_page=5');
  T('R1 both sends carry the same payload', w.fetches[0].options.payload === w.fetches[2].options.payload && w.fetches[0].url === w.fetches[2].url);
  T('R1 one OK row that says so', w.logRows.length === 1 && w.logRows[0][4] === 'OK' && w.logRows[0][7] === 204 &&
    / after 2 tries \(500, 204\)$/.test(String(w.logRows[0][5])) && JSON.stringify(details(w, 0).attempts) === '[500,204]', String(w.logRows[0][5]));
  T('R1 LAST_OK set', /^\d{13}$/.test(String(w.props.TFB_S1_DISPATCH_LAST_OK_MS || '')));
}
// R2 the fault started the run all the same -> found in the runs list -> NOT sent again
{
  const w = makeWorld([{ code: 500, body: 'x' }, STARTED]);
  const r = w.ctx.tfbDispatchShadowScorer();
  T('R2 a run found after the fault is not sent again', r === 'OK:run_started' && methods(w) === 'post,get', r + ' ' + methods(w));
  T('R2 row OK with the fault\'s code, LAST_OK set', w.logRows[0][4] === 'OK' && w.logRows[0][1] === 'INFO' && w.logRows[0][7] === 500 &&
    /answered HTTP 500 but the run started \(seen in the runs list\); not sent again/.test(String(w.logRows[0][5])) &&
    /^\d{13}$/.test(String(w.props.TFB_S1_DISPATCH_LAST_OK_MS || '')), String(w.logRows[0][5]));
  // the next trigger the same day is held by the min gap, as after a clean accept
  w.nowMs += 30 * 60000;
  T('R2 min gap holds the next trigger', w.ctx.tfbDispatchShadowScorer() === 'SKIPPED:min_gap' && w.fetches.length === 2);
}
// R3 three faults, no run ever seen -> FAILED with the last code; three sends at most
{
  const w = makeWorld([{ code: 500 }, NONE, { throws: 'Address unavailable ' + TOKEN }, NONE, { code: 502, body: 'Bad Gateway' }, NONE]);
  const r = w.ctx.tfbDispatchShadowScorer();
  T('R3 three sends at most, then FAILED', r === 'FAILED:502' && methods(w) === 'post,get,post,get,post,get' && w.sleeps.join(',') === '5000,20000,5000', r + ' ' + methods(w) + ' ' + w.sleeps.join(','));
  T('R3 row FAILED lists the tries, LAST_OK not set', w.logRows.length === 1 && w.logRows[0][4] === 'FAILED' && w.logRows[0][1] === 'ERROR' && w.logRows[0][7] === 502 &&
    / after 3 tries \(500, -1, 502\)$/.test(String(w.logRows[0][5])) && !w.props.TFB_S1_DISPATCH_LAST_OK_MS, String(w.logRows[0][5]));
  T('R3 token never leaks', !leak(w));
}
// R4 an answer that is ours (422, 401, 404) is final: one send, no wait, no runs reading
for (const c of [422, 401, 404, 403]) {
  const w = makeWorld([{ code: c, body: '{"message":"no"}' }]);
  const r = w.ctx.tfbDispatchShadowScorer();
  T('R4 HTTP ' + c + ' is not sent again', r === 'FAILED:' + c && methods(w) === 'post' && w.sleeps.length === 0 && !/tries/.test(String(w.logRows[0][5])) && details(w, 0).attempts === undefined, r);
}
// R5 a rate limit (429) and a transport error are tried again
{
  const w = makeWorld([{ code: 429, body: 'slow down' }, NONE, { code: 204 }]);
  T('R5 429 is sent again', w.ctx.tfbDispatchShadowBoardNow() === 'OK:204' && methods(w) === 'post,get,post');
  const w2 = makeWorld([{ throws: 'DNS error' }, NONE, { code: 204 }]);
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
  const w = makeWorld([{ code: 500 }, NONE, { code: 500 }, NONE, { code: 500 }, STARTED]);
  const r = w.ctx.tfbDispatchShadowScorer();
  T('R7 a run seen on the final reading counts', r === 'OK:run_started' && methods(w) === 'post,get,post,get,post,get' && /^\d{13}$/.test(String(w.props.TFB_S1_DISPATCH_LAST_OK_MS || '')), r);
}
// R8 the daily sync goes the same way, with its own payload and its own LAST_OK
{
  const w = makeWorld([{ code: 503, body: 'Service Unavailable' }, NONE, { code: 204 }]);
  const r = w.ctx.tfbDispatchDailySyncNow();
  T('R8 daily_sync: a 503 is sent again', r === 'OK:204' && methods(w) === 'post,get,post', r + ' ' + methods(w));
  T('R8 daily_sync payload unchanged on both sends',
    w.fetches[0].options.payload === JSON.stringify({ ref: 'main', inputs: { run_mode: 'full_sync', include_portfolio: 'true' } }) && w.fetches[2].options.payload === w.fetches[0].options.payload);
  T('R8 daily_sync runs list and LAST_OK are its own',
    /\/actions\/workflows\/daily_sync\.yml\/runs\?event=workflow_dispatch&per_page=5$/.test(w.fetches[1].url) &&
    /^\d{13}$/.test(String(w.props.TFB_SYNC_DISPATCH_LAST_OK_MS || '')) && !w.props.TFB_S1_DISPATCH_LAST_OK_MS);
  const w2 = makeWorld([{ code: 500 }, STARTED]);
  T('R8 daily_sync: a started run is not sent twice', w2.ctx.tfbDispatchDailySyncNow() === 'OK:run_started' && methods(w2) === 'post,get' && /^\d{13}$/.test(String(w2.props.TFB_SYNC_DISPATCH_LAST_OK_MS || '')));
}
// R9 a run from before the send (beyond the clock tolerance) is not this send's
{
  const w = makeWorld([{ code: 500 }, { code: 200, body: runs('2026-11-25T15:47:40Z') }, { code: 204 }]);     // 18 s before
  T('R9 an earlier run is not adopted', w.ctx.tfbDispatchShadowScorer() === 'OK:204' && methods(w) === 'post,get,post');
  const w2 = makeWorld([{ code: 500 }, { code: 200, body: runs('2026-11-25T15:47:50Z') }]);                   // 8 s before: inside the tolerance
  T('R9 a run inside the clock tolerance is', w2.ctx.tfbDispatchShadowScorer() === 'OK:run_started' && methods(w2) === 'post,get');
  const w3 = makeWorld([{ code: 500 }, { code: 200, body: JSON.stringify({ workflow_runs: [{ event: 'schedule', created_at: '2026-11-25T15:48:03Z' }] }) }, { code: 204 }]);
  T('R9 a scheduled run is not a dispatch', w3.ctx.tfbDispatchShadowScorer() === 'OK:204' && methods(w3) === 'post,get,post');
}
// R10 a clean first try writes the row it always wrote; the guards still stand before any send
{
  const w = makeWorld([{ code: 204 }]);
  const r = w.ctx.tfbDispatchShadowScorer();
  T('R10 clean accept: one send, no wait, the old message', r === 'OK:204' && methods(w) === 'post' && w.sleeps.length === 0 &&
    String(w.logRows[0][5]) === 'workflow_dispatch accepted (HTTP 204) ref=main inputs={"dry_run":"false"}' && details(w, 0).attempts === undefined && details(w, 0).version === '1.2.0', String(w.logRows[0][5]));
  const w2 = makeWorld([{ code: 204 }], { props: { TFB_GH_DISPATCH_TOKEN: TOKEN, TFB_S1_DISPATCH_DISABLED: 'true' } });
  T('R10 kill switch: nothing is sent', /^SKIPPED:/.test(w2.ctx.tfbDispatchShadowScorer()) && w2.fetches.length === 0);
  const w3 = makeWorld([{ code: 204 }], { nowMs: Date.parse('2026-11-25T15:05:00Z') });
  T('R10 before the slot: nothing is sent', w3.ctx.tfbDispatchShadowScorer() === 'SKIPPED:before_slot' && w3.fetches.length === 0);
}
// R11 the token is never written, also when a body echoes it
{
  const w = makeWorld([{ code: 500, body: 'Bearer ' + TOKEN }, { code: 200, body: runs('2026-11-24T15:41:00Z') }, { code: 500, body: TOKEN }, NONE, { code: 500, body: TOKEN }, NONE]);
  w.ctx.tfbDispatchShadowScorer();
  T('R11 token never leaks', !leak(w) && w.logRows.length === 1);
  T('R11 every request carries the bearer header and mutes HTTP exceptions', w.fetches.every((f) => f.options.headers.Authorization === 'Bearer ' + TOKEN && f.options.muteHttpExceptions === true));
}
// R12 the pure battery, and the version
{
  const w = makeWorld([]);
  const v = w.ctx.tfbSyncDispatchSelfTest();
  T('R12 selftest both batteries', v === 'sync dispatch core: ok | s1 lane core: ok', v);
  T('R12 version 1.2.0, ES5 only', w.ctx.TFB_SYNC_DISPATCH_VERSION === '1.2.0' && !/\b(let|const)\s|=>/.test(code.split('\n').filter((l) => !/^\s*\/\//.test(l)).join('\n')));
}

console.log((pass === total ? 'PASS ' : 'FAIL ') + pass + '/' + total);
process.exit(pass === total ? 0 : 1);
