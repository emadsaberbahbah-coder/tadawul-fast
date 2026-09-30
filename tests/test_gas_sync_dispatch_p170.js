#!/usr/bin/env node
/* tests/test_gas_sync_dispatch_p170.js
 * Harness for apps_script/24_Sync_Dispatch.gs v1.0.0 surface, re-pinned on v1.1.0 [P-170 DISPATCH-FROM-GAS].
 * Loads the REAL .gs source into a vm context whose only additions are stubs of
 * the Apps Script services it touches (PropertiesService, UrlFetchApp,
 * SpreadsheetApp, ScriptApp, Logger). Every test calls the real functions.
 * Run: node tests/test_gas_sync_dispatch_p170.js   (exit 0 = all PASS)
 */
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const crypto = require('crypto');

const SRC = path.join(__dirname, '..', 'apps_script', '24_Sync_Dispatch.gs');
const code = fs.readFileSync(SRC, 'utf8');
const TOKEN = 'github_pat_TESTTOKEN_0123456789abcdef';

function makeWorld(opts) {
  const o = Object.assign({
    props: {}, fetchCode: 204, fetchBody: '', fetchThrow: null,
    holdUntil: '', gmStamp: '', triggers: []
  }, opts || {});
  const world = { fetches: [], logRows: [], logger: [], deleted: [], created: [], props: Object.assign({}, o.props) };
  const sheets = {
    '_Run_Log': { appendRow: (row) => { world.logRows.push(row); }, getDataRange: () => ({ getValues: () => [] }) },
    '_Sync_Control': { getDataRange: () => ({ getValues: () => [['Key', 'Value', ''], ['Manual Hold Until', '', ''], ['backend sync hold until', o.holdUntil, '[SYNC-HOLD] x']] }) },
    '_Status': { getDataRange: () => ({ getValues: () => [
      ['Page', 'Last Updated', 'Status', 'Message', 'Endpoint', 'HTTP Code', 'Rows', 'Columns', 'Duration ms', 'Warnings', '', 'Global Key', 'Value'],
      ['Market_Leaders', '2026-09-29 03:27:50+03:00', 'SUCCESS', '', '', '', 255, 115, 1, 5, '', 'Last Global Update', '9/28/2026'],
      ['Global_Markets', o.gmStamp, 'SUCCESS', '', '', '', 6609, 115, 1, 9, '', 'TFB Feed Global_Markets', o.gmStamp ? 'OK | cov=100.0 | run=36502914213 | ' + o.gmStamp : '']
    ] }) }
  };
  if (o.noSheets) { for (const k of Object.keys(sheets)) { delete sheets[k]; } }
  const triggers = o.triggers.map((h) => ({ getHandlerFunction: () => h, _h: h }));
  const ctx = {
    Date, JSON, Math, Number, String, parseInt, isNaN, encodeURIComponent, Object, Array, RegExp, Error,
    PropertiesService: { getScriptProperties: () => ({
      getProperty: (k) => (Object.prototype.hasOwnProperty.call(world.props, k) ? world.props[k] : null),
      setProperty: (k, v) => { world.props[k] = String(v); },
      deleteProperty: (k) => { delete world.props[k]; },
      getProperties: () => Object.assign({}, world.props)
    }) },
    UrlFetchApp: { fetch: (url, options) => {
      world.fetches.push({ url, options });
      if (o.fetchThrow) { throw new Error(o.fetchThrow); }
      return { getResponseCode: () => o.fetchCode, getContentText: () => o.fetchBody };
    } },
    SpreadsheetApp: { getActiveSpreadsheet: () => ({ getSheetByName: (n) => sheets[n] || null }) },
    ScriptApp: {
      getProjectTriggers: () => triggers.slice(),
      deleteTrigger: (t) => { world.deleted.push(t._h); const i = triggers.indexOf(t); if (i >= 0) { triggers.splice(i, 1); } },
      newTrigger: (h) => {
        const spec = { handler: h };
        const b = {
          timeBased: () => b, everyDays: (n) => { spec.everyDays = n; return b; }, atHour: (x) => { spec.atHour = x; return b; },
          nearMinute: (m) => { spec.nearMinute = m; return b; }, inTimezone: (tz) => { spec.tz = tz; return b; },
          create: () => { world.created.push(spec); triggers.push({ getHandlerFunction: () => h, _h: h }); return {}; }
        };
        return b;
      }
    },
    Logger: { log: (s) => { world.logger.push(String(s)); } }
  };
  vm.createContext(ctx);
  vm.runInContext(code + '\n;', ctx, { filename: '24_Sync_Dispatch.gs' });
  world.ctx = ctx;
  world.sheets = sheets;
  return world;
}

const out = [];
let fails = 0;
function T(name, cond, detail) {
  const line = (cond ? 'PASS ' : 'FAIL ') + name + (detail ? ' | ' + detail : '');
  out.push(line);
  if (!cond) { fails++; }
}
function noTokenLeak(world, label) {
  const hay = JSON.stringify(world.logRows) + JSON.stringify(world.logger) + JSON.stringify(world.props[ 'TFB_SYNC_DISPATCH_LAST_EVENT'] || '');
  T(label + ' token never leaks', hay.indexOf(TOKEN) === -1);
}

function run() {
  // T1 self-test (pure)
  {
    const w = makeWorld({});
    const v = w.ctx.tfbSyncDispatchSelfTest();
    T('T1 selftest', v.indexOf('sync dispatch core: ok') === 0, v);
    T('T1 version', w.ctx.TFB_SYNC_DISPATCH_VERSION === '1.1.0');
  }
  // T2 happy path: token, stale GM (3 h), expired hold -> dispatch 204
  {
    const now = Date.now();
    const gm = new Date(now - 3 * 3600 * 1000);
    const iso = gm.toISOString().replace('T', ' ').replace(/\.\d+Z$/, '+00:00');
    const w = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, gmStamp: iso, holdUntil: new Date(now - 5000).toISOString(), fetchCode: 204 });
    const r = w.ctx.tfbDispatchDailySync();
    T('T2 dispatch result', r === 'OK:204', r);
    T('T2 one fetch', w.fetches.length === 1);
    const f = w.fetches[0];
    T('T2 url', f.url === 'https://api.github.com/repos/emadsaberbahbah-coder/tadawul-fast/actions/workflows/daily_sync.yml/dispatches', f.url);
    T('T2 payload', JSON.parse(f.options.payload).inputs.run_mode === 'full_sync' && JSON.parse(f.options.payload).ref === 'main');
    T('T2 bearer', f.options.headers.Authorization === 'Bearer ' + TOKEN);
    T('T2 log row', w.logRows.length === 1 && w.logRows[0][2] === 'tfbDispatchDailySync' && w.logRows[0][4] === 'OK' && w.logRows[0][7] === 204 && w.logRows[0][3] === 'daily_sync.yml', JSON.stringify(w.logRows[0] && w.logRows[0].slice(1, 8)));
    T('T2 details reason ok', /"reason":"ok"/.test(String(w.logRows[0][9])) && /"gm_stamp_age_min":18[0-9]/.test(String(w.logRows[0][9])), String(w.logRows[0][9]).slice(0, 120));
    T('T2 LAST_OK set', /^\d{13}$/.test(String(w.props.TFB_SYNC_DISPATCH_LAST_OK_MS || '')));
    noTokenLeak(w, 'T2');
    // T3 second call inside the gap -> SKIPPED:min_gap, no new fetch
    const r2 = w.ctx.tfbDispatchDailySync();
    T('T3 min gap skip', r2 === 'SKIPPED:min_gap' && w.fetches.length === 1, r2);
    T('T3 skip log row', w.logRows.length === 2 && w.logRows[1][4] === 'SKIPPED' && w.logRows[1][1] === 'INFO');
    // T3b manual Now bypasses the gap -> dispatches again
    const r3 = w.ctx.tfbDispatchDailySyncNow();
    T('T3b forced dispatch', r3 === 'OK:204' && w.fetches.length === 2 && /"reason":"forced"/.test(String(w.logRows[2][9])), r3);
  }
  // T4 fresh GM stamp (10 min) -> SKIPPED:gm_fresh; hold live -> SKIPPED:sync_in_flight; disabled -> SKIPPED:disabled
  {
    const now = Date.now();
    const fresh = new Date(now - 10 * 60000).toISOString();
    const w = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, gmStamp: fresh });
    const r = w.ctx.tfbDispatchDailySync();
    T('T4 gm fresh skip', r === 'SKIPPED:gm_fresh' && w.fetches.length === 0, r);
    const w2 = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, holdUntil: new Date(now + 90 * 1000).toISOString().replace('Z', '+00:00') });
    const r2 = w2.ctx.tfbDispatchDailySync();
    T('T4 hold live skip', r2 === 'SKIPPED:sync_in_flight' && w2.fetches.length === 0, r2);
    const r2b = w2.ctx.tfbDispatchDailySyncNow();
    T('T4 hold live blocks Now too', r2b === 'SKIPPED:sync_in_flight' && w2.fetches.length === 0, r2b);
    const w3 = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN, TFB_SYNC_DISPATCH_DISABLED: '1' } });
    const r3 = w3.ctx.tfbDispatchDailySync();
    const r3b = w3.ctx.tfbDispatchDailySyncNow();
    T('T4 kill switch', r3 === 'SKIPPED:disabled' && r3b === 'SKIPPED:disabled' && w3.fetches.length === 0, r3 + '/' + r3b);
    // custom gap/fresh props honoured
    const w4 = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN, TFB_SYNC_DISPATCH_FRESH_SKIP_MIN: '5' }, gmStamp: fresh });
    const r4 = w4.ctx.tfbDispatchDailySync();
    T('T4 fresh window prop (5 min) lets a 10-min stamp through', r4 === 'OK:204', r4);
  }
  // T5 no token -> FAILED:no_token, zero network, ERROR row
  {
    const w = makeWorld({ props: {} });
    const r = w.ctx.tfbDispatchDailySync();
    T('T5 no token', r === 'FAILED:no_token' && w.fetches.length === 0, r);
    T('T5 error row', w.logRows.length === 1 && w.logRows[0][1] === 'ERROR' && w.logRows[0][4] === 'FAILED');
    const p = w.ctx.tfbSyncDispatchProbe();
    T('T5 probe no token', p === 'FAILED:no_token' && w.fetches.length === 0, p);
  }
  // T6 GitHub 401 -> FAILED:401, LAST_OK untouched, token redacted in the body echo
  {
    const w = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, fetchCode: 401, fetchBody: '{"message":"Bad credentials ' + TOKEN + '"}' });
    const r = w.ctx.tfbDispatchDailySync();
    T('T6 401 result', r === 'FAILED:401', r);
    T('T6 LAST_OK not set', !w.props.TFB_SYNC_DISPATCH_LAST_OK_MS);
    T('T6 row FAILED 401', w.logRows[0][4] === 'FAILED' && w.logRows[0][7] === 401 && w.logRows[0][1] === 'ERROR');
    noTokenLeak(w, 'T6');
    const w2 = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, fetchThrow: 'DNS error ' + TOKEN });
    const r2 = w2.ctx.tfbDispatchDailySync();
    T('T6 fetch throws -> FAILED:-1', r2 === 'FAILED:-1', r2);
    noTokenLeak(w2, 'T6b');
  }
  // T7 sheets missing entirely -> guards fail open, dispatch proceeds
  {
    const w = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, noSheets: true });
    const r = w.ctx.tfbDispatchDailySync();
    T('T7 no sheets still dispatches', r === 'OK:204' && w.fetches.length === 1, r);
    T('T7 logger fallback', w.logger.some((s) => s.indexOf('tfbDispatchDailySync OK') !== -1));
  }
  // T8 probe 200 / 404
  {
    const w = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, fetchCode: 200, fetchBody: JSON.stringify({ id: 12345, name: 'x', state: 'active', path: '.github/workflows/daily_sync.yml' }) });
    const r = w.ctx.tfbSyncDispatchProbe();
    T('T8 probe ok', r === 'OK:200:active', r);
    T('T8 probe is GET', w.fetches[0].options.method === 'get' && /\/actions\/workflows\/daily_sync\.yml$/.test(w.fetches[0].url));
    noTokenLeak(w, 'T8');
    const w2 = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, fetchCode: 404, fetchBody: '{"message":"Not Found"}' });
    const r2 = w2.ctx.tfbSyncDispatchProbe();
    T('T8 probe 404 hint', r2 === 'FAILED:404:repo/workflow not visible to this token', r2);
  }
  // T9 trigger install / remove (idempotent, foreign triggers untouched)
  {
    const w = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN, TFB_SYNC_DISPATCH_HOURS: '15,6', TFB_SYNC_DISPATCH_MINUTE: '20' }, triggers: ['tfbDispatchDailySync', 'tfbAutoRefreshTrigger', 'tfbDispatchDailySync'] });
    const r = w.ctx.tfbInstallSyncDispatchTriggers();
    T('T9 install result', r === 'OK:hours=6,15:minute=20:removed=2', r);
    T('T9 deleted only own', w.deleted.length === 2 && w.deleted.every((h) => h === 'tfbDispatchDailySync'));
    T('T9 created specs', w.created.length === 2 && w.created[0].atHour === 6 && w.created[1].atHour === 15 && w.created.every((s) => s.everyDays === 1 && s.nearMinute === 20 && s.tz === 'Asia/Riyadh' && s.handler === 'tfbDispatchDailySync'));
    const st = w.ctx.tfbSyncDispatchStatus();
    T('T9 status line', /triggers=2/.test(st) && /hours=6,15/.test(st) && /token=present\(/.test(st) && st.indexOf(TOKEN) === -1, st.slice(0, 100));
    const rr = w.ctx.tfbRemoveSyncDispatchTriggers();
    T('T9 remove', rr === 'OK:removed=2' && w.ctx.tfbSyncDispatchStatus().indexOf('triggers=0') !== -1, rr);
    // defaults when no hour/minute props
    const w2 = makeWorld({ props: {} });
    const r2 = w2.ctx.tfbInstallSyncDispatchTriggers();
    T('T9 defaults 6/15', r2 === 'OK:hours=6:minute=15:removed=0', r2);
  }
  // T10 ISO forms from the live sheets
  {
    const w = makeWorld({});
    const a = w.ctx.tfbSdParseIsoMs_('2026-09-29T01:28:32.942810+00:00');
    const b = w.ctx.tfbSdParseIsoMs_('2026-09-29 04:27:48+03:00');
    T('T10 iso micro', a === Date.parse('2026-09-29T01:28:32.942+00:00'));
    T('T10 iso space', b === Date.parse('2026-09-29T04:27:48+03:00'));
    T('T10 iso feed key', w.ctx.tfbSdParseIsoMs_('OK | cov=100.0 | run=36502914213 | 2026-09-29 04:27:50+03:00') === Date.parse('2026-09-29T04:27:50+03:00'));
  }
}

run();
const digest = crypto.createHash('sha256').update(out.join('\n')).digest('hex').slice(0, 12);
console.log(out.join('\n'));
console.log(`SUMMARY ${out.length - fails}/${out.length} PASS | digest ${digest}`);
process.exit(fails ? 1 : 0);
