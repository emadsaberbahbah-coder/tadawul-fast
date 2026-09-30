#!/usr/bin/env node
/* tests/test_gas_s1_dispatch_p170b.js
 * Harness for apps_script/24_Sync_Dispatch.gs v1.1.0 [P-170b S-1 LANE DISPATCH].
 * Loads the REAL .gs source into a vm context whose only additions are stubs of
 * the Apps Script services (PropertiesService, UrlFetchApp, SpreadsheetApp,
 * ScriptApp, Logger) and an injectable clock (tfbSdNowMs_ overridden per test).
 * Every test calls the real functions.
 * Run: node tests/test_gas_s1_dispatch_p170b.js   (exit 0 = all PASS)
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
    holdUntil: '', gmStamp: '', boardAsOf: '', triggers: [], nowMs: Date.parse('2026-09-30T15:40:00Z')
  }, opts || {});
  const world = { fetches: [], logRows: [], logger: [], deleted: [], created: [], props: Object.assign({}, o.props), nowMs: o.nowMs };
  const boardRow = ['SHADOW BOARD v1.5.0', o.boardAsOf ? 'as of ' + o.boardAsOf + ' Riyadh' : '', 'equity=130,000 SAR'];
  const sheets = {
    '_Run_Log': { appendRow: (row) => { world.logRows.push(row); }, getDataRange: () => ({ getValues: () => [] }) },
    '_Sync_Control': { getDataRange: () => ({ getValues: () => [['Key', 'Value', ''], ['Manual Hold Until', '', ''], ['backend sync hold until', o.holdUntil, '[SYNC-HOLD] x']] }) },
    '_Status': { getDataRange: () => ({ getValues: () => [
      ['Page', 'Last Updated', 'Status'], ['Global_Markets', o.gmStamp, 'SUCCESS']
    ] }) },
    'Shadow_Board': { getLastColumn: () => 3, getRange: (r, c, nr, nc) => ({ getValues: () => [boardRow.slice(0, nc)] }) }
  };
  if (o.noBoard) { delete sheets.Shadow_Board; }
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
  ctx.tfbSdNowMs_ = () => world.nowMs;          // injectable clock (global lookup at call time)
  world.ctx = ctx;
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
  const hay = JSON.stringify(world.logRows) + JSON.stringify(world.logger) + JSON.stringify(world.props['TFB_SYNC_DISPATCH_LAST_EVENT'] || '');
  T(label + ' token never leaks', hay.indexOf(TOKEN) === -1);
}
const payload = (w, i) => JSON.parse(w.fetches[i].options.payload);

function run() {
  // S1 version + self-test (both batteries)
  {
    const w = makeWorld({});
    const v = w.ctx.tfbSyncDispatchSelfTest();
    T('S1 selftest both batteries', v === 'sync dispatch core: ok | s1 lane core: ok', v);
    T('S1 version', w.ctx.TFB_SYNC_DISPATCH_VERSION === '1.1.0');
    T('S1 v1.0.0 surface present', ['tfbDispatchDailySync', 'tfbDispatchDailySyncNow', 'tfbSyncDispatchProbe', 'tfbInstallSyncDispatchTriggers', 'tfbRemoveSyncDispatchTriggers', 'tfbSyncDispatchStatus', 'tfbSdDecide_', 'tfbSdBuildDispatchRequest_'].every((f) => typeof w.ctx[f] === 'function'));
  }
  // S2 board dispatch at 08:10 Riyadh: stale board (yesterday) -> POST with dry_run=false, no run_mode, Page=shadow_board.yml
  {
    const w = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, boardAsOf: '2026-09-29 22:23', nowMs: Date.parse('2026-09-30T05:10:00Z') });
    const r = w.ctx.tfbDispatchShadowBoard();
    T('S2 board dispatch', r === 'OK:204' && w.fetches.length === 1, r);
    T('S2 board url', w.fetches[0].url === 'https://api.github.com/repos/emadsaberbahbah-coder/tadawul-fast/actions/workflows/shadow_board.yml/dispatches', w.fetches[0].url);
    const p = payload(w, 0);
    T('S2 board payload dry_run=false and NO run_mode', p.ref === 'main' && p.inputs.dry_run === 'false' && p.inputs.run_mode === undefined, JSON.stringify(p));
    T('S2 board row', w.logRows.length === 1 && w.logRows[0][2] === 'tfbDispatchShadowBoard' && w.logRows[0][3] === 'shadow_board.yml' && w.logRows[0][4] === 'OK' && w.logRows[0][7] === 204, JSON.stringify(w.logRows[0].slice(1, 8)));
    T('S2 board row age + version', /"board_asof_age_min":587/.test(String(w.logRows[0][9])) && /"version":"1.1.0"/.test(String(w.logRows[0][9])), String(w.logRows[0][9]).slice(0, 160));
    T('S2 SB LAST_OK set, sync LAST_OK untouched', /^\d{13}$/.test(String(w.props.TFB_SB_DISPATCH_LAST_OK_MS || '')) && !w.props.TFB_SYNC_DISPATCH_LAST_OK_MS);
    noTokenLeak(w, 'S2');
    // second call within 60 min -> min_gap; manual Now -> forced
    w.nowMs += 10 * 60000;
    const r2 = w.ctx.tfbDispatchShadowBoard();
    T('S2 board min gap', r2 === 'SKIPPED:min_gap' && w.fetches.length === 1, r2);
    const r3 = w.ctx.tfbDispatchShadowBoardNow();
    T('S2 board Now forced', r3 === 'OK:204' && w.fetches.length === 2 && /"reason":"forced"/.test(String(w.logRows[2][9])), r3);
  }
  // S3 board fresh (20 min old stamp) -> SKIPPED board_fresh; Now bypasses; missing tab fails open
  {
    const now = Date.parse('2026-09-30T14:10:00Z');
    const w = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, boardAsOf: '2026-09-30 16:50', nowMs: now });
    const r = w.ctx.tfbDispatchShadowBoard();
    T('S3 board fresh skip', r === 'SKIPPED:board_fresh' && w.fetches.length === 0, r);
    const r2 = w.ctx.tfbDispatchShadowBoardNow();
    T('S3 Now bypasses board_fresh', r2 === 'OK:204' && w.fetches.length === 1, r2);
    const w2 = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, noBoard: true, nowMs: now });
    const r3 = w2.ctx.tfbDispatchShadowBoard();
    T('S3 no board tab fails open -> dispatch', r3 === 'OK:204', r3);
    const w3 = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN, TFB_SB_DISPATCH_FRESH_SKIP_MIN: '10' }, boardAsOf: '2026-09-30 16:50', nowMs: now });
    T('S3 custom fresh window (10 min) lets a 20-min stamp through', w3.ctx.tfbDispatchShadowBoard() === 'OK:204');
  }
  // S4 scorer: before slot (15:05Z) -> SKIPPED before_slot even when forced; at 15:40Z -> OK; repeat within 20 h -> min_gap
  {
    const w = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, nowMs: Date.parse('2026-09-30T15:05:00Z') });
    const r = w.ctx.tfbDispatchShadowScorer();
    T('S4 scorer before slot', r === 'SKIPPED:before_slot' && w.fetches.length === 0, r);
    const rf = w.ctx.tfbDispatchShadowScorerNow();
    T('S4 Now cannot bypass before_slot', rf === 'SKIPPED:before_slot' && w.fetches.length === 0, rf);
    T('S4 skip row carries slot_utc', /"slot_utc":"15:20"/.test(String(w.logRows[0][9])) && w.logRows[0][3] === 'shadow_scorer.yml', String(w.logRows[0][9]).slice(0, 160));
    w.nowMs = Date.parse('2026-09-30T15:40:00Z');
    const r2 = w.ctx.tfbDispatchShadowScorer();
    T('S4 scorer dispatch 18:40 Riyadh', r2 === 'OK:204' && w.fetches.length === 1, r2);
    T('S4 scorer url + payload', /shadow_scorer\.yml\/dispatches$/.test(w.fetches[0].url) && payload(w, 0).inputs.dry_run === 'false' && payload(w, 0).inputs.rollback_drill_passed === undefined);
    T('S4 S1 LAST_OK set', /^\d{13}$/.test(String(w.props.TFB_S1_DISPATCH_LAST_OK_MS || '')));
    w.nowMs += 6 * 3600 * 1000;
    const r3 = w.ctx.tfbDispatchShadowScorer();
    T('S4 scorer once per day (min gap 1200)', r3 === 'SKIPPED:min_gap' && w.fetches.length === 1, r3);
    w.nowMs += 15 * 3600 * 1000;   // next day 12:40Z -> before slot again
    T('S4 next day before slot', w.ctx.tfbDispatchShadowScorer() === 'SKIPPED:before_slot');
    w.nowMs += 3 * 3600 * 1000;    // next day 15:40Z
    T('S4 next day after slot dispatches', w.ctx.tfbDispatchShadowScorer() === 'OK:204' && w.fetches.length === 2);
    noTokenLeak(w, 'S4');
    // custom slot property
    const w2 = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN, TFB_S1_DISPATCH_SLOT_UTC: '16:00' }, nowMs: Date.parse('2026-09-30T15:40:00Z') });
    T('S4 custom slot 16:00 blocks 15:40Z', w2.ctx.tfbDispatchShadowScorer() === 'SKIPPED:before_slot');
  }
  // S5 no token / kill switches
  {
    const w = makeWorld({ props: {} });
    T('S5 board no token', w.ctx.tfbDispatchShadowBoard() === 'FAILED:no_token' && w.logRows[0][1] === 'ERROR' && w.logRows[0][3] === 'shadow_board.yml');
    T('S5 scorer no token', w.ctx.tfbDispatchShadowScorer() === 'FAILED:no_token' && w.fetches.length === 0);
    T('S5 probe no token', w.ctx.tfbS1DispatchProbe() === 'shadow_board=FAILED:no_token | shadow_scorer=FAILED:no_token' && w.fetches.length === 0);
    const w2 = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN, TFB_SYNC_DISPATCH_DISABLED: '1' } });
    T('S5 global kill', w2.ctx.tfbDispatchShadowBoard() === 'SKIPPED:disabled' && w2.ctx.tfbDispatchShadowScorerNow() === 'SKIPPED:disabled' && w2.fetches.length === 0);
    const w3 = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN, TFB_SB_DISPATCH_DISABLED: '1' }, boardAsOf: '2026-09-29 22:23' });
    T('S5 lane kill (board only)', w3.ctx.tfbDispatchShadowBoard() === 'SKIPPED:lane_disabled' && w3.ctx.tfbDispatchShadowScorer() === 'OK:204' && w3.fetches.length === 1);
    const w4 = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN, TFB_S1_DISPATCH_DISABLED: '1' }, boardAsOf: '2026-09-29 22:23' });
    T('S5 lane kill (scorer only)', w4.ctx.tfbDispatchShadowScorer() === 'SKIPPED:lane_disabled' && w4.ctx.tfbDispatchShadowBoard() === 'OK:204');
  }
  // S6 GitHub 422 (bad inputs) -> FAILED:422, no LAST_OK, token redacted; fetch throws -> FAILED:-1
  {
    const w = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, fetchCode: 422, fetchBody: '{"message":"Unexpected inputs provided ' + TOKEN + '"}' });
    const r = w.ctx.tfbDispatchShadowScorer();
    T('S6 422 result', r === 'FAILED:422' && !w.props.TFB_S1_DISPATCH_LAST_OK_MS && w.logRows[0][4] === 'FAILED' && w.logRows[0][7] === 422, r);
    noTokenLeak(w, 'S6');
    const w2 = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, fetchThrow: 'DNS error ' + TOKEN, boardAsOf: '2026-09-29 22:23' });
    T('S6 fetch throws -> FAILED:-1', w2.ctx.tfbDispatchShadowBoard() === 'FAILED:-1');
    noTokenLeak(w2, 'S6b');
  }
  // S7 probe 200 for both lanes (GET, no dispatch)
  {
    const w = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, fetchCode: 200, fetchBody: JSON.stringify({ id: 777, state: 'active' }) });
    const r = w.ctx.tfbS1DispatchProbe();
    T('S7 probe both', r === 'shadow_board=OK:200:active | shadow_scorer=OK:200:active', r);
    T('S7 probe GETs', w.fetches.length === 2 && w.fetches.every((f) => f.options.method === 'get') && /shadow_board\.yml$/.test(w.fetches[0].url) && /shadow_scorer\.yml$/.test(w.fetches[1].url));
    T('S7 probe rows', w.logRows.length === 2 && w.logRows[0][3] === 'shadow_board.yml' && w.logRows[1][3] === 'shadow_scorer.yml' && /hours=8,17 minute=10/.test(String(w.logRows[0][5])) && /hours=18 minute=40/.test(String(w.logRows[1][5])));
    noTokenLeak(w, 'S7');
  }
  // S8 triggers: install both lanes (defaults), idempotent, daily_sync + foreign triggers untouched; remove
  {
    const w = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, triggers: ['tfbDispatchDailySync', 'tfbAutoRefreshTrigger', 'tfbDispatchShadowBoard'] });
    const r = w.ctx.tfbInstallS1DispatchTriggers();
    T('S8 install result', r === 'OK:shadow_board:hours=8,17:minute=10:removed=1 | shadow_scorer:hours=18:minute=40:removed=0', r);
    T('S8 deleted only own stale board trigger', w.deleted.length === 1 && w.deleted[0] === 'tfbDispatchShadowBoard');
    T('S8 created specs', w.created.length === 3 &&
      w.created[0].handler === 'tfbDispatchShadowBoard' && w.created[0].atHour === 8 && w.created[0].nearMinute === 10 &&
      w.created[1].handler === 'tfbDispatchShadowBoard' && w.created[1].atHour === 17 && w.created[1].nearMinute === 10 &&
      w.created[2].handler === 'tfbDispatchShadowScorer' && w.created[2].atHour === 18 && w.created[2].nearMinute === 40 &&
      w.created.every((s) => s.everyDays === 1 && s.tz === 'Asia/Riyadh'), JSON.stringify(w.created));
    const st = w.ctx.tfbS1DispatchStatus();
    T('S8 status line', /shadow_board: .*hours=8,17 minute=10.*triggers=2/.test(st) && /shadow_scorer: .*hours=18 minute=40.*slot_utc=15:20.*triggers=1/.test(st) && st.indexOf(TOKEN) === -1, st.slice(0, 200));
    const r2 = w.ctx.tfbInstallS1DispatchTriggers();
    T('S8 idempotent re-install', /removed=2 \| .*removed=1$/.test(r2) && w.created.length === 6, r2);
    const rr = w.ctx.tfbRemoveS1DispatchTriggers();
    T('S8 remove both lanes', rr === 'OK:removed=3' && /triggers=0/.test(w.ctx.tfbS1DispatchStatus()), rr);
    T('S8 daily_sync trigger untouched throughout', w.deleted.indexOf('tfbDispatchDailySync') === -1 && w.deleted.indexOf('tfbAutoRefreshTrigger') === -1);
    // custom hours/minute props
    const w2 = makeWorld({ props: { TFB_SB_DISPATCH_HOURS: '9', TFB_SB_DISPATCH_MINUTE: '5', TFB_S1_DISPATCH_HOURS: '19', TFB_S1_DISPATCH_MINUTE: '0' } });
    T('S8 custom props', w2.ctx.tfbInstallS1DispatchTriggers() === 'OK:shadow_board:hours=9:minute=5:removed=0 | shadow_scorer:hours=19:minute=0:removed=0');
  }
  // S9 v1.0.0 daily_sync path byte-behavior: payload still {run_mode}, Page daily_sync.yml, status line unchanged shape
  {
    const w = makeWorld({ props: { TFB_GH_DISPATCH_TOKEN: TOKEN }, gmStamp: '2026-09-30 02:00:00+03:00', nowMs: Date.parse('2026-09-30T03:15:00Z') });
    const r = w.ctx.tfbDispatchDailySync();
    T('S9 daily_sync dispatch', r === 'OK:204' && /daily_sync\.yml\/dispatches$/.test(w.fetches[0].url), r);
    T('S9 daily_sync payload unchanged', JSON.stringify(payload(w, 0)) === JSON.stringify({ ref: 'main', inputs: { run_mode: 'full_sync' } }), JSON.stringify(payload(w, 0)));
    T('S9 daily_sync row Page', w.logRows[0][3] === 'daily_sync.yml' && w.logRows[0][2] === 'tfbDispatchDailySync');
    T('S9 v1.0.0 status still renders', /^SYNC-DISPATCH v1\.1\.0 \| token=present/.test(w.ctx.tfbSyncDispatchStatus()));
  }
}

run();
const digest = crypto.createHash('sha256').update(out.join('\n')).digest('hex').slice(0, 12);
console.log(out.join('\n'));
console.log(`SUMMARY ${out.length - fails}/${out.length} PASS | digest ${digest}`);
process.exit(fails ? 1 : 0);
