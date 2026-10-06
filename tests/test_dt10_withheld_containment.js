/* Actual GAS render and log functions with an in-memory Spreadsheet service.
 * Optional source path supports a frozen baseline adverse replay.
 */
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const assert = require('assert');
const source = fs.readFileSync(process.argv[2] ||
  path.join(__dirname, '../apps_script/16_Decision_Top10.gs'), 'utf8');

function stamp(state, time = Date.now()) {
  return state + ' | ' + new Date(time).toISOString().slice(0, 19).replace('T', ' ') + '+0000';
}

function fixture() {
  return {
    status: 'ok', meta: {},
    kpis: {passed: 1, selected_count: 1, max_selected: 10, fundable_now: 1,
      expected_gain_12m_sar: 1000, capital_call_topn_sar: 1000},
    selected: [{symbol: 'SAFE.US', suggested_shares: 20, suggested_sar: 7500,
      price_sar: 375, advisor_note: 'INVEST 20 shares funded from cash',
      detail: {funds_from: 'cash'}}],
    near_miss: [{symbol: 'LATER.US', failed_gate: 'Funding', current: 'capital exhausted',
      required: 'CAPITAL_CALL: deposit 1,000 SAR', improve_note: 'add Cash Available'}],
    alerts: [{type: 'capital_call', count: 1, required_action: 'Deposit 1,000 SAR'},
      {type: 'rotation_proposal', count: 1, required_action: 'SAFE.US is fundable by exit of OLD.US'},
      {type: 'missing_fx', count: 1, required_action: 'Add approved FX data'}],
    candidates_rows: [{symbol: 'SAFE.US', verdict: 'INVEST', selected: true,
      deferral: 'CAPITAL_CALL: deposit 1,000 SAR',
      failure_reason: 'fundable amount requires deposit 1,000 SAR', opportunity_score: 80}],
  };
}

// Exact strings captured from opportunity_builder._build_alerts,
// _cash_floor_alert_text and the additive funding alerts in _build.
// Cash-floor observe/enforce both carry sizing amounts despite being research
// diagnostics. A WITHHELD display retains only the containment disclosure.
const producerFundingAlerts = [
  {type: 'no_deployable_capital', count: 1, required_action:
    'Set PF: Cash Available SAR (My_Portfolio controls / _Lists_Config defaults) — tickets are sized at 0'},
  {type: 'unfunded_candidates', count: 1, required_action:
    "These name(s) passed every gate and ranked, but deployable capital was exhausted before funding — increase Cash Available (or reduce Max Selected). Shown as WATCH, not executable tickets."},
  {type: 'rotation_proposal', count: 1, required_action:
    'SAFE.US is fundable by exit of OLD.US 5,000 SAR (engine 12M edge +10pp after cost). A rotation is a ticket, not an order: written GO with a live quote required.'},
  {type: 'capital_call', count: 1, required_action:
    "Deposit ≥ 1,000 SAR to take the top-1 qualified ticket(s): SAFE.US. Qualified names are shown regardless of cash; funding is the operator's decision."},
  {type: 'cash_floor', mode: 'observe', count: 2, required_action:
    'Cash floor 10% of NAV 100,000 SAR = 10,000 SAR (observe): deployable would fall 15,000 SAR -> 5,000 SAR; 2 of 3 sized seat(s) would lose funding (10,000 SAR). No ticket changed.'},
  {type: 'cash_floor', mode: 'enforce', count: 1, required_action:
    'Cash floor 10% of NAV 100,000 SAR = 10,000 SAR ENFORCED: deployable 5,000 SAR (reserve kept out of sizing and funding).'},
];

// The first two were captured by calling the actual _select_and_size with
// 500 SAR cash and 1,000 SAR floor (venue lot 100 for the second case).
// Remaining strings come from _build's exhausted-capital deferral and
// _funding_plan_text. No funding-plan suffix is needed to classify the first
// three: the builder may run with the additive funding-plan feature disabled.
const producerFundingDeferrals = [
  'Unfunded — sized ticket 500 SAR below minimum ticket floor 1,000 SAR',
  'Unfunded — one board lot (100 sh ≈ 5,000 SAR) exceeds the sized allocation 500 SAR',
  'Unfunded — passed all gates and ranked, but deployable capital was exhausted before funding',
  'Unfunded — sized ticket 500 SAR below minimum ticket floor 1,000 SAR | FUNDABLE_BY_ROTATION: exit OLD.US 5,000 SAR (engine 12M edge +10pp after cost) → ticket 1,000 SAR',
  'Unfunded — sized ticket 500 SAR below minimum ticket floor 1,000 SAR | CAPITAL_CALL: deposit ≥ 500 SAR for a 1,000 SAR ticket (cash 500 SAR) — ROTATION_INSUFFICIENT: exit OLD.US covers 400 SAR of 500 SAR (engine 12M edge +10pp after cost); no exit proposed',
  'Duplicate issuer — already funded FIRST.US',
];

function cleanFundingFixture() {
  const payload = fixture();
  payload.alerts = [];
  payload.near_miss = [];
  payload.candidates_rows[0].deferral = null;
  payload.candidates_rows[0].failure_reason = null;
  return payload;
}

function environment(initialVerdict, properties = {}) {
  const env = {verdict: initialVerdict, reads: 0, writes: [], properties, afterRead: null,
    pageStatuses: [], httpBodies: [], backgrounds: new Map()};
  function sheet(name) {
    const grid = new Map();
    const sh = {name, grid,
      getMaxRows: () => 500, getMaxColumns: () => 40, getParent: () => ss,
      getLastRow: () => Math.max(0, ...Array.from(grid.keys()).map(key => +key.split(':')[0])),
      insertColumnsAfter: () => sh, setFrozenRows: () => sh,
      getRange(r, c, nr = 1, nc = 1) {
        if (typeof r === 'string') {
          assert.strictEqual(r, 'L1:M60');
          return {getValues() {
            env.reads++;
            if (env.readError) throw new Error('status unavailable');
            const result = [['TFB Decision Feed', env.verdict]];
            if (env.afterRead) env.afterRead();
            return result;
          }};
        }
        const get = (rr, cc) => grid.get(rr + ':' + cc) ?? '';
        let range;
        range = new Proxy({}, {get: (_, method) => (...args) => {
          if (method === 'getValues') return Array.from({length: nr}, (_, i) =>
            Array.from({length: nc}, (_, j) => get(r + i, c + j)));
          if (method === 'getValue') return get(r, c);
          if (method === 'getRow') return r;
          if (method === 'getColumn') return c;
          if (method === 'getNumRows') return nr;
          if (method === 'getNumColumns') return nc;
          if (method === 'setValues') {
            if (env.failWrite === env.writes.length + 1) throw new Error('interrupted write');
            const values = JSON.parse(JSON.stringify(args[0]));
            env.writes.push({sheet: name, r, c, values});
            values.forEach((row, i) => row.forEach((value, j) => grid.set((r + i) + ':' + (c + j), value)));
          }
          if (method === 'setValue') grid.set(r + ':' + c, args[0]);
          if (method === 'setBackground') {
            for (let i = 0; i < nr; i++) for (let j = 0; j < nc; j++)
              env.backgrounds.set(name + ':' + (r + i) + ':' + (c + j), args[0]);
          }
          if (method === 'clear' || method === 'clearContent') {
            for (let i = 0; i < nr; i++) for (let j = 0; j < nc; j++) grid.delete((r + i) + ':' + (c + j));
          }
          return range;
        }});
        return range;
      },
    };
    return sh;
  }
  const sheets = {};
  const ss = {getSheetByName: name => sheets[name] || null, getRangeByName: () => null,
    insertSheet: name => (sheets[name] = sheet(name))};
  sheets._Status = sheet('_Status');
  sheets.Top_10_Investments = sheet('Top_10_Investments');
  env.context = {
    PropertiesService: {getScriptProperties: () => ({
      getProperty: key => properties[key] ?? null,
      setProperty: (key, value) => {properties[key] = value;},
      deleteProperty: key => {delete properties[key];},
    })},
    SpreadsheetApp: {BorderStyle: {SOLID: 'solid'}, getActiveSpreadsheet: () => ss, flush: () => {}},
    UrlFetchApp: {fetch: (url, options) => {
      env.httpBodies.push(JSON.parse(options.payload));
      return {getResponseCode: () => 200, getContentText: () => JSON.stringify(env.httpPayload)};
    }},
    // External house writer is an I/O sink; actual dt10WritePageStatus_
    // supplies the message. Its native 02_Core implementation is unavailable.
    writePageStatus_: (page, status) => {
      env.pageStatuses.push({page, ...status});
      sheets._Status.grid.set('61:13', status.message);
    },
    Utilities: {formatDate: date => date.toISOString().slice(0, 10)},
    Session: {getScriptTimeZone: () => 'Asia/Riyadh'}, Logger: {log: () => {}},
  };
  vm.createContext(env.context);
  vm.runInContext(source, env.context);
  env.ss = ss;
  env.sheet = sheets.Top_10_Investments;
  env.render = payload => env.context.dt10RenderPayload_(env.sheet, payload,
    env.context.DT10_FALLBACK_TOKENS);
  env.row = (header, sheetName = 'Top_10_Investments') => {
    const table = env.writes.find(write => write.sheet === sheetName &&
      write.values.length === 1 && write.values[0].includes(header));
    assert.ok(table, 'missing table ' + header);
    return table.values[0].map((_, j) => sheets[sheetName].grid.get((table.r + 1) + ':' + (j + 1)) ?? '');
  };
  env.log = (payload, state) => env.context.dt10AppendSelectionLog_(ss,
    payload.selected, 'rendered ' + state, {}, state);
  env.refresh = payload => {
    env.httpPayload = payload;
    env.sheet.grid.set(env.context.DT10_ROW_PANEL_HEAD + ':1', 'CONTROL PANEL');
    env.sheet.grid.set(env.context.DT10_ROW_KPI_HEAD + ':1', 'KPIs');
    for (const [index, item] of env.context.DT10_PANEL.entries()) {
      const position = env.context.dt10PanelPos_(index);
      const value = item.label === 'Pool Source' ? 'Backend' :
        item.label === 'T10: Stability Enabled' ? 'No' : item.def;
      env.sheet.grid.set(position.row + ':' + position.valueCol, value);
    }
    properties.DT10_SYNC_INFLIGHT = 'off';
    properties.DT10_SEND_HOLDINGS = '0';
    return env.context.refreshDecisionTop10();
  };
  return env;
}

function assertWithheld(env, payload) {
  assert.strictEqual(env.context.dt10OutputStatus_(payload), 'WITHHELD');
  const board = env.row('Ticket SAR');
  for (const index of env.context.DT10_UV_BOARD_WITHHOLD_IDX) assert.strictEqual(board[index], '—');
  assert.ok(board[env.context.DT10_UV_BOARD_NOTE_IDX].startsWith('SIZING WITHHELD'));
  const near = env.row('How To Qualify');
  assert.ok(near[3].startsWith('WITHHELD'));
  const qualified = env.row('Why Not Selected');
  assert.strictEqual(qualified[13], 'WITHHELD');
  const qualifiedHeader = env.writes.find(write => write.sheet === 'Top_10_Investments' &&
    write.values.length === 1 && write.values[0].includes('Why Not Selected'));
  assert.notStrictEqual(env.backgrounds.get('Top_10_Investments:' + (qualifiedHeader.r + 1) + ':14'),
    env.context.DT10_FALLBACK_TOKENS.VERDICT_POSITIVE.bg);
  const candidates = env.row('Deferral');
  assert.strictEqual(candidates[23], 'WITHHELD');
  assert.ok(candidates[21].startsWith('WITHHELD'));
  assert.ok(candidates[24].startsWith('WITHHELD'));
  const displayed = Array.from(env.sheet.grid.values()).map(String).join('\n');
  assert.ok(!/CAPITAL_CALL:|deposit 1,000|is fundable by/i.test(displayed), displayed);
  // The first KPI write is already withheld, even if a later write fails.
  assert.strictEqual(env.writes[0].values[0][2], '0 / 10 (withheld)');
  assert.strictEqual(env.writes[0].values[0][1], '—');
  for (const index of [7, 8, 9, 10]) assert.strictEqual(env.writes[0].values[0][index], '—');
  assert.strictEqual(env.reads, 1);
}

let passed = 0, failed = 0;
function test(name, run) {
  try { run(); passed++; console.log('PASS ' + name); }
  catch (error) { failed++; console.error('FAIL ' + name + ': ' + error.message); }
}

for (const [name, verdict] of [
  ['known invalid feed', stamp('NOT_ACTIONABLE(partial:GM)')],
  ['missing verdict', ''],
  ['stale verdict', stamp('EXECUTABLE', Date.now() - 481 * 60000)],
  ['future verdict', stamp('EXECUTABLE', Date.now() + 31 * 60000)],
  ['partial feed', stamp('PARTIAL')],
]) test(name, () => {
  const env = environment(verdict), payload = fixture();
  const before = JSON.stringify([payload.selected, payload.near_miss, payload.alerts, payload.candidates_rows]);
  env.render(payload);
  assertWithheld(env, payload);
  assert.strictEqual(JSON.stringify([payload.selected, payload.near_miss, payload.alerts,
    payload.candidates_rows]), before, 'raw research evidence changed');
});

test('read failure remains withheld', () => {
  const env = environment(stamp('EXECUTABLE')), payload = fixture();
  env.readError = true;
  env.render(payload);
  assertWithheld(env, payload);
});

test('known WITHHELD ignores legacy funding/KPI display escapes', () => {
  const env = environment(stamp('NOT_ACTIONABLE(partial:GM)'), {
    DT10_FUNDING_WITHHOLD_LEGACY: '1', DT10_KPI_TRUTH: '0',
  }), payload = fixture();
  env.render(payload);
  assertWithheld(env, payload);
});

test('later valid feed cannot release a WITHHELD selection log', () => {
  const env = environment(stamp('NOT_ACTIONABLE(partial:GM)')), payload = fixture();
  env.afterRead = () => {env.verdict = stamp('EXECUTABLE');};
  env.render(payload);
  assert.strictEqual(env.log(payload, 'WITHHELD'), 'SelLog: +1');
  const logged = env.row('Ticket SAR', '_Selection_Log');
  assert.strictEqual(logged[13], '—');
  assert.strictEqual(logged[14], '—');
  for (const index of env.context.DT10_UV_LOG_WITHHOLD_IDX) assert.strictEqual(logged[index], '—');
  assert.ok(logged[env.context.DT10_UV_LOG_NOTE_IDX].includes('snapshot WITHHELD'));
  assert.strictEqual(env.reads, 1, 'log reclassified the rendered snapshot');
  assertWithheld(env, payload);
});

test('rotation funding alert is suppressed independently of near misses', () => {
  const env = environment(stamp('NOT_ACTIONABLE(partial:GM)')), payload = fixture();
  payload.near_miss = [];
  payload.alerts = payload.alerts.filter(alert => alert.type === 'rotation_proposal');
  payload.meta.upstream_verdict = 'NOT_ACTIONABLE(partial:GM)';
  env.render(payload);
  const alert = env.row('Required Action');
  assert.strictEqual(alert[0], 'funding_withheld');
  assert.ok(!Array.from(env.sheet.grid.values()).some(value => /is fundable by/i.test(String(value))));
});

test('candidate funding text is contained independently of funding alerts', () => {
  const env = environment(stamp('NOT_ACTIONABLE(partial:GM)')), payload = fixture();
  payload.near_miss = [];
  payload.alerts = [];
  env.render(payload);
  const candidate = env.row('Deferral');
  assert.ok(candidate[21].startsWith('WITHHELD'));
  assert.ok(candidate[24].startsWith('WITHHELD'));
  assert.strictEqual(candidate[23], 'WITHHELD');
});

for (const alert of producerFundingAlerts) test('actual producer alert: ' +
  alert.type + (alert.mode ? ' ' + alert.mode : ''), () => {
  const payload = cleanFundingFixture();
  payload.alerts = [alert];
  const original = JSON.stringify(payload.alerts);
  const blocked = environment(stamp('NOT_ACTIONABLE(partial:GM)'));
  blocked.render(payload);
  const displayed = blocked.row('Required Action');
  assert.strictEqual(displayed[0], 'funding_withheld');
  assert.ok(displayed[2].includes('sizing withheld'));
  assert.ok(!Array.from(blocked.sheet.grid.values()).includes(alert.required_action));
  assert.strictEqual(JSON.stringify(payload.alerts), original, 'producer alert evidence changed');
  const healthy = environment(stamp('EXECUTABLE'));
  healthy.render(payload);
  assert.strictEqual(healthy.row('Required Action')[0], alert.type);
  assert.strictEqual(healthy.row('Required Action')[2], alert.required_action);
});

for (const [index, deferral] of producerFundingDeferrals.entries())
  test('actual producer candidate deferral ' + (index + 1), () => {
    const payload = cleanFundingFixture();
    payload.candidates_rows[0].selected = false;
    payload.candidates_rows[0].deferral = deferral;
    const blocked = environment(stamp('NOT_ACTIONABLE(partial:GM)'));
    blocked.render(payload);
    assert.ok(blocked.row('Deferral')[24].startsWith('WITHHELD'));
    assert.strictEqual(blocked.row('Why Not Selected')[13], 'WITHHELD');
    assert.strictEqual(payload.candidates_rows[0].deferral, deferral);
    const healthy = environment(stamp('EXECUTABLE'));
    healthy.render(payload);
    assert.strictEqual(healthy.row('Deferral')[24], deferral);
  });

test('actual board-lot near miss is contained despite its diversification label', () => {
  const payload = cleanFundingFixture();
  payload.near_miss = [{symbol: 'SAFE.US', failed_gate: 'Diversification',
    current: producerFundingDeferrals[1], required: 'within sector/market caps',
    improve_note: 'Qualified (INVEST) — deferred by diversification cap'}];
  const blocked = environment(stamp('NOT_ACTIONABLE(partial:GM)'));
  blocked.render(payload);
  assert.strictEqual(blocked.row('How To Qualify')[2], '—');
  assert.ok(blocked.row('How To Qualify')[3].startsWith('WITHHELD'));
});

test('interrupted render never initially writes an executable KPI', () => {
  const env = environment(stamp('NOT_ACTIONABLE(partial:GM)')), payload = fixture();
  env.failWrite = 4;
  assert.throws(() => env.render(payload), /interrupted write/);
  assert.strictEqual(env.writes[0].values[0][2], '0 / 10 (withheld)');
  const board = env.row('Ticket SAR');
  assert.strictEqual(board[10], '—');
  assert.strictEqual(board[11], '—');
});

test('ordinary valid feed retains sizing and funding research', () => {
  const env = environment(stamp('EXECUTABLE')), payload = fixture();
  env.render(payload);
  assert.strictEqual(env.context.dt10OutputStatus_(payload), 'EXECUTABLE');
  const board = env.row('Ticket SAR');
  assert.strictEqual(board[10], 7500);
  assert.strictEqual(board[11], 20);
  assert.strictEqual(env.row('How To Qualify')[3], 'CAPITAL_CALL: deposit 1,000 SAR');
  assert.strictEqual(env.row('Deferral')[24], 'CAPITAL_CALL: deposit 1,000 SAR');
  const qualifiedHeader = env.writes.find(write => write.sheet === 'Top_10_Investments' &&
    write.values.length === 1 && write.values[0].includes('Why Not Selected'));
  assert.strictEqual(env.backgrounds.get('Top_10_Investments:' + (qualifiedHeader.r + 1) + ':14'),
    env.context.DT10_FALLBACK_TOKENS.VERDICT_POSITIVE.bg);
  assert.strictEqual(env.log(payload, 'EXECUTABLE'), 'SelLog: +1');
  assert.strictEqual(env.row('Ticket SAR', '_Selection_Log')[13], 7500);
  assert.strictEqual(env.row('Ticket SAR', '_Selection_Log')[14], 20);
});

test('a later invalid feed may conservatively withhold a previously valid log', () => {
  const env = environment(stamp('EXECUTABLE')), payload = fixture();
  env.render(payload);
  env.verdict = stamp('NOT_ACTIONABLE(partial:GM)');
  assert.strictEqual(env.log(payload, 'EXECUTABLE'), 'SelLog: +1');
  assert.strictEqual(env.row('Ticket SAR', '_Selection_Log')[13], '—');
});

function refreshDestinations(env) {
  assert.strictEqual(env.httpBodies.length, 1, 'refresh did not post the request');
  assert.strictEqual(env.pageStatuses.length, 1, 'refresh did not write page status');
  assert.strictEqual(env.pageStatuses[0].status, 'OK');
  assert.strictEqual(env.pageStatuses[0].page, 'Top_10_Investments');
  const visibleStatus = env.sheet.grid.get(env.context.DT10_ROW_STATUS + ':2');
  const pageStatus = env.ss.getSheetByName('_Status').grid.get('61:13');
  assert.strictEqual(pageStatus, visibleStatus, '_Status differs from the visible status');
  const logRunInfo = env.row('Run Info', '_Selection_Log')[1];
  assert.ok(visibleStatus.startsWith(logRunInfo));
  return [visibleStatus, pageStatus, logRunInfo];
}

test('actual refresh contains grace funded/gain diagnostics in all three destinations', () => {
  const env = environment(stamp('NOT_ACTIONABLE(partial:GM)')), payload = fixture();
  payload.selected[0]._grace_hold = true;
  assert.ok(env.context.dt10SeatCheckNote_(payload).includes('gain kpi 1000 SAR'));
  env.refresh(payload);
  for (const displayed of refreshDestinations(env)) {
    assert.ok(displayed.includes('output: WITHHELD'));
    assert.ok(!/SEAT-CHECK|gain kpi|1 funded|1000 SAR/.test(displayed), displayed);
  }
  assert.strictEqual(env.row('Ticket SAR', '_Selection_Log')[13], '—');
  assert.strictEqual(env.row('Ticket SAR', '_Selection_Log')[14], '—');
});

test('actual refresh retains actionable-feed grace diagnostics in all three destinations', () => {
  const env = environment(stamp('EXECUTABLE')), payload = fixture();
  payload.selected[0]._grace_hold = true;
  const expected = env.context.dt10SeatCheckNote_(payload);
  assert.strictEqual(expected,
    'SEAT-CHECK kpi 1 funded vs board 0 exec +1 grace; gain kpi 1000 SAR vs board 0 exec');
  env.refresh(payload);
  for (const displayed of refreshDestinations(env)) {
    assert.ok(displayed.includes('output: HELD'));
    assert.ok(displayed.includes(expected));
  }
});

test('actual refresh preserves executable seat diagnostics and quantities', () => {
  const env = environment(stamp('EXECUTABLE')), payload = fixture();
  payload.kpis.selected_count = 2;
  const expected = env.context.dt10SeatCheckNote_(payload);
  assert.strictEqual(expected, 'SEAT-CHECK kpi 2 funded vs board 1 exec');
  env.refresh(payload);
  for (const displayed of refreshDestinations(env)) {
    assert.ok(displayed.includes('output: EXECUTABLE'));
    assert.ok(displayed.includes(expected));
  }
  assert.strictEqual(env.row('Ticket SAR', '_Selection_Log')[13], 7500);
  assert.strictEqual(env.row('Ticket SAR', '_Selection_Log')[14], 20);
});

console.log(`GAS withheld containment: ${passed} passed, ${failed} failed`);
process.exitCode = failed ? 1 : 0;
