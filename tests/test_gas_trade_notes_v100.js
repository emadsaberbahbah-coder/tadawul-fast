#!/usr/bin/env node
/* tests/test_gas_trade_notes_v100.js
 * Harness for apps_script/25_Trade_Notes.gs v1.0.0 [Program v2 trade notes +
 * divergence ledger]. Loads the REAL .gs into a vm context with a small
 * in-memory Sheets model (getSheetByName / insertSheet / getRange(...).
 * getValues / setValues / setValue / clearContent / setDataValidation /
 * appendRow / getLastRow / setFrozenRows), PropertiesService and Logger.
 * Every test calls the real functions.  Run: node tests/test_gas_trade_notes_v100.js
 */
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const crypto = require('crypto');

const SRC = path.join(__dirname, '..', 'apps_script', '25_Trade_Notes.gs');
const code = fs.readFileSync(SRC, 'utf8');

class Sheet {
  constructor(name, w) { this.name = name; this.rows = []; this.frozen = 0; this.validations = {}; this.appendLog = []; }
  _ensure(r, c) { while (this.rows.length < r) { this.rows.push([]); } for (const row of this.rows) { while (row.length < c) { row.push(''); } } }
  getLastRow() { let last = 0; this.rows.forEach((row, i) => { if (row.some((v) => v !== '' && v !== null && v !== undefined)) { last = i + 1; } }); return last; }
  setFrozenRows(n) { this.frozen = n; }
  appendRow(row) { const last = this.getLastRow(); this._ensure(last + 1, row.length); this.rows[last] = row.slice(); this.appendLog.push(row.slice()); }
  getRange(r, c, nr, nc) {
    nr = nr || 1; nc = nc || 1; const sh = this;
    return {
      getValues() { sh._ensure(r + nr - 1, c + nc - 1); const out = []; for (let i = 0; i < nr; i++) { out.push(sh.rows[r - 1 + i].slice(c - 1, c - 1 + nc)); } return out; },
      getValue() { return this.getValues()[0][0]; },
      setValues(vals) { sh._ensure(r + vals.length - 1, c + vals[0].length - 1); vals.forEach((row, i) => row.forEach((v, j) => { sh.rows[r - 1 + i][c - 1 + j] = v; })); },
      setValue(v) { this.setValues([[v]]); },
      clearContent() { this.setValues(Array.from({ length: nr }, () => Array(nc).fill(''))); },
      setDataValidation(rule) { sh.validations[r + ':' + c] = rule; }
    };
  }
}

function makeWorld(props) {
  const sheets = { _Run_Log: new Sheet('_Run_Log') };
  const ss = { getSheetByName: (n) => sheets[n] || null, insertSheet: (n) => { sheets[n] = new Sheet(n); return sheets[n]; } };
  const world = { sheets, logger: [], props: Object.assign({}, props || {}) };
  const ctx = {
    Date, JSON, Math, Number, String, parseInt, isNaN, Object, Array, RegExp, Error,
    PropertiesService: { getScriptProperties: () => ({ getProperty: (k) => (k in world.props ? world.props[k] : null) }) },
    SpreadsheetApp: {
      getActiveSpreadsheet: () => ss,
      newDataValidation: () => { const b = { list: null, allow: null, requireValueInList: (l, d) => { b.list = l; return b; }, setAllowInvalid: (a) => { b.allow = a; return b; }, build: () => ({ list: b.list, allow: b.allow }) }; return b; }
    },
    Logger: { log: (s) => world.logger.push(String(s)) }
  };
  vm.createContext(ctx);
  vm.runInContext(code + '\n;', ctx, { filename: '25_Trade_Notes.gs' });
  world.ctx = ctx;
  return world;
}

const out = []; let fails = 0;
function T(name, cond, detail) { out.push((cond ? 'PASS ' : 'FAIL ') + name + (detail ? ' | ' + detail : '')); if (!cond) { fails++; } }

// Version floor, not an exact pin: a later release must not turn this
// harness red (2026-10-07, v1.0.1 bumped the constant).
function verAtLeast(v, floor) {
  const a = String(v).split('.').map(Number), b = floor.split('.').map(Number);
  for (let i = 0; i < 3; i++) { if ((a[i] || 0) !== (b[i] || 0)) { return (a[i] || 0) > (b[i] || 0); } }
  return true;
}

function formIndex(ctx, key) {
  const rows = ctx.TFB_TN_FORM.concat(ctx.TFB_TN_SCORE_FORM);
  return rows.findIndex((r) => r[0] === key) + 2;
}

function run() {
  // T1 self-test + shape
  {
    const w = makeWorld();
    const v = w.ctx.tbTradeNotesSelfTest();
    T('T1 selftest', v === 'trade notes core: ok', v);
    T('T1 header 28 cols, Gen-independent', w.ctx.TFB_TN_HEADER.length === 28 && verAtLeast(w.ctx.TFB_TRADE_NOTES_VERSION, '1.0.0'));
  }
  // T2 setup idempotent
  {
    const w = makeWorld();
    const r1 = w.ctx.tbTradeNotesSetup();
    const tn = w.sheets._Trade_Notes, inp = w.sheets._Trade_Notes_Input;
    T('T2 setup creates both tabs', r1 === 'OK:_Trade_Notes,_Trade_Notes_Input' && tn && inp);
    T('T2 header row + frozen', JSON.stringify(tn.rows[0]) === JSON.stringify(w.ctx.TFB_TN_HEADER) && tn.frozen === 1);
    T('T2 form labels + dropdowns', inp.rows.length === 26 && inp.rows[1][0] === 'Type' && Object.keys(inp.validations).length === 5 && inp.validations['2:2'].list.join() === 'FILL,DIVERGENCE,FILL+DIVERGENCE');
    const before = JSON.stringify([tn.rows, inp.rows]);
    w.ctx.tbTradeNotesSetup();
    T('T2 second setup changes nothing', JSON.stringify([tn.rows, inp.rows]) === before);
    T('T2 run_log rows', w.sheets._Run_Log.appendLog.length === 2 && w.sheets._Run_Log.appendLog[0][2] === 'tbTradeNotesSetup');
  }
  // T3 seed D1 -> validation fails on blank lines 1/5 -> fill -> log -> id + row + cleared form
  {
    const w = makeWorld();
    w.ctx.tbTradeNotesSetup();
    const s = w.ctx.tbSeedD1_AER();
    const inp = w.sheets._Trade_Notes_Input;
    T('T3 seed D1', s.indexOf('OK:seeded D-1') === 0 && inp.rows[formIndex(w.ctx, 'symbol') - 1][1] === 'AER.US' && inp.rows[formIndex(w.ctx, 'qty') - 1][1] === 14 && inp.rows[formIndex(w.ctx, 'price') - 1][1] === 148.18);
    const fail = w.ctx.tbLogTradeNoteFromInput();
    T('T3 blank thesis/size rejected', fail === 'FAILED:missing thesis; missing size_rationale', fail);
    T('T3 nothing appended on failure', w.sheets._Trade_Notes.getLastRow() === 1);
    inp.getRange(formIndex(w.ctx, 'thesis'), 2).setValue('AER earns its 9x multiple as lease rates hold through 2027');
    inp.getRange(formIndex(w.ctx, 'size_rationale'), 2).setValue('8.3% NAV from IBKR cash, inside the 10% cap');
    const ok = w.ctx.tbLogTradeNoteFromInput();
    T('T3 logged id', ok === 'OK:TN-20260928-001', ok);
    const row = w.sheets._Trade_Notes.rows[1];
    T('T3 row content', row[0] === 'TN-20260928-001' && row[2] === 'FILL+DIVERGENCE' && row[3] === '2026-09-28' && row[4] === 'AER.US' && row[5] === 'BUY' && row[6] === 14 && row[7] === 148.18 && row[11] === 'OVERRIDE' && row[12] === 'YES' && row[13] === 'JUDGMENT' && row[14] === 'D-1' && row[16].indexOf('AER earns') === 0 && row[21] === 8.3 && row[22] === '' && row[27] === w.ctx.TFB_TRADE_NOTES_VERSION, JSON.stringify([row[0]].concat(row.slice(2, 16))).slice(0, 200));
    const formVals = inp.getRange(2, 2, w.ctx.TFB_TN_FORM.length, 1).getValues().map((r) => r[0]);
    T('T3 form cleared after log', formVals.every((v) => v === ''));
    // D2 same day -> -002
    w.ctx.tbSeedD2_KRP();
    inp.getRange(formIndex(w.ctx, 'thesis'), 2).setValue('11% distribution yield holds while WTI stays above 85');
    inp.getRange(formIndex(w.ctx, 'size_rationale'), 2).setValue('7.6% NAV, second sleeve of the same cash, inside the 10% cap');
    const ok2 = w.ctx.tbLogTradeNoteFromInput();
    T('T3 second id same day', ok2 === 'OK:TN-20260928-002' && w.sheets._Trade_Notes.rows[2][4] === 'KRP.US' && w.sheets._Trade_Notes.rows[2][6] === 130, ok2);
    T('T3 KRP system verdict carried', w.sheets._Trade_Notes.rows[2][15].indexOf('EXIT (-21.1%') !== -1);
    // T4 score by id
    inp.getRange(formIndex(w.ctx, 'score_note_id'), 2).setValue('TN-20260928-002');
    inp.getRange(formIndex(w.ctx, 'score_outcome'), 2).setValue('loss');
    inp.getRange(formIndex(w.ctx, 'score_pnl_sar'), 2).setValue(-249);
    inp.getRange(formIndex(w.ctx, 'score_by'), 2).setValue('rule: stop 13.95');
    const sc = w.ctx.tbScoreTradeNoteFromInput();
    const r2 = w.sheets._Trade_Notes.rows[2];
    T('T4 scored', sc === 'OK:TN-20260928-002' && r2[22] === 'LOSS' && r2[23] === -249 && /^\d{4}-\d{2}-\d{2} /.test(r2[24]) && r2[25] === 'rule: stop 13.95', sc);
    T('T4 other row untouched', w.sheets._Trade_Notes.rows[1][22] === '');
    T('T4 score form cleared, note form untouched', inp.rows[formIndex(w.ctx, 'score_note_id') - 1][1] === '' && inp.rows[formIndex(w.ctx, 'score_outcome') - 1][1] === '');
    T('T4 unknown id', w.ctx.tbScoreTradeNote('TN-19990101-001', 'WIN', 1, 'x') === 'FAILED:note id not found');
    T('T4 bad outcome', w.ctx.tbScoreTradeNote('TN-20260928-001', 'maybe', 1, 'x').indexOf('FAILED:outcome not in') === 0);
    // run_log evidence
    const rl = w.sheets._Run_Log.appendLog.map((r) => r[2] + ':' + r[4]);
    T('T5 run_log trail', rl.includes('tbLogTradeNote:FAILED') && rl.filter((x) => x === 'tbLogTradeNote:OK').length === 2 && rl.includes('tbScoreTradeNote:OK'), rl.join(','));
  }
  // T6 programmatic API + date cell handling + custom tab names
  {
    const w = makeWorld({ TFB_TRADE_NOTES_TAB: 'Notes_X', TFB_TRADE_NOTES_INPUT_TAB: 'Notes_X_In' });
    const rec = { type: 'FILL', trade_date: '2026-10-05', symbol: 'nvda.us', side: 'buy', qty: 11, price: 225, ccy: 'usd', venue: 'IBKR', board_ref: 'Sat 2026-10-03 board rank 1', origin: 'board', divergence: 'no', reason_class: 'N/A (board-originated)', reason_ref: '', system_verdict: 'board ticket', thesis: 't', evidence: 'e', benchmark: 'b', exit_rule: 'stop 207.90', size_rationale: 's', size_pct_nav: 9.9, ledger_ref: 'NVDA.US 2026-10-05' };
    const r = w.ctx.tbLogTradeNote(rec);
    T('T6 api log to custom tab', r === 'OK:TN-20261005-001' && w.sheets.Notes_X && w.sheets.Notes_X.rows[1][4] === 'NVDA.US' && w.sheets.Notes_X.rows[1][12] === 'NO' && w.sheets.Notes_X.rows[1][5] === 'BUY', r);
    w.ctx.tbTradeNotesSetup();
    const inp = w.sheets.Notes_X_In;
    // a Date cell for the trade date (Sheets auto-parse): midnight Riyadh = 21:00Z previous day
    inp.getRange(formIndex(w.ctx, 'trade_date'), 2).setValue(new Date(Date.UTC(2026, 9, 5, 21, 0, 0)));
    ['type', 'symbol', 'side', 'origin', 'thesis', 'evidence', 'benchmark', 'exit_rule', 'size_rationale'].forEach((k, i) => inp.getRange(formIndex(w.ctx, k), 2).setValue(['DIVERGENCE', 'ADAM.US', 'NONE', 'NO-TRADE', 'declined the board ticket', 'rank 1 grace seat', 'Global', 'n/a', 'n/a'][i]));
    inp.getRange(formIndex(w.ctx, 'reason_class'), 2).setValue('JUDGMENT');
    inp.getRange(formIndex(w.ctx, 'system_verdict'), 2).setValue('board INVEST 269 sh');
    const r2 = w.ctx.tbLogTradeNoteFromInput();
    T('T6 date cell -> Riyadh date, no-trade divergence', r2 === 'OK:TN-20261006-001' && w.sheets.Notes_X.rows[2][3] === '2026-10-06' && w.sheets.Notes_X.rows[2][12] === 'YES' && w.sheets.Notes_X.rows[2][6] === '', r2 + ' ' + JSON.stringify(w.sheets.Notes_X.rows[2] && w.sheets.Notes_X.rows[2].slice(2, 8)));
  }
  // T7 header guard: existing empty tab gets the header; non-empty header untouched
  {
    const w = makeWorld();
    w.sheets._Trade_Notes = new Sheet('_Trade_Notes');
    w.ctx.tbTradeNotesSetup();
    T('T7 empty existing tab gets header', JSON.stringify(w.sheets._Trade_Notes.rows[0]) === JSON.stringify(w.ctx.TFB_TN_HEADER));
    w.sheets._Trade_Notes.rows[0][0] = 'CUSTOM';
    w.ctx.tbTradeNotesSetup();
    T('T7 non-empty header never overwritten', w.sheets._Trade_Notes.rows[0][0] === 'CUSTOM');
  }
}

run();
const digest = crypto.createHash('sha256').update(out.join('\n')).digest('hex').slice(0, 12);
console.log(out.join('\n'));
console.log(`SUMMARY ${out.length - fails}/${out.length} PASS | digest ${digest}`);
process.exit(fails ? 1 : 0);
