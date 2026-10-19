/* Full native Portfolio_Decision source with in-memory Apps Script services. */
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const SOURCE = fs.readFileSync(process.env.PF_GAS ||
  path.join(__dirname, '../../apps_script/10_My_Portfolio.gs'), 'utf8');
const NOW = Date.parse('2026-10-15T05:45:00Z');
const CAPTURE_ID = 'synthetic-capture';
const HOLD_HEADERS = ['Symbol', 'Name', 'Currency', 'Position Qty', 'Avg Cost', 'Current Price'];
const HOLD_ROW = ['SYNTH.US', 'Synthetic holding', 'USD', 10, 999, 6];
const VALID = {status: 'ok', kpis: {positions: 1},
  actions: [{action: 'BLOCK', symbol: 'SYNTH.US', name: 'Synthetic holding'}],
  sector_summary: [], alerts: [], meta: {route: {version: 'synthetic'}}};
const copy = value => JSON.parse(JSON.stringify(value));

function world(options = {}) {
  const operations = [], requests = [], logs = [], properties = Object.assign({
    PD_ACTION_LOG: 'always', PD_IBKR_EVIDENCE: options.watcher ? 'observe' : 'off',
    PD_IBKR_CAPTURE_FOLDER_ID: 'synthetic_folder_0123456789',
  }, options.properties);
  const state = {response: copy(options.response === undefined ? VALID : options.response),
    code: options.code === undefined ? 200 : options.code, networkError: false,
    operations, requests, logs, properties, locks: 0};

  class Sheet {
    constructor(name, rows = [], firstRow = 1) {
      this.name = name;
      this.cells = new Map();
      rows.forEach((row, i) => row.forEach((value, j) => this.put(firstRow + i, j + 1, value)));
    }
    put(row, col, value) { this.cells.set(row + ':' + col, value); }
    value(row, col) { return this.cells.has(row + ':' + col) ? this.cells.get(row + ':' + col) : ''; }
    getLastRow() { return Math.max(0, ...Array.from(this.cells.keys(), key => +key.split(':')[0])); }
    getLastColumn() { return Math.max(0, ...Array.from(this.cells.keys(), key => +key.split(':')[1])); }
    getMaxRows() { return 80; }
    getMaxColumns() { return 40; }
    setFrozenRows() { return this; }
    getRange(row, col, nr = 1, nc = 1) {
      const sheet = this;
      let range;
      const methods = {
        getValue: () => sheet.value(row, col),
        getValues: () => Array.from({length: nr}, (_, i) =>
          Array.from({length: nc}, (_, j) => sheet.value(row + i, col + j))),
        setValue(value) {
          sheet.put(row, col, value);
          operations.push({sheet: sheet.name, method: 'setValue', row, col, value});
          return range;
        },
        setValues(values) {
          values.forEach((valuesRow, i) => valuesRow.forEach((value, j) => sheet.put(row + i, col + j, value)));
          operations.push({sheet: sheet.name, method: 'setValues', row, col, values: copy(values)});
          return range;
        },
        setNumberFormat(format) {
          operations.push({sheet: sheet.name, method: 'setNumberFormat', row, col, nr, nc, format});
          return range;
        },
        clear() {
          for (let i = 0; i < nr; i++) for (let j = 0; j < nc; j++) sheet.cells.delete((row + i) + ':' + (col + j));
          operations.push({sheet: sheet.name, method: 'clear', row, col, nr, nc});
          return range;
        },
      };
      range = new Proxy({}, {get(_target, method) { return methods[method] || (() => range); }});
      return range;
    }
  }
  const decision = new Sheet('Portfolio_Decision');
  decision.put(4, 1, 'CONTROL PANEL');
  decision.put(11, 1, 'prior-kpi');
  decision.put(13, 1, 'prior-board');
  const holdings = new Sheet('My_Portfolio', [HOLD_HEADERS, HOLD_ROW]);
  const sheets = {Portfolio_Decision: decision, My_Portfolio: holdings};
  if (options.ledger) {
    sheets._Portfolio_CostBasis = new Sheet('_Portfolio_CostBasis',
      [options.ledger.headers].concat(options.ledger.rows), 4);
  }
  const ss = {
    getSheetByName: name => sheets[name] || null,
    getRangeByName: () => null,
    insertSheet(name) { sheets[name] = new Sheet(name); return sheets[name]; },
  };
  const captured = new Date(NOW - 60000).toISOString();
  const capture = {schema: 'tfb.ibkr-capture/v1', account_alias: 'synthetic-account', captured_at: captured,
    positions: {positions: []}, balances: {balances: [{currency: 'USD', settled_cash: 100}]},
    orders: {orders: []}};
  const propertyService = store => ({
    getProperty: key => Object.prototype.hasOwnProperty.call(store, key) ? store[key] : null,
    setProperty(key, value) { store[key] = String(value); },
  });
  class FixedDate extends Date {
    constructor(...args) { super(...(args.length ? args : [NOW])); }
    static now() { return NOW; }
  }
  const ctx = {
    SHEET_NAMES: {MY_PORTFOLIO: 'My_Portfolio'}, Date: FixedDate,
    Logger: {log: message => logs.push(String(message))},
    Session: {getScriptTimeZone: () => 'Asia/Riyadh'},
    Utilities: {formatDate: date => new Date(date.getTime() + 3 * 3600000).toISOString().slice(0, 19)},
    PropertiesService: {getScriptProperties: () => propertyService(properties),
      getDocumentProperties: () => propertyService({})},
    SpreadsheetApp: {getActiveSpreadsheet: () => ss, getActive: () => ss, flush() {},
      BorderStyle: {SOLID: 'SOLID', SOLID_THICK: 'SOLID_THICK'}},
    UrlFetchApp: {fetch(url, options) {
      requests.push({url, body: JSON.parse(options.payload)});
      if (state.networkError) throw new Error('synthetic network failure');
      return {getContentText: () => JSON.stringify(state.response), getResponseCode: () => state.code};
    }},
    DriveApp: {searchFiles() {
      let next = true;
      return {hasNext: () => next, next() {
        next = false;
        return {getName: () => 'TFB_IBKR_Capture_synthetic.json', getId: () => CAPTURE_ID,
          getDateCreated: () => new Date(NOW - 50000),
          getBlob: () => ({getDataAsString: () => JSON.stringify(capture)})};
      }};
    }},
    LockService: {getScriptLock: () => ({tryLock() { state.locks++; return true; },
      releaseLock() { state.locks--; }})},
    ensureSheetExists_: name => ss.getSheetByName(name),
    readCurrentSheetHeaders_: sheet => sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0],
  };
  vm.createContext(ctx);
  vm.runInContext(SOURCE, ctx, {filename: '10_My_Portfolio.gs'});
  if (options.legacyCost) ctx.readCostBasisMap_ = () => copy(options.legacyCost);
  state.ctx = ctx;
  state.sheets = sheets;
  state.decision = decision;
  state.call = name => ctx[name]();
  return state;
}

module.exports = {world, VALID, CAPTURE_ID, HOLD_ROW, copy};
