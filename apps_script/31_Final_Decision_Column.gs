/**
 * 31_Final_Decision_Column.gs
 * Emad Bahbah Financial Advisor - the FINAL DECISION column on the engine pages
 * Version: 1.0.0 (2026-11-16)
 *   1.0.0: ONE DECISION ON EVERY PAGE (owner, Mon 16 Nov 2026 01:01 and 08:49 Riyadh: "in the end they should provide
 *          the same recommendations ... the final decision should be one decision"; "let's complete the remaining").
 *          The Today Action Plan writes the Final_Decisions tab (today_action_plan 1.6.0): one decision per stock -
 *          SELL / BUY / HOLD / WATCH / DO NOT INVEST - from the rules of record. The v2 pages show it since Decision
 *          Pages v2 1.4.0. This file adds the same decision to the three engine pages, as one column to the right of
 *          each page's own table (one empty column between):
 *            Top_10_Investments   the seats table (Rank / Symbol / Ticket SAR / Shares)
 *            Portfolio_Decision   the ACTIONS table (Action / Symbol)
 *            My_Portfolio         the holdings table (Symbol / Position Qty)
 *          The column's header reads "FINAL DECISION . Today Action Plan <time>"; each cell is a formula that looks up
 *          the symbol of ITS OWN ROW in Final_Decisions (INDIRECT on ROW()), so a sort, a refresh or a rebuilt table
 *          can never show one stock's decision on another stock's row, and the cells follow every new Final_Decisions
 *          without a rewrite. A stock not on Final_Decisions reads "n/a - not on Final_Decisions (no order)". A table
 *          older than FDC_STALE_H hours (Script Property; 6, 1-48) says so in the header. Colours: SELL red, BUY green,
 *          HOLD navy, WATCH amber, DO NOT INVEST dark red.
 *          The page's own Action / Ticket / Recommendation columns are unchanged and stay the engine's analysis; no
 *          value another script reads is touched (the column sits outside the ranges the readers use).
 *          The pages are rebuilt by their own scripts, which clear them; the column is put back by fdcTick, run every
 *          10 minutes by one time trigger. Setup once from the editor: fdcInstallTrigger (it also stamps the pages
 *          now). fdcStatus shows the last tick; fdcRemoveColumn + fdcRemoveTrigger roll it back.
 *          ADDITIVE: no other file is changed.
 * CONVENTIONS: ES5 only; no onOpen here.
 */
'use strict';
var FDC_VERSION = '1.0.0';   // lockstep with the header

var FDC = {
  SOURCE: 'Final_Decisions',
  HEAD_PREFIX: 'FINAL DECISION',
  HANDLER: 'fdcTick',
  EVERY_MIN: 10,
  MAX_ROWS: 400,
  MAX_COLS: 600,
  WIDTH_PX: 330,
  PROP_STALE: 'FDC_STALE_H',
  PROP_LAST: 'FDC_LAST_TICK',
  PAGES: [
    { name: 'Top_10_Investments', need: ['Rank', 'Symbol', 'Ticket SAR', 'Shares'], until: 'blank' },
    { name: 'Portfolio_Decision', need: ['Action', 'Symbol'], until: 'blank' },
    { name: 'My_Portfolio', need: ['Symbol', 'Position Qty'], until: 'last' }
  ],
  COLORS: [['SELL', '#fdecea', '#b3261e'], ['BUY', '#e6f4ea', '#2e7d32'], ['HOLD', '#eef1f5', '#1b2a41'],
           ['WATCH', '#fff3e0', '#b45309'], ['DO NOT INVEST', '#f3dede', '#7b1f1f'], ['n/a', '#f7f7f7', '#6b7785']]
};

/* ------------------------------------------------------------ pure */
function fdcTrim_(v) { return String(v === null || v === undefined ? '' : v).replace(/^\s+|\s+$/g, ''); }
function fdcColLetter_(n) { var s = ''; while (n > 0) { var m = (n - 1) % 26; s = String.fromCharCode(65 + m) + s; n = Math.floor((n - 1) / 26); } return s; }
/* PURE: a page's values -> null | { hdr (1-based row), symCol (1-based), rows: [1-based rows], col (1-based overlay
 * column), existing (true when a FINAL DECISION column is already there) }. The header row is the first row holding
 * every name in 'need'; data rows run until a blank symbol ('blank') or to the last row ('last'); a cell with a space
 * is a label, not a symbol. */
function fdcFindTable_(vals, need, until) {
  vals = vals || [];
  var hdr = -1, names = null, r, c;
  for (r = 0; r < vals.length && hdr < 0; r++) {
    var row = vals[r] || [], seen = {};
    for (c = 0; c < row.length; c++) seen[fdcTrim_(row[c])] = c;
    var all = true;
    for (var k = 0; k < need.length; k++) if (!seen.hasOwnProperty(need[k])) all = false;
    if (all) { hdr = r; names = seen; }
  }
  if (hdr < 0) return null;
  var hrow = vals[hdr], symCol = names.Symbol, existing = -1, last = -1;
  for (c = 0; c < hrow.length; c++) {
    var t = fdcTrim_(hrow[c]);
    if (t.indexOf(FDC.HEAD_PREFIX) === 0) { if (existing < 0) existing = c; }
    else if (t) last = c;
  }
  var rows = [];
  for (r = hdr + 1; r < vals.length; r++) {
    var sym = fdcTrim_((vals[r] || [])[symCol]);
    if (!sym || /\s/.test(sym)) { if (until === 'blank') break; continue; }
    rows.push(r + 1);
  }
  return { hdr: hdr + 1, symCol: symCol + 1, rows: rows, col: existing >= 0 ? existing + 1 : last + 3, existing: existing >= 0 };
}
/* PURE: the cell formula for the row it sits in (its own symbol, looked up in Final_Decisions). */
function fdcFormula_(symCol) {
  var key = 'TRIM(INDIRECT("R"&ROW()&"C' + symCol + '",FALSE))';
  var src = FDC.SOURCE + '!$A:$C';
  return '=IFERROR(VLOOKUP(' + key + ',' + src + ',2,FALSE)&" \u2014 "&VLOOKUP(' + key + ',' + src + ',3,FALSE),' +
         '"n/a \u2014 not on ' + FDC.SOURCE + ' (no order)")';
}
/* PURE: hours from a 'yyyy-MM-dd HH:mm' Riyadh (UTC+3) stamp to nowMs; null when not read. */
function fdcAgeH_(at, nowMs) {
  var m = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})/.exec(fdcTrim_(at));
  if (!m) return null;
  return Math.max(0, (nowMs - Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4] - 3, +m[5])) / 3600000);
}
/* PURE: the header text. */
function fdcHeader_(src, staleH) {
  if (!src || !src.ok) return FDC.HEAD_PREFIX + ' \u00b7 ' + FDC.SOURCE + ' not read' + (src && src.why ? ' (' + src.why + ')' : '') + ': take orders from Today_Action_Plan';
  var old = src.ageH !== null && src.ageH > staleH;
  return FDC.HEAD_PREFIX + ' \u00b7 Today Action Plan ' + src.at + (old ? ' \u00b7 OLD (' + Math.round(src.ageH * 10) / 10 + ' h): take orders from Today_Action_Plan' : '');
}
/* PURE: Final_Decisions values -> { ok, at, why } (the time is the first row's "Decided At (Riyadh)"). */
function fdcSourceFromValues_(vals) {
  vals = vals || [];
  var head = vals[0] || [], iS = -1, iD = -1, iT = -1;
  for (var c = 0; c < head.length; c++) {
    var h = fdcTrim_(head[c]);
    if (h === 'Symbol') iS = c; else if (h === 'Decision') iD = c; else if (h === 'Decided At (Riyadh)') iT = c;
  }
  if (iS !== 0 || iD !== 1 || iT < 0) return { ok: false, at: '', why: 'no decisions table in columns A:C' };
  for (var r = 1; r < vals.length; r++) { if (fdcTrim_(vals[r][0])) return { ok: true, at: fdcTrim_(vals[r][iT]), why: '' }; }
  return { ok: false, at: '', why: 'no decision rows' };
}
function fdcStaleH_() {
  var v = null;
  try { v = parseFloat(PropertiesService.getScriptProperties().getProperty(FDC.PROP_STALE)); } catch (e) { v = null; }
  return (v >= 1 && v <= 48) ? v : 6;
}

/* ------------------------------------------------------------ the workbook */
function fdcReadSource_(ss) {
  var sh = ss.getSheetByName(FDC.SOURCE);
  if (!sh || sh.getLastRow() < 2) return { ok: false, at: '', ageH: null, why: 'tab missing or empty' };
  var src = fdcSourceFromValues_(sh.getRange(1, 1, Math.min(sh.getLastRow(), 3), Math.min(Math.max(sh.getLastColumn(), 1), 13)).getValues());
  src.ageH = src.ok ? fdcAgeH_(src.at, new Date().getTime()) : null;
  return src;
}
/* Puts the column on one page; returns a short status. */
function fdcStampPage_(ss, cfg, src, staleH) {
  var sh = ss.getSheetByName(cfg.name);
  if (!sh) return cfg.name + ': tab missing';
  var lastRow = Math.min(sh.getLastRow(), FDC.MAX_ROWS), lastCol = Math.min(sh.getLastColumn(), FDC.MAX_COLS);
  if (lastRow < 2 || lastCol < 1) return cfg.name + ': empty';
  var t = fdcFindTable_(sh.getRange(1, 1, lastRow, lastCol).getValues(), cfg.need, cfg.until);
  if (!t) return cfg.name + ': table not found';
  if (t.col > sh.getMaxColumns()) sh.insertColumnsAfter(sh.getMaxColumns(), t.col - sh.getMaxColumns());
  if (t.existing && lastRow > t.hdr) sh.getRange(t.hdr + 1, t.col, lastRow - t.hdr, 1).clearContent();
  sh.getRange(t.hdr, t.col).setValue(fdcHeader_(src, staleH)).setFontWeight('bold').setFontColor('#ffffff')
    .setBackground(src.ok && !(src.ageH !== null && src.ageH > staleH) ? '#0f5c63' : '#b45309').setWrap(true).setVerticalAlignment('middle');
  sh.setColumnWidth(t.col, FDC.WIDTH_PX);
  if (!t.rows.length) return cfg.name + ': header only (no rows)';
  var first = t.rows[0], last = t.rows[t.rows.length - 1], f = fdcFormula_(t.symCol), grid = [], want = {};
  for (var i = 0; i < t.rows.length; i++) want[t.rows[i]] = true;
  for (var r = first; r <= last; r++) grid.push([want[r] ? f : '']);
  var block = sh.getRange(first, t.col, grid.length, 1);
  block.setFormulas(grid);
  block.setFontWeight('bold').setFontSize(9).setWrap(true).setVerticalAlignment('middle');
  // colours: replace this column's own rules, keep every other rule of the page
  var keep = [], rules = sh.getConditionalFormatRules();
  for (var j = 0; j < rules.length; j++) {
    var rs = rules[j].getRanges(), mine = false;
    for (var q = 0; q < rs.length; q++) if (rs[q].getColumn() <= t.col && rs[q].getColumn() + rs[q].getNumColumns() - 1 >= t.col) mine = true;
    if (!mine) keep.push(rules[j]);
  }
  for (var k = 0; k < FDC.COLORS.length; k++) {
    keep.push(SpreadsheetApp.newConditionalFormatRule().whenTextStartsWith(FDC.COLORS[k][0])
      .setBackground(FDC.COLORS[k][1]).setFontColor(FDC.COLORS[k][2]).setRanges([block]).build());
  }
  sh.setConditionalFormatRules(keep);
  return cfg.name + ': ' + t.rows.length + ' row(s) in column ' + fdcColLetter_(t.col);
}
/* The time trigger's handler (and the manual refresh). */
function fdcTick() {
  var lock = null;
  try { lock = LockService.getScriptLock(); if (!lock.tryLock(5000)) return 'busy: another script holds the lock'; } catch (e0) { lock = null; }
  try {
    var ss = SpreadsheetApp.getActive(), src = fdcReadSource_(ss), staleH = fdcStaleH_(), out = [];
    for (var i = 0; i < FDC.PAGES.length; i++) {
      try { out.push(fdcStampPage_(ss, FDC.PAGES[i], src, staleH)); }
      catch (e) { out.push(FDC.PAGES[i].name + ': ' + String(e && e.message || e).slice(0, 120)); }
    }
    var line = new Date().toISOString().slice(0, 16) + 'Z \u00b7 ' + (src.ok ? FDC.SOURCE + ' ' + src.at : FDC.SOURCE + ' not read') + ' \u00b7 ' + out.join(' \u00b7 ');
    try { PropertiesService.getScriptProperties().setProperty(FDC.PROP_LAST, line); } catch (e1) {}
    return line;
  } finally { if (lock) { try { lock.releaseLock(); } catch (e2) {} } }
}
function fdcRemoveTrigger_() {
  var n = 0, ts = ScriptApp.getProjectTriggers();
  for (var i = 0; i < ts.length; i++) if (ts[i].getHandlerFunction() === FDC.HANDLER) { ScriptApp.deleteTrigger(ts[i]); n++; }
  return n;
}
function fdcInstallTrigger() {
  fdcRemoveTrigger_();
  ScriptApp.newTrigger(FDC.HANDLER).timeBased().everyMinutes(FDC.EVERY_MIN).create();
  var line = fdcTick();
  fdcNotify_('FINAL DECISION column ' + FDC_VERSION + ': trigger installed (every ' + FDC.EVERY_MIN + ' min). ' + line);
  return line;
}
function fdcRemoveTrigger() { var n = fdcRemoveTrigger_(); fdcNotify_('FINAL DECISION column: ' + n + ' trigger(s) removed.'); return n; }
/* Rollback: takes the column off the three pages (the trigger would put it back - remove the trigger first). */
function fdcRemoveColumn() {
  var ss = SpreadsheetApp.getActive(), out = [];
  for (var i = 0; i < FDC.PAGES.length; i++) {
    var cfg = FDC.PAGES[i], sh = ss.getSheetByName(cfg.name);
    if (!sh || sh.getLastRow() < 1) { out.push(cfg.name + ': nothing'); continue; }
    var lastRow = Math.min(sh.getLastRow(), FDC.MAX_ROWS), lastCol = Math.min(sh.getLastColumn(), FDC.MAX_COLS);
    var t = fdcFindTable_(sh.getRange(1, 1, lastRow, lastCol).getValues(), cfg.need, cfg.until);
    if (!t || !t.existing) { out.push(cfg.name + ': no column'); continue; }
    sh.getRange(t.hdr, t.col, lastRow - t.hdr + 1, 1).clear();
    out.push(cfg.name + ': removed from column ' + fdcColLetter_(t.col));
  }
  fdcNotify_(out.join(' \u00b7 '));
  return out;
}
function fdcStatus() {
  var last = '';
  try { last = PropertiesService.getScriptProperties().getProperty(FDC.PROP_LAST) || 'never run'; } catch (e) { last = 'n/a'; }
  var n = 0, ts = ScriptApp.getProjectTriggers();
  for (var i = 0; i < ts.length; i++) if (ts[i].getHandlerFunction() === FDC.HANDLER) n++;
  var msg = 'FINAL DECISION column ' + FDC_VERSION + ' \u00b7 trigger ' + (n ? 'installed' : 'NOT installed (run fdcInstallTrigger)') + ' \u00b7 self-test ' + (fdcCoreOk_() ? 'ok' : 'FAIL') + ' \u00b7 last tick: ' + last;
  fdcNotify_(msg);
  return msg;
}
function fdcNotify_(msg) {
  try { Logger.log(msg); } catch (e) {}
  try { SpreadsheetApp.getActive().toast(String(msg).slice(0, 300), 'FINAL DECISION column', 8); } catch (e2) {}
}
/* The self-test core (pure). */
function fdcCoreOk_() {
  try {
    var v = [['TOP 10'], ['Status', 'x'], [''], ['Rank', 'Symbol', 'Name', 'Ticket SAR', 'Shares'], [1, 'AX.US', 'Axos', 0, 0], [2, 'MBIN.US', 'Merchants', 0, 0], ['', '', ''], ['ALERTS']];
    var t = fdcFindTable_(v, FDC.PAGES[0].need, 'blank');
    var t2 = fdcFindTable_([['Symbol', 'Position Qty', 'x'], ['A.US', 1, ''], ['', '', ''], ['B.US', 2, ''], ['TOTAL ROW', '', '']], FDC.PAGES[2].need, 'last');
    var src = fdcSourceFromValues_([['Symbol', 'Decision', 'Do today', 'Qty', 'Limit', 'Stop', 'Cost SAR', 'Why', 'Rule of record', 'Other', 'Diff', 'Decided At (Riyadh)'], ['AX.US', 'BUY', 'Buy 8', '', '', '', '', '', '', '', '', '2026-11-16 08:41']]);
    return !!t && t.hdr === 4 && t.symCol === 2 && t.col === 7 && t.rows.join(',') === '5,6' && !t.existing &&
      t2.rows.join(',') === '2,4' && t2.col === 5 && src.ok && src.at === '2026-11-16 08:41' &&
      fdcFormula_(2).indexOf('INDIRECT("R"&ROW()&"C2",FALSE)') > 0 && fdcAgeH_('2026-11-16 08:41', Date.UTC(2026, 10, 16, 6, 41)) === 1 &&
      fdcHeader_({ ok: true, at: '2026-11-16 08:41', ageH: 7 }, 6).indexOf('OLD (7 h)') > 0;
  } catch (e) { return false; }
}
