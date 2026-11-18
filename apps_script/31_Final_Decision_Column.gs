/**
 * 31_Final_Decision_Column.gs
 * Emad Bahbah Financial Advisor - the FINAL DECISION column on the engine pages
 * Version: 1.3.0 (2026-11-18)
 *   1.3.0: THE HOLDING READ ON THE PORTFOLIO PAGE (owner, Wed 18 Nov 2026 09:36 Riyadh: "add two new [fields] to the
 *          analysis of my portfolio: the main news that affected the last day's performance of each stock, in the row
 *          of analysis of each stock, and the expectation for the next day's performance"; 16:04: "go to the
 *          next step"). The Holding_Read page (scripts/holding_read.py 1.0.0) has carried both since that morning, on
 *          a page of its own. This version puts them in the row of each holding on Portfolio_Decision, as two columns
 *          directly right of FINAL DECISION:
 *            LAST SESSION . what moved it   the holding's move, the driver (COMPANY NEWS / SECTOR / MARKET / NORMAL
 *                                           RANGE / NO NEWS FOUND) and the main news of that session
 *            NEXT SESSION READ              the read (REBOUND MORE LIKELY, NO EDGE, ...) and how often the next
 *                                           session closed up after this stock's own similar days
 *          Each cell is a formula that looks up the symbol of ITS OWN ROW on Holding_Read (INDIRECT on ROW(), as the
 *          FINAL DECISION cell does), so a sort or a rebuilt table cannot show one stock's read on another's row, and
 *          the cells follow every new Holding_Read without a rewrite. The Holding_Read columns are found by their
 *          header names on every tick, so a column added to that page cannot shift what is shown. The header carries
 *          the session the read is for and says OLD when that session is more than FDC.READ.OLD_DAYS (5) days back;
 *          a holding not on the page reads "n/a - not on Holding_Read". Both columns are analysis: no order follows
 *          from them, and the header says so. Top_10_Investments gets neither (its seats are candidates, not
 *          holdings; Holding_Read covers holdings). The same 10-minute tick (fdcTick) writes them: NO new trigger and
 *          no setup - paste the file and save. Columns AC and AD on today's page, outside A1:AA150, the range the
 *          readers of Portfolio_Decision use. fdcRemoveColumn takes them off with the FINAL DECISION column.
 *   1.2.0: A SKIPPED RENDER IS RE-RUN TOO (16_Decision_Top10 1.30.0 [PAINT-BUDGET], same day). A render that reaches the
 *          paint later than its budget (300 s; Google stops an execution at 360 s) no longer clears the board: it puts
 *          the previous status line back with ' | SKIPPED render <time> (...)' at its end. fdcStuck_ reads that tail as
 *          a stopped run keyed 'skipped <time>' (the line no longer says 'running', so the 1.1.0 rule alone would never
 *          re-run it) and fdcRerunDue_ re-runs it on the next tick, without the 20-minute wait; once per skipped run,
 *          07:00-23:00 Riyadh, FDC_RERUN = off as before. New: at most FDC.RERUN_MAX_DAY (4) re-runs a Riyadh day, in
 *          Script Property FDC_RERUN_DAY ('YYYY-MM-DD|n'), counted when the trigger is scheduled, so a busy day cannot
 *          chain re-runs. The toast and the tick line name the skipped run; fdcStatus shows the day's count.
 *   1.1.0: TOP_10_INVESTMENTS REVIEW (owner, Mon 16 Nov 2026 11:23 Riyadh: "Top_10_Investments, what about this page,
 *          review and improve or is it ok?"). Read at 11:24: the page works (render 10:16, status ok) but (1) its render of
 *          08:44 - the one after the 08:41 IBKR capture, stopped by Google on 9, 10 and 16 Nov - left the board empty with
 *          'status: running...' for 90 minutes until the owner re-ran it by hand; (2) the FINAL DECISION column sits in
 *          column AF, 30 columns to the right, so the day's decision is not on screen. Two additions, both on the same
 *          10-minute tick:
 *          (a) [TODAY-LINE] cell A3 of Top_10_Investments (an empty spacer row inside the frozen header, so always on
 *              screen) reads the day's decisions: "TODAY (Today Action Plan HH:MM): BUY ... . SELL ... . The FINAL
 *              DECISION of every seat is in column AF; orders come from Today_Action_Plan." Written only while A3 is
 *              empty or already holds this line; no reader uses row 3 (the panel is read from row 4 on, the banner from
 *              the KPI row on).
 *          (b) [RE-RUN] a render whose status has read 'running' for FDC_STUCK_MIN minutes (20) was stopped by Google
 *              (a finished render takes 2-4 minutes): the tick schedules refreshDecisionTop10 once for that stopped run,
 *              in its own execution one minute later (fdcRerunTop10, a one-off trigger that removes itself), so the
 *              render has the whole 6 minutes; between 07:00 and 23:00 Riyadh only. Script Property FDC_RERUN = off turns
 *              it off; FDC_RERUN_FOR records the stopped run it was done for (never twice for the same run).
 *   1.0.0: ONE DECISION ON EVERY PAGE (owner, Mon 16 Nov 2026 01:01 and 08:49 Riyadh: "in the end they should provide
 *          the same recommendations ... the final decision should be one decision"; "let's complete the remaining").
 *          The Today Action Plan writes the Final_Decisions tab (today_action_plan 1.6.0): one decision per stock -
 *          SELL / BUY / HOLD / WATCH / DO NOT INVEST - from the rules of record. The v2 pages show it since Decision
 *          Pages v2 1.4.0. This file adds the same decision to the three engine pages, as one column to the right of
 *          each page's own table (one empty column between):
 *            Top_10_Investments   the seats table (Rank / Symbol / Ticket SAR / Shares)
 *            Portfolio_Decision   the ACTIONS table (Action / Symbol)
 *          My_Portfolio is a schema-checked data page: its refresh writer and validator count every column (Codex
 *          P2 x2, #1001), so it gets no column - its engine decision headers (Recommendation, Final Action, Investor
 *          Decision) carry a cell note instead, pointing to the final decision. A note changes no value, header or
 *          width.
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
var FDC_VERSION = '1.3.0';   // lockstep with the header

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
  PROP_RERUN: 'FDC_RERUN',
  PROP_RERUN_FOR: 'FDC_RERUN_FOR',
  PROP_RERUN_DAY: 'FDC_RERUN_DAY',   // 1.2.0: 'YYYY-MM-DD|n' re-runs scheduled that Riyadh day
  RERUN_MAX_DAY: 4,
  STUCK_MIN: 20,
  RERUN_HANDLER: 'fdcRerunTop10',
  RERUN_FROM_H: 7,
  RERUN_TO_H: 23,
  T10: 'Top_10_Investments',
  PAGES: [
    { name: 'Top_10_Investments', need: ['Rank', 'Symbol', 'Ticket SAR', 'Shares'], until: 'blank' },
    { name: 'Portfolio_Decision', need: ['Action', 'Symbol'], until: 'blank', read: true },   // 1.3.0
    { name: 'My_Portfolio', need: ['Symbol', 'Position Qty'], until: 'last', mode: 'note',
      notes: ['Recommendation', 'Final Action', 'Investor Decision'] }
  ],
  NOTE: 'Engine field (analysis), not the decision. The one decision per stock is on Today_Action_Plan and in the ' +
        'Final_Decisions tab (SELL / BUY / HOLD / WATCH / DO NOT INVEST); Portfolio_Decision and Top_10_Investments ' +
        'show it in their FINAL DECISION column.',
  READ: {   // 1.3.0: the two Holding_Read columns on a page with read: true
    SOURCE: 'Holding_Read',
    NEED: ['Symbol', 'Last session', 'Stock %', 'Driver', 'Main news (last session)', 'Next session read', 'Up after similar days'],
    MOVED_PREFIX: 'LAST SESSION \u00b7 what moved it',
    NEXT_PREFIX: 'NEXT SESSION READ',
    WIDTH_MOVED: 430,
    WIDTH_NEXT: 300,
    OLD_DAYS: 5,
    MAX_ROWS: 120,
    MAX_COLS: 40,
    COLORS: [['REBOUND MORE LIKELY', '#e6f4ea', '#2e7d32'], ['RISE MORE LIKELY', '#e6f4ea', '#2e7d32'], ['UP MORE LIKELY', '#e6f4ea', '#2e7d32'],
             ['FURTHER FALL MORE LIKELY', '#fdecea', '#b3261e'], ['PULLBACK MORE LIKELY', '#fdecea', '#b3261e'], ['DOWN MORE LIKELY', '#fdecea', '#b3261e'],
             ['PINNED BY AN OFFER', '#fff3e0', '#b45309'], ['EARNINGS DUE', '#fff3e0', '#b45309'],
             ['NO EDGE', '#f7f7f7', '#6b7785'], ['NOT ENOUGH HISTORY', '#f7f7f7', '#6b7785'], ['n/a', '#f7f7f7', '#6b7785']]
  },
  COLORS: [['SELL', '#fdecea', '#b3261e'], ['BUY', '#e6f4ea', '#2e7d32'], ['HOLD', '#eef1f5', '#1b2a41'],
           ['WATCH', '#fff3e0', '#b45309'], ['DO NOT INVEST', '#f3dede', '#7b1f1f'], ['n/a', '#f7f7f7', '#6b7785']]
};

/* ------------------------------------------------------------ pure */
function fdcTrim_(v) { return String(v === null || v === undefined ? '' : v).replace(/^\s+|\s+$/g, ''); }
function fdcColLetter_(n) { var s = ''; while (n > 0) { var m = (n - 1) % 26; s = String.fromCharCode(65 + m) + s; n = Math.floor((n - 1) / 26); } return s; }
/* PURE: a page's values -> null | { hdr (1-based row), symCol (1-based), rows: [1-based rows], col (1-based overlay
 * column), existing (true when a FINAL DECISION column is already there), extras: [1-based columns already holding a
 * Holding_Read header (1.3.0)] }. The header row is the first row holding every name in 'need'; data rows run until a
 * blank symbol ('blank') or to the last row ('last'); a cell with a space is a label, not a symbol. The Holding_Read
 * headers are this file's own, so they never count as the page's last column. */
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
  var hrow = vals[hdr], symCol = names.Symbol, existing = -1, last = -1, extras = [];
  for (c = 0; c < hrow.length; c++) {
    var t = fdcTrim_(hrow[c]);
    if (t.indexOf(FDC.HEAD_PREFIX) === 0) { if (existing < 0) existing = c; }
    else if (t.indexOf(FDC.READ.MOVED_PREFIX) === 0 || t.indexOf(FDC.READ.NEXT_PREFIX) === 0) extras.push(c + 1);   // 1.3.0
    else if (t) last = c;
  }
  var rows = [];
  for (r = hdr + 1; r < vals.length; r++) {
    var sym = fdcTrim_((vals[r] || [])[symCol]);
    if (!sym || /\s/.test(sym)) { if (until === 'blank') break; continue; }
    rows.push(r + 1);
  }
  return { hdr: hdr + 1, symCol: symCol + 1, rows: rows, col: existing >= 0 ? existing + 1 : last + 3, existing: existing >= 0, extras: extras };
}
/* PURE (1.3.0): Holding_Read values -> { ok, why, session ('yyyy-MM-dd' of the first holding row, '' when not read),
 * cols: { sym, stock, driver, news, next, up } (1-based) }. The header row is the first row holding every name in
 * FDC.READ.NEED, wherever the page puts it. */
function fdcReadFromValues_(vals) {
  vals = vals || [];
  var need = FDC.READ.NEED, hdr = -1, seen = null, r, c;
  for (r = 0; r < vals.length && hdr < 0; r++) {
    var row = vals[r] || [], names = {};
    for (c = 0; c < row.length; c++) { var h = fdcTrim_(row[c]); if (h && !names.hasOwnProperty(h)) names[h] = c + 1; }
    var all = true;
    for (var k = 0; k < need.length; k++) if (!names.hasOwnProperty(need[k])) all = false;
    if (all) { hdr = r; seen = names; }
  }
  if (hdr < 0) return { ok: false, why: 'no read table on the page', session: '', cols: null };
  var session = '';
  for (r = hdr + 1; r < vals.length && !session; r++) {
    if (!fdcTrim_((vals[r] || [])[seen.Symbol - 1])) continue;
    var v = vals[r][seen['Last session'] - 1];
    if (Object.prototype.toString.call(v) === '[object Date]' && !isNaN(v.getTime())) session = new Date(v.getTime() + 12 * 3600000).toISOString().slice(0, 10);
    else { var m = /^(\d{4}-\d{2}-\d{2})/.exec(fdcTrim_(v)); session = m ? m[1] : ''; }
    if (!session) break;
  }
  if (!session) return { ok: false, why: 'no holding rows', session: '', cols: null };
  return { ok: true, why: '', session: session,
           cols: { sym: seen.Symbol, stock: seen['Stock %'], driver: seen.Driver, news: seen['Main news (last session)'],
                   next: seen['Next session read'], up: seen['Up after similar days'] } };
}
/* PURE (1.3.0): whole days from a 'yyyy-MM-dd' session to nowMs (Riyadh calendar days); null when not read. */
function fdcSessionAgeD_(session, nowMs) {
  var m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(fdcTrim_(session));
  if (!m) return null;
  var today = new Date(nowMs + 3 * 3600000);
  return Math.round((Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()) - Date.UTC(+m[1], +m[2] - 1, +m[3])) / 86400000);
}
/* PURE (1.3.0): the two header texts [what moved it, next session read]. */
function fdcReadHeaders_(rs, nowMs) {
  var R = FDC.READ;
  if (!rs || !rs.ok) {
    var why = R.SOURCE + ' not read' + (rs && rs.why ? ' (' + rs.why + ')' : '');
    return [R.MOVED_PREFIX + ' \u00b7 ' + why, R.NEXT_PREFIX + ' \u00b7 ' + why];
  }
  var age = fdcSessionAgeD_(rs.session, nowMs), old = age !== null && age > R.OLD_DAYS ? ' \u00b7 OLD (' + age + ' days)' : '';
  return [R.MOVED_PREFIX + ' \u00b7 Holding Read, session ' + rs.session + old + ' \u00b7 analysis, not an order',
          R.NEXT_PREFIX + ' \u00b7 after the session of ' + rs.session + old + ' \u00b7 a count from its own history, not a forecast of size'];
}
/* PURE (1.3.0): the two cell formulas for the row they sit in (its own symbol, looked up on Holding_Read). */
function fdcReadFormulas_(symCol, cols) {
  var S = FDC.READ.SOURCE, key = 'TRIM(INDIRECT("R"&ROW()&"C' + symCol + '",FALSE))';
  var col = function (n) { var L = fdcColLetter_(n); return S + '!$' + L + ':$' + L; };
  var at = function (n) { return 'INDEX(' + col(n) + ',MATCH(' + key + ',' + col(cols.sym) + ',0))'; };
  var miss = '"n/a \u2014 not on ' + S + '"';
  return ['=IFERROR(' + at(cols.stock) + '&" \u00b7 "&' + at(cols.driver) + '&" \u2014 "&' + at(cols.news) + ',' + miss + ')',
          '=IFERROR(' + at(cols.next) + '&IF(' + at(cols.up) + '="",""," \u2014 closed up after similar days: "&' + at(cols.up) + '),' + miss + ')'];
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

/* PURE (1.1.0 [TODAY-LINE]): Final_Decisions values -> the line for Top_10_Investments!A3, or '' when not read. */
function fdcTodayLine_(vals, src, staleH) {
  if (!src || !src.ok) return '';
  var buy = [], sell = [];
  for (var r = 1; r < (vals || []).length; r++) {
    var sym = fdcTrim_(vals[r][0]), dec = fdcTrim_(vals[r][1]).toUpperCase();
    if (!sym) continue;
    if (dec === 'BUY') buy.push(sym); else if (dec === 'SELL') sell.push(sym);
  }
  var old = src.ageH !== null && src.ageH > staleH;
  return 'TODAY (Today Action Plan ' + src.at + (old ? ', OLD' : '') + '): BUY ' + (buy.length ? buy.join(', ') : 'none') +
    ' \u00b7 SELL ' + (sell.length ? sell.join(', ') : 'none') + '. The FINAL DECISION of every seat is in the FINAL DECISION ' +
    'column at the right; orders come from Today_Action_Plan - this page is the engine\'s research.';
}
/* PURE (1.1.0 [RE-RUN]): the Top 10 status cell -> { run: 'yyyy-MM-dd HH:mm:ss', minutes } when it reads running, else null. */
function fdcStuck_(status, nowMs) {
  var t = fdcTrim_(status);
  var sk = /\| SKIPPED render (\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})/.exec(t);   // 1.2.0: 16 v1.30.0 [PAINT-BUDGET]
  if (sk) {
    var skMs = Date.UTC(+sk[1], +sk[2] - 1, +sk[3], +sk[4] - 3, +sk[5], +sk[6]);
    return { run: 'skipped ' + sk[1] + '-' + sk[2] + '-' + sk[3] + ' ' + sk[4] + ':' + sk[5] + ':' + sk[6],
             minutes: Math.max(0, (nowMs - skMs) / 60000), skipped: true };
  }
  if (!/status:\s*running/i.test(t)) return null;
  var m = /Last run (\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})/.exec(t);
  if (!m) return null;
  var ms = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4] - 3, +m[5], +m[6]);
  return { run: m[1] + '-' + m[2] + '-' + m[3] + ' ' + m[4] + ':' + m[5] + ':' + m[6], minutes: Math.max(0, (nowMs - ms) / 60000) };
}
/* PURE (1.1.0 [RE-RUN]): whether to re-run now. */
function fdcRerunDue_(stuck, rerunFor, riyadhHour, on) {
  return !!(on && stuck && (stuck.skipped === true || stuck.minutes >= FDC.STUCK_MIN) && rerunFor !== stuck.run &&
            riyadhHour >= FDC.RERUN_FROM_H && riyadhHour < FDC.RERUN_TO_H);
}
/* PURE (1.2.0): re-runs already scheduled on Riyadh day 'day' from FDC_RERUN_DAY ('YYYY-MM-DD|n'; another day = 0). */
function fdcRerunCount_(raw, day) {
  var m = /^(\d{4}-\d{2}-\d{2})\|(\d{1,3})$/.exec(fdcTrim_(raw));
  return m && m[1] === day ? +m[2] : 0;
}
/* PURE (1.2.0): 'stopped 2026-11-16 08:30:00' / 'skipped 2026-11-16 08:49:57' for the tick line and the toast. */
function fdcRunLabel_(run) { return /^skipped /.test(String(run)) ? String(run) : 'stopped ' + run; }
/* The Riyadh calendar day of an instant ('YYYY-MM-DD'). */
function fdcRiyadhDay_(ms) { return new Date(ms + 3 * 3600000).toISOString().slice(0, 10); }

/* ------------------------------------------------------------ the workbook */
function fdcReadSource_(ss) {
  var sh = ss.getSheetByName(FDC.SOURCE);
  if (!sh || sh.getLastRow() < 2) return { ok: false, at: '', ageH: null, why: 'tab missing or empty' };
  var src = fdcSourceFromValues_(sh.getRange(1, 1, Math.min(sh.getLastRow(), 3), Math.min(Math.max(sh.getLastColumn(), 1), 13)).getValues());
  src.ageH = src.ok ? fdcAgeH_(src.at, new Date().getTime()) : null;
  return src;
}
/* 1.3.0: Holding_Read as the tick sees it. */
function fdcReadRead_(ss) {
  var sh = ss.getSheetByName(FDC.READ.SOURCE);
  if (!sh || sh.getLastRow() < 2) return { ok: false, why: 'tab missing or empty', session: '', cols: null };
  return fdcReadFromValues_(sh.getRange(1, 1, Math.min(sh.getLastRow(), FDC.READ.MAX_ROWS), Math.min(Math.max(sh.getLastColumn(), 1), FDC.READ.MAX_COLS)).getValues());
}
/* 1.3.0: the two Holding_Read columns, directly right of the FINAL DECISION column of a page with read: true. Returns
 * { text, block (the NEXT SESSION READ cells, for the colours; null without rows), cols: [c1, c2] }. */
function fdcStampRead_(sh, t, rs, lastRow) {
  var R = FDC.READ, c1 = t.col + 1, c2 = t.col + 2, heads = fdcReadHeaders_(rs, new Date().getTime()), e;
  if (c2 > sh.getMaxColumns()) sh.insertColumnsAfter(sh.getMaxColumns(), c2 - sh.getMaxColumns());
  for (e = 0; e < t.extras.length; e++) {   // a header of ours left somewhere else by an older layout
    if (t.extras[e] !== c1 && t.extras[e] !== c2) sh.getRange(t.hdr, t.extras[e], Math.max(1, lastRow - t.hdr + 1), 1).clear();
  }
  if (lastRow > t.hdr) sh.getRange(t.hdr + 1, c1, lastRow - t.hdr, 2).clearContent();
  var bg = rs.ok && heads[0].indexOf('OLD (') < 0 ? '#1b2a41' : '#b45309';
  sh.getRange(t.hdr, c1).setValue(heads[0]).setFontWeight('bold').setFontColor('#ffffff').setBackground(bg).setWrap(true).setVerticalAlignment('middle');
  sh.getRange(t.hdr, c2).setValue(heads[1]).setFontWeight('bold').setFontColor('#ffffff').setBackground(bg).setWrap(true).setVerticalAlignment('middle');
  sh.setColumnWidth(c1, R.WIDTH_MOVED); sh.setColumnWidth(c2, R.WIDTH_NEXT);
  if (!rs.ok) return { text: 'read: headers only (' + rs.why + ')', block: null, cols: [c1, c2] };
  if (!t.rows.length) return { text: 'read: headers only (no rows)', block: null, cols: [c1, c2] };
  var first = t.rows[0], last = t.rows[t.rows.length - 1], f = fdcReadFormulas_(t.symCol, rs.cols), grid = [], want = {};
  for (var i = 0; i < t.rows.length; i++) want[t.rows[i]] = true;
  for (var r = first; r <= last; r++) grid.push(want[r] ? [f[0], f[1]] : ['', '']);
  var both = sh.getRange(first, c1, grid.length, 2);
  both.setFormulas(grid);
  both.setFontSize(9).setWrap(true).setVerticalAlignment('middle');
  return { text: 'read: session ' + rs.session + ' in columns ' + fdcColLetter_(c1) + '-' + fdcColLetter_(c2),
           block: sh.getRange(first, c2, grid.length, 1), cols: [c1, c2] };
}
/* Puts the column on one page; returns a short status. */
/* A data page that must keep its width: a note on its engine decision headers, nothing else. */
function fdcNotePage_(sh, cfg) {
  var lastCol = Math.min(sh.getLastColumn(), FDC.MAX_COLS);
  if (lastCol < 1) return cfg.name + ': empty';
  var head = sh.getRange(1, 1, 1, lastCol).getValues()[0], n = 0;
  for (var c = 0; c < head.length; c++) {
    if (cfg.notes.indexOf(fdcTrim_(head[c])) >= 0) { sh.getRange(1, c + 1).setNote(FDC.NOTE); n++; }
  }
  return cfg.name + ': ' + n + ' header note(s), no column';
}
function fdcStampPage_(ss, cfg, src, staleH, rs) {
  var sh = ss.getSheetByName(cfg.name);
  if (!sh) return cfg.name + ': tab missing';
  if (cfg.mode === 'note') return fdcNotePage_(sh, cfg);
  var lastRow = Math.min(sh.getLastRow(), FDC.MAX_ROWS), lastCol = Math.min(sh.getLastColumn(), FDC.MAX_COLS);
  if (lastRow < 2 || lastCol < 1) return cfg.name + ': empty';
  var t = fdcFindTable_(sh.getRange(1, 1, lastRow, lastCol).getValues(), cfg.need, cfg.until);
  if (!t) return cfg.name + ': table not found';
  if (t.col > sh.getMaxColumns()) sh.insertColumnsAfter(sh.getMaxColumns(), t.col - sh.getMaxColumns());
  if (t.existing && lastRow > t.hdr) sh.getRange(t.hdr + 1, t.col, lastRow - t.hdr, 1).clearContent();
  sh.getRange(t.hdr, t.col).setValue(fdcHeader_(src, staleH)).setFontWeight('bold').setFontColor('#ffffff')
    .setBackground(src.ok && !(src.ageH !== null && src.ageH > staleH) ? '#0f5c63' : '#b45309').setWrap(true).setVerticalAlignment('middle');
  sh.setColumnWidth(t.col, FDC.WIDTH_PX);
  var rd = cfg.read && rs ? fdcStampRead_(sh, t, rs, lastRow) : null;   // 1.3.0
  if (!t.rows.length) return cfg.name + ': header only (no rows)' + (rd ? ' \u00b7 ' + rd.text : '');
  var first = t.rows[0], last = t.rows[t.rows.length - 1], f = fdcFormula_(t.symCol), grid = [], want = {};
  for (var i = 0; i < t.rows.length; i++) want[t.rows[i]] = true;
  for (var r = first; r <= last; r++) grid.push([want[r] ? f : '']);
  var block = sh.getRange(first, t.col, grid.length, 1);
  block.setFormulas(grid);
  block.setFontWeight('bold').setFontSize(9).setWrap(true).setVerticalAlignment('middle');
  // colours: replace the rules of this file's own columns, keep every other rule of the page
  var keep = [], rules = sh.getConditionalFormatRules(), own = [t.col].concat(rd ? rd.cols : []);
  for (var j = 0; j < rules.length; j++) {
    var rg = rules[j].getRanges(), mine = false;
    for (var q = 0; q < rg.length; q++) for (var o = 0; o < own.length; o++) {
      if (rg[q].getColumn() <= own[o] && rg[q].getColumn() + rg[q].getNumColumns() - 1 >= own[o]) mine = true;
    }
    if (!mine) keep.push(rules[j]);
  }
  for (var k = 0; k < FDC.COLORS.length; k++) {
    keep.push(SpreadsheetApp.newConditionalFormatRule().whenTextStartsWith(FDC.COLORS[k][0])
      .setBackground(FDC.COLORS[k][1]).setFontColor(FDC.COLORS[k][2]).setRanges([block]).build());
  }
  if (rd && rd.block) {   // 1.3.0: the NEXT SESSION READ cells
    for (var n = 0; n < FDC.READ.COLORS.length; n++) {
      keep.push(SpreadsheetApp.newConditionalFormatRule().whenTextStartsWith(FDC.READ.COLORS[n][0])
        .setBackground(FDC.READ.COLORS[n][1]).setFontColor(FDC.READ.COLORS[n][2]).setRanges([rd.block]).build());
    }
  }
  sh.setConditionalFormatRules(keep);
  return cfg.name + ': ' + t.rows.length + ' row(s) in column ' + fdcColLetter_(t.col) + (rd ? ' \u00b7 ' + rd.text : '');
}
/* The time trigger's handler (and the manual refresh). */
function fdcTick() {
  var lock = null, rerun = '', line = '';
  try { lock = LockService.getScriptLock(); if (!lock.tryLock(5000)) return 'busy: another script holds the lock'; } catch (e0) { lock = null; }
  try {
    var ss = SpreadsheetApp.getActive(), src = fdcReadSource_(ss), staleH = fdcStaleH_(), out = [], rs = null;
    try { rs = fdcReadRead_(ss); } catch (eH) { rs = { ok: false, why: String(eH && eH.message || eH).slice(0, 80), session: '', cols: null }; }   // 1.3.0
    for (var i = 0; i < FDC.PAGES.length; i++) {
      try { out.push(fdcStampPage_(ss, FDC.PAGES[i], src, staleH, rs)); }
      catch (e) { out.push(FDC.PAGES[i].name + ': ' + String(e && e.message || e).slice(0, 120)); }
    }
    try { out.push(fdcStampToday_(ss, src, staleH)); } catch (eT) { out.push('A3: ' + String(eT && eT.message || eT).slice(0, 80)); }   // 1.1.0
    rerun = fdcRerunCheck_(ss);   // 1.1.0: decided under the lock, run after it is released
    if (rerun) out.push('re-run of the ' + fdcRunLabel_(rerun) + ' render started');
    line = new Date().toISOString().slice(0, 16) + 'Z \u00b7 ' + (src.ok ? FDC.SOURCE + ' ' + src.at : FDC.SOURCE + ' not read') + ' \u00b7 ' + out.join(' \u00b7 ');
    try { PropertiesService.getScriptProperties().setProperty(FDC.PROP_LAST, line); } catch (e1) {}
  } finally { if (lock) { try { lock.releaseLock(); } catch (e2) {} } }
  if (rerun) {   // 1.1.0 [RE-RUN]: its own execution one minute on, so the render has the whole 6 minutes
    try {
      fdcRemoveRerunTriggers_(); ScriptApp.newTrigger(FDC.RERUN_HANDLER).timeBased().after(60 * 1000).create();
      try {   // 1.2.0: counted once the trigger exists
        var dp = PropertiesService.getScriptProperties(), day = fdcRiyadhDay_(new Date().getTime());
        dp.setProperty(FDC.PROP_RERUN_DAY, day + '|' + (fdcRerunCount_(dp.getProperty(FDC.PROP_RERUN_DAY), day) + 1));
      } catch (eD) {}
    }
    catch (eR) {
      try { PropertiesService.getScriptProperties().deleteProperty(FDC.PROP_RERUN_FOR); } catch (eP) {}   // Codex P2, #1003: the next tick tries again
      fdcNotify_('FINAL DECISION column: the re-run of the ' + fdcRunLabel_(rerun) + ' render could not be scheduled (the next tick tries again): ' + String(eR && eR.message || eR).slice(0, 160));
    }
  }
  return line;
}
function fdcRemoveRerunTriggers_() {
  var ts = ScriptApp.getProjectTriggers();
  for (var i = 0; i < ts.length; i++) if (ts[i].getHandlerFunction() === FDC.RERUN_HANDLER) ScriptApp.deleteTrigger(ts[i]);
}
/* 1.1.0 [RE-RUN]: the one-off trigger's handler - removes itself, then runs the Top 10 refresh. */
function fdcRerunTop10() {
  try { fdcRemoveRerunTriggers_(); } catch (e0) {}
  if (typeof refreshDecisionTop10 !== 'function') { fdcNotify_('FINAL DECISION column: refreshDecisionTop10 is not in this project.'); return 'missing'; }
  refreshDecisionTop10();
  return 'ran';
}
/* 1.1.0 [TODAY-LINE]: Top_10_Investments!A3, only while it is empty or already this line. */
function fdcStampToday_(ss, src, staleH) {
  var sh = ss.getSheetByName(FDC.T10), fd = ss.getSheetByName(FDC.SOURCE);
  if (!sh) return 'A3: skipped';
  var cell = sh.getRange(3, 1), cur = fdcTrim_(cell.getValues()[0][0]);
  if (cur && cur.indexOf('TODAY (') !== 0) return 'A3: not empty, left';
  if (!fd) { if (cur) cell.setValue(''); return 'A3: cleared (' + FDC.SOURCE + ' missing)'; }   // Codex P2, #1003: never an old line
  var vals = fd.getLastRow() > 1 ? fd.getRange(1, 1, Math.min(fd.getLastRow(), 400), 2).getValues() : [];
  var line = fdcTodayLine_(vals, src, staleH);
  if (!line) { if (cur) cell.setValue(''); return 'A3: cleared (no current decisions)'; }
  cell.setValue(line).setFontWeight('bold').setFontColor('#0f5c63').setWrap(false);
  return 'A3: today line';
}
/* 1.1.0 [RE-RUN]: the stopped render to re-run now ('' = none); records it so it is done once. */
function fdcRerunCheck_(ss) {
  var props = PropertiesService.getScriptProperties();
  var on = !/^(0|off|false|no)$/i.test(fdcTrim_(props.getProperty(FDC.PROP_RERUN)));
  var sh = ss.getSheetByName(FDC.T10);
  if (!sh) return '';
  var stuck = fdcStuck_(sh.getRange(2, 2).getValues()[0][0], new Date().getTime());
  var hour = (new Date().getUTCHours() + 3) % 24;
  if (!fdcRerunDue_(stuck, fdcTrim_(props.getProperty(FDC.PROP_RERUN_FOR)), hour, on)) return '';
  if (fdcRerunCount_(props.getProperty(FDC.PROP_RERUN_DAY), fdcRiyadhDay_(new Date().getTime())) >= FDC.RERUN_MAX_DAY) return '';   // 1.2.0
  props.setProperty(FDC.PROP_RERUN_FOR, stuck.run);
  return stuck.run;
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
    if (cfg.mode === 'note') {
      var head = sh.getRange(1, 1, 1, Math.min(sh.getLastColumn(), FDC.MAX_COLS)).getValues()[0], k = 0;
      for (var c = 0; c < head.length; c++) if (cfg.notes.indexOf(fdcTrim_(head[c])) >= 0) { sh.getRange(1, c + 1).setNote(''); k++; }
      out.push(cfg.name + ': ' + k + ' note(s) removed'); continue;
    }
    var lastRow = Math.min(sh.getLastRow(), FDC.MAX_ROWS), lastCol = Math.min(sh.getLastColumn(), FDC.MAX_COLS);
    var t = fdcFindTable_(sh.getRange(1, 1, lastRow, lastCol).getValues(), cfg.need, cfg.until);
    if (t) for (var x = 0; x < t.extras.length; x++) sh.getRange(t.hdr, t.extras[x], lastRow - t.hdr + 1, 1).clear();   // 1.3.0
    var gone = t && t.extras.length ? ' and ' + t.extras.length + ' Holding Read column(s)' : '';
    if (!t || !t.existing) { out.push(cfg.name + ': no column' + (gone ? ' (' + t.extras.length + ' Holding Read column(s) removed)' : '')); continue; }
    sh.getRange(t.hdr, t.col, lastRow - t.hdr + 1, 1).clear();
    out.push(cfg.name + ': removed from column ' + fdcColLetter_(t.col) + gone);
  }
  try {   // 1.1.0: the today line goes with it
    var t10 = ss.getSheetByName(FDC.T10);
    if (t10 && fdcTrim_(t10.getRange(3, 1).getValues()[0][0]).indexOf('TODAY (') === 0) { t10.getRange(3, 1).setValue(''); out.push('A3 today line removed'); }
  } catch (eA) {}
  fdcNotify_(out.join(' \u00b7 '));
  return out;
}
function fdcStatus() {
  var last = '';
  try { last = PropertiesService.getScriptProperties().getProperty(FDC.PROP_LAST) || 'never run'; } catch (e) { last = 'n/a'; }
  var n = 0, ts = ScriptApp.getProjectTriggers();
  for (var i = 0; i < ts.length; i++) if (ts[i].getHandlerFunction() === FDC.HANDLER) n++;
  var rr = 0;
  try { rr = fdcRerunCount_(PropertiesService.getScriptProperties().getProperty(FDC.PROP_RERUN_DAY), fdcRiyadhDay_(new Date().getTime())); } catch (eC) {}
  var msg = 'FINAL DECISION column ' + FDC_VERSION + ' \u00b7 trigger ' + (n ? 'installed' : 'NOT installed (run fdcInstallTrigger)') + ' \u00b7 self-test ' + (fdcCoreOk_() ? 'ok' : 'FAIL') +
            ' \u00b7 re-runs today ' + rr + '/' + FDC.RERUN_MAX_DAY + ' \u00b7 last tick: ' + last;
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
    var hr = fdcReadFromValues_([['HOLDING READ'], ['status'], [], ['Symbol', 'Name', 'Last session', 'Stock %', 'Market %', 'Sector %', 'Driver', 'Main news (last session)', 'Next session read', 'Up after similar days'],
      ['AX.US', 'Axos', '2026-11-17', '-1.20%', '-1.00%', '-1.90%', 'SECTOR', 'news', 'NO EDGE', '']]);
    var tx = fdcFindTable_([['Action', 'Symbol', 'Name', '', 'FINAL DECISION x', 'LAST SESSION \u00b7 what moved it y', 'NEXT SESSION READ z'], ['HOLD', 'A.US', 'a']], ['Action', 'Symbol'], 'blank');
    return hr.ok && hr.session === '2026-11-17' && hr.cols.sym === 1 && hr.cols.news === 8 && hr.cols.up === 10 &&
      fdcSessionAgeD_('2026-11-17', Date.UTC(2026, 10, 18, 6, 0)) === 1 && fdcSessionAgeD_('2026-11-17', Date.UTC(2026, 10, 17, 21, 30)) === 1 &&
      fdcReadHeaders_(hr, Date.UTC(2026, 10, 25, 6, 0))[0].indexOf('OLD (8 days)') > 0 && fdcReadHeaders_(hr, Date.UTC(2026, 10, 18, 6, 0))[1].indexOf('OLD') < 0 &&
      fdcReadFormulas_(2, hr.cols)[0].indexOf('MATCH(TRIM(INDIRECT("R"&ROW()&"C2",FALSE)),Holding_Read!$A:$A,0)') > 0 &&
      tx.col === 5 && tx.existing && tx.extras.join(',') === '6,7' &&
      !!t && t.hdr === 4 && t.symCol === 2 && t.col === 7 && t.rows.join(',') === '5,6' && !t.existing &&
      t2.rows.join(',') === '2,4' && t2.col === 5 && src.ok && src.at === '2026-11-16 08:41' &&
      fdcFormula_(2).indexOf('INDIRECT("R"&ROW()&"C2",FALSE)') > 0 && fdcAgeH_('2026-11-16 08:41', Date.UTC(2026, 10, 16, 6, 41)) === 1 &&
      fdcHeader_({ ok: true, at: '2026-11-16 08:41', ageH: 7 }, 6).indexOf('OLD (7 h)') > 0 &&
      fdcStuck_('Last run 2026-11-16 08:44:11 | status: running\u2026', Date.UTC(2026, 10, 16, 6, 44, 11)).minutes === 60 &&
      fdcStuck_('Last run 2026-11-16 10:16:20 | status: ok', 0) === null &&
      fdcRerunDue_({ run: 'r', minutes: 25 }, '', 9, true) && !fdcRerunDue_({ run: 'r', minutes: 25 }, 'r', 9, true) &&
      fdcStuck_('Last run 2026-11-16 08:10:08 | status: ok | SKIPPED render 2026-11-16 08:49:57 (out of time: t+331.9s, paint budget 300s) - x', Date.UTC(2026, 10, 16, 5, 50, 57)).run === 'skipped 2026-11-16 08:49:57' &&
      fdcRerunDue_({ run: 's', minutes: 1, skipped: true }, '', 9, true) && fdcRerunCount_('2026-11-16|3', '2026-11-16') === 3 &&
      fdcRerunCount_('2026-11-15|3', '2026-11-16') === 0 && fdcRiyadhDay_(Date.UTC(2026, 10, 15, 21, 30)) === '2026-11-16' &&
      fdcTodayLine_([['Symbol', 'Decision'], ['AX.US', 'BUY'], ['DDI.US', 'HOLD']], { ok: true, at: 'x', ageH: 0 }, 6).indexOf('BUY AX.US \u00b7 SELL none') > 0;
  } catch (e) { return false; }
}
