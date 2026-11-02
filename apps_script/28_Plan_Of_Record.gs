/**
 * 28_Plan_Of_Record.gs -- TFB Plan of Record v1.0.0 (2026-11-03)
 * ============================================================================
 * One stable decision page: the plan you can act on for each market session,
 * locked from the cockpit's EXECUTABLE render, tracked to execution against
 * your IBKR holdings and working orders, and never silently replaced.
 *
 * WHY (owner review, Tue 3 Nov 2026 01:08 Riyadh, verbatim: "today morning
 * there was 6 symboles as quailifed for investing , but after that now no one
 * is qualified , I have make decision for investing as per quilifed and
 * allocation of cash now all change the program should show in consistance
 * beahviour and more stable recommdadtions"). Evidence, Mon 2 Nov
 * (_Selection_Log, _Status, Render logs, release runs, IBKR read-only):
 *  - 08:49 the cockpit rendered 6 EXECUTABLE tickets (ITRN 62, DDI 244,
 *    CRC 41, GLPI 83, EFC 268, INDV 93; release 41371798113, run
 *    41363258387). All six trade on NYSE/NASDAQ, which opened at 17:30. The
 *    feed expired at 10:42, seven hours before the market could fill them.
 *  - 17:30:23 the builder still qualified 26 names (pool 26); the 17:33
 *    release made run 41445971593 EXECUTABLE until 23:37; at 17:38:26 the
 *    builder qualified 2 (0440.HK, 0883.HK). The W-2 quote-freshness gate
 *    (opportunity_builder v1.4.0+, default on) rejects every quote older
 *    than 15 minutes while its venue trades, and the sheet's US prices dated
 *    from before the open. The 17:39 board: 0 executable, ITRN / DDI / CRC /
 *    INDV grace, GLPI / EFC / OLP hard exits. No render ever showed US
 *    tickets while the US market was open.
 *  - The owner bought ITRN 67, DDI 240, GLPI 79 and INDV 95 (17:30-18:01).
 *    Afterwards Top_10 shows them as grace seats, Portfolio_Decision as
 *    BLOCK (reconciliation evidence missing) and v2 as grace-held: three
 *    pages, three statuses, none of them "executed".
 *  - 23:31 the scheduled release re-verified and withdrew the feed
 *    (decision_failed); 00:55 refused (release_window_consumed).
 * The plan existed; the pages lost it. Nothing in the workbook records
 * "this was the plan for this session, and this is what happened to it".
 *
 * WHAT (new file; no other file changes; nothing audited is written):
 *  - Reads only: _Selection_Log (the cockpit's own append-only record of
 *    every render), My_Portfolio (holdings: Position Qty, Avg Cost, Buy
 *    Date), _Status L:M 'TFB Decision Feed', and the latest read-only IBKR
 *    capture through 10_My_Portfolio pdIbkrLatestCapture_ (working stop and
 *    limit orders). Never calls the backend, never calls the broker.
 *  - Writes only its own two sheets: _Plan_Of_Record (ledger, hidden; rebuilt
 *    on every refresh from the last 4 days of renders) and Plan_Of_Record
 *    (the page). Top_10_Investments, Portfolio_Decision,
 *    _Status and every audited surface are untouched, so no release audit,
 *    acceptance check or receipt can change because of this file.
 *  - LOCK RULE, per venue session: a plan is the tickets (Shares > 0) of an
 *    EXECUTABLE render. For each venue the plan session is the first regular
 *    session whose close is after the render. A newer EXECUTABLE render
 *    issued BEFORE that session opens replaces the plan (pre-open re-lock);
 *    once the session is open the plan is frozen until it closes: later
 *    renders never change it. A render issued during an open session locks
 *    only when no plan exists for that session yet.
 *  - STATUS per ticket, in order: EXECUTED (the holding grew by >= 90% of
 *    the plan's shares since the lock, or, when the lock-time quantity is
 *    unknown, a holding bought on or after the lock date) / PARTIAL /
 *    INVALIDATED (release kill switch) / EXPIRED (session closed without a
 *    fill) / NOT EXECUTABLE (validity 'feed': the decision feed does not
 *    release this plan's run) / WAITING (before the open) / OPEN (buy only
 *    inside the entry zone).
 *  - Protection: for executed shares, the IBKR working SELL stop (level and
 *    covered shares) against the plan stop; 'NO STOP AT IBKR' when none.
 *  - Live board: each ticket's state on the latest render (seat stability
 *    or exit), shown beside the plan so a re-ranking is visible and never
 *    mistaken for a change of plan.
 *
 * GATES (Script Properties, read on every run; no re-paste to change):
 *  POR_MODE     = off (default) | observe | on. Any other value is off.
 *                 off: porRefresh returns at once, nothing is read or written.
 *                 observe: the ledger _Plan_Of_Record is written, no page.
 *                 on: ledger + the Plan_Of_Record page.
 *  POR_VALIDITY = feed (default) | session. Any other value is feed.
 *                 feed: an unexecuted ticket is OPEN only while the decision
 *                 feed reads EXECUTABLE for the plan's own run (the release
 *                 TTL keeps its meaning; nothing becomes executable that the
 *                 feed does not release).
 *                 session: a plan stays OPEN through its venue session
 *                 (built on the last close, which the freshness gate itself
 *                 certifies while the venue is closed); the release kill
 *                 switch still invalidates it. An owner decision.
 *
 * INSTALL (owner): paste this file as a new script file; run porSelfTest once
 * (authorizes Drive for the capture reader); set POR_MODE=observe; run
 * porInstallTrigger (every 15 minutes); after one day read _Plan_Of_Record,
 * then POR_MODE=on. Rollback: POR_MODE=off (or porRemoveTrigger), or delete
 * the file; the two sheets can then be deleted by hand.
 *
 * KNOWN LIMITS: venue sessions are the regular hours of US, Tadawul, HKEX,
 * LSE, TSE and the main EU markets (time-zone aware, DST included); exchange
 * holidays and half days are not modelled (a ticket on a holiday expires at
 * that day's close without a fill). Tickets of other venues are listed with
 * 'venue calendar unknown' and follow the feed only. A US ticket is matched
 * to IBKR by its base symbol (ITRN.US <-> ITRN); other venues by base symbol
 * without leading zeros. Holdings come from My_Portfolio, so a fill shows
 * after the next IBKR capture reaches the page (hourly in the US session).
 *
 * Not a licensed financial adviser; the owner decides and executes every
 * trade. ES5, ASCII. Functions: 45, all new (prefix por). Removed: 0.
 * ============================================================================
 */

var POR_VERSION = '1.0.0';
var POR_MODE_PROP = 'POR_MODE';           // off (default) | observe | on
var POR_VALIDITY_PROP = 'POR_VALIDITY';   // feed (default) | session
var POR_HANDLER = 'porRefreshTrigger';
var POR_EVERY_MIN = 15;
var POR_TZ = 'Asia/Riyadh';
var POR_LOG_SHEET = '_Selection_Log';
var POR_PORTFOLIO_SHEET = 'My_Portfolio';
var POR_STATUS_SHEET = '_Status';
var POR_FEED_KEY = 'TFB Decision Feed';
var POR_LEDGER_SHEET = '_Plan_Of_Record';
var POR_PAGE_SHEET = 'Plan_Of_Record';
var POR_SCAN_ROWS = 1500;                 // tail of _Selection_Log (about 6 days of renders)
var POR_LOOKBACK_MS = 4 * 86400000;       // renders older than this never lock a plan; the ledger is these 4 days
var POR_PAGE_KEEP_MS = 36 * 3600000;      // the page shows sessions closed in the last 36 h
var POR_EXEC_FRACTION = 0.9;              // executed when >= 90% of the plan's shares were bought
var POR_HELD_CAPTURE_MS = 30 * 60000;     // lock-time quantity recorded only within 30 min of the lock
var POR_STOP_TOL = 0.01;                  // 1%: IBKR stop vs plan stop
var POR_RUN_KEY = 'POR_RUNNING';
var POR_RUN_TTL_S = 240;

var POR_VENUES = {
  US: {label: 'NYSE/NASDAQ', tz: 'America/New_York', open: [9, 30], close: [16, 0], days: [1, 2, 3, 4, 5]},
  SR: {label: 'Tadawul', tz: 'Asia/Riyadh', open: [10, 0], close: [15, 0], days: [0, 1, 2, 3, 4]},
  HK: {label: 'HKEX', tz: 'Asia/Hong_Kong', open: [9, 30], close: [16, 0], days: [1, 2, 3, 4, 5]},
  L: {label: 'LSE', tz: 'Europe/London', open: [8, 0], close: [16, 30], days: [1, 2, 3, 4, 5]},
  T: {label: 'TSE', tz: 'Asia/Tokyo', open: [9, 0], close: [15, 30], days: [1, 2, 3, 4, 5]},
  EU: {label: 'EU main markets', tz: 'Europe/Paris', open: [9, 0], close: [17, 30], days: [1, 2, 3, 4, 5]}
};
var POR_EU_SUFFIXES = ',PA,AS,BR,DE,F,MI,MC,LS,SW,ST,CO,HE,OL,VI,IR,';

var POR_LEDGER_HEADER = ['Plan Key', 'Venue', 'Session Date', 'Session Open (Riyadh)',
  'Session Close (Riyadh)', 'Locked At (Riyadh)', 'Lock Rule', 'Replaced Lock', 'Source Run',
  'Rank', 'Symbol', 'Name', 'Ccy', 'FX to SAR', 'Ref Price', 'Entry Low', 'Entry High',
  'Shares', 'Ticket SAR', 'Stop', 'TP1', 'TP2', 'ROI % (TP1)', 'Engine ROI % (12M)', 'Conf',
  'Held At Lock', 'Status', 'Status Reason', 'Executed Qty', 'Avg Cost', 'IBKR Protection',
  'Live Board', 'Cockpit Note', 'Updated At (Riyadh)'];

var POR_PAGE_HEADER = ['Session', 'Market hours (Riyadh)', 'Locked', 'Rank', 'Symbol', 'Name',
  'Entry zone', 'Shares', 'Ticket SAR', 'Stop', 'TP1', 'TP2', 'Status', 'Why', 'Executed',
  'IBKR protection', 'Live board now', 'Cockpit note at lock'];

var POR_STATUS_COLORS = {
  'EXECUTED': '#d9ead3', 'PARTIAL': '#fff2cc', 'OPEN': '#cfe2f3', 'WAITING': '#eeeeee',
  'NOT EXECUTABLE': '#fce5cd', 'EXPIRED': '#efefef', 'INVALIDATED': '#f4cccc'
};

/* ------------------------------------------------------------------ gates */

/** PURE. 'observe' / 'on' (any case, trimmed), else 'off'. */
function porModeOf_(raw) {
  var v = String(raw === null || raw === undefined ? '' : raw).replace(/^\s+|\s+$/g, '').toLowerCase();
  return (v === 'observe' || v === 'on') ? v : 'off';
}

/** PURE. 'session' (any case, trimmed), else 'feed'. */
function porValidityOf_(raw) {
  var v = String(raw === null || raw === undefined ? '' : raw).replace(/^\s+|\s+$/g, '').toLowerCase();
  return v === 'session' ? 'session' : 'feed';
}

/** A Script Property, or null on any fault. */
function porProp_(name) {
  try {
    return PropertiesService.getScriptProperties().getProperty(name);
  } catch (e) {
    return null;
  }
}

function porNow_() {
  return new Date().getTime();
}

/* ------------------------------------------------------------- utilities */

function porStr_(v) {
  return String(v === null || v === undefined ? '' : v).replace(/^\s+|\s+$/g, '');
}

/** PURE. A number from a cell: numbers pass, text loses commas / SAR / %, '' and dashes are null. */
function porNum_(v) {
  if (typeof v === 'number') return isFinite(v) ? v : null;
  var s = porStr_(v).replace(/,/g, '').replace(/SAR|%/gi, '').replace(/^\s+|\s+$/g, '');
  if (!s || !/^[-+]?[0-9]*\.?[0-9]+$/.test(s)) return null;
  var n = parseFloat(s);
  return isFinite(n) ? n : null;
}

/** PURE. Header text -> lowercase letters and digits only ('FX->SAR' -> 'fxsar'). */
function porNorm_(h) {
  return porStr_(h).toLowerCase().replace(/[^a-z0-9]/g, '');
}

/** PURE. {normalized header: 0-based index} for the first occurrence of each header. */
function porHeaderMap_(row) {
  var out = {};
  for (var i = 0; i < row.length; i++) {
    var k = porNorm_(row[i]);
    if (k && !out.hasOwnProperty(k)) out[k] = i;
  }
  return out;
}

function porPad2_(n) {
  return (n < 10 ? '0' : '') + n;
}

/** PURE. A price as text with 2 decimals, '?' when unknown. */
function porFix2_(v) {
  return (v === null || v === undefined || !isFinite(v)) ? '?' : (+v).toFixed(2);
}

function porRound_(v, dp) {
  if (v === null || v === undefined || !isFinite(v)) return null;
  var f = Math.pow(10, dp);
  return Math.round(v * f) / f;
}

/** Wall-clock fields of an instant in a time zone (Utilities.formatDate). */
function porWall_(ms, tz) {
  var s = Utilities.formatDate(new Date(ms), tz, "yyyy-MM-dd'T'HH:mm:ss");
  var m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})$/.exec(String(s));
  if (!m) return null;
  return {y: +m[1], mo: +m[2], d: +m[3], h: +m[4], mi: +m[5], s: +m[6]};
}

/** The UTC instant of a wall-clock time in a time zone (DST aware, iterative). */
function porLocalToMs_(y, mo, d, h, mi, tz) {
  var target = Date.UTC(y, mo - 1, d, h, mi, 0);
  var guess = target;
  for (var k = 0; k < 4; k++) {
    var w = porWall_(guess, tz);
    if (!w) return null;
    var diff = target - Date.UTC(w.y, w.mo - 1, w.d, w.h, w.mi, w.s);
    if (diff === 0) break;
    guess += diff;
  }
  return guess;
}

function porRiyadh_(ms, fmt) {
  return ms === null || ms === undefined ? '' : Utilities.formatDate(new Date(ms), POR_TZ, fmt || 'yyyy-MM-dd HH:mm');
}

/** A 'Logged At' cell: a Date, or Riyadh text 'yyyy-MM-dd H:mm[:ss]'. null when unreadable. */
function porParseAt_(v) {
  if (v && typeof v.getTime === 'function') {
    var t = v.getTime();
    return isFinite(t) ? t : null;
  }
  var m = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{1,2}):(\d{2})(?::(\d{2}))?/.exec(porStr_(v));
  if (!m) return null;
  var ms = porLocalToMs_(+m[1], +m[2], +m[3], +m[4], +m[5], POR_TZ);
  return ms === null ? null : ms + (m[6] ? (+m[6]) * 1000 : 0);
}

/** A 'Buy Date' cell -> 'yyyy-MM-dd' (Riyadh) or ''. */
function porDateText_(v) {
  if (v && typeof v.getTime === 'function') return porRiyadh_(v.getTime(), 'yyyy-MM-dd');
  var m = /^(\d{4})-(\d{2})-(\d{2})/.exec(porStr_(v));
  return m ? m[1] + '-' + m[2] + '-' + m[3] : '';
}

/* ---------------------------------------------------------------- venues */

/** PURE. Venue code of a TFB symbol ('ITRN.US' -> 'US', '1050.SR' -> 'SR'), or null. */
function porVenueOf_(symbol) {
  var s = porStr_(symbol).toUpperCase();
  if (!s) return null;
  var dot = s.lastIndexOf('.');
  var sfx = dot >= 0 ? s.slice(dot + 1) : 'US';
  if (sfx === 'US') return 'US';
  if (POR_VENUES.hasOwnProperty(sfx)) return sfx;
  if (POR_EU_SUFFIXES.indexOf(',' + sfx + ',') >= 0) return 'EU';
  return null;
}

/** The first regular session of a venue whose close is after atMs:
 * {venue, label, date, openMs, closeMs}, or null for an unknown venue. */
function porSessionFor_(venue, atMs) {
  var v = POR_VENUES[venue];
  if (!v) return null;
  var w = porWall_(atMs, v.tz);
  if (!w) return null;
  var base = Date.UTC(w.y, w.mo - 1, w.d);
  for (var k = 0; k < 10; k++) {
    var day = new Date(base + k * 86400000);
    var y = day.getUTCFullYear(), mo = day.getUTCMonth() + 1, d = day.getUTCDate();
    if (v.days.indexOf(day.getUTCDay()) < 0) continue;
    var openMs = porLocalToMs_(y, mo, d, v.open[0], v.open[1], v.tz);
    var closeMs = porLocalToMs_(y, mo, d, v.close[0], v.close[1], v.tz);
    if (openMs === null || closeMs === null) return null;
    if (closeMs > atMs) {
      return {venue: venue, label: v.label, date: y + '-' + porPad2_(mo) + '-' + porPad2_(d),
              openMs: openMs, closeMs: closeMs};
    }
  }
  return null;
}

/* ------------------------------------------------------------------ reads */

/** _Selection_Log tail -> {ok, reason, renders: [{atMs, output, run, rows}] ascending, latest}. */
function porReadLog_(ss, nowMs) {
  var res = {ok: false, reason: '', renders: [], latest: null};
  var sh = ss.getSheetByName(POR_LOG_SHEET);
  if (!sh) { res.reason = POR_LOG_SHEET + ' missing'; return res; }
  var last = sh.getLastRow(), ncol = sh.getLastColumn();
  if (last < 2 || ncol < 5) { res.reason = POR_LOG_SHEET + ' empty'; return res; }
  var hmap = porHeaderMap_(sh.getRange(1, 1, 1, ncol).getValues()[0]);
  var need = ['loggedat', 'runinfo', 'symbol', 'shares', 'entryzone', 'stopsar', 'tp1sar', 'fxsar'];
  for (var n = 0; n < need.length; n++) {
    if (!hmap.hasOwnProperty(need[n])) { res.reason = POR_LOG_SHEET + ' header lacks ' + need[n]; return res; }
  }
  var start = Math.max(2, last - POR_SCAN_ROWS + 1);
  var vals = sh.getRange(start, 1, last - start + 1, ncol).getValues();
  var col = function (r, key) { return hmap.hasOwnProperty(key) ? r[hmap[key]] : ''; };
  var byKey = {}, order = [];
  for (var i = 0; i < vals.length; i++) {
    var r = vals[i];
    var atMs = porParseAt_(col(r, 'loggedat'));
    if (atMs === null || atMs > nowMs + 60000 || atMs < nowMs - POR_LOOKBACK_MS) continue;
    var sym = porStr_(col(r, 'symbol')).toUpperCase();
    if (!sym) continue;
    var key = String(atMs);
    if (!byKey.hasOwnProperty(key)) {
      var info = porStr_(col(r, 'runinfo'));
      var mo = /output:\s*([A-Z_]+)/.exec(info);
      var mr = /VERIFIED \(run (\d+)\)/.exec(info);
      byKey[key] = {atMs: atMs, output: mo ? mo[1] : '', run: mr ? mr[1] : '', rows: []};
      order.push(key);
    }
    byKey[key].rows.push({
      rank: porStr_(col(r, 'rank')), symbol: sym, name: porStr_(col(r, 'name')),
      ccy: porStr_(col(r, 'ccy')).toUpperCase(), fx: porNum_(col(r, 'fxsar')),
      price: porNum_(col(r, 'price')), zone: porStr_(col(r, 'entryzone')),
      ticketSar: porNum_(col(r, 'ticketsar')), shares: porNum_(col(r, 'shares')),
      stopSar: porNum_(col(r, 'stopsar')), tp1Sar: porNum_(col(r, 'tp1sar')),
      tp2Sar: porNum_(col(r, 'tp2sar')), roi: porNum_(col(r, 'roi')),
      eroi: porNum_(col(r, 'engineroi')), conf: porStr_(col(r, 'conf')),
      note: porStr_(col(r, 'advisornote')), outcome: porStr_(col(r, 'outcome')),
      stability: porStr_(col(r, 'stability'))
    });
  }
  order.sort(function (a, b) { return byKey[a].atMs - byKey[b].atMs; });
  for (var j = 0; j < order.length; j++) res.renders.push(byKey[order[j]]);
  res.latest = res.renders.length ? res.renders[res.renders.length - 1] : null;
  res.ok = true;
  return res;
}

/** My_Portfolio -> {ok, reason, map: {SYMBOL: {qty, avg, buyDate}}}. */
function porReadHoldings_(ss) {
  var res = {ok: false, reason: '', map: {}};
  var sh = ss.getSheetByName(POR_PORTFOLIO_SHEET);
  if (!sh) { res.reason = POR_PORTFOLIO_SHEET + ' missing'; return res; }
  var last = sh.getLastRow(), ncol = sh.getLastColumn();
  if (last < 1 || ncol < 2) { res.reason = POR_PORTFOLIO_SHEET + ' empty'; return res; }
  var vals = sh.getRange(1, 1, Math.min(last, 600), ncol).getValues();
  var hmap = porHeaderMap_(vals[0]);
  if (!hmap.hasOwnProperty('symbol') || !hmap.hasOwnProperty('positionqty')) {
    res.reason = POR_PORTFOLIO_SHEET + ' header lacks Symbol / Position Qty';
    return res;
  }
  for (var i = 1; i < vals.length; i++) {
    var sym = porStr_(vals[i][hmap.symbol]).toUpperCase();
    var qty = porNum_(vals[i][hmap.positionqty]);
    if (!sym || qty === null) continue;
    res.map[sym] = {
      qty: qty,
      avg: hmap.hasOwnProperty('avgcost') ? porNum_(vals[i][hmap.avgcost]) : null,
      buyDate: hmap.hasOwnProperty('buydate') ? porDateText_(vals[i][hmap.buydate]) : ''
    };
  }
  res.ok = true;
  return res;
}

/** PURE. A 'TFB Decision Feed' value -> {head, run, killed, text}. */
function porFeedOf_(value) {
  var text = porStr_(value);
  var head = porStr_(text.split('|')[0]).toUpperCase();
  var m = /run[=_ ]?(\d+)/i.exec(text);
  var token = /^EXECUTABLE\b/.test(head) ? 'EXECUTABLE' : (head ? head.replace(/\(.*$/, '') : 'MISSING');
  return {head: token, raw: head, run: m ? m[1] : '', killed: /release_killed/i.test(text), text: text};
}

/** _Status L:M 'TFB Decision Feed'. */
function porReadFeed_(ss) {
  var sh = ss.getSheetByName(POR_STATUS_SHEET);
  if (!sh) return porFeedOf_('');
  var last = Math.min(sh.getLastRow(), 400);
  if (last < 1 || sh.getLastColumn() < 13) return porFeedOf_('');
  var vals = sh.getRange(1, 12, last, 2).getValues();
  for (var i = 0; i < vals.length; i++) {
    if (porStr_(vals[i][0]) === POR_FEED_KEY) return porFeedOf_(vals[i][1]);
  }
  return porFeedOf_('');
}

/** PURE. IBKR base symbol of a TFB symbol: 'ITRN.US' -> 'ITRN', '0440.HK' -> '440'. */
function porIbkrBase_(symbol) {
  var s = porStr_(symbol).toUpperCase();
  var dot = s.lastIndexOf('.');
  var base = dot >= 0 ? s.slice(0, dot) : s;
  return /^[0-9]+$/.test(base) ? String(parseInt(base, 10)) : base;
}

/** PURE. Capture orders (tfb.ibkr-capture/v1 'orders') -> {stops: {BASE: [{px, qty}]}, limits: {...}}. */
function porOrdersOf_(orders) {
  var out = {stops: {}, limits: {}};
  var list = (orders && orders.orders) || orders || [];
  if (Object.prototype.toString.call(list) !== '[object Array]') return out;
  for (var i = 0; i < list.length; i++) {
    var o = list[i] || {};
    var st = porStr_(o.order_status).toUpperCase();
    if (/FILLED|CANCEL|EXPIRED|INACTIVE|REJECT/.test(st)) continue;
    var m = /^(buy|sell)\s+[\d,\.]+\s+(\S+)/i.exec(porStr_(o.primary_description));
    var side = porStr_(o.side).toUpperCase() || (m ? m[1].toUpperCase() : '');
    if (side !== 'SELL' || !m) continue;
    var base = porIbkrBase_(m[2]);
    var rem = porNum_(o.remaining_shares_qty);
    var qty = rem !== null ? rem : porNum_(o.total_shares_qty);
    if (!(qty > 0)) continue;
    var typ = porStr_(o.order_type).toUpperCase();
    if (/STOP|STP/.test(typ)) {
      var ms = /STP\s+(?:LMT\s+)?([\d,]*\.?\d+)/i.exec(porStr_(o.secondary_description));
      var px = ms ? porNum_(ms[1]) : null;
      (out.stops[base] = out.stops[base] || []).push({px: px, qty: qty});
    } else if (/LIMIT|LMT/.test(typ)) {
      (out.limits[base] = out.limits[base] || []).push({px: porNum_(o.limit_price), qty: qty});
    }
  }
  return out;
}

/** Latest read-only IBKR capture (10_My_Portfolio reader) -> {ok, note, stops, limits}. */
function porReadOrders_(nowMs) {
  if (typeof pdIbkrLatestCapture_ !== 'function') {
    return {ok: false, note: 'capture reader not installed (10_My_Portfolio)', stops: {}, limits: {}};
  }
  var cap;
  try {
    cap = pdIbkrLatestCapture_(nowMs);
  } catch (e) {
    return {ok: false, note: 'capture read failed', stops: {}, limits: {}};
  }
  if (!cap || !cap.ok || !cap.json) {
    return {ok: false, note: 'no capture (' + porStr_(cap && cap.reason) + ')', stops: {}, limits: {}};
  }
  if (porStr_(cap.json.schema) !== 'tfb.ibkr-capture/v1') {
    return {ok: false, note: 'capture schema not recognised', stops: {}, limits: {}};
  }
  var parsed = porOrdersOf_(cap.json.orders);
  var at = Date.parse(porStr_(cap.json.captured_at));
  parsed.ok = true;
  parsed.note = 'capture ' + (isNaN(at) ? porStr_(cap.name) : porRiyadh_(at, 'yyyy-MM-dd HH:mm'));
  return parsed;
}

/* ----------------------------------------------------------------- plans */

/** PURE. Two numbers of an entry zone ('190.86-198.73 SAR'), or null. */
function porZoneOf_(text) {
  var m = /([0-9][0-9,]*\.?[0-9]*)[^0-9]+([0-9][0-9,]*\.?[0-9]*)/.exec(porStr_(text));
  if (!m) return null;
  var a = porNum_(m[1]), b = porNum_(m[2]);
  if (a === null || b === null) return null;
  return {low: Math.min(a, b), high: Math.max(a, b)};
}

/** PURE. One locked ticket from a render row (SAR levels converted with the row's FX to SAR). */
function porTicketOf_(row) {
  var fx = row.fx && row.fx > 0 ? row.fx : null;
  var local = function (sar) { return (sar === null || fx === null) ? null : porRound_(sar / fx, 4); };
  var z = porZoneOf_(row.zone);
  return {
    rank: row.rank, symbol: row.symbol, name: row.name, ccy: row.ccy, fx: fx,
    price: row.price, low: z ? local(z.low) : null, high: z ? local(z.high) : null,
    shares: row.shares, ticketSar: row.ticketSar, stop: local(row.stopSar),
    tp1: local(row.tp1Sar), tp2: local(row.tp2Sar), roi: row.roi, eroi: row.eroi,
    conf: row.conf, note: row.note
  };
}

/** PURE. The lock rule over the renders (ascending) -> plans ordered by session open:
 * [{key, venue, label, date, openMs, closeMs, lockedAtMs, lockRule, replaced, run, tickets}]. */
function porDerivePlans_(renders) {
  var plans = {}, keys = [];
  for (var i = 0; i < renders.length; i++) {
    var r = renders[i];
    if (r.output !== 'EXECUTABLE') continue;
    var byVenue = {}, venues = [];
    for (var j = 0; j < r.rows.length; j++) {
      var row = r.rows[j];
      if (!(row.shares > 0)) continue;
      var v = porVenueOf_(row.symbol) || '?';
      if (!byVenue.hasOwnProperty(v)) { byVenue[v] = []; venues.push(v); }
      byVenue[v].push(porTicketOf_(row));
    }
    for (var k = 0; k < venues.length; k++) {
      var venue = venues[k];
      var s = venue === '?' ? null : porSessionFor_(venue, r.atMs);
      var key = s ? venue + '|' + s.date : '?|' + porRiyadh_(r.atMs, 'yyyy-MM-dd');
      var preOpen = s ? r.atMs < s.openMs : false;
      var cur = plans.hasOwnProperty(key) ? plans[key] : null;
      var plan = {
        key: key, venue: venue, label: s ? s.label : 'venue calendar unknown',
        date: s ? s.date : porRiyadh_(r.atMs, 'yyyy-MM-dd'), openMs: s ? s.openMs : null,
        closeMs: s ? s.closeMs : null, lockedAtMs: r.atMs,
        lockRule: s ? (preOpen ? 'pre-open' : 'in-session') : 'feed only', replaced: '',
        run: r.run, tickets: byVenue[venue]
      };
      if (!cur) {
        plans[key] = plan;
        keys.push(key);
      } else if (s && preOpen && cur.lockRule === 'pre-open' && r.atMs > cur.lockedAtMs) {
        plan.replaced = porRiyadh_(cur.lockedAtMs, 'yyyy-MM-dd HH:mm');
        plans[key] = plan;
      }
    }
  }
  var out = [];
  for (var n = 0; n < keys.length; n++) out.push(plans[keys[n]]);
  out.sort(function (a, b) {
    var ao = a.openMs === null ? a.lockedAtMs : a.openMs, bo = b.openMs === null ? b.lockedAtMs : b.openMs;
    return ao - bo || (a.venue < b.venue ? -1 : (a.venue > b.venue ? 1 : 0));
  });
  return out;
}

/** Prior ledger -> {'planKey|SYMBOL|lockedAt': heldAtLock} (only numeric values). */
function porReadLedger_(ss) {
  var out = {};
  var sh = ss.getSheetByName(POR_LEDGER_SHEET);
  if (!sh) return out;
  var last = sh.getLastRow(), ncol = sh.getLastColumn();
  if (last < 2 || ncol < POR_LEDGER_HEADER.length) return out;
  var vals = sh.getRange(1, 1, last, POR_LEDGER_HEADER.length).getValues();
  var h = porHeaderMap_(vals[0]);
  if (!h.hasOwnProperty('plankey') || !h.hasOwnProperty('heldatlock')) return out;
  for (var i = 1; i < vals.length; i++) {
    var held = porNum_(vals[i][h.heldatlock]);
    if (held === null) continue;
    out[porStr_(vals[i][h.plankey]) + '|' + porStr_(vals[i][h.symbol]).toUpperCase() + '|' +
        porAtText_(vals[i][h.lockedatriyadh])] = held;
  }
  return out;
}

/** A ledger time cell (Sheets may have turned the text into a Date) -> 'yyyy-MM-dd HH:mm' Riyadh. */
function porAtText_(v) {
  if (v && typeof v.getTime === 'function') return porRiyadh_(v.getTime(), 'yyyy-MM-dd HH:mm');
  return porStr_(v);
}

/** PURE. Status of one ticket of a plan. ctx = {nowMs, validity, feed, holdings, orders, live, prior}. */
function porStatusOf_(plan, t, ctx) {
  var lockText = porRiyadh_(plan.lockedAtMs, 'yyyy-MM-dd HH:mm');
  var priorKey = plan.key + '|' + t.symbol + '|' + lockText;
  var h = ctx.holdings.map.hasOwnProperty(t.symbol) ? ctx.holdings.map[t.symbol] : null;
  var qtyNow = h ? h.qty : 0;
  var held = ctx.prior.hasOwnProperty(priorKey) ? ctx.prior[priorKey] : null;
  if (held === null && ctx.holdings.ok && ctx.nowMs - plan.lockedAtMs <= POR_HELD_CAPTURE_MS) held = qtyNow;
  var execQty = 0, basis = '';
  if (held !== null) {
    execQty = Math.max(0, qtyNow - held);
    basis = 'since the lock';
  } else if (h && h.buyDate && h.buyDate >= porRiyadh_(plan.lockedAtMs, 'yyyy-MM-dd')) {
    execQty = qtyNow;
    basis = 'bought ' + h.buyDate;
  }
  var st = {held: held, execQty: execQty, avg: h ? h.avg : null, status: '', why: '', protect: '', noStop: false};
  if (execQty > 0 && execQty >= POR_EXEC_FRACTION * t.shares) {
    st.status = 'EXECUTED';
    st.why = execQty + ' sh held, ' + basis;
  } else if (execQty > 0) {
    st.status = 'PARTIAL';
    st.why = execQty + ' of ' + t.shares + ' sh, ' + basis;
  } else if (ctx.feed.killed) {
    st.status = 'INVALIDATED';
    st.why = 'release kill switch';
  } else if (plan.closeMs !== null && ctx.nowMs >= plan.closeMs) {
    st.status = 'EXPIRED';
    st.why = 'session closed ' + porRiyadh_(plan.closeMs, 'yyyy-MM-dd HH:mm') + ' without a fill';
  } else if (ctx.validity === 'feed' || plan.closeMs === null) {
    if (ctx.feed.head === 'EXECUTABLE' && ctx.feed.run === plan.run) {
      st.status = (plan.openMs !== null && ctx.nowMs < plan.openMs) ? 'WAITING' : 'OPEN';
    } else {
      st.status = 'NOT EXECUTABLE';
      st.why = 'feed ' + ctx.feed.raw + (ctx.feed.run ? ' (run ' + ctx.feed.run + ')' : '') +
          ' does not release this plan (run ' + plan.run + ')';
    }
  } else {
    st.status = (plan.openMs !== null && ctx.nowMs < plan.openMs) ? 'WAITING' : 'OPEN';
  }
  if (st.status === 'WAITING') st.why = 'market opens ' + porRiyadh_(plan.openMs, 'yyyy-MM-dd HH:mm');
  if (st.status === 'OPEN') {
    st.why = 'buy only inside the entry zone' +
        (ctx.validity === 'session' && !(ctx.feed.head === 'EXECUTABLE' && ctx.feed.run === plan.run) ?
            ' (held by session validity; feed ' + ctx.feed.raw + ')' : '');
  }
  if (execQty > 0) {
    if (!ctx.orders.ok) {
      st.protect = 'capture n/a: ' + ctx.orders.note;
    } else {
      var stops = ctx.orders.stops[porIbkrBase_(t.symbol)] || [];
      if (!stops.length) {
        st.protect = 'NO STOP AT IBKR';
        st.noStop = true;
      } else {
        var cover = 0;
        for (var i = 0; i < stops.length; i++) cover += stops[i].qty;
        var px = stops[0].px;
        st.protect = 'stop ' + porFix2_(px) + (cover < execQty ? ' covers ' + cover + '/' + execQty : '');
        if (px !== null && t.stop !== null && Math.abs(px - t.stop) > POR_STOP_TOL * t.stop) {
          st.protect += ' (plan ' + porFix2_(t.stop) + ')';
        }
      }
      var tps = ctx.orders.limits[porIbkrBase_(t.symbol)] || [];
      if (tps.length && tps[0].px !== null) st.protect += '; TP ' + porFix2_(tps[0].px);
    }
  }
  return st;
}

/** PURE. The latest render's view of a symbol: seat stability, or its exit, or absent. */
function porLiveOf_(latest, symbol) {
  if (!latest) return 'no render';
  for (var i = 0; i < latest.rows.length; i++) {
    var r = latest.rows[i];
    if (r.symbol !== symbol) continue;
    var at = porRiyadh_(latest.atMs, 'HH:mm');
    if (r.outcome && /EXIT/i.test(r.outcome)) return r.outcome + ' (' + at + ')';
    return (r.stability || latest.output || 'listed') + ' (' + at + ', ' + (latest.output || '?') + ')';
  }
  return 'not on the board (' + porRiyadh_(latest.atMs, 'HH:mm') + ')';
}

/** PURE. The last render at a time and, for each symbol, its last mention (exits are logged right after). */
function porLiveView_(renders) {
  if (!renders.length) return null;
  var lastAt = renders[renders.length - 1].atMs;
  var view = {atMs: lastAt, output: renders[renders.length - 1].output, rows: []};
  var seen = {};
  for (var i = renders.length - 1; i >= 0 && renders[i].atMs >= lastAt - 5 * 60000; i--) {
    for (var j = 0; j < renders[i].rows.length; j++) {
      var r = renders[i].rows[j];
      if (seen[r.symbol]) continue;
      seen[r.symbol] = true;
      view.rows.push(r);
    }
  }
  return view;
}

/** Ledger rows (arrays, POR_LEDGER_HEADER order) + summary for the page. */
function porBuildRows_(plans, ctx) {
  var rows = [], summary = {plans: 0, tickets: 0, executed: 0, open: 0, waiting: 0, missingStops: 0, statuses: {}};
  var nowText = porRiyadh_(ctx.nowMs, 'yyyy-MM-dd HH:mm');
  for (var i = 0; i < plans.length; i++) {
    var p = plans[i];
    summary.plans++;
    for (var j = 0; j < p.tickets.length; j++) {
      var t = p.tickets[j];
      var st = porStatusOf_(p, t, ctx);
      summary.tickets++;
      summary.statuses[st.status] = (summary.statuses[st.status] || 0) + 1;
      if (st.status === 'EXECUTED' || st.status === 'PARTIAL') summary.executed++;
      if (st.status === 'OPEN') summary.open++;
      if (st.status === 'WAITING') summary.waiting++;
      if (st.noStop) summary.missingStops++;
      rows.push([p.key, p.label, p.date, porRiyadh_(p.openMs, 'yyyy-MM-dd HH:mm'),
        porRiyadh_(p.closeMs, 'yyyy-MM-dd HH:mm'), porRiyadh_(p.lockedAtMs, 'yyyy-MM-dd HH:mm'),
        p.lockRule, p.replaced, p.run, t.rank, t.symbol, t.name, t.ccy, t.fx, t.price, t.low, t.high,
        t.shares, t.ticketSar, t.stop, t.tp1, t.tp2, t.roi, t.eroi, t.conf,
        st.held === null ? '' : st.held, st.status, st.why, st.execQty, st.avg === null ? '' : st.avg,
        st.protect, porLiveOf_(ctx.live, t.symbol), t.note, nowText]);
    }
  }
  return {rows: rows, summary: summary};
}

/* ----------------------------------------------------------------- writes */

/** The sheet, created when missing, sized to rows x cols (never fewer than 2 rows). */
function porSheet_(ss, name, rows, cols, hide) {
  var sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
    if (hide) sh.hideSheet();
  }
  var needRows = Math.max(rows, 2), maxR = sh.getMaxRows(), maxC = sh.getMaxColumns();
  if (maxR < needRows) sh.insertRowsAfter(maxR, needRows - maxR);
  else if (maxR > needRows + 5) sh.deleteRows(needRows + 1, maxR - needRows);
  if (maxC < cols) sh.insertColumnsAfter(maxC, cols - maxC);
  else if (maxC > cols) sh.deleteColumns(cols + 1, maxC - cols);
  return sh;
}

function porWriteLedger_(ss, built) {
  var data = [POR_LEDGER_HEADER].concat(built.rows);
  var sh = porSheet_(ss, POR_LEDGER_SHEET, data.length, POR_LEDGER_HEADER.length, true);
  sh.getRange(1, 1, sh.getMaxRows(), POR_LEDGER_HEADER.length).clearContent();
  sh.getRange(1, 1, data.length, POR_LEDGER_HEADER.length).setValues(data);
  sh.setFrozenRows(1);
  return data.length - 1;
}

function porFmt_(v, dp) {
  return v === null || v === undefined || v === '' ? '' : porRound_(+v, dp);
}

/** The visible page from the ledger rows. */
function porRenderPage_(ss, built, ctx) {
  var L = {};
  for (var c = 0; c < POR_LEDGER_HEADER.length; c++) L[porNorm_(POR_LEDGER_HEADER[c])] = c;
  var s = built.summary, out = [], colors = [];
  var width = POR_PAGE_HEADER.length;
  var blank = function () { var r = []; for (var k = 0; k < width; k++) r.push(''); return r; };
  var line = function (text) { var r = blank(); r[0] = text; return r; };
  out.push(line('TODAY\'S PLAN OF RECORD - the plan you can act on, locked per market session (Plan of Record v' + POR_VERSION + ')'));
  out.push(line('Refreshed ' + porRiyadh_(ctx.nowMs, 'yyyy-MM-dd HH:mm') + ' Riyadh | validity ' + ctx.validity +
      ' | feed ' + ctx.feed.raw + (ctx.feed.run ? ' (run ' + ctx.feed.run + ')' : '') + ' | plans ' + s.plans +
      ' | tickets ' + s.tickets + ' | executed ' + s.executed + ' | open ' + s.open + ' | waiting ' + s.waiting +
      ' | missing stops ' + s.missingStops + ' | ' + ctx.orders.note));
  out.push(line('A plan is the tickets of the last EXECUTABLE cockpit render before its market opens; once the market is open it never changes until the close. ' +
      'The live board below may re-rank intraday (quotes older than 15 minutes while a market trades are deferred); that never changes this plan. ' +
      'Not a licensed financial adviser; you decide and execute every trade.'));
  out.push(blank());
  out.push(POR_PAGE_HEADER.slice());
  var first = out.length;
  for (var i = 0; i < built.rows.length; i++) {
    var r = built.rows[i];
    var closeMs = porParseAt_(r[L.sessioncloseriyadh]);
    if (closeMs !== null && closeMs < ctx.nowMs - POR_PAGE_KEEP_MS) continue;
    var ccy = r[L.ccy] || '';
    var zone = (r[L.entrylow] === null || r[L.entrylow] === '') ? '' :
        porFix2_(r[L.entrylow]) + ' - ' + porFix2_(r[L.entryhigh]) + ' ' + ccy;
    var hours = porRiyadh_(porParseAt_(r[L.sessionopenriyadh]), 'HH:mm') + '-' + porRiyadh_(closeMs, 'HH:mm');
    var executed = r[L.executedqty] > 0 ? r[L.executedqty] + ' sh' + (r[L.avgcost] !== '' ? ' @ ' + porFmt_(r[L.avgcost], 2) : '') : '';
    out.push([r[L.sessiondate] + ' ' + r[L.venue], hours,
      porRiyadh_(porParseAt_(r[L.lockedatriyadh]), 'MM-dd HH:mm') + ' ' + r[L.lockrule] + (r[L.replacedlock] ? ' (replaced ' + r[L.replacedlock] + ')' : ''),
      r[L.rank], r[L.symbol], r[L.name], zone, r[L.shares], r[L.ticketsar], porFmt_(r[L.stop], 2),
      porFmt_(r[L.tp1], 2), porFmt_(r[L.tp2], 2), r[L.status], r[L.statusreason], executed,
      r[L.ibkrprotection], r[L.liveboard], String(r[L.cockpitnote]).slice(0, 160)]);
    colors.push(r[L.status]);
  }
  if (out.length === first) out.push(line('No locked plan in the last 36 hours: no EXECUTABLE cockpit render with tickets.'));
  out.push(blank());
  var live = ctx.live;
  out.push(line('LIVE BOARD NOW: ' + (live ? 'latest cockpit render ' + porRiyadh_(live.atMs, 'yyyy-MM-dd HH:mm') +
      ', output ' + (live.output || '?') + ', ' + live.rows.length + ' rows' : 'no render in the last 4 days') +
      '. It is research until a new plan locks; this page changes only when a plan locks, fills or expires.'));
  out.push(line('Known limits: exchange holidays and half days are not modelled; fills appear after the next IBKR capture reaches My_Portfolio.'));
  var sh = porSheet_(ss, POR_PAGE_SHEET, out.length, width, false);
  sh.getRange(1, 1, sh.getMaxRows(), width).clearContent();
  sh.getRange(1, 1, sh.getMaxRows(), width).setBackground(null);
  sh.getRange(1, 1, out.length, width).setValues(out);
  sh.getRange(1, 1, 1, width).setFontWeight('bold');
  sh.getRange(first, 1, 1, width).setFontWeight('bold').setBackground('#1f3864').setFontColor('#ffffff');
  if (colors.length) {
    sh.getRange(first + 1, 9, colors.length, 1).setNumberFormat('#,##0');
    sh.getRange(first + 1, 10, colors.length, 3).setNumberFormat('0.00');
  }
  for (var k = 0; k < colors.length; k++) {
    var bg = POR_STATUS_COLORS.hasOwnProperty(colors[k]) ? POR_STATUS_COLORS[colors[k]] : null;
    if (bg) sh.getRange(first + 1 + k, 13, 1, 1).setBackground(bg);
    if (String(out[first + k][15]).indexOf('NO STOP') === 0) sh.getRange(first + 1 + k, 16, 1, 1).setBackground('#f4cccc');
  }
  sh.setFrozenRows(first);
  return out.length;
}

/* ------------------------------------------------------------ entry points */

function porClaim_() {
  try {
    var cache = CacheService.getScriptCache();
    if (cache.get(POR_RUN_KEY)) return false;
    cache.put(POR_RUN_KEY, String(porNow_()), POR_RUN_TTL_S);
    return true;
  } catch (e) {
    return true;
  }
}

function porRelease_() {
  try {
    CacheService.getScriptCache().remove(POR_RUN_KEY);
  } catch (e) {
    // nothing to release
  }
}

/** One refresh. Returns a summary object; never writes outside its own two sheets. */
function porRefresh() {
  var mode = porModeOf_(porProp_(POR_MODE_PROP));
  if (mode === 'off') return {mode: 'off', version: POR_VERSION};
  if (!porClaim_()) return {mode: mode, version: POR_VERSION, skipped: 'another refresh is running'};
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var nowMs = porNow_();
    var validity = porValidityOf_(porProp_(POR_VALIDITY_PROP));
    var log = porReadLog_(ss, nowMs);
    if (!log.ok) return {mode: mode, version: POR_VERSION, error: log.reason};
    var ctx = {nowMs: nowMs, validity: validity, feed: porReadFeed_(ss), holdings: porReadHoldings_(ss),
               orders: porReadOrders_(nowMs), live: porLiveView_(log.renders), prior: porReadLedger_(ss)};
    var plans = porDerivePlans_(log.renders);
    var built = porBuildRows_(plans, ctx);
    var res = {mode: mode, version: POR_VERSION, validity: validity, feed: ctx.feed.raw,
               holdings: ctx.holdings.ok ? 'ok' : ctx.holdings.reason, orders: ctx.orders.note,
               ledgerRows: porWriteLedger_(ss, built), summary: built.summary};
    if (mode === 'on') res.pageRows = porRenderPage_(ss, built, ctx);
    return res;
  } finally {
    porRelease_();
  }
}

/** Time-driven trigger handler. */
function porRefreshTrigger() {
  try {
    var res = porRefresh();
    if (res && res.error) Logger.log('[POR ' + POR_VERSION + '] ' + res.error);
  } catch (e) {
    Logger.log('[POR ' + POR_VERSION + '] refresh failed: ' + (e && e.message ? e.message : e));
  }
}

/** Install the 15-minute trigger (removes this file's earlier triggers first). */
function porInstallTrigger() {
  var removed = porRemoveTrigger();
  ScriptApp.newTrigger(POR_HANDLER).timeBased().everyMinutes(POR_EVERY_MIN).create();
  return 'installed ' + POR_HANDLER + ' every ' + POR_EVERY_MIN + ' min' + (removed ? ' (replaced ' + removed + ')' : '');
}

/** Remove this file's triggers; returns how many were removed. */
function porRemoveTrigger() {
  var n = 0, ts = ScriptApp.getProjectTriggers();
  for (var i = 0; i < ts.length; i++) {
    if (ts[i].getHandlerFunction() === POR_HANDLER) {
      ScriptApp.deleteTrigger(ts[i]);
      n++;
    }
  }
  return n;
}

/** Read-only self test: gates, inputs and what a refresh would lock now. Writes nothing. */
function porSelfTest() {
  var out = ['Plan of Record ' + POR_VERSION];
  out.push('POR_MODE=' + porModeOf_(porProp_(POR_MODE_PROP)) + ' POR_VALIDITY=' + porValidityOf_(porProp_(POR_VALIDITY_PROP)));
  var ss = SpreadsheetApp.getActiveSpreadsheet(), nowMs = porNow_();
  var log = porReadLog_(ss, nowMs);
  out.push(POR_LOG_SHEET + ': ' + (log.ok ? log.renders.length + ' renders in 4 days' : log.reason));
  var h = porReadHoldings_(ss);
  var nh = 0, k;
  for (k in h.map) if (h.map.hasOwnProperty(k)) nh++;
  out.push(POR_PORTFOLIO_SHEET + ': ' + (h.ok ? nh + ' holdings' : h.reason));
  var feed = porReadFeed_(ss);
  out.push('feed: ' + (feed.raw || 'missing') + (feed.run ? ' run ' + feed.run : '') + (feed.killed ? ' (kill)' : ''));
  out.push('orders: ' + porReadOrders_(nowMs).note);
  if (log.ok) {
    var plans = porDerivePlans_(log.renders);
    for (var i = 0; i < plans.length; i++) {
      out.push('plan ' + plans[i].key + ' locked ' + porRiyadh_(plans[i].lockedAtMs) + ' ' + plans[i].lockRule +
          ' run ' + plans[i].run + ' tickets ' + plans[i].tickets.length);
    }
  }
  var triggers = 0, ts = ScriptApp.getProjectTriggers();
  for (var j = 0; j < ts.length; j++) if (ts[j].getHandlerFunction() === POR_HANDLER) triggers++;
  out.push('trigger ' + POR_HANDLER + ': ' + triggers);
  Logger.log(out.join('\n'));
  return out;
}
