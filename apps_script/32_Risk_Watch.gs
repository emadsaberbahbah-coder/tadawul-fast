/**
 * 32_Risk_Watch.gs
 * Emad Bahbah Financial Advisor - risk watch: an e-mail within minutes when a holding nears its stop or its target,
 * falls sharply, or sits exposed at the open
 * Version: 1.0.0 (2026-11-26)
 *   1.0.0: WHY (owner, Thu 26 Nov 2026 09:36 Riyadh, in Arabic: does the system study and adjust take-profit and
 *          stop-loss more precisely, faster and directly, so that what happened yesterday is not repeated - and can an
 *          e-mail or alerts be sent at once when that risk exists?).
 *          WHAT HAPPENED (IBKR, read 26 Nov): two stop orders sold in the first second of a session - NMM on 20 Nov
 *          (17:30:01 Riyadh) and IBOC on 25 Nov (17:30:00). IBOC's stop stood 1.7 % under the last close after days of
 *          drifting toward it; the broker's own chart shows no trade at or below the stop that day (low 62.75, stop
 *          62.67) and the share closed above the sale. The Exit Plan reads the close, once a day, and its e-mail goes
 *          out at 07:30: nothing told the owner that a stop sat inside one ordinary day's range, and nothing watches
 *          a price between two closes.
 *          WHAT THIS FILE DOES: rwTick (ONE time trigger every 5 minutes, installed by tfbInstallRiskWatchTrigger)
 *          works on New York weekdays from 75 minutes before the open to 10 minutes after the close and returns at
 *          once at any other time. Each run reads the holdings and their levels from the Exit Plan's own page
 *          (Exit_Plan: quantity, entry, stop, target, ATR, the stop working at the broker) and ledger (_Exit_Plan:
 *          first-target flag, offer judgment), reads one Yahoo chart per US holding and one for the S&P 500 fund
 *          (5-minute bars with the pre-market), and e-mails the owner once per holding, rule and day:
 *            BEFORE THE OPEN  OPEN_RISK   the stop working at the broker is within max(2.5 %, 1 ATR) of the last close -
 *                                         inside one ordinary day's range, so the first seconds of the open can reach
 *                                         it; with the pre-market trade when there is one
 *                             PRE_BELOW   a pre-market trade at or below that stop
 *            IN THE SESSION   NEAR        the price is within 1 % above the stop (the broker's when it is known, else
 *                                         the Exit Plan's)
 *                             CROSSED     the price is at or below it
 *                             TARGET      the target of record is reached (the first one, +1R, or the next)
 *                             DROP        the share is down 4 % or more on the day (again at 8 %, 12 % ...), with the
 *                                         market's own move beside it
 *                             MARKET      the S&P 500 fund is down 2 % or more (again at 3 %, 4 % ...)
 *            A CASH OFFER     OFFER_AWAY  a holding kept under an offer (judgment offer:manual) trades 8 % or more under
 *                                         the offer price, or falls 3 % on the day
 *                             OFFER_AT    it trades within 0.5 % of the offer price
 *            ITS OWN HEALTH   PRICES      three runs in a row without a price
 *                             LEVELS      the Exit Plan page is missing, unreadable or older than 5 days
 *          A TRADING DAY: in the session it is proven by data - the S&P 500 fund must have traded within 10 minutes and
 *          a share's own price must be at most 20 minutes old, so on a holiday or after an early close nothing is fresh
 *          and nothing is sent. Before the open there is no trade to prove it, so the day is a trading day unless it is
 *          a listed New York holiday (RW_HOLIDAYS_, the dates of core/analysis/portfolio_actions.py and 2027; more in
 *          Script Property TFB_RISK_WATCH_HOLIDAYS, comma list). OPEN_RISK needs no price feed at all: the last close
 *          and the broker's stop are on the Exit Plan page; the pre-market trade is added when Yahoo gives one.
 *          IT IS A WATCH: it places, changes and cancels no order, calls no broker and writes no decision page. It
 *          writes its own tab (Risk_Watch: every holding, its price, its distance to the stop and the target) and its
 *          own log (_Risk_Watch_Log: one row per alert).
 *          WHAT IT CANNOT DO: a stop that sells in the first second of the open is faster than any watch. OPEN_RISK is
 *          the warning before that open; the order's own trigger settings are the owner's, at the broker. Listings
 *          outside the US are shown on the tab as not watched.
 *          GATE: Script Property TFB_RISK_WATCH = off (default: one property read and return) | observe (the rules
 *          run, the tab and the log are written, no e-mail) | on. tfbInstallRiskWatchTrigger sets it to on when it is
 *          not set. TFB_RISK_WATCH_TO = recipient (default DP2_EXIT_ALERT_TO, else the script owner).
 *          TFB_RISK_WATCH_CFG = optional JSON of limits (RW_LIMITS_). At most 12 e-mails a day.
 *          TFB_RISK_WATCH_EVERY_MIN = 1, 5, 10, 15 or 30 (default 5; read when the trigger is installed).
 *          COST: a run inside the hours takes a few seconds (one page read, one batch of charts, one tab write) - about
 *          100 runs a day; outside them it reads one property and the clock.
 *          Rollback: TFB_RISK_WATCH = off, or tfbRemoveRiskWatchTrigger.
 * ES5 only; ASCII only.
 */
var RW_VERSION = '1.0.0';   // lockstep with the header

var RW_ = {
  HANDLER: 'rwTick',
  EVERY_MIN: 5,
  PROP_MODE: 'TFB_RISK_WATCH',
  PROP_TO: 'TFB_RISK_WATCH_TO',
  PROP_TO_FALLBACK: 'DP2_EXIT_ALERT_TO',
  PROP_CFG: 'TFB_RISK_WATCH_CFG',
  PROP_STATE: 'TFB_RISK_WATCH_STATE',
  PROP_HOLIDAYS: 'TFB_RISK_WATCH_HOLIDAYS',
  PROP_EVERY: 'TFB_RISK_WATCH_EVERY_MIN',
  TAB_PAGE: 'Exit_Plan',
  TAB_LEDGER: '_Exit_Plan',
  TAB_STATUS: 'Risk_Watch',
  TAB_LOG: '_Risk_Watch_Log',
  TZ_NY: 'America/New_York',
  TZ_RIYADH: 'Asia/Riyadh',
  MARKET: 'SPY',
  MARKET_NAME: 'S&P 500 fund',
  HOSTS: ['query1.finance.yahoo.com', 'query2.finance.yahoo.com'],
  OPEN_MIN: 570,          // 09:30 New York
  CLOSE_MIN: 960,         // 16:00 New York
  PRE_MIN: 75,            // the watch starts this long before the open
  GRACE_MIN: 10,          // and ends this long after the close
  LEDGER_TAIL: 400,       // ledger rows read from the bottom
  STATUS_ROWS: 40,
  STATUS_COLS: 11
};

/* New York full-day closures: 2026 as core/analysis/portfolio_actions.py (_CONFIRM_SESSION_HOLIDAYS US), and 2027.
 * Only the before-the-open rules read it; a date missing here costs one early e-mail on a closed day, never a missed one. */
var RW_HOLIDAYS_ = ['2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03', '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07',
  '2026-11-26', '2026-12-25', '2027-01-01', '2027-01-18', '2027-02-15', '2027-03-26', '2027-05-31', '2027-06-18', '2027-07-05',
  '2027-09-06', '2027-11-25', '2027-12-24'];

/* The limits of the rules. TFB_RISK_WATCH_CFG may replace any of them with a positive number. */
var RW_LIMITS_ = {
  near_pct: 1.0,            // NEAR: within this much above the stop
  open_pct: 2.5,            // OPEN_RISK: the stop within this much of the last close ...
  open_atr: 1.0,            // ... or within this many ATR, whichever is wider
  drop_pct: 4.0,            // DROP: down this much on the day (and each further multiple)
  market_pct: 2.0,          // MARKET: the fund down this much on the day ...
  market_step: 1.0,         // ... and each further step
  offer_gap_pct: 8.0,       // OFFER_AWAY: this far under the offer price
  offer_drop_pct: 3.0,      // OFFER_AWAY: or down this much on the day
  offer_at_pct: 0.5,        // OFFER_AT: within this much of the offer price
  fresh_min: 20,            // a share's price is at most this old
  market_fresh_min: 10,     // the fund's price is at most this old in the session
  page_max_days: 5,         // LEVELS: the Exit Plan page is at most this old
  fail_runs: 3,             // PRICES: this many runs in a row without a price
  mail_cap: 12              // e-mails a day
};

/* ------------------------------------------------------------------ pure helpers */
function rwTrim_(v) { return String(v === null || v === undefined ? '' : v).replace(/^\s+|\s+$/g, ''); }

function rwNum_(v) {
  if (typeof v === 'number') return isFinite(v) ? v : null;
  var s = rwTrim_(v).replace(/,/g, '');
  if (!/^[-+]?(\d+\.?\d*|\.\d+)$/.test(s)) return null;
  var n = parseFloat(s);
  return isFinite(n) ? n : null;
}

function rwF_(n, d) {
  if (n === null || n === undefined || !isFinite(n)) return 'n/a';
  return Number(n).toFixed(d === undefined ? 2 : d);
}

function rwPct_(n, d) {
  if (n === null || n === undefined || !isFinite(n)) return 'n/a';
  return (n >= 0 ? '+' : '-') + Math.abs(n).toFixed(d === undefined ? 1 : d) + '%';
}

function rwModeCore_(raw) {
  var v = rwTrim_(raw).toLowerCase();
  if (v === 'on' || v === '1' || v === 'true' || v === 'yes') return 'on';
  return v === 'observe' ? 'observe' : 'off';
}

function rwLimitsCore_(raw) {
  var out = {}, k, j = null;
  for (k in RW_LIMITS_) { if (RW_LIMITS_.hasOwnProperty(k)) out[k] = RW_LIMITS_[k]; }
  try { j = JSON.parse(rwTrim_(raw) || '{}'); } catch (e) { j = null; }
  if (j && typeof j === 'object') {
    for (k in out) {
      if (out.hasOwnProperty(k) && j.hasOwnProperty(k)) {
        var n = rwNum_(j[k]);
        if (n !== null && n > 0) out[k] = n;
      }
    }
  }
  return out;
}

/* 'yyyy-MM-dd HH:mm' in New York -> { day, dow (0 = Sunday), minutes }. The weekday comes from the date itself. */
function rwClockCore_(stamp) {
  var m = /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})$/.exec(rwTrim_(stamp));
  if (!m) return null;
  return { day: m[1] + '-' + m[2] + '-' + m[3], dow: new Date(Date.UTC(+m[1], +m[2] - 1, +m[3])).getUTCDay(),
           minutes: (+m[4]) * 60 + (+m[5]) };
}

function rwPhaseCore_(clock) {
  if (!clock || clock.dow === 0 || clock.dow === 6) return 'idle';
  if (clock.minutes >= RW_.OPEN_MIN - RW_.PRE_MIN && clock.minutes < RW_.OPEN_MIN) return 'pre';
  if (clock.minutes >= RW_.OPEN_MIN && clock.minutes <= RW_.CLOSE_MIN + RW_.GRACE_MIN) return 'session';
  return 'idle';
}

/* The trigger's interval: one of the values Apps Script accepts, else the default. */
function rwEveryCore_(raw) {
  var n = rwNum_(raw);
  return n === 1 || n === 5 || n === 10 || n === 15 || n === 30 ? n : RW_.EVERY_MIN;
}

function rwHolidayCore_(day, extra) {
  var more = rwTrim_(extra).split(/[\s,;]+/), i;
  for (i = 0; i < RW_HOLIDAYS_.length; i++) if (RW_HOLIDAYS_[i] === day) return true;
  for (i = 0; i < more.length; i++) if (more[i] && more[i] === day) return true;
  return false;
}

function rwWatched_(symbol) { return /\.US$/.test(rwTrim_(symbol).toUpperCase()); }

/* As scripts/exit_plan.py yahoo_symbol: IBOC.US -> IBOC, BRK.B.US -> BRK-B. */
function rwYahooSymbol_(symbol) {
  var s = rwTrim_(symbol).toUpperCase();
  return /\.US$/.test(s) ? s.slice(0, -3).replace(/\./g, '-') : s;
}

function rwShort_(symbol) { return rwTrim_(symbol).toUpperCase().replace(/\.US$/, ''); }

function rwChartUrl_(host, ysym) {
  return 'https://' + host + '/v8/finance/chart/' + encodeURIComponent(ysym) + '?interval=5m&range=1d&includePrePost=true';
}

/* One Yahoo chart body -> the last regular trade, the previous close and today's pre-market trade.
 * { ok, price, priceMs, prevClose, regStartMs, regEndMs, preStartMs, preLast, preMs, preVol, why } */
function rwParseChartCore_(body) {
  var out = { ok: false, price: null, priceMs: 0, prevClose: null, regStartMs: 0, regEndMs: 0, preStartMs: 0,
              preLast: null, preMs: 0, preVol: 0, why: '' };
  var j = null;
  try { j = JSON.parse(body); } catch (e) { out.why = 'not JSON'; return out; }
  var r = j && j.chart && j.chart.result && j.chart.result[0];
  if (!r || !r.meta) { out.why = 'no result'; return out; }
  var meta = r.meta, px = rwNum_(meta.regularMarketPrice), ts = rwNum_(meta.regularMarketTime);
  if (px === null || px <= 0 || ts === null || ts <= 0) { out.why = 'no price'; return out; }
  out.ok = true; out.price = px; out.priceMs = ts * 1000;
  // the prior session's close, in the order core/providers/yahoo_chart_provider.py reads an intraday chart
  var pc = rwNum_(meta.regularMarketPreviousClose);
  if (pc === null) pc = rwNum_(meta.previousClose);
  if (pc === null) pc = rwNum_(meta.chartPreviousClose);
  out.prevClose = pc !== null && pc > 0 ? pc : null;
  var ctp = meta.currentTradingPeriod || {}, reg = ctp.regular || {}, pre = ctp.pre || {};
  out.regStartMs = (rwNum_(reg.start) || 0) * 1000;
  out.regEndMs = (rwNum_(reg.end) || 0) * 1000;
  out.preStartMs = (rwNum_(pre.start) || 0) * 1000;
  // as scripts/pre_open_check.py parse_prepost: without a pre-market period, the 6 hours before the regular open
  if (out.regStartMs > 0 && (out.preStartMs <= 0 || out.preStartMs > out.regStartMs)) out.preStartMs = out.regStartMs - 6 * 3600000;
  var t = r.timestamp || [], q = (r.indicators && r.indicators.quote && r.indicators.quote[0]) || {};
  var c = q.close || [], v = q.volume || [];
  if (out.preStartMs > 0 && out.regStartMs > out.preStartMs) {
    for (var i = 0; i < t.length; i++) {
      var ms = (rwNum_(t[i]) || 0) * 1000, cl = rwNum_(c[i]);
      if (ms >= out.preStartMs && ms < out.regStartMs && cl !== null && cl > 0) {
        out.preLast = cl; out.preMs = ms; out.preVol += rwNum_(v[i]) || 0;
      }
    }
  }
  return out;
}

/* The Exit_Plan page (values) and the _Exit_Plan ledger (values, header first) -> the holdings to watch.
 * { ok, asOf ('yyyy-MM-dd HH:mm' Riyadh or ''), brokerKnown, count, list: [{ symbol, shares, entry, lastClose, atrPct,
 *   stop, target, action, brokerStop, tp1, offer }], why }
 * dayOf(cell) turns a ledger date cell (a Date or text) into 'yyyy-MM-dd'. */
function rwHoldingsCore_(page, ledger, dayOf) {
  var out = { ok: false, asOf: '', brokerKnown: false, count: 0, list: [], why: '' };
  page = page || [];
  var r, c, top = -1, n = null, m;
  for (r = 0; r < page.length; r++) {
    var first = rwTrim_((page[r] || [])[0]);
    if (!out.asOf) { m = /^As of (\d{4}-\d{2}-\d{2} \d{2}:\d{2})/.exec(first); if (m) out.asOf = m[1]; }
    if (/broker orders (read|from)/i.test(first) && !/broker orders unknown/i.test(first)) out.brokerKnown = true;
    m = /^HOLDINGS \((\d+)\)/.exec(first);
    if (m && top < 0) { top = r; n = +m[1]; }
  }
  if (top < 0 || top + 1 >= page.length) { out.why = RW_.TAB_PAGE + ' has no HOLDINGS table'; return out; }
  var col = {}, hdr = page[top + 1] || [];
  for (c = 0; c < hdr.length; c++) col[rwTrim_(hdr[c])] = c;
  if (!col.hasOwnProperty('Symbol') || !col.hasOwnProperty('Stop') || !col.hasOwnProperty('Target')) {
    out.why = RW_.TAB_PAGE + ' HOLDINGS header not recognised'; return out;
  }
  var get = function (row, name) { return col.hasOwnProperty(name) ? row[col[name]] : ''; };
  var flags = rwLedgerFlagsCore_(ledger, dayOf);
  for (r = top + 2; r < page.length; r++) {
    var row = page[r] || [], sym = rwTrim_(get(row, 'Symbol')).toUpperCase();
    if (!sym) break;
    var last = rwNum_(get(row, 'Last close')), atrPct = rwNum_(get(row, 'ATR %')), f = flags[sym] || {};
    out.list.push({ symbol: sym, shares: rwNum_(get(row, 'Qty')), entry: rwNum_(get(row, 'Entry')), lastClose: last,
                    atrPct: atrPct, stop: rwNum_(get(row, 'Stop')), target: rwNum_(get(row, 'Target')),
                    action: rwTrim_(get(row, 'Action')), brokerStop: out.brokerKnown ? rwNum_(get(row, 'Broker stop')) : null,
                    tp1: f.tp1 === true, offer: f.offer === true });
  }
  out.ok = true; out.count = n === null ? out.list.length : n;
  return out;
}

/* The ledger's newest day -> { SYMBOL: { tp1, offer } }. */
function rwLedgerFlagsCore_(ledger, dayOf) {
  var out = {}, col = {}, r, c;
  ledger = ledger || [];
  if (!ledger.length) return out;
  for (c = 0; c < ledger[0].length; c++) col[rwTrim_(ledger[0][c])] = c;
  if (!col.hasOwnProperty('Date') || !col.hasOwnProperty('Symbol')) return out;
  var maxDay = '', days = [];
  for (r = 1; r < ledger.length; r++) {
    var d = dayOf ? dayOf(ledger[r][col.Date]) : rwTrim_(ledger[r][col.Date]).slice(0, 10);
    days.push(d);
    if (d && d > maxDay) maxDay = d;
  }
  for (r = 1; r < ledger.length; r++) {
    if (!maxDay || days[r - 1] !== maxDay) continue;
    var sym = rwTrim_(ledger[r][col.Symbol]).toUpperCase();
    if (!sym) continue;
    var f = out[sym] || { tp1: false, offer: false };
    if (col.hasOwnProperty('TP1 Reached') && /^yes$/i.test(rwTrim_(ledger[r][col['TP1 Reached']]))) f.tp1 = true;
    if (col.hasOwnProperty('Judgment') && /^offer:manual/i.test(rwTrim_(ledger[r][col.Judgment]))) f.offer = true;
    out[sym] = f;
  }
  return out;
}

/* The market's reading for this run: { ok, tradingDay, chgPct, price, openMs, why }.
 * Before the open a trading day is any day that is not a listed holiday (no trade can prove it yet); in the session
 * it is the fund's own fresh trade. openMs: today's open, from this script's New York clock. */
function rwMarketCore_(q, phase, nowMs, lim, holiday, openMs) {
  var out = { ok: !!(q && q.ok), tradingDay: false, chgPct: null, price: q && q.ok ? q.price : null, openMs: openMs || 0, why: '' };
  if (phase === 'pre') {
    out.tradingDay = holiday !== true;
    if (!out.tradingDay) out.why = 'a listed market holiday';
    return out;
  }
  if (!out.ok) { out.why = 'no market price'; return out; }
  var age = nowMs - q.priceMs;
  out.tradingDay = age >= -60000 && age <= lim.market_fresh_min * 60000 && q.priceMs >= q.regStartMs && q.regStartMs > 0;
  if (!out.tradingDay) out.why = 'no regular trade in the last ' + lim.market_fresh_min + ' min';
  else if (q.prevClose) out.chgPct = (q.price / q.prevClose - 1) * 100;
  return out;
}

function rwAlert_(code, symbol, sev, title, line) {
  return { key: symbol + '|' + code, code: code, symbol: symbol, sev: sev, title: title, line: line };
}

/* The rules for one holding. h: a holding of rwHoldingsCore_; q: its parsed chart; mkt: rwMarketCore_;
 * hm(ms) -> 'HH:mm' Riyadh. Returns alerts; an empty list when nothing is fresh or nothing is due. */
function rwEvaluateCore_(h, q, mkt, phase, lim, nowMs, hm) {
  var out = [];
  if (!h || !mkt || !mkt.tradingDay) return out;
  var sym = h.symbol, tag = rwShort_(sym), live = !!(q && q.ok);
  var mktTxt = mkt.chgPct === null ? '' : '; ' + RW_.MARKET_NAME + ' ' + rwPct_(mkt.chgPct);
  if (phase === 'pre') {
    if (h.offer || h.brokerStop === null || h.brokerStop <= 0) return out;
    // the last close: the last regular trade when Yahoo gives one from before this open, else the Exit Plan page's
    var close = live && (!mkt.openMs || q.priceMs < mkt.openMs) ? q.price : h.lastClose;
    if (!close || close <= 0) return out;
    var d = (close - h.brokerStop) / close * 100;
    var band = Math.max(lim.open_pct, h.atrPct !== null && h.atrPct > 0 ? lim.open_atr * h.atrPct : 0);
    var openTxt = mkt.openMs ? ' (' + hm(mkt.openMs) + ' Riyadh)' : '';
    // a pre-market trade counts only when it is of THIS morning (a chart can still hold the prior session's bars)
    var today = live && q.preLast !== null && q.preMs <= nowMs && (!mkt.openMs || (q.preMs < mkt.openMs && q.preMs >= mkt.openMs - 6 * 3600000));
    if (!today) q = { preLast: null, preMs: 0, preVol: 0 };
    var preTxt = !live ? ' The pre-market price could not be read.' : q.preLast === null ? ' No pre-market trade yet.'
      : ' Pre-market: ' + rwF_(q.preLast) + ' at ' + hm(q.preMs) + ' Riyadh' + (q.preVol > 0 ? ' on ' + Math.round(q.preVol) + ' shares' : '') + ' (thin trading).';
    if (q.preLast !== null && q.preLast <= h.brokerStop) {
      out.push(rwAlert_('PRE_BELOW', sym, 1, tag + ' pre-market at or below its stop',
        sym + ': a pre-market trade at ' + rwF_(q.preLast) + ' (' + hm(q.preMs) + ' Riyadh) is at or below the stop working at the broker, ' +
        rwF_(h.brokerStop) + '. If the price stays there, the order sells at the open' + openTxt + '. Last close ' + rwF_(close) + '.'));
    }
    if (d <= band) {
      out.push(rwAlert_('OPEN_RISK', sym, 2, tag + ' stop ' + rwF_(Math.max(d, 0), 1) + '% under the last close',
        sym + ': the stop working at the broker, ' + rwF_(h.brokerStop) + ', is ' + rwF_(Math.max(d, 0), 1) + '% under the last close ' + rwF_(close) +
        (h.atrPct !== null && h.atrPct > 0 ? ' - inside one ordinary day\'s range (ATR ' + rwF_(h.atrPct, 1) + '%)' : '') +
        '. A plain stop order can be reached in the first seconds of the open' + openTxt + '.' + preTxt));
    }
    return out;
  }
  if (phase !== 'session' || !live) return out;
  var age = nowMs - q.priceMs;
  if (age > lim.fresh_min * 60000 || age < -60000 || q.priceMs < q.regStartMs) return out;   // no fresh trade of this session
  var px = q.price, chg = q.prevClose ? (px / q.prevClose - 1) * 100 : null;
  var dayTxt = chg === null ? '' : ' Today ' + rwPct_(chg) + mktTxt + '.';
  if (h.offer) {
    if (h.target !== null && h.target > 0) {
      var gap = (h.target - px) / h.target * 100;
      if (gap <= lim.offer_at_pct) {
        out.push(rwAlert_('OFFER_AT', sym, 2, tag + ' at the offer price',
          sym + ' (cash offer ' + rwF_(h.target) + ', the sale is the owner\'s decision): ' + rwF_(px) + ' now, ' +
          (gap <= 0 ? 'at or above' : 'within ' + rwF_(gap, 1) + '% of') + ' the offer price.' + dayTxt));
      } else if (gap >= lim.offer_gap_pct || (chg !== null && chg <= -lim.offer_drop_pct)) {
        out.push(rwAlert_('OFFER_AWAY', sym, 1, tag + ' ' + rwF_(gap, 1) + '% under the offer',
          sym + ' (cash offer ' + rwF_(h.target) + ', the sale is the owner\'s decision): ' + rwF_(px) + ' now, ' + rwF_(gap, 1) +
          '% under the offer price. A widening gap can mean doubt about the offer; no stop order is asked for on this holding.' + dayTxt));
      }
    }
    return out;
  }
  var atBroker = h.brokerStop !== null && h.brokerStop > 0;
  var ref = atBroker ? h.brokerStop : h.stop;
  var both = atBroker && h.stop !== null && Math.abs(h.stop / h.brokerStop - 1) > 0.0025 ? ' (Exit Plan stop ' + rwF_(h.stop) + ')' : '';
  if (ref !== null && ref > 0) {
    if (px <= ref) {
      out.push(rwAlert_('CROSSED', sym, 1, tag + ' at or below its stop',
        sym + ': ' + rwF_(px) + ' now - at or below ' + (atBroker
          ? 'the stop working at the broker, ' + rwF_(ref) + both + '. The order has probably sold: check the broker.'
          : 'the Exit Plan stop ' + rwF_(ref) + ', and no working stop is known at the broker. The plan reads the close: at or below the stop at the close it says SELL.') + dayTxt));
    } else if (px <= ref * (1 + lim.near_pct / 100)) {
      out.push(rwAlert_('NEAR', sym, 2, tag + ' ' + rwF_((px / ref - 1) * 100, 1) + '% above its stop',
        sym + ': ' + rwF_(px) + ' now, ' + rwF_((px / ref - 1) * 100, 1) + '% above ' + (atBroker
          ? 'the stop working at the broker, ' + rwF_(ref) + both + '.'
          : 'the Exit Plan stop ' + rwF_(ref) + '; no working stop is known at the broker.') + dayTxt));
    }
  }
  if (h.target !== null && h.target > 0 && px >= h.target) {
    out.push(rwAlert_('TARGET', sym, 2, tag + ' target reached',
      sym + ': ' + rwF_(px) + ' now - ' + (h.tp1
        ? 'the target of record ' + rwF_(h.target) + ' (the next +1R step) is reached.'
        : 'the first target ' + rwF_(h.target) + ' (+1R) is reached. The plan\'s rule: sell half, and the stop rises to at least the entry' +
          (h.entry !== null ? ' ' + rwF_(h.entry) : '') + '.') + ' The Exit Plan confirms it on the close.' + dayTxt));
  }
  if (chg !== null && chg <= -lim.drop_pct) {
    var step = Math.floor(-chg / lim.drop_pct) * lim.drop_pct;
    out.push(rwAlert_('DROP' + rwF_(step, 0), sym, 1, tag + ' ' + rwPct_(chg) + ' today',
      sym + ': ' + rwPct_(chg) + ' today (' + rwF_(px) + ' now, ' + rwF_(q.prevClose) + ' at the last close)' + mktTxt + '.' +
      (ref !== null && ref > 0 ? ' Its stop ' + rwF_(ref) + ' is ' + rwF_((1 - ref / px) * 100, 1) + '% under the price.' : ' No stop is known for it.')));
  }
  return out;
}

/* The market's own alert: the fund down market_pct or more, once per further step. near: [{ symbol, distPct }]. */
function rwMarketAlertCore_(mkt, lim, near) {
  if (!mkt || !mkt.tradingDay || mkt.chgPct === null || mkt.chgPct > -lim.market_pct) return null;
  var step = lim.market_pct + Math.floor((-mkt.chgPct - lim.market_pct) / lim.market_step) * lim.market_step;
  var list = (near || []).slice().sort(function (a, b) { return a.distPct - b.distPct; }).slice(0, 5)
    .map(function (x) { return rwShort_(x.symbol) + ' ' + rwF_(x.distPct, 1) + '%'; });
  return rwAlert_('MKT' + rwF_(step, 0), 'MARKET', 1, 'market ' + rwPct_(mkt.chgPct),
    'The ' + RW_.MARKET_NAME + ' is ' + rwPct_(mkt.chgPct) + ' today.' +
    (list.length ? ' Holdings nearest to their stops: ' + list.join(', ') + '.' : ''));
}

/* The state of one New York day: { day, sent: { key: ms }, tried: { key: 1 }, mails, fail }. A new day starts clean;
 * fail (runs in a row without a price) carries over. tried = alerts whose e-mail failed: logged once, sent when it works. */
function rwStateCore_(raw, day) {
  var st = null;
  try { st = JSON.parse(rwTrim_(raw) || 'null'); } catch (e) { st = null; }
  var fail = st && typeof st.fail === 'number' && st.fail > 0 ? st.fail : 0;
  if (!st || st.day !== day || typeof st.sent !== 'object' || st.sent === null) return { day: day, sent: {}, tried: {}, mails: 0, fail: fail };
  return { day: day, sent: st.sent, tried: typeof st.tried === 'object' && st.tried !== null ? st.tried : {},
           mails: typeof st.mails === 'number' && st.mails > 0 ? st.mails : 0, fail: fail };
}

/* The alerts not yet sent today. A NEAR after a CROSSED of the same holding is not news. */
function rwDueCore_(st, alerts) {
  var out = [];
  for (var i = 0; i < (alerts || []).length; i++) {
    var a = alerts[i];
    if (st.sent.hasOwnProperty(a.key)) continue;
    if (a.code === 'NEAR' && st.sent.hasOwnProperty(a.symbol + '|CROSSED')) continue;
    out.push(a);
  }
  return out.sort(function (x, y) { return x.sev - y.sev; });
}

function rwMailCore_(alerts, ctx) {
  var titles = alerts.map(function (a) { return a.title; });
  var subject = 'TFB risk watch: ' + titles.slice(0, 3).join('; ') + (titles.length > 3 ? ' (+' + (titles.length - 3) + ' more)' : '');
  var lines = alerts.map(function (a) { return '- ' + a.line; });
  var foot = ['', 'Levels: the Exit Plan of ' + (ctx.asOf || 'n/a') + ' Riyadh. Prices: Yahoo, read ' + ctx.now + ' Riyadh.',
    'This watch places, changes and cancels no order: any order is yours, at the broker. Each alert is sent once a day.',
    ctx.last ? 'This is e-mail ' + ctx.cap + ' of ' + ctx.cap + ' today: further alerts today are on the ' + RW_.TAB_STATUS + ' tab only.' : '',
    'Not investment advice. Risk watch ' + RW_VERSION + '.'];
  var body = lines.concat(foot).filter(function (s, i) { return s !== '' || i === lines.length; }).join('\n');
  return { subject: subject, body: body };
}

/* ------------------------------------------------------------------ Apps Script services (each fails soft) */
function rwProps_() { return PropertiesService.getScriptProperties(); }

function rwHm_(ms) {
  try { return Utilities.formatDate(new Date(ms), RW_.TZ_RIYADH, 'HH:mm'); } catch (e) { return '?'; }
}

function rwDayOf_(v) {
  if (v instanceof Date) { try { return Utilities.formatDate(v, RW_.TZ_RIYADH, 'yyyy-MM-dd'); } catch (e) { return ''; } }
  var m = /^(\d{4}-\d{2}-\d{2})/.exec(rwTrim_(v));
  return m ? m[1] : '';
}

function rwRecipient_(p) {
  var to = rwTrim_(p.getProperty(RW_.PROP_TO)) || rwTrim_(p.getProperty(RW_.PROP_TO_FALLBACK));
  if (!to) { try { to = rwTrim_(Session.getEffectiveUser().getEmail()); } catch (e) { to = ''; } }
  return to;
}

function rwReadLevels_() {
  var ss = SpreadsheetApp.getActive(), page = [], ledger = [];
  var sp = ss.getSheetByName(RW_.TAB_PAGE);
  if (!sp || sp.getLastRow() < 2) return { ok: false, asOf: '', brokerKnown: false, count: 0, list: [], why: RW_.TAB_PAGE + ' tab missing or empty' };
  page = sp.getRange(1, 1, Math.min(sp.getLastRow(), 60), Math.min(Math.max(sp.getLastColumn(), 1), 16)).getValues();
  var sl = ss.getSheetByName(RW_.TAB_LEDGER);
  if (sl && sl.getLastRow() >= 2) {
    var lastRow = sl.getLastRow(), width = Math.min(Math.max(sl.getLastColumn(), 1), 19);
    var from = Math.max(2, lastRow - RW_.LEDGER_TAIL + 1);
    ledger = sl.getRange(1, 1, 1, width).getValues().concat(sl.getRange(from, 1, lastRow - from + 1, width).getValues());
  }
  return rwHoldingsCore_(page, ledger, rwDayOf_);
}

/* One chart per symbol in one batch; the ones without an answer once more on the second host. { SYMBOL: parsed } */
function rwFetchQuotes_(symbols) {
  var out = {}, left = symbols.slice(), h, i;
  for (h = 0; h < RW_.HOSTS.length && left.length; h++) {
    var reqs = [];
    for (i = 0; i < left.length; i++) {
      reqs.push({ url: rwChartUrl_(RW_.HOSTS[h], rwYahooSymbol_(left[i])), method: 'get', muteHttpExceptions: true, followRedirects: true,
                  headers: { 'User-Agent': 'Mozilla/5.0 (compatible; TFB-RiskWatch/' + RW_VERSION + ')', 'Accept': 'application/json' } });
    }
    var resps = [], again = [];
    try { resps = UrlFetchApp.fetchAll(reqs); } catch (e) { resps = []; }
    for (i = 0; i < left.length; i++) {
      var parsed = { ok: false, why: 'no answer' };
      try {
        if (resps[i]) parsed = resps[i].getResponseCode() === 200 ? rwParseChartCore_(resps[i].getContentText()) : { ok: false, why: 'HTTP ' + resps[i].getResponseCode() };
      } catch (e2) { parsed = { ok: false, why: 'unreadable answer' }; }
      out[left[i]] = parsed;
      if (!parsed.ok) again.push(left[i]);
    }
    left = again;
  }
  return out;
}

function rwLog_(rows) {
  if (!rows.length) return;
  try {
    var ss = SpreadsheetApp.getActive(), sh = ss.getSheetByName(RW_.TAB_LOG);
    if (!sh) { sh = ss.insertSheet(RW_.TAB_LOG); sh.appendRow(['At (Riyadh)', 'Mode', 'Phase', 'Code', 'Symbol', 'Alert', 'E-mail']); }
    for (var i = 0; i < rows.length; i++) sh.appendRow(rows[i]);
  } catch (e) { /* the alert never depends on its log */ }
}

function rwWriteStatus_(head, rows) {
  try {
    var ss = SpreadsheetApp.getActive(), sh = ss.getSheetByName(RW_.TAB_STATUS) || ss.insertSheet(RW_.TAB_STATUS);
    var grid = [['RISK WATCH - every holding against its stop and its target, read every few minutes in US market hours'],
                [head],
                ['It watches and e-mails; it places, changes and cancels no order. Levels come from the Exit Plan. Not investment advice.'],
                [],
                ['Symbol', 'Price', 'Price time (Riyadh)', 'Today %', 'Stop used', 'Stop is', 'Above the stop %', 'Target', 'To the target %', 'State', 'Alerts today']]
      .concat(rows);
    var block = [];
    for (var r = 0; r < RW_.STATUS_ROWS; r++) {
      var src = grid[r] || [], line = [];
      for (var c = 0; c < RW_.STATUS_COLS; c++) line.push(src[c] === undefined || src[c] === null ? '' : src[c]);
      block.push(line);
    }
    sh.getRange(1, 1, RW_.STATUS_ROWS, RW_.STATUS_COLS).setValues(block);
  } catch (e) { /* the alert never depends on its tab */ }
}

/* ------------------------------------------------------------------ the run */
function rwRun_(force) {
  var p, mode;
  try { p = rwProps_(); mode = rwModeCore_(p.getProperty(RW_.PROP_MODE)); } catch (e0) { return 'off (properties not read)'; }
  if (mode === 'off') {
    if (!force) return 'off';
    mode = 'observe';                                   // a manual run with the gate off fills the tab and sends nothing
  }
  var nowMs = new Date().getTime();
  var clock = rwClockCore_(Utilities.formatDate(new Date(nowMs), RW_.TZ_NY, 'yyyy-MM-dd HH:mm'));
  var phase = rwPhaseCore_(clock);
  if (phase === 'idle' && !force) return 'idle';
  var lock = null;
  try { lock = LockService.getScriptLock(); if (!lock.tryLock(500)) return 'busy'; } catch (eL) { lock = null; }
  try {
    return rwRunLocked_(p, mode, phase, clock, nowMs, !!force);
  } finally {
    try { if (lock) lock.releaseLock(); } catch (eR) { /* noop */ }
  }
}

function rwRunLocked_(p, mode, phase, clock, nowMs, force) {
  var lim = rwLimitsCore_(p.getProperty(RW_.PROP_CFG));
  var st = rwStateCore_(p.getProperty(RW_.PROP_STATE), clock.day);
  var stamp = Utilities.formatDate(new Date(nowMs), RW_.TZ_RIYADH, 'yyyy-MM-dd HH:mm');
  var lv;
  try { lv = rwReadLevels_(); } catch (eS) { lv = { ok: false, asOf: '', brokerKnown: false, count: 0, list: [], why: 'workbook not read: ' + String(eS && eS.message || eS).slice(0, 80) }; }
  var alerts = [], health = [];
  // LEVELS: the page is missing, unreadable or old
  var pageAge = null;
  if (lv.ok && lv.asOf) {
    var pm = /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})$/.exec(lv.asOf);
    if (pm) pageAge = (nowMs - Date.UTC(+pm[1], +pm[2] - 1, +pm[3], +pm[4] - 3, +pm[5])) / 86400000;
  }
  if (!lv.ok) health.push(rwAlert_('LEVELS', 'HEALTH', 1, 'the Exit Plan levels are not readable', 'Risk watch cannot read the holdings and their levels: ' + lv.why + '. No price rule can run until it can.'));
  else if (pageAge === null || pageAge > lim.page_max_days) {
    health.push(rwAlert_('LEVELS', 'HEALTH', 1, 'the Exit Plan levels are old', 'Risk watch: the ' + RW_.TAB_PAGE + ' page was last written ' +
      (lv.asOf ? lv.asOf + ' Riyadh (' + rwF_(pageAge, 0) + ' days ago)' : 'at an unknown time') + '. The stops and targets it watches are old.'));
  }
  var watched = [], i;
  for (i = 0; i < lv.list.length; i++) if (rwWatched_(lv.list[i].symbol)) watched.push(lv.list[i]);
  var quotes = {};
  try { quotes = rwFetchQuotes_([RW_.MARKET].concat(watched.map(function (h) { return h.symbol; }))); } catch (eQ) { quotes = {}; }
  var mq = quotes[RW_.MARKET] || { ok: false, why: 'no answer' };
  // PRICES: runs in a row without the market's price
  st.fail = mq.ok ? 0 : st.fail + 1;
  if (st.fail >= lim.fail_runs) {
    health.push(rwAlert_('PRICES', 'HEALTH', 1, 'prices are not readable', 'Risk watch cannot read prices (' + st.fail + ' runs in a row; last answer: ' +
      rwTrim_(mq.why || 'none') + '). No alert can be raised until it can.'));
  }
  var holiday = rwHolidayCore_(clock.day, p.getProperty(RW_.PROP_HOLIDAYS));
  var openMs = Math.floor(nowMs / 60000) * 60000 + (RW_.OPEN_MIN - clock.minutes) * 60000;      // today's open, by this clock
  var mkt = rwMarketCore_(mq.ok ? mq : null, phase, nowMs, lim, holiday, openMs), rows = [], near = [];
  for (i = 0; i < lv.list.length; i++) {
    var h = lv.list[i], q = quotes[h.symbol], mine = [];
    if (!rwWatched_(h.symbol)) {
      rows.push([h.symbol, '', '', '', h.stop === null ? '' : h.stop, 'Exit Plan', '', h.target === null ? '' : h.target, '', 'not watched: listed outside the US', '']);
      continue;
    }
    mine = rwEvaluateCore_(h, q, mkt, phase, lim, nowMs, rwHm_);
    alerts = alerts.concat(mine);
    var atBroker = h.brokerStop !== null && h.brokerStop > 0, ref = h.offer ? null : (atBroker ? h.brokerStop : h.stop);
    var px = q && q.ok ? q.price : (phase === 'pre' ? h.lastClose : null);
    var fresh = !!(q && q.ok) && nowMs - q.priceMs <= lim.fresh_min * 60000 && q.priceMs >= q.regStartMs;
    var dist = px !== null && ref ? (px / ref - 1) * 100 : null;
    if (dist !== null && fresh && phase === 'session') near.push({ symbol: h.symbol, distPct: dist });
    var sentToday = [];
    for (var k in st.sent) if (st.sent.hasOwnProperty(k) && k.indexOf(h.symbol + '|') === 0) sentToday.push(k.split('|')[1]);
    rows.push([h.symbol, px === null ? '' : px, q && q.ok ? Utilities.formatDate(new Date(q.priceMs), RW_.TZ_RIYADH, 'yyyy-MM-dd HH:mm') : (px === null ? '' : 'last close (Exit Plan)'),
      fresh && q.prevClose ? Math.round((px / q.prevClose - 1) * 1000) / 10 : '',
      ref === null ? '' : ref, h.offer ? 'none: cash offer, manual sale' : (atBroker ? 'working at the broker' : (lv.brokerKnown ? 'Exit Plan (no working stop at the broker)' : 'Exit Plan (broker orders not read)')),
      dist === null ? '' : Math.round(dist * 10) / 10, h.target === null ? '' : h.target,
      px !== null && h.target ? Math.round((h.target / px - 1) * 1000) / 10 : '',
      mine.length ? mine.map(function (a) { return a.code; }).join(', ') : (!q || !q.ok ? 'no price (' + rwTrim_(q && q.why || 'no answer') + ')'
        : (phase === 'pre' ? 'before the open: last close' : (fresh ? 'watched' : 'no fresh trade: last price only'))),
      sentToday.concat(mine.filter(function (a) { return !st.sent.hasOwnProperty(a.key); }).map(function (a) { return a.code; })).join(', ')]);
  }
  var ma = rwMarketAlertCore_(mkt, lim, near);
  if (ma) alerts.push(ma);
  var due = rwDueCore_(st, health.concat(alerts));
  // send (on), or only record (observe)
  var note = 'nothing new', logRows = [], sent = false, why = '';
  if (due.length) {
    if (mode === 'on') {
      var to = rwRecipient_(p);
      if (!to) why = 'no recipient - set ' + RW_.PROP_TO;
      else if (st.mails >= lim.mail_cap) why = 'daily cap of ' + lim.mail_cap + ' e-mails reached';
      else {
        var mail = rwMailCore_(due, { asOf: lv.asOf, now: stamp, cap: lim.mail_cap, last: st.mails + 1 >= lim.mail_cap });
        try { MailApp.sendEmail({ to: to, subject: mail.subject, body: mail.body }); sent = true; st.mails += 1; }
        catch (eM) { why = 'e-mail failed: ' + String(eM && eM.message || eM).slice(0, 120); }
      }
    }
    // an alert is marked once it is sent, recorded in observe, or held back by the cap; a failed send is tried again
    var mark = sent || mode === 'observe' || /daily cap/.test(why);
    for (i = 0; i < due.length; i++) {
      if (mark) st.sent[due[i].key] = nowMs;
      else if (st.tried.hasOwnProperty(due[i].key)) continue;              // a failed send is logged once, then tried quietly
      else st.tried[due[i].key] = 1;
      logRows.push([stamp, mode, phase, due[i].code, due[i].symbol, due[i].line, sent ? 'sent' : (mode === 'observe' ? 'observe: not sent' : 'NOT sent: ' + why)]);
    }
    note = due.length + ' alert(s) ' + (sent ? 'sent' : (mode === 'observe' ? 'recorded (observe)' : 'NOT sent: ' + why));
  }
  rwLog_(logRows);
  var head = 'Read ' + stamp + ' Riyadh - mode ' + mode + ' - ' +
    (phase === 'pre' ? 'before the open' : (phase === 'session' ? 'session' : 'outside market hours (manual run)')) + ' - market: ' +
    (mkt.tradingDay ? 'trading' + (mkt.chgPct === null ? '' : ', ' + RW_.MARKET_NAME + ' ' + rwPct_(mkt.chgPct)) : 'not trading (' + (mkt.why || 'closed') + ')') +
    ' - levels: Exit Plan of ' + (lv.asOf || 'n/a') + ' - e-mails today ' + st.mails + ' of ' + lim.mail_cap + ' - ' + note;
  rwWriteStatus_(head, rows);
  try { p.setProperty(RW_.PROP_STATE, JSON.stringify(st)); } catch (eP) { /* the next run reads the old state and may repeat an alert */ }
  return (force && phase === 'idle' ? 'manual' : phase) + ': ' + note;
}

/* ------------------------------------------------------------------ owner functions */
/* Trigger handler (installed by tfbInstallRiskWatchTrigger). */
function rwTick() { return rwRun_(false); }

/* Run once now, also outside market hours: fills the Risk_Watch tab. Outside market hours no price is fresh, so no
 * price rule fires. */
function tfbRiskWatchNow() { return rwNotify_(rwRun_(true)); }

function rwNotify_(msg) {
  try { Logger.log(msg); } catch (eL) { /* noop */ }
  try { SpreadsheetApp.getActive().toast(String(msg).slice(0, 250), 'Risk watch ' + RW_VERSION, 15); } catch (eT) { /* noop */ }
  return msg;
}

function rwRemoveTrigger_() {
  var n = 0, ts = ScriptApp.getProjectTriggers();
  for (var i = 0; i < ts.length; i++) { if (ts[i].getHandlerFunction() === RW_.HANDLER) { ScriptApp.deleteTrigger(ts[i]); n++; } }
  return n;
}

function tfbInstallRiskWatchTrigger() {
  var old = rwRemoveTrigger_();
  var p = rwProps_(), set = '', every = rwEveryCore_(p.getProperty(RW_.PROP_EVERY));
  ScriptApp.newTrigger(RW_.HANDLER).timeBased().everyMinutes(every).create();
  if (!rwTrim_(p.getProperty(RW_.PROP_MODE))) { p.setProperty(RW_.PROP_MODE, 'on'); set = ' (set to on)'; }
  var to = rwRecipient_(p);
  return rwNotify_('Risk watch installed: ' + RW_.HANDLER + ' every ' + every + ' min' + (old ? ' (replaced ' + old + ')' : '') +
    '. ' + RW_.PROP_MODE + ' = ' + rwModeCore_(p.getProperty(RW_.PROP_MODE)) + set + '. E-mails go to ' + (to || 'NOBODY - set ' + RW_.PROP_TO) +
    '. It works on New York weekdays from 08:15 to 16:10 New York time and is idle otherwise.');
}

function tfbRemoveRiskWatchTrigger() {
  var n = rwRemoveTrigger_();
  return rwNotify_(n ? 'Removed ' + n + ' risk-watch trigger(s): no risk e-mail is sent until it is installed again.' : 'No risk-watch trigger was installed.');
}

function tfbRiskWatchStatus() {
  var p = rwProps_(), n = 0, ts = ScriptApp.getProjectTriggers(), quota = -1;
  for (var i = 0; i < ts.length; i++) if (ts[i].getHandlerFunction() === RW_.HANDLER) n++;
  try { quota = MailApp.getRemainingDailyQuota(); } catch (e) { quota = -1; }
  var clock = rwClockCore_(Utilities.formatDate(new Date(), RW_.TZ_NY, 'yyyy-MM-dd HH:mm'));
  var st = rwStateCore_(p.getProperty(RW_.PROP_STATE), clock ? clock.day : '');
  var keys = [];
  for (var k in st.sent) if (st.sent.hasOwnProperty(k)) keys.push(k);
  return rwNotify_('RISK WATCH v' + RW_VERSION + ' | mode=' + rwModeCore_(p.getProperty(RW_.PROP_MODE)) + ' | triggers=' + n +
    ' | to=' + (rwRecipient_(p) || '(nobody)') + ' | phase now=' + rwPhaseCore_(clock) + ' | e-mails today=' + st.mails +
    ' | alerts today=' + (keys.length ? keys.join(', ') : 'none') + ' | runs without a price=' + st.fail + ' | mail quota=' + (quota >= 0 ? quota : 'n/a'));
}

/* One test e-mail to the recipient: proves the address and the permission. */
function tfbRiskWatchTestMail() {
  var to = rwRecipient_(rwProps_());
  if (!to) return rwNotify_('No recipient - set Script Property ' + RW_.PROP_TO + '.');
  MailApp.sendEmail({ to: to, subject: 'TFB risk watch: TEST',
    body: 'This is a test of risk watch ' + RW_VERSION + '. Real alerts look like this, one e-mail per new alert:\n\n' +
      '- EXAMPLE.US: 49.10 now, 0.8% above the stop working at the broker, 48.69. Today -1.3%; ' + RW_.MARKET_NAME + ' -0.2%.\n\n' +
      'This watch places, changes and cancels no order. Not investment advice.' });
  return rwNotify_('Test e-mail sent to ' + to + '.');
}

/* Pure-logic self-test: no network, no sheet, no e-mail, no trigger. */
function tfbRiskWatchSelfTest() {
  var fails = [];
  function ok(cond, name) { if (!cond) fails.push(name); }
  var lim = rwLimitsCore_('');
  ok(rwModeCore_('') === 'off' && rwModeCore_(' ON ') === 'on' && rwModeCore_('observe') === 'observe' && rwModeCore_('junk') === 'off', 'mode');
  ok(rwLimitsCore_('{"near_pct":2,"mail_cap":-1,"x":9}').near_pct === 2 && rwLimitsCore_('{"mail_cap":-1}').mail_cap === 12 && rwLimitsCore_('junk').drop_pct === 4, 'limits');
  ok(rwNum_('1,234.50') === 1234.5 && rwNum_('unknown') === null && rwNum_('') === null && rwNum_(62.67) === 62.67, 'numbers');
  ok(rwEveryCore_('') === 5 && rwEveryCore_('10') === 10 && rwEveryCore_('7') === 5 && rwEveryCore_('1') === 1, 'trigger interval');
  var fri = rwClockCore_('2026-11-27 08:15'), sat = rwClockCore_('2026-11-28 10:00');
  ok(fri.dow === 5 && rwPhaseCore_(fri) === 'pre' && rwPhaseCore_(rwClockCore_('2026-11-27 08:14')) === 'idle', 'pre window');
  ok(rwPhaseCore_(rwClockCore_('2026-11-27 09:30')) === 'session' && rwPhaseCore_(rwClockCore_('2026-11-27 16:10')) === 'session' && rwPhaseCore_(rwClockCore_('2026-11-27 16:11')) === 'idle', 'session window');
  ok(rwPhaseCore_(sat) === 'idle' && rwPhaseCore_(null) === 'idle', 'weekend');
  ok(rwYahooSymbol_('IBOC.US') === 'IBOC' && rwYahooSymbol_('brk.b.us') === 'BRK-B' && rwWatched_('MBIN.US') && !rwWatched_('5023.SR'), 'symbols');
  var now = Date.UTC(2026, 10, 25, 13, 20), open = Date.UTC(2026, 10, 25, 14, 30);
  var mkt = rwMarketCore_(null, 'pre', now, lim, false, open);
  ok(mkt.tradingDay === true && rwMarketCore_(null, 'pre', now, lim, true, open).tradingDay === false, 'before the open: a trading day unless a holiday');
  ok(rwHolidayCore_('2026-11-26', '') && !rwHolidayCore_('2026-11-27', '') && rwHolidayCore_('2026-11-27', '2026-12-24, 2026-11-27'), 'holidays');
  var hm = function () { return '17:30'; };
  var iboc = { symbol: 'IBOC.US', shares: 10, entry: 66.0, lastClose: 63.77, atrPct: 1.9, stop: 62.67, target: 74.8, brokerStop: 62.67, tp1: false, offer: false };
  var q = { ok: true, price: 63.77, priceMs: open - 17.5 * 3600000, prevClose: 63.85, regStartMs: open, regEndMs: open + 6.5 * 3600000, preStartMs: open - 5.5 * 3600000, preLast: null, preMs: 0, preVol: 0 };
  var a = rwEvaluateCore_(iboc, q, mkt, 'pre', lim, now, hm);
  ok(a.length === 1 && a[0].code === 'OPEN_RISK' && /1\.7% under the last close 63\.77/.test(a[0].line) && /No pre-market trade yet/.test(a[0].line), 'the IBOC morning of 25 Nov');
  ok(rwEvaluateCore_({ symbol: 'GLPI.US', lastClose: 39.01, atrPct: 1.6, stop: 35.74, target: 42.0, brokerStop: 35.74, tp1: false, offer: false },
     { ok: true, price: 39.01, priceMs: open - 17.5 * 3600000, regStartMs: open, preLast: null }, mkt, 'pre', lim, now, hm).length === 0, 'a stop 8% away is not open risk');
  var blind = rwEvaluateCore_(iboc, null, mkt, 'pre', lim, now, hm);
  ok(blind.length === 1 && blind[0].code === 'OPEN_RISK' && /The pre-market price could not be read/.test(blind[0].line), 'open risk needs no price feed');
  var s = { ok: true, price: 63.20, priceMs: open + 600000, prevClose: 63.77, regStartMs: open, regEndMs: open + 6.5 * 3600000, preLast: null };
  var sm = { ok: true, tradingDay: true, chgPct: -0.4 };
  ok(rwEvaluateCore_(iboc, s, sm, 'session', lim, open + 660000, hm)[0].code === 'NEAR', 'near');
  s.price = 62.60;
  ok(rwEvaluateCore_(iboc, s, sm, 'session', lim, open + 660000, hm)[0].code === 'CROSSED', 'crossed');
  ok(rwEvaluateCore_(iboc, s, sm, 'session', lim, open + 40 * 60000, hm).length === 0, 'an old price fires nothing');
  ok(rwEvaluateCore_(iboc, s, { ok: true, tradingDay: false, chgPct: null }, 'session', lim, open + 660000, hm).length === 0, 'a closed market fires nothing');
  var st = rwStateCore_('', '2026-11-25');
  var due = rwDueCore_(st, [rwAlert_('NEAR', 'A.US', 2, 't', 'l'), rwAlert_('CROSSED', 'A.US', 1, 't', 'l')]);
  ok(due.length === 2 && due[0].code === 'CROSSED', 'due, most severe first');
  st.sent['A.US|CROSSED'] = 1;
  ok(rwDueCore_(st, [rwAlert_('NEAR', 'A.US', 2, 't', 'l'), rwAlert_('CROSSED', 'A.US', 1, 't', 'l')]).length === 0, 'sent once a day; no NEAR after CROSSED');
  ok(rwStateCore_(JSON.stringify(st), '2026-11-26').sent['A.US|CROSSED'] === undefined, 'a new day starts clean');
  var verdict = fails.length === 0 ? 'risk watch core: ok' : 'risk watch core: FAIL ' + fails.length + ' [' + fails.join('; ') + ']';
  try { Logger.log('[RISK-WATCH v' + RW_VERSION + '] selftest -> ' + verdict); } catch (e) { /* noop */ }
  return verdict;
}
