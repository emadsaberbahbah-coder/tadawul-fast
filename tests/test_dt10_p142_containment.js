/* T1-T7 harness for 16_Decision_Top10.gs v1.11.8 [P-142]
 * REAL-MODULE discipline: the four pure functions are extracted verbatim
 * from the DELIVERED file and eval'd — no re-implementation, no stand-ins.
 * Fixtures are the verbatim strings from the 2026-09-15 08:08 live board.
 * Run three times; digests must be identical.                       ES5. */
var fs = require('fs');
var crypto = require('crypto');
var src = fs.readFileSync(process.argv[2] || '16_Decision_Top10.gs', 'utf8');

function extract(name) {
  var i = src.indexOf('function ' + name + '(');
  if (i < 0) throw new Error('missing ' + name);
  var j = src.indexOf('\r\n}\r\n', i);
  if (j < 0) throw new Error('unterminated ' + name);
  return src.slice(i, j + 3);
}
var code = ['dt10IsFundingAlert_', 'dt10IsFundingNearMiss_',
            'dt10ContainFundingCore_'].map(extract).join('\n');
eval(code);

/* ---- REAL fixtures: 2026-09-15 08:08 board (verbatim cells) ---- */
function alertsFx() { return [
  { type: 'missing_fx', count: 1,
    required_action: 'Add FX rate(s) to _Lists_Config: Global' },
  { type: 'missing_valuation', count: 3288,
    required_action: 'No target/intrinsic value \u2014 check engine forecast coverage for these rows' },
  { type: 'unfunded_candidates', count: 3,
    required_action: 'These name(s) passed every gate and ranked, but deployable capital was exhausted before funding \u2014 increase Cash Available (or reduce Max Selected). Shown as WATCH, not executable tickets.' },
  { type: 'capital_call', count: 3,
    required_action: 'Deposit \u2265 37,650 SAR to take the top-3 qualified ticket(s): IGG.L, GBCI.US, VNOM.US. Qualified names are shown regardless of cash; funding is the operator\u2019s decision.' }]; }
function nmFx() {
  var f = function (s) { return {
    symbol: s, failed_gate: 'Funding',
    current: '0 SAR (capital exhausted)',
    required: 'CAPITAL_CALL: deposit \u2265 5,000 SAR for a 5,000 SAR ticket (cash 50 SAR)',
    verdict: 'WATCH',
    improve_note: 'deployable capital to fund \u2265 1 lot' }; };
  var d = function (s) { return {
    symbol: s, failed_gate: 'Diversification',
    current: 'Diversification: sector cap 2/2 (Financials)',
    required: 'within sector/market caps', verdict: 'INVEST',
    improve_note: 'Qualified (INVEST) \u2014 deferred by diversification cap' }; };
  return [f('GBCI.US'), f('VNOM.US'), f('IBOC.US'),
          d('HBAN.US'), d('VLY.US'), d('AUB.US'), d('RNST.US'), d('MCB.US'),
          d('FBK.US'), d('KEY.US'), d('PNFP.US'), d('CBAN.US'), d('PDLB.US')];
}
function snap(x) { return JSON.stringify(x); }

function runOnce() {
  var out = [];
  var A = alertsFx(), N = nmFx();
  var a0 = snap(A), n0 = snap(N);

  /* T1 WITHHELD + containment: 2 alerts + 3 near-miss suppressed = 5 */
  var w = dt10ContainFundingCore_(N, A, 'WITHHELD', false);
  var t1 = w.suppressed === 5 &&
    w.alerts.length === 3 &&
    w.alerts[0].type === 'missing_fx' &&
    w.alerts[1].type === 'missing_valuation' &&
    w.alerts[2].type === 'funding_withheld' && w.alerts[2].count === 5 &&
    w.nearMiss.length === 13 &&
    w.nearMiss[0].symbol === 'GBCI.US' &&
    w.nearMiss[0].current === '\u2014' &&
    w.nearMiss[0].required.indexOf('WITHHELD \u2014') === 0 &&
    w.nearMiss[2].required.indexOf('WITHHELD \u2014') === 0 &&
    w.nearMiss[0].verdict === 'WATCH' &&
    w.nearMiss[3].required === 'within sector/market caps' &&
    w.nearMiss[12].improve_note.indexOf('deferred') !== -1;
  out.push(['T1', t1, w.suppressed, w.alerts.length]);

  /* T2 EXECUTABLE: original references returned untouched */
  var e = dt10ContainFundingCore_(N, A, 'EXECUTABLE', false);
  out.push(['T2', e.suppressed === 0 && e.alerts === A && e.nearMiss === N]);

  /* T3 HELD (deliberate scope cut): untouched */
  var h = dt10ContainFundingCore_(N, A, 'HELD', false);
  out.push(['T3', h.suppressed === 0 && h.alerts === A && h.nearMiss === N]);

  /* T4 kill switch on a WITHHELD board: untouched */
  var k = dt10ContainFundingCore_(N, A, 'WITHHELD', true);
  out.push(['T4', k.suppressed === 0 && k.alerts === A && k.nearMiss === N]);

  /* T5 empty inputs on WITHHELD: no disclosure row invented */
  var z = dt10ContainFundingCore_([], [], 'WITHHELD', false);
  out.push(['T5', z.suppressed === 0 && z.alerts.length === 0 &&
                  z.nearMiss.length === 0]);

  /* T6 purity: T1 never mutated the inputs */
  out.push(['T6', snap(A) === a0 && snap(N) === n0]);

  /* T7 idempotence: containing the contained output changes nothing */
  var w2 = dt10ContainFundingCore_(w.nearMiss, w.alerts, 'WITHHELD', false);
  out.push(['T7', w2.suppressed === 0 &&
                  snap(w2.alerts) === snap(w.alerts) &&
                  snap(w2.nearMiss) === snap(w.nearMiss)]);

  var pass = out.every(function (r) { return r[1] === true; });
  return { pass: pass, out: out,
           digest: crypto.createHash('sha256').update(snap(out) + snap(w))
                         .digest('hex').slice(0, 16) };
}

var runs = [runOnce(), runOnce(), runOnce()];
runs.forEach(function (r, i) {
  console.log('run', i + 1, r.pass ? 'PASS' : 'FAIL', 'digest', r.digest,
              JSON.stringify(r.out));
});
var same = runs[0].digest === runs[1].digest &&
           runs[1].digest === runs[2].digest;
console.log(same && runs.every(function (r) { return r.pass; })
            ? 'T1-T7 PASS x3 identical digest ' + runs[0].digest
            : 'HARNESS FAIL');
if (!(same && runs.every(function (r) { return r.pass; }))) process.exit(1);
