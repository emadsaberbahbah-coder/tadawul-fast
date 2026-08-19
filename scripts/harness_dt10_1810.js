// v1.8.10 harness — REAL functions extracted from the .gs (no stand-ins):
// dt10FastTrackSuspend_, dt10StabGhost_, dt10MapHeaderCols_ + the pool
// send-map, plus the wired fast-track branch replayed on real code text.
var fs = require('fs');
var src = fs.readFileSync('16_Decision_Top10.gs', 'utf8');

// Extract needed top-level pieces verbatim and eval in this sandbox.
function grab(name) {
  var re = new RegExp('function ' + name + '\\([\\s\\S]*?\\r\\n\\}', '');
  var m = src.match(re);
  if (!m) throw new Error('extract fail: ' + name);
  return m[0];
}
function grabVar(name) {
  var re = new RegExp('var ' + name + ' = \\[[\\s\\S]*?\\r\\n\\];', '');
  var m = src.match(re);
  if (!m) { re = new RegExp('var ' + name + ' = [^;]+;', ''); m = src.match(re); }
  if (!m) throw new Error('var extract fail: ' + name);
  return m[0];
}
eval(grabVar('DT10_POOL_FIELDS'));
eval(grabVar('DT10_V1810_FASTTRACK_SIZING_SUSPEND'));
eval(grab('dt10NormToken_'));
eval(grab('dt10MapHeaderCols_'));
eval(grab('dt10FastTrackSuspend_'));
eval(grab('dt10StabGhost_'));

var pass = 0, fail = 0;
function check(label, cond, detail) {
  if (cond) { pass++; console.log('  PASS  ' + label); }
  else { fail++; console.log('  FAIL  ' + label + '  ' + (detail || '')); }
}

// T1: suspension semantics on a src-copied ticket
var tk = { symbol: 'ZZZ.US', name: 'Z', price: 10, price_sar: 37.5,
  entry_zone: '9.8-10.1', suggested_sar: 19000, suggested_shares: 506,
  stop_sar: 9.0, tp1_sar: 11.5, tp2_sar: 12.4, exp_gain_12m_sar: 4400,
  roi_pct: 24, engine_roi_pct: 3.1, ann_roi_pct: 24, reliability: 71,
  dq: 88, rank: 7, advisor_note: 'builder note' };
dt10FastTrackSuspend_(tk);
check('T1 sizing fields all suspended to em-dash',
  ['entry_zone','suggested_sar','suggested_shares','stop_sar','tp1_sar','tp2_sar','exp_gain_12m_sar']
    .every(function(k){ return tk[k] === '\u2014'; }));
check('T1 identity/context preserved (price/roi/rel/dq/rank live)',
  tk.price === 10 && tk.roi_pct === 24 && tk.reliability === 71 && tk.dq === 88 && tk.rank === 7);
check('T1 advisor note states withholding + not-executable',
  /sizing suspended under strict/.test(tk.advisor_note) && /not an executable ticket/.test(tk.advisor_note));
check('T1 class marker _ft_suspended set', tk._ft_suspended === true);

// T2: em-dash vocabulary identical to ghost path
var ghost = dt10StabGhost_({ symbol:'G', name:'g' }, 1, 3);
check('T2 same suspension vocabulary as grace ghost', ghost.suggested_sar === tk.suggested_sar
  && ghost.stop_sar === tk.stop_sar && ghost.tp1_sar === tk.tp1_sar);

// T3: branch condition replay — the EXACT wired condition text from source
var branch = src.match(/if \(DT10_V1810_FASTTRACK_SIZING_SUSPEND && src &&[\s\S]*?dt10FastTrackSuspend_\(tk\);/);
check('T3 branch present, gated on toggle+src+knobs.hard_strict===true',
  !!branch && /knobs && knobs\.hard_strict === true/.test(branch[0]));
function branchFires(toggle, hasSrc, knobs) {
  var DT = toggle, s = hasSrc ? {} : null;
  return !!(DT && s && knobs && knobs.hard_strict === true);
}
check('T3a fires: toggle+src+strict', branchFires(true, true, {hard_strict:true}) === true);
check('T3b silent: strict disarmed', branchFires(true, true, {hard_strict:false}) === false);
check('T3c silent: toggle off (v1.8.9 restore)', branchFires(false, true, {hard_strict:true}) === false);
check('T3d silent: ghost path (no src)', branchFires(true, false, {hard_strict:true}) === false);
check('T3e default toggle is ON (protective)', DT10_V1810_FASTTRACK_SIZING_SUSPEND === true);

// T4: Warnings send-map entry, exact-token mapper picks it up
var wf = null;
for (var i = 0; i < DT10_POOL_FIELDS.length; i++)
  if (DT10_POOL_FIELDS[i].send === 'Warnings') wf = DT10_POOL_FIELDS[i];
check('T4 send-map has Warnings->[warnings]', !!wf && wf.match.length === 1 && wf.match[0] === 'warnings');
var hdr = ['Symbol','Name','Data Quality Score','Warnings','Forecast Reliability Score'];
var map = dt10MapHeaderCols_(hdr);
check('T4 mapper resolves Warnings col', map['Warnings'] === 3);
check('T4 mapper DQ col unchanged', map['Data Quality Score'] === 2);

console.log('RESULT: ' + pass + ' passed, ' + fail + ' failed');
process.exit(fail ? 1 : 0);
