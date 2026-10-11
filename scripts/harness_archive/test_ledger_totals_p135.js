// P-135 harness — pure-helper totals vs today's real ledger export, x3
const fs = require('fs');
const vm = require('vm');
const crypto = require('crypto');

function loadGs(path) {
  const ctx = { Object, String, Number, Math, Date, isNaN, JSON, console };
  vm.createContext(ctx);
  vm.runInContext(fs.readFileSync(path, 'utf8'), ctx, { filename: path });
  return ctx;
}

// Parse today's real _Portfolio_CostBasis TSV into PL-column-order rows
function parseTsv(path) {
  const lines = fs.readFileSync(path, 'utf8').split(/\r?\n/);
  const rows = [];
  for (let i = 4; i < lines.length; i++) {           // data starts row 5
    const cells = lines[i].split('\t');
    if (!cells[0] || !cells[0].trim()) continue;
    const row = cells.slice(0, 23).map((c, j) => {
      const s = c.trim();
      if (s === '' || s === '\u2014') return s === '' ? '' : '\u2014';
      // display numbers: (82) -> -82 ; 1,234 -> 1234 ; percents left as text
      if (/^\(?-?[0-9.,]+\)?$/.test(s)) {
        let neg = s.startsWith('(') && s.endsWith(')');
        let n = Number(s.replace(/[(),]/g, ''));
        return isNaN(n) ? s : (neg ? -n : n);
      }
      return s;
    });
    while (row.length < 23) row.push('');
    rows.push(row);
  }
  return rows;
}

const TSV = '/mnt/user-data/uploads/_Market_Share_Deepseek-V3_-__Portfolio_CostBasis__1_.tsv';

function onePass() {
  const base = loadGs('ledger_base.gs');     // v1.4.1 — must NOT have the fn
  const fixd = loadGs('ledger_v150.gs');     // v1.5.0
  const out = {};
  out.base_lacks_fn = (typeof base.plTotalsFromGrid_ === 'undefined');
  const rows = parseTsv(TSV);
  out.rows = rows.length;
  const t = fixd.plTotalsFromGrid_(rows);
  out.totals = { realizedClosed: Math.round(t.realizedClosed),
                 activeUnrl: Math.round(t.activeUnrl),
                 activeTot: Math.round(t.activeTot),
                 lifetime: Math.round(t.lifetime),
                 nClosed: t.nClosed, nActive: t.nActive };
  // Golden assertions vs independent Python sums from the same export
  const A = out.totals;
  const ok = (x, y) => Math.abs(x - y) <= 2;   // display-rounding tolerance
  if (!(ok(A.realizedClosed, 1075) && ok(A.activeUnrl, 75) &&
        ok(A.activeTot, 880) && ok(A.lifetime, 1955) &&
        A.nClosed === 30 && A.nActive === 7)) {
    throw new Error('GOLDEN MISMATCH ' + JSON.stringify(A));
  }
  // Golden-negative: orphaned manual values are NOT what the truth sums to
  if (ok(A.realizedClosed, 2236) || ok(A.lifetime, 3174) ||
      ok(A.realizedClosed, 1157) || ok(A.lifetime, 2094)) {
    throw new Error('orphan values unexpectedly matched');
  }
  // Edge battery on synthetic grid
  const g = [
    ['X', 'x', 'USD', 'Active', '', 1, 1, 0, '', '', '', 0, '', 10, 11, 3.75, 41, 3.75, '', '', 7.5, 0.75, 5],
    ['Y', 'y', 'USD', 'Inactive', '', 1, 1, 0, '', '', '', 0, '', 10, '\u2014', 3.75, '\u2014', '\u2014', 12, -5, -5, -0.5, 9],
    ['', '', '', 'Active', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 99, '', ''],   // blank sym -> skipped
    ['Z', 'z', 'USD', 'Inactive', '', 1, 1, 0, '', '', '', 0, '', 10, '', 3.75, '', '\u2014', 12, '\u2014', '\u2014', '', ''], // pending: REAL dash -> not counted
  ];
  const e = fixd.plTotalsFromGrid_(g);
  if (!(e.nActive === 1 && e.nClosed === 1 && e.realizedClosed === -5 &&
        e.activeTot === 7.5 && e.activeUnrl === 3.75 && e.lifetime === 2.5)) {
    throw new Error('EDGE MISMATCH ' + JSON.stringify(e));
  }
  out.edge = e;
  return out;
}

const digests = [];
for (let i = 0; i < 3; i++) {
  const r = onePass();
  const d = crypto.createHash('sha256').update(JSON.stringify(r)).digest('hex').slice(0, 16);
  digests.push(d);
  if (i === 0) console.log(JSON.stringify(r, null, 1));
  console.log('pass', i + 1, 'digest', d);
}
if (new Set(digests).size !== 1) throw new Error('digest drift');
console.log('HARNESS PASS x3, digest', digests[0]);
