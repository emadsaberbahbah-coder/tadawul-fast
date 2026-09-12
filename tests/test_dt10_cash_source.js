// 16_Decision v1.11.7 N-harness — extracts the four REAL new functions from
// the delivered file and runs them in node with a PropertiesService stub.
// N1 mode reader | N2 scan on the LIVE exported PF row shape + negatives
// N3 choose(): panel byte-identical, observe log-only, portfolio switch,
// unreadable fallback | N4 payload-equivalence: mode 'panel' yields the
// exact legacy value. Run x3, identical digest.
const fs = require("fs"), crypto = require("crypto");
const src = fs.readFileSync("16_Decision_Top10.gs", "utf8");
function extract(name) {
  const i = src.indexOf("function " + name);
  if (i < 0) throw new Error("missing " + name);
  let d = 0, j = src.indexOf("{", i);
  for (let k = j; k < src.length; k++) {
    if (src[k] === "{") d++;
    else if (src[k] === "}") { d--; if (!d) return src.slice(i, k + 1); }
  }
  throw new Error("unbalanced " + name);
}
let PROPS = {};
global.PropertiesService = { getScriptProperties: () => ({ getProperty: k => PROPS[k] || null }) };
for (const f of ["dt10CashSourceMode_", "dt10CashScan_", "dt10CashChoose_"]) eval(extract(f));

// N1
PROPS = {}; if (dt10CashSourceMode_() !== "panel") throw "N1 default";
PROPS = { DT10_CASH_SOURCE: "OBSERVE" }; if (dt10CashSourceMode_() !== "observe") throw "N1 observe";
PROPS = { DT10_CASH_SOURCE: "portfolio" }; if (dt10CashSourceMode_() !== "portfolio") throw "N1 portfolio";
PROPS = { DT10_CASH_SOURCE: "junk" }; if (dt10CashSourceMode_() !== "panel") throw "N1 junk";
console.log("N1 PASS  mode reader: default/junk->panel, observe/portfolio bind (case-insensitive)");

// N2 — the LIVE exported Portfolio_Decision rows (row 5 shape verbatim)
const live = [
  ["Portfolio Decision — Actions & Rebalancing"], [],
  ["Review Date", "2026-09-12", "", "Rebalance Mode", "Advisory"], [],
  ["PF: Cash Available SAR", "23,242.50", "", "PF: Target Cash %", "10.0", "", "PF: Max Position %", "20.0"],
];
const hit = dt10CashScan_(live);
if (!(hit.found && hit.value === 23242.5 && hit.row === 5)) throw "N2 live " + JSON.stringify(hit);
if (dt10CashScan_([["Cash (SAR)", "123"]]).found) throw "N2 kpi-header must NOT match";
if (dt10CashScan_([]).found || dt10CashScan_([["PF: Cash Available SAR"]]).found) throw "N2 negatives";
console.log("N2 PASS  scan: live PF row -> 23242.5 @ row5; KPI header 'Cash (SAR)' rejected; edge negatives clean");

// N3
const legacy = Number("9026") || 0;
let c = dt10CashChoose_("panel", legacy, { found: true, value: 23242.5 });
if (!(c.sent === 9026 && c.note === "")) throw "N3 panel";
c = dt10CashChoose_("observe", 9026, { found: true, value: 23242.5 });
if (!(c.sent === 9026 && /observe/.test(c.note) && /23243/.test(c.note) && /\u0394/.test(c.note))) throw "N3 obs " + c.note;
c = dt10CashChoose_("portfolio", 9026, { found: true, value: 23242.5 });
if (!(c.sent === 23242.5 && /cash=PF 23243/.test(c.note) && /panel 9026/.test(c.note))) throw "N3 pf " + c.note;
c = dt10CashChoose_("portfolio", 9026, { found: false, err: "sheet-missing" });
if (!(c.sent === 9026 && /PF unreadable: sheet-missing/.test(c.note))) throw "N3 fb " + c.note;
console.log("N3 PASS  choose: panel byte-identical (note ''), observe log-only with \u0394, portfolio switches, unreadable falls back");

// N4 payload equivalence
const panelVal = Number("9026") || 0;
const chosen = dt10CashChoose_(dt10CashSourceMode_(), panelVal, null);
PROPS = {};
const chosenDefault = dt10CashChoose_(dt10CashSourceMode_(), panelVal, null);
if (chosenDefault.sent !== panelVal || chosenDefault.note !== "") throw "N4";
console.log("N4 PASS  default-mode payload value === legacy Number(panel)||0 exactly");

console.log("RUN-DIGEST", crypto.createHash("sha256")
  .update(JSON.stringify([hit, c.note])).digest("hex").slice(0, 16));
