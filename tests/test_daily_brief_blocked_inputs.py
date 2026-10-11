"""The brief preserves withheld decisions and never invents missing book totals."""
import datetime as dt
import io
import json
from pathlib import Path
import shutil
import subprocess

import pytest

from scripts import run_daily_brief as brief


WHEN = dt.datetime(2026, 10, 11, 8, 0)
HEADER = ["Action", "Symbol", "Name", "P&L %", "Weight %", "Engine ROI %",
          "Rel", "Conf", "Advisor Note", "MV SAR", "Cost SAR"]


def row(action, symbol, mv=1_100, cost=1_000, note=""):
    return [action, symbol, "Synthetic holding", None, None, None, None,
            "", note, mv, cost]


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("TFB_BRIEF_NEWS_CONTEXT", "0")
    monkeypatch.setenv("TFB_MIN_TICKET_SAR", "0")


def model(rows):
    return brief.build_model({brief.PAGE_DECISION: rows})


@pytest.mark.parametrize("rows", [[], [["unreadable decision page"]], [HEADER]])
def test_empty_decision_remains_unavailable_and_renders(rows):
    data = model(rows)
    decision = data["decision"]
    assert decision["mv"] is decision["cost"] is decision["pl_pct"] is None
    text = brief.render_text(data, "Synthetic owner", WHEN)
    html = brief.render_html(data, "Synthetic owner", WHEN)
    assert "Portfolio decision unavailable" in text
    assert "Portfolio decision unavailable" in html
    assert "unavailable vs unavailable SAR" in text
    assert "BOOK: 0 SAR" not in text


@pytest.mark.parametrize("blocked_first", [True, False])
def test_blocked_row_does_not_end_holdings_or_join_executable_groups(blocked_first):
    blocked = row("BLOCK", "BLOCKED.US", mv=None, cost=None,
                  note="RECONCILIATION_REQUIRED: quantity_mismatch; old amount ~9,999 SAR")
    kept = row("EXIT", "EXIT.US", note="Full exit ~300 SAR")
    rows = [blocked, kept] if blocked_first else [kept, blocked, row("HOLD", "HELD.US")]
    decision = brief.extract_decision([HEADER] + rows + [["next section"]])
    assert [r["symbol"] for r in decision["block"]] == ["BLOCKED.US"]
    assert [r["symbol"] for r in decision["sell"]] == ["EXIT.US"]
    assert [r["symbol"] for r in decision["hold"]] == ([] if blocked_first else ["HELD.US"])
    assert decision["block"][0]["sar"] == 0
    assert decision["freed_cash"] == 300
    assert decision["mv"] is decision["cost"] is decision["pl_pct"] is None


@pytest.mark.parametrize("missing", [None, "", "n/a", "NaN", "Infinity"])
@pytest.mark.parametrize("column", [9, 10])
def test_missing_value_withholds_total_instead_of_summing_partial_book(missing, column):
    incomplete = row("HOLD", "INCOMPLETE.US")
    incomplete[column] = missing
    decision = brief.extract_decision([HEADER, row("HOLD", "COMPLETE.US"), incomplete])
    missing_key, known_key = ("mv", "cost") if column == 9 else ("cost", "mv")
    assert decision[missing_key] is None
    assert decision[known_key] == (2_000 if known_key == "cost" else 2_200)
    assert decision["pl_pct"] is None
    text = brief.render_text(model([HEADER, incomplete]), "Synthetic owner", WHEN)
    assert "Book totals/P&L unavailable" in text


def test_complete_book_preserves_real_zero_and_percentage():
    decision = brief.extract_decision([HEADER, row("HOLD", "ZERO.US", mv=0, cost=1_000)])
    assert decision["mv"] == 0
    assert decision["cost"] == 1_000
    assert decision["pl_pct"] == -100
    text = brief.render_text(model([HEADER, row("HOLD", "GAIN.US")]), "Synthetic owner", WHEN)
    assert "BOOK: +10.0% overall (1,100 vs 1,000 SAR)" in text
    assert "unavailable" not in text


def test_held_blocked_symbol_cannot_be_a_new_buy():
    blocked = row("BLOCK", "BLOCKED.US", mv=None, cost=None,
                  note="RECONCILIATION_REQUIRED: position_closed")
    data = brief.build_model({
        brief.PAGE_DECISION: [HEADER, blocked],
        brief.PAGE_TOP10: [
            ["Rank", "Symbol", "Name", "ROI %", "Rel", "Conf", "Advisor Note"],
            [1, "BLOCKED.US", "Synthetic holding", 20, 80, "High", "INVEST"],
            [2, "NEW.US", "Synthetic candidate", 15, 80, "High", "INVEST"],
        ],
    })
    assert [r["symbol"] for r in data["top10"]["top"]] == ["NEW.US"]
    assert not data["decision"]["hold"]
    assert not data["decision"]["add"]


def test_blocked_evidence_is_preserved_on_html_text_and_pdf():
    data = model([HEADER, row("BLOCK", "BLOCKED.US", mv=None, cost=None,
                             note="RECONCILIATION_REQUIRED: quantity_mismatch")])
    html = brief.render_html(data, "Synthetic owner", WHEN)
    text = brief.render_text(data, "Synthetic owner", WHEN)
    assert "Review/reconcile portfolio inputs before acting" in html
    assert "No portfolio action required today" not in html
    assert "RECONCILIATION_REQUIRED: quantity_mismatch" in html
    assert "BLOCK (input review/reconciliation required - do NOT execute)" in text
    assert "RECONCILIATION_REQUIRED: quantity_mismatch" in text
    assert "  BLOCK: 1 - BLOCKED.US" in text
    assert "  HOLD:" not in text
    assert "1 holdings" in html
    assert "Portfolio execution withheld" in html
    assert ">Act now " not in html
    glance = brief._glance_block(data["decision"])
    assert "BLOCKED.US" in glance
    assert "unavailable" in glance
    assert "largest move" not in glance

    pytest.importorskip("reportlab")
    pdf = brief.render_pdf(data, "Synthetic owner", WHEN)
    assert pdf is not None
    assert pdf.startswith(b"%PDF-")
    try:
        from pypdf import PdfReader
    except ImportError:
        if not shutil.which("pdftotext"):
            return  # PDF generation is checked even without an extraction tool.
        result = subprocess.run(["pdftotext", "-", "-"], input=pdf,
                                capture_output=True, check=True)
        pdf_text = result.stdout.decode("utf-8")
    else:
        pdf_text = "\n".join(p.extract_text() for p in PdfReader(io.BytesIO(pdf)).pages)
    assert "unavailable SAR" in pdf_text
    assert "review/reconciliation" in pdf_text
    assert "RECONCILIATION_REQUIRED: quantity_mismatch" in pdf_text
    assert "BLOCKED.US" in pdf_text


@pytest.fixture(scope="module")
def cockpit_layouts():
    """Generate real headers, final output, title and rows from deployed GAS."""
    if not shutil.which("node"):
        pytest.skip("Node is required to execute the deployed cockpit fixture")
    source_path = Path(__file__).resolve().parents[1] / "apps_script/16_Decision_Top10.gs"
    script = r"""
const fs = require('node:fs'), vm = require('node:vm');
const ctx = {Logger: {log() {}}, PropertiesService: {getScriptProperties() {
  return {getProperty() {return null;}};
}}, Utilities: {formatDate() {return '2026-10-11 08:00:00';}},
Session: {getScriptTimeZone() {return 'Asia/Riyadh';}}};
vm.createContext(ctx); vm.runInContext(fs.readFileSync(process.argv[1], 'utf8'), ctx);
const layouts = {};
for (const mode of ['EXECUTABLE', 'HELD', 'WITHHELD', 'QUALIFIED_UNFUNDED']) {
  const t = {rank: 1, symbol: 'SELECTED.US', name: 'Synthetic selected',
    market: 'US', sector: 'Energy', currency: 'USD', price: 100, price_sar: 375,
    fx_to_sar: 3.75, entry_zone: '100', stop_sar: 90, tp1_sar: 120, tp2_sar: 130,
    roi_pct: 20, engine_roi_pct: 25, ann_roi_pct: 80, reliability: 80,
    confidence_band: 'High', suggested_sar: 3750, suggested_shares: 10,
    advisor_note: 'INVEST — synthetic allocated ticket', _stab_status: 'ACTIVE (day 3)',
    detail: {funds_from: 'Cash'}};
  if (mode === 'HELD') {t._ft_suspended = true; t._stab_status = 'FAST-TRACK (day 1)';}
  const c = {symbol: 'AUDIT.US', name: 'Synthetic audit', verdict: 'INVEST',
    market: 'US', sector: 'Energy', roi_pct: 30, reliability: 80,
    confidence_band: 'High', opportunity_score: 90, selected: false};
  const p = {version: 'synthetic-version', selected: mode === 'QUALIFIED_UNFUNDED' ? [] : [t],
    candidates_rows: [c], kpis: {passed: 1},
    _dt10_uv: {state: mode === 'WITHHELD' ? 'NOT_ACTIONABLE' : 'EXECUTABLE', reason: 'synthetic veto'},
    meta: {board_funding: {finalized: true, stage: 'allocate', contract_version: 1,
      allocation_version: 'synthetic-version', eligible_symbols: ['SELECTED.US']}}};
  ctx.dt10FinalizeBoard_(p);
  const output = ctx.dt10OutputStatus_(p);
  const selected = p.selected.map(ctx.dt10TicketToRow_);
  if (mode === 'WITHHELD') selected.forEach(r => ctx.dt10UvWithholdRow_(r,
    ctx.DT10_UV_BOARD_WITHHOLD_IDX, ctx.DT10_UV_BOARD_NOTE_IDX, 'synthetic veto'));
  layouts[mode] = [['TOP 10 INVESTMENTS — DECISION'],
    ['Status:', ctx.dt10StatusLine_('ok', '', output)], [],
    [ctx.dt10SelectedTitle_(p, true)], ctx.DT10_SELECTED_HEADERS,
    ...(selected.length ? selected : [['No qualifying opportunities']]), [],
    ['ALL QUALIFIED — INVEST opportunity set (1)'], ctx.DT10_QUAL_HEADERS,
    ctx.dt10QualToRow_(c, 1, {}, 1, p.selected.length, {})];
}
process.stdout.write(JSON.stringify(layouts));
"""
    result = subprocess.run(["node", "-e", script, str(source_path)],
                            capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


@pytest.mark.parametrize("state", ["HELD", "WITHHELD", "QUALIFIED_UNFUNDED"])
def test_real_final_cockpit_output_vetoes_research_candidates(cockpit_layouts, state):
    top10 = brief.extract_top10(cockpit_layouts[state])
    assert top10["top"] == [] and top10["rest"] == {}
    assert "withheld_reason" in top10
    data = brief.build_model({brief.PAGE_TOP10: cockpit_layouts[state]})
    html = brief.render_html(data, "Synthetic owner", WHEN)
    text = brief.render_text(data, "Synthetic owner", WHEN)
    assert "New opportunities withheld" in html
    assert "WHERE YOUR DRY POWDER GOES" not in html
    assert ">Best new buys " not in html
    assert "BEST NEW BUYS" not in text
    assert "AUDIT.US" not in html


def test_real_executable_cockpit_reads_only_selected_section(cockpit_layouts):
    top10 = brief.extract_top10(cockpit_layouts["EXECUTABLE"])
    assert [r["symbol"] for r in top10["top"]] == ["SELECTED.US"]
    assert top10["rest"] == {}
    assert top10["top"][0]["stop"] == 90
    assert top10["top"][0]["roi"] == 20


@pytest.mark.parametrize("note", ["DO_NOT_INVEST", "DO NOT INVEST — bad price", "NOT INVEST",
                                  "NO INVEST", "INVESTIGATION ongoing", "REINVEST", "Research INVEST",
                                  "INVEST — SIZING WITHHELD", "INVEST — research only", "INVEST — NOT_ACTIONABLE"])
def test_invest_substrings_and_negations_are_never_buy_candidates(note):
    rows = [["Rank", "Symbol", "Name", "Advisor Note"], [1, "NEGATIVE.US", "Synthetic", note]]
    assert brief.extract_top10(rows) == {"top": [], "rest": {}}


@pytest.mark.parametrize("note", ["INVEST", "INVEST — synthetic ticket", "⚠ earnings ≤10d · INVEST — synthetic ticket"])
def test_positive_invest_token_remains_compatible(note):
    rows = [["Rank", "Symbol", "Name", "Advisor Note"], [1, "POSITIVE.US", "Synthetic", note]]
    assert [r["symbol"] for r in brief.extract_top10(rows)["top"]] == ["POSITIVE.US"]


def test_audit_header_cannot_replace_missing_selected_section():
    rows = [["ALL QUALIFIED — INVEST opportunity set (1)"],
            ["Rank", "Symbol", "Name", "Advisor Note"],
            [1, "AUDIT.US", "Synthetic", "INVEST"]]
    top10 = brief.extract_top10(rows)
    assert top10["top"] == [] and top10["rest"] == {}


@pytest.mark.parametrize("spacer", [[], ["Rank", "Symbol", "Name", "Advisor Note"]])
def test_selected_table_boundary_prevents_later_invest_rows(spacer):
    rows = [["SELECTED — EXECUTABLE TICKETS (1)"],
            ["Rank", "Symbol", "Name", "Advisor Note"],
            [1, "SELECTED.US", "Synthetic", "INVEST"], spacer,
            [2, "AUDIT.US", "Synthetic", "INVEST"]]
    assert [r["symbol"] for r in brief.extract_top10(rows)["top"]] == ["SELECTED.US"]


FINAL_FEED_VETO_CASES = ["NOT_ACTIONABLE(final_publication) | run=90001 | synthetic audit failure",
                        "WITHHELD | synthetic final receipt"]


def _model_with_final_feed_veto(cockpit_layouts, feed):
    # Full _Status A:M layout as read by the brief; the final publisher owns L:M.
    status = [["Page", "Last Updated", "Status", "Message"],
              ["Top_10_Investments", "2026-10-11 08:00:00", "OK", "old executable page",
               "", "", "", "", "", "", "", "TFB Decision Feed", feed]]
    return brief.build_model({
        brief.PAGE_STATUS: status,
        brief.PAGE_TOP10: cockpit_layouts["EXECUTABLE"],
        brief.PAGE_DECISION: [HEADER,
            row("EXIT", "EXIT.US", note="Full exit ~500 SAR"),
            row("TRIM", "TRIM.US", note="Trim ~200 SAR"),
            row("ADD", "ADD.US", note="ADD 10 sh ~700 SAR")],
    })


@pytest.mark.parametrize("feed", FINAL_FEED_VETO_CASES)
def test_final_status_feed_veto_overrides_older_executable_plan(cockpit_layouts, feed):
    data = _model_with_final_feed_veto(cockpit_layouts, feed)
    decision = data["decision"]
    assert brief.PAGE_STATUS in brief.ALL_PAGES
    assert decision["execution_veto"] == feed
    assert decision["sell"] == decision["trim"] == decision["add"] == []
    assert decision["freed_cash"] == 0
    assert len(decision["block"]) == 3
    assert all(r["sar"] == 0 and r["stop"] is None and r["shares"] is None for r in decision["block"])
    assert data["top10"]["top"] == [] and data["top10"]["rest"] == {}
    html = brief.render_html(data, "Synthetic owner", WHEN)
    text = brief.render_text(data, "Synthetic owner", WHEN)
    assert "Final decision feed withheld execution" in text
    assert ">Act now " not in html and ">Best new buys " not in html
    assert "  Free ~" not in text and "  ADD:" not in text and "  TRIM:" not in text
    assert "funded</strong>" not in html and "WHERE YOUR DRY POWDER GOES" not in html


@pytest.mark.parametrize("feed", FINAL_FEED_VETO_CASES)
def test_final_status_feed_veto_is_preserved_in_pdf(cockpit_layouts, feed):
    # Keep the feed/model/HTML/text contract independent of optional PDF deps.
    # The required heavy CI lane installs these and exercises real PDF output.
    pytest.importorskip("reportlab")
    PdfReader = pytest.importorskip("pypdf").PdfReader
    data = _model_with_final_feed_veto(cockpit_layouts, feed)
    pdf = brief.render_pdf(data, "Synthetic owner", WHEN)
    assert pdf is not None and pdf.startswith(b"%PDF-")
    pdf_text = " ".join(" ".join(page.extract_text() for page in PdfReader(io.BytesIO(pdf)).pages).split())
    assert "Final decision feed withheld execution" in pdf_text
    assert feed in pdf_text
    assert "New opportunities withheld" in pdf_text
    assert "No executable new-buy candidates are shown." in pdf_text
    assert "Best new buys" not in pdf_text
    assert "SELECTED.US" not in pdf_text and "AUDIT.US" not in pdf_text
    assert "~500 SAR" not in pdf_text and "~700 SAR" not in pdf_text


def test_final_feed_veto_cannot_look_actionable_when_portfolio_only_has_holds(cockpit_layouts):
    data = brief.build_model({
        brief.PAGE_STATUS: [["TFB Decision Feed", "NOT_ACTIONABLE(final_publication) | synthetic failure"]],
        brief.PAGE_DECISION: [HEADER, row("HOLD", "HELD.US")],
        brief.PAGE_TOP10: cockpit_layouts["EXECUTABLE"],
    })
    html = brief.render_html(data, "Synthetic owner", WHEN)
    text = brief.render_text(data, "Synthetic owner", WHEN)
    assert "Final decision feed withheld execution" in html and "Portfolio execution withheld" in html
    assert "No portfolio action required today" not in html and ">Act now " not in html
    assert "No high-confidence portfolio action today" not in text
