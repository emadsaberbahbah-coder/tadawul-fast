/**
 * TFB_Global_Only.gs
 * ------------------------------------------------------------
 * Global-only bridge (2 pages):
 *  - Global_Markets : pulls ENRICHED rows from backend (/api/v1/global/enrich)
 *  - Insights_Analysis : Top 7 opportunities from Global_Markets
 *
 * Requires Script Properties:
 *  - BACKEND_BASE_URL  (e.g. https://tadawul-fast-bridge.onrender.com)
 *  - APP_TOKEN
 */

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("TFB (Global Only)")
    .addItem("Setup Wizard", "tfbSetupWizard_GlobalOnly")
    .addSeparator()
    .addItem("Refresh Global_Markets (ENRICH)", "tfbRefreshGlobalMarkets")
    .addItem("Build Global_Markets headers from backend config", "tfbBuildGlobalHeaders")
    .addSeparator()
    .addItem("Generate Insights_Analysis (Top 7)", "tfbGenerateTop7Insights")
    .addToUi();
}

/** ---------------------------
 *  Settings / Helpers
 *  --------------------------- */

function tfbProps_() {
  const p = PropertiesService.getScriptProperties();
  const base = String(p.getProperty("BACKEND_BASE_URL") || "").trim();
  const token = String(p.getProperty("APP_TOKEN") || "").trim();
  if (!base) throw new Error("Missing Script Property: BACKEND_BASE_URL");
  if (!token) throw new Error("Missing Script Property: APP_TOKEN");
  return { base: base.replace(/\/+$/, ""), token };
}

function tfbFetchJson_(path, method, payloadObj) {
  const { base, token } = tfbProps_();
  const url = base + path;

  const params = {
    method: method || "get",
    muteHttpExceptions: true,
    contentType: "application/json",
    headers: { "X-APP-TOKEN": token },
  };

  if (payloadObj !== undefined && payloadObj !== null) {
    params.payload = JSON.stringify(payloadObj);
  }

  const resp = UrlFetchApp.fetch(url, params);
  const code = resp.getResponseCode();
  const text = resp.getContentText() || "";

  let json;
  try { json = text ? JSON.parse(text) : null; } catch (e) { json = null; }

  if (code >= 400) {
    const msg = (json && json.detail) ? json.detail : text;
    throw new Error("HTTP " + code + " " + msg);
  }
  return json;
}

function tfbEnsureSheet_(name) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(name);
  if (!sh) sh = ss.insertSheet(name);
  return sh;
}

/** ---------------------------
 *  Build headers from backend config (page_02_market_summary_global)
 *  --------------------------- */

function tfbBuildGlobalHeaders() {
  const pageId = "page_02_market_summary_global";
  const sh = tfbEnsureSheet_("Global_Markets");
  const cfg = tfbFetchJson_("/api/v1/config/" + pageId, "get");

  const cols = (cfg && cfg.columns) ? cfg.columns : [];
  if (!cols.length) throw new Error("No columns returned from backend config for " + pageId);

  const headers = cols.map(c => c.name);

  // write headers row 1
  sh.getRange(1, 1, 1, headers.length).setValues([headers]);
  sh.setFrozenRows(1);
  sh.getRange(1, 1, sh.getMaxRows(), sh.getMaxColumns()).setWrap(false);

  SpreadsheetApp.getUi().alert("Built headers ✅", "Global_Markets headers updated from backend config.", SpreadsheetApp.getUi().ButtonSet.OK);
}

/** ---------------------------
 *  Refresh Global_Markets via /api/v1/global/enrich
 *  --------------------------- */

function tfbRefreshGlobalMarkets() {
  const sh = tfbEnsureSheet_("Global_Markets");

  // Expect a "symbol" header
  const header = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
  const symIdx = header.indexOf("symbol");
  if (symIdx < 0) throw new Error("Global_Markets must have 'symbol' column in header row 1. Run 'Build Global_Markets headers' first.");

  const lastRow = sh.getLastRow();
  if (lastRow < 2) throw new Error("No symbols found. Put symbols under 'symbol' column starting row 2.");

  const symbols = sh.getRange(2, symIdx + 1, lastRow - 1, 1).getValues()
    .map(r => String(r[0] || "").trim())
    .filter(s => !!s);

  if (!symbols.length) throw new Error("No symbols found in 'symbol' column.");

  // Call backend ENRICH
  const res = tfbFetchJson_("/api/v1/global/enrich", "post", { symbols: symbols });

  const rows = (res && res.rows) ? res.rows : [];
  if (!rows.length) throw new Error("Backend returned 0 rows.");

  // Write back: align by headers
  tfbWriteObjectsByHeader_(sh, header, rows);

  SpreadsheetApp.getUi().alert("Refresh done ✅", "Global_Markets enriched rows: " + rows.length, SpreadsheetApp.getUi().ButtonSet.OK);
}

function tfbWriteObjectsByHeader_(sheet, header, objects) {
  // Build 2D array matching header order
  const out = objects.map(obj => header.map(h => (obj && obj[h] !== undefined ? obj[h] : "")));

  // Clear old data area (keep header)
  const maxCols = header.length;
  const neededRows = out.length;
  if (sheet.getLastRow() > 1) {
    sheet.getRange(2, 1, Math.max(1, sheet.getLastRow() - 1), maxCols).clearContent();
  }

  sheet.getRange(2, 1, neededRows, maxCols).setValues(out);
}

/** ---------------------------
 *  Insights_Analysis: Top 7 by opportunity_score
 *  --------------------------- */

function tfbGenerateTop7Insights() {
  const src = tfbEnsureSheet_("Global_Markets");
  const dst = tfbEnsureSheet_("Insights_Analysis");

  const header = src.getRange(1, 1, 1, src.getLastColumn()).getValues()[0];
  const lastRow = src.getLastRow();
  if (lastRow < 2) throw new Error("Global_Markets has no data. Run Refresh first.");

  const data = src.getRange(2, 1, lastRow - 1, header.length).getValues();

  const idxOpp = header.indexOf("opportunity_score");
  if (idxOpp < 0) throw new Error("Missing 'opportunity_score' column. Make sure your backend enrich returns it and your YAML includes it.");

  // Convert rows to objects
  const objs = data.map(r => {
    const o = {};
    header.forEach((h, i) => o[h] = r[i]);
    return o;
  });

  // Sort by opportunity_score desc
  objs.sort((a, b) => (Number(b.opportunity_score || -1e9) - Number(a.opportunity_score || -1e9)));
  const top7 = objs.slice(0, 7);

  // Output columns for Insights (you can adjust)
  const outCols = [
    "symbol","company_name","exchange","sector","industry",
    "last_price","market_cap",
    "return_1m","return_3m","return_1y",
    "volatility_30d",
    "analyst_target_price","upside_to_target_percent",
    "value_score","quality_score","momentum_score","risk_score",
    "opportunity_score","recommendation","rationale","last_updated"
  ];

  // Build sheet
  dst.clear();
  dst.getRange(1, 1, 1, outCols.length).setValues([outCols]);
  dst.setFrozenRows(1);

  const out = top7.map(o => outCols.map(k => (o[k] !== undefined ? o[k] : "")));
  if (out.length) dst.getRange(2, 1, out.length, outCols.length).setValues(out);

  SpreadsheetApp.getUi().alert("Insights updated ✅", "Top 7 opportunities generated into Insights_Analysis.", SpreadsheetApp.getUi().ButtonSet.OK);
}

/** ---------------------------
 *  Setup Wizard
 *  --------------------------- */

function tfbSetupWizard_GlobalOnly() {
  const ui = SpreadsheetApp.getUi();
  const props = PropertiesService.getScriptProperties();

  const curBase = String(props.getProperty("BACKEND_BASE_URL") || "");
  const curTok  = String(props.getProperty("APP_TOKEN") || "");

  const b = ui.prompt(
    "TFB Global Setup",
    "BACKEND_BASE_URL\n\nCurrent:\n" + (curBase || "(empty)") + "\n\nExample:\nhttps://tadawul-fast-bridge.onrender.com",
    ui.ButtonSet.OK_CANCEL
  );
  if (b.getSelectedButton() !== ui.Button.OK) return;
  const base = String(b.getResponseText() || "").trim();

  const t = ui.prompt(
    "TFB Global Setup",
    "APP_TOKEN\n\nCurrent:\n" + (curTok ? "(set)" : "(empty)") + "\n\nPaste your APP_TOKEN",
    ui.ButtonSet.OK_CANCEL
  );
  if (t.getSelectedButton() !== ui.Button.OK) return;
  const tok = String(t.getResponseText() || "").trim();

  props.setProperties({ BACKEND_BASE_URL: base, APP_TOKEN: tok }, true);

  ui.alert("Saved ✅", "Now run: Build Global headers → Refresh Global_Markets → Generate Top 7", ui.ButtonSet.OK);
}
