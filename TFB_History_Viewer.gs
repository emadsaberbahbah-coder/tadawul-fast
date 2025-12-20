/**
 * TFB_History_Viewer.gs
 * ------------------------------------------------------------
 * Phase-1 Required Utility:
 * Quickly VIEW history rows appended into History_KSA / History_Global
 * without manual filters.
 *
 * Provides:
 *  - tfbHistory_ShowLatest(region, limit)
 *  - tfbHistory_FindBySymbol(region, symbol, limit)
 *  - tfbHistory_FindByPage(region, pageId, limit)
 *
 * Optional UI menu actions:
 *  - tfbUi_HistoryLatest()
 *  - tfbUi_HistoryBySymbol()
 *  - tfbUi_HistoryByPage()
 */

function tfbHistory_ShowLatest(region, limit) {
  limit = Number(limit || 20);
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(region === "ksa" ? "History_KSA" : "History_Global");
  if (!sheet) throw new Error("History sheet not found for region: " + region);

  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  if (lastRow < 2) return { ok: true, message: "No history rows yet." };

  const start = Math.max(2, lastRow - limit + 1);
  const values = sheet.getRange(start, 1, lastRow - start + 1, lastCol).getValues();

  return {
    ok: true,
    region,
    sheet: sheet.getName(),
    total_rows: lastRow - 1,
    returned: values.length,
    rows: values,
  };
}

function tfbHistory_FindBySymbol(region, symbol, limit) {
  limit = Number(limit || 50);
  symbol = String(symbol || "").trim().toUpperCase();
  if (!symbol) throw new Error("symbol is required");

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(region === "ksa" ? "History_KSA" : "History_Global");
  if (!sheet) throw new Error("History sheet not found for region: " + region);

  const header = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const symIdx = header.indexOf("symbol");
  if (symIdx < 0) throw new Error("History sheet has no 'symbol' column.");

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return { ok: true, message: "No history rows yet." };

  const data = sheet.getRange(2, 1, lastRow - 1, header.length).getValues();

  const matches = [];
  for (let i = data.length - 1; i >= 0; i--) {
    const row = data[i];
    const v = String(row[symIdx] || "").trim().toUpperCase();
    if (v === symbol) {
      matches.push(row);
      if (matches.length >= limit) break;
    }
  }

  return {
    ok: true,
    region,
    symbol,
    returned: matches.length,
    rows: matches,
  };
}

function tfbHistory_FindByPage(region, pageId, limit) {
  limit = Number(limit || 50);
  pageId = String(pageId || "").trim();
  if (!pageId) throw new Error("pageId is required");

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(region === "ksa" ? "History_KSA" : "History_Global");
  if (!sheet) throw new Error("History sheet not found for region: " + region);

  const header = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const pageIdx = header.indexOf("page_id");
  if (pageIdx < 0) throw new Error("History sheet has no 'page_id' column.");

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return { ok: true, message: "No history rows yet." };

  const data = sheet.getRange(2, 1, lastRow - 1, header.length).getValues();

  const matches = [];
  for (let i = data.length - 1; i >= 0; i--) {
    const row = data[i];
    const v = String(row[pageIdx] || "").trim();
    if (v === pageId) {
      matches.push(row);
      if (matches.length >= limit) break;
    }
  }

  return {
    ok: true,
    region,
    page_id: pageId,
    returned: matches.length,
    rows: matches,
  };
}

/** -------------------------
 * Optional UI prompts
 * ------------------------- */

function tfbUi_HistoryLatest() {
  const ui = SpreadsheetApp.getUi();
  const resp = ui.prompt("History Latest", "Region? (ksa/global) then limit (e.g. ksa,20)", ui.ButtonSet.OK_CANCEL);
  if (resp.getSelectedButton() !== ui.Button.OK) return;

  const parts = String(resp.getResponseText() || "").split(",").map(s => s.trim());
  const region = (parts[0] || "ksa").toLowerCase();
  const limit = Number(parts[1] || 20);

  const res = tfbHistory_ShowLatest(region, limit);
  ui.alert("Result", JSON.stringify(res, null, 2), ui.ButtonSet.OK);
}

function tfbUi_HistoryBySymbol() {
  const ui = SpreadsheetApp.getUi();
  const resp = ui.prompt("History By Symbol", "Region? (ksa/global), Symbol, Limit (e.g. ksa,1120.SR,50)", ui.ButtonSet.OK_CANCEL);
  if (resp.getSelectedButton() !== ui.Button.OK) return;

  const parts = String(resp.getResponseText() || "").split(",").map(s => s.trim());
  const region = (parts[0] || "ksa").toLowerCase();
  const symbol = parts[1] || "";
  const limit = Number(parts[2] || 50);

  const res = tfbHistory_FindBySymbol(region, symbol, limit);
  ui.alert("Result", JSON.stringify(res, null, 2), ui.ButtonSet.OK);
}

function tfbUi_HistoryByPage() {
  const ui = SpreadsheetApp.getUi();
  const resp = ui.prompt("History By Page", "Region? (ksa/global), page_id, Limit (e.g. ksa,page_01_market_summary_ksa,50)", ui.ButtonSet.OK_CANCEL);
  if (resp.getSelectedButton() !== ui.Button.OK) return;

  const parts = String(resp.getResponseText() || "").split(",").map(s => s.trim());
  const region = (parts[0] || "ksa").toLowerCase();
  const pageId = parts[1] || "";
  const limit = Number(parts[2] || 50);

  const res = tfbHistory_FindByPage(region, pageId, limit);
  ui.alert("Result", JSON.stringify(res, null, 2), ui.ButtonSet.OK);
}
