/**
 * TFB_History_Viewer.gs
 * ------------------------------------------------------------
 * REVIEWED + ENHANCED (Phase-1 safe)
 *
 * Improvements:
 *  - Robust region validation (ksa/global only)
 *  - Uses column indexes dynamically and returns objects (header->value)
 *  - Optional "History_View" output sheet (instead of long alerts)
 *  - Better error messages + safe limits
 *
 * Keeps original public APIs:
 *  - tfbHistory_ShowLatest(region, limit)
 *  - tfbHistory_FindBySymbol(region, symbol, limit)
 *  - tfbHistory_FindByPage(region, pageId, limit)
 *
 * Adds optional:
 *  - tfbHistory_WriteToView(region, title, header, rows)
 *  - UI: tfbUi_HistoryLatest(), tfbUi_HistoryBySymbol(), tfbUi_HistoryByPage()
 *        now can write to "History_View" sheet for easy review.
 */

function tfbHistory_ShowLatest(region, limit) {
  region = tfbHistory_normRegion_(region);
  limit = tfbHistory_safeLimit_(limit, 20, 500);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = tfbHistory_getHistorySheet_(ss, region);

  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  if (lastRow < 2 || lastCol < 1) return { ok: true, region, message: "No history rows yet." };

  const header = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
  const start = Math.max(2, lastRow - limit + 1);
  const values = sheet.getRange(start, 1, lastRow - start + 1, lastCol).getValues();

  return {
    ok: true,
    region,
    sheet: sheet.getName(),
    total_rows: lastRow - 1,
    returned: values.length,
    header,
    rows: tfbHistory_rowsToObjects_(header, values),
  };
}

function tfbHistory_FindBySymbol(region, symbol, limit) {
  region = tfbHistory_normRegion_(region);
  limit = tfbHistory_safeLimit_(limit, 50, 2000);

  symbol = String(symbol || "").trim().toUpperCase();
  if (!symbol) throw new Error("symbol is required");

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = tfbHistory_getHistorySheet_(ss, region);

  const lastCol = sheet.getLastColumn();
  const header = sheet.getRange(1, 1, 1, lastCol).getValues()[0];

  const symIdx = header.indexOf("symbol");
  if (symIdx < 0) throw new Error("History sheet has no 'symbol' column.");

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return { ok: true, region, symbol, message: "No history rows yet." };

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
    header,
    rows: tfbHistory_rowsToObjects_(header, matches),
  };
}

function tfbHistory_FindByPage(region, pageId, limit) {
  region = tfbHistory_normRegion_(region);
  limit = tfbHistory_safeLimit_(limit, 50, 2000);

  pageId = String(pageId || "").trim();
  if (!pageId) throw new Error("pageId is required");

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = tfbHistory_getHistorySheet_(ss, region);

  const lastCol = sheet.getLastColumn();
  const header = sheet.getRange(1, 1, 1, lastCol).getValues()[0];

  const pageIdx = header.indexOf("page_id");
  if (pageIdx < 0) throw new Error("History sheet has no 'page_id' column.");

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return { ok: true, region, page_id: pageId, message: "No history rows yet." };

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
    header,
    rows: tfbHistory_rowsToObjects_(header, matches),
  };
}

/** -------------------------
 * Optional UI prompts
 * ------------------------- */

function tfbUi_HistoryLatest() {
  const ui = SpreadsheetApp.getUi();
  const resp = ui.prompt(
    "History Latest",
    "Region? (ksa/global), Limit, Output (alert/view)\nExample: ksa,20,view",
    ui.ButtonSet.OK_CANCEL
  );
  if (resp.getSelectedButton() !== ui.Button.OK) return;

  const parts = String(resp.getResponseText() || "").split(",").map(s => s.trim());
  const region = (parts[0] || "ksa").toLowerCase();
  const limit = Number(parts[1] || 20);
  const out = String(parts[2] || "view").toLowerCase(); // default: view

  try {
    const res = tfbHistory_ShowLatest(region, limit);
    tfbHistory_present_(res, out, `Latest History (${res.region})`);
  } catch (e) {
    ui.alert("Error", tfbHistory_err_(e), ui.ButtonSet.OK);
  }
}

function tfbUi_HistoryBySymbol() {
  const ui = SpreadsheetApp.getUi();
  const resp = ui.prompt(
    "History By Symbol",
    "Region? (ksa/global), Symbol, Limit, Output (alert/view)\nExample: ksa,1120.SR,50,view",
    ui.ButtonSet.OK_CANCEL
  );
  if (resp.getSelectedButton() !== ui.Button.OK) return;

  const parts = String(resp.getResponseText() || "").split(",").map(s => s.trim());
  const region = (parts[0] || "ksa").toLowerCase();
  const symbol = parts[1] || "";
  const limit = Number(parts[2] || 50);
  const out = String(parts[3] || "view").toLowerCase();

  try {
    const res = tfbHistory_FindBySymbol(region, symbol, limit);
    tfbHistory_present_(res, out, `History By Symbol: ${symbol} (${res.region})`);
  } catch (e) {
    ui.alert("Error", tfbHistory_err_(e), ui.ButtonSet.OK);
  }
}

function tfbUi_HistoryByPage() {
  const ui = SpreadsheetApp.getUi();
  const resp = ui.prompt(
    "History By Page",
    "Region? (ksa/global), page_id, Limit, Output (alert/view)\nExample: ksa,page_01_market_summary_ksa,50,view",
    ui.ButtonSet.OK_CANCEL
  );
  if (resp.getSelectedButton() !== ui.Button.OK) return;

  const parts = String(resp.getResponseText() || "").split(",").map(s => s.trim());
  const region = (parts[0] || "ksa").toLowerCase();
  const pageId = parts[1] || "";
  const limit = Number(parts[2] || 50);
  const out = String(parts[3] || "view").toLowerCase();

  try {
    const res = tfbHistory_FindByPage(region, pageId, limit);
    tfbHistory_present_(res, out, `History By Page: ${pageId} (${res.region})`);
  } catch (e) {
    ui.alert("Error", tfbHistory_err_(e), ui.ButtonSet.OK);
  }
}

/** -------------------------
 * Presentation
 * ------------------------- */

function tfbHistory_present_(res, outMode, title) {
  const ui = SpreadsheetApp.getUi();
  if (!res || res.ok !== true) {
    ui.alert("Result", JSON.stringify(res, null, 2), ui.ButtonSet.OK);
    return;
  }

  const mode = (outMode === "alert") ? "alert" : "view";

  if (mode === "alert") {
    ui.alert("Result", JSON.stringify(res, null, 2), ui.ButtonSet.OK);
    return;
  }

  // write to History_View
  const header = res.header || [];
  const rows = (res.rows || []).map(obj => header.map(h => obj[h]));
  tfbHistory_WriteToView(res.region, title, header, rows);

  ui.alert("Written to History_View ✅", `${title}\n\nRows: ${rows.length}`, ui.ButtonSet.OK);
}

function tfbHistory_WriteToView(region, title, header, rows) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const name = "History_View";
  let sheet = ss.getSheetByName(name);
  if (!sheet) sheet = ss.insertSheet(name);

  sheet.clearContents();

  const meta = [
    ["Title", title],
    ["Region", region],
    ["GeneratedAt", new Date().toISOString()],
    ["Rows", rows.length],
  ];

  sheet.getRange(1, 1, meta.length, 2).setValues(meta);

  if (header && header.length) {
    sheet.getRange(6, 1, 1, header.length).setValues([header]);
    if (rows && rows.length) {
      sheet.getRange(7, 1, rows.length, header.length).setValues(rows);
    }
    sheet.setFrozenRows(6);
    sheet.getRange(6, 1, 1, header.length).setFontWeight("bold");
    sheet.autoResizeColumns(1, Math.min(header.length, 30));
  }
}

/** -------------------------
 * Helpers
 * ------------------------- */

function tfbHistory_getHistorySheet_(ss, region) {
  const sheetName = (region === "ksa") ? "History_KSA" : "History_Global";
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) throw new Error("History sheet not found: " + sheetName);
  return sheet;
}

function tfbHistory_normRegion_(region) {
  const r = String(region || "").trim().toLowerCase();
  if (r !== "ksa" && r !== "global") throw new Error("Invalid region. Use 'ksa' or 'global'.");
  return r;
}

function tfbHistory_safeLimit_(limit, def, max) {
  let n = Number(limit);
  if (!Number.isFinite(n) || n <= 0) n = Number(def || 20);
  n = Math.floor(n);
  if (n > max) n = max;
  return n;
}

function tfbHistory_rowsToObjects_(header, values) {
  const out = [];
  for (let i = 0; i < values.length; i++) {
    const row = values[i];
    const obj = {};
    for (let c = 0; c < header.length; c++) {
      obj[String(header[c] || `col_${c + 1}`)] = row[c];
    }
    out.push(obj);
  }
  return out;
}

function tfbHistory_err_(e) {
  try {
    if (!e) return "Unknown error";
    if (typeof e === "string") return e;
    if (e.message) return String(e.message);
    return JSON.stringify(e, null, 2);
  } catch (_) {
    return String(e);
  }
}
