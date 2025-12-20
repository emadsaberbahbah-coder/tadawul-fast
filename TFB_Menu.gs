/**
 * TFB_Menu.gs
 * ------------------------------------------------------------
 * Simple one-click menu for Phase-1 bridge.
 * Requires: TFB_Phase1_Bridge.gs (already added)
 *
 * Adds menu:
 *   Tadawul Fast Bridge
 *     - List Pages (test)
 *     - Build (Active Sheet) -> choose page_id
 *     - Ingest (Active Sheet) -> KSA
 *     - Ingest (Active Sheet) -> Global
 *
 * NOTE: We keep it minimal & stable for Phase-1.
 */

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("Tadawul Fast Bridge")
    .addItem("List Pages (test)", "tfbUi_ListPages")
    .addSeparator()
    .addItem("Build Active Sheet (by page_id)", "tfbUi_BuildActive")
    .addSeparator()
    .addItem("Ingest Active Sheet -> KSA (by page_id)", "tfbUi_IngestKsaActive")
    .addItem("Ingest Active Sheet -> Global (by page_id)", "tfbUi_IngestGlobalActive")
    .addToUi();
}

/** -------------------------
 *  UI Actions
 *  ------------------------- */

function tfbUi_ListPages() {
  const ui = SpreadsheetApp.getUi();
  try {
    const res = tfbListPages();
    ui.alert("Backend OK", JSON.stringify(res, null, 2), ui.ButtonSet.OK);
  } catch (e) {
    ui.alert("Error", String(e && e.message ? e.message : e), ui.ButtonSet.OK);
  }
}

function tfbUi_BuildActive() {
  const ui = SpreadsheetApp.getUi();
  const pageId = tfbUi_PromptPageId_("Enter page_id to BUILD headers/validation", "page_01_market_summary_ksa");
  if (!pageId) return;

  try {
    const res = tfbBuildFromConfig(pageId);
    ui.alert("Build done", JSON.stringify(res, null, 2), ui.ButtonSet.OK);
  } catch (e) {
    ui.alert("Build error", String(e && e.message ? e.message : e), ui.ButtonSet.OK);
  }
}

function tfbUi_IngestKsaActive() {
  const ui = SpreadsheetApp.getUi();
  const pageId = tfbUi_PromptPageId_("Enter KSA page_id to INGEST active sheet", "page_01_market_summary_ksa");
  if (!pageId) return;

  try {
    const res = tfbIngestActiveSheet("ksa", pageId);
    ui.alert("Ingest KSA done", JSON.stringify(res, null, 2), ui.ButtonSet.OK);
  } catch (e) {
    ui.alert("Ingest KSA error", String(e && e.message ? e.message : e), ui.ButtonSet.OK);
  }
}

function tfbUi_IngestGlobalActive() {
  const ui = SpreadsheetApp.getUi();
  const pageId = tfbUi_PromptPageId_("Enter Global page_id to INGEST active sheet", "page_02_market_summary_global");
  if (!pageId) return;

  try {
    const res = tfbIngestActiveSheet("global", pageId);
    ui.alert("Ingest Global done", JSON.stringify(res, null, 2), ui.ButtonSet.OK);
  } catch (e) {
    ui.alert("Ingest Global error", String(e && e.message ? e.message : e), ui.ButtonSet.OK);
  }
}

/** -------------------------
 *  Helpers
 *  ------------------------- */

function tfbUi_PromptPageId_(title, defaultValue) {
  const ui = SpreadsheetApp.getUi();
  const resp = ui.prompt(title, `Example: ${defaultValue}\n\nPaste page_id:`, ui.ButtonSet.OK_CANCEL);
  if (resp.getSelectedButton() !== ui.Button.OK) return "";
  const v = String(resp.getResponseText() || "").trim();
  if (!v) {
    ui.alert("Missing page_id", "Please enter a valid page_id.", ui.ButtonSet.OK);
    return "";
  }
  return v;
}
