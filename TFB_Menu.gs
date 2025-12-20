/**
 * TFB_Menu.gs
 * ------------------------------------------------------------
 * Enhanced Phase-1 Menu (stable)
 *
 * Requires:
 *  - TFB_Phase1_Bridge.gs
 * Optional (if you added them):
 *  - TFB_PageMap.gs
 *  - TFB_Menu_Auto.gs   (provides tfbMenuAuto_addItems_())
 *  - TFB_Settings_Wizard.gs
 *  - TFB_History_Viewer.gs
 *
 * What’s improved:
 *  - Adds Settings Wizard + History Viewer items (if present)
 *  - Adds Auto menu items (if present) without breaking if missing
 *  - Keeps original prompt-based actions
 *  - Single onOpen() only (prevents conflicts)
 */

function onOpen() {
  const ui = SpreadsheetApp.getUi();

  const menu = ui
    .createMenu("Tadawul Fast Bridge")
    .addItem("List Pages (test)", "tfbUi_ListPages")
    .addSeparator()
    .addItem("Build Active Sheet (by page_id)", "tfbUi_BuildActive")
    .addSeparator()
    .addItem("Ingest Active Sheet -> KSA (by page_id)", "tfbUi_IngestKsaActive")
    .addItem("Ingest Active Sheet -> Global (by page_id)", "tfbUi_IngestGlobalActive");

  // Optional: Settings Wizard (safe if script exists)
  if (typeof tfbSettingsWizard_Run === "function") {
    menu.addSeparator()
      .addItem("Settings Wizard", "tfbSettingsWizard_Run")
      .addItem("Show Settings", "tfbSettingsWizard_Show");
  }

  // Optional: History Viewer (safe if script exists)
  if (typeof tfbUi_HistoryLatest === "function") {
    menu.addSeparator()
      .addItem("History Latest (prompt)", "tfbUi_HistoryLatest")
      .addItem("History By Symbol (prompt)", "tfbUi_HistoryBySymbol")
      .addItem("History By Page (prompt)", "tfbUi_HistoryByPage");
  }

  menu.addToUi();

  // Optional: Add Auto menu in a second menu, if present
  // (we avoid adding it inside the same menu to keep it clean)
  try {
    if (typeof tfbMenuAuto_addItems_ === "function") {
      tfbMenuAuto_addItems_();
    }
  } catch (e) {
    // Do nothing (menu should never break onOpen)
  }
}

/** -------------------------
 *  UI Actions (Prompt-based)
 *  ------------------------- */

function tfbUi_ListPages() {
  const ui = SpreadsheetApp.getUi();
  try {
    const res = tfbListPages();
    ui.alert("Backend OK", JSON.stringify(res, null, 2), ui.ButtonSet.OK);
  } catch (e) {
    ui.alert("Error", tfbUi_Err_(e), ui.ButtonSet.OK);
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
    ui.alert("Build error", tfbUi_Err_(e), ui.ButtonSet.OK);
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
    ui.alert("Ingest KSA error", tfbUi_Err_(e), ui.ButtonSet.OK);
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
    ui.alert("Ingest Global error", tfbUi_Err_(e), ui.ButtonSet.OK);
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

function tfbUi_Err_(e) {
  try {
    if (!e) return "Unknown error";
    if (typeof e === "string") return e;
    if (e.message) return String(e.message);
    return JSON.stringify(e, null, 2);
  } catch (_) {
    return String(e);
  }
}
