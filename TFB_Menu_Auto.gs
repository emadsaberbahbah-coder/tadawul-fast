/**
 * TFB_Menu_Auto.gs
 * ------------------------------------------------------------
 * Extends the menu with AUTO actions (no page_id prompt)
 * Requires:
 *  - TFB_Phase1_Bridge.gs
 *  - TFB_PageMap.gs
 *  - TFB_Menu.gs (base menu)
 *
 * This file only adds menu items; it does not change core logic.
 */

function tfbMenuAuto_install() {
  // Optional manual installer if onOpen didn't refresh yet
  tfbMenuAuto_addItems_();
}

function onOpen() {
  // If you already have an onOpen in TFB_Menu.gs, DO NOT duplicate.
  // In Apps Script, multiple onOpen functions conflict.
  // ✅ We will NOT use this onOpen automatically.
  //
  // Keep this function here but disabled by default.
  // If you want to use it, rename it to onOpen and remove the onOpen in TFB_Menu.gs.
}

/**
 * Call this from the existing onOpen in TFB_Menu.gs
 * by adding:
 *   tfbMenuAuto_addItems_();
 */
function tfbMenuAuto_addItems_() {
  const ui = SpreadsheetApp.getUi();
  const menu = ui.createMenu("Tadawul Fast Bridge (Auto)");

  menu
    .addItem("Show Active Sheet Mapping", "tfbUi_ShowActiveMap")
    .addSeparator()
    .addItem("Build Active (Auto)", "tfbUi_BuildActiveAuto")
    .addItem("Ingest Active (Auto)", "tfbUi_IngestActiveAuto")
    .addSeparator()
    .addItem("List Mapping Sheet Names", "tfbUi_ListMapKeys")
    .addToUi();
}

/** -------------------------
 *  UI Actions
 *  ------------------------- */

function tfbUi_ShowActiveMap() {
  const ui = SpreadsheetApp.getUi();
  try {
    const m = tfbGetActivePageMap();
    ui.alert("Active Sheet Map", JSON.stringify(m, null, 2), ui.ButtonSet.OK);
  } catch (e) {
    ui.alert("Mapping error", String(e && e.message ? e.message : e), ui.ButtonSet.OK);
  }
}

function tfbUi_BuildActiveAuto() {
  const ui = SpreadsheetApp.getUi();
  try {
    const m = tfbGetActivePageMap();
    const res = tfbBuildFromConfig(m.page_id);
    ui.alert("Build done (Auto)", JSON.stringify({ map: m, result: res }, null, 2), ui.ButtonSet.OK);
  } catch (e) {
    ui.alert("Build error (Auto)", String(e && e.message ? e.message : e), ui.ButtonSet.OK);
  }
}

function tfbUi_IngestActiveAuto() {
  const ui = SpreadsheetApp.getUi();
  try {
    const m = tfbGetActivePageMap();
    const res = tfbIngestActiveSheet(m.region, m.page_id);
    ui.alert("Ingest done (Auto)", JSON.stringify({ map: m, result: res }, null, 2), ui.ButtonSet.OK);
  } catch (e) {
    ui.alert("Ingest error (Auto)", String(e && e.message ? e.message : e), ui.ButtonSet.OK);
  }
}

function tfbUi_ListMapKeys() {
  const ui = SpreadsheetApp.getUi();
  try {
    const keys = tfbListPageMap();
    ui.alert("Mapped Sheet Names", keys.join("\n"), ui.ButtonSet.OK);
  } catch (e) {
    ui.alert("Error", String(e && e.message ? e.message : e), ui.ButtonSet.OK);
  }
}
