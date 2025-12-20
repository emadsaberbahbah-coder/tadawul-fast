/**
 * TFB_Menu_Auto.gs
 * ------------------------------------------------------------
 * REVIEWED + ENHANCED (Phase-1 safe)
 *
 * Goals:
 *  - NO onOpen() here (prevents conflicts)
 *  - Adds/refreshes the Auto menu safely
 *  - Provides robust error formatting
 *  - Optional: single "Build+Ingest Active (Auto)" action
 *
 * Requires:
 *  - TFB_Phase1_Bridge.gs
 *  - TFB_PageMap.gs
 *  - TFB_Menu.gs calls: tfbMenuAuto_addItems_()
 */

function tfbMenuAuto_install() {
  // Manual install/refresh (safe to run anytime)
  tfbMenuAuto_addItems_();
}

/**
 * Called from TFB_Menu.gs onOpen() (recommended).
 * Builds a separate menu: "Tadawul Fast Bridge (Auto)"
 */
function tfbMenuAuto_addItems_() {
  const ui = SpreadsheetApp.getUi();
  const menu = ui.createMenu("Tadawul Fast Bridge (Auto)");

  menu
    .addItem("Show Active Sheet Mapping", "tfbUi_ShowActiveMap")
    .addSeparator()
    .addItem("Build Active (Auto)", "tfbUi_BuildActiveAuto")
    .addItem("Ingest Active (Auto)", "tfbUi_IngestActiveAuto")
    .addItem("Build + Ingest Active (Auto)", "tfbUi_BuildAndIngestActiveAuto")
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
    return m;
  } catch (e) {
    ui.alert("Mapping error", tfbUi_Err_(e), ui.ButtonSet.OK);
    throw e;
  }
}

function tfbUi_BuildActiveAuto() {
  const ui = SpreadsheetApp.getUi();
  try {
    const m = tfbGetActivePageMap();
    const res = tfbBuildFromConfig(m.page_id);
    ui.alert("Build done (Auto)", JSON.stringify({ map: m, result: res }, null, 2), ui.ButtonSet.OK);
    return { map: m, result: res };
  } catch (e) {
    ui.alert("Build error (Auto)", tfbUi_Err_(e), ui.ButtonSet.OK);
    throw e;
  }
}

function tfbUi_IngestActiveAuto() {
  const ui = SpreadsheetApp.getUi();
  try {
    const m = tfbGetActivePageMap();
    const res = tfbIngestActiveSheet(m.region, m.page_id);
    ui.alert("Ingest done (Auto)", JSON.stringify({ map: m, result: res }, null, 2), ui.ButtonSet.OK);
    return { map: m, result: res };
  } catch (e) {
    ui.alert("Ingest error (Auto)", tfbUi_Err_(e), ui.ButtonSet.OK);
    throw e;
  }
}

/**
 * Convenience: builds headers/validation first, then ingests.
 * Helps avoid ingesting with old headers after YAML changes.
 */
function tfbUi_BuildAndIngestActiveAuto() {
  const ui = SpreadsheetApp.getUi();
  try {
    const m = tfbGetActivePageMap();

    const build = tfbBuildFromConfig(m.page_id);
    const ingest = tfbIngestActiveSheet(m.region, m.page_id);

    ui.alert(
      "Build + Ingest (Auto) done",
      JSON.stringify({ map: m, build_result: build, ingest_result: ingest }, null, 2),
      ui.ButtonSet.OK
    );

    return { map: m, build_result: build, ingest_result: ingest };
  } catch (e) {
    ui.alert("Build+Ingest error (Auto)", tfbUi_Err_(e), ui.ButtonSet.OK);
    throw e;
  }
}

function tfbUi_ListMapKeys() {
  const ui = SpreadsheetApp.getUi();
  try {
    const keys = tfbListPageMap();
    ui.alert("Mapped Sheet Names", keys.join("\n"), ui.ButtonSet.OK);
    return keys;
  } catch (e) {
    ui.alert("Error", tfbUi_Err_(e), ui.ButtonSet.OK);
    throw e;
  }
}

/** -------------------------
 *  Helper (local)
 *  ------------------------- */

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
