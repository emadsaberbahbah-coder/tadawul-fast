/**
 * TFB_PageMap.gs
 * ------------------------------------------------------------
 * Phase-1 Convenience: map Sheet Name -> (region, page_id)
 * So user doesn't type page_id every time.
 *
 * Works with:
 *  - TFB_Phase1_Bridge.gs
 *  - TFB_Menu.gs
 *
 * You can change the mapping anytime without touching other scripts.
 */

const TFB_PAGEMAP = {
  // ✅ Rename these sheet names to match YOUR Google Sheet tabs exactly.
  // SheetName: { region: "ksa"|"global", page_id: "..." }

  "Market_Summary_KSA": { region: "ksa", page_id: "page_01_market_summary_ksa" },
  "Market_Summary_Global": { region: "global", page_id: "page_02_market_summary_global" },

  "Company_Profile": { region: "ksa", page_id: "page_03_company_profile" },
  "Financial_Statements": { region: "ksa", page_id: "page_04_financial_statements" },
  "Economic_Calendar": { region: "ksa", page_id: "page_05_economic_calendar" },
  "Major_Shareholders": { region: "ksa", page_id: "page_06_major_shareholders" },
  "Daily_Transactions": { region: "ksa", page_id: "page_07_daily_transactions" },
  "Analyst_Estimates": { region: "ksa", page_id: "page_08_analyst_estimates" },
  "Project_Monitor": { region: "ksa", page_id: "page_09_project_monitor" },
};

/**
 * Get mapping for active sheet.
 * @returns {{region:string,page_id:string,sheet_name:string}}
 */
function tfbGetActivePageMap() {
  const sheet = SpreadsheetApp.getActiveSheet();
  const name = sheet.getName();
  const m = TFB_PAGEMAP[name];
  if (!m) {
    throw new Error(
      `No mapping for active sheet: "${name}". ` +
      `Edit TFB_PageMap.gs and add:\n` +
      `"${name}": { region: "ksa", page_id: "page_01_market_summary_ksa" }`
    );
  }
  return { region: m.region, page_id: m.page_id, sheet_name: name };
}

/**
 * Build active sheet using its mapping (no prompts).
 */
function tfbBuildActiveMapped() {
  const m = tfbGetActivePageMap();
  return tfbBuildFromConfig(m.page_id);
}

/**
 * Ingest active sheet using its mapping (no prompts).
 */
function tfbIngestActiveMapped() {
  const m = tfbGetActivePageMap();
  return tfbIngestActiveSheet(m.region, m.page_id);
}

/**
 * Quick check: show which (region,page_id) is mapped to active sheet.
 */
function tfbShowActiveMap() {
  const m = tfbGetActivePageMap();
  SpreadsheetApp.getUi().alert("Active Sheet Map", JSON.stringify(m, null, 2), SpreadsheetApp.getUi().ButtonSet.OK);
  return m;
}

/**
 * Optional: list mapping keys for review.
 */
function tfbListPageMap() {
  return Object.keys(TFB_PAGEMAP).sort();
}
