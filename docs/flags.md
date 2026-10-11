# Flag registry

Generated from source by `python scripts/flag_registry.py --output docs/flags.md`.

This is a static inventory, not a readback of Render or GitHub Variables. It lists literal TFB_/TRACK_ consumers, observed read/helper defaults, deployment-manifest declarations and workflow env mappings. Multiple defaults are preserved; computed defaults and dynamic names are not guessed. Read/helper defaults can be transformed by their parser; consult the linked source for the final runtime default. Render/library flags require dashboard configuration. A mapping anywhere in a workflow does not prove every launch lane maps it; lane-specific tests remain authoritative. Script imports and indirect launches can require manual lane review.

Discovered 869 flag names; 164 env reads use dynamic names (listed below).

| Flag | Observed read/helper defaults | Consumer lane | Verifier | Workflow mappings | Source |
| --- | --- | --- | --- | --- | --- |
| `TFB_ACCEPTANCE_LEGACY_NA` | None | Actions: acceptance_check.yml | absent | none | scripts/tfb_acceptance.py:1142; scripts/tfb_acceptance.py:903 |
| `TFB_ADVISOR_RULE_OVERRIDE` | '1' | Render/library | absent | none | routes/advisor.py:1353 |
| `TFB_ADV_BRIDGE_TIMEOUT_SEC` | computed | Render/library | absent | none | routes/investment_advisor.py:1540 |
| `TFB_ADV_ENGINE_CALL_TIMEOUT` | True | Render/library | declared (default '0') | none | routes/advanced_analysis.py:918 |
| `TFB_ADV_ENGINE_CALL_TIMEOUT_S` | 75.0 | Render/library | declared (default '75') | none | routes/advanced_analysis.py:927 |
| `TFB_ADV_LIFT_TRUST_WARNINGS` | '' | Render/library | absent | none | routes/investment_advisor.py:1591 |
| `TFB_ADV_SPECIAL_BUILD_TIMEOUT_S` | 70.0 | Render/library | absent | none | routes/advanced_analysis.py:1194 |
| `TFB_ADV_TOTAL_TIMEOUT_S` | 88.0 | Render/library | absent | none | routes/advanced_analysis.py:933 |
| `TFB_ADV_TYPEERROR_LEGACY` | '0' | Render/library | absent | none | routes/investment_advisor.py:1178 |
| `TFB_ALLOWED_TOKENS` | None | Render/library | absent | none | core/config.py:1894 |
| `TFB_ALLOW_LEGACY_FABRICATION` | '' | Render/library | absent | none | routes/advanced_analysis.py:2714 |
| `TFB_ALLOW_QUERY_TOKEN` | False | Render/library | absent | none | config.py:495; routes/advanced_analysis.py:2176 |
| `TFB_ANALYSIS_ENGINE_TIMEOUT_SEC` | '' | Render/library | absent | none | routes/analysis_sheet_rows.py:456 |
| `TFB_ANALYSIS_GLOBAL_DEDUP` | '' | Render/library | absent | none | routes/analysis_sheet_rows.py:1413 |
| `TFB_ANALYSIS_GLOBAL_RANK` | '' | Render/library | absent | none | routes/analysis_sheet_rows.py:1320 |
| `TFB_ANALYSIS_PLACEHOLDER_MODE` | '' | Render/library | absent | none | routes/advanced_analysis.py:2713 |
| `TFB_ANALYSIS_PROXY_TIMEOUT_SEC` | '' | Render/library | absent | none | routes/advanced_sheet_rows.py:259; routes/analysis_sheet_rows.py:436 |
| `TFB_ANALYST_TREND_BLOCK` | '' | Render/library | absent | none | core/data_engine_v2.py:16089 |
| `TFB_API_KEY` | '' | Render/library | absent | none | config.py:438; main.py:1190 |
| `TFB_APP_NAME` | 'Tadawul Fast Bridge' | Render/library | absent | none | config.py:466 |
| `TFB_APP_TOKEN` | '', None | Actions: ci.yml; Render/library | absent | none | config.py:410; config.py:504; core/config.py:1214; core/config.py:1899; main.py:1184; scripts/smoke_endpoints.py:210 |
| `TFB_APP_TOKENS` | 'APP_TOKENS' | Render/library | absent | none | config.py:405 |
| `TFB_APP_VERSION` | 'unknown' | Render/library | absent | none | config.py:470 |
| `TFB_AUDIT_EODHD_FAIL_PCT` | '0' | script/operator (no direct workflow launch found) | absent | none | scripts/tfb_export_audit.py:1747 |
| `TFB_AUDIT_MARGIN_RECEIPTS` | '0' | script/operator (no direct workflow launch found) | absent | none | scripts/tfb_export_audit.py:1744 |
| `TFB_AUDIT_MIN_FRESH_PCT` | '95' | Actions: page_refresh_recovery.yml, sync_outcome_audit.yml | absent | daily_sync.yml:1647 `"95"`; page_refresh_recovery.yml:83 `'95'`; page_refresh_recovery.yml:165 `'95'`; sync_outcome_audit.yml:74 `"95"` | scripts/audit_sync_outcome.py:89 |
| `TFB_AUDIT_REQUIRE_FULL_FETCH` | '0' | Actions: page_refresh_recovery.yml, sync_outcome_audit.yml | absent | daily_sync.yml:1646 `"1"`; page_refresh_recovery.yml:82 `'1'`; page_refresh_recovery.yml:164 `'1'`; sync_outcome_audit.yml:73 `"1"` | scripts/audit_sync_outcome.py:83 |
| `TFB_AUTH_HEADER_NAME` | computed | Render/library | absent | none | config.py:500 |
| `TFB_AUTH_TOKEN` | '' | Render/library | absent | none | config.py:415; config.py:434; config.py:515 |
| `TFB_AUTH_TOKENS` | 'AUTH_TOKENS' | Render/library | absent | none | config.py:432 |
| `TFB_AW2_SECTOR_PASSTHROUGH` | '0' | Render/library | absent | none | core/data_engine_v2.py:12588 |
| `TFB_BACKEND_URL` | computed | Actions: intraday_refresh.yml | absent | intraday_refresh.yml:142 `${{ vars.BACKEND_BASE_URL &#124;&#124; 'https://tadawul-fast-bridge.onrender.com' }}`; intraday_refresh.yml:203 `${{ vars.BACKEND_BASE_URL &#124;&#124; 'https://tadawul-fast-bridge.onrender.com' }}` | scripts/intraday_quote_refresh.py:589 |
| `TFB_BACKTEST_DSR_GATE` | '0' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | declared (default '0') | none | scripts/track_performance.py:7932 |
| `TFB_BACKTEST_KSA_MIN_BARS` | 60 | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10602 |
| `TFB_BACKTEST_KSA_YF` | False | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | declared (default '0') | none | scripts/track_performance.py:10601 |
| `TFB_BACKTEST_KSA_YF_PERIOD` | '2y' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10603 |
| `TFB_BACKTEST_MIN_DSR` | 0.95 | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:7951 |
| `TFB_BACKTEST_NONOVERLAP` | False | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | declared (default '0') | none | scripts/track_performance.py:8140 |
| `TFB_BACKTEST_TSTAT_FLOOR` | 0.0 | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:8142 |
| `TFB_BACKUP_DEST` | 'drive' | Actions: workbook_backup.yml | absent | workbook_backup.yml:95 `${{ vars.TFB_BACKUP_DEST }}` | scripts/backup_workbook.py:381 |
| `TFB_BACKUP_DRIVE_FOLDER_NAME` | 'TFB Workbook Backups' | Actions: workbook_backup.yml | absent | workbook_backup.yml:105 `${{ vars.TFB_BACKUP_DRIVE_FOLDER_NAME }}` | scripts/backup_workbook.py:390 |
| `TFB_BACKUP_FOLDER_ID` | computed | Actions: workbook_backup.yml | absent | workbook_backup.yml:89 `${{ vars.TFB_BACKUP_FOLDER_ID }}` | scripts/backup_workbook.py:768; scripts/backup_workbook.py:929 |
| `TFB_BACKUP_GCS_BUCKET` | computed | Actions: workbook_backup.yml | absent | workbook_backup.yml:96 `${{ vars.TFB_BACKUP_GCS_BUCKET }}` | scripts/backup_workbook.py:454 |
| `TFB_BACKUP_GCS_PREFIX` | None | Actions: workbook_backup.yml | absent | workbook_backup.yml:97 `${{ vars.TFB_BACKUP_GCS_PREFIX }}` | scripts/backup_workbook.py:460 |
| `TFB_BACKUP_KEEP_DAILY` | 30 | Actions: workbook_backup.yml | absent | none | scripts/backup_workbook.py:898 |
| `TFB_BACKUP_KEEP_MONTHLY` | 12 | Actions: workbook_backup.yml | absent | none | scripts/backup_workbook.py:899 |
| `TFB_BACKUP_LOCAL_COPY` | computed | Actions: workbook_backup.yml | absent | workbook_backup.yml:148 `${{ (vars.TFB_DQ_AUDIT_MODE == 'observe' &#124;&#124; vars.TFB_DQ_AUDIT_MODE == 'enforce') && format('{0}/tfb_export.xlsx', runner.temp) &#124;&#124; '' }}` | scripts/backup_workbook.py:272 |
| `TFB_BACKUP_MIN_BYTES` | 1000000 | Actions: workbook_backup.yml | absent | none | scripts/backup_workbook.py:811 |
| `TFB_BACKUP_OAUTH_CLIENT_ID` | computed | Actions: workbook_backup.yml | absent | workbook_backup.yml:102 `${{ secrets.TFB_BACKUP_OAUTH_CLIENT_ID }}` | scripts/backup_workbook.py:398 |
| `TFB_BACKUP_OAUTH_CLIENT_SECRET` | computed | Actions: workbook_backup.yml | absent | workbook_backup.yml:103 `${{ secrets.TFB_BACKUP_OAUTH_CLIENT_SECRET }}` | scripts/backup_workbook.py:399 |
| `TFB_BACKUP_OAUTH_REFRESH_TOKEN` | computed | Actions: workbook_backup.yml | absent | workbook_backup.yml:104 `${{ secrets.TFB_BACKUP_OAUTH_REFRESH_TOKEN }}` | scripts/backup_workbook.py:400 |
| `TFB_BAR_AGE_FAILOVER` | '' | Render/library | absent | none | core/data_engine_v2.py:14046 |
| `TFB_BAR_AGE_MAX_SESSIONS` | '1' | Render/library | absent | none | core/data_engine_v2.py:12445 |
| `TFB_BASE_URL` | '', 'http://127.0.0.1:8000' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml; Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:203; scripts/track_performance.py:8511 |
| `TFB_BEARER_TOKEN` | '' | Render/library | absent | none | config.py:419; config.py:428; config.py:510; main.py:1188 |
| `TFB_BEARER_TOKENS` | 'BEARER_TOKENS' | Render/library | absent | none | config.py:427 |
| `TFB_BOARD_ENGINE_ROI` | 'off' | Actions: shadow_board.yml | absent | shadow_board.yml:93 `"enforce"` | scripts/run_shadow_board.py:421 |
| `TFB_BOARD_FREEZE_HOLIDAYS` | '' | Actions: shadow_board.yml | absent | none | scripts/run_shadow_board.py:273 |
| `TFB_BOARD_FREEZE_NONTRADING` | 'off' | Actions: shadow_board.yml | absent | none | scripts/run_shadow_board.py:263 |
| `TFB_BOARD_FREEZE_TODAY` | '' | Actions: shadow_board.yml | absent | none | scripts/run_shadow_board.py:291 |
| `TFB_BOARD_SCORECARD` | '', None | Actions: board_scorecard.yml | absent | board_scorecard.yml:59 `"observe"` | scripts/score_selection_log.py:1220; scripts/score_selection_log.py:341 |
| `TFB_BOARD_SCORECARD_MIN_COVERAGE` | '' | Actions: board_scorecard.yml | absent | none | scripts/score_selection_log.py:350 |
| `TFB_BRIEF_CANDIDATE_EARNINGS_GUARD` | '1' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:1271 |
| `TFB_BRIEF_COHERENCE_GUARD` | '1' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:975 |
| `TFB_BRIEF_COHERENCE_REL_GAP` | '15' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:981 |
| `TFB_BRIEF_COHERENCE_ROI_GAP` | '15' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:989 |
| `TFB_BRIEF_EARNINGS_FLAG_DAYS` | '7' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:1792 |
| `TFB_BRIEF_EARNINGS_ON_ACTIONS` | '1' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:1913 |
| `TFB_BRIEF_FRESH_SHARE_BANNER` | '1' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:1106 |
| `TFB_BRIEF_FRESH_SHARE_MIN_PCT` | '60' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:1115 |
| `TFB_BRIEF_FUNNEL` | '1' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:1138 |
| `TFB_BRIEF_FV_CAP_MARKER` | '' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:2082 |
| `TFB_BRIEF_FV_SAT_MIN` | '3' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:2099 |
| `TFB_BRIEF_HOLD_NARRATIVE_V2` | '1' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:2634 |
| `TFB_BRIEF_LEVELS` | '1' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:1526 |
| `TFB_BRIEF_LOCAL_XLSX` | '' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:3419 |
| `TFB_BRIEF_NEWS_CONTEXT` | '1' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:1855 |
| `TFB_BRIEF_NEWS_TIMEOUT_SEC` | '8' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:1861 |
| `TFB_BRIEF_OUTPUT_PATH` | 'daily_brief.html' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:3421 |
| `TFB_BRIEF_OWNER_NAME` | 'Emad Bahbah' | Actions: ci.yml, daily_brief.yml | absent | daily_brief.yml:115 `"Emad Bahbah"` | scripts/run_daily_brief.py:3360; scripts/run_daily_brief.py:3427 |
| `TFB_BRIEF_PDF` | '1' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:3449 |
| `TFB_BRIEF_SEND` | '' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:3423 |
| `TFB_BRIEF_SHADOW_ROTATION` | '1' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:1532 |
| `TFB_BRIEF_STALE_HOURS` | '24' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:1099 |
| `TFB_BRIEF_STALE_PER_PAGE` | '1' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:1055 |
| `TFB_BRIEF_UNIT_COHERENCE` | '1' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:1265 |
| `TFB_BT_ADJUSTED` | '1' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:7902 |
| `TFB_CALENDAR_CONCURRENCY` | 4 | Render/library | absent | none | core/providers/calendar_provider.py:372; core/providers/calendar_provider.py:577 |
| `TFB_CALENDAR_DAYS_AHEAD` | 45 | Render/library | absent | none | core/providers/calendar_provider.py:315 |
| `TFB_CALENDAR_DIV_DAYS_AHEAD` | 90 | Render/library | absent | none | core/providers/calendar_provider.py:371 |
| `TFB_CALENDAR_EARNINGS_CHUNK` | 50 | Render/library | absent | none | core/providers/calendar_provider.py:316 |
| `TFB_CALENDAR_ENABLED` | '0' | Render/library | absent | calendar_sync.yml:96 `"1"` | core/providers/calendar_provider.py:220; core/providers/calendar_provider.py:225 |
| `TFB_CALENDAR_PAGES` | 'Top_10_Investments,My_Portfolio' | Actions: calendar_sync.yml | absent | none | scripts/run_calendar_sync.py:401 |
| `TFB_CALENDAR_SHEET` | 'Calendar_Events' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml; Actions: calendar_sync.yml | absent | none | scripts/run_calendar_sync.py:403; scripts/track_performance.py:8965 |
| `TFB_CALENDAR_TICKER_GUARD` | '1' | Actions: calendar_sync.yml | absent | none | scripts/run_calendar_sync.py:139 |
| `TFB_CALENDAR_TIMEOUT_SEC` | 12 | Render/library | absent | none | core/providers/calendar_provider.py:288; core/providers/calendar_provider.py:527 |
| `TFB_CALIBRATION_ADJUST` | '' | Render/library | absent | none | core/data_engine_v2.py:6513 |
| `TFB_CALIBRATION_DOWNWARD_ONLY` | '' | Render/library | absent | none | core/data_engine_v2.py:6522 |
| `TFB_CALIBRATION_FACTORS` | '' | Render/library | absent | none | core/data_engine_v2.py:6533 |
| `TFB_CAL_YAHOO_FALLBACK` | '1' | Render/library | absent | none | core/providers/calendar_provider.py:448 |
| `TFB_CANDLE_STRUCTURE` | '' | Render/library | absent | none | core/data_engine_v2.py:9281 |
| `TFB_CANDLE_STRUCTURE_MOMENTUM` | '', False | Render/library | absent | none | core/data_engine_v2.py:9317; core/scoring.py:1877 |
| `TFB_CANDLE_STRUCTURE_MOMENTUM_WEIGHT` | 0.6 | Render/library | absent | none | core/scoring.py:1884 |
| `TFB_CANDLE_STRUCTURE_STSIGNAL` | '' | Render/library | absent | none | core/data_engine_v2.py:9299 |
| `TFB_CHART_IDENTITY_GUARD` | '1' | Render/library | absent | none | core/providers/yahoo_chart_provider.py:932 |
| `TFB_CHART_PRICE_COHERENCE_GUARD` | '0' | Render/library | absent | none | core/providers/yahoo_chart_provider.py:939 |
| `TFB_CHART_PRICE_RATIO_HIGH` | 8.0 | Render/library | absent | none | core/providers/yahoo_chart_provider.py:944 |
| `TFB_CHART_PRICE_RATIO_LOW` | 0.125 | Render/library | absent | none | core/providers/yahoo_chart_provider.py:952 |
| `TFB_CLEAR_STALE_IDENTITY` | '' | Render/library | absent | none | core/data_engine_v2.py:16686 |
| `TFB_COLLAPSE_RECOMMENDATION_TO_6TIER` | '' | Render/library | absent | none | core/data_engine_v2.py:4418 |
| `TFB_COLOR` | False | Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:255 |
| `TFB_COMMODITY_TEST_SYMBOL` | 'GC=F' | Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:219 |
| `TFB_COMPLIANCE_AUTHORITY_MAX_AGE_DAYS` | '120' | Render/library | absent | none | core/compliance_gate.py:124 |
| `TFB_COMPLIANCE_DEBT_RATIO_MAX` | '0.30' | Render/library | absent | none | core/compliance_gate.py:127 |
| `TFB_COMPLIANCE_GATE_ENABLED` | '0' | Render/library | declared (default '0') | shadow_board.yml:73 `"1"`; shadow_scorer.yml:40 `"1"`; weekly_brief.yml:30 `"1"` | core/compliance_gate.py:113 |
| `TFB_COMPLIANCE_IMPURE_INCOME_MAX` | '0.05' | Render/library | absent | none | core/compliance_gate.py:130 |
| `TFB_COMPLIANCE_MAX_POSITION_PCT` | '0.15' | Render/library | absent | none | core/compliance_gate.py:133 |
| `TFB_COMPLIANCE_MODEL_SCREEN_ENABLED` | '1' | Render/library | absent | none | core/compliance_gate.py:116 |
| `TFB_COMPLIANCE_SCREEN_RETIRED` | '0' | Render/library | absent | shadow_board.yml:80 `"1"`; shadow_scorer.yml:50 `"1"` | core/compliance_gate.py:121 |
| `TFB_COMPLIANCE_SURFACE_GATE` | '1' | Render/library | declared (default '1') | none | core/analysis/opportunity_builder.py:4310 |
| `TFB_CONF_CLUSTER_TAG` | '' | Render/library | absent | none | core/data_engine_v2.py:6322 |
| `TFB_CONF_CLUSTER_VALUES` | '66.41,64.82,63.23,58.0' | Render/library | absent | none | core/data_engine_v2.py:6333 |
| `TFB_CONSERVATIVE_GATE` | '' | Render/library | absent | none | core/data_engine_v2.py:7088 |
| `TFB_CONSERVATIVE_GATE_RISK_CAP` | computed | Render/library | absent | none | core/data_engine_v2.py:7122 |
| `TFB_CONSERVATIVE_GATE_SCORE_FLOOR` | computed | Render/library | absent | none | core/data_engine_v2.py:7108 |
| `TFB_CORE_BUILDER_TIMEOUT_SEC` | 45.0 | Render/library | absent | none | config.py:548 |
| `TFB_CORS_ORIGINS` | 'CORS_ORIGINS' | Render/library | absent | none | config.py:521 |
| `TFB_CRITERIA_SNAPSHOT_MAX_CHARS` | '2000' | Render/library | absent | none | core/data_engine_v2.py:17100 |
| `TFB_CRYPTO_NAME_GATE` | 'off' | Render/library | declared (default 'off') | none | core/data_engine_v2.py:12546 |
| `TFB_DEBUG` | False | Render/library | absent | none | config.py:482 |
| `TFB_DECISION_SOURCE_MAX_AGE_H` | 30, 30.0 | Actions: daily_sync.yml; Actions: decision_surface_freshness.yml, workflow_repair_tests.yml | absent | daily_sync.yml:1866 `'30'`; decision_surface_freshness.yml:84 `'30'`; workflow_repair_tests.yml:71 `'30'` | scripts/audit_decision_surface_freshness.py:520; scripts/finalize_decision_publication.py:59 |
| `TFB_DECISION_SURFACE_MAX_AGE_H` | 8, 8.0 | Actions: daily_sync.yml; Actions: decision_surface_freshness.yml, workflow_repair_tests.yml | absent | daily_sync.yml:1867 `'8'`; decision_surface_freshness.yml:83 `'8'`; workflow_repair_tests.yml:70 `'8'` | scripts/audit_decision_surface_freshness.py:522; scripts/finalize_decision_publication.py:60 |
| `TFB_DEFAULT_PAGE` | 'Market_Leaders' | Render/library | absent | none | config.py:522 |
| `TFB_DEGRADED_PROVIDER_LABEL` | '' | Render/library | absent | none | core/data_engine_v2.py:12485; core/data_engine_v2.py:16713 |
| `TFB_DISABLE_V572_SANITIZATION` | '' | Render/library | absent | none | core/data_engine_v2.py:11928 |
| `TFB_DIVERGENCE_ROI_MIN` | '' | Render/library | absent | none | core/data_engine_v2.py:6308 |
| `TFB_DIVERGENCE_TAG` | '' | Render/library | absent | none | core/data_engine_v2.py:6299 |
| `TFB_DQ_AUDIT_MODE` | 'off' | Actions: workbook_backup.yml | declared (default 'off') | workbook_backup.yml:166 `${{ vars.TFB_DQ_AUDIT_MODE }}` | scripts/tfb_workbook_dq_audit.py:3264 |
| `TFB_DQ_COHERENCE_LEGACY` | '0' | Render/library | absent | none | core/data_engine_v2.py:7216 |
| `TFB_ELIGIBILITY_GATE` | '1' | Render/library | declared (default '1') | none | core/analysis/opportunity_builder.py:4340 |
| `TFB_ENGINE_52W_CEILING_SCRUB` | '1' | Render/library | absent | none | core/data_engine_v2.py:11981 |
| `TFB_ENGINE_BATCH_FPRINT` | '0' | Render/library | absent | none | core/data_engine_v2.py:12922 |
| `TFB_ENGINE_CALL_TIMEOUT_SEC` | 45.0 | Render/library | absent | none | config.py:540 |
| `TFB_ENGINE_CHART_META_CLASS_GUARD` | '0' | Render/library | absent | none | core/data_engine_v2.py:6676 |
| `TFB_ENGINE_FUND_LKG` | '1' | Render/library | absent | none | core/data_engine_v2.py:13035 |
| `TFB_ENGINE_FUND_LKG_MAX` | '20000' | Render/library | absent | none | core/data_engine_v2.py:13069 |
| `TFB_ENGINE_FUND_LKG_MIN_FIELDS` | '8' | Render/library | absent | none | core/data_engine_v2.py:13054 |
| `TFB_ENGINE_FUND_LKG_REDIS` | '0' | Render/library | absent | none | core/data_engine_v2.py:13236 |
| `TFB_ENGINE_FUND_LKG_TTL_H` | '72' | Render/library | absent | none | core/data_engine_v2.py:13043 |
| `TFB_ENGINE_IDENTITY_ECHO` | '' | Render/library | absent | none | core/data_engine_v2.py:5683 |
| `TFB_ENGINE_IDENTITY_GUARD` | '1' | Render/library | absent | none | core/data_engine_v2.py:12460 |
| `TFB_ENGINE_IDENTITY_RESCUE` | '1' | Render/library | absent | none | core/data_engine_v2.py:6711 |
| `TFB_ENGINE_OHLC_COHERENCE` | '0' | Render/library | absent | none | core/data_engine_v2.py:12714 |
| `TFB_ENGINE_OHLC_COHERENCE_FINAL` | '' | Render/library | absent | none | core/data_engine_v2.py:12820 |
| `TFB_ENGINE_OHLC_COHERENCE_MODE` | '' | Render/library | absent | none | core/data_engine_v2.py:12833 |
| `TFB_ENGINE_OHLC_RANGE_TOL` | 0.01 | Render/library | absent | none | core/data_engine_v2.py:12735 |
| `TFB_ENGINE_OHLC_RATIO_HIGH` | 3.0 | Render/library | absent | none | core/data_engine_v2.py:12719 |
| `TFB_ENGINE_OHLC_RATIO_LOW` | computed | Render/library | absent | none | core/data_engine_v2.py:12727 |
| `TFB_ENGINE_PAIR_BIND` | '1' | Render/library | absent | none | core/data_engine_v2.py:19437 |
| `TFB_ENGINE_PE_COHERENCE` | '1' | Render/library | absent | none | core/data_engine_v2.py:12678 |
| `TFB_ENGINE_PRICE_COHERENCE` | '1' | Render/library | absent | none | core/data_engine_v2.py:12631 |
| `TFB_ENGINE_PRICE_RATIO_HIGH` | 8.0 | Render/library | absent | none | core/data_engine_v2.py:12636 |
| `TFB_ENGINE_PRICE_RATIO_LOW` | 0.125 | Render/library | absent | none | core/data_engine_v2.py:12644 |
| `TFB_ENGINE_SHEET_IDENTITY_GUARD` | '1' | Render/library | absent | none | core/data_engine_v2.py:17684 |
| `TFB_ENGINE_SUKUK_CLASS` | '' | Render/library | absent | none | core/data_engine_v2.py:5894 |
| `TFB_ENGINE_TARGET_KLG` | '0' | Render/library | absent | none | core/data_engine_v2.py:13693 |
| `TFB_ENGINE_TARGET_KLG_MAX` | '20000' | Render/library | absent | none | core/data_engine_v2.py:13706 |
| `TFB_ENGINE_TARGET_KLG_REDIS` | '0' | Render/library | absent | none | core/data_engine_v2.py:13736 |
| `TFB_ENGINE_TARGET_KLG_TTL_H` | '72' | Render/library | absent | none | core/data_engine_v2.py:13698 |
| `TFB_ENRICH_COMPLIANCE` | '0' | Render/library | declared (default '0') | none | core/enriched_quote.py:3678 |
| `TFB_ENTRY_VERSION` | '' | Render/library | absent | none | config.py:474 |
| `TFB_ENV` | 'production', None | Render/library | absent | none | config.py:478; core/config.py:1210; routes/advanced_analysis.py:1878 |
| `TFB_EODHD_API_KEY` | '' | Actions: board_scorecard.yml; Actions: shadow_scorer.yml | absent | shadow_scorer.yml:79 `${{ secrets.EODHD_API_KEY }}` | scripts/run_shadow_scorer.py:1295; scripts/run_shadow_scorer.py:2250; scripts/score_selection_log.py:244; scripts/score_selection_log.py:963 |
| `TFB_EODHD_COHERENCE_GUARD` | '0' | Render/library | absent | none | core/providers/eodhd_provider.py:1321 |
| `TFB_EODHD_CRYPTO_MAP` | False | Render/library | declared (default '0') | none | core/symbols/normalize.py:1346 |
| `TFB_EODHD_DAILY_BUDGET` | 0.0 | Render/library | absent | none | core/providers/eodhd_provider.py:2286 |
| `TFB_EODHD_ENGINE_PATCH_BIND` | '0' | Render/library | absent | none | core/providers/eodhd_provider.py:1331 |
| `TFB_EODHD_EOD_WITNESS` | True | Render/library | absent | none | core/providers/eodhd_provider.py:1265 |
| `TFB_EODHD_FUNDAMENTALS_FALLBACK` | '' | Render/library | absent | none | core/data_engine_v2.py:4766 |
| `TFB_EODHD_FUND_CACHE` | '' | Render/library | absent | none | core/data_engine_v2.py:13427 |
| `TFB_EODHD_FUND_CACHE_TTL_H` | '24' | Render/library | absent | none | core/data_engine_v2.py:13435 |
| `TFB_EODHD_FUND_FALLBACK_SKIP_PAGES` | '' | Render/library | absent | none | core/data_engine_v2.py:13461 |
| `TFB_EODHD_FUND_NEG_TTL_H` | '168' | Render/library | absent | none | core/data_engine_v2.py:13452 |
| `TFB_EODHD_FX_MAP` | False | Render/library | absent | none | core/symbols/normalize.py:1337 |
| `TFB_EODHD_IDENTITY_GUARD` | '1' | Render/library | absent | none | core/providers/eodhd_provider.py:1875 |
| `TFB_EODHD_PLAN_RESTRICTED_ISOLATION` | '1' | Render/library | absent | none | core/providers/eodhd_provider.py:955 |
| `TFB_EODHD_PLAN_RESTRICTED_TOKENS` | '' | Render/library | absent | none | core/providers/eodhd_provider.py:967 |
| `TFB_EODHD_PLAN_RESTRICTED_TTL_SEC` | '86400' | Render/library | absent | none | core/providers/eodhd_provider.py:960 |
| `TFB_EODHD_SENTINEL_SCRUB` | '1' | Render/library | absent | none | core/providers/eodhd_provider.py:1313 |
| `TFB_EODHD_UNIT_AWARE` | '1' | Render/library | absent | none | core/data_engine_v2.py:4650; core/providers/eodhd_provider.py:1690 |
| `TFB_EQ_ROI_UNIT_SENTRY` | '' | Render/library | declared (default 'off') | none | core/data_engine_v2.py:11790; core/enriched_quote.py:1622 |
| `TFB_EXIT_BY_RULE_EXTRA` | computed | Render/library | declared (default '') | none | core/analysis/opportunity_builder.py:4361 |
| `TFB_EXIT_BY_RULE_GATE` | '1' | Render/library | declared (default '1') | none | core/analysis/portfolio_actions.py:1197 |
| `TFB_EXPECTED_MIN_ROWS_COMMODITIES_FX` | 453 | Actions: decision_surface_freshness.yml, workflow_repair_tests.yml | absent | daily_sync.yml:1870 `${{ vars.EXPECTED_MIN_ROWS_COMMODITIES_FX &#124;&#124; '453' }}`; decision_surface_freshness.yml:94 `${{ vars.EXPECTED_MIN_ROWS_COMMODITIES_FX &#124;&#124; '453' }}`; full_refresh_coverage.yml:92 `${{ vars.EXPECTED_MIN_ROWS_COMMODITIES_FX &#124;&#124; '453' }}`; workflow_repair_tests.yml:68 `${{ vars.EXPECTED_MIN_ROWS_COMMODITIES_FX &#124;&#124; '453' }}` | scripts/audit_decision_surface_freshness.py:352 |
| `TFB_EXPECTED_MIN_ROWS_GLOBAL_MARKETS` | 6512 | Actions: decision_surface_freshness.yml, workflow_repair_tests.yml | absent | daily_sync.yml:1869 `${{ vars.EXPECTED_MIN_ROWS_GLOBAL_MARKETS &#124;&#124; '6512' }}`; decision_surface_freshness.yml:93 `${{ vars.EXPECTED_MIN_ROWS_GLOBAL_MARKETS &#124;&#124; '6512' }}`; full_refresh_coverage.yml:91 `${{ vars.EXPECTED_MIN_ROWS_GLOBAL_MARKETS &#124;&#124; '6512' }}`; workflow_repair_tests.yml:67 `${{ vars.EXPECTED_MIN_ROWS_GLOBAL_MARKETS &#124;&#124; '6512' }}` | scripts/audit_decision_surface_freshness.py:351 |
| `TFB_EXPECTED_MIN_ROWS_MARKET_LEADERS` | 1025 | Actions: decision_surface_freshness.yml, workflow_repair_tests.yml | absent | daily_sync.yml:1868 `${{ vars.EXPECTED_MIN_ROWS_MARKET_LEADERS &#124;&#124; '1025' }}`; decision_surface_freshness.yml:92 `${{ vars.EXPECTED_MIN_ROWS_MARKET_LEADERS &#124;&#124; '1025' }}`; full_refresh_coverage.yml:90 `${{ vars.EXPECTED_MIN_ROWS_MARKET_LEADERS &#124;&#124; '1025' }}`; workflow_repair_tests.yml:66 `${{ vars.EXPECTED_MIN_ROWS_MARKET_LEADERS &#124;&#124; '1025' }}` | scripts/audit_decision_surface_freshness.py:350 |
| `TFB_EXPECTED_MIN_ROWS_MUTUAL_FUNDS` | 4496 | Actions: decision_surface_freshness.yml, workflow_repair_tests.yml | absent | daily_sync.yml:1871 `${{ vars.EXPECTED_MIN_ROWS_MUTUAL_FUNDS &#124;&#124; '4496' }}`; decision_surface_freshness.yml:95 `${{ vars.EXPECTED_MIN_ROWS_MUTUAL_FUNDS &#124;&#124; '4496' }}`; full_refresh_coverage.yml:93 `${{ vars.EXPECTED_MIN_ROWS_MUTUAL_FUNDS &#124;&#124; '4496' }}`; workflow_repair_tests.yml:69 `${{ vars.EXPECTED_MIN_ROWS_MUTUAL_FUNDS &#124;&#124; '4496' }}` | scripts/audit_decision_surface_freshness.py:353 |
| `TFB_FC_SOURCE_TYPE_GUARD` | '' | Render/library | absent | none | core/data_engine_v2.py:6350 |
| `TFB_FC_TUPLE_COHERENT` | '' | Render/library | absent | none | core/data_engine_v2.py:7520 |
| `TFB_FINAL_ACTION_INVARIANT` | '1' | Render/library | absent | none | core/data_engine_v2.py:13992 |
| `TFB_FIXED_INCOME_SYMBOLS` | '5023.SR' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml; Render/library | absent | none | core/data_engine_v2.py:5910; scripts/run_dashboard_sync.py:4836 |
| `TFB_FLOW` | '' | Render/library | absent | none | core/data_engine_v2.py:9382 |
| `TFB_FLOW_SCORE` | '', False | Render/library | absent | none | core/data_engine_v2.py:9391; core/scoring.py:1980 |
| `TFB_FLOW_SCORE_WEIGHT` | 0.1 | Render/library | absent | none | core/scoring.py:1987 |
| `TFB_FORECAST_BASIS` | '', 'legacy' | Render/library | absent | none | core/analysis/opportunity_builder.py:2616; core/analysis/portfolio_actions.py:3191 |
| `TFB_FORECAST_PAIR_COHERENCE` | False | Render/library | absent | none | core/data_engine_v2.py:16208 |
| `TFB_FUNDAMENTALS_WARN` | None | Render/library | absent | none | env.py:1329 |
| `TFB_FUND_IDENTITY_GUARD` | '1' | Render/library | absent | none | core/data_engine_v2.py:12577; core/providers/yahoo_fundamentals_provider.py:505 |
| `TFB_FUND_PRICE_BAND_HIGH` | 2.0 | Render/library | absent | none | core/providers/yahoo_fundamentals_provider.py:524 |
| `TFB_FUND_PRICE_BAND_LOW` | 0.5 | Render/library | absent | none | core/providers/yahoo_fundamentals_provider.py:533 |
| `TFB_FUND_PRICE_COHERENCE_GUARD` | '1' | Render/library | absent | none | core/providers/yahoo_fundamentals_provider.py:515 |
| `TFB_FUND_UNIT_SENTRY` | '' | Render/library | absent | none | core/data_engine_v2.py:4613 |
| `TFB_GATE_BLOCK_NEGATIVE_ROI` | '' | Render/library | absent | none | core/data_engine_v2.py:6200 |
| `TFB_GATE_FINANCIALS_FUNDAMENTALS_EXEMPT` | '' | Render/library | absent | none | core/data_engine_v2.py:6230 |
| `TFB_GATE_PRICE_STALENESS` | '1' | Render/library | absent | none | core/data_engine_v2.py:12430 |
| `TFB_GATE_PROVIDER_BAR_AGE` | '' | Render/library | absent | none | core/data_engine_v2.py:12438 |
| `TFB_GATE_RECO_COHERENCE` | '' | Render/library | absent | none | core/data_engine_v2.py:7017 |
| `TFB_GLOBAL_ACTIVITY_SCREEN` | '' | Render/library | absent | none | core/analysis/opportunity_builder.py:4390 |
| `TFB_GLOBAL_AUTH_ENFORCE` | True | Render/library | absent | none | main.py:1260 |
| `TFB_GLOBAL_TEST_SYMBOL` | 'AAPL' | Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:217 |
| `TFB_GRID_REPAIR_APPLY` | no static consumer | declaration/mapping only | absent | grid_capacity_repair.yml:73 `${{ inputs.apply == 'true' && '1' &#124;&#124; '0' }}` |  |
| `TFB_GRID_REPAIR_MIN_BUFFER` | computed | Actions: grid_capacity_repair.yml | absent | none | scripts/repair_grid_capacity.py:418 |
| `TFB_HARNESS_IC` | False | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:7342 |
| `TFB_HEALTH_STRICT` | False | Render/library | absent | none | main.py:2334 |
| `TFB_HISTORY_GAP_BACKFILL` | '' | Render/library | absent | none | core/data_engine_v2.py:16079 |
| `TFB_HORIZON_COHERENT` | '' | Render/library | declared (default '') | none | core/data_engine_v2.py:15919 |
| `TFB_HOST` | '0.0.0.0' | Render/library | absent | none | config.py:491 |
| `TFB_IDENTITY_QUARANTINE_KEYS` | '0' | Render/library | absent | none | core/analysis/identity_guard.py:552 |
| `TFB_IDENTITY_WIPE_RAISE` | '0' | Render/library | absent | none | core/analysis/identity_guard.py:1056 |
| `TFB_IDG_SCHEMA_SHIFT` | '' | Render/library | absent | none | core/analysis/identity_guard.py:402 |
| `TFB_INLINE_RECOVERY_MAX_CYCLES` | '1' | Actions: daily_sync.yml, page_refresh_recovery.yml, sync_outcome_audit.yml | absent | daily_sync.yml:1657 `"1"` | scripts/run_inline_page_recovery.py:59 |
| `TFB_INLINE_RECOVERY_QUOTA_GUARD` | '' | Actions: daily_sync.yml, page_refresh_recovery.yml, sync_outcome_audit.yml | absent | daily_sync.yml:1701 `"observe"` | scripts/run_inline_page_recovery.py:85 |
| `TFB_INLINE_RECOVERY_QUOTA_SKIP_PCT` | '' | Actions: daily_sync.yml, page_refresh_recovery.yml, sync_outcome_audit.yml | absent | none | scripts/run_inline_page_recovery.py:91 |
| `TFB_INSIGHTS_COMMODITIES_FX` | 'GC=F,BZ=F,USDSAR=X,EURUSD=X' | Render/library | absent | none | core/analysis/insights_builder.py:3309 |
| `TFB_INSIGHTS_DECISION_MAX_SYMBOLS` | 120 | Render/library | absent | none | core/analysis/insights_builder.py:625 |
| `TFB_INSIGHTS_DECISION_SCOPE` | False | Render/library | absent | none | core/analysis/insights_builder.py:639 |
| `TFB_INSIGHTS_EQUITY_MAX_SYMBOLS` | 25 | Render/library | absent | none | core/analysis/insights_builder.py:613 |
| `TFB_INSIGHTS_EQUITY_PAGES` | computed | Render/library | absent | none | core/analysis/insights_builder.py:668 |
| `TFB_INSIGHTS_EQUITY_UNIVERSE` | False | Render/library | absent | none | core/analysis/insights_builder.py:631 |
| `TFB_INSIGHTS_FLOW` | False | Render/library | absent | none | core/analysis/insights_builder.py:656 |
| `TFB_INSIGHTS_INDICES` | 'TASI,NOMU,^GSPC,^IXIC,^FTSE' | Render/library | absent | none | core/analysis/insights_builder.py:3308 |
| `TFB_INSIGHTS_SCAN_LIMIT` | 3000 | Render/library | absent | none | core/data_engine_v2.py:7175 |
| `TFB_INSIGHTS_STRUCTURE` | False | Render/library | absent | none | core/analysis/insights_builder.py:648 |
| `TFB_INTRINSIC_DISPLAY_CAP` | '' | Render/library | absent | none | core/data_engine_v2.py:5459 |
| `TFB_INTRINSIC_DISPLAY_MAX_PCT` | 35.0 | Render/library | absent | none | core/data_engine_v2.py:5469 |
| `TFB_INTRINSIC_DISPLAY_SOFTCAP` | '' | Render/library | absent | none | core/data_engine_v2.py:5662 |
| `TFB_INTRINSIC_DISPLAY_SOFTCAP_BAND` | computed | Render/library | absent | none | core/data_engine_v2.py:5671 |
| `TFB_INVESTABILITY_GATE` | '' | Render/library | absent | none | core/data_engine_v2.py:6186 |
| `TFB_INVESTMENT_POLICY_PATH` | '' | Render/library | absent | investment_policy_shadow.yml:22 `config/project_wide_execution_learning_policy_v1.json` | core/investment_policy.py:68 |
| `TFB_IQR_MAX_SYMBOLS` | 400 | Actions: intraday_refresh.yml | absent | intraday_refresh.yml:206 `"400"` | scripts/intraday_quote_refresh.py:600 |
| `TFB_IQR_SYMBOL_PAGES` | 'Top_10_Investments,My_Portfolio,Shadow_Board' | Actions: intraday_refresh.yml | absent | intraday_refresh.yml:204 `"Top_10_Investments,My_Portfolio,Shadow_Board"` | scripts/intraday_quote_refresh.py:590 |
| `TFB_IQR_TARGET_PAGES` | 'Market_Leaders,Global_Markets' | Actions: intraday_refresh.yml | absent | intraday_refresh.yml:205 `"Market_Leaders,Global_Markets"` | scripts/intraday_quote_refresh.py:593 |
| `TFB_IQR_TIMEOUT_SEC` | 45 | Actions: intraday_refresh.yml | absent | intraday_refresh.yml:207 `"45"` | scripts/intraday_quote_refresh.py:601 |
| `TFB_JSON_OUTPUT` | None | Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:1350 |
| `TFB_KSA_FOREIGN_RESTRICTED` | '4030.SR' | Render/library | declared (default '4030.SR') | none | core/analysis/opportunity_builder.py:4362 |
| `TFB_KSA_SECTOR_MAP` | '' | Render/library | absent | none | core/data_engine_v2.py:15359 |
| `TFB_KSA_TEST_SYMBOL` | '1120.SR' | Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:216 |
| `TFB_KW_FILS_NORMALIZE` | '' | Render/library | absent | none | core/data_engine_v2.py:5272 |
| `TFB_LOG_LEVEL` | 'INFO' | Render/library | absent | none | config.py:483 |
| `TFB_LOG_REPLACE_ROOT_HANDLERS` | False | Render/library | absent | none | main.py:750 |
| `TFB_MAIL_FROM` | '' | Actions: ci.yml, daily_brief.yml; Actions: weekly_brief.yml | absent | daily_brief.yml:116 `${{ secrets.TFB_MAIL_FROM }}`; weekly_brief.yml:32 `${{ secrets.TFB_MAIL_FROM }}` | scripts/run_daily_brief.py:3357; scripts/run_weekly_brief.py:281 |
| `TFB_MAIL_PASSWORD` | '' | Actions: ci.yml, daily_brief.yml; Actions: weekly_brief.yml | absent | daily_brief.yml:117 `${{ secrets.TFB_MAIL_PASSWORD }}`; weekly_brief.yml:33 `${{ secrets.TFB_MAIL_PASSWORD }}` | scripts/run_daily_brief.py:3358; scripts/run_weekly_brief.py:282 |
| `TFB_MAIL_SMTP_HOST` | 'smtp.gmail.com' | Actions: ci.yml, daily_brief.yml; Actions: weekly_brief.yml | absent | none | scripts/run_daily_brief.py:3355; scripts/run_weekly_brief.py:279 |
| `TFB_MAIL_SMTP_PORT` | '587' | Actions: ci.yml, daily_brief.yml; Actions: weekly_brief.yml | absent | none | scripts/run_daily_brief.py:3356; scripts/run_weekly_brief.py:280 |
| `TFB_MAIL_TO` | '' | Actions: ci.yml, daily_brief.yml; Actions: weekly_brief.yml | absent | daily_brief.yml:118 `${{ secrets.TFB_MAIL_TO }}`; weekly_brief.yml:34 `${{ secrets.TFB_MAIL_TO }}` | scripts/run_daily_brief.py:3359; scripts/run_weekly_brief.py:283 |
| `TFB_MARGIN_PUBLISH` | '' | Render/library | declared (default 'off') | none | core/data_engine_v2.py:7313 |
| `TFB_MARKET_REGIME` | 'neutral' | Render/library | absent | none | core/data_engine_v2.py:7096 |
| `TFB_MARKET_SYMBOL_READBACK` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:5413 |
| `TFB_MARKET_SYMBOL_READBACK_PAGES` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:5420 |
| `TFB_MAX_LIMIT` | 5000 | Render/library | absent | none | config.py:527 |
| `TFB_MIN_TICKET_SAR` | '' | Actions: ci.yml, daily_brief.yml | absent | none | scripts/run_daily_brief.py:918 |
| `TFB_MISSING_PRICE_HOLD_GUARD` | '' | Render/library | absent | none | core/data_engine_v2.py:5882 |
| `TFB_MP_BLOCKED_NULLS` | '0' | Render/library | absent | none | core/data_engine_v2.py:10875 |
| `TFB_NAME_UNRESOLVED_TAG` | '' | Render/library | absent | none | core/data_engine_v2.py:6748 |
| `TFB_NEWS_ARCHIVE_SECTOR_JOIN` | '0' | Actions: news_archive.yml | absent | none | scripts/news_archive_collector.py:106 |
| `TFB_NEWS_DISPLAY` | False | Render/library | absent | none | routes/advanced_analysis.py:913 |
| `TFB_NEWS_DISPLAY_MAX` | 60.0 | Render/library | absent | none | routes/advanced_analysis.py:1228 |
| `TFB_NEWS_DISPLAY_NEG` | -0.15 | Render/library | absent | none | routes/advanced_analysis.py:1243 |
| `TFB_NEWS_DISPLAY_POS` | 0.15 | Render/library | absent | none | routes/advanced_analysis.py:1241 |
| `TFB_NEWS_DISPLAY_TIMEOUT_S` | 25.0 | Render/library | absent | none | routes/advanced_analysis.py:1223 |
| `TFB_NEWS_VETO` | '' | Render/library | absent | none | core/data_engine_v2.py:7140 |
| `TFB_NEWS_VETO_MIN_ARTICLES` | computed | Render/library | absent | none | core/data_engine_v2.py:7158 |
| `TFB_NEWS_VETO_SENTIMENT_FLOOR` | computed | Render/library | absent | none | core/data_engine_v2.py:7149 |
| `TFB_NOPE_X` | 30 | Actions: workbook_backup.yml | absent | none | scripts/backup_workbook.py:1024 |
| `TFB_ONLY` | None | Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:1303 |
| `TFB_OPEN_MODE` | False | Render/library | absent | none | config.py:445; routes/advanced_analysis.py:2217 |
| `TFB_OPP_ANN_LEGACY_COPY` | '0' | Render/library | absent | none | core/analysis/opportunity_builder.py:2597 |
| `TFB_OPP_AUDIT_ORDER` | 'depth' | Render/library | absent | none | core/analysis/opportunity_builder.py:2858 |
| `TFB_OPP_AUDIT_ROI_LEGACY` | '0' | Render/library | absent | none | core/analysis/opportunity_builder.py:2586 |
| `TFB_OPP_AUDIT_ROWS_MAX` | computed | Render/library | declared (default '0') | none | core/analysis/opportunity_builder.py:2957 |
| `TFB_OPP_BLENDED_RR_BASIS` | 'tp2' | Render/library | absent | none | core/analysis/opportunity_builder.py:4371 |
| `TFB_OPP_BLOCKED_IDENTITY_GATE` | '' | Render/library | absent | none | core/analysis/opportunity_builder.py:2732 |
| `TFB_OPP_BUILD_OFFLOOP` | True | Render/library | declared (default '0') | none | routes/advanced_analysis.py:1167 |
| `TFB_OPP_BUILD_TIMEOUT_S` | 70.0 | Render/library | declared (default '70') | none | routes/advanced_analysis.py:1172 |
| `TFB_OPP_CANON_MARKET` | '1' | Render/library | absent | none | core/analysis/opportunity_builder.py:2906 |
| `TFB_OPP_CAPITAL_CALL_TOPN` | 3 | Render/library | absent | none | core/analysis/opportunity_builder.py:5450 |
| `TFB_OPP_CASH_FLOOR_MODE` | 'observe' | Render/library | absent | none | core/analysis/opportunity_builder.py:5289 |
| `TFB_OPP_CASH_FLOOR_PCT` | '' | Render/library | absent | none | core/analysis/opportunity_builder.py:5270 |
| `TFB_OPP_CASH_FLOOR_SAR` | '' | Render/library | absent | none | core/analysis/opportunity_builder.py:5243 |
| `TFB_OPP_DISTRIBUTED_LOCK` | computed | Render/library | absent | none | routes/advanced_analysis.py:955 |
| `TFB_OPP_DISTRIBUTED_LOCK_REQUIRED` | False | Render/library | absent | none | routes/advanced_analysis.py:969 |
| `TFB_OPP_ENABLED` | '1' | Render/library | absent | none | core/analysis/opportunity_builder.py:2201 |
| `TFB_OPP_ENGINE_ROI_DISPLAY` | '0' | Render/library | absent | none | core/analysis/opportunity_builder.py:2524 |
| `TFB_OPP_EXPLICIT_ROWS_MAX` | 5000.0 | Render/library | absent | none | routes/advanced_analysis.py:1213 |
| `TFB_OPP_FORECAST_GATE` | '1' | Render/library | absent | none | core/analysis/opportunity_builder.py:2500 |
| `TFB_OPP_FUNDING_PLAN` | '1' | Render/library | absent | none | core/analysis/opportunity_builder.py:5423 |
| `TFB_OPP_FUNDING_SETTLED_ONLY` | '0' | Render/library | absent | none | core/analysis/opportunity_builder.py:5675 |
| `TFB_OPP_FX_SANITY` | '0' | Render/library | absent | none | core/analysis/opportunity_builder.py:3013 |
| `TFB_OPP_FX_USX_SUBUNIT` | '', '0' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml; Render/library; script/operator (no direct workflow launch found) | declared (default '0') | daily_sync.yml:400 `${{ vars.TFB_OPP_FX_USX_SUBUNIT &#124;&#124; '0' }}`; daily_sync.yml:1585 `${{ vars.TFB_OPP_FX_USX_SUBUNIT &#124;&#124; '0' }}` | core/analysis/identity_guard.py:346; core/analysis/opportunity_builder.py:3018; core/data_engine_v2.py:5184; core/portfolio_reconciliation.py:43; core/providers/eodhd_provider.py:688; core/surface_action_invariants.py:283; scripts/audit_workbook.py:660; scripts/run_dashboard_sync.py:5148 |
| `TFB_OPP_HELD_MATCH_VARIANTS` | '1' | Render/library | absent | none | core/analysis/opportunity_builder.py:2684 |
| `TFB_OPP_HELD_TARGET_NO_NEW_MONEY` | '0' | Render/library | absent | none | core/analysis/opportunity_builder.py:3024 |
| `TFB_OPP_INVESTABILITY_GATE` | '0' | Render/library | declared (default '0') | none | core/analysis/opportunity_builder.py:2751 |
| `TFB_OPP_ISSUER_DEDUP` | '0' | Render/library | absent | none | core/analysis/opportunity_builder.py:2878 |
| `TFB_OPP_LOCK_KEY` | 'tfb:opportunity:build-lock' | Render/library | absent | none | routes/advanced_analysis.py:978 |
| `TFB_OPP_LOCK_TTL_S` | 900.0 | Render/library | absent | none | routes/advanced_analysis.py:973 |
| `TFB_OPP_LOT_SIZE` | computed | Render/library | absent | none | core/analysis/opportunity_builder.py:2917 |
| `TFB_OPP_MAX_CANDIDATES` | computed | Render/library | declared (default '0') | none | core/analysis/opportunity_builder.py:2927 |
| `TFB_OPP_MAX_DATA_AGE_HOURS` | computed | Render/library | absent | none | core/analysis/opportunity_builder.py:2943 |
| `TFB_OPP_MAX_VALUATION_ROI_PCT` | computed | Render/library | absent | none | core/analysis/opportunity_builder.py:2936 |
| `TFB_OPP_MAX_WEIGHT_PCT` | computed | Render/library | absent | none | core/analysis/opportunity_builder.py:2915 |
| `TFB_OPP_MIN_ENGINE_ROI_PCT` | computed | Render/library | absent | none | core/analysis/opportunity_builder.py:2932 |
| `TFB_OPP_MIN_TICKET_SAR` | computed | Render/library | absent | none | core/analysis/opportunity_builder.py:2954 |
| `TFB_OPP_MIN_TRUST_FIELDS` | computed | Render/library | absent | none | core/analysis/opportunity_builder.py:2946 |
| `TFB_OPP_NEARMISS_TEXT_LEGACY` | '0' | Render/library | absent | none | core/analysis/opportunity_builder.py:3078 |
| `TFB_OPP_NEAR_MISS_N` | computed | Render/library | absent | none | core/analysis/opportunity_builder.py:2913 |
| `TFB_OPP_NETEDGE_ANNOTATE` | '0' | Render/library | declared (default '0') | none | core/analysis/opportunity_builder.py:2993 |
| `TFB_OPP_PF_MAX_SECTOR_PCT` | computed | Render/library | absent | none | core/analysis/opportunity_builder.py:2930 |
| `TFB_OPP_POOL_TIMEOUT_S` | 70.0 | Render/library | absent | none | routes/advanced_analysis.py:1184 |
| `TFB_OPP_PREGATE_ORDER` | '1' | Render/library | declared (default '1') | none | core/analysis/opportunity_builder.py:2887 |
| `TFB_OPP_PRIMARY_ROI_BASIS` | 'plan' | Render/library | absent | none | core/analysis/opportunity_builder.py:2672 |
| `TFB_OPP_RANK_BY_ENGINE_ROI` | '0' | Render/library | declared (default '0') | none | core/analysis/opportunity_builder.py:2868 |
| `TFB_OPP_REF_CONSERVATIVE` | '0' | Render/library | declared (default '0') | none | core/analysis/opportunity_builder.py:2532 |
| `TFB_OPP_RESEARCH_DIVERSIFY` | '' | Render/library | absent | none | core/analysis/opportunity_builder.py:2830 |
| `TFB_OPP_REVIEW_DAYS` | computed | Render/library | absent | none | core/analysis/opportunity_builder.py:2925 |
| `TFB_OPP_ROTATION_COST_PCT` | 1.1 | Render/library | absent | none | core/analysis/opportunity_builder.py:5437 |
| `TFB_OPP_ROTATION_EDGE_PP` | 8.0 | Render/library | absent | none | core/analysis/opportunity_builder.py:5429 |
| `TFB_OPP_ROTATION_EXCLUDE` | '' | Render/library | absent | none | core/analysis/opportunity_builder.py:5444 |
| `TFB_OPP_ROTATION_FLOOR_GUARD` | '1' | Render/library | absent | none | core/analysis/opportunity_builder.py:5464 |
| `TFB_OPP_ROTATION_MIN_HELD_DAYS` | 7 | Render/library | absent | none | core/analysis/opportunity_builder.py:5513 |
| `TFB_OPP_ROTATION_TP1_PROXIMITY_PCT` | 3 | Render/library | absent | none | core/analysis/opportunity_builder.py:5521 |
| `TFB_OPP_SCAN_UNCAPPED` | '0' | Render/library | absent | none | core/analysis/opportunity_builder.py:2897 |
| `TFB_OPP_SECTOR_CAP_BASIS` | 'budget' | Render/library | absent | none | core/analysis/opportunity_builder.py:5363 |
| `TFB_OPP_SECTOR_NORMALIZE` | '1' | Render/library | absent | none | core/analysis/opportunity_builder.py:2843 |
| `TFB_OPP_SELL_CLASS_GATE` | '1' | Render/library | declared (default '1') | none | core/analysis/opportunity_builder.py:2508 |
| `TFB_OPP_SHARIAH_MODEL_GATE` | '' | Render/library | absent | none | core/analysis/opportunity_builder.py:4348 |
| `TFB_OPP_SIZE_AT_ENTRY_HIGH` | '0' | Render/library | absent | none | core/analysis/opportunity_builder.py:5683 |
| `TFB_OPP_STOP_FLOOR_PCT` | computed | Render/library | absent | none | core/analysis/opportunity_builder.py:2919 |
| `TFB_OPP_STOP_MAX_PCT` | computed | Render/library | absent | none | core/analysis/opportunity_builder.py:2923 |
| `TFB_OPP_STOP_VOL_MULT` | computed | Render/library | declared (default '2.5') | none | core/analysis/opportunity_builder.py:2921 |
| `TFB_OPP_STOP_VOL_UNITS_FIX` | '0' | Render/library | declared (default '0') | none | core/analysis/opportunity_builder.py:6061 |
| `TFB_OPP_TOTAL_TIMEOUT_S` | 88.0 | Render/library | absent | none | routes/advanced_analysis.py:1178 |
| `TFB_OPP_TRUST_GATE` | '1' | Render/library | absent | none | core/analysis/opportunity_builder.py:2711 |
| `TFB_OPP_TRUST_LINEAGE` | '' | Render/library | absent | none | core/analysis/opportunity_builder.py:2818 |
| `TFB_OPP_UNFUNDED_WATCH` | '0' | Render/library | absent | none | core/analysis/opportunity_builder.py:2762 |
| `TFB_OPP_VALUATION_SANITY_GATE` | '1' | Render/library | absent | none | core/analysis/opportunity_builder.py:2515 |
| `TFB_OPP_VENUE_FLOORS` | '0' | Render/library | declared (default '0') | none | core/analysis/opportunity_builder.py:2997 |
| `TFB_PAGE_DEDUPE_SYMBOLS` | '' | Render/library | absent | none | core/data_engine_v2.py:12086 |
| `TFB_PAGE_MAX_ROWS_COMMODITIES_FX` | 2000 | Render/library | absent | none | core/symbols_reader.py:753 |
| `TFB_PAGE_MAX_ROWS_DEFAULT` | 8000 | Render/library | absent | none | core/symbols_reader.py:756 |
| `TFB_PAGE_MAX_ROWS_GLOBAL_MARKETS` | 10000 | Render/library | absent | none | core/symbols_reader.py:752 |
| `TFB_PAGE_MAX_ROWS_MARKET_LEADERS` | 3000 | Render/library | absent | none | core/symbols_reader.py:751 |
| `TFB_PAGE_MAX_ROWS_MUTUAL_FUNDS` | 8000 | Render/library | absent | none | core/symbols_reader.py:754 |
| `TFB_PA_HOLDEDGE_ANNOTATE` | '0' | Render/library | declared (default '0') | none | core/analysis/portfolio_actions.py:1528 |
| `TFB_PA_PRECEDENCE_GATE` | '1' | Render/library | declared (default '1') | none | core/analysis/portfolio_actions.py:1158 |
| `TFB_PA_PROTECT_SUKUK` | '1' | Render/library | declared (default '1') | none | core/analysis/portfolio_actions.py:1709 |
| `TFB_PA_REDUCE_ACTION` | 'exit' | Render/library | absent | none | core/analysis/portfolio_actions.py:3112 |
| `TFB_PA_SUKUK_ASSET_CLASS` | '0' | Render/library | declared (default '0') | none | core/analysis/portfolio_actions.py:1684 |
| `TFB_PA_SUKUK_LADDER_LEGACY` | '0' | Render/library | absent | none | core/analysis/portfolio_actions.py:3025 |
| `TFB_PERCENT_MODE` | '', 'points' | Render/library; script/operator (no direct workflow launch found) | absent | none | env.py:1276; scripts/setup_sheet_headers.py:1458 |
| `TFB_PERF_TARGET_UNIT_SENTRY` | '', None | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | daily_sync.yml:1224 `"observe"`; track_performance.yml:345 `"observe"` | scripts/track_performance.py:4081; scripts/track_performance.py:9878 |
| `TFB_PF_ADD_CONFIRM_DAYS` | computed | Render/library | absent | none | core/analysis/portfolio_actions.py:1514 |
| `TFB_PF_ADD_CONFIRM_LEGACY_FAILOPEN` | '0' | Render/library | absent | none | core/analysis/portfolio_actions.py:1219 |
| `TFB_PF_ADD_LOSER_PCT` | 2.0 | Render/library | absent | none | core/analysis/portfolio_actions.py:3309 |
| `TFB_PF_ADD_LOSER_VETO` | '' | Render/library | absent | none | core/analysis/portfolio_actions.py:3302 |
| `TFB_PF_ADD_ROI_3M_PCT` | computed | Render/library | absent | none | core/analysis/portfolio_actions.py:1479 |
| `TFB_PF_ADD_ROI_PCT` | computed | Render/library | absent | none | core/analysis/portfolio_actions.py:1476 |
| `TFB_PF_ADD_STOP_PROX_PCT` | 2.0 | Render/library | absent | none | core/analysis/portfolio_actions.py:3315 |
| `TFB_PF_BASE_CCY` | None | Render/library | absent | none | core/data_engine_v2.py:11161; core/data_engine_v2.py:11221 |
| `TFB_PF_BLOCK_MISSING_COST_BASIS` | '0' | Render/library | absent | none | core/analysis/portfolio_actions.py:1405 |
| `TFB_PF_BLOCK_THIN_COVERAGE` | '0' | Render/library | absent | none | core/analysis/portfolio_actions.py:1420 |
| `TFB_PF_BUILD_OFFLOOP` | False | Render/library | absent | none | routes/advanced_analysis.py:1205 |
| `TFB_PF_BUILD_TIMEOUT_S` | 20.0 | Render/library | absent | none | routes/advanced_analysis.py:1199 |
| `TFB_PF_CONFIRM_PERSIST` | '1' | Render/library | declared (default '1') | none | core/analysis/portfolio_actions.py:1209 |
| `TFB_PF_CONFIRM_SESSION` | '' | Render/library | absent | none | core/analysis/portfolio_actions.py:2267 |
| `TFB_PF_COST_BASIS_GATE` | '1' | Render/library | absent | none | core/analysis/portfolio_actions.py:1397 |
| `TFB_PF_COST_MAX_RATIO` | computed | Render/library | absent | none | core/analysis/portfolio_actions.py:1495 |
| `TFB_PF_COST_MIN_RATIO` | computed | Render/library | absent | none | core/analysis/portfolio_actions.py:1497 |
| `TFB_PF_DD_EXIT` | '' | Render/library | absent | none | core/analysis/portfolio_actions.py:3182 |
| `TFB_PF_DD_EXIT_PCT` | 8.0 | Render/library | absent | none | core/analysis/portfolio_actions.py:3262 |
| `TFB_PF_DD_TIME_D` | 45 | Render/library | absent | none | core/analysis/portfolio_actions.py:3263 |
| `TFB_PF_ENABLED` | '1' | Render/library | absent | none | core/analysis/portfolio_actions.py:1151 |
| `TFB_PF_ENGINE_ROI_DISPLAY` | '0' | Render/library | absent | none | core/analysis/portfolio_actions.py:1455 |
| `TFB_PF_EXIT_ROI_PCT` | computed | Render/library | absent | none | core/analysis/portfolio_actions.py:1483 |
| `TFB_PF_FEE_FUNDING` | '' | Render/library | absent | none | core/analysis/portfolio_actions.py:2952 |
| `TFB_PF_FEE_SAR` | 9.0 | Render/library | absent | none | core/analysis/portfolio_actions.py:2955 |
| `TFB_PF_FX_AS_OF` | None | Render/library | absent | none | core/data_engine_v2.py:11111; core/data_engine_v2.py:19603 |
| `TFB_PF_FX_MAX_AGE_DAYS` | None | Render/library | absent | none | core/data_engine_v2.py:11262 |
| `TFB_PF_FX_RATES` | None | Render/library | absent | none | core/data_engine_v2.py:11165; core/data_engine_v2.py:11223 |
| `TFB_PF_HOLDINGS_MAX` | 5000.0 | Render/library | absent | none | routes/advanced_analysis.py:1218 |
| `TFB_PF_IDENTITY_GATE` | '0' | Render/library | declared (default '0') | none | core/analysis/portfolio_actions.py:1429 |
| `TFB_PF_IDENTITY_MIN_RELIABILITY` | computed | Render/library | absent | none | core/analysis/portfolio_actions.py:1508 |
| `TFB_PF_LOT_SIZE` | computed | Render/library | absent | none | core/analysis/portfolio_actions.py:1490 |
| `TFB_PF_MAX_DATA_AGE_HOURS` | computed | Render/library | absent | none | core/analysis/portfolio_actions.py:1501 |
| `TFB_PF_MAX_HOLDINGS` | computed | Render/library | absent | none | core/analysis/portfolio_actions.py:1491 |
| `TFB_PF_MINOR_UNIT_CCY_GUARD` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:519 `${{ vars.TFB_PF_MINOR_UNIT_CCY_GUARD &#124;&#124; '1' }}`; daily_sync.yml:1537 `${{ vars.TFB_PF_MINOR_UNIT_CCY_GUARD &#124;&#124; '1' }}`; page_refresh_recovery.yml:182 `${{ vars.TFB_PF_MINOR_UNIT_CCY_GUARD &#124;&#124; '1' }}` | scripts/run_dashboard_sync.py:5156 |
| `TFB_PF_MIN_TICKET_SAR` | '0' | Render/library | absent | none | core/analysis/portfolio_actions.py:1016 |
| `TFB_PF_MIN_TRUST_FIELDS` | computed | Render/library | absent | none | core/analysis/portfolio_actions.py:1504 |
| `TFB_PF_NULL_LEVELS` | '1' | Render/library | absent | none | core/analysis/portfolio_actions.py:3011 |
| `TFB_PF_REQUIRE_FRESH_BASIS` | '0' | Render/library | absent | none | core/analysis/portfolio_actions.py:1438 |
| `TFB_PF_REVIEW_DAYS` | computed | Render/library | absent | none | core/analysis/portfolio_actions.py:1488 |
| `TFB_PF_SESSION_HOLIDAYS` | '' | Render/library | absent | none | core/analysis/portfolio_actions.py:2304 |
| `TFB_PF_SWITCH_BUFFER_PCT` | 1.0 | Render/library | absent | none | core/analysis/portfolio_actions.py:1600 |
| `TFB_PF_SWITCH_MAXAGE_H` | 6.0 | Render/library | absent | none | core/analysis/portfolio_actions.py:1604 |
| `TFB_PF_SWITCH_SCAN` | '0' | Render/library | absent | none | core/analysis/portfolio_actions.py:1595 |
| `TFB_PF_TRIM_ROI_PCT` | computed | Render/library | absent | none | core/analysis/portfolio_actions.py:1481 |
| `TFB_PF_TRUST_GATE` | '1' | Render/library | absent | none | core/analysis/portfolio_actions.py:1412 |
| `TFB_PF_VALUATION_TRIM_FRAC` | computed | Render/library | absent | none | core/analysis/portfolio_actions.py:1485 |
| `TFB_PF_VF_CONFLICT_GUARD` | '0' | Render/library | declared (default '0') | none | core/analysis/portfolio_actions.py:1447 |
| `TFB_PF_VF_CONFLICT_MIN_ENGINE_ROI` | computed | Render/library | absent | none | core/analysis/portfolio_actions.py:1516 |
| `TFB_PF_WEIGHT_FX` | '1' | Render/library | absent | none | core/data_engine_v2.py:11103 |
| `TFB_PIT_SHEET` | '_PIT_Fundamentals' | Actions: pit_snapshot.yml | absent | none | scripts/pit_snapshot.py:343; scripts/pit_snapshot.py:370 |
| `TFB_PIT_SOURCE_PAGES` | 'Market_Leaders,Global_Markets,Mutual_Funds' | Actions: pit_snapshot.yml | absent | none | scripts/pit_snapshot.py:367 |
| `TFB_PIT_SYMBOL_PAGES` | 'Top_10_Investments,My_Portfolio,Shadow_Board' | Actions: pit_snapshot.yml | absent | none | scripts/pit_snapshot.py:364 |
| `TFB_PORTFOLIO_REBALANCE_BAND_PP` | None | Render/library | absent | none | core/data_engine_v2.py:10778 |
| `TFB_PORTFOLIO_REBUILD` | '0' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:514 `"1"`; daily_sync.yml:1532 `"1"`; page_refresh_recovery.yml:177 `'1'` | scripts/run_dashboard_sync.py:4829 |
| `TFB_PORTFOLIO_TARGETS` | '' | Render/library | absent | none | core/data_engine_v2.py:10760 |
| `TFB_PORTFOLIO_WEAK_SCORE` | None | Render/library | absent | none | core/data_engine_v2.py:10784 |
| `TFB_POSITION_MATH_AUTHORITATIVE` | '' | Render/library | absent | none | core/data_engine_v2.py:10864 |
| `TFB_PRICE_PRECISION` | '' | Render/library | declared (default '0') | none | core/price_precision.py:18 |
| `TFB_PROVIDER_ATTRIBUTION_FIX` | '1' | Render/library | absent | none | core/data_engine_v2.py:14038 |
| `TFB_PROVIDER_HEALTH_TIMEOUT_S` | 3.0 | Render/library | absent | none | routes/advanced_analysis.py:1189 |
| `TFB_PROVIDER_POOL_WORKERS` | '200' | Render/library | absent | none | core/data_engine_v2.py:17194 |
| `TFB_PROVIDER_TARGET_GATE_ENABLED` | '' | Render/library | absent | none | core/data_engine_v2.py:15518 |
| `TFB_PROVIDER_TARGET_MAX_MULT` | computed | Render/library | absent | none | core/data_engine_v2.py:15527 |
| `TFB_PROVIDER_TARGET_MIN_MULT` | computed | Render/library | absent | none | core/data_engine_v2.py:15526 |
| `TFB_PROVIDER_TARGET_SOFT_CAP` | '' | Render/library | absent | none | core/data_engine_v2.py:10141 |
| `TFB_PROVIDER_TARGET_SOFT_CAP_BAND` | computed | Render/library | absent | none | core/data_engine_v2.py:10152 |
| `TFB_PTC_ACCEPT_BASELINE` | computed | Actions: provider_target_coverage.yml, workflow_repair_tests.yml | absent | provider_target_coverage.yml:105 `${{ github.event.inputs.accept_baseline == 'true' && '1' &#124;&#124; '0' }}`; workflow_repair_tests.yml:81 `'0'` | scripts/audit_provider_target_coverage.py:754 |
| `TFB_PTC_BASELINE_MAX_AGE_H` | no static consumer | declaration/mapping only | absent | provider_target_coverage.yml:101 `${{ vars.TFB_PTC_BASELINE_MAX_AGE_H &#124;&#124; '72' }}`; workflow_repair_tests.yml:76 `${{ vars.TFB_PTC_BASELINE_MAX_AGE_H &#124;&#124; '72' }}` |  |
| `TFB_PTC_BOOTSTRAP` | computed | Actions: provider_target_coverage.yml, workflow_repair_tests.yml | absent | provider_target_coverage.yml:104 `${{ github.event.inputs.bootstrap == 'true' && '1' &#124;&#124; '0' }}`; workflow_repair_tests.yml:80 `'0'` | scripts/audit_provider_target_coverage.py:753 |
| `TFB_PTC_DROP_PCT` | no static consumer | declaration/mapping only | absent | provider_target_coverage.yml:97 `${{ github.event.inputs.drop_pct &#124;&#124; vars.TFB_PTC_DROP_PCT &#124;&#124; '20' }}`; workflow_repair_tests.yml:72 `${{ vars.TFB_PTC_DROP_PCT &#124;&#124; '20' }}` |  |
| `TFB_PTC_FLOOR_PCT` | no static consumer | declaration/mapping only | absent | provider_target_coverage.yml:100 `${{ vars.TFB_PTC_FLOOR_PCT &#124;&#124; '0' }}`; workflow_repair_tests.yml:75 `${{ vars.TFB_PTC_FLOOR_PCT &#124;&#124; '0' }}` |  |
| `TFB_PTC_FREEZE` | computed | Actions: provider_target_coverage.yml, workflow_repair_tests.yml | absent | provider_target_coverage.yml:106 `${{ vars.TFB_PTC_FREEZE &#124;&#124; '0' }}`; workflow_repair_tests.yml:79 `'1'` | scripts/audit_provider_target_coverage.py:609 |
| `TFB_PTC_MIN_ROWS` | no static consumer | declaration/mapping only | absent | provider_target_coverage.yml:99 `${{ vars.TFB_PTC_MIN_ROWS &#124;&#124; '50' }}`; workflow_repair_tests.yml:74 `${{ vars.TFB_PTC_MIN_ROWS &#124;&#124; '50' }}` |  |
| `TFB_PTC_PAGES` | '' | Actions: provider_target_coverage.yml, workflow_repair_tests.yml | absent | provider_target_coverage.yml:103 `${{ vars.TFB_PTC_PAGES &#124;&#124; '' }}`; workflow_repair_tests.yml:78 `${{ vars.TFB_PTC_PAGES &#124;&#124; '' }}` | scripts/audit_provider_target_coverage.py:736 |
| `TFB_PTC_ROW_DROP_PCT` | no static consumer | declaration/mapping only | absent | provider_target_coverage.yml:98 `${{ vars.TFB_PTC_ROW_DROP_PCT &#124;&#124; '30' }}`; workflow_repair_tests.yml:73 `${{ vars.TFB_PTC_ROW_DROP_PCT &#124;&#124; '30' }}` |  |
| `TFB_PTC_ZERO_PAGES` | '' | Actions: provider_target_coverage.yml, workflow_repair_tests.yml | absent | provider_target_coverage.yml:102 `${{ vars.TFB_PTC_ZERO_PAGES &#124;&#124; 'Commodities_FX,Mutual_Funds' }}`; workflow_repair_tests.yml:77 `${{ vars.TFB_PTC_ZERO_PAGES &#124;&#124; 'Commodities_FX,Mutual_Funds' }}` | scripts/audit_provider_target_coverage.py:146 |
| `TFB_QUOTE_CALL_TIMEOUT_SEC` | 45.0 | Render/library | absent | none | config.py:537 |
| `TFB_RANK_OVERALL_PAGE` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | declared (default 'off') | daily_sync.yml:584 `${{ vars.TFB_RANK_OVERALL_PAGE &#124;&#124; 'off' }}`; daily_sync.yml:1642 `${{ vars.TFB_RANK_OVERALL_PAGE &#124;&#124; 'off' }}`; page_refresh_recovery.yml:196 `${{ vars.TFB_RANK_OVERALL_PAGE &#124;&#124; 'off' }}` | scripts/run_dashboard_sync.py:9532 |
| `TFB_READYZ_STRICT` | False | Render/library | absent | none | main.py:2317 |
| `TFB_REFRESH_AUDIT_MAX_ROWS` | computed | Actions: decision_surface_freshness.yml, full_refresh_coverage.yml, workflow_repair_tests.yml | absent | full_refresh_coverage.yml:82 `"20000"` | scripts/audit_full_refresh_coverage.py:280 |
| `TFB_REFRESH_MAX_AGE_H_COMMODITIES_FX` | no static consumer | declaration/mapping only | absent | full_refresh_coverage.yml:101 `"30"` |  |
| `TFB_REFRESH_MAX_AGE_H_GLOBAL_MARKETS` | no static consumer | declaration/mapping only | absent | full_refresh_coverage.yml:100 `"30"` |  |
| `TFB_REFRESH_MAX_AGE_H_MARKET_LEADERS` | no static consumer | declaration/mapping only | absent | full_refresh_coverage.yml:99 `"30"` |  |
| `TFB_REFRESH_MAX_AGE_H_MUTUAL_FUNDS` | no static consumer | declaration/mapping only | absent | full_refresh_coverage.yml:102 `"30"` |  |
| `TFB_REFRESH_MAX_AGE_H_MY_PORTFOLIO` | 8, computed | Actions: daily_sync.yml; Actions: decision_surface_freshness.yml, full_refresh_coverage.yml, workflow_repair_tests.yml | absent | full_refresh_coverage.yml:103 `"8"` | scripts/audit_full_refresh_coverage.py:206; scripts/finalize_decision_publication.py:69 |
| `TFB_REFRESH_MIN_FRESH_PCT_COMMODITIES_FX` | no static consumer | declaration/mapping only | absent | full_refresh_coverage.yml:96 `"95"` |  |
| `TFB_REFRESH_MIN_FRESH_PCT_GLOBAL_MARKETS` | no static consumer | declaration/mapping only | absent | full_refresh_coverage.yml:95 `"95"` |  |
| `TFB_REFRESH_MIN_FRESH_PCT_MARKET_LEADERS` | no static consumer | declaration/mapping only | absent | full_refresh_coverage.yml:94 `"95"` |  |
| `TFB_REFRESH_MIN_FRESH_PCT_MUTUAL_FUNDS` | no static consumer | declaration/mapping only | absent | full_refresh_coverage.yml:97 `"95"` |  |
| `TFB_REFRESH_MIN_FRESH_PCT_MY_PORTFOLIO` | no static consumer | declaration/mapping only | absent | full_refresh_coverage.yml:98 `"100"` |  |
| `TFB_REGIME_BEAR_NEWS_DELTA` | computed | Render/library | absent | none | core/data_engine_v2.py:7151 |
| `TFB_REGIME_BEAR_RISK_DELTA` | computed | Render/library | absent | none | core/data_engine_v2.py:7125 |
| `TFB_REGIME_BEAR_SCORE_DELTA` | computed | Render/library | absent | none | core/data_engine_v2.py:7112 |
| `TFB_REGIME_BULL_SCORE_DELTA` | computed | Render/library | absent | none | core/data_engine_v2.py:7114 |
| `TFB_RELIABILITY_RECALIBRATION` | '' | Render/library | absent | none | core/data_engine_v2.py:6435 |
| `TFB_REL_PATH_TAG` | '' | Render/library | absent | none | core/data_engine_v2.py:6455 |
| `TFB_REQUEST_TIMEOUT_SEC` | 45.0 | Render/library | absent | none | config.py:534 |
| `TFB_REQUIRE_AUTH` | '', False, True | Render/library | absent | none | config.py:450; config.py:455; routes/advanced_analysis.py:2235 |
| `TFB_ROI12_SOFTCAP_GATE` | '' | Render/library | declared (default 'off') | none | core/data_engine_v2.py:7697 |
| `TFB_ROUTE_COHERENCE_MAX_BAD_PCT` | 25.0 | Render/library | absent | none | routes/analysis_sheet_rows.py:1486 |
| `TFB_ROUTE_COHERENCE_MIN_ROWS` | 50 | Render/library | absent | none | routes/analysis_sheet_rows.py:1497 |
| `TFB_ROUTE_COHERENCE_TRIPWIRE` | '' | Render/library | absent | none | routes/analysis_sheet_rows.py:1477 |
| `TFB_ROUTE_PE_COHERENCE` | '' | Render/library | absent | none | routes/analysis_sheet_rows.py:1469 |
| `TFB_ROUTE_REHYDRATE_CONCURRENCY` | 6 | Render/library | absent | none | config.py:531 |
| `TFB_ROW_SANITY_QUARANTINE` | 'TFB_SURFACE_ROW_SANITY', computed | Render/library | absent | none | core/data_engine_v2.py:17778; core/data_engine_v2.py:20099; core/data_engine_v2.py:6042; core/surface_action_invariants.py:328 |
| `TFB_RULE1B_CAPPED_EXIT_GATE` | '1' | Render/library | declared (default '1') | none | core/analysis/portfolio_actions.py:1318 |
| `TFB_S1_BASE_POLICY` | 'legacy', None | Actions: shadow_scorer.yml | absent | none | scripts/run_shadow_scorer.py:1615; scripts/run_shadow_scorer.py:2833 |
| `TFB_S1_BOARD_FRESH_GUARD` | '' | Actions: shadow_scorer.yml | absent | shadow_scorer.yml:62 `"observe"` | scripts/run_shadow_scorer.py:1370 |
| `TFB_S1_BOARD_WAIT_MIN` | '8' | Actions: shadow_scorer.yml | absent | none | scripts/run_shadow_scorer.py:1421 |
| `TFB_S1_CALIBRATION` | '1' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:3996 |
| `TFB_S1_CAL_BAND_PP` | '10' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:4054 |
| `TFB_S1_CAL_CONSUME` | '1' | Actions: shadow_scorer.yml | absent | none | scripts/run_shadow_scorer.py:606 |
| `TFB_S1_CAL_MAX_AGE_H` | '48' | Actions: shadow_scorer.yml | absent | none | scripts/run_shadow_scorer.py:612 |
| `TFB_S1_CAL_MIN_SAMPLE` | '20' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:4061 |
| `TFB_S1_CRITERIA_V2` | 'off', None | Actions: shadow_scorer.yml | absent | shadow_scorer.yml:69 `${{ vars.TFB_S1_CRITERIA_V2 &#124;&#124; 'off' }}` | scripts/run_shadow_scorer.py:2934; scripts/run_shadow_scorer.py:972 |
| `TFB_S1_DAY_KEY` | '', None | Actions: shadow_scorer.yml | absent | none | scripts/run_shadow_scorer.py:2787; scripts/run_shadow_scorer.py:522 |
| `TFB_S1_DRILL_LEGACY` | '0' | Actions: shadow_scorer.yml | absent | none | scripts/run_shadow_scorer.py:1807 |
| `TFB_S1_SLOT_UTC` | '15:20', None | Actions: shadow_scorer.yml | absent | none | scripts/run_shadow_scorer.py:2783; scripts/run_shadow_scorer.py:534 |
| `TFB_S1_WINDOW_START` | '' | Actions: shadow_scorer.yml | absent | none | scripts/run_shadow_scorer.py:981 |
| `TFB_S1_ZERO_BASELINE` | '0', None | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10035; scripts/track_performance.py:4006 |
| `TFB_SANITIZATION_ENABLED` | 'true' | Render/library | absent | none | core/data_engine_v2.py:11931 |
| `TFB_SANITIZE_PRIMARY_PATH` | '1' | Render/library | absent | none | core/data_engine_v2.py:11903 |
| `TFB_SANITIZE_SIGN_AWARE` | '1' | Render/library | absent | none | core/data_engine_v2.py:11910 |
| `TFB_SB_ATOMIC_WRITE` | '1' | Actions: shadow_board.yml | absent | none | scripts/run_shadow_board.py:723 |
| `TFB_SB_COST_LEGACY` | '' | Actions: shadow_board.yml | absent | none | scripts/run_shadow_board.py:591 |
| `TFB_SB_REGIME_HISTORY` | '1' | Actions: shadow_board.yml | absent | none | scripts/run_shadow_board.py:206 |
| `TFB_SCORE_ROI_SOFTCAP` | '0' | Render/library | declared (default '0') | none | core/scoring.py:764 |
| `TFB_SCORING_NULLABLE_OVERALL` | True | Render/library | absent | none | core/scoring.py:381 |
| `TFB_SCORING_REASON_FORMAT` | 'structured' | Render/library | absent | none | core/scoring.py:385 |
| `TFB_SCORING_ROI_PARSE_LEGACY` | '' | Render/library | absent | none | core/scoring.py:916; core/scoring.py:939 |
| `TFB_SCORING_SETTLE` | '' | Render/library | declared (default 'off') | none | core/data_engine_v2.py:9870 |
| `TFB_SCORING_SETTLE_MAX_PASSES` | computed | Render/library | absent | none | core/data_engine_v2.py:9877 |
| `TFB_SCORING_STRICT_BUCKET_LOGS` | True | Render/library | absent | none | core/scoring.py:395 |
| `TFB_SETTLE_UNRESOLVED` | '' | Render/library | declared (default 'off') | none | core/data_engine_v2.py:9655 |
| `TFB_SHADOW_BENCHMARK_EQW` | '1' | Actions: shadow_scorer.yml | absent | none | scripts/run_shadow_scorer.py:668 |
| `TFB_SHADOW_COMPLIANCE` | False | Render/library | declared (default '0') | none | core/analysis/top10_selector.py:4801 |
| `TFB_SHADOW_DAY_CLASS` | 'venue' | Actions: shadow_scorer.yml | absent | none | scripts/run_shadow_scorer.py:682 |
| `TFB_SHADOW_EODHD` | '0', None | Actions: shadow_scorer.yml | absent | shadow_scorer.yml:78 `${{ vars.TFB_SHADOW_EODHD }}` | scripts/run_shadow_scorer.py:1292; scripts/run_shadow_scorer.py:1324; scripts/run_shadow_scorer.py:2741 |
| `TFB_SHADOW_EQUITY_SAR` | '130000', 130000.0 | Actions: shadow_board.yml; Actions: weekly_brief.yml; Render/library | absent | shadow_board.yml:94 `${{ vars.TFB_SHADOW_EQUITY_SAR }}`; weekly_brief.yml:31 `${{ vars.TFB_SHADOW_EQUITY_SAR }}` | core/analysis/top10_selector.py:4814; scripts/run_shadow_board.py:778; scripts/run_weekly_brief.py:320 |
| `TFB_SHADOW_MIN_FRESH_PCT` | 60.0 | Actions: shadow_scorer.yml | declared (default '60') | none | scripts/run_shadow_scorer.py:598 |
| `TFB_SHADOW_PRICE_HONESTY` | '1' | Actions: shadow_scorer.yml | declared (default '1') | none | scripts/run_shadow_scorer.py:591 |
| `TFB_SHADOW_SHAPE_GUARD` | '1', None | Actions: shadow_scorer.yml | absent | none | scripts/run_shadow_scorer.py:1333; scripts/run_shadow_scorer.py:2702 |
| `TFB_SHARIAH_CACHE_TTL_SEC` | '21600' | Render/library | absent | none | core/shariah_authority.py:98 |
| `TFB_SHARIAH_FAIL_LIST` | computed | Render/library | declared (default '') | none | core/analysis/opportunity_builder.py:4358 |
| `TFB_SHARIAH_MONITOR_URLS` | computed | script/operator (no direct workflow launch found) | absent | none | scripts/refresh_shariah_authority.py:395 |
| `TFB_SHARIAH_OFFICIAL_URLS` | '' | script/operator (no direct workflow launch found) | absent | none | scripts/refresh_shariah_authority.py:394 |
| `TFB_SHARIAH_REFRESH_ALERT_DAYS` | '30' | script/operator (no direct workflow launch found) | absent | none | scripts/refresh_shariah_authority.py:315 |
| `TFB_SHARIAH_RETAIN_LAST_GOOD` | '1' | Render/library | absent | none | core/shariah_authority.py:104 |
| `TFB_SHARIAH_SHEET_ID` | None | Render/library; script/operator (no direct workflow launch found) | declared (default '') | none | core/shariah_authority.py:121; scripts/refresh_shariah_authority.py:235 |
| `TFB_SHEET_ROWS_READER` | None | Render/library | absent | none | core/sheets/rows_reader.py:100 |
| `TFB_SIGNAL_TRENDS_SHEET` | computed | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:8457 |
| `TFB_SIGNAL_TRENDS_TAB` | False | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | track_performance.yml:310 `"1"` | scripts/track_performance.py:8451 |
| `TFB_SKIP_META` | False | Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:1312 |
| `TFB_SKIP_QUOTES` | False | Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:1330 |
| `TFB_SKIP_SCHEMA` | False | Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:1336 |
| `TFB_SKIP_SHEET_ROWS` | False | Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:1318 |
| `TFB_SKIP_TOP10` | False | Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:1324 |
| `TFB_SNAPSHOT_POISON_REFUSAL` | '1' | Render/library | absent | none | core/data_engine_v2.py:12478 |
| `TFB_SNAPSHOT_PROVENANCE_LABEL` | '1' | Render/library | absent | none | core/data_engine_v2.py:12470 |
| `TFB_SPECIAL_BUILDER_TIMEOUT_SEC` | 45.0 | Render/library | absent | none | config.py:544 |
| `TFB_SPREADSHEET_ID` | None | Actions: grid_capacity_repair.yml | absent | none | scripts/repair_grid_capacity.py:408 |
| `TFB_SR_TRANSIENT_RETRY` | '1' | Render/library | declared (default '1') | none | core/symbols_reader.py:281 |
| `TFB_STATIC_NAME_FALLBACK` | '1' | Render/library | absent | none | core/data_engine_v2.py:6725; core/enriched_quote.py:1305 |
| `TFB_STATIC_NAME_MAP` | '' | Render/library | absent | none | core/data_engine_v2.py:6726; core/enriched_quote.py:1288 |
| `TFB_STRICT` | False | Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:1342 |
| `TFB_STRICT_INVEST_DQ_MIN` | computed | Render/library | absent | none | core/data_engine_v2.py:6409 |
| `TFB_STRICT_INVEST_GATE` | '' | Render/library | declared (default '0') | none | core/data_engine_v2.py:6402 |
| `TFB_STRICT_INVEST_RELIABILITY_MIN` | computed | Render/library | absent | none | core/data_engine_v2.py:6416 |
| `TFB_SUBUNIT_MARKET_CAP_NORMALIZE` | '' | Render/library | absent | none | core/data_engine_v2.py:5166 |
| `TFB_SURFACE_ACTION_PRECEDENCE` | '0' | Render/library | absent | none | core/analysis/identity_guard.py:561 |
| `TFB_SURFACE_BLOCKED_INVARIANT` | computed | Render/library | absent | none | core/surface_action_invariants.py:311 |
| `TFB_SURFACE_FETCHFAIL_BLOCKED` | computed | Render/library | absent | none | core/surface_action_invariants.py:316 |
| `TFB_SURFACE_ROW_SANITY` | computed | Render/library | absent | none | core/surface_action_invariants.py:328 |
| `TFB_SURFACE_WARN_INVEST` | computed | Render/library | absent | none | core/surface_action_invariants.py:322 |
| `TFB_SVC_CAPACITY_GUARD` | '1' | Render/library | absent | none | integrations/google_sheets_service.py:263 |
| `TFB_SYMBOLS_CACHE_BACKEND` | computed | Render/library | absent | none | core/symbols_reader.py:705 |
| `TFB_SYMBOLS_CACHE_COMPRESSION` | None | Render/library | absent | none | core/symbols_reader.py:716 |
| `TFB_SYMBOLS_CACHE_DISABLE` | None | Render/library | absent | none | core/symbols_reader.py:702 |
| `TFB_SYMBOLS_CACHE_MAX_SIZE` | computed | Render/library | absent | none | core/symbols_reader.py:709 |
| `TFB_SYMBOLS_CACHE_TTL_SEC` | computed | Render/library | absent | none | core/symbols_reader.py:696 |
| `TFB_SYMBOLS_DEFAULT_LIMIT` | 20000 | Render/library | absent | none | core/symbols_reader.py:760 |
| `TFB_SYMBOLS_LOG_DETECTION` | None | Render/library | absent | none | core/symbols_reader.py:726 |
| `TFB_SYMBOLS_LOG_PERFORMANCE` | None | Render/library | absent | none | core/symbols_reader.py:727 |
| `TFB_SYMBOLS_MAX_RETRIES` | 4 | Render/library | absent | none | core/symbols_reader.py:720 |
| `TFB_SYMBOLS_ML_CONFIDENCE` | 0.8 | Render/library | absent | none | core/symbols_reader.py:724 |
| `TFB_SYMBOLS_ML_DETECTION` | None | Render/library | absent | none | core/symbols_reader.py:723 |
| `TFB_SYMBOLS_PAGE_CAP_V2` | None | Render/library | absent | none | core/symbols_reader.py:749 |
| `TFB_SYMBOLS_TIMEOUT` | 30.0 | Render/library | absent | none | core/symbols_reader.py:721 |
| `TFB_SYMBOL_HEADER_ROW` | 5 | Render/library | absent | none | core/symbols_reader.py:692; env.py:1190 |
| `TFB_SYMBOL_MAX_ROWS` | 5000 | Render/library | absent | none | core/symbols_reader.py:694 |
| `TFB_SYMBOL_START_ROW` | 6 | Render/library | absent | none | core/symbols_reader.py:693; env.py:1193 |
| `TFB_SYM_BARE_ROOT_EQUITY` | False | Render/library | absent | none | core/symbols/normalize.py:607 |
| `TFB_SYM_CRYPTO_PAIR_CLASS` | '' | Render/library | absent | none | core/data_engine_v2.py:15102 |
| `TFB_SYNC_APPEND_FAIL_IS_ERROR` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:437 `${{ vars.TFB_SYNC_APPEND_FAIL_IS_ERROR &#124;&#124; '1' }}`; daily_sync.yml:1620 `${{ vars.TFB_SYNC_APPEND_FAIL_IS_ERROR &#124;&#124; '1' }}` | scripts/run_dashboard_sync.py:6914 |
| `TFB_SYNC_AUDIT_ACCEPT_CANCELLED` | no static consumer | declaration/mapping only | absent | sync_outcome_audit.yml:91 `${{ vars.TFB_SYNC_AUDIT_ACCEPT_CANCELLED &#124;&#124; '0' }}` |  |
| `TFB_SYNC_BATCH_DELAY_MS` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:11805 |
| `TFB_SYNC_BATCH_IDENTITY` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:10709 |
| `TFB_SYNC_BATCH_RETRY` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:11778 |
| `TFB_SYNC_CAPACITY_STATUS` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:429 `${{ vars.TFB_SYNC_CAPACITY_STATUS &#124;&#124; '1' }}`; daily_sync.yml:1614 `${{ vars.TFB_SYNC_CAPACITY_STATUS &#124;&#124; '1' }}` | scripts/run_dashboard_sync.py:9829 |
| `TFB_SYNC_COHERENCE_MAX_BAD_PCT` | None | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:10596 |
| `TFB_SYNC_COHERENCE_MIN_ROWS` | None | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:10605 |
| `TFB_SYNC_COHERENCE_TRIPWIRE` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:10588 |
| `TFB_SYNC_CRIT_FRONT` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:12418 |
| `TFB_SYNC_DECISION_GUARD` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:543 `"1"`; daily_sync.yml:1538 `"1"`; page_refresh_recovery.yml:183 `'1'` | scripts/run_dashboard_sync.py:4597 |
| `TFB_SYNC_DECISION_GUARD_PAGES` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:4604 |
| `TFB_SYNC_EMPTY_GUARD` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:4648 |
| `TFB_SYNC_EMPTY_RETRY` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:494 `"0"`; daily_sync.yml:1526 `"0"`; page_refresh_recovery.yml:170 `'0'` | scripts/run_dashboard_sync.py:4486 |
| `TFB_SYNC_EMPTY_RETRY_DELAY_SEC` | '0' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:4499 |
| `TFB_SYNC_EMPTY_RETRY_MAX` | '120' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:4491 |
| `TFB_SYNC_EODHD_QUOTA` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:295 `"observe"`; daily_sync.yml:1669 `"observe"` | scripts/run_dashboard_sync.py:7012 |
| `TFB_SYNC_EODHD_QUOTA_CRIT_PCT` | 90.0 | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:7251 |
| `TFB_SYNC_EODHD_QUOTA_GUARD` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:310 `"enforce"`; daily_sync.yml:1676 `"enforce"` | scripts/run_dashboard_sync.py:7316 |
| `TFB_SYNC_EODHD_QUOTA_GUARD_ALLOW_EXTRA` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:7342 |
| `TFB_SYNC_EODHD_QUOTA_GUARD_PCT` | '97' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:7323 |
| `TFB_SYNC_EODHD_QUOTA_GUARD_POISON_PCT` | '25' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:7333 |
| `TFB_SYNC_EODHD_QUOTA_TIMEOUT_S` | '5' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:7027 |
| `TFB_SYNC_EODHD_QUOTA_URL` | 'https://eodhd.com/api/user' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:7087 |
| `TFB_SYNC_EODHD_QUOTA_WARN_PCT` | 80.0 | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:7250 |
| `TFB_SYNC_FALSE_GREEN_SCREEN` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:430 `${{ vars.TFB_SYNC_FALSE_GREEN_SCREEN &#124;&#124; '1' }}`; daily_sync.yml:1615 `${{ vars.TFB_SYNC_FALSE_GREEN_SCREEN &#124;&#124; '1' }}` | scripts/run_dashboard_sync.py:5772 |
| `TFB_SYNC_FETCHFAIL_TRUTH` | 'off' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:317 `"observe"`; daily_sync.yml:1679 `"observe"` | scripts/run_dashboard_sync.py:8479 |
| `TFB_SYNC_FLOOR_STRICT` | '0' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:11785 |
| `TFB_SYNC_FORCE_REFETCH_SYMBOLS` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:387 `${{ vars.TFB_SYNC_FORCE_REFETCH_SYMBOLS &#124;&#124; '' }}`; daily_sync.yml:1575 `${{ vars.TFB_SYNC_FORCE_REFETCH_SYMBOLS &#124;&#124; '' }}` | scripts/run_dashboard_sync.py:5735 |
| `TFB_SYNC_FW_KEEP_LAST_GOOD` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:5883 |
| `TFB_SYNC_HEAL_FIRST` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:5430 |
| `TFB_SYNC_HOLD_POST_GRACE_SEC` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:422 `${{ vars.TFB_SYNC_HOLD_POST_GRACE_SEC &#124;&#124; '45' }}`; daily_sync.yml:1607 `${{ vars.TFB_SYNC_HOLD_POST_GRACE_SEC &#124;&#124; '45' }}` | scripts/run_dashboard_sync.py:7905 |
| `TFB_SYNC_HOLD_TTL_SEC` | '180' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:421 `${{ vars.TFB_SYNC_HOLD_TTL_SEC &#124;&#124; '180' }}`; daily_sync.yml:1606 `${{ vars.TFB_SYNC_HOLD_TTL_SEC &#124;&#124; '180' }}` | scripts/run_dashboard_sync.py:7870 |
| `TFB_SYNC_IDENTITY_ANCHORS_EXTRA` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:10730 |
| `TFB_SYNC_IDENTITY_MIN_FAILS` | None | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:10723 |
| `TFB_SYNC_IDENTITY_REFETCH` | '0' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:389 `${{ vars.TFB_SYNC_IDENTITY_REFETCH &#124;&#124; '0' }}`; daily_sync.yml:1577 `${{ vars.TFB_SYNC_IDENTITY_REFETCH &#124;&#124; '0' }}` | scripts/run_dashboard_sync.py:8969 |
| `TFB_SYNC_IDENTITY_TRIPWIRE` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:10716 |
| `TFB_SYNC_IDFW_RUNLOG` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:6921 |
| `TFB_SYNC_KEEP_LAST_GOOD` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:10047 |
| `TFB_SYNC_KLG_CRYPTO_NAME_GATE` | 'off' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:436 `${{ vars.TFB_SYNC_KLG_CRYPTO_NAME_GATE &#124;&#124; 'off' }}`; daily_sync.yml:1619 `${{ vars.TFB_SYNC_KLG_CRYPTO_NAME_GATE &#124;&#124; 'off' }}` | scripts/run_dashboard_sync.py:10178 |
| `TFB_SYNC_KLG_FETCHFAIL` | 'off', None | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:10110; scripts/run_dashboard_sync.py:10145 |
| `TFB_SYNC_KLG_IDENTITY_GATE` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:431 `${{ vars.TFB_SYNC_KLG_IDENTITY_GATE &#124;&#124; '1' }}`; daily_sync.yml:1616 `${{ vars.TFB_SYNC_KLG_IDENTITY_GATE &#124;&#124; '1' }}` | scripts/run_dashboard_sync.py:5746 |
| `TFB_SYNC_KLG_STUB_PROVIDERS` | None | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:10084 |
| `TFB_SYNC_LEG` | 'leg' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:7896 |
| `TFB_SYNC_MANUAL_GUARD` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:4297 |
| `TFB_SYNC_MANUAL_GUARD_PAGES` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:4291 |
| `TFB_SYNC_MANUAL_HOLD_GATE` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:11381 |
| `TFB_SYNC_MARKET_ANALYSIS_GATEWAY` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:11582 |
| `TFB_SYNC_MARKET_GATEWAY` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:644 `"analysis"`; daily_sync.yml:1662 `"analysis"`; page_refresh_recovery.yml:187 `analysis` | scripts/run_dashboard_sync.py:11593 |
| `TFB_SYNC_MAX_SYMBOLS_MARKET` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:622 `"7000"`; daily_sync.yml:1661 `"7000"`; page_refresh_recovery.yml:186 `'7000'` | scripts/run_dashboard_sync.py:11304 |
| `TFB_SYNC_MIN_COVERAGE_PCT` | '70' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:4635 |
| `TFB_SYNC_NAME_DEDUP_EXEMPT_NAMES` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:6526 |
| `TFB_SYNC_NAME_DEDUP_FAMILY_EXEMPT` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:6518 |
| `TFB_SYNC_NAME_DEDUP_MIN` | '3' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:6504 |
| `TFB_SYNC_NAME_DEDUP_MODE` | 'observe' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | declared (default '') | daily_sync.yml:513 `"observe"`; daily_sync.yml:1527 `"observe"`; page_refresh_recovery.yml:171 `observe` | scripts/run_dashboard_sync.py:6497 |
| `TFB_SYNC_NAME_DEDUP_SAFE` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:6538 |
| `TFB_SYNC_NULL_CLEAR_SCOPE` | 'ohlc' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:377 `${{ vars.TFB_SYNC_NULL_CLEAR_SCOPE &#124;&#124; 'ohlc' }}`; daily_sync.yml:1565 `${{ vars.TFB_SYNC_NULL_CLEAR_SCOPE &#124;&#124; 'ohlc' }}` | scripts/run_dashboard_sync.py:9226 |
| `TFB_SYNC_NULL_KEEP_COLS` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:378 `${{ vars.TFB_SYNC_NULL_KEEP_COLS &#124;&#124; '' }}`; daily_sync.yml:1566 `${{ vars.TFB_SYNC_NULL_KEEP_COLS &#124;&#124; '' }}` | scripts/run_dashboard_sync.py:9233 |
| `TFB_SYNC_OHLC_FILL_GUARD` | '0', computed | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:365 `${{ vars.TFB_SYNC_OHLC_FILL_GUARD &#124;&#124; '0' }}`; daily_sync.yml:1553 `${{ vars.TFB_SYNC_OHLC_FILL_GUARD &#124;&#124; '0' }}`; page_refresh_recovery.yml:174 `${{ vars.TFB_SYNC_OHLC_FILL_GUARD &#124;&#124; '0' }}` | scripts/run_dashboard_sync.py:8414; scripts/run_dashboard_sync.py:9187 |
| `TFB_SYNC_OHLC_FILL_GUARD_COLS` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:367 `${{ vars.TFB_SYNC_OHLC_FILL_GUARD_COLS &#124;&#124; '' }}`; daily_sync.yml:1555 `${{ vars.TFB_SYNC_OHLC_FILL_GUARD_COLS &#124;&#124; '' }}`; page_refresh_recovery.yml:176 `${{ vars.TFB_SYNC_OHLC_FILL_GUARD_COLS &#124;&#124; '' }}` | scripts/run_dashboard_sync.py:9199 |
| `TFB_SYNC_OHLC_FILL_GUARD_MODE` | '', computed | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:366 `${{ vars.TFB_SYNC_OHLC_FILL_GUARD_MODE &#124;&#124; 'observe' }}`; daily_sync.yml:1554 `${{ vars.TFB_SYNC_OHLC_FILL_GUARD_MODE &#124;&#124; 'observe' }}`; page_refresh_recovery.yml:175 `${{ vars.TFB_SYNC_OHLC_FILL_GUARD_MODE &#124;&#124; 'observe' }}` | scripts/run_dashboard_sync.py:8415; scripts/run_dashboard_sync.py:9193 |
| `TFB_SYNC_OHLC_LAKE` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:8980 |
| `TFB_SYNC_OHLC_PREWRITE` | '0' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:359 `${{ vars.TFB_SYNC_OHLC_PREWRITE &#124;&#124; '0' }}`; daily_sync.yml:1547 `${{ vars.TFB_SYNC_OHLC_PREWRITE &#124;&#124; '0' }}` | scripts/run_dashboard_sync.py:6246 |
| `TFB_SYNC_OHLC_PREWRITE_ENFORCE_CLASSES` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:424 `${{ vars.TFB_SYNC_OHLC_PREWRITE_ENFORCE_CLASSES &#124;&#124; '' }}`; daily_sync.yml:1609 `${{ vars.TFB_SYNC_OHLC_PREWRITE_ENFORCE_CLASSES &#124;&#124; '' }}` | scripts/run_dashboard_sync.py:8197 |
| `TFB_SYNC_OHLC_PREWRITE_MODE` | 'observe', None | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:360 `${{ vars.TFB_SYNC_OHLC_PREWRITE_MODE &#124;&#124; 'observe' }}`; daily_sync.yml:1548 `${{ vars.TFB_SYNC_OHLC_PREWRITE_MODE &#124;&#124; 'observe' }}` | scripts/run_dashboard_sync.py:6253; scripts/run_dashboard_sync.py:7698; scripts/run_dashboard_sync.py:9062 |
| `TFB_SYNC_OHLC_PREWRITE_TOL` | '0.01' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:361 `${{ vars.TFB_SYNC_OHLC_PREWRITE_TOL &#124;&#124; '0.01' }}`; daily_sync.yml:1549 `${{ vars.TFB_SYNC_OHLC_PREWRITE_TOL &#124;&#124; '0.01' }}` | scripts/run_dashboard_sync.py:6260 |
| `TFB_SYNC_OHLC_READBACK` | '0' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:406 `${{ vars.TFB_SYNC_OHLC_READBACK &#124;&#124; '0' }}`; daily_sync.yml:1591 `${{ vars.TFB_SYNC_OHLC_READBACK &#124;&#124; '0' }}` | scripts/run_dashboard_sync.py:6300 |
| `TFB_SYNC_OHLC_RUNLOG` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:362 `${{ vars.TFB_SYNC_OHLC_RUNLOG &#124;&#124; '1' }}`; daily_sync.yml:1550 `${{ vars.TFB_SYNC_OHLC_RUNLOG &#124;&#124; '1' }}` | scripts/run_dashboard_sync.py:7508 |
| `TFB_SYNC_OLDEST_FIRST` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:12530 |
| `TFB_SYNC_PAGE_LIMIT_FIX` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:4625 |
| `TFB_SYNC_PAGE_ORDER` | '', None | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:487 `${{ matrix.page_order }}`; page_refresh_recovery.yml:169 `${{ matrix.page }}` | scripts/run_dashboard_sync.py:4431; scripts/run_dashboard_sync.py:7896 |
| `TFB_SYNC_PAGE_READ_MAX_ROW` | '' | Actions: ci.yml, daily_brief.yml; Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml; Actions: digest.yml | absent | none | scripts/run_daily_brief.py:748; scripts/run_dashboard_sync.py:11273; scripts/send_digest.py:194 |
| `TFB_SYNC_PERSISTENCE_HARD` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:5620 |
| `TFB_SYNC_PERSIST_SANITY` | '0' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:286 `"1"`; daily_sync.yml:1531 `"1"` | scripts/run_dashboard_sync.py:6051 |
| `TFB_SYNC_PERSIST_V2` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:5626 |
| `TFB_SYNC_PLACEHOLDER_GUARD` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:5912 |
| `TFB_SYNC_PRIORITY_EXTRA` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:12436 |
| `TFB_SYNC_PRIORITY_FETCH` | '0' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:553 `"0"`; daily_sync.yml:1631 `"0"` | scripts/run_dashboard_sync.py:12410 |
| `TFB_SYNC_PRIORITY_MAX` | '150' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:12427 |
| `TFB_SYNC_QUARANTINE_SYMBOLS` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:388 `${{ vars.TFB_SYNC_QUARANTINE_SYMBOLS &#124;&#124; '' }}`; daily_sync.yml:1576 `${{ vars.TFB_SYNC_QUARANTINE_SYMBOLS &#124;&#124; '' }}` | scripts/run_dashboard_sync.py:6122 |
| `TFB_SYNC_RB_TOL_PCT` | '0', computed | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:372 `${{ vars.TFB_SYNC_RB_TOL_PCT &#124;&#124; '0' }}`; daily_sync.yml:1560 `${{ vars.TFB_SYNC_RB_TOL_PCT &#124;&#124; '0' }}` | scripts/run_dashboard_sync.py:8416; scripts/run_dashboard_sync.py:8431 |
| `TFB_SYNC_RB_TOL_ROWS` | '0', computed | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:371 `${{ vars.TFB_SYNC_RB_TOL_ROWS &#124;&#124; '0' }}`; daily_sync.yml:1559 `${{ vars.TFB_SYNC_RB_TOL_ROWS &#124;&#124; '0' }}` | scripts/run_dashboard_sync.py:8416; scripts/run_dashboard_sync.py:8427 |
| `TFB_SYNC_READBACK_EMPTY_GUARD` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:5611 |
| `TFB_SYNC_READBACK_REPAIR` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:423 `${{ vars.TFB_SYNC_READBACK_REPAIR &#124;&#124; '' }}`; daily_sync.yml:1608 `${{ vars.TFB_SYNC_READBACK_REPAIR &#124;&#124; '' }}` | scripts/run_dashboard_sync.py:8207 |
| `TFB_SYNC_RETIRE_MAX_PCT` | '20' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:397 `${{ vars.TFB_SYNC_RETIRE_MAX_PCT &#124;&#124; '20' }}`; daily_sync.yml:1583 `${{ vars.TFB_SYNC_RETIRE_MAX_PCT &#124;&#124; '20' }}` | scripts/run_dashboard_sync.py:10880 |
| `TFB_SYNC_RETIRE_TAB` | 'off', None | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:395 `${{ vars.TFB_SYNC_RETIRE_TAB &#124;&#124; 'off' }}`; daily_sync.yml:1581 `${{ vars.TFB_SYNC_RETIRE_TAB &#124;&#124; 'off' }}` | scripts/run_dashboard_sync.py:10858; scripts/run_dashboard_sync.py:11067 |
| `TFB_SYNC_RETIRE_TAB_NAME` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:396 `${{ vars.TFB_SYNC_RETIRE_TAB_NAME &#124;&#124; '_Retired_Symbols' }}`; daily_sync.yml:1582 `${{ vars.TFB_SYNC_RETIRE_TAB_NAME &#124;&#124; '_Retired_Symbols' }}` | scripts/run_dashboard_sync.py:10852 |
| `TFB_SYNC_ROW_ID_FIREWALL` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:5891 |
| `TFB_SYNC_SAFE_GATEWAYS` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:10702 |
| `TFB_SYNC_SKIP_MAX_STALE_H` | '30' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:12306 |
| `TFB_SYNC_STALE_SKIP_RED` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:12297 |
| `TFB_SYNC_STATUS_FRESH_MIN` | '95' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:8782; scripts/run_dashboard_sync.py:9729 |
| `TFB_SYNC_STATUS_STAMP` | '0' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:407 `${{ vars.TFB_SYNC_STATUS_STAMP &#124;&#124; '0' }}`; daily_sync.yml:1592 `${{ vars.TFB_SYNC_STATUS_STAMP &#124;&#124; '0' }}` | scripts/run_dashboard_sync.py:8706 |
| `TFB_SYNC_STATUS_STAMP_PAGES` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:408 `${{ vars.TFB_SYNC_STATUS_STAMP_PAGES &#124;&#124; '' }}`; daily_sync.yml:1593 `${{ vars.TFB_SYNC_STATUS_STAMP_PAGES &#124;&#124; '' }}` | scripts/run_dashboard_sync.py:8713 |
| `TFB_SYNC_STATUS_TRUTH` | '0' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:428 `${{ vars.TFB_SYNC_STATUS_TRUTH &#124;&#124; '0' }}`; daily_sync.yml:1613 `${{ vars.TFB_SYNC_STATUS_TRUTH &#124;&#124; '0' }}` | scripts/run_dashboard_sync.py:8397 |
| `TFB_SYNC_STRICT_MEMBERSHIP` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:5598 |
| `TFB_SYNC_SYMBOL_BATCH_SIZE` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:574 `"25"`; daily_sync.yml:1632 `"25"`; page_refresh_recovery.yml:184 `'25'` | scripts/run_dashboard_sync.py:11793 |
| `TFB_SYNC_SYMBOL_PERSISTENCE` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:5587 |
| `TFB_SYNC_TIME_BUDGET_SEC` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:600 `"5400"`; daily_sync.yml:1660 `"5400"`; page_refresh_recovery.yml:185 `'3600'` | scripts/run_dashboard_sync.py:11753 |
| `TFB_SYNC_UNIVERSE_CAP_V2` | '1' | Actions: ci.yml, daily_brief.yml; Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml; Actions: digest.yml | absent | none | scripts/run_daily_brief.py:741; scripts/run_dashboard_sync.py:11263; scripts/send_digest.py:187 |
| `TFB_SYNC_UNIVERSE_DENY` | '^TICK\\d+' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:10809 |
| `TFB_SYNC_UPSTREAM_VERDICT` | '0' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:410 `${{ vars.TFB_SYNC_UPSTREAM_VERDICT &#124;&#124; '0' }}`; daily_sync.yml:1595 `${{ vars.TFB_SYNC_UPSTREAM_VERDICT &#124;&#124; '0' }}` | scripts/run_dashboard_sync.py:9696 |
| `TFB_SYNC_VERDICT_MAX_AGE_MIN` | '240' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:412 `${{ vars.TFB_SYNC_VERDICT_MAX_AGE_MIN &#124;&#124; '240' }}`; daily_sync.yml:1597 `${{ vars.TFB_SYNC_VERDICT_MAX_AGE_MIN &#124;&#124; '240' }}` | scripts/run_dashboard_sync.py:9713 |
| `TFB_SYNC_VERDICT_PAGES` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:411 `${{ vars.TFB_SYNC_VERDICT_PAGES &#124;&#124; '' }}`; daily_sync.yml:1596 `${{ vars.TFB_SYNC_VERDICT_PAGES &#124;&#124; '' }}` | scripts/run_dashboard_sync.py:9702 |
| `TFB_SYNC_WRITE_RETRIES` | '3' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:11628 |
| `TFB_SYNC_WRITE_SENTINEL` | '0' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:420 `${{ vars.TFB_SYNC_WRITE_SENTINEL &#124;&#124; '0' }}`; daily_sync.yml:1605 `${{ vars.TFB_SYNC_WRITE_SENTINEL &#124;&#124; '0' }}` | scripts/run_dashboard_sync.py:7864 |
| `TFB_SYNC_WRITE_THEN_TRIM` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:11642 |
| `TFB_T10_ADMIT_LEGACY` | '0' | Render/library | absent | none | core/analysis/top10_selector.py:1284 |
| `TFB_T10_BLOCKED_INVARIANT` | 'TFB_SURFACE_BLOCKED_INVARIANT', computed | Render/library | absent | none | core/data_engine_v2.py:17769; core/data_engine_v2.py:20090; core/data_engine_v2.py:6036; core/surface_action_invariants.py:311 |
| `TFB_T10_CAP_BAND` | '' | Render/library | absent | none | core/analysis/opportunity_builder.py:2446 |
| `TFB_T10_CAP_BAND_RAW_EXEMPT` | '1' | Render/library | absent | none | core/analysis/opportunity_builder.py:2464 |
| `TFB_T10_CONF_DEFAULT_TOKENS` | '' | Render/library | absent | none | core/analysis/opportunity_builder.py:2304 |
| `TFB_T10_DISCLOSE_LEGACY` | '0' | Render/library | absent | none | core/analysis/top10_selector.py:1291 |
| `TFB_T10_EXCLUDE_CAP_BAND` | '0' | Render/library | absent | none | core/analysis/opportunity_builder.py:2438 |
| `TFB_T10_EXCLUDE_DEFAULT_CONF` | '0' | Render/library | absent | none | core/analysis/opportunity_builder.py:2213 |
| `TFB_T10_EXCLUDE_REL_CLUSTER` | '0' | Render/library | absent | none | core/analysis/opportunity_builder.py:2251 |
| `TFB_T10_FASTTRACK_LEGACY` | '0' | Render/library | absent | none | core/analysis/top10_selector.py:1298 |
| `TFB_T10_FETCHFAIL_BLOCKED` | 'TFB_SURFACE_FETCHFAIL_BLOCKED', computed | Render/library | absent | none | core/data_engine_v2.py:17772; core/data_engine_v2.py:20093; core/data_engine_v2.py:6038; core/surface_action_invariants.py:316 |
| `TFB_T10_HARD_EXCLUDE_OVER_GRACE` | '0' | Render/library | absent | none | core/analysis/top10_selector.py:1269 |
| `TFB_T10_PRICE_XCHECK` | 'off' | Render/library | absent | none | core/analysis/opportunity_builder.py:3988 |
| `TFB_T10_PRICE_XCHECK_BUDGET_S` | 20.0 | Render/library | absent | none | core/analysis/opportunity_builder.py:4023 |
| `TFB_T10_PRICE_XCHECK_MAX_FETCH` | 15 | Render/library | absent | none | core/analysis/opportunity_builder.py:4007 |
| `TFB_T10_PRICE_XCHECK_STRICT` | '0' | Render/library | absent | none | core/analysis/opportunity_builder.py:4001 |
| `TFB_T10_PRICE_XCHECK_TIMEOUT_S` | 4.0 | Render/library | absent | none | core/analysis/opportunity_builder.py:4015 |
| `TFB_T10_PRICE_XCHECK_TOL_PCT` | 1.0 | Render/library | absent | none | core/analysis/opportunity_builder.py:3994 |
| `TFB_T10_REL_CLUSTER_BASIS` | 'values' | Render/library | absent | none | core/analysis/opportunity_builder.py:2294 |
| `TFB_T10_REL_CLUSTER_VALUES` | '' | Render/library | absent | none | core/analysis/opportunity_builder.py:2261 |
| `TFB_T10_REL_FLOOR_MODE` | 'gate' | Render/library | declared (default 'gate') | none | core/analysis/opportunity_builder.py:3833 |
| `TFB_T10_REQ_ROI_3M_PCT` | '' | Render/library | absent | none | core/analysis/opportunity_builder.py:2628 |
| `TFB_T10_SHADOW_HARD_LEGACY` | '' | Render/library | absent | none | core/analysis/top10_selector.py:1369 |
| `TFB_T10_SHOCK_PCT` | -5.0 | Render/library | absent | none | core/analysis/opportunity_builder.py:3123 |
| `TFB_T10_SYNTHETIC_SOURCES` | '' | Render/library | absent | none | core/analysis/opportunity_builder.py:2222 |
| `TFB_T10_VENUE_ALLOWLIST` | '' | Render/library | absent | none | core/analysis/opportunity_builder.py:4317 |
| `TFB_T10_VENUE_LOTS` | '' | Render/library | absent | none | core/analysis/opportunity_builder.py:2385 |
| `TFB_T10_W52_HIGH_PCT` | 85.0 | Render/library | absent | none | core/analysis/opportunity_builder.py:3113 |
| `TFB_T10_W52_LOW_PCT` | 15.0 | Render/library | absent | none | core/analysis/opportunity_builder.py:3104 |
| `TFB_T10_W52_TIMING` | 'off' | Render/library | absent | none | core/analysis/opportunity_builder.py:3095 |
| `TFB_TARGET_MEAN_PUBLISH` | '1' | Render/library | absent | none | core/data_engine_v2.py:16251 |
| `TFB_TARGET_SANITY_GUARD` | '1' | Render/library | absent | none | core/data_engine_v2.py:16284 |
| `TFB_TARGET_SANITY_MAX_RATIO` | None | Render/library | absent | none | core/data_engine_v2.py:16293 |
| `TFB_TARGET_SANITY_MIN_RATIO` | None | Render/library | absent | none | core/data_engine_v2.py:16292 |
| `TFB_TEST_EXPORT_XLSX` | None | Actions: workbook_backup.yml | absent | none | scripts/tfb_workbook_dq_audit.py:3958 |
| `TFB_THIN_HISTORY_GUARD` | '' | Render/library | absent | none | core/data_engine_v2.py:7132 |
| `TFB_TICKET_FALLBACK_MAX_AGE_H` | 78.0 | Render/library | declared (default '78') | none | core/analysis/opportunity_builder.py:3964 |
| `TFB_TICKET_FRESHNESS_GATE` | '1' | Render/library | declared (default '1') | none | core/analysis/opportunity_builder.py:3950 |
| `TFB_TICKET_MAX_QUOTE_AGE_MIN` | 15.0 | Render/library | declared (default '15') | none | core/analysis/opportunity_builder.py:3956 |
| `TFB_TIMEOUT_LONG` | 90.0 | Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:223 |
| `TFB_TIMEOUT_MED` | 30.0 | Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:222 |
| `TFB_TIMEOUT_SHORT` | 15.0 | Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:221 |
| `TFB_TIMEZONE` | computed | Render/library | absent | none | config.py:487 |
| `TFB_TOKEN` | '' | Actions: ci.yml; Render/library | absent | none | main.py:1185; scripts/smoke_endpoints.py:209 |
| `TFB_TOP10_ADMISSION_FILTER` | '1' | Render/library | absent | none | core/analysis/top10_selector.py:1517 |
| `TFB_TOP10_ENGINE_CALL_TIMEOUT_SEC` | 20.0 | Render/library | absent | none | core/analysis/top10_selector.py:1060 |
| `TFB_TOP10_EXCLUDE_SELL_CLASS` | '1' | Render/library | absent | none | core/analysis/top10_selector.py:1558 |
| `TFB_TOP10_FINAL_GATE` | '1' | Render/library | absent | none | core/analysis/top10_selector.py:1211 |
| `TFB_TOP10_FULL_UNIVERSE` | False | Render/library | absent | none | core/analysis/top10_selector.py:1105; routes/advanced_analysis.py:3966 |
| `TFB_TOP10_GOLDEN_COMPOSITE` | '1' | Render/library | absent | none | core/analysis/top10_selector.py:1532 |
| `TFB_TOP10_HEADER_SELFHEAL` | '1' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:4657 |
| `TFB_TOP10_OUTER_GRACE_SEC` | 8.0 | Render/library | absent | none | core/analysis/top10_selector.py:1068 |
| `TFB_TOP10_PAGE_TIMEOUT_SEC` | 30.0 | Render/library | absent | none | core/analysis/top10_selector.py:1061 |
| `TFB_TOP10_POOL_CEILING` | None | Render/library | absent | none | routes/advanced_analysis.py:3969 |
| `TFB_TOP10_QUALITY_FILTER` | '' | Render/library | absent | none | core/data_engine_v2.py:5936 |
| `TFB_TOP10_RESILIENT_FETCH` | False | Render/library | absent | none | core/analysis/top10_selector.py:1116 |
| `TFB_TOP10_RESILIENT_PAGE_LIMIT` | 30 | Render/library | absent | none | core/analysis/top10_selector.py:1117 |
| `TFB_TOP10_SCAN_LIMIT` | 3000 | Render/library | absent | none | core/data_engine_v2.py:7166 |
| `TFB_TOP10_SECTOR_CAP` | 4 | Render/library | absent | none | core/analysis/top10_selector.py:1562 |
| `TFB_TOP10_SECTOR_NORMALIZE` | False | Render/library | absent | none | core/analysis/top10_selector.py:1571 |
| `TFB_TOP10_STABILITY_SCORE_BASIS` | '' | Render/library | absent | none | core/analysis/top10_selector.py:1452 |
| `TFB_TOP10_SYMBOL_VARIANT_DEDUP` | '1' | Render/library | absent | none | core/analysis/top10_selector.py:4181 |
| `TFB_TOP10_TEST_SYMBOL` | 'NVDA' | Actions: ci.yml | absent | none | scripts/smoke_endpoints.py:218 |
| `TFB_TOP10_TIER1_RELIABILITY_FLOOR` | 65.0 | Render/library | absent | none | core/analysis/top10_selector.py:1552 |
| `TFB_TOP10_TOTAL_RESERVE_SEC` | 6.0 | Render/library | absent | none | core/analysis/top10_selector.py:1067 |
| `TFB_TOP10_TOTAL_TIMEOUT_SEC` | 70.0 | Render/library | absent | none | core/analysis/top10_selector.py:1062 |
| `TFB_TOP10_TRADABILITY_GATE` | False | Render/library | declared (default '0') | none | core/analysis/top10_selector.py:4884 |
| `TFB_TP_APPEND_FATAL` | '0' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:5740 |
| `TFB_TP_CAPACITY_GUARD` | '1' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:5624 |
| `TFB_TP_CAPACITY_GUARD_MODE` | 'observe' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:5632 |
| `TFB_TP_CAPACITY_LIMIT` | '10000000' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:5641 |
| `TFB_TP_CAPACITY_PCT` | '85' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:5651 |
| `TFB_TP_EVIDENCE_SPREADSHEET_ID` | '' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:5750 |
| `TFB_TRACK_CA_GUARD` | '1' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:3397 |
| `TFB_TRACK_CA_LEDGER` | '1' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | declared (default '1') | none | scripts/track_performance.py:3548 |
| `TFB_TRACK_CA_VERIFY_PCT` | '' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:3404 |
| `TFB_TRACK_COSTBASIS_TAB` | '_Portfolio_CostBasis' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2133 |
| `TFB_TRACK_ENTRY_SANITY` | True | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:1903 |
| `TFB_TRACK_ENTRY_SANITY_MIN_N` | 3 | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:1921 |
| `TFB_TRACK_ENTRY_SANITY_PCT` | 0.35 | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:1933 |
| `TFB_TRACK_FALLBACK_BREAK_429` | '15' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2847 |
| `TFB_TRACK_FALLBACK_BUDGET_SEC` | '420' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2883 |
| `TFB_TRACK_FALLBACK_CONC` | '2' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2735 |
| `TFB_TRACK_FALLBACK_RETRY429` | '1' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2752 |
| `TFB_TRACK_FALLBACK_SHUFFLE` | '1' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2856 |
| `TFB_TRACK_FALLBACK_SPACING_SEC` | '0.45' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2744 |
| `TFB_TRACK_FALLBACK_TIMEOUT_SEC` | '8' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2872 |
| `TFB_TRACK_FEED_BACKOFF_SEC` | '2' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2799 |
| `TFB_TRACK_FEED_BISECT` | '1' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2818 |
| `TFB_TRACK_FEED_BUDGET_SEC` | '240' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2809 |
| `TFB_TRACK_FEED_RETRIES` | '3' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2788 |
| `TFB_TRACK_FORCE_DECISION_SYMBOLS` | True | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2071 |
| `TFB_TRACK_FORCE_MAX` | 40 | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2082 |
| `TFB_TRACK_FORCE_ROWS_ENDPOINT` | '/v1/analysis/sheet-rows' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2086 |
| `TFB_TRACK_FORCE_ROWS_PAGE` | 'Market_Leaders' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2091 |
| `TFB_TRACK_FORCE_SCOPE` | 'all', None | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2099; scripts/track_performance.py:9931 |
| `TFB_TRACK_MATURE_FRESH_ONLY` | '1' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:3910 |
| `TFB_TRACK_MATURE_GRACE_DAYS` | '5' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:3976 |
| `TFB_TRACK_PLACEHOLDER_GATE` | True | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:1852 |
| `TFB_TRACK_PRICE_CHUNK` | '40' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2758 |
| `TFB_TRACK_PRICE_CHUNK_TIMEOUT_SEC` | '45' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2767 |
| `TFB_TRACK_PRICE_FALLBACK` | '1' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2727 |
| `TFB_TRACK_PRIORITY_SYMBOLS` | computed | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2076 |
| `TFB_TRACK_REGRET_TOPK` | '10' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:3926 |
| `TFB_TRACK_RUNLOG_VERDICT` | '1' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:3376 |
| `TFB_TRACK_SHADOW_COHORTS` | '0' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | daily_sync.yml:335 `"1"`; track_performance.yml:335 `"1"` | scripts/track_performance.py:3917 |
| `TFB_TRACK_SUMMARY_SIDE` | '1' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2698 |
| `TFB_TRACK_SYMBOL_SHAPE_GUARD` | '1' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:2156 |
| `TFB_TREND_DAY_KEYED` | '1' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:6541 |
| `TFB_TREND_MIN_COHORT` | 3 | Render/library | absent | none | core/analysis/trend_signals.py:181 |
| `TFB_TREND_NEG_BREADTH` | 0.5 | Render/library | absent | none | core/analysis/trend_signals.py:184 |
| `TFB_TREND_NEG_ROI` | 0.0 | Render/library | absent | none | core/analysis/trend_signals.py:183 |
| `TFB_TREND_POS_BREADTH` | 0.2 | Render/library | absent | none | core/analysis/trend_signals.py:185 |
| `TFB_TREND_POS_ROI` | 10.0 | Render/library | absent | none | core/analysis/trend_signals.py:182 |
| `TFB_TREND_SIGNALS` | '' | Render/library | absent | none | core/analysis/trend_signals.py:152 |
| `TFB_TRIM_BY_RULE_GATE` | '1' | Render/library | declared (default '1') | none | core/analysis/portfolio_actions.py:1353 |
| `TFB_TRUST_DEGENERATE_GUARD` | '' | Render/library | absent | none | core/data_engine_v2.py:6813 |
| `TFB_TRUST_DEGENERATE_RANGE_PCT` | computed | Render/library | absent | none | core/data_engine_v2.py:6824 |
| `TFB_TRUST_DQ_HIGH` | computed | Render/library | absent | none | core/data_engine_v2.py:6768 |
| `TFB_TRUST_DQ_LOW` | computed | Render/library | absent | none | core/data_engine_v2.py:6776 |
| `TFB_TRUST_LEVEL` | '' | Render/library | absent | none | core/data_engine_v2.py:6759 |
| `TFB_TRUST_PROVIDER_RECO` | '' | Render/library | absent | none | core/data_engine_v2.py:4423 |
| `TFB_TUPLE_NOISE_RELEASE` | '' | Render/library | declared (default 'off') | none | core/data_engine_v2.py:7690 |
| `TFB_WARNINGS_COMPACT` | 'off' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | declared (default 'off') | daily_sync.yml:327 `${{ vars.TFB_WARNINGS_COMPACT &#124;&#124; 'off' }}`; daily_sync.yml:1689 `${{ vars.TFB_WARNINGS_COMPACT &#124;&#124; 'off' }}`; page_refresh_recovery.yml:191 `${{ vars.TFB_WARNINGS_COMPACT &#124;&#124; 'off' }}` | scripts/run_dashboard_sync.py:3354 |
| `TFB_WARNINGS_COMPACT_KEEP` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:328 `${{ vars.TFB_WARNINGS_COMPACT_KEEP &#124;&#124; '' }}`; daily_sync.yml:1690 `${{ vars.TFB_WARNINGS_COMPACT_KEEP &#124;&#124; '' }}`; page_refresh_recovery.yml:192 `${{ vars.TFB_WARNINGS_COMPACT_KEEP &#124;&#124; '' }}` | scripts/run_dashboard_sync.py:3363 |
| `TFB_WARNINGS_SINK_DIR` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:3372 |
| `TFB_WARN_INVEST_INVARIANT` | 'TFB_SURFACE_WARN_INVEST', computed | Render/library | absent | none | core/data_engine_v2.py:17775; core/data_engine_v2.py:20096; core/data_engine_v2.py:6040; core/surface_action_invariants.py:322 |
| `TFB_WORKER_BACKOFF_BASE_SEC` | 1.0 | script/operator (no direct workflow launch found) | absent | none | scripts/worker.py:352 |
| `TFB_WORKER_BACKOFF_MAX_SEC` | 30.0 | script/operator (no direct workflow launch found) | absent | none | scripts/worker.py:357 |
| `TFB_WORKER_CPU_WORKERS` | 4 | script/operator (no direct workflow launch found) | absent | none | scripts/worker.py:362 |
| `TFB_WORKER_DLQ_NAME` | 'tfb_background_jobs_dead' | script/operator (no direct workflow launch found) | absent | none | scripts/worker.py:335 |
| `TFB_WORKER_FAIL_FAST_NO_REDIS` | False | script/operator (no direct workflow launch found) | absent | none | scripts/worker.py:365 |
| `TFB_WORKER_IDLE_SLEEP_SEC` | 5.0 | script/operator (no direct workflow launch found) | absent | none | scripts/worker.py:344 |
| `TFB_WORKER_MAX_RETRIES` | 5 | script/operator (no direct workflow launch found) | absent | none | scripts/worker.py:349 |
| `TFB_WORKER_QUEUE_NAME` | 'tfb_background_jobs' | script/operator (no direct workflow launch found) | absent | none | scripts/worker.py:332 |
| `TFB_WORKER_REDIS_BLOCK_TIMEOUT_SEC` | 5 | script/operator (no direct workflow launch found) | absent | none | scripts/worker.py:339 |
| `TFB_XPAGE_MAX_REPORT` | '50' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:4343 |
| `TFB_XPAGE_PRICE_CHECK` | '' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | daily_sync.yml:474 `"1"`; daily_sync.yml:1525 `"1"`; page_refresh_recovery.yml:168 `'1'` | scripts/run_dashboard_sync.py:4325 |
| `TFB_XPAGE_PRICE_DELTA_PCT` | '2.0' | Actions: ci.yml, daily_sync.yml, page_refresh_recovery.yml, track_performance.yml | absent | none | scripts/run_dashboard_sync.py:4334 |
| `TFB_XPROVIDER_DELTA_PCT` | '3.0' | Render/library | absent | none | core/data_engine_v2.py:14060 |
| `TFB_XPROVIDER_VERIFY` | '' | Render/library | absent | none | core/data_engine_v2.py:14052 |
| `TFB_YAHOO_RAW_CHART` | '1' | Render/library | absent | none | core/providers/yahoo_chart_provider.py:1109 |
| `TFB_YAHOO_SHARE_CLASS_DASH` | False | Render/library | absent | none | core/symbols/normalize.py:1307 |
| `TFB_YC_ADJUSTED_HISTORY` | '1' | Render/library | absent | none | core/providers/yahoo_chart_provider.py:1130 |
| `TFB_YC_OPEN_FALLBACK` | '1' | Render/library | absent | none | core/providers/yahoo_chart_provider.py:1642 |
| `TFB_YC_RANGE_FALLBACKS` | '1' | Render/library | absent | none | core/providers/yahoo_chart_provider.py:1678 |
| `TFB_YC_SYMBOL_MAP` | '1' | Render/library | absent | none | core/providers/yahoo_chart_provider.py:711 |
| `TFB_YC_SYMBOL_SKIP` | '' | Render/library | declared (default '') | none | core/providers/yahoo_chart_provider.py:888 |
| `TFB_YF_CB_AUTH_GUARD` | True | Render/library | absent | none | core/providers/yahoo_fundamentals_provider.py:787 |
| `TFB_YF_HISTORY_RETRY` | '' | Render/library | absent | none | core/providers/yahoo_chart_provider.py:1013 |
| `TFB_YF_HISTORY_RETRY_ATTEMPTS` | computed | Render/library | absent | none | core/providers/yahoo_chart_provider.py:1027 |
| `TFB_YF_HISTORY_RETRY_BASE_SEC` | computed | Render/library | absent | none | core/providers/yahoo_chart_provider.py:1039 |
| `TFB_YF_HISTORY_RETRY_CAP_SEC` | computed | Render/library | absent | none | core/providers/yahoo_chart_provider.py:1051 |
| `TFB_YF_TARGET_FALLBACK` | '0' | Render/library | absent | none | core/data_engine_v2.py:4827 |
| `TFB_YF_TARGET_FALLBACK_MAX` | '200' | Render/library | absent | none | core/data_engine_v2.py:4836 |
| `TRACK_ANALYZE` | False | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10699 |
| `TRACK_AUDIT` | False | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10693 |
| `TRACK_BACKTEST` | False | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10793 |
| `TRACK_BACKTEST_UNIVERSE` | None | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10517 |
| `TRACK_CALIBRATE` | False | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10717 |
| `TRACK_CALIBRATION` | True | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:8468 |
| `TRACK_CANONICAL_TARGETS` | '0' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | declared (default '0') | daily_sync.yml:1244 `${{ vars.TRACK_CANONICAL_TARGETS &#124;&#124; '0' }}`; track_performance.yml:300 `${{ vars.TRACK_CANONICAL_TARGETS &#124;&#124; '0' }}` | scripts/track_performance.py:3786 |
| `TRACK_CA_VERIFY_BUDGET` | '60' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:3434 |
| `TRACK_CA_VERIFY_TIME_SEC` | '240' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:3442 |
| `TRACK_CONFIDENCE` | 0.95 | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10757 |
| `TRACK_DAEMON` | False | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10736 |
| `TRACK_DEDUP_KEYS` | '1' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:3698 |
| `TRACK_DEDUP_MATURED` | 'off', None | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | daily_sync.yml:1235 `${{ vars.TRACK_DEDUP_MATURED &#124;&#124; 'off' }}`; track_performance.yml:291 `${{ vars.TRACK_DEDUP_MATURED &#124;&#124; 'off' }}` | scripts/track_performance.py:3710; scripts/track_performance.py:9954 |
| `TRACK_DEDUP_SIGNAL_HISTORY` | 'off', None | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | declared (default 'off') | daily_sync.yml:1237 `${{ vars.TRACK_DEDUP_SIGNAL_HISTORY &#124;&#124; 'off' }}`; track_performance.yml:293 `${{ vars.TRACK_DEDUP_SIGNAL_HISTORY &#124;&#124; 'off' }}` | scripts/track_performance.py:10058; scripts/track_performance.py:3773 |
| `TRACK_DROP_DUPLICATE_ROWS` | '0' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | track_performance.yml:272 `${{ vars.TRACK_DROP_DUPLICATE_ROWS &#124;&#124; '0' }}` | scripts/track_performance.py:3675 |
| `TRACK_EVENT_CONTEXT` | True | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:8907; scripts/track_performance.py:8960 |
| `TRACK_EXPORT` | False | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10711 |
| `TRACK_FORMAT` | 'html' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10769 |
| `TRACK_HORIZONS` | computed | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | declared (default '1M,3M') | daily_sync.yml:1212 `"1W,2W,1M,3M"`; track_performance.yml:327 `"1W,2W,1M,3M"` | scripts/track_performance.py:10742 |
| `TRACK_HYPOTHESIS` | '' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | backtest_harness.yml:110 `${{ github.event.inputs.hypothesis }}` | scripts/track_performance.py:10814 |
| `TRACK_HYPOTHESIS_SHEET` | 'Hypothesis_Registry' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10444 |
| `TRACK_INTERVAL` | 3600 | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10780 |
| `TRACK_ITERATIONS` | 10000 | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10763 |
| `TRACK_LIST_HYPOTHESES` | False | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10809 |
| `TRACK_MAX_RECORDS` | '200000', 200000 | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10751; scripts/track_performance.py:3667 |
| `TRACK_OUTPUT` | None | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10774 |
| `TRACK_PUBLICATION_ID` | None | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | declared (default '') | daily_sync.yml:1246 `${{ github.run_id }}`; track_performance.yml:302 `${{ github.run_id }}` | scripts/track_performance.py:8944 |
| `TRACK_PUBLISH_CALIBRATION` | '1' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:3884 |
| `TRACK_RECORD` | False | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10684 |
| `TRACK_REGISTER_HYPOTHESES` | False | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10803 |
| `TRACK_RISK_BLANK` | '0', None | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | daily_sync.yml:1236 `${{ vars.TRACK_RISK_BLANK &#124;&#124; '0' }}`; track_performance.yml:292 `${{ vars.TRACK_RISK_BLANK &#124;&#124; '0' }}` | scripts/track_performance.py:3719; scripts/track_performance.py:9975 |
| `TRACK_SHEET_ID` | '', None | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml; Actions: news_archive.yml; Actions: shadow_board.yml; script/operator (no direct workflow launch found) | absent | daily_sync.yml:1202 `${{ env.TARGET_SHEET_ID }}`; track_performance.yml:270 `${{ env.TARGET_SHEET_ID }}` | scripts/news_archive_collector.py:471; scripts/news_archive_collector.py:518; scripts/quarantine_placeholder_records.py:216; scripts/refresh_shariah_authority.py:236; scripts/repair_corporate_actions.py:156; scripts/run_shadow_board.py:702; scripts/track_performance.py:10673; scripts/track_performance.py:3573; scripts/track_performance.py:8482 |
| `TRACK_SHEET_NAME` | 'Performance_Log' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10678 |
| `TRACK_SIGNAL_HISTORY` | True | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:8430 |
| `TRACK_SIGNAL_PUBLICATION_MODE` | 'off' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | declared (default 'off') | daily_sync.yml:1245 `${{ vars.TRACK_SIGNAL_PUBLICATION_MODE &#124;&#124; 'off' }}`; track_performance.yml:301 `${{ vars.TRACK_SIGNAL_PUBLICATION_MODE &#124;&#124; 'off' }}` | scripts/track_performance.py:3781 |
| `TRACK_SIGNAL_SHEET` | computed | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:8437 |
| `TRACK_SIMULATE` | False | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10705 |
| `TRACK_STABILITY_HYPOTHESES` | True | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:7855 |
| `TRACK_TOP10_ENDPOINTS` | '' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:5222 |
| `TRACK_TOP10_PAGE` | computed | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:5233 |
| `TRACK_TOP10_TIMEOUT_SEC` | '100' | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:5214 |
| `TRACK_TREND_WINDOW` | 5 | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:8431 |
| `TRACK_VERBOSE` | False | Actions: backtest_harness.yml, ci.yml, daily_sync.yml, tests.yml, track_performance.yml | absent | none | scripts/track_performance.py:10787 |

Dynamic env reads (helper definitions and constructed names require manual review):

- `config.py:153`
- `config.py:241`
- `core/__init__.py:157`
- `core/__init__.py:151`
- `core/analysis/criteria_model.py:627`
- `core/analysis/identity_guard.py:543`
- `core/analysis/insights_builder.py:576`
- `core/analysis/insights_builder.py:581`
- `core/analysis/insights_builder.py:594`
- `core/analysis/opportunity_builder.py:2182`
- `core/analysis/opportunity_builder.py:2988`
- `core/analysis/opportunity_builder.py:4305`
- `core/analysis/portfolio_actions.py:1132`
- `core/analysis/top10_selector.py:1027`
- `core/analysis/top10_selector.py:1037`
- `core/analysis/top10_selector.py:1047`
- `core/analysis/trend_signals.py:136`
- `core/analysis/trend_signals.py:144`
- `core/compliance_gate.py:108`
- `core/compliance_gate.py:104`
- `core/data_engine.py:3965`
- `core/data_engine_v2.py:14420`
- `core/data_engine_v2.py:14424`
- `core/data_engine_v2.py:14428`
- `core/data_engine_v2.py:14433`
- `core/data_engine_v2.py:5956`
- `core/enriched_quote.py:827`
- `core/enriched_quote.py:833`
- `core/investment_advisor_engine.py:1591`
- `core/investment_advisor_engine.py:1278`
- `core/investment_advisor_engine.py:1283`
- `core/news_intelligence.py:238`
- `core/news_intelligence.py:243`
- `core/news_intelligence.py:249`
- `core/news_intelligence.py:260`
- `core/news_intelligence.py:254`
- `core/providers/argaam_provider.py:417`
- `core/providers/argaam_provider.py:423`
- `core/providers/argaam_provider.py:437`
- `core/providers/calendar_provider.py:191`
- `core/providers/eodhd_provider.py:1198`
- `core/providers/eodhd_provider.py:1202`
- `core/providers/eodhd_provider.py:1226`
- `core/providers/eodhd_provider.py:1214`
- `core/providers/finnhub_provider.py:186`
- `core/providers/finnhub_provider.py:189`
- `core/providers/finnhub_provider.py:211`
- `core/providers/finnhub_provider.py:200`
- `core/providers/tadawul_provider.py:346`
- `core/providers/tadawul_provider.py:350`
- `core/providers/tadawul_provider.py:357`
- `core/providers/tadawul_provider.py:364`
- `core/providers/yahoo_chart_provider.py:1362`
- `core/providers/yahoo_chart_provider.py:1366`
- `core/providers/yahoo_chart_provider.py:1383`
- `core/providers/yahoo_chart_provider.py:1377`
- `core/providers/yahoo_fundamentals_provider.py:717`
- `core/providers/yahoo_fundamentals_provider.py:724`
- `core/providers/yahoo_fundamentals_provider.py:732`
- `core/providers/yahoo_fundamentals_provider.py:739`
- `core/reco_normalize.py:467`
- `core/scoring.py:347`
- `core/scoring.py:373`
- `core/scoring.py:360`
- `core/scoring.py:367`
- `core/secret_redaction.py:123`
- `core/secret_redaction.py:125`
- `core/secret_redaction.py:129`
- `core/secret_redaction.py:144`
- `core/secret_redaction.py:153`
- `core/shariah_authority.py:112`
- `core/sheets/rows_reader.py:112`
- `core/surface_action_invariants.py:638`
- `core/surface_action_invariants.py:306`
- `core/symbols/normalize.py:261`
- `core/symbols_reader.py:552`
- `core/symbols_reader.py:569`
- `core/symbols_reader.py:557`
- `core/symbols_reader.py:250`
- `env.py:523`
- `integrations/google_sheets_service.py:2375`
- `integrations/google_sheets_service.py:2380`
- `integrations/google_sheets_service.py:511`
- `integrations/setup_credentials.py:290`
- `integrations/setup_credentials.py:367`
- `integrations/symbols_reader.py:127`
- `integrations/symbols_reader.py:143`
- `integrations/symbols_reader.py:554`
- `main.py:535`
- `main.py:2049`
- `routes/__init__.py:74`
- `routes/advanced_analysis.py:871`
- `routes/advanced_analysis.py:1926`
- `routes/advanced_analysis.py:903`
- `routes/advanced_analysis.py:948`
- `routes/advanced_analysis.py:890`
- `routes/advisor.py:525`
- `routes/advisor.py:615`
- `routes/advisor.py:653`
- `routes/config.py:116`
- `routes/enriched_quote.py:971`
- `routes/investment_advisor.py:1163`
- `routes/routes_argaam.py:161`
- `routes/routes_argaam.py:147`
- `scripts/audit_data_quality.py:1782`
- `scripts/audit_decision_surface_freshness.py:162`
- `scripts/audit_decision_surface_freshness.py:155`
- `scripts/audit_full_refresh_coverage.py:113`
- `scripts/audit_full_refresh_coverage.py:109`
- `scripts/audit_provider_target_coverage.py:125`
- `scripts/audit_provider_target_coverage.py:152`
- `scripts/backup_workbook.py:250`
- `scripts/build_universes.py:107`
- `scripts/cleanup_cache.py:152`
- `scripts/diag_eodhd_fundamentals.py:29`
- `scripts/drift_detection.py:145`
- `scripts/finalize_decision_publication.py:44`
- `scripts/intraday_quote_refresh.py:118`
- `scripts/intraday_quote_refresh.py:123`
- `scripts/migrate_schema_v2.py:173`
- `scripts/news_archive_collector.py:172`
- `scripts/news_archive_collector.py:160`
- `scripts/news_archive_collector.py:152`
- `scripts/pit_snapshot.py:180`
- `scripts/refresh_shariah_authority.py:304`
- `scripts/repo_hygiene_check.py:244`
- `scripts/repo_hygiene_check.py:210`
- `scripts/repo_hygiene_check.py:229`
- `scripts/repo_hygiene_check.py:194`
- `scripts/run_calendar_sync.py:126`
- `scripts/run_dashboard_sync.py:8406`
- `scripts/run_dashboard_sync.py:2955`
- `scripts/run_dashboard_sync.py:7034`
- `scripts/run_dashboard_sync.py:12718`
- `scripts/run_dashboard_sync.py:7018`
- `scripts/run_market_scan.py:677`
- `scripts/run_market_scan.py:209`
- `scripts/run_market_scan.py:174`
- `scripts/run_market_scan.py:195`
- `scripts/run_market_scan.py:656`
- `scripts/run_market_scan.py:158`
- `scripts/send_digest.py:206`
- `scripts/setup_sheet_headers.py:196`
- `scripts/setup_sheet_headers.py:182`
- `scripts/setup_sheet_headers.py:166`
- `scripts/smoke_endpoints.py:147`
- `scripts/smoke_endpoints.py:168`
- `scripts/smoke_endpoints.py:131`
- `scripts/tfb_reco_coherence.py:96`
- `scripts/tfb_reco_coherence.py:97`
- `scripts/track_performance.py:1950`
- `scripts/track_performance.py:9840`
- `scripts/track_performance.py:1808`
- `scripts/track_performance.py:1941`
- `scripts/track_performance.py:8515`
- `scripts/track_performance.py:1792`
- `scripts/track_performance.py:2779`
- `scripts/validate_dashboard.py:310`
- `scripts/validate_dashboard.py:300`
- `scripts/validate_dashboard.py:286`
- `scripts/verify_deployment.py:591`
- `scripts/worker.py:122`
- `scripts/worker.py:143`
- `scripts/worker.py:106`
