"""A repair's actual main/phase code must distinguish completion from failed I/O."""
from __future__ import annotations

import contextlib
import io
import json
import os
import unittest
from unittest.mock import Mock, patch

from scripts import repair_stores as repair


class RepairStoresFailureTests(unittest.TestCase):
    def workbook(self):
        tabs = {name: Mock(name=name) for name in (
            "_Portfolio_CostBasis", "Signal_History", "Signal_Trends", "Performance_Log",
            "Market_Leaders", "Global_Markets", "_Run_Log",
        )}
        tabs["_Portfolio_CostBasis"].get_values.return_value = [["Portfolio ledger title"]]
        for name in ("Signal_History", "Signal_Trends", "Performance_Log"):
            tabs[name].get_all_values.return_value = [["Symbol"], ["ALPHA.SR"]]
        tabs["Performance_Log"].get_values.return_value = []
        for name in ("Market_Leaders", "Global_Markets"):
            tabs[name].get_all_values.return_value = [
                ["Symbol", "Name", "Price", "EPS TTM", "P/E TTM"],
                ["ALPHA.SR", "Alpha", "100", "10", "10"],
            ]
        book = Mock(name="workbook")
        book.worksheet.side_effect = tabs.__getitem__
        return book, tabs

    def run_repair(self, book, *, apply=True, max_deletes="2000"):
        output = io.StringIO()
        with patch.dict(os.environ, {
            "REPAIR_APPLY": "1" if apply else "0", "REPAIR_MAX_DELETES": max_deletes,
            "REPAIR_STORES": "Signal_History,Signal_Trends,Performance_Log",
            "REPAIR_PAGES": "Market_Leaders,Global_Markets",
        }), patch.object(repair, "_open_sheet", return_value=book), contextlib.redirect_stdout(output):
            code = repair.main()
        return code, output.getvalue()

    def report(self, tabs):
        return json.loads(tabs["_Run_Log"].append_row.call_args.args[0][-1])

    def test_successful_dry_run_keeps_repair_boundaries_inert(self):
        book, tabs = self.workbook()
        tabs["Signal_History"].get_all_values.return_value.append(["not a ticker"])
        tabs["Market_Leaders"].get_all_values.return_value[1][1] = ""
        code, _ = self.run_repair(book, apply=False)
        self.assertEqual(code, 0)
        book.batch_update.assert_not_called()
        tabs["Market_Leaders"].batch_update.assert_not_called()
        tabs["Performance_Log"].batch_clear.assert_not_called()
        report = self.report(tabs)
        self.assertEqual(report["mode"], "DRY")
        self.assertEqual(report["phase_a"]["stores"]["Signal_History"]["would_delete"], 1)
        self.assertEqual(report["phase_b"]["pages"]["Market_Leaders"]["corrupt"], 1)

    def test_every_missing_required_tab_fails_the_final_status(self):
        _, complete = self.workbook()
        for name in complete:
            with self.subTest(tab=name):
                book, tabs = self.workbook()
                del tabs[name]
                code, output = self.run_repair(book)
                self.assertEqual(code, 1, output)
                if "_Run_Log" in tabs:
                    self.assertEqual(tabs["_Run_Log"].append_row.call_args.args[0][4], "PARTIAL")

    def test_failed_ledger_read_cannot_run_a_partial_junk_purge(self):
        book, tabs = self.workbook()
        tabs["_Portfolio_CostBasis"].get_values.side_effect = TimeoutError("ledger read unavailable")
        tabs["Signal_History"].get_all_values.return_value.append(["not a ticker"])
        code, output = self.run_repair(book)
        self.assertEqual(code, 1, output)
        book.batch_update.assert_not_called()
        tabs["Signal_History"].get_all_values.assert_not_called()
        self.assertIn("ledger_read_error", self.report(tabs)["phase_a"])

    def test_api_lookup_failure_is_reported_without_hiding_other_phases(self):
        book, tabs = self.workbook()

        def lookup(name):
            if name == "Signal_History":
                raise TimeoutError("worksheet lookup unavailable")
            return tabs[name]

        book.worksheet.side_effect = lookup
        code, output = self.run_repair(book)
        self.assertEqual(code, 1, output)
        report = self.report(tabs)
        self.assertIn("lookup unavailable", report["phase_a"]["stores"]["Signal_History"]["error"])
        self.assertEqual(report["phase_b"]["pages"]["Global_Markets"]["corrupt"], 0)

    def test_failed_delete_is_not_reported_as_completed(self):
        book, tabs = self.workbook()
        tabs["Signal_History"].get_all_values.return_value.append(["not a ticker"])
        book.batch_update.side_effect = OSError("delete unavailable")
        code, output = self.run_repair(book)
        self.assertEqual(code, 1, output)
        self.assertIn("junk_rows repaired/deleted: 0", output)
        self.assertIn("error", self.report(tabs)["phase_a"]["stores"]["Signal_History"])

    def test_safety_ceiling_refusal_fails_without_counting_proposed_deletions(self):
        book, tabs = self.workbook()
        tabs["Signal_History"].get_all_values.return_value.extend([["bad symbol"], ["another bad symbol"]])
        code, output = self.run_repair(book, max_deletes="1")
        self.assertEqual(code, 1, output)
        book.batch_update.assert_not_called()
        self.assertIn("junk_rows repaired/deleted: 0", output)
        self.assertEqual(self.report(tabs)["phase_a"]["stores"]["Signal_History"]["would_delete"], 2)

    def test_partial_identity_write_retains_the_completed_batch_count(self):
        book, tabs = self.workbook()
        tabs["Market_Leaders"].get_all_values.return_value = [
            ["Symbol", "Name", "Price"],
            *[[f"S{i}.SR", "", "100"] for i in range(201)],
        ]
        tabs["Market_Leaders"].batch_update.side_effect = [None, OSError("second batch unavailable")]
        code, output = self.run_repair(book)
        self.assertEqual(code, 1, output)
        page = self.report(tabs)["phase_b"]["pages"]["Market_Leaders"]
        self.assertEqual(page["corrupt"], 201)
        self.assertEqual(page["repaired"], 200)
        self.assertIn("second batch unavailable", page["error"])

    def test_failed_ghost_clear_fails_the_final_status(self):
        book, tabs = self.workbook()
        tabs["Performance_Log"].get_values.return_value = [["stale KPI"]]
        tabs["Performance_Log"].batch_clear.side_effect = OSError("clear unavailable")
        code, output = self.run_repair(book)
        self.assertEqual(code, 1, output)
        self.assertIn("clear unavailable", self.report(tabs)["phase_c"]["error"])

    def test_failed_verdict_append_does_not_report_success(self):
        book, tabs = self.workbook()
        tabs["_Run_Log"].append_row.side_effect = TimeoutError("verdict append unavailable")
        code, output = self.run_repair(book)
        self.assertEqual(code, 1, output)
        self.assertIn("run-log verdict failed", output)

    def test_invalid_delete_ceiling_is_rejected_before_any_workbook_access(self):
        for value in ("not an integer", "0", "-1"):
            with self.subTest(value=value), patch.dict(os.environ, {"REPAIR_MAX_DELETES": value}), \
                    patch.object(repair, "_open_sheet") as open_sheet, contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(repair.main(), 2)
                open_sheet.assert_not_called()

    def test_workbook_open_error_is_an_explicit_setup_failure(self):
        with patch.dict(os.environ, {"REPAIR_MAX_DELETES": "2000"}), \
                patch.object(repair, "_open_sheet", side_effect=OSError("workbook unavailable")), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(repair.main(), 2)


if __name__ == "__main__":
    unittest.main()
