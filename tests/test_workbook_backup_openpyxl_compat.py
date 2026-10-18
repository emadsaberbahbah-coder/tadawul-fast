"""Cross-check structural validation with the already pinned restore reader."""
import io

import pytest

from scripts import backup_workbook as backup
from tests.workbook_backup_fixtures import CORE_TABS, workbook_export

openpyxl = pytest.importorskip("openpyxl")


def test_fixture_is_readable_by_existing_restore_tool():
    workbook = openpyxl.load_workbook(io.BytesIO(workbook_export()), read_only=True)
    try:
        assert set(workbook.sheetnames) == set(CORE_TABS)
        assert workbook["_Status"]["A1"].value == "Symbol"
        assert workbook["_Status"]["A2"].value == "SYNTH.TEST"
    finally:
        workbook.close()


def test_real_openpyxl_export_is_accepted():
    workbook = openpyxl.Workbook()
    workbook.active.title = CORE_TABS[0]
    for name in CORE_TABS[1:]:
        workbook.create_sheet(name)
    workbook["_Status"].append(["Synthetic header", "Synthetic value"])
    output = io.BytesIO()
    workbook.save(output)
    assert backup._validate_export_xlsx(output.getvalue()) == set(CORE_TABS)
