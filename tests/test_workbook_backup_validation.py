"""Actual export validation and storage ordering, with every remote call fake."""
from __future__ import annotations

import datetime as dt
import io
import zipfile
from types import SimpleNamespace
from xml.etree import ElementTree as ET
from xml.parsers import expat

import pytest

from scripts import backup_workbook as backup
from tests.workbook_backup_fixtures import (
    CORE_TABS,
    DOCUMENT_NS,
    SHEET_NS,
    workbook_export,
    workbook_parts,
    zip_parts,
)


def change_xml(parts, path, mutation):
    root = ET.fromstring(parts[path])
    mutation(root)
    parts[path] = ET.tostring(root)


def invalid_export(case):
    parts = workbook_parts()
    sheet = "xl/worksheets/synthetic-1.xml"
    rels = "xl/_rels/workbook.xml.rels"
    if case == "html":
        return b"<html>temporary export failure</html>" + b" " * 1_000_000
    if case == "truncated":
        return zip_parts(parts)[:-20]
    if case == "crc":
        # Alter actual stored XML bytes without changing the ZIP's CRC.
        return zip_parts(parts, compression=zipfile.ZIP_STORED).replace(b"SYNTH.TEST", b"BROKE.TEST", 1)
    if case == "missing_core":
        return workbook_export(tabs=CORE_TABS[:-1])
    if case == "malformed_sheet":
        parts[sheet] = b'<worksheet xmlns="' + SHEET_NS.encode() + b'"><sheetData>'
    elif case == "wrong_sheet_root":
        parts[sheet] = b'<not_a_worksheet xmlns="' + SHEET_NS.encode() + b'"/>'
    elif case == "missing_sheet":
        del parts[sheet]
    elif case == "external_sheet":
        change_xml(parts, rels, lambda root: root[0].set("TargetMode", "External"))
    elif case == "wrong_sheet_type":
        change_xml(parts, rels, lambda root: root[0].set("Type", DOCUMENT_NS + "/chartsheet"))
    elif case == "duplicate_relationship":
        change_xml(parts, rels, lambda root: root[1].set("Id", root[0].get("Id")))
    elif case == "duplicate_title":
        change_xml(parts, "xl/workbook.xml", lambda root: root[0][1].set("name", root[0][0].get("name")))
    elif case == "shared_sheet_part":
        change_xml(parts, rels, lambda root: root[1].set("Target", root[0].get("Target")))
    elif case == "bad_content_type":
        change_xml(parts, "[Content_Types].xml", lambda root: root[2].set("ContentType", "text/plain"))
    elif case == "bad_sheet_content_type":
        change_xml(parts, "[Content_Types].xml", lambda root: root[3].set("ContentType", "text/plain"))
    elif case == "missing_package_relationships":
        del parts["_rels/.rels"]
    elif case == "traversal":
        change_xml(parts, rels, lambda root: root[0].set("Target", "../../outside.xml"))
    elif case == "dtd":
        parts[sheet] = (b'<!DOCTYPE worksheet [<!ENTITY sample "not workbook data">]>'
                        + parts[sheet])
    elif case == "binary_crc":
        parts["xl/media/synthetic.bin"] = b"synthetic binary attachment"
        return zip_parts(parts, compression=zipfile.ZIP_STORED).replace(
            b"synthetic binary attachment", b"damaged__ binary attachment", 1)
    else:
        raise AssertionError(case)
    return zip_parts(parts)


@pytest.mark.parametrize("options", ({}, {"absolute": True}, {"strict": True},
                                      {"hidden": True}, {"empty": True}))
def test_valid_package_variants_keep_existing_core_tab_contract(options):
    assert backup._validate_export_xlsx(workbook_export(**options)) == set(CORE_TABS)


@pytest.mark.parametrize("case", (
    "html", "truncated", "crc", "binary_crc", "missing_core", "malformed_sheet",
    "wrong_sheet_root", "missing_sheet", "external_sheet", "wrong_sheet_type",
    "duplicate_relationship", "duplicate_title", "shared_sheet_part",
    "bad_content_type", "bad_sheet_content_type", "missing_package_relationships", "traversal", "dtd",
))
def test_damaged_or_structurally_invalid_packages_are_rejected(case):
    expected = (zipfile.BadZipFile if case in {"html", "truncated", "crc", "binary_crc"}
                else expat.ExpatError if case == "malformed_sheet" else ValueError)
    with pytest.raises(expected):
        backup._validate_export_xlsx(invalid_export(case))


def test_valid_noncore_chartsheet_is_permitted():
    parts = workbook_parts(tabs=CORE_TABS + ("Chart",))
    change_xml(parts, "xl/_rels/workbook.xml.rels",
               lambda root: root[-1].set("Type", DOCUMENT_NS + "/chartsheet"))
    change_xml(parts, "[Content_Types].xml", lambda root: root[-1].set(
        "ContentType", "application/vnd.openxmlformats-officedocument.spreadsheetml.chartsheet+xml"))
    parts["xl/worksheets/synthetic-7.xml"] = b'<chartsheet xmlns="' + SHEET_NS.encode() + b'"/>'
    assert backup._validate_export_xlsx(zip_parts(parts)) == set(CORE_TABS) | {"Chart"}


def test_noncore_sheet_cannot_reference_an_image():
    parts = workbook_parts(tabs=CORE_TABS + ("Not a sheet",))
    change_xml(parts, "xl/_rels/workbook.xml.rels",
               lambda root: root[-1].set("Type", DOCUMENT_NS + "/image"))
    with pytest.raises(ValueError, match="typed sheet part"):
        backup._validate_export_xlsx(zip_parts(parts))


def test_duplicate_zip_members_are_rejected():
    output = io.BytesIO(workbook_export())
    with zipfile.ZipFile(output, "a") as archive, pytest.warns(UserWarning, match="Duplicate name"):
        archive.writestr("xl/workbook.xml", b"duplicate")
    with pytest.raises(ValueError, match="repeats a part"):
        backup._validate_export_xlsx(output.getvalue())


@pytest.mark.parametrize("limit", ("_XLSX_MAX_PARTS", "_XLSX_MAX_EXPANDED_BYTES"))
def test_package_bounds_are_checked_before_decompression(monkeypatch, limit):
    payload = workbook_export()
    monkeypatch.setattr(backup, limit, 1)
    monkeypatch.setattr(zipfile.ZipFile, "open", lambda *_: pytest.fail("bounds must precede reads"))
    with pytest.raises(ValueError, match="exceeds bounds"):
        backup._validate_export_xlsx(payload)


def test_metadata_has_an_independent_memory_bound(monkeypatch):
    monkeypatch.setattr(backup, "_XLSX_MAX_METADATA_BYTES", 1)
    with pytest.raises(ValueError, match="metadata.*exceeds bounds"):
        backup._validate_export_xlsx(workbook_export())


def test_repetitive_valid_worksheet_has_no_compression_ratio_rejection():
    parts = workbook_parts()
    sheet = "xl/worksheets/synthetic-1.xml"
    parts[sheet] = parts[sheet].replace(b"SYNTH.TEST", b"SYNTH.TEST " * 100_000)
    payload = zip_parts(parts)
    assert len(parts[sheet]) > 100 * len(payload)
    assert backup._validate_export_xlsx(payload) == set(CORE_TABS)


@pytest.fixture
def offline_backup(monkeypatch):
    events = []
    receipts = []
    for name, value in {
        "DEFAULT_SPREADSHEET_ID": "synthetic-sheet", "TFB_BACKUP_FOLDER_ID": "synthetic-folder",
        "TFB_BACKUP_GCS_BUCKET": "synthetic-bucket", "TFB_BACKUP_MIN_BYTES": "1",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("TFB_BACKUP_LOCAL_COPY", raising=False)
    monkeypatch.setattr(backup, "_credentials", lambda: SimpleNamespace(_tfb_client_email="offline"))
    monkeypatch.setattr(backup, "_session", lambda _: object())
    monkeypatch.setattr(backup, "_now_riyadh", lambda: dt.datetime(2026, 10, 17, tzinfo=backup._RIYADH))
    monkeypatch.setattr(backup, "_oauth_session", lambda: events.append("oauth_token") or object())
    monkeypatch.setattr(backup, "drive_ensure_app_folder", lambda *_: events.append("folder") or "app-folder")
    monkeypatch.setattr(backup, "export_xlsx", lambda *_: events.append("export") or workbook_export())
    monkeypatch.setattr(backup, "_write_local_copy", lambda *_: events.append("local_copy"))
    monkeypatch.setattr(backup, "_store_upload", lambda *args: events.append("upload") or {"id": "new", "name": args[-2]})
    monkeypatch.setattr(backup, "_store_list", lambda *_: events.append("list") or [
        {"id": "expired", "name": backup.daily_name(dt.date(2026, 10, 15))},
        {"id": "new", "name": backup.daily_name(dt.date(2026, 10, 17))},
    ])
    monkeypatch.setattr(backup, "_store_copy", lambda *_: events.append("monthly_copy"))
    monkeypatch.setattr(backup, "_store_delete", lambda *_: events.append("delete"))
    monkeypatch.setenv("TFB_BACKUP_KEEP_DAILY", "1")
    monkeypatch.setattr(backup, "append_run_log", lambda *args: receipts.append(args))
    return events, receipts


@pytest.mark.parametrize("dest", ("drive", "oauth", "gcs"))
@pytest.mark.parametrize("case", ("html", "truncated", "crc", "missing_core", "malformed_sheet"))
def test_invalid_exports_fail_before_destination_and_local_mutation(monkeypatch, offline_backup, dest, case):
    events, receipts = offline_backup
    monkeypatch.setenv("TFB_BACKUP_DEST", dest)
    monkeypatch.setattr(backup, "export_xlsx", lambda *_: events.append("export") or invalid_export(case))
    if case == "html":
        monkeypatch.setenv("TFB_BACKUP_MIN_BYTES", "1000000")
    assert backup.run_backup(dry_run=False) == 3
    assert events == (["oauth_token", "export"] if dest == "oauth" else ["export"])
    assert receipts[-1][0] == "FAIL"
    assert receipts[-1][2]["stage"] == "export_validation"


@pytest.mark.parametrize("dest", ("drive", "oauth", "gcs"))
def test_valid_backup_keeps_upload_monthly_and_retention_flow(monkeypatch, offline_backup, dest):
    events, receipts = offline_backup
    monkeypatch.setenv("TFB_BACKUP_DEST", dest)
    assert backup.run_backup(dry_run=False) == 0
    prefix = ["oauth_token", "export", "local_copy", "folder"] if dest == "oauth" else ["export", "local_copy"]
    assert events == prefix + ["upload", "list", "monthly_copy", "delete"]
    assert receipts[-1][0] == "OK"
    assert receipts[-1][2]["pruned"] == 1


@pytest.mark.parametrize("dest", ("drive", "oauth", "gcs"))
@pytest.mark.parametrize("valid", (False, True))
def test_dry_run_validates_without_destination_setup(monkeypatch, offline_backup, dest, valid):
    events, receipts = offline_backup
    monkeypatch.setenv("TFB_BACKUP_DEST", dest)
    payload = workbook_export() if valid else invalid_export("html")
    monkeypatch.setattr(backup, "export_xlsx", lambda *_: events.append("export") or payload)
    assert backup.run_backup(dry_run=True) == (0 if valid else 3)
    expected = ["oauth_token", "export"] if dest == "oauth" else ["export"]
    assert events == expected + (["local_copy"] if valid else [])
    assert not valid or not receipts


def test_oauth_credential_failure_still_precedes_export(monkeypatch, offline_backup):
    events, receipts = offline_backup
    monkeypatch.setenv("TFB_BACKUP_DEST", "oauth")
    def failed_grant():
        events.append("oauth_token")
        raise RuntimeError("synthetic expired grant")
    monkeypatch.setattr(backup, "_oauth_session", failed_grant)
    assert backup.run_backup(dry_run=False) == 2
    assert events == ["oauth_token"]
    assert receipts[-1][2]["stage"] == "oauth"


def test_validation_failure_diagnostics_do_not_quote_private_input(monkeypatch, offline_backup, capsys):
    events, receipts = offline_backup
    private_marker = "SYNTHETIC_PRIVATE_XML_MEMBER_4791"
    def invalid_package(_data):
        raise ValueError(private_marker)
    monkeypatch.setenv("TFB_BACKUP_DEST", "oauth")
    monkeypatch.setattr(backup, "_validate_export_xlsx", invalid_package)
    assert backup.run_backup(dry_run=False) == 3
    assert events == ["oauth_token", "export"]
    assert receipts[-1][0] == "FAIL"
    assert private_marker not in repr(receipts)
    stderr = capsys.readouterr().err
    assert private_marker not in stderr
    assert "FAIL export validation: ValueError" in stderr
