"""Small real OOXML packages with synthetic cells; no spreadsheet dependency."""
from __future__ import annotations

import io
import zipfile
from xml.etree import ElementTree as ET

CORE_TABS = ("_Status", "_Run_Log", "Top_10_Investments",
             "_Portfolio_CostBasis", "My_Portfolio", "Shadow_History")
SHEET_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
DOCUMENT_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PACKAGE_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
TYPE_NS = "http://schemas.openxmlformats.org/package/2006/content-types"


def workbook_parts(*, tabs=CORE_TABS, absolute=False, strict=False, hidden=False, empty=False):
    sheet_ns = "http://purl.oclc.org/ooxml/spreadsheetml/main" if strict else SHEET_NS
    doc_ns = "http://purl.oclc.org/ooxml/officeDocument/relationships" if strict else DOCUMENT_NS
    types = ET.Element("Types", xmlns=TYPE_NS)
    ET.SubElement(types, "Default", Extension="rels",
                  ContentType="application/vnd.openxmlformats-package.relationships+xml")
    ET.SubElement(types, "Default", Extension="xml", ContentType="application/xml")
    ET.SubElement(types, "Override", PartName="/xl/workbook.xml",
                  ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml")
    root_rels = ET.Element("Relationships", xmlns=PACKAGE_NS)
    ET.SubElement(root_rels, "Relationship", Id="book", Type=doc_ns + "/officeDocument",
                  Target="/xl/workbook.xml" if absolute else "xl/workbook.xml")
    workbook = ET.Element("workbook", xmlns=sheet_ns)
    sheets = ET.SubElement(workbook, "sheets")
    rels = ET.Element("Relationships", xmlns=PACKAGE_NS)
    parts = {}
    for index, name in enumerate(tabs, 1):
        rid = "arbitrary-id-" + str(index)
        attrs = {"name": name, "sheetId": str(index), "{" + doc_ns + "}id": rid}
        if hidden and index > 1:
            attrs["state"] = "hidden" if index % 2 else "veryHidden"
        ET.SubElement(sheets, "sheet", attrs)
        sheet_path = "worksheets/synthetic-" + str(index) + ".xml"
        ET.SubElement(rels, "Relationship", Id=rid, Type=doc_ns + "/worksheet",
                      Target="/xl/" + sheet_path if absolute else sheet_path)
        ET.SubElement(types, "Override", PartName="/xl/" + sheet_path,
                      ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml")
        worksheet = ET.Element("worksheet", xmlns=sheet_ns)
        rows = ET.SubElement(worksheet, "sheetData")
        if not empty:
            for number, value in ((1, "Symbol"), (2, "SYNTH.TEST")):
                row = ET.SubElement(rows, "row", r=str(number))
                cell = ET.SubElement(row, "c", r="A" + str(number), t="inlineStr")
                ET.SubElement(ET.SubElement(cell, "is"), "t").text = value
        parts["xl/" + sheet_path] = ET.tostring(worksheet, encoding="utf-8")
    parts.update({"[Content_Types].xml": ET.tostring(types),
                  "_rels/.rels": ET.tostring(root_rels),
                  "xl/workbook.xml": ET.tostring(workbook),
                  "xl/_rels/workbook.xml.rels": ET.tostring(rels)})
    return parts


def zip_parts(parts, *, compression=zipfile.ZIP_DEFLATED):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=compression) as archive:
        for path, content in parts.items():
            archive.writestr(path, content)
    return output.getvalue()


def workbook_export(**options):
    return zip_parts(workbook_parts(**options))
