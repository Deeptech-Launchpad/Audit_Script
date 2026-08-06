"""Regression tests for pipeline/writeback.py.

is_same_substance()/clean()/norm() are pure-function regression guards.
run_writeback_phase() is exercised end-to-end against a real temp workbook +
result JSON files to lock in the exact output wording/columns requested:
"Value Correct" (not "Correct"), "No Data Found in Source" for a genuinely
empty attribute, and a new "UOM Comments" column populated only on rows
that actually have a UOM issue.
"""
import json
import os

import openpyxl
import pytest

from pipeline.writeback import clean, is_same_substance, norm, run_writeback_phase

HEADER = [
    "Part No", "Manufacturer", "Product URL",
    "Attribute Section", "Attribute Name", "Attribute Value", "UOM", "Source Flag", "Status",
]


def _make_workbook(tmp_path, rows):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    ws.append(HEADER)
    for row in rows:
        ws.append(row)
    path = os.path.join(tmp_path, "input.xlsx")
    wb.save(path)
    return path


def _write_manifest(manifests_dir, part, row_indices):
    manifest = {
        "product": {"Part No": part},
        "sources": {"html": {}, "docs": {}},
        "attributes": [{"row_idx": i, "sources": {"docs": []}} for i in row_indices],
    }
    with open(os.path.join(manifests_dir, f"{part}.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f)


def _write_result(results_dir, part, results):
    with open(os.path.join(results_dir, f"{part}.json"), "w", encoding="utf-8") as f:
        json.dump({"results": results}, f)


def test_norm_handles_none_and_whitespace():
    assert norm(None) == ""
    assert norm("  Amp  ") == "Amp"


def test_same_substance_unit_abbreviation():
    assert is_same_substance("110", "MPa", "110", "Mpa") is True
    assert is_same_substance("250", "V", "230", "V") is False


def test_clean_strips_illegal_xml_characters():
    assert clean("110\x08 MPa") == "110 MPa"
    assert clean(None) is None


@pytest.fixture
def workbook_env(tmp_path):
    rows = [
        # row 2: verified correct as-is
        ["SKU-1", "TestCo", "https://example.com/1", "General", "Voltage", "230", "V", "MFR_HTML", "Active"],
        # row 3: genuinely blank, nothing found anywhere
        ["SKU-1", "TestCo", "https://example.com/1", "General", "Weight", "", "", "MFR_HTML", "Active"],
        # row 4: has a UOM issue to report
        ["SKU-1", "TestCo", "https://example.com/1", "General", "Length", "10", "mm", "MFR_HTML", "Active"],
    ]
    input_path = _make_workbook(tmp_path, rows)
    manifests_dir = os.path.join(tmp_path, "manifests")
    results_dir = os.path.join(tmp_path, "results")
    os.makedirs(manifests_dir)
    os.makedirs(results_dir)
    _write_manifest(manifests_dir, "SKU-1", [2, 3, 4])
    return {"input_path": input_path, "manifests_dir": manifests_dir, "results_dir": results_dir, "tmp_path": tmp_path}


def test_value_correct_wording_not_bare_correct(workbook_env):
    _write_result(workbook_env["results_dir"], "SKU-1", [
        {"row_idx": 2, "final_value": "230", "final_uom": "V", "flag_text": "Value Correct", "uom_comment": None},
        {"row_idx": 3, "final_value": None, "final_uom": None, "flag_text": "No Data Found in Source", "uom_comment": None},
        {"row_idx": 4, "final_value": "10", "final_uom": "mm", "flag_text": "Value Correct",
         "uom_comment": "UOM recorded as mm but source indicates in"},
    ])
    output_path = os.path.join(workbook_env["tmp_path"], "out.xlsx")
    stats = run_writeback_phase(
        input_path=workbook_env["input_path"], output_path=output_path,
        results_dir=workbook_env["results_dir"], manifests_dir=workbook_env["manifests_dir"], sheet="Sheet1",
    )

    wb = openpyxl.load_workbook(output_path, data_only=True)
    ws = wb["Sheet1"]
    header = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    assert header[-1] == "UOM Comments"
    assert header[-2] == "Audit Flag"

    audit_flag_col = len(header) - 1
    uom_comments_col = len(header)

    assert ws.cell(row=2, column=audit_flag_col).value == "Value Correct"
    assert ws.cell(row=2, column=audit_flag_col).value != "Correct"
    assert ws.cell(row=3, column=audit_flag_col).value == "No Data Found in Source"
    assert ws.cell(row=4, column=audit_flag_col).value == "Value Correct"

    # UOM Comments: populated only for the row with an actual issue.
    assert ws.cell(row=2, column=uom_comments_col).value is None
    assert ws.cell(row=3, column=uom_comments_col).value is None
    assert ws.cell(row=4, column=uom_comments_col).value == "UOM recorded as mm but source indicates in"

    assert stats["no_data_found"] == 1
    assert stats["confirmed_ok"] == 2


def test_mislabeled_fill_downgrades_to_no_data_found(workbook_env):
    """Same backstop as before (a "Filled from X" marker where nothing was
    actually filled), just with the updated wording."""
    _write_result(workbook_env["results_dir"], "SKU-1", [
        {"row_idx": 2, "final_value": "230", "final_uom": "V", "flag_text": "Value Correct", "uom_comment": None},
        {"row_idx": 3, "final_value": None, "final_uom": None, "flag_text": "Filled from HTML", "uom_comment": None},
        {"row_idx": 4, "final_value": "10", "final_uom": "mm", "flag_text": "Value Correct", "uom_comment": None},
    ])
    output_path = os.path.join(workbook_env["tmp_path"], "out2.xlsx")
    run_writeback_phase(
        input_path=workbook_env["input_path"], output_path=output_path,
        results_dir=workbook_env["results_dir"], manifests_dir=workbook_env["manifests_dir"], sheet="Sheet1",
    )
    wb = openpyxl.load_workbook(output_path, data_only=True)
    ws = wb["Sheet1"]
    header = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    audit_flag_col = len(header) - 1
    assert ws.cell(row=3, column=audit_flag_col).value == "No Data Found in Source"
