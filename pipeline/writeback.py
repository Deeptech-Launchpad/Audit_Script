"""
Write-back phase: write validation results into a COPY of the original
workbook. The original file is never touched. Build Value/UOM themselves are
only ever filled in when genuinely blank - a detected correction to an
EXISTING value is reported only through Audit Flag/UOM Comments, never by
overwriting the cell. Other classification logic (the is_same_substance()
notation-difference backstop, the "Filled from X" mislabel downgrade, and the
openpyxl None-clear gotcha) is unchanged from the CLI pipeline - each one was
a real bug found and fixed against production data.
"""
import glob
import json
import os
import re
import shutil

import openpyxl
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

from pipeline.schema import detect_schema


def norm(v):
    if v is None:
        return ""
    return str(v).strip()


def _normalize_for_compare(s):
    if not s:
        return ""
    s = str(s).lower()
    s = s.replace("°", "").replace("²", "2").replace("³", "3").replace("�", "")
    s = re.sub(r"[\s\-_/,;:()]+", "", s)
    return s


def _extract_numbers(s):
    if not s:
        return []
    return sorted(float(n) for n in re.findall(r"-?\d+\.?\d*", str(s)))


def is_same_substance(orig_value, orig_uom, new_value, new_uom):
    """True if a flagged 'correction' is actually just a notation/formatting
    difference (same underlying number or one string is an expansion of the
    other) rather than a substantive contradiction."""
    orig_combo = f"{orig_value or ''} {orig_uom or ''}".strip()
    new_combo = f"{new_value or ''} {new_uom or ''}".strip()
    if not orig_combo or not new_combo:
        return False
    orig_norm = _normalize_for_compare(orig_combo)
    new_norm = _normalize_for_compare(new_combo)
    if orig_norm == new_norm:
        return True
    orig_nums = _extract_numbers(orig_combo)
    new_nums = _extract_numbers(new_combo)
    if orig_nums and new_nums and orig_nums == new_nums:
        return True
    if orig_norm in new_norm or new_norm in orig_norm:
        return True
    return False


def clean(v):
    if v is None:
        return None
    return ILLEGAL_CHARACTERS_RE.sub("", str(v))


def run_writeback_phase(input_path, output_path, results_dir, manifests_dir, sheet, product_keys=None, log=None):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    shutil.copyfile(input_path, output_path)

    wb = openpyxl.load_workbook(output_path)
    ws = wb[sheet] if sheet else wb.worksheets[0]

    header = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    schema = detect_schema(header)
    COL_Z = schema["attr_value_idx"] + 1
    COL_AA = schema["attr_uom_idx"] + 1
    COL_AD = schema["audit_flag_idx"] + 1
    COL_UOM_COMMENTS = schema["uom_comments_idx"] + 1

    if ws.cell(row=1, column=COL_AD).value in (None, ""):
        ws.cell(row=1, column=COL_AD, value="Audit Flag")
    if ws.cell(row=1, column=COL_UOM_COMMENTS).value in (None, ""):
        ws.cell(row=1, column=COL_UOM_COMMENTS, value="UOM Comments")

    result_paths = sorted(glob.glob(os.path.join(results_dir, "*.json")))
    if product_keys is not None:
        result_paths = [rp for rp in result_paths if os.path.splitext(os.path.basename(rp))[0] in product_keys]

    stats = {"rows_covered": 0, "corrected": 0, "filled": 0, "unconfirmed": 0,
             "errors": 0, "confirmed_ok": 0, "load_errors": 0, "no_data_found": 0}

    for rp in result_paths:
        with open(rp, "r", encoding="utf-8") as f:
            data = json.load(f)
        part = os.path.splitext(os.path.basename(rp))[0]

        if "error" in data:
            stats["load_errors"] += 1
            manifest_path = os.path.join(manifests_dir, part + ".json")
            if os.path.exists(manifest_path):
                with open(manifest_path, "r", encoding="utf-8") as mf:
                    manifest = json.load(mf)
                flag = clean(f"VALIDATION ERROR: could not obtain a reliable analysis "
                             f"for this product after retries ({data['error'][:150]}) - needs manual review")
                for attr in manifest.get("attributes", []):
                    ws.cell(row=attr["row_idx"], column=COL_AD, value=flag)
            continue

        seen_rows = set()
        for item in data.get("results", []):
            row_idx = item["row_idx"]
            if row_idx in seen_rows:
                continue
            seen_rows.add(row_idx)

            corrupted = False
            for field in ("final_value", "final_uom", "flag_text", "uom_comment"):
                v = item.get(field)
                if v and (len(str(v)) > 500 or "</body" in str(v) or "<html" in str(v)):
                    corrupted = True
                    break
            if corrupted:
                continue

            final_value = item.get("final_value")
            final_uom = item.get("final_uom")
            flag_text = item.get("flag_text")
            uom_comment = item.get("uom_comment")

            orig_value = norm(ws.cell(row=row_idx, column=COL_Z).value)
            orig_uom = norm(ws.cell(row=row_idx, column=COL_AA).value)
            new_value = norm(final_value)
            new_uom = norm(final_uom)

            stats["rows_covered"] += 1

            is_error = bool(flag_text) and flag_text.startswith(("HEAD NODE ERROR", "PDF ERROR"))
            is_unconfirmed = bool(flag_text) and flag_text.startswith("Could not verify")
            is_marked_correct = flag_text == "Value Correct"
            is_filled_marker = bool(flag_text) and flag_text.startswith("Filled from")
            is_no_data_found = flag_text == "No Data Found in Source"
            is_real_correction = (bool(flag_text) and not is_error and not is_unconfirmed
                                   and not is_marked_correct and not is_filled_marker and not is_no_data_found)
            fill_write = not orig_value

            # allow_write no longer permits writing a correction into Z/AA
            # (see below) - it's kept only to drive this notation-difference
            # downgrade, which still matters for flag_text/stats.
            allow_write = is_real_correction

            if allow_write and not fill_write and is_same_substance(orig_value, orig_uom, final_value, final_uom):
                allow_write = False
                is_real_correction = False
                is_marked_correct = True
                flag_text = "Value Correct"

            if is_filled_marker and not fill_write:
                is_filled_marker = False
                is_marked_correct = True
                flag_text = "Value Correct"
            elif is_filled_marker and fill_write and not new_value and not new_uom:
                is_filled_marker = False
                is_no_data_found = True
                flag_text = "No Data Found in Source"

            if is_error:
                stats["errors"] += 1
            elif is_unconfirmed:
                stats["unconfirmed"] += 1
            elif is_no_data_found:
                stats["no_data_found"] += 1
            elif fill_write and new_value:
                stats["filled"] += 1
            elif is_real_correction and (new_value != orig_value or new_uom != orig_uom):
                stats["corrected"] += 1
            else:
                stats["confirmed_ok"] += 1

            # Build Value/UOM are only ever filled in when genuinely blank -
            # a detected correction to an EXISTING value is reported only via
            # Audit Flag/UOM Comments below; the cell itself is never touched.
            if fill_write:
                # openpyxl's ws.cell(value=...) is a silent no-op when value=None,
                # so an intentional clear needs direct attribute assignment.
                if new_value != orig_value:
                    ws.cell(row=row_idx, column=COL_Z).value = clean(final_value)
                if new_uom != orig_uom:
                    ws.cell(row=row_idx, column=COL_AA).value = clean(final_uom)

            ad_value = clean(flag_text) if flag_text else None
            ws.cell(row=row_idx, column=COL_AD).value = ad_value

            # "Only report rows where a UOM issue exists" - the output is a
            # fresh copy of the original file each run, so the cell already
            # starts blank; only ever write when there's an actual comment.
            if uom_comment:
                ws.cell(row=row_idx, column=COL_UOM_COMMENTS).value = clean(uom_comment)

    wb.save(output_path)
    if log:
        log(f"Saved audited workbook: {json.dumps(stats)}")
    return stats
