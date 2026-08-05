"""Regression tests for pipeline/schema.py's column-name-driven detection.

The two "real header" tests guard the shapes actually seen in production
(MMEM, Rexel). The "unrecognized" tests guard a real bug found in production:
when a file's Attribute Label/Value columns can't be matched at all,
attr_section_idx used to silently fall back to `n` (one past the last valid
column index), and the very next `row[attr_section_idx]` lookup during the
fetch phase crashed with a bare "tuple index out of range" - a confusing
crash instead of a clear "this file's layout isn't supported" error.
"""
from pipeline.schema import detect_schema


def _mmem_style_header():
    return [
        "Part No", "Manufacturer", "Description",
        "Product URL", "Datasheet", "Datasheet Page#",
        "Attribute Section", "Attribute Label", "Attribute Value",
        "Attribute UOM", "Attribute Flag", "Status",
    ]


def _rexel_style_header():
    return [
        "SKU", "Brand", "Title", "Category",
        "HTML URL 1", "HTML URL 2",
        "Brochure", "Brochure Page#",
        "Manual",
        "Attr Section", "Attr Label", "Attr Value", "Attr UOM", "Attr Flag",
        "Status",
    ]


def test_mmem_style_schema():
    schema = detect_schema(_mmem_style_header())
    assert schema["part_no_idx"] == 0
    assert schema["product_info_idx"] == [1, 2]
    assert schema["html_idx"] == [3]
    assert schema["doc_idx"] == [(4, 5)]
    assert schema["names"][schema["attr_section_idx"]] == "Attribute Section"
    assert schema["names"][schema["attr_label_idx"]] == "Attribute Label"
    assert schema["names"][schema["attr_value_idx"]] == "Attribute Value"
    assert schema["names"][schema["attr_uom_idx"]] == "Attribute UOM"
    assert schema["names"][schema["attr_flag_idx"]] == "Attribute Flag"
    assert schema["names"][schema["status_idx"]] == "Status"
    assert schema["audit_flag_idx"] == len(schema["names"])


def test_rexel_style_schema_multiple_html_and_page_hint():
    schema = detect_schema(_rexel_style_header())
    assert schema["part_no_idx"] == 0
    assert schema["product_info_idx"] == [1, 2, 3]
    assert schema["html_idx"] == [4, 5]
    doc_by_col = dict(schema["doc_idx"])
    header = _rexel_style_header()
    brochure_idx = header.index("Brochure")
    page_idx = header.index("Brochure Page#")
    manual_idx = header.index("Manual")
    assert doc_by_col[brochure_idx] == page_idx
    assert doc_by_col[manual_idx] is None
    assert schema["names"][schema["attr_value_idx"]] == "Attr Value"
    assert schema["names"][schema["attr_uom_idx"]] == "Attr UOM"


def test_attr_value_not_confused_with_attr_uom():
    header = ["Part No", "Attr Label", "Attr Value", "Attr UOM"]
    schema = detect_schema(header)
    assert schema["attr_value_idx"] == 2
    assert schema["attr_uom_idx"] == 3


def test_unrecognized_layout_returns_none_instead_of_out_of_range_index():
    """The real production bug: a header using different wording for the
    attribute block ("Property Name"/"Reading" instead of "Attr Label"/
    "Attr Value") must NOT produce an attr_section_idx that points past the
    end of every data row."""
    header = ["Part No", "Manufacturer", "Product URL", "Section",
              "Property Name", "Reading", "UOM", "Source", "Status"]
    schema = detect_schema(header)

    assert schema["attr_label_idx"] is None
    assert schema["attr_value_idx"] is None
    # This is the crux of the fix: must be None (unknown), never `len(header)`
    # (which is exactly one past the last valid index in a same-length data row).
    assert schema["attr_section_idx"] is None
    assert schema["attr_section_idx"] != len(header)

    # A same-length data row must never be indexable-out-of-range using any
    # of the returned indices when they ARE present.
    row = tuple(range(len(header)))
    for key in ("attr_label_idx", "attr_value_idx", "attr_uom_idx", "attr_flag_idx", "attr_section_idx"):
        idx = schema[key]
        if idx is not None:
            row[idx]  # must not raise


def test_anchor_at_column_zero_is_not_treated_as_not_found():
    """`0 is not None` - a legitimate match at column 0 must not be treated
    the same as "nothing found" (0 is falsy in Python, which is exactly the
    subtle bug this guards against)."""
    header = ["Attr Label", "Attr Value", "Attr UOM"]
    schema = detect_schema(header)
    assert schema["attr_label_idx"] == 0
    assert schema["attr_section_idx"] == 0  # clamped to 0, not -1 (which would wrap to the last column)
