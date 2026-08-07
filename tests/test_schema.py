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


def _deeptech_template_header():
    """The real header from Templete/Audit_Data_Sample_Input.xlsx - same
    underlying shape as the Rexel template, but a different naming
    convention for the attribute block: "Attribute Name" instead of "Attr
    Label", "Build Value" instead of "Attr Value" (plus an extra "Raw
    Value" column that must NOT be mistaken for it), a bare "UOM" instead
    of "Attr UOM", and "Source Flag" instead of "Attr Flag"."""
    return [
        "SKU", "Short Description", "Taxonomy", "Manufacturer Name", "MPN 1", "MPN 2",
        "Data Source URL 1", "Data Source URL 2",
        "Datasheet 1", "Datasheet 1 Page#", "Datasheet 2", "Datasheet 2 Page#",
        "Brochure 1", "Brochure 1 Page#", "Brochure 2", "Brochure 2 Page#",
        "Installation Manual 1", "Instruction Manual 1", "User/Owner Manual 1",
        "Drawing Sheet", "Parts List", "SDS/MSDS Sheet", "Warranty",
        "Certfications/Standards 1", "Environmental Product Declaration and PEP",
        "Additional Document 1", "Additional Document 2", "Additional Document 3",
        "Client URL", "Client PDF",
        "Third Party URL 1", "Third Party URL 2", "Third Party URL 3",
        "Third Party URL 4", "Third Party URL 5",
        "Third Party PDF URL", "Third Party PDF Page#",
        "Attribute Section", "Attribute Name", "Raw Value", "Build Value", "UOM", "Source Flag",
        "Status",
    ]


def test_deeptech_template_attribute_block_detected():
    header = _deeptech_template_header()
    schema = detect_schema(header)
    assert header[schema["attr_section_idx"]] == "Attribute Section"
    assert header[schema["attr_label_idx"]] == "Attribute Name"
    assert header[schema["attr_value_idx"]] == "Build Value"  # not "Raw Value"
    assert header[schema["attr_uom_idx"]] == "UOM"
    assert header[schema["attr_flag_idx"]] == "Source Flag"
    assert header[schema["status_idx"]] == "Status"

    # "Raw Value" must never be picked up as any recognized field.
    raw_value_idx = header.index("Raw Value")
    recognized = {schema[k] for k in
                  ("attr_section_idx", "attr_label_idx", "attr_value_idx", "attr_uom_idx", "attr_flag_idx")}
    assert raw_value_idx not in recognized


def test_deeptech_template_sources_and_page_hints():
    header = _deeptech_template_header()
    schema = detect_schema(header)

    html_names = {header[i] for i in schema["html_idx"]}
    assert html_names == {
        "Data Source URL 1", "Data Source URL 2", "Client URL",
        "Third Party URL 1", "Third Party URL 2", "Third Party URL 3",
        "Third Party URL 4", "Third Party URL 5",
    }

    doc_by_name = {header[col]: (header[page] if page is not None else None) for col, page in schema["doc_idx"]}
    assert doc_by_name["Datasheet 1"] == "Datasheet 1 Page#"
    assert doc_by_name["Datasheet 2"] == "Datasheet 2 Page#"
    assert doc_by_name["Brochure 1"] == "Brochure 1 Page#"
    assert doc_by_name["Brochure 2"] == "Brochure 2 Page#"
    # The real mismatch this template exposed: the document column repeats
    # "URL" in its own name while the page-hint column's base name doesn't,
    # so an exact-match pairing would silently miss it.
    assert doc_by_name["Third Party PDF URL"] == "Third Party PDF Page#"
    assert "Certfications/Standards 1" in doc_by_name  # typo'd keyword still lands in doc_idx via the fallback branch


def test_deeptech_template_raw_value_detected_as_its_own_field():
    """"Raw Value" is never one of the core attr_* fields (see above), but it
    IS its own optional enrichment field - needed as UOM-check evidence."""
    header = _deeptech_template_header()
    schema = detect_schema(header)
    assert header[schema["raw_value_idx"]] == "Raw Value"


def _enrichment_header():
    """pilot_Audit_Data_Sample_Input1/23.xlsx add Definition/Allowed Values/
    Data Type ahead of Raw Value/Build Value."""
    return [
        "SKU", "Attribute Section", "Attribute Name", "Definition", "Allowed Values", "Data Type",
        "Raw Value", "Build Value", "UOM", "Source Flag", "Status",
    ]


def test_enrichment_columns_detected():
    header = _enrichment_header()
    schema = detect_schema(header)
    assert header[schema["definition_idx"]] == "Definition"
    assert header[schema["allowed_values_idx"]] == "Allowed Values"
    assert header[schema["data_type_idx"]] == "Data Type"
    assert header[schema["raw_value_idx"]] == "Raw Value"
    assert header[schema["attr_value_idx"]] == "Build Value"
