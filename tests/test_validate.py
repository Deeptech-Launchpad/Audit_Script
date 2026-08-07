"""Regression tests for pipeline/validate.py's deterministic UOM backstop.

Gemini reports raw uom_recorded/uom_source tokens; _resolve_uom_comment()
turns them into the fixed "UOM Missing"/"UOM Wrong"/None vocabulary that
matches the reference audit output (input Excel/pilot_Audit_Data_Sample_
Input1.xlsx) this was modeled on. Doing the equivalence judgment in Python
rather than trusting Gemini's own wording is deliberate: live testing
showed it reporting "A" vs "Amp", literally "mm" vs "mm", and vague
non-units like "ID" as if they were mismatches.
"""
from pipeline.validate import (
    _dedupe_uom_from_flag_text, _looks_like_unit_token, _resolve_uom_comment, _uom_equivalent,
)


def test_uom_equivalent_recognizes_same_unit_synonyms():
    assert _uom_equivalent("A", "Amp") is True
    assert _uom_equivalent("V", "Volt") is True
    assert _uom_equivalent("mm", "mm") is True
    assert _uom_equivalent("in", "inch") is True


def test_uom_equivalent_rejects_different_units():
    assert _uom_equivalent("mm", "in") is False
    assert _uom_equivalent("A", "V") is False


def test_looks_like_unit_token_rejects_non_units():
    assert _looks_like_unit_token("ID") is False
    assert _looks_like_unit_token("OD") is False
    assert _looks_like_unit_token("no unit") is False
    assert _looks_like_unit_token("embedded in value") is False
    assert _looks_like_unit_token(None) is False


def test_looks_like_unit_token_accepts_real_units():
    assert _looks_like_unit_token("mm") is True
    assert _looks_like_unit_token("in") is True
    assert _looks_like_unit_token("A") is True


def test_resolve_uom_comment_missing_when_no_recorded_unit():
    item = {"uom_recorded": None, "uom_source": "A"}
    assert _resolve_uom_comment(item) == "UOM Missing"


def test_resolve_uom_comment_wrong_when_units_differ():
    item = {"uom_recorded": "mm", "uom_source": "in"}
    assert _resolve_uom_comment(item) == "UOM Wrong"


def test_resolve_uom_comment_none_when_same_unit_synonym():
    item = {"uom_recorded": "A", "uom_source": "Amp"}
    assert _resolve_uom_comment(item) is None


def test_resolve_uom_comment_none_when_no_source_evidence():
    assert _resolve_uom_comment({"uom_recorded": None, "uom_source": None}) is None
    assert _resolve_uom_comment({"uom_recorded": "mm", "uom_source": None}) is None


def test_resolve_uom_comment_none_when_source_is_not_a_real_unit():
    item = {"uom_recorded": "in", "uom_source": "ID"}
    assert _resolve_uom_comment(item) is None


def test_resolve_uom_comment_pops_raw_fields_from_item():
    item = {"uom_recorded": "mm", "uom_source": "in", "final_value": "x"}
    _resolve_uom_comment(item)
    assert "uom_recorded" not in item
    assert "uom_source" not in item
    assert item["final_value"] == "x"


def test_dedupe_uom_from_flag_text_overrides_when_value_unchanged():
    """Reproduced live against the reference file's own 3.5in=88.9mm example:
    Gemini kept describing the unit issue in flag_text even when explicitly
    told not to - this backstop forces the intended "Value Correct" wording
    whenever uom_comment already covers it and the value itself didn't change."""
    item = {"uom_comment": "UOM Wrong", "flag_text": "Attribute UOM recorded as mm but PDF states in",
            "final_value": "3.5"}
    _dedupe_uom_from_flag_text(item, orig_value="3.5")
    assert item["flag_text"] == "Value Correct"


def test_dedupe_uom_from_flag_text_leaves_real_corrections_alone():
    """Must never touch a genuine case-2 value correction just because its
    wording happens to mention "UOM" - only fires when uom_comment already
    resolved to "UOM Wrong" AND the value itself didn't change."""
    item = {"uom_comment": None, "flag_text": "Attribute UOM recorded as V but HTML states A", "final_value": "5"}
    _dedupe_uom_from_flag_text(item, orig_value="3.5")
    assert item["flag_text"] == "Attribute UOM recorded as V but HTML states A"

    item2 = {"uom_comment": "UOM Wrong", "flag_text": "Attribute Value recorded as 3.5 but PDF states 10",
              "final_value": "10"}
    _dedupe_uom_from_flag_text(item2, orig_value="3.5")
    assert item2["flag_text"] == "Attribute Value recorded as 3.5 but PDF states 10"
