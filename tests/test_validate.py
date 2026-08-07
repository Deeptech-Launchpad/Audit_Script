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
    _dedupe_uom_from_flag_text, _fix_unreadable_source_flag, _looks_like_unit_token,
    _resolve_uom_comment, _uom_equivalent, units_from_allowed_values, units_from_definition,
)


# --------------------------------------------------------------------------
# Allowed Values / Definition as the authoritative source of the expected unit
# --------------------------------------------------------------------------

def test_units_from_allowed_values_reads_a_unit_list():
    assert units_from_allowed_values('[ "0.5 g/cc", "0.55 g/cc", "0.8 g/cc"]') == {"g/cc"}
    assert units_from_allowed_values('[ "5 sec", "10 min", "24 hr", "3 day"]') == {"sec", "min", "hr", "day"}
    assert units_from_allowed_values('[ "23.0 cu ft", "7.2 cu ft"]') == {"cu ft"}
    # Ranges and attached degree symbols still yield the trailing unit.
    assert units_from_allowed_values('[ "1 to 2 3/4 in", "2 3/8 in"]') == {"in"}
    assert units_from_allowed_values('[ "-4°F", "-20°C"]') == {"f", "c"}


def test_units_from_allowed_values_ignores_controlled_vocabularies():
    """A list of NAMES must never be read as a list of units - "1/8 in NPT"
    ends in a short word a shape-based check would call a unit."""
    assert units_from_allowed_values("1/8 in NPT; 3/16 Hose Barb; 1/4 in NPT") == set()
    assert units_from_allowed_values("Round; Oval; Square") == set()
    assert units_from_allowed_values('[ "100,000 Cycles", "1 yr"]') == set()
    assert units_from_allowed_values(None) == set()


def test_units_from_definition():
    assert units_from_definition("capture the interior capacity of the oven in units of cu. ft.") == {"cu ft"}
    assert units_from_definition("overall capacity of the freezer measured in cubic feet.") == {"cu ft"}
    assert units_from_definition("the type of connection on the fittings") == set()


def test_allowed_values_unit_overrides_noisy_raw_value():
    """The reported false positives: the recorded UOM matched the schema
    exactly, but Raw Value spelled it differently - a typo ("g/c3"), a plural
    ("mils"), a long form ("hours"), punctuation ("Cu. Ft."), an inch mark, or
    a parenthetical conversion ("20 cm"). None are real UOM problems."""
    for recorded, raw_unit, allowed in [
        ("g/cc", "g/c3", '[ "0.5 g/cc", "0.8 g/cc"]'),
        ("hr", "hours", '[ "5 sec", "10 min", "24 hr"]'),
        ("cu ft", "Cu. Ft.", '[ "23.0 cu ft", "7.2 cu ft"]'),
        ("in", '"', '[ "30 in", "22-1/8 in"]'),
        ("in", "cm", '[ "12 in", "16 in"]'),
    ]:
        item = {"uom_recorded": recorded, "uom_source": raw_unit}
        assert _resolve_uom_comment(item, "number", allowed, None, "10") is None, f"{recorded} vs {raw_unit}"


def test_allowed_values_still_catches_a_real_unit_mismatch():
    """Schema says inches; the value was recorded in mils."""
    item = {"uom_recorded": "mil", "uom_source": "mils"}
    assert _resolve_uom_comment(item, "number", '[ "0.065 in", "0.14 in"]', None, "3.5") == "UOM Wrong"


def test_allowed_values_flags_missing_unit():
    item = {"uom_recorded": None, "uom_source": None}
    assert _resolve_uom_comment(item, "number", '[ "-4°F", "-20°C"]', None, "-30") == "UOM Missing"


def test_multi_part_uom_cell_is_accepted():
    """A value covering two measurements repeats its unit ("in; in")."""
    item = {"uom_recorded": "in; in", "uom_source": None}
    assert _resolve_uom_comment(item, "number", '[ "1 3/16 in", "2 3/8 in"]', None, "1 1/2; 2 1/8") is None


def test_non_unit_vocabulary_attribute_never_gets_a_uom_comment():
    """Collar Type (Round/Oval/Square) has no unit concept at all."""
    item = {"uom_recorded": None, "uom_source": "Round"}
    assert _resolve_uom_comment(item, "text", "Round; Oval; Square", None, "Round") is None


# --------------------------------------------------------------------------
# "No Data Found in Source" must not be claimed when nothing could be read
# --------------------------------------------------------------------------

def _manifest(html_status, doc_status=None):
    manifest = {"sources": {"html": {"h1": {"status": html_status, "error": "HTTP 401"}}, "docs": {}}}
    attr = {"sources": {"html": ["h1"], "docs": []}}
    if doc_status:
        manifest["sources"]["docs"]["d1"] = {"status": doc_status, "error": "PDF parse error"}
        attr["sources"]["docs"] = [{"hash": "d1"}]
    return manifest, attr


def test_no_data_found_rewritten_when_every_source_failed():
    manifest, attr = _manifest("error")
    item = {"flag_text": "No Data Found in Source"}
    _fix_unreadable_source_flag(item, manifest, attr)
    assert item["flag_text"] == "HEAD NODE ERROR: HTTP 401"


def test_no_data_found_kept_when_a_source_actually_loaded():
    """The wording is a real finding when the sources were readable."""
    manifest, attr = _manifest("ok")
    item = {"flag_text": "No Data Found in Source"}
    _fix_unreadable_source_flag(item, manifest, attr)
    assert item["flag_text"] == "No Data Found in Source"


def test_other_flags_are_never_rewritten_by_the_source_backstop():
    manifest, attr = _manifest("error")
    item = {"flag_text": "Value Correct"}
    _fix_unreadable_source_flag(item, manifest, attr)
    assert item["flag_text"] == "Value Correct"


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
    assert _resolve_uom_comment(item, attr_value="30 to 100") == "UOM Missing"


def test_resolve_uom_comment_wrong_when_units_differ():
    item = {"uom_recorded": "mm", "uom_source": "in"}
    assert _resolve_uom_comment(item, attr_value="3.5") == "UOM Wrong"


def test_resolve_uom_comment_none_when_same_unit_synonym():
    item = {"uom_recorded": "A", "uom_source": "Amp"}
    assert _resolve_uom_comment(item, attr_value="200") is None


def test_resolve_uom_comment_none_when_no_source_evidence():
    assert _resolve_uom_comment({"uom_recorded": None, "uom_source": None}, attr_value="5") is None
    assert _resolve_uom_comment({"uom_recorded": "mm", "uom_source": None}, attr_value="5") is None


def test_resolve_uom_comment_wrong_for_non_unit_on_number_data_type():
    """Reported by the user against a real row: data_type="number" but the
    recorded unit is "ID" (inner diameter - a dimension qualifier, not a
    unit) - must be "UOM Wrong" even with no raw_value evidence at all."""
    item = {"uom_recorded": "ID", "uom_source": None}
    assert _resolve_uom_comment(item, data_type="number", attr_value="1/8 through 1/4 ID") == "UOM Wrong"


def test_resolve_uom_comment_non_unit_ignored_for_text_data_type():
    """The structural number-data-type check must not fire for other data
    types - a "text" attribute may legitimately embed non-unit qualifiers."""
    item = {"uom_recorded": "ID", "uom_source": None}
    assert _resolve_uom_comment(item, data_type="text", attr_value="1/8 through 1/4 ID") is None


def test_resolve_uom_comment_real_unit_on_number_data_type_is_fine():
    item = {"uom_recorded": "mm", "uom_source": None}
    assert _resolve_uom_comment(item, data_type="number", attr_value="88.9") is None


def test_resolve_uom_comment_none_when_source_is_not_a_real_unit():
    item = {"uom_recorded": "in", "uom_source": "ID"}
    assert _resolve_uom_comment(item, attr_value="1/4") is None


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


def test_blank_attribute_never_gets_a_uom_comment():
    """A row with no value has nothing for a unit to qualify - its Audit Flag
    already reports "No Data Found in Source". Flagging "UOM Missing" too was
    duplicate noise on every blank row of a unit-bearing attribute."""
    for blank in (None, "", "   "):
        item = {"uom_recorded": None, "uom_source": "in"}
        assert _resolve_uom_comment(item, "number", '[ "3 in", "4 in"]', None, blank) is None
