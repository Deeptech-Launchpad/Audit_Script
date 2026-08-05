"""
Column-name-driven schema detection for the audit workbooks.

Different batches (MMEM, Rexel, ...) use different column layouts and names,
but share the same underlying shape: a part-number column, some product-info
columns, a variable number of HTML/document source columns (optionally paired
with a "<name> Page#" column), then a fixed Attribute Section/Label/Value/
UOM/Flag/Status block. Detecting by column NAME instead of hardcoded position
lets one pipeline handle any such layout without per-file code changes.
"""
import re

HTML_KEYWORDS = ("url",)
DOC_NAME_KEYWORDS = (
    "pdf", "datasheet", "brochure", "manual", "sheet", "list", "warranty",
    "certif", "declaration", "document", "drawing", "catalogue", "catalog",
)


def _norm(name):
    return (name or "").strip().lower()


def is_html_col(name):
    n = _norm(name)
    return "url" in n and "pdf" not in n


def is_doc_col(name):
    n = _norm(name)
    if is_html_col(name):
        return False
    return any(kw in n for kw in DOC_NAME_KEYWORDS)


def is_page_hint_col(name):
    n = _norm(name)
    return bool(re.search(r"page\s*#?$", n))


def page_hint_base_name(name):
    n = (name or "").strip()
    return re.sub(r"\s*page\s*#?$", "", n, flags=re.IGNORECASE).strip()


def detect_schema(header_row):
    """header_row: list of column header strings (0-indexed, column A first).
    Returns a dict describing the layout; all *_idx fields are 0-indexed."""
    names = [h if isinstance(h, str) else ("" if h is None else str(h)) for h in header_row]
    n = len(names)

    def find_exact(*candidates):
        cands = [c.lower() for c in candidates]
        for i, h in enumerate(names):
            if _norm(h) in cands:
                return i
        return None

    def find_contains(*must_contain_all):
        for i, h in enumerate(names):
            hl = _norm(h)
            if all(kw in hl for kw in must_contain_all):
                return i
        return None

    attr_section_idx = find_exact("attribute section")
    attr_label_idx = find_contains("attr", "label")
    attr_value_idx = None
    for i, h in enumerate(names):
        hl = _norm(h)
        if "attr" in hl and "value" in hl and "uom" not in hl:
            attr_value_idx = i
            break
    attr_uom_idx = find_contains("attr", "uom")
    attr_flag_idx = find_contains("attr", "flag")
    status_idx = find_exact("status")

    if attr_section_idx is None:
        # Fallback: derive from attr_label/value position if the exact label differs.
        # `is not None` matters here - a legitimate column 0 is falsy but valid,
        # and previously fell through to the "not found" branch by mistake.
        anchor = attr_label_idx if attr_label_idx is not None else attr_value_idx
        if anchor is not None:
            attr_section_idx = max(0, anchor - 1)
        else:
            # Neither an Attribute Label nor an Attribute Value column could be
            # found at all - this file's layout doesn't match what this pipeline
            # expects. Previously this fell back to `n` (one past the last
            # valid column), which any later `row[attr_section_idx]` lookup
            # would crash on with "tuple index out of range" instead of a
            # clear, actionable error. None here forces callers to check and
            # fail with a real explanation before ever touching a data row.
            attr_section_idx = None

    part_no_idx = 0  # column A is always the product identifier by convention

    page_hints = {}
    html_idx = []
    doc_idx = []  # list of (col_idx, page_idx_or_None)
    product_info_idx = []

    # If attr_section_idx couldn't be determined at all (no Attribute
    # Label/Value column found by any heuristic), there's no reliable
    # boundary for this loop - leave everything empty rather than guess, and
    # let the caller see attr_section_idx is None and fail with a clear
    # message instead of misinterpreting arbitrary columns as product info.
    if attr_section_idx is not None:
        # Page-hint columns: name -> column idx, keyed by their base name (page# stripped)
        for i in range(1, attr_section_idx):
            if is_page_hint_col(names[i]):
                page_hints[_norm(page_hint_base_name(names[i]))] = i

        source_region_started = False
        for i in range(1, attr_section_idx):
            name = names[i]
            if is_page_hint_col(name):
                continue  # handled via page_hints, not its own column
            if is_html_col(name):
                source_region_started = True
                html_idx.append(i)
            elif is_doc_col(name):
                source_region_started = True
                page_idx = page_hints.get(_norm(name))
                doc_idx.append((i, page_idx))
            elif not source_region_started:
                product_info_idx.append(i)
            else:
                # Unrecognized column inside the source region (e.g. a URL-bearing
                # column whose name doesn't say "URL"): treat as a doc column: if
                # the cell value isn't actually a URL it will simply contribute no
                # sources for that row (parsed and skipped downstream).
                page_idx = page_hints.get(_norm(name))
                doc_idx.append((i, page_idx))

    return {
        "names": names,
        "part_no_idx": part_no_idx,
        "product_info_idx": product_info_idx,
        "html_idx": html_idx,
        "doc_idx": doc_idx,
        "attr_section_idx": attr_section_idx,
        "attr_label_idx": attr_label_idx,
        "attr_value_idx": attr_value_idx,
        "attr_uom_idx": attr_uom_idx,
        "attr_flag_idx": attr_flag_idx,
        "status_idx": status_idx,
        "audit_flag_idx": n,  # append new column right after the last existing one
    }
