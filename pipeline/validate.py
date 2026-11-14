"""
Validate phase: independently verify every attribute row against its HTML/PDF
sources using Gemini, one product manifest at a time. Business rules and
prompt are unchanged from the original CLI pipeline (validated against two
real client datasets).
"""
import glob
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from pipeline.fetch import pdf_pages

# Concurrent Gemini calls. Override with the VALIDATE_WORKERS env var (lower it
# if the log shows HTTP 429s - that means the key's rate limit is the ceiling).
VALIDATE_WORKERS = int(os.environ.get("VALIDATE_WORKERS") or 6)
# A product's attribute rows are sent to Gemini in slices of about this many
# rows, in parallel, instead of one call per product. Generating ~50 rows of
# JSON is what takes the time, so a few products with many rows used to leave
# most workers idle while one call ground through every row in sequence.
ROWS_PER_CALL = 20
MAX_CHARS_PER_SOURCE = 15000

GEMINI_MODEL = "gemini-2.5-flash"

RESULT_SCHEMA = {
    "type": "object",
    "properties": {
        "results": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "row_idx": {"type": "integer"},
                    "final_value": {"type": "string", "nullable": True},
                    "final_uom": {"type": "string", "nullable": True},
                    "flag_text": {"type": "string", "nullable": True},
                    "uom_recorded": {"type": "string", "nullable": True},
                    "uom_source": {"type": "string", "nullable": True},
                    "reasoning": {"type": "string"},
                },
                "required": ["row_idx", "final_value", "final_uom", "flag_text", "uom_recorded", "uom_source",
                             "reasoning"],
            },
        }
    },
    "required": ["results"],
}

RULES_PROMPT = """You are independently verifying every product row using the HTML page and PDF as the PRIMARY sources - you are not trusting any pre-existing spreadsheet data blindly. For each attribute row you are given:
- attr_label: the attribute name (e.g. "Voltage", "Current").
- attr_value, attr_uom: the currently recorded Attribute Value and Attribute UOM for this attribute.
- attr_flag: which source type was previously cited as backing this value (MFR_HTML = the HTML product page, MFR_PDF = a PDF document, MFR_Image = an image, Legacy_Data/Existing_Data = pre-existing data not necessarily sourced from these docs, or blank/None = not sourced).
- definition, allowed_values, data_type, raw_value: present on some templates only (null when not available) - see the ADDITIONAL CONTEXT and UOM VALIDATION sections below for how to use them.

You are also given the extracted text of the HTML product page(s) and PDF document(s) associated with this product (labeled by source).

IMPORTANT WORDING RULE: Never use the word "office" or phrases like "Office listed" in flag_text or reasoning. Refer to the actual field by name instead, e.g. "Attribute Value" / "Attribute UOM". Example: "Attribute Value recorded as 250V but HTML states 230V" (not "Office listed Voltage=250V but HTML states 230V").

For EACH attribute row, decide:
1. If the recorded Attribute Value/UOM is directly supported by the source text (exact or clearly equivalent match) -> it is CORRECT. Return final_value/final_uom as an EXACT, VERBATIM COPY of the original attr_value/attr_uom given to you (same text, same abbreviation style, same spelling) - do NOT rephrase, normalize, expand, or abbreviate it (e.g. do not change "Amp" to "A", or "Volt AC/DC" to "V AC/DC") even if the source phrases the unit differently. flag_text = "Value Correct". reasoning may briefly note where it was confirmed.
2. If the recorded Attribute Value/UOM CONTRADICTS what the source text says (the source explicitly states a DIFFERENT, SUBSTANTIVE value for that same attribute - a different number, a different range, a different named category/color/material) -> it is INCORRECT. Return final_value/final_uom = the CORRECTED value taken verbatim from the source text, and flag_text = a short description of the discrepancy naming the field, e.g. "Attribute Value recorded as 250V but HTML states 230V".
   CRITICAL - do NOT treat these as contradictions (they are case 1, CORRECT, verbatim echo, flag_text = "Value Correct"): mere notation/abbreviation/formatting differences that represent the SAME underlying value. Examples of things that are the SAME (not a contradiction): "Amp" vs "A"; "Volt AC" vs "VAC" vs "V AC"; "3 Phase" vs "3" vs "3PH"; "2 Changeover (SPDT)" vs "2 C/O (SPDT)" vs "2 CO"; "100000 Cycles" vs "100000 Operations"; different digit grouping/spacing/capitalization of the same number or word. Only flag a contradiction when the actual substantive value differs (e.g. a different number, a different named color, a different specific rating) - not when it's the same value written in different shorthand.
   This also covers unit-conversion equivalence: if the recorded number, converted using the standard conversion factor between its own unit and a different unit the source states, equals the source's number for that same measurement (e.g. 3.5 in = 88.9 mm), the underlying physical value is the SAME, not a contradiction - treat it as case 1: final_value/final_uom = verbatim echo of the original, flag_text = exactly "Value Correct" (do NOT describe the unit discrepancy in flag_text - that is the separate UOM VALIDATION section's job below, never this one's). Only treat it as a genuine case-2 contradiction when no such unit conversion reconciles the two numbers.
3. If attr_value is empty/missing AND the correct value IS explicitly present in the source text -> FILL IT. Return final_value/final_uom = the value found in the source. This is NOT a problem, but you MUST still record where the value came from so it can be verified: set flag_text to "Filled from HTML" if only the HTML page stated it, "Filled from PDF" if only a PDF document stated it, or "Filled from HTML and PDF" if both independently confirm it. Always name the source type this way for every auto-fill - never leave flag_text null for a fill.
4. If attr_value is empty/missing AND the value is NOT found in any available source -> leave final_value/final_uom = null, and set flag_text = "No Data Found in Source". Never guess or generate information that is not explicitly available. CRITICAL: this wording is a factual finding - it asserts that the sources WERE read and simply do not state this attribute. It therefore only applies when at least one source for this row actually loaded. If every source for this row is marked UNAVAILABLE, you learned nothing about this attribute and MUST use case 5 instead, even though attr_value is empty.
4b. IMPORTANT: If attr_value/attr_uom already has a recorded value, and that specific value is simply NOT MENTIONED anywhere in the available source text (this is different from case 2 - there is no explicit contradicting value, the topic is just absent from what you were given) -> do NOT null it out. This is not necessarily wrong; our source excerpt may simply be incomplete. Return final_value = the ORIGINAL attr_value unchanged, final_uom = the ORIGINAL attr_uom unchanged, and flag_text = a short note such as "Could not verify Attribute Value against available sources" so a human can double check. Only use case 2 (correction) when the source gives an explicit, specific, conflicting value for that exact attribute.
5. If none of the available sources for this row could be accessed/parsed at all (i.e. every relevant source for this row is marked UNAVAILABLE) -> this applies WHETHER OR NOT attr_value is empty. You MUST still evaluate against whatever sources DID load; only if literally none loaded should flag_text carry an error, formatted EXACTLY as "HEAD NODE ERROR: <reason>" if the failure was an HTML page, or "PDF ERROR: <reason>" if it was a PDF. Use the error reason text given to you in the source list. Leave final_value/final_uom as the ORIGINAL values (or null if there were none) - never null out or invent a value on the basis of a source you could not read.
6. Never invent or infer values not explicitly present in the given source text. If uncertain, treat as case 4 (leave blank, flag_text = "No Data Found in Source") rather than guessing.
7. flag_text must be exactly "Value Correct" whenever the row was already filled in and is verified correct (case 1). For an auto-fill (case 3), flag_text must instead be exactly one of "Filled from HTML", "Filled from PDF", or "Filled from HTML and PDF" (never "Value Correct" for a fill - the source must always be stated). Only use a different flag_text when there is an actual problem: a contradiction (case 2), an access error (case 5), or an unconfirmed pre-existing value (case 4b). For case 4 (nothing found anywhere), flag_text is exactly "No Data Found in Source".

=== ADDITIONAL CONTEXT (only when definition/allowed_values/data_type are not null - CONTEXT ONLY, does not change cases 1-7 above) ===
Some templates also provide, per attribute:
- definition: what this attribute is intended to represent.
- allowed_values: a documented list of valid options for this attribute, when it has a fixed/controlled vocabulary (e.g. "1/8 in NPT; 3/16 Hose Barb; 1/4 in NPT; 3/8 in Hose Barb").
- data_type: "number" or "text", the expected kind of value.
Use definition/data_type ONLY to understand an ambiguous attr_label - they must NOT make you more willing to flag something as incorrect, uncertain, or worth commenting on than you would be under cases 1-7 alone. If they're null or don't clearly resolve something, proceed exactly as if this part of the section did not exist.

allowed_values is different: when present, it is a hard schema-conformance requirement for that attribute, checked BEFORE cases 1-7:
- If the recorded attr_value/attr_uom together correspond to one of the listed options (exactly, or via a clear equivalent/synonym/notation variant per the case 2 CRITICAL note) -> proceed to cases 1-7 as normal (source-check it like any other attribute).
- If they do NOT correspond to any listed option, and no source explicitly confirms one of the LISTED OPTIONS ITSELF as the correct value -> leave final_value/final_uom UNCHANGED (do not guess which option was intended) and set flag_text = exactly "Wrong Value based on the Schema Definition and allowed value". This is the default whenever the recorded value doesn't match the list - a source saying something else true and specific ABOUT the attribute, that is still not itself one of the listed options, does NOT count as confirming an option; it still falls in this bullet.
- Only if a source explicitly states a value matching one of the allowed_values options THEMSELVES (word-for-word or a clear equivalent/synonym) -> follow case 2 instead (final_value/final_uom = that confirmed option, flag_text = the usual discrepancy sentence).
When allowed_values governs an attribute this way, never separately populate uom_recorded/uom_source for it below - this check already covers its correctness completely.

=== UOM VALIDATION (a SEPARATE, OPTIONAL annotation - must have ZERO effect on final_value, final_uom, or flag_text, which are decided purely by cases 1-7 and the allowed_values check above) ===
flag_text must NEVER mention a unit/UOM discrepancy, even when that's the only issue found for the row (e.g. even the unit-conversion case just above still gets flag_text = "Value Correct", nothing more) - a unit problem is reported ONLY through uom_recorded/uom_source below, never through flag_text.

This only ever applies to an attribute whose value is fundamentally a QUANTITY - a number, a numeric range, or a fraction that measures something (e.g. a current rating, a length, a voltage) - and only when allowed_values is null for it (an attribute governed by allowed_values is fully covered above and must never get uom_recorded/uom_source here). Never apply this to an attribute whose value is a name, category, color, material, or other non-numeric descriptor - those have no unit to check, no matter what words happen to appear nearby in raw_value.

When it applies, ALWAYS populate uom_recorded by checking BOTH places a unit could currently be captured - attr_uom, AND any unit embedded directly inside attr_value's own text (e.g. attr_value "600 V" embeds unit "V" even though attr_uom is null; attr_value "1/8 through 1/4 ID" embeds "ID" at the end even though it isn't a real unit - report it anyway, verbatim, and let the automatic check decide whether it's valid). Only leave uom_recorded null if genuinely no unit-like token appears in EITHER place. Report whatever is actually there verbatim, even if it looks wrong to you - never write a sentence, description, or vague phrase into it (e.g. never "no unit", "embedded in value") - only a short literal token, or null.

Separately, use raw_value as ground truth for what unit THIS SPECIFIC attribute should have, when available. raw_value is sometimes a longer passage describing the whole product (covering several attributes at once) - only use a unit from it when it unambiguously belongs to this attribute's own measurement (e.g. its number matches attr_value's number), never one that happens to describe a different attribute mentioned nearby in the same text. If raw_value is null, doesn't mention a unit, or its unit can't be clearly attributed to this attribute, leave uom_source null - that alone does not block uom_recorded above from being populated. Never use the HTML/PDF source text for uom_source; that text is not needed here and must be ignored for this specific check (it may be exactly what confirmed case 1/2 above, but does not feed uom_source).

Whether uom_recorded and uom_source actually differ (e.g. "A" and "Amp" are the same unit), and whether uom_recorded is even a real unit of measure at all when data_type is "number" (e.g. "ID"/"OD" are dimension qualifiers, not units, and are wrong for a number-type attribute regardless of raw_value), is all checked automatically after your response - just report what you literally find, verbatim, without judging validity or equivalence yourself.

Respond with a JSON object matching the required schema, one entry per attribute row_idx you were given (do not skip any). Keep "reasoning" to ONE short sentence (about 15 words or fewer) - it is only a debugging note, and long explanations just slow the response down.
"""

_page_cache_lock = threading.Lock()


def _load_page_cache(cache_dir):
    path = os.path.join(cache_dir, "pdf_page_cache.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_page_cache(cache_dir, cache):
    path = os.path.join(cache_dir, "pdf_page_cache.json")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f)
    os.replace(tmp, path)


def extract_pdf_page_range(cache_dir, doc_hash, raw_path, page_hint, num_pages, window=1, timeout=60):
    if not page_hint:
        return None
    try:
        page_hint = int(page_hint)
    except (TypeError, ValueError):
        return None
    lo = max(1, page_hint - window)
    hi = min(num_pages or page_hint + window, page_hint + window)

    with _page_cache_lock:
        cache = _load_page_cache(cache_dir)
    key = f"{doc_hash}:{lo}-{hi}"
    if key in cache:
        return cache[key]

    info = pdf_pages(raw_path, lo, hi, timeout=timeout)
    if "error" in info:
        result = f"(failed to extract pages {lo}-{hi}: {info['error']})"
    else:
        result = info.get("text", "")

    with _page_cache_lock:
        cache = _load_page_cache(cache_dir)
        cache[key] = result
        _save_page_cache(cache_dir, cache)
    return result


def gather_sources_text(manifest):
    texts = {}
    errors = {}

    for h, meta in manifest["sources"]["html"].items():
        label = f"HTML[{meta.get('url', '?')[:100]}]"
        if meta.get("status") == "ok" and meta.get("text_path") and os.path.exists(meta["text_path"]):
            with open(meta["text_path"], "r", encoding="utf-8") as f:
                t = f.read()
            texts[h] = (label, t[:MAX_CHARS_PER_SOURCE])
        else:
            err = meta.get("error", "unknown error")
            texts[h] = (label, None)
            errors[h] = f"HEAD NODE ERROR: {err}"

    for h, meta in manifest["sources"]["docs"].items():
        url = meta.get("url", "?")
        label = f"DOC[{url[:100]}]"
        status = meta.get("status")
        if status == "ok":
            if meta.get("text_path") and os.path.exists(meta["text_path"]):
                with open(meta["text_path"], "r", encoding="utf-8") as f:
                    t = f.read()
                texts[h] = (label, t[:MAX_CHARS_PER_SOURCE])
            else:
                texts[h] = (label, "__LARGE_PDF__")
        elif status == "unextractable":
            texts[h] = (label, None)
            errors[h] = f"PDF ERROR: {meta.get('error', 'not a text-extractable document')}"
        else:
            texts[h] = (label, None)
            errors[h] = f"PDF ERROR: {meta.get('error', 'unknown error')}"

    return texts, errors


def build_prompt(manifest, texts, errors, cache_dir, extra_instructions=None):
    product = manifest["product"]
    lines = [RULES_PROMPT]

    if extra_instructions:
        lines.append(
            "\n=== ADDITIONAL GUIDANCE FROM THE USER (from reviewing an earlier sample) ===\n"
            "The user reviewed a sample of results from an earlier pass and asked for these "
            "adjustments. Apply them as refinements within the rules above - they narrow HOW "
            "you apply the rules, they do not override the non-negotiable ones (never guess or "
            "invent a value, always cite verbatim from the source, never use the word \"office\").\n"
            + extra_instructions
        )

    lines += ["\n=== PRODUCT ===", json.dumps(product), "\n=== SOURCES ==="]

    for h, (label, text) in texts.items():
        if text is None:
            lines.append(f"\n--- {label} ---\n[UNAVAILABLE: {errors.get(h)}]")
        elif text == "__LARGE_PDF__":
            lines.append(f"\n--- {label} ---\n[LARGE MULTI-PRODUCT CATALOG — see per-row page excerpt below]")
        else:
            lines.append(f"\n--- {label} ---\n{text}")

    lines.append("\n=== ATTRIBUTE ROWS TO VALIDATE ===")
    for attr in manifest["attributes"]:
        row_view = {
            "row_idx": attr["row_idx"],
            "attr_label": attr["attr_label"],
            "attr_value": attr["attr_value"],
            "attr_uom": attr["attr_uom"],
            "attr_flag": attr["attr_flag"],
            # .get(), not [...]: absent on manifests built before this field
            # existed, and null on any template that doesn't have these columns.
            "definition": attr.get("definition"),
            "allowed_values": attr.get("allowed_values"),
            "data_type": attr.get("data_type"),
            "raw_value": attr.get("raw_value"),
        }
        extra_excerpts = []
        for d in attr["sources"]["docs"]:
            h = d["hash"]
            meta = manifest["sources"]["docs"].get(h, {})
            if meta.get("status") == "ok" and not meta.get("text_path") and meta.get("raw_path"):
                excerpt = extract_pdf_page_range(
                    cache_dir, h, meta["raw_path"], d.get("page"), meta.get("num_pages") or 1)
                if excerpt:
                    extra_excerpts.append(f"[{meta.get('url', '?')[:80]} p.{d.get('page')}]\n{excerpt[:6000]}")
        lines.append(json.dumps(row_view))
        for ex in extra_excerpts:
            lines.append(f"  PAGE EXCERPT: {ex}")

    return "\n".join(lines)


def call_gemini(prompt: str, gemini_key: str, retries: int = 4, temperature: float = 0) -> dict:
    # The key goes in a header, not the URL: requests puts the full URL into
    # its exception text, which ends up in the job log and the output file.
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{GEMINI_MODEL}:generateContent")
    headers = {"x-goog-api-key": gemini_key}
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": RESULT_SCHEMA,
            "temperature": temperature,
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }
    last_err = None
    for attempt in range(retries):
        delay = 3 * (attempt + 1)
        try:
            r = requests.post(url, json=body, headers=headers, timeout=120)
            if r.status_code == 200:
                data = r.json()
                text = data["candidates"][0]["content"]["parts"][0]["text"]
                return json.loads(text)
            last_err = f"HTTP {r.status_code}: {r.text[:300]}"
            if r.status_code in (429, 500, 502, 503, 504):
                delay = 8 * 2 ** attempt  # rate-limited/overloaded: back off properly (8s, 16s, 32s...)
            elif r.status_code in (400, 401, 403, 404):
                break  # bad key / bad request - retrying can't change the answer
        except Exception as e:
            last_err = str(e).replace(gemini_key, "***")
        if attempt < retries - 1:
            time.sleep(delay)
    raise RuntimeError(f"Gemini call failed after {attempt + 1} attempt(s): {last_err}")


def validate_result(result, manifest):
    """Structural sanity-check on a parsed Gemini response - catches
    duplicated/garbled/incomplete responses before they're trusted."""
    expected = [a["row_idx"] for a in manifest["attributes"]]
    items = result.get("results")
    if not isinstance(items, list):
        return "no 'results' list in response"
    got_rows = [it.get("row_idx") for it in items]
    if len(got_rows) != len(set(got_rows)):
        return f"duplicate row_idx in response ({len(got_rows)} entries, {len(set(got_rows))} unique)"
    if set(got_rows) != set(expected):
        missing = set(expected) - set(got_rows)
        extra = set(got_rows) - set(expected)
        return f"row_idx mismatch: missing={missing} extra={extra}"
    for it in items:
        for field in ("final_value", "final_uom", "flag_text", "uom_recorded", "uom_source"):
            v = it.get(field)
            if v and (len(str(v)) > 500 or "</body" in str(v) or "<html" in str(v)):
                return f"row {it.get('row_idx')} field '{field}' looks corrupted (length {len(str(v))})"
    return None


# Canonical unit spellings, keyed by lowercased/de-pluralized form - lets
# _uom_equivalent() catch same-unit variants (e.g. "A" vs "Amp", "mm" vs
# "millimeter") deterministically rather than trusting Gemini to reliably
# self-apply the same notation-equivalence judgment it's asked to apply
# elsewhere (in practice it doesn't: it reported "A" vs "Amp" and even "mm"
# vs "mm" as mismatches, so this is a hard backstop, not just prompt wording).
_UOM_CANON = {
    "a": "a", "amp": "a", "amps": "a", "ampere": "a", "amperes": "a",
    "v": "v", "volt": "v", "volts": "v",
    "w": "w", "watt": "w", "watts": "w",
    "hz": "hz", "hertz": "hz",
    "in": "in", "inch": "in", "inches": "in",
    "ft": "ft", "foot": "ft", "feet": "ft",
    "mm": "mm", "millimeter": "mm", "millimeters": "mm", "millimetre": "mm", "millimetres": "mm",
    "cm": "cm", "centimeter": "cm", "centimeters": "cm", "centimetre": "cm", "centimetres": "cm",
    "m": "m", "meter": "m", "meters": "m", "metre": "m", "metres": "m",
    "lb": "lb", "lbs": "lb", "pound": "lb", "pounds": "lb",
    "kg": "kg", "kilogram": "kg", "kilograms": "kg",
    "g": "g", "gram": "g", "grams": "g",
    "c": "c", "celsius": "c",
    "f": "f", "fahrenheit": "f",
    "psi": "psi",
    "sec": "sec", "second": "sec", "seconds": "sec",
    "min": "min", "minute": "min", "minutes": "min",
    "hr": "hr", "hour": "hr", "hours": "hr", "hrs": "hr",
    "day": "day", "days": "day",
    "yr": "yr", "year": "yr", "years": "yr",
    "mil": "mil", "mils": "mil",
    "deg": "deg", "degree": "deg", "degrees": "deg",
    "oz": "oz", "ounce": "oz", "ounces": "oz",
    "gal": "gal", "gallon": "gal", "gallons": "gal",
    "qt": "qt", "quart": "qt", "quarts": "qt",
    "l": "l", "liter": "l", "liters": "l", "litre": "l", "litres": "l",
    "ml": "ml", "milliliter": "ml", "milliliters": "ml",
    "rpm": "rpm", "cfm": "cfm", "gpm": "gpm", "psf": "psf",
    "db": "db", "lm": "lm", "lumen": "lm", "lumens": "lm",
    "btu": "btu", "kw": "kw", "kva": "kva", "va": "va", "mah": "mah", "ah": "ah",
    "vac": "v", "vdc": "v", "kg": "kg",
    "cu": "cu", "cubic": "cu", "sq": "sq", "square": "sq", "lin": "lin", "linear": "lin",
}

# Every canonical spelling a unit can normalize to - used as a whitelist when
# deciding whether an Allowed Values list is a list of MEASUREMENTS at all.
# Deliberately a whitelist: "1/8 in NPT" and "3/16 Hose Barb" both end in a
# short word that a shape-based regex would happily accept as a unit, but
# they are connection types, not measurements.
_UOM_CANON_VALUES = set(_UOM_CANON.values())

# Multiplicative prefixes that legitimately make a unit two words ("cu ft",
# "square meter"). Used to tell a real compound unit apart from a controlled
# vocabulary entry that merely starts with a unit ("in NPT", "Hose Barb").
_DIM_PREFIXES = {"cu", "cubic", "sq", "square", "lin", "linear"}

# A bare unit-ish token: a short word, optionally with a digit exponent or a
# "per" denominator ("g/cc", "m3", "ft/min"). Deliberately shape-based rather
# than a fixed list, so unfamiliar-but-real units still qualify.
_UNIT_WORD_RE = re.compile(r"^[a-zµ°%]{1,8}[0-9]?(?:\s*/\s*[a-z0-9]{1,8})?$")

# How a Definition typically names the unit an attribute should be recorded in.
_DEF_UNIT_RE = re.compile(
    r"(?:measured in|in units of|expressed in|reported in|recorded in|stated in)\s+([a-z.\s/]{1,24})",
    re.IGNORECASE,
)

# Any of these appearing in what's supposed to be a bare unit word means
# Gemini ignored the "single concrete unit word" instruction and wrote a
# description instead (e.g. "no unit", "embedded in value") - never usable
# as one side of a mismatch.
_UOM_INVALID_MARKERS = ("no unit", "not ", "unclear", "embed", "unknown", "n/a", "none", "null", " for ")

# Short abbreviations that look like a unit but aren't one (e.g. "ID"/"OD" =
# inner/outer diameter, a dimension qualifier, not a unit of measure) -
# reported in testing as a false "UOM mismatch" against a real unit like "in".
_UOM_NONUNIT_WORDS = {"id", "od", "na", "tbd", "ea", "qty", "pcs", "no", "ref"}


def _looks_like_unit_token(s):
    if not s:
        return False
    s = str(s).strip()
    if not s or len(s) > 20 or len(s.split()) > 2:
        return False
    low = s.lower().rstrip(".")
    if low in _UOM_NONUNIT_WORDS:
        return False
    return not any(m in low for m in _UOM_INVALID_MARKERS)


def _normalize_uom_token(s):
    """Folds the cosmetic ways the same unit gets written - inch/foot marks,
    trailing periods ("Cu. Ft."), plurals ("mils", "hours"), case, and
    spacing - onto one canonical form."""
    if s is None:
        return ""
    s = str(s).strip().lower()
    # "°F" -> "f", but a bare "°" is itself the unit (degrees of arc).
    s = re.sub(r"°\s*(?=[a-z])", "", s)
    s = s.replace("°", "deg")
    # Inch/foot marks (including the curly variants real datasheets use).
    s = s.replace('"', "in").replace("”", "in").replace("″", "in")
    s = s.replace("'", "ft").replace("’", "ft").replace("′", "ft")
    s = s.replace(".", " ")
    s = re.sub(r"\s+", " ", s).strip()
    if s in _UOM_CANON:
        return _UOM_CANON[s]
    parts = [_UOM_CANON.get(p, _UOM_CANON.get(p[:-1], p) if p.endswith("s") else p) for p in s.split()]
    return " ".join(parts)


def _uom_equivalent(a, b):
    na, nb = _normalize_uom_token(a), _normalize_uom_token(b)
    if na == nb:
        return True
    # "cu ft" vs "cuft" vs "cu-ft" are the same unit written three ways.
    squash = lambda x: re.sub(r"[\s\-./]", "", x)
    return bool(na) and squash(na) == squash(nb)


def _is_unit_phrase(s):
    """True if `s` reads as an actual unit of measure ("in", "g/cc", "cu ft")
    rather than a vocabulary term that merely contains one ("in NPT")."""
    n = _normalize_uom_token(s)
    if not n or n in _UOM_NONUNIT_WORDS:
        return False
    if "/" in n and _UNIT_WORD_RE.match(n):
        return True  # compound rate unit, e.g. "g/cc", "ft/min"
    toks = n.split()
    if len(toks) == 1:
        return toks[0] in _UOM_CANON_VALUES
    if len(toks) == 2 and toks[0] in _DIM_PREFIXES:
        return toks[1] in _UOM_CANON_VALUES
    return False


def _matches_any_unit(recorded, schema_units):
    """A UOM cell covering a multi-part value repeats its unit once per part
    ("in; in" alongside a value of "1 1/2; 2 1/8") - correct as long as every
    part is one of the schema's units."""
    parts = [p for p in re.split(r"[;,]", str(recorded)) if p.strip()]
    if not parts:
        return False
    return all(any(_uom_equivalent(p, u) for u in schema_units) for p in parts)


def _split_allowed_options(allowed_values):
    """Allowed Values arrives in several real-world shapes: a JSON array
    ('[ "0.5 g/cc", "0.8 g/cc"]'), a semicolon list ("Round; Oval"), or a
    comma list. Returns the individual option strings."""
    if allowed_values is None:
        return []
    raw = str(allowed_values).strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(o).strip() for o in parsed if str(o).strip()]
    except (ValueError, TypeError):
        pass
    raw = raw.strip("[]")
    sep = ";" if ";" in raw else ","
    return [o.strip().strip('"').strip("'").strip() for o in raw.split(sep) if o.strip(' "\'')]


def units_from_allowed_values(allowed_values):
    """The set of units an Allowed Values list implies, or an empty set when
    it isn't a unit-bearing list at all.

    "[ '5 sec', '10 min', '24 hr' ]" -> {"sec", "min", "hr"} (any is valid),
    but "1/8 in NPT; 3/16 Hose Barb" -> {} because those options are named
    connection types, not measurements - every option must parse as
    "<amount> <unit>" before the list is trusted as unit evidence.
    """
    options = _split_allowed_options(allowed_values)
    if not options:
        return set()
    units = set()
    for opt in options:
        # Take whatever trails the last digit - handles "0.5 g/cc", "23.0 cu
        # ft", ranges like "1 to 2 3/4 in", and "-4°F" alike.
        last_digit = max((k for k, ch in enumerate(opt) if ch.isdigit()), default=None)
        if last_digit is None:
            return set()  # no quantity at all -> a vocabulary of names
        rest = opt[last_digit + 1:].strip()
        if not _is_unit_phrase(rest):
            return set()
        units.add(_normalize_uom_token(rest))
    return units


def units_from_definition(definition):
    """Units named outright by the Definition text, e.g. "...capacity of the
    oven in units of cu. ft." or "...measured in cubic feet"."""
    if not definition:
        return set()
    units = set()
    for m in _DEF_UNIT_RE.finditer(str(definition)):
        candidate = m.group(1).strip().rstrip(".").strip()
        # Trim trailing prose ("cubic feet from the floor" -> "cubic feet").
        words = candidate.split()
        for width in (2, 1):
            if len(words) >= width and _is_unit_phrase(" ".join(words[:width])):
                units.add(_normalize_uom_token(" ".join(words[:width])))
                break
    return units


def _resolve_uom_comment(item, data_type=None, allowed_values=None, definition=None, attr_value=None):
    """Decides the final uom_comment deterministically, from strongest
    evidence to weakest. Fixed vocabulary ("UOM Missing" / "UOM Wrong")
    matches the reference audit output this was modeled on.

    Evidence order matters. Allowed Values and Definition describe what unit
    the SCHEMA requires, so they are authoritative; Raw Value is just how one
    supplier happened to write it that day, and is routinely a typo
    ("g/c3" for "g/cc"), an abbreviation ("hours" vs the schema's "hr"), a
    symbol ('"' vs "in"), or a parenthetical conversion ("8\" (20 cm)").
    Judging against Raw Value alone is what produced the false "UOM Wrong"
    reports - every one of those units matched the schema exactly.
    """
    recorded = item.pop("uom_recorded", None)
    source = item.pop("uom_source", None)
    dtype = str(data_type or "").strip().lower()

    # An attribute with no value has nothing for a unit to qualify. Its Audit
    # Flag already says so ("No Data Found in Source"); adding "UOM Missing"
    # on top is duplicate noise on rows that are simply blank.
    if attr_value is None or not str(attr_value).strip():
        return None

    schema_units = units_from_allowed_values(allowed_values) or units_from_definition(definition)

    if schema_units:
        if recorded and _matches_any_unit(recorded, schema_units):
            return None  # matches what the schema asks for - authoritative
        return "UOM Missing" if not recorded else "UOM Wrong"

    # Allowed Values exists but names no units at all -> a controlled
    # vocabulary of terms (e.g. "Round; Oval; Square"). Such an attribute has
    # no unit to audit, whatever the free text happens to mention nearby.
    if _split_allowed_options(allowed_values):
        return None

    if dtype == "number" and recorded and not _looks_like_unit_token(recorded):
        return "UOM Wrong"
    if not source or not _looks_like_unit_token(source):
        return None  # no attributable unit evidence to check against
    if not recorded or not _looks_like_unit_token(recorded):
        return "UOM Missing"
    if not _uom_equivalent(recorded, source):
        return "UOM Wrong"
    return None


def _row_source_status(manifest, attr):
    """(any_source_loaded, error_flag_text) for one attribute row, read
    straight from the manifest's per-source fetch status."""
    srcs = (attr or {}).get("sources") or {}
    all_html = (manifest.get("sources") or {}).get("html") or {}
    all_docs = (manifest.get("sources") or {}).get("docs") or {}
    first_error = None
    for h in srcs.get("html") or []:
        meta = all_html.get(h) or {}
        if meta.get("status") == "ok":
            return True, None
        first_error = first_error or f"HEAD NODE ERROR: {meta.get('error', 'source unavailable')}"
    for d in srcs.get("docs") or []:
        meta = all_docs.get((d or {}).get("hash")) or {}
        if meta.get("status") == "ok":
            return True, None
        first_error = first_error or f"PDF ERROR: {meta.get('error', 'document unavailable')}"
    return False, first_error


def _fix_unreadable_source_flag(item, manifest, attr):
    """"No Data Found in Source" is a factual claim: the sources were read and
    do not mention this attribute. When every source for the row failed to
    fetch, that claim is simply false - nothing was read - and it hides an
    infrastructure problem (an exhausted API plan, a timeout) behind what
    looks like a clean audit result. The model is told this in case 4/5, but
    this backstop guarantees it, since the wording is what users act on."""
    if item.get("flag_text") != "No Data Found in Source":
        return
    any_loaded, error_flag = _row_source_status(manifest, attr)
    if not any_loaded and error_flag:
        item["flag_text"] = error_flag[:300]


def _dedupe_uom_from_flag_text(item, orig_value):
    """Backstop for a persistent model tendency: even when explicitly told
    flag_text must never describe a unit-only discrepancy, it sometimes does
    anyway for a row whose uom_comment already says "UOM Wrong" (reproduced
    in testing against the reference file's own unit-conversion example,
    e.g. 3.5 in == 88.9 mm). Only overrides when final_value didn't actually
    change from what was recorded - i.e. this really is a pure unit
    mislabeling (case 1), never a genuine case-2 value correction that
    happens to also mention "UOM"."""
    if item.get("uom_comment") != "UOM Wrong":
        return
    flag = (item.get("flag_text") or "").lower()
    def _norm(v):
        return str(v).strip() if v is not None else ""

    if "uom" in flag and _norm(item.get("final_value")) == _norm(orig_value):
        item["flag_text"] = "Value Correct"


def _call_with_retries(label, manifest, texts, errors, gemini_key, cache_dir, log, extra_instructions, verbose):
    """One structurally-valid Gemini response for `manifest` (a whole product,
    or a slice of its rows). Returns (result, None) or (None, reason)."""
    started = time.monotonic()
    try:
        prompt = build_prompt(manifest, texts, errors, cache_dir, extra_instructions)
    except Exception as e:
        log(f"{label}: could not build prompt: {e}")
        return None, f"could not build prompt: {e}"
    prompt_secs = time.monotonic() - started

    last_err = None
    for attempt in range(3):
        # temperature=0 is deterministic - retrying identically after a bad
        # response would just reproduce it, so nudge temperature up on retry.
        temperature = 0 if attempt == 0 else 0.3 + 0.2 * attempt
        try:
            candidate = call_gemini(prompt, gemini_key, temperature=temperature)
        except Exception as e:
            last_err = str(e)
            log(f"{label}: Gemini call failed (attempt {attempt + 1}/3): {last_err[:200]}")
            continue
        problem = validate_result(candidate, manifest)
        if problem is None:
            elapsed = time.monotonic() - started
            # Always report slow calls (that's the thing worth knowing); report
            # every call too on small runs so the first run explains itself.
            if verbose or elapsed >= 60:
                log(f"{label}: done in {elapsed:.0f}s (prompt {len(prompt) // 1000}k chars, "
                    f"{prompt_secs:.0f}s building it, attempt {attempt + 1})")
            return candidate, None
        last_err = f"response failed validation: {problem}"
        log(f"{label}: retry (attempt {attempt + 1}): {last_err}")

    log(f"{label}: giving up after retries - {last_err}")
    return None, last_err


def _row_slices(attributes):
    """Splits a product's rows into the fewest slices of <= ROWS_PER_CALL,
    sized evenly (52 rows -> 18+17+17, not 20+20+12)."""
    n = len(attributes)
    if n == 0:
        return []
    count = -(-n // ROWS_PER_CALL)
    size = -(-n // count)
    return [attributes[i:i + size] for i in range(0, n, size)]


def _process_one(mpath, results_dir, chunk_pool, gemini_key, cache_dir, log, extra_instructions=None,
                 verbose=True):
    part = os.path.splitext(os.path.basename(mpath))[0]
    result_path = os.path.join(results_dir, f"{part}.json")
    if os.path.exists(result_path):
        return

    with open(mpath, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    texts, errors = gather_sources_text(manifest)

    slices = _row_slices(manifest["attributes"])
    futures, first = [], 0
    for rows in slices:
        label = part if len(slices) == 1 else f"{part} rows {first + 1}-{first + len(rows)}/{len(manifest['attributes'])}"
        first += len(rows)
        futures.append(chunk_pool.submit(
            _call_with_retries, label, {**manifest, "attributes": rows}, texts, errors,
            gemini_key, cache_dir, log, extra_instructions, verbose))

    items, last_err = [], None
    for fut in futures:
        res, err = fut.result()
        if res is None:
            last_err = last_err or err
        else:
            items.extend(res.get("results", []))

    if last_err is not None:
        result = {"error": last_err}
    else:
        result = {"results": items}
        by_row = {a["row_idx"]: a for a in manifest["attributes"]}
        for it in items:
            attr = by_row.get(it.get("row_idx")) or {}
            it["uom_comment"] = _resolve_uom_comment(
                it, attr.get("data_type"), attr.get("allowed_values"), attr.get("definition"),
                attr.get("attr_value"))
            _fix_unreadable_source_flag(it, manifest, attr)
            _dedupe_uom_from_flag_text(it, attr.get("attr_value"))

    # Write-then-rename: an existing result file means "done, skip", so a
    # half-written one (crash mid-write) must never be visible.
    tmp = result_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=1)
    os.replace(tmp, result_path)


def run_validate_phase(
    manifests_dir: str, results_dir: str, product_filter, gemini_key: str, cache_dir: str, log,
    extra_instructions=None,
) -> None:
    os.makedirs(results_dir, exist_ok=True)
    manifest_paths = sorted(glob.glob(os.path.join(manifests_dir, "*.json")))
    selected = [
        mpath for i, mpath in enumerate(manifest_paths)
        if product_filter(os.path.splitext(os.path.basename(mpath))[0], i)
    ]

    log(f"Validating {len(selected)} product manifests, up to {VALIDATE_WORKERS} Gemini calls at once "
        f"({ROWS_PER_CALL} rows per call)" + (" (with user feedback applied)" if extra_instructions else ""))

    verbose = len(selected) <= 50  # per-call timing lines only when they won't flood the log
    started = time.monotonic()
    done = {"n": 0}
    # Two pools: product threads load/prepare a product and wait; chunk
    # threads do the actual Gemini calls. They're separate so a product
    # waiting on its slices can never occupy the thread a slice needs.
    with ThreadPoolExecutor(max_workers=VALIDATE_WORKERS) as chunk_pool, \
            ThreadPoolExecutor(max_workers=VALIDATE_WORKERS) as product_pool:
        futures = [
            product_pool.submit(_process_one, mpath, results_dir, chunk_pool, gemini_key, cache_dir, log,
                                extra_instructions, verbose)
            for mpath in selected
        ]
        for fut in as_completed(futures):
            fut.result()
            done["n"] += 1
            if verbose or done["n"] % 10 == 0:
                log(f"Validated {done['n']}/{len(selected)} products "
                    f"({time.monotonic() - started:.0f}s elapsed)")

    log(f"Validate phase complete in {time.monotonic() - started:.0f}s")
