"""
Validate phase: independently verify every attribute row against its HTML/PDF
sources using Gemini, one product manifest at a time. Business rules and
prompt are unchanged from the original CLI pipeline (validated against two
real client datasets).
"""
import glob
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from pipeline.fetch import pdf_pages

VALIDATE_WORKERS = 4
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
                    "reasoning": {"type": "string"},
                },
                "required": ["row_idx", "final_value", "final_uom", "flag_text", "reasoning"],
            },
        }
    },
    "required": ["results"],
}

RULES_PROMPT = """You are independently verifying every product row using the HTML page and PDF as the PRIMARY sources - you are not trusting any pre-existing spreadsheet data blindly. For each attribute row you are given:
- attr_label: the attribute name (e.g. "Voltage", "Current").
- attr_value, attr_uom: the currently recorded Attribute Value and Attribute UOM for this attribute.
- attr_flag: which source type was previously cited as backing this value (MFR_HTML = the HTML product page, MFR_PDF = a PDF document, MFR_Image = an image, Legacy_Data/Existing_Data = pre-existing data not necessarily sourced from these docs, or blank/None = not sourced).

You are also given the extracted text of the HTML product page(s) and PDF document(s) associated with this product (labeled by source).

IMPORTANT WORDING RULE: Never use the word "office" or phrases like "Office listed" in flag_text or reasoning. Refer to the actual field by name instead, e.g. "Attribute Value" / "Attribute UOM". Example: "Attribute Value recorded as 250V but HTML states 230V" (not "Office listed Voltage=250V but HTML states 230V").

For EACH attribute row, decide:
1. If the recorded Attribute Value/UOM is directly supported by the source text (exact or clearly equivalent match) -> it is CORRECT. Return final_value/final_uom as an EXACT, VERBATIM COPY of the original attr_value/attr_uom given to you (same text, same abbreviation style, same spelling) - do NOT rephrase, normalize, expand, or abbreviate it (e.g. do not change "Amp" to "A", or "Volt AC/DC" to "V AC/DC") even if the source phrases the unit differently. flag_text = "Correct". reasoning may briefly note where it was confirmed.
2. If the recorded Attribute Value/UOM CONTRADICTS what the source text says (the source explicitly states a DIFFERENT, SUBSTANTIVE value for that same attribute - a different number, a different range, a different named category/color/material) -> it is INCORRECT. Return final_value/final_uom = the CORRECTED value taken verbatim from the source text, and flag_text = a short description of the discrepancy naming the field, e.g. "Attribute Value recorded as 250V but HTML states 230V".
   CRITICAL - do NOT treat these as contradictions (they are case 1, CORRECT, verbatim echo, flag_text = "Correct"): mere notation/abbreviation/formatting differences that represent the SAME underlying value. Examples of things that are the SAME (not a contradiction): "Amp" vs "A"; "Volt AC" vs "VAC" vs "V AC"; "3 Phase" vs "3" vs "3PH"; "2 Changeover (SPDT)" vs "2 C/O (SPDT)" vs "2 CO"; "100000 Cycles" vs "100000 Operations"; different digit grouping/spacing/capitalization of the same number or word. Only flag a contradiction when the actual substantive value differs (e.g. a different number, a different named color, a different specific rating) - not when it's the same value written in different shorthand.
3. If attr_value is empty/missing AND the correct value IS explicitly present in the source text -> FILL IT. Return final_value/final_uom = the value found in the source. This is NOT a problem, but you MUST still record where the value came from so it can be verified: set flag_text to "Filled from HTML" if only the HTML page stated it, "Filled from PDF" if only a PDF document stated it, or "Filled from HTML and PDF" if both independently confirm it. Always name the source type this way for every auto-fill - never leave flag_text null for a fill.
4. If attr_value is empty/missing AND the value is NOT found in any available source -> leave final_value/final_uom = null, flag_text = null (leave both fields empty). Never guess or generate information that is not explicitly available.
4b. IMPORTANT: If attr_value/attr_uom already has a recorded value, and that specific value is simply NOT MENTIONED anywhere in the available source text (this is different from case 2 - there is no explicit contradicting value, the topic is just absent from what you were given) -> do NOT null it out. This is not necessarily wrong; our source excerpt may simply be incomplete. Return final_value = the ORIGINAL attr_value unchanged, final_uom = the ORIGINAL attr_uom unchanged, and flag_text = a short note such as "Could not verify Attribute Value against available sources" so a human can double check. Only use case 2 (correction) when the source gives an explicit, specific, conflicting value for that exact attribute.
5. If a value exists but none of the available sources for this row could be accessed/parsed at all (i.e. every relevant source for this row failed), you MUST still evaluate against whatever sources DID load; only if literally none loaded for this row's cited source type should flag_text carry an error, formatted EXACTLY as "HEAD NODE ERROR: <reason>" if the failure was an HTML page, or "PDF ERROR: <reason>" if it was a PDF. Use the error reason text given to you in the source list.
6. Never invent or infer values not explicitly present in the given source text. If uncertain, treat as case 4 (leave blank) rather than guessing.
7. flag_text must be exactly "Correct" whenever the row was already filled in and is verified correct (case 1). For an auto-fill (case 3), flag_text must instead be exactly one of "Filled from HTML", "Filled from PDF", or "Filled from HTML and PDF" (never "Correct" for a fill - the source must always be stated). Only use a different flag_text when there is an actual problem: a contradiction (case 2), an access error (case 5), or an unconfirmed pre-existing value (case 4b). For case 4 (left blank, nothing found), flag_text is null.

Respond with a JSON object matching the required schema, one entry per attribute row_idx you were given (do not skip any).
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


def call_gemini(prompt: str, gemini_key: str, retries: int = 3, temperature: float = 0) -> dict:
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{GEMINI_MODEL}:generateContent?key={gemini_key}")
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
        try:
            r = requests.post(url, json=body, timeout=120)
            if r.status_code == 200:
                data = r.json()
                text = data["candidates"][0]["content"]["parts"][0]["text"]
                return json.loads(text)
            last_err = f"HTTP {r.status_code}: {r.text[:300]}"
        except Exception as e:
            last_err = str(e)
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"Gemini call failed after {retries} attempts: {last_err}")


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
        for field in ("final_value", "final_uom", "flag_text"):
            v = it.get(field)
            if v and (len(str(v)) > 500 or "</body" in str(v) or "<html" in str(v)):
                return f"row {it.get('row_idx')} field '{field}' looks corrupted (length {len(str(v))})"
    return None


def _process_one(mpath, results_dir, gemini_key, cache_dir, log, extra_instructions=None):
    part = os.path.splitext(os.path.basename(mpath))[0]
    result_path = os.path.join(results_dir, f"{part}.json")
    if os.path.exists(result_path):
        return

    with open(mpath, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    texts, errors = gather_sources_text(manifest)
    prompt = build_prompt(manifest, texts, errors, cache_dir, extra_instructions)

    result = None
    last_err = None
    for attempt in range(3):
        # temperature=0 is deterministic - retrying identically after a bad
        # response would just reproduce it, so nudge temperature up on retry.
        temperature = 0 if attempt == 0 else 0.3 + 0.2 * attempt
        try:
            candidate = call_gemini(prompt, gemini_key, temperature=temperature)
        except Exception as e:
            last_err = str(e)
            continue
        problem = validate_result(candidate, manifest)
        if problem is None:
            result = candidate
            break
        last_err = f"response failed validation: {problem}"
        log(f"{part}: retry (attempt {attempt + 1}): {last_err}")

    if result is None:
        log(f"{part}: giving up after retries - {last_err}")
        result = {"error": last_err}

    with open(result_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=1)


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

    log(f"Validating {len(selected)} product manifests with {VALIDATE_WORKERS} workers"
        + (" (with user feedback applied)" if extra_instructions else ""))

    done = {"n": 0}
    with ThreadPoolExecutor(max_workers=VALIDATE_WORKERS) as ex:
        futures = [
            ex.submit(_process_one, mpath, results_dir, gemini_key, cache_dir, log, extra_instructions)
            for mpath in selected
        ]
        for fut in as_completed(futures):
            fut.result()
            done["n"] += 1
            if done["n"] % 10 == 0:
                log(f"Validated {done['n']}/{len(selected)}")

    log("Validate phase complete")
