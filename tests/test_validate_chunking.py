"""A product's rows are validated in parallel slices, then merged back into the
single per-product result file the write-back phase expects."""
import json
import re
from concurrent.futures import ThreadPoolExecutor

import pipeline.validate as v


def _manifest(n_rows):
    return {
        "product": {"Name": "Relay"},
        "sources": {"html": {}, "docs": {}},
        "attributes": [
            {"row_idx": 100 + i, "attr_label": f"L{i}", "attr_value": "1", "attr_uom": None,
             "attr_flag": "MFR_HTML", "sources": {"html": [], "docs": []}}
            for i in range(n_rows)
        ],
    }


def _fake_gemini(calls, fail_on_row=None):
    def fake(prompt, key, temperature=0, **_):
        rows = [int(m) for m in re.findall(r'"row_idx": (\d+)', prompt)]
        calls.append(rows)
        if fail_on_row in rows:
            raise RuntimeError("HTTP 429: quota")
        return {"results": [
            {"row_idx": r, "final_value": "1", "final_uom": None, "flag_text": "Value Correct",
             "uom_recorded": None, "uom_source": None, "reasoning": "ok"} for r in rows]}
    return fake


def _run(tmp_path, n_rows, monkeypatch, fail_on_row=None):
    mpath = tmp_path / "P1.json"
    mpath.write_text(json.dumps(_manifest(n_rows)))
    results = tmp_path / "results"
    results.mkdir()
    calls = []
    monkeypatch.setattr(v, "call_gemini", _fake_gemini(calls, fail_on_row))
    monkeypatch.setattr(v.time, "sleep", lambda s: None)
    with ThreadPoolExecutor(4) as pool:
        v._process_one(str(mpath), str(results), pool, "key", str(tmp_path), lambda m: None)
    return json.loads((results / "P1.json").read_text()), calls


def test_row_slices_are_even_and_bounded():
    sizes = lambda n: [len(s) for s in v._row_slices(list(range(n)))]
    assert sizes(0) == []
    assert sizes(5) == [5]
    assert sizes(20) == [20]
    assert sizes(21) == [11, 10]
    assert sizes(52) == [18, 18, 16]
    assert all(s <= v.ROWS_PER_CALL for s in sizes(1000))


def test_slices_are_merged_back_in_row_order(tmp_path, monkeypatch):
    result, calls = _run(tmp_path, 45, monkeypatch)
    assert len(calls) == 3  # 45 rows -> three calls of 15
    assert [it["row_idx"] for it in result["results"]] == list(range(100, 145))
    assert all("uom_comment" in it for it in result["results"])


def test_small_product_is_still_one_call(tmp_path, monkeypatch):
    result, calls = _run(tmp_path, 7, monkeypatch)
    assert len(calls) == 1
    assert len(result["results"]) == 7


def test_one_failed_slice_marks_the_whole_product_as_error(tmp_path, monkeypatch):
    """Same contract as before slicing: a product is either fully validated or
    flagged for manual review - never silently half-done."""
    result, _ = _run(tmp_path, 45, monkeypatch, fail_on_row=130)
    assert "error" in result and "results" not in result


def test_gemini_key_is_not_sent_in_the_url(monkeypatch):
    seen = {}

    class R:
        status_code = 200
        def json(self):
            return {"candidates": [{"content": {"parts": [{"text": '{"results": []}'}]}}]}

    def fake_post(url, **kw):
        seen["url"], seen["headers"] = url, kw.get("headers")
        return R()

    monkeypatch.setattr(v.requests, "post", fake_post)
    v.call_gemini("p", "SECRET-KEY")
    assert "SECRET-KEY" not in seen["url"]
    assert seen["headers"]["x-goog-api-key"] == "SECRET-KEY"


def test_network_error_text_does_not_leak_the_key(monkeypatch):
    def boom(url, **kw):
        raise RuntimeError("Max retries exceeded with url: /x?key=SECRET-KEY")

    monkeypatch.setattr(v.requests, "post", boom)
    monkeypatch.setattr(v.time, "sleep", lambda s: None)
    try:
        v.call_gemini("p", "SECRET-KEY", retries=2)
    except RuntimeError as e:
        assert "SECRET-KEY" not in str(e)
    else:
        raise AssertionError("expected RuntimeError")
