"""Regression tests for pipeline/fetch.py's cache-reuse policy.

A cached FAILURE used to be permanent: fetch_and_cache_html/doc returned any
existing index entry regardless of status, and the "todo" list skipped every
URL already present. So the first time a URL failed - which happens in bulk
when the Scrape.do plan runs out and every request comes back HTTP 401 - that
failure was frozen into the shared cache forever. Re-running the audit,
topping up the plan, or fixing the key changed nothing, and every attribute
citing that URL kept being reported as if the source had been read.
"""
from pipeline.fetch import FETCH_WORKERS, _is_retryable


def test_error_entries_are_retryable():
    assert _is_retryable({"status": "error", "error": "HTTP 401"}) is True


def test_successful_and_permanently_unextractable_entries_are_not_refetched():
    assert _is_retryable({"status": "ok", "text_path": "x.txt"}) is False
    # "unextractable" = not a text-bearing document at all; retrying can't help.
    assert _is_retryable({"status": "unextractable"}) is False


def test_missing_entry_is_not_treated_as_a_cached_failure():
    assert _is_retryable(None) is False
    assert _is_retryable({}) is False


def test_fetch_workers_within_entry_plan_concurrency_cap():
    """Scrape.do's entry plans allow 5 concurrent requests. This was 8, which
    produced a constant stream of HTTP 429s and read timeouts."""
    assert FETCH_WORKERS <= 5
