"""
Fetch phase: HTML pages and PDF/doc attachments for every product row.

File-based cache (storage/cache/index.json + storage/cache/html|docs/*) -
same approach as the original CLI scripts, kept deliberately simple (no
database) since this app runs as a single process for one team/user.

Two fetch methods, selected once per run:
  - Scrape.do (if the user supplied a key): proxy-based HTTP fetch, handles
    anti-bot-protected sites. Logic unchanged from the original script.
  - Playwright (fallback, no key required): a real headless Chromium browser.
    Both paths funnel through the same extract_text_from_html() so
    validation/write-back never need to know which one ran.
"""
import asyncio
import json
import os
import subprocess
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import requests

if sys.platform == "win32":
    # Playwright's sync API needs asyncio subprocess support (to spawn the
    # Chromium process) in whatever thread it's started from. Windows' default
    # SelectorEventLoop doesn't support subprocesses at all, and a plain
    # ThreadPoolExecutor worker thread (which is what PlaywrightPool uses, and
    # what a FastAPI background thread runs inside) has no event loop of its
    # own to begin with - so without this, every single launch fails with
    # "NotImplementedError launching/using the browser" the moment a real
    # request runs inside uvicorn (a standalone script's main thread can mask
    # this, which is why it's easy to miss in isolated testing). Setting the
    # process-wide policy here, before any thread touches Playwright, makes
    # every new event loop default to ProactorEventLoop, which does support it.
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
from bs4 import BeautifulSoup

from pipeline.schema import detect_schema

FETCH_WORKERS = 8
PDF_PARSE_CONCURRENCY = 4
PLAYWRIGHT_CONCURRENCY = 5

PDF_WORKER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pdf_worker.py")

_pdf_parse_semaphore = threading.Semaphore(PDF_PARSE_CONCURRENCY)

# requests' own `timeout` only bounds gaps BETWEEN received bytes, not total
# request duration - a server trickling keep-alive data can hang well past
# the nominal timeout. Run network calls in a bounded side-pool and give up
# on a hard wall-clock deadline instead.
_net_executor = ThreadPoolExecutor(max_workers=64)


def extract_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text("\n", strip=True)


def url_hash(url: str) -> str:
    import hashlib
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:20]


def split_urls(value) -> list[str]:
    if not value:
        return []
    parts = [p.strip() for p in str(value).split("|")]
    out, seen = [], set()
    for p in parts:
        if p.startswith("http://") or p.startswith("https://"):
            if p not in seen:
                seen.add(p)
                out.append(p)
    return out


# ---------------------------------------------------------------- cache index

_index_lock = threading.Lock()


def _index_path(cache_dir: str) -> str:
    return os.path.join(cache_dir, "index.json")


def load_index(cache_dir: str) -> dict:
    path = _index_path(cache_dir)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"html": {}, "docs": {}}


def save_index(cache_dir: str, index: dict) -> None:
    path = _index_path(cache_dir)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(index, f)
    os.replace(tmp, path)


# -------------------------------------------------------------- PDF subprocess

def pdf_info(raw_path: str, timeout: int = 25) -> dict:
    try:
        with _pdf_parse_semaphore:
            r = subprocess.run(
                [sys.executable, PDF_WORKER, "info", raw_path],
                capture_output=True, text=True, timeout=timeout,
            )
        out = (r.stdout or "").strip().splitlines()
        if out:
            return json.loads(out[-1])
        return {"error": (r.stderr or "worker produced no output").strip()[:300]}
    except subprocess.TimeoutExpired:
        return {"error": f"PDF parsing timed out after {timeout}s (large/complex document)"}
    except Exception as e:
        return {"error": str(e)}


def pdf_pages(raw_path: str, lo: int, hi: int, timeout: int = 60) -> dict:
    try:
        with _pdf_parse_semaphore:
            r = subprocess.run(
                [sys.executable, PDF_WORKER, "pages", raw_path, str(lo), str(hi)],
                capture_output=True, text=True, timeout=timeout,
            )
        out = (r.stdout or "").strip().splitlines()
        if not out:
            return {"error": (r.stderr or "worker produced no output").strip()[:300]}
        return json.loads(out[-1])
    except subprocess.TimeoutExpired:
        return {"error": f"PDF page extraction timed out after {timeout}s"}
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------- Scrape.do

def _bounded_get(url, timeout, **kwargs):
    fut = _net_executor.submit(requests.get, url, timeout=timeout, **kwargs)
    return fut.result(timeout=timeout + 10)


def _scrape_do_url(url: str, token: str) -> str:
    return "https://api.scrape.do/?token=" + token + "&url=" + urllib.parse.quote(url, safe="")


@dataclass
class HtmlResult:
    status: str  # ok | error
    text: str | None = None
    error: str | None = None


@dataclass
class DocResult:
    status: str  # ok | error
    content: bytes | None = None
    error: str | None = None


def fetch_html_scrape_do(url: str, token: str, retries: int = 4, timeout: int = 30) -> HtmlResult:
    last_err = None
    api_url = _scrape_do_url(url, token)
    for attempt in range(retries + 1):
        try:
            r = _bounded_get(api_url, timeout=timeout)
            if r.status_code == 200 and len(r.text) > 200:
                return HtmlResult(status="ok", text=extract_text_from_html(r.text))
            last_err = f"HTTP {r.status_code}"
            if r.status_code == 429:
                time.sleep(6 * (attempt + 1))
                continue
        except Exception as e:
            last_err = str(e)
        time.sleep(1)
    return HtmlResult(status="error", error=last_err)


def fetch_doc_scrape_do(url: str, token: str, timeout: int = 30) -> DocResult:
    """Direct HTTP GET first; Scrape.do proxy only as a fallback (handles
    anti-bot-blocked hosts) - most documents succeed on the direct attempt."""
    last_err = None
    try:
        r = _bounded_get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200 and len(r.content) > 100:
            return DocResult(status="ok", content=r.content)
        last_err = f"HTTP {r.status_code}"
    except Exception as e:
        last_err = str(e)

    try:
        api_url = _scrape_do_url(url, token)
        r = _bounded_get(api_url, timeout=timeout * 2)
        if r.status_code == 200 and len(r.content) > 100:
            return DocResult(status="ok", content=r.content)
        last_err = f"scrape.do HTTP {r.status_code}"
    except Exception as e:
        last_err = str(e)

    return DocResult(status="error", error=last_err)


# ---------------------------------------------------------------- Playwright

_RECYCLE_EVERY = 200


class _ThreadState:
    def __init__(self):
        self.playwright_ctx = None
        self.browser = None
        self.context = None
        self.fetch_count = 0


class PlaywrightPool:
    """Playwright's sync API can't share a Browser across threads - each
    worker thread lazily owns its own browser+context (thread-local),
    recycled periodically to bound memory over a long run."""

    def __init__(self, size: int = PLAYWRIGHT_CONCURRENCY, headless: bool = True):
        self.size = size
        self.headless = headless
        self._executor = ThreadPoolExecutor(max_workers=size, thread_name_prefix="pw-fetch")
        self._local = threading.local()

    def _get_context(self):
        from playwright.sync_api import sync_playwright
        state = getattr(self._local, "state", None)
        if state is None or state.fetch_count >= _RECYCLE_EVERY:
            self._teardown_thread_state()
            pw = sync_playwright().start()
            browser = pw.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (compatible; AuditBot/1.0)", accept_downloads=True,
            )
            state = _ThreadState()
            state.playwright_ctx = pw
            state.browser = browser
            state.context = context
            self._local.state = state
        return state

    def _teardown_thread_state(self):
        state = getattr(self._local, "state", None)
        if state is not None:
            try:
                if state.context:
                    state.context.close()
                if state.browser:
                    state.browser.close()
                if state.playwright_ctx:
                    state.playwright_ctx.stop()
            except Exception:
                pass
            self._local.state = None

    def _fetch_html_worker(self, url: str) -> HtmlResult:
        from playwright.sync_api import TimeoutError as PWTimeout
        page = None
        try:
            # Getting/creating the browser context can itself fail (e.g. the
            # Chromium executable isn't installed, or the browser process
            # crashed) - that must fail only THIS fetch, never escape and
            # take down the whole run, so it's inside the same try/except as
            # the actual navigation.
            state = self._get_context()
            state.fetch_count += 1
            page = state.context.new_page()
            page.goto(url, timeout=30_000, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=5_000)
            except PWTimeout:
                pass  # many pages poll forever and never go idle - best-effort only
            html = page.content()
            return HtmlResult(status="ok", text=extract_text_from_html(html))
        except PWTimeout as e:
            return HtmlResult(status="error", error=f"playwright navigation timeout: {e}")
        except Exception as e:
            return HtmlResult(status="error", error=str(e) or f"{type(e).__name__} launching/using the browser")
        finally:
            if page is not None:
                page.close()

    def _fetch_doc_worker(self, url: str) -> DocResult:
        page = None
        listener_attached = False
        captured: dict = {}

        def on_response(response):
            if captured.get("body") is not None:
                return
            try:
                ct = response.headers.get("content-type", "")
            except Exception:
                ct = ""
            if "pdf" in ct or "octet-stream" in ct:
                try:
                    captured["body"] = response.body()
                except Exception:
                    pass

        try:
            # Same reasoning as _fetch_html_worker: acquiring the browser
            # context can itself fail and must not escape uncaught.
            state = self._get_context()
            state.fetch_count += 1
            page = state.context.new_page()
            page.on("response", on_response)
            listener_attached = True
            try:
                # A direct PDF/doc link resolves one of two ways in Chromium:
                # rendered inline (on_response above captures it via CDP), or
                # served as Content-Disposition: attachment, which Chromium
                # hands to its download manager instead - CDP can't read that
                # response's body, so expect_download() is the only way to
                # get the bytes in that case. goto() is documented to raise
                # ("Download is starting") when it happens; that's expected.
                with page.expect_download(timeout=3_000) as download_info:
                    try:
                        page.goto(url, timeout=30_000, wait_until="domcontentloaded")
                    except Exception:
                        pass
                download = download_info.value
                with open(download.path(), "rb") as f:
                    captured["body"] = f.read()
            except Exception:
                pass
            if captured.get("body"):
                return DocResult(status="ok", content=captured["body"])
            return DocResult(status="error", error="no PDF/octet-stream response captured")
        except Exception as e:
            return DocResult(status="error", error=str(e) or f"{type(e).__name__} launching/using the browser")
        finally:
            if page is not None:
                if listener_attached:
                    page.remove_listener("response", on_response)
                page.close()

    def fetch_html(self, url: str, hard_timeout: float = 40.0) -> HtmlResult:
        future = self._executor.submit(self._fetch_html_worker, url)
        try:
            return future.result(timeout=hard_timeout)
        except TimeoutError:
            return HtmlResult(status="error", error=f"playwright hard timeout after {hard_timeout}s")
        except Exception as e:
            # Defense in depth: _fetch_html_worker should always return a
            # Result rather than raise, but a crash here must still fail
            # only this one fetch, never the whole run.
            return HtmlResult(status="error", error=str(e) or type(e).__name__)

    def fetch_doc(self, url: str, hard_timeout: float = 40.0) -> DocResult:
        future = self._executor.submit(self._fetch_doc_worker, url)
        try:
            return future.result(timeout=hard_timeout)
        except TimeoutError:
            return DocResult(status="error", error=f"playwright hard timeout after {hard_timeout}s")
        except Exception as e:
            return DocResult(status="error", error=str(e) or type(e).__name__)

    def close(self):
        self._executor.shutdown(wait=False, cancel_futures=True)


# ---------------------------------------------------------------- orchestrator

def _cache_html_dir(cache_dir):
    d = os.path.join(cache_dir, "html")
    os.makedirs(d, exist_ok=True)
    return d


def _cache_docs_dir(cache_dir):
    d = os.path.join(cache_dir, "docs")
    os.makedirs(d, exist_ok=True)
    return d


def run_fetch_phase(
    workbook_path: str,
    sheet: str | None,
    product_filter,
    manifests_dir: str,
    cache_dir: str,
    scrape_do_key: str | None,
    log,
) -> None:
    """product_filter(product_key, ordinal) -> bool selects which products to
    fetch (e.g. first N for a pilot). log(message) reports progress."""
    import openpyxl

    os.makedirs(manifests_dir, exist_ok=True)
    playwright_pool = None if scrape_do_key else PlaywrightPool()

    def fetch_and_cache_html(url, index):
        h = url_hash(url)
        with _index_lock:
            existing = index["html"].get(h)
        if existing is not None:
            return existing

        if scrape_do_key:
            result = fetch_html_scrape_do(url, scrape_do_key)
            method = "scrape_do"
        else:
            result = playwright_pool.fetch_html(url)
            method = "playwright"

        if result.status == "ok":
            text_path = os.path.join(_cache_html_dir(cache_dir), h + ".txt")
            with open(text_path, "w", encoding="utf-8") as f:
                f.write(result.text)
            meta = {"url": url, "status": "ok", "error": None, "text_path": text_path, "method": method}
        else:
            meta = {"url": url, "status": "error", "error": result.error, "text_path": None, "method": method}

        with _index_lock:
            index["html"][h] = meta
        return meta

    def fetch_and_cache_doc(url, index):
        h = url_hash(url)
        with _index_lock:
            existing = index["docs"].get(h)
        if existing is not None:
            return existing

        if scrape_do_key:
            result = fetch_doc_scrape_do(url, scrape_do_key)
            method = "scrape_do"
        else:
            result = playwright_pool.fetch_doc(url)
            method = "playwright"

        if result.status != "ok" or not result.content:
            meta = {"url": url, "status": "error", "error": result.error, "text_path": None,
                    "raw_path": None, "num_pages": None, "method": method}
            with _index_lock:
                index["docs"][h] = meta
            return meta

        ext = os.path.splitext(urllib.parse.urlparse(url).path)[1].lower()
        raw_path = os.path.join(_cache_docs_dir(cache_dir), h + (ext if ext else ".bin"))
        with open(raw_path, "wb") as f:
            f.write(result.content)

        is_pdf = result.content[:4] == b"%PDF" or ext == ".pdf"
        if not is_pdf:
            meta = {"url": url, "status": "unextractable",
                     "error": f"Non-PDF content/extension ({ext or 'unknown'}), no text extraction possible",
                     "text_path": None, "raw_path": raw_path, "num_pages": None, "method": method}
        else:
            info = pdf_info(raw_path)
            if "error" in info:
                meta = {"url": url, "status": "error", "error": f"PDF parse error: {info['error']}",
                         "text_path": None, "raw_path": raw_path, "num_pages": None, "method": method}
            else:
                num_pages = info.get("num_pages")
                text_path = None
                if info.get("full_text") is not None:
                    text_path = os.path.join(_cache_docs_dir(cache_dir), h + ".txt")
                    with open(text_path, "w", encoding="utf-8") as f:
                        f.write(info["full_text"])
                status = "ok" if num_pages else "error"
                meta = {"url": url, "status": status, "error": None if num_pages else "PDF has 0 pages",
                         "text_path": text_path, "raw_path": raw_path, "num_pages": num_pages, "method": method}

        with _index_lock:
            index["docs"][h] = meta
        return meta

    try:
        wb = openpyxl.load_workbook(workbook_path, read_only=True, data_only=True)
        ws = wb[sheet] if sheet else wb.worksheets[0]

        header = list(next(ws.iter_rows(min_row=1, max_row=1, values_only=True)))
        schema = detect_schema(header)
        names = schema["names"]

        missing = [
            label for label, key in (
                ("Attribute Section", "attr_section_idx"), ("Attribute Label", "attr_label_idx"),
                ("Attribute Value", "attr_value_idx"), ("Attribute UOM", "attr_uom_idx"),
                ("Attribute Flag", "attr_flag_idx"),
            ) if schema[key] is None
        ]
        if missing:
            raise ValueError(
                f"Could not detect these required columns in sheet '{ws.title}': {', '.join(missing)}. "
                f"This file's column layout doesn't match what the pipeline expects (an Attribute "
                f"Section/Label/Value/UOM/Flag block) - detected columns were: {names}"
            )

        log(f"Detected columns: part_no='{names[schema['part_no_idx']]}', "
            f"{len(schema['html_idx'])} HTML source col(s), {len(schema['doc_idx'])} document col(s) "
            f"(fetch method: {'Scrape.do' if scrape_do_key else 'Playwright (no key supplied)'})")

        header_len = len(names)

        def _pad(row):
            # openpyxl's read-only mode (used here for memory efficiency on
            # large sheets) returns a row exactly as wide as that row's last
            # written cell - a row whose trailing attribute columns were
            # simply never touched comes back SHORTER than the header, and
            # indexing into it at a schema-derived column position beyond
            # its length raises "tuple index out of range". Real vendor
            # exports do this constantly, so every row is padded up to the
            # header's width before anything indexes into it.
            if len(row) < header_len:
                return row + (None,) * (header_len - len(row))
            return row

        seen_parts: list[str] = []
        rows_by_part: dict[str, list] = {}
        for row_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
            row = _pad(row)
            part = row[schema["part_no_idx"]]
            if part is None:
                continue
            part = str(part)
            if part not in rows_by_part and not product_filter(part, len(seen_parts)):
                continue
            if part not in rows_by_part:
                seen_parts.append(part)
                rows_by_part[part] = []
            rows_by_part[part].append((row_idx, row))

        total_rows = sum(len(v) for v in rows_by_part.values())
        log(f"Selected {len(seen_parts)} products, {total_rows} attribute rows")

        html_urls: set[str] = set()
        doc_urls: set[str] = set()
        for part in seen_parts:
            for _row_idx, row in rows_by_part[part]:
                for i in schema["html_idx"]:
                    html_urls.update(split_urls(row[i]))
                for i, _page_i in schema["doc_idx"]:
                    doc_urls.update(split_urls(row[i]))

        index = load_index(cache_dir)
        html_todo = [u for u in sorted(html_urls) if url_hash(u) not in index["html"]]
        doc_todo = [u for u in sorted(doc_urls) if url_hash(u) not in index["docs"]]
        log(f"{len(html_urls)} unique HTML URLs ({len(html_todo)} new), "
            f"{len(doc_urls)} unique document URLs ({len(doc_todo)} new)")

        done = {"n": 0}
        total_todo = len(html_todo) + len(doc_todo)

        def _progress_tick():
            done["n"] += 1
            if total_todo and done["n"] % 25 == 0:
                log(f"Fetched {done['n']}/{total_todo}")

        def _html_task(url):
            try:
                meta = fetch_and_cache_html(url, index)
                if meta["status"] == "error":
                    log(f"HTML error: {url[:100]} -> {meta['error']}")
            except Exception as e:
                # One unexpected failure (e.g. a browser crash) must not
                # abort the whole batch - record it against this URL only.
                log(f"HTML error: {url[:100]} -> unexpected error: {e}")
                with _index_lock:
                    index["html"][url_hash(url)] = {
                        "url": url, "status": "error", "error": f"unexpected error: {e}", "text_path": None,
                    }
            _progress_tick()

        def _doc_task(url):
            try:
                meta = fetch_and_cache_doc(url, index)
                if meta["status"] == "error":
                    log(f"Doc error: {url[:100]} -> {meta['error']}")
            except Exception as e:
                log(f"Doc error: {url[:100]} -> unexpected error: {e}")
                with _index_lock:
                    index["docs"][url_hash(url)] = {
                        "url": url, "status": "error", "error": f"unexpected error: {e}",
                        "text_path": None, "raw_path": None, "num_pages": None,
                    }
            _progress_tick()

        with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
            futures = [ex.submit(_html_task, u) for u in html_todo]
            for fut in as_completed(futures):
                fut.result()
        save_index(cache_dir, index)

        with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
            futures = [ex.submit(_doc_task, u) for u in doc_todo]
            for fut in as_completed(futures):
                fut.result()
        save_index(cache_dir, index)

        for part in seen_parts:
            rows = rows_by_part[part]
            first_row = rows[0][1]
            product_info = {names[i]: first_row[i] for i in schema["product_info_idx"]}
            sources = {"html": {}, "docs": {}}
            attrs = []
            for row_idx, row in rows:
                row_sources = {"html": [], "docs": []}
                for i in schema["html_idx"]:
                    for u in split_urls(row[i]):
                        h = url_hash(u)
                        row_sources["html"].append(h)
                        sources["html"][h] = index["html"].get(h, {"url": u, "status": "error", "error": "not fetched"})
                for i, page_i in schema["doc_idx"]:
                    col_name = names[i]
                    page = row[page_i] if page_i is not None else None
                    for u in split_urls(row[i]):
                        h = url_hash(u)
                        row_sources["docs"].append({"hash": h, "col": col_name, "page": page})
                        sources["docs"][h] = index["docs"].get(h, {"url": u, "status": "error", "error": "not fetched"})
                attrs.append({
                    "row_idx": row_idx,
                    "attr_section": row[schema["attr_section_idx"]],
                    "attr_label": row[schema["attr_label_idx"]],
                    "attr_value": row[schema["attr_value_idx"]],
                    "attr_uom": row[schema["attr_uom_idx"]],
                    "attr_flag": row[schema["attr_flag_idx"]],
                    "status": row[schema["status_idx"]] if schema["status_idx"] is not None else None,
                    # Optional enrichment columns (Deeptech template) - None
                    # on templates that don't have them, which validate.py
                    # treats as "no extra context available" and skips.
                    "definition": row[schema["definition_idx"]] if schema["definition_idx"] is not None else None,
                    "allowed_values": row[schema["allowed_values_idx"]] if schema["allowed_values_idx"] is not None else None,
                    "data_type": row[schema["data_type_idx"]] if schema["data_type_idx"] is not None else None,
                    "raw_value": row[schema["raw_value_idx"]] if schema["raw_value_idx"] is not None else None,
                    "sources": row_sources,
                })
            manifest = {"product": product_info, "sources": sources, "attributes": attrs}
            safe_name = part.replace("/", "_").replace("\\", "_")
            manifest_path = os.path.join(manifests_dir, f"{safe_name}.json")
            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=1)

        log(f"Wrote {len(seen_parts)} product manifests")
    finally:
        if playwright_pool is not None:
            playwright_pool.close()
