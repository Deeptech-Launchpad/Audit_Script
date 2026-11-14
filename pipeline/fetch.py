"""
Fetch phase: HTML pages and PDF/doc attachments for every product row.

File-based cache (storage/cache/index.json + storage/cache/html|docs/*) -
same approach as the original CLI scripts, kept deliberately simple (no
database) since this app runs as a single process for one team/user.

Three fetch strategies, applied in priority order:
  1. Moat scraped-URL store  (no Scrape.do credits; needs MOAT_BASE_URL and
     MOAT_API_KEY env vars, sent as the X-API-Key header)
       GET /moat/scraped?url=<url>              → {scraped, type, ...}
       GET /moat/scraped/content?url=<url>      → {"text": ...}, 404 if not scraped
     Tried first for every HTML URL when configured.
  2. Scrape.do live proxy  (consumes credits, only if plan has remaining)
     proxy-based HTTP fetch, handles anti-bot-protected sites.
  3. Playwright fallback  (no key required, or when plan is exhausted)
     A real headless Chromium browser.
     All paths funnel through the same extract_text_from_html() so
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
from requests.adapters import HTTPAdapter

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

# Scrape.do enforces a per-plan concurrent-request cap (5 on the entry plans).
# This used to be 8, which guaranteed a steady stream of HTTP 429s and read
# timeouts no matter how healthy the account was. run_fetch_phase() lowers it
# further at runtime if the account reports a smaller cap. It is enforced with
# a semaphore around Scrape.do calls ONLY - it no longer throttles the whole
# fetch phase.
FETCH_WORKERS = 5
# Overall fetch concurrency. Most URLs never touch Scrape.do (Moat cache hits,
# direct document downloads), so they shouldn't queue behind its cap.
NETWORK_WORKERS = 16
PDF_PARSE_CONCURRENCY = max(4, min(8, os.cpu_count() or 4))
PLAYWRIGHT_CONCURRENCY = 5
# Scrape.do/target statuses worth retrying; any other 4xx is a permanent answer
# for this URL (e.g. a 404 product page) and retrying only burns minutes.
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}

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


def _is_retryable(meta: dict) -> bool:
    """A cached FAILURE must not be permanent. Fetch errors are usually
    transient or environmental - an expired/exhausted Scrape.do plan (HTTP
    401/429), a read timeout, a site that was briefly down. The cache used to
    return any existing entry regardless of status, so the very first failure
    for a URL was frozen in forever: re-running the audit, topping up the
    API plan, or fixing the key changed nothing, and every attribute citing
    that URL kept reporting as if the source had been read and come up empty.
    Successful and permanently-unextractable entries still short-circuit."""
    return (meta or {}).get("status") == "error"


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

SCRAPE_DO_API_BASE = "https://api.scrape.do"


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


_session_local = threading.local()


def _session() -> requests.Session:
    """One pooled Session per network thread - reuses TCP/TLS connections to
    the same host (Moat, Scrape.do, a vendor's CDN) instead of a fresh
    handshake on every request, which plain requests.get() does."""
    s = getattr(_session_local, "session", None)
    if s is None:
        s = requests.Session()
        adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        _session_local.session = s
    return s


def _bounded_get(url, timeout, **kwargs):
    fut = _net_executor.submit(lambda: _session().get(url, timeout=timeout, **kwargs))
    return fut.result(timeout=timeout + 10)


def _scrape_do_url(url: str, token: str) -> str:
    return f"{SCRAPE_DO_API_BASE}/?token={token}&url={urllib.parse.quote(url, safe='')}"


# ---- Scrape.do dedupe / moat cache helpers (zero-credit lookups) ----

def moat_config() -> tuple[str, str] | None:
    """(base_url, api_key) of the Moat scraped-URL store, from the MOAT_BASE_URL
    and MOAT_API_KEY environment variables. None if either is unset."""
    base = (os.environ.get("MOAT_BASE_URL") or "").strip().rstrip("/")
    key = (os.environ.get("MOAT_API_KEY") or "").strip()
    return (base, key) if base and key else None


def moat_is_scraped(url: str, timeout: int = 15) -> bool:
    """GET /moat/scraped?url=<url> -> {scraped: bool, ...}. Never raises; any
    error counts as "not scraped"."""
    cfg = moat_config()
    if not cfg:
        return False
    base, key = cfg
    try:
        r = _bounded_get(f"{base}/moat/scraped", timeout=timeout,
                         params={"url": url}, headers={"X-API-Key": key})
        return r.status_code == 200 and bool(r.json().get("scraped"))
    except Exception:
        return False


def moat_get_cached_text(url: str, timeout: int = 30) -> HtmlResult:
    """GET /moat/scraped/content?url=<url> -> {"text": ...} (404 if not scraped
    yet). Uses no Scrape.do credits."""
    cfg = moat_config()
    if not cfg:
        return HtmlResult(status="error", error="Moat not configured")
    base, key = cfg
    try:
        r = _bounded_get(f"{base}/moat/scraped/content", timeout=timeout,
                         params={"url": url}, headers={"X-API-Key": key})
        if r.status_code == 404:
            return HtmlResult(status="error", error="URL not in Moat store (404)")
        if r.status_code != 200:
            return HtmlResult(status="error", error=f"Moat HTTP {r.status_code}")
        try:
            payload = r.json()
            text = payload.get("text") if isinstance(payload, dict) else None
        except ValueError:
            text = r.text
        text = text or ""
        if text.lstrip().startswith("<"):
            text = extract_text_from_html(text)
        if len(text) > 50:
            return HtmlResult(status="ok", text=text)
        return HtmlResult(status="error", error="Moat content too short to be useful")
    except Exception as e:
        return HtmlResult(status="error", error=f"Moat fetch error: {e}")


def scrape_do_account(token: str, timeout: int = 20) -> dict | None:
    """Plan status: {IsActive, ConcurrentRequest, MaxMonthlyRequest,
    RemainingMonthlyRequest}. Returns None if it can't be determined."""
    try:
        r = requests.get(f"https://api.scrape.do/info?token={token}", timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def fetch_html_scrape_do(url: str, token: str, retries: int = 2, timeout: int = 60) -> HtmlResult:
    last_err = None
    api_url = _scrape_do_url(url, token)
    for attempt in range(retries + 1):
        try:
            r = _bounded_get(api_url, timeout=timeout)
            if r.status_code == 200 and len(r.text) > 200:
                return HtmlResult(status="ok", text=extract_text_from_html(r.text))
            last_err = f"HTTP {r.status_code}"
            # 401 here means the plan is exhausted/invalid, not a bad page -
            # retrying burns time without any chance of succeeding.
            if r.status_code == 401:
                return HtmlResult(status="error", error=f"HTTP 401 ({_scrape_do_reason(r)})")
            if r.status_code == 429:
                time.sleep(6 * (attempt + 1))
                continue
            if r.status_code != 200 and r.status_code not in _RETRYABLE_STATUS:
                # e.g. 404/403/410 from the target page - a permanent answer.
                return HtmlResult(status="error", error=last_err)
        except Exception as e:
            last_err = str(e)
            time.sleep(3 * (attempt + 1))  # back off on timeouts too, not just 429s
            continue
        time.sleep(1)
    return HtmlResult(status="error", error=last_err)


def _scrape_do_reason(response) -> str:
    try:
        msg = response.json().get("Message")
        if isinstance(msg, list):
            return "; ".join(str(m) for m in msg)[:160]
        if msg:
            return str(msg)[:160]
    except Exception:
        pass
    return "check the Scrape.do dashboard"


_BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


def _looks_like_document(response) -> bool:
    """A 200 that is really an HTML page (login wall, cookie interstitial,
    "file moved" landing page) must not be accepted as the document - it
    would be cached as permanently "unextractable" and never retried."""
    if response.content[:4] == b"%PDF":
        return True
    ct = (response.headers.get("content-type") or "").lower()
    return "text/html" not in ct


def fetch_doc_direct(url: str, timeout: int = 30) -> DocResult:
    """Plain HTTP download - no credits, no browser. Most datasheet/PDF links
    are static files on a CDN, so this alone succeeds for the majority."""
    try:
        r = _bounded_get(url, timeout=timeout, headers={"User-Agent": _BROWSER_UA})
        if r.status_code == 200 and len(r.content) > 100 and _looks_like_document(r):
            return DocResult(status="ok", content=r.content)
        if r.status_code == 200:
            return DocResult(status="error", error="direct download returned an HTML page, not a document")
        return DocResult(status="error", error=f"HTTP {r.status_code}")
    except Exception as e:
        return DocResult(status="error", error=str(e))


def fetch_doc_via_scrape_do(url: str, token: str, timeout: int = 60) -> DocResult:
    """Scrape.do proxy for documents on anti-bot-protected hosts."""
    try:
        r = _bounded_get(_scrape_do_url(url, token), timeout=timeout)
        if r.status_code == 200 and len(r.content) > 100:
            return DocResult(status="ok", content=r.content)
        if r.status_code == 401:
            return DocResult(status="error", error=f"scrape.do HTTP 401 ({_scrape_do_reason(r)})")
        return DocResult(status="error", error=f"scrape.do HTTP {r.status_code}")
    except Exception as e:
        return DocResult(status="error", error=str(e))


# ---------------------------------------------------------------- Playwright

_RECYCLE_EVERY = 200
# Only the page's HTML/text is used - never download images, video, or fonts.
_BLOCKED_RESOURCE_TYPES = {"image", "media", "font"}


def _block_heavy_resources(route):
    if route.request.resource_type in _BLOCKED_RESOURCE_TYPES:
        route.abort()
    else:
        route.continue_()


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
        # Callers wait here for a free browser BEFORE the hard timeout clock
        # starts - otherwise a URL queued behind 5 slow pages would "time
        # out" without ever having been tried.
        self._slots = threading.Semaphore(size)
        self._states: list[_ThreadState] = []
        self._states_lock = threading.Lock()

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
            context.route("**/*", _block_heavy_resources)
            state = _ThreadState()
            state.playwright_ctx = pw
            state.browser = browser
            state.context = context
            self._local.state = state
            with self._states_lock:
                self._states.append(state)
        return state

    @staticmethod
    def _close_state(state):
        try:
            if state.context:
                state.context.close()
            if state.browser:
                state.browser.close()
            if state.playwright_ctx:
                state.playwright_ctx.stop()
        except Exception:
            pass

    def _teardown_thread_state(self):
        state = getattr(self._local, "state", None)
        if state is not None:
            self._close_state(state)
            with self._states_lock:
                if state in self._states:
                    self._states.remove(state)
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
        with self._slots:
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
        with self._slots:
            future = self._executor.submit(self._fetch_doc_worker, url)
            try:
                return future.result(timeout=hard_timeout)
            except TimeoutError:
                return DocResult(status="error", error=f"playwright hard timeout after {hard_timeout}s")
            except Exception as e:
                return DocResult(status="error", error=str(e) or type(e).__name__)

    def close(self):
        # Each browser belongs to the worker thread that started it (sync
        # Playwright objects can't be used from another thread), so ask every
        # worker to close its own. Previously nothing closed them and every
        # run left up to `size` Chromium processes behind on the server.
        # The barrier holds each task until all `size` are running, which
        # forces them onto `size` DIFFERENT threads (a thread parked in
        # wait() can't pick up a second task) - one teardown per browser.
        barrier = threading.Barrier(self.size)

        def _close_own():
            self._teardown_thread_state()
            try:
                barrier.wait(timeout=10)
            except threading.BrokenBarrierError:
                pass  # a worker stuck on a hung page - can't reach its browser

        futures = [self._executor.submit(_close_own) for _ in range(self.size)]
        for f in futures:
            try:
                f.result(timeout=15)
            except Exception:
                pass
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
    plan_exhausted = False  # True when Scrape.do credits are at 0; moat cache + Playwright take over
    playwright_pool = None  # created lazily below if/when needed

    # Check the plan BEFORE doing any work.  An exhausted plan no longer
    # hard-fails: we instead activate "moat-cache + Playwright" mode so
    # already-scraped URLs are retrieved free-of-charge and any URL that
    # isn't in the moat cache is handled by the headless browser fallback.
    fetch_workers = FETCH_WORKERS
    if scrape_do_key:
        acct = scrape_do_account(scrape_do_key)
        if acct is not None:
            remaining = acct.get("RemainingMonthlyRequest")
            concurrent = acct.get("ConcurrentRequest")
            is_active = acct.get("IsActive", True)
            log(f"Scrape.do plan: {remaining}/{acct.get('MaxMonthlyRequest')} requests remaining this month, "
                f"{concurrent} concurrent allowed")
            if isinstance(concurrent, int) and concurrent > 0:
                fetch_workers = min(fetch_workers, concurrent)
            if remaining == 0 or not is_active:
                plan_exhausted = True
                log(
                    f"⚠️  Scrape.do plan exhausted ({remaining}/{acct.get('MaxMonthlyRequest')} remaining). "
                    "Activating moat-cache-first mode: URLs already scraped will be retrieved for free; "
                    "any URL not in the Scrape.do cache will fall back to the built-in Playwright browser."
                )
                # Playwright is needed for the fallback path
                playwright_pool = PlaywrightPool()
    if not scrape_do_key:
        playwright_pool = PlaywrightPool()

    # The plan's concurrency cap applies to Scrape.do calls only; Moat lookups
    # and direct downloads run alongside at full NETWORK_WORKERS width.
    scrape_slots = threading.BoundedSemaphore(fetch_workers)

    def fetch_and_cache_html(url, index):
        h = url_hash(url)
        with _index_lock:
            existing = index["html"].get(h)
        if existing is not None and not _is_retryable(existing):
            return existing

        # Cache-first: Moat store (free), then live Scrape.do (if key + credits),
        # then Playwright.
        result, method = None, None
        if moat_config():
            result, method = moat_get_cached_text(url), "moat_cache"
        if result is None or result.status != "ok":
            if scrape_do_key and not plan_exhausted:
                with scrape_slots:
                    result, method = fetch_html_scrape_do(url, scrape_do_key), "scrape_do"
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
        if existing is not None and not _is_retryable(existing):
            return existing

        # Cheapest first: plain download (free, fast - most datasheets are
        # static files), then Scrape.do (credits), then a headless browser.
        result, method = fetch_doc_direct(url), "direct"
        if result.status != "ok":
            direct_err = result.error
            if scrape_do_key and not plan_exhausted:
                with scrape_slots:
                    result, method = fetch_doc_via_scrape_do(url, scrape_do_key), "scrape_do"
            elif playwright_pool is not None:
                result = playwright_pool.fetch_doc(url)
                method = "playwright_fallback" if scrape_do_key else "playwright"
            if result.status != "ok":
                result.error = f"{result.error} (direct download: {direct_err})"

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

        moat_prefix = "Moat cache -> " if moat_config() else ""
        if scrape_do_key and plan_exhausted:
            fetch_method_label = moat_prefix + "Playwright (Scrape.do plan exhausted)"
        elif scrape_do_key:
            fetch_method_label = moat_prefix + "Scrape.do"
        else:
            fetch_method_label = moat_prefix + "Playwright (no Scrape.do key supplied)"
        log(f"Detected columns: part_no='{names[schema['part_no_idx']]}', "
            f"{len(schema['html_idx'])} HTML source col(s), {len(schema['doc_idx'])} document col(s) "
            f"(fetch method: {fetch_method_label})")

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
        html_todo = [u for u in sorted(html_urls)
                     if url_hash(u) not in index["html"] or _is_retryable(index["html"][url_hash(u)])]
        doc_todo = [u for u in sorted(doc_urls)
                    if url_hash(u) not in index["docs"] or _is_retryable(index["docs"][url_hash(u)])]
        retry_html = sum(1 for u in html_todo if url_hash(u) in index["html"])
        retry_doc = sum(1 for u in doc_todo if url_hash(u) in index["docs"])
        log(f"{len(html_urls)} unique HTML URLs ({len(html_todo)} to fetch, {retry_html} retrying past failures), "
            f"{len(doc_urls)} unique document URLs ({len(doc_todo)} to fetch, {retry_doc} retrying past failures)")

        done = {"n": 0}
        done_lock = threading.Lock()
        total_todo = len(html_todo) + len(doc_todo)
        started = time.monotonic()

        def _progress_tick():
            with done_lock:
                done["n"] += 1
                n = done["n"]
            if total_todo and n % 25 == 0:
                rate = n / max(1e-6, time.monotonic() - started)
                log(f"Fetched {n}/{total_todo} ({rate * 60:.0f} URLs/min)")
            if n % 200 == 0:
                # Checkpoint, so a crash/restart mid-run keeps what's fetched.
                with _index_lock:
                    save_index(cache_dir, index)

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

        # HTML pages and documents in ONE pool, concurrently - documents used
        # to wait for every HTML page to finish first. Scrape.do and the
        # browser are still capped by their own semaphores inside.
        with ThreadPoolExecutor(max_workers=NETWORK_WORKERS) as ex:
            futures = [ex.submit(_doc_task, u) for u in doc_todo]
            futures += [ex.submit(_html_task, u) for u in html_todo]
            for fut in as_completed(futures):
                fut.result()
        log(f"Fetch finished in {time.monotonic() - started:.0f}s")
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
            playwright_pool = None
