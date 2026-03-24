"""Anna's Archive backend: HTML scraping for search, Playwright for DDoS-Guard bypass."""

import asyncio
import itertools
import re
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote as url_quote

import requests
from bs4 import BeautifulSoup

from models import BROWSER_UA, BookResult

FORMAT_RE = re.compile(r"(?i)\b(EPUB|PDF|MOBI|AZW3|AZW|DJVU|CBZ|CBR|FB2|DOCX?|TXT)\b")
SIZE_RE = re.compile(r"\d+\.?\d*\s*(MB|KB|GB|TB)", re.IGNORECASE)


class AnnasArchiveSource:
    SERVERS = [5, 6, 7, 8]

    def __init__(self, base_url: str = "annas-archive.gl"):
        self._base = base_url
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="annas")
        self._pw = None
        self._browser = None
        self._server_cycle = itertools.cycle(self.SERVERS)
        self._server_sems: dict[int, asyncio.Semaphore] = {}
        self._backoff_until = 0.0
        self._backoff_seconds = 0

    def init_semaphores(self, per_server: int = 1):
        """Create per-server semaphores. Call after event loop is running."""
        self._server_sems = {s: asyncio.Semaphore(per_server) for s in self.SERVERS}

    def next_server(self) -> int:
        return next(self._server_cycle)

    def server_sem(self, server_id: int) -> asyncio.Semaphore:
        return self._server_sems[server_id]

    # ------------------------------------------------------------------
    # Search (sync, uses requests + BeautifulSoup)
    # ------------------------------------------------------------------

    def search_isbn(self, isbn: str) -> list[BookResult]:
        search_url = f"https://{self._base}/search?q={url_quote(isbn)}&content=book_any"
        try:
            resp = requests.get(
                search_url, headers={"User-Agent": BROWSER_UA}, timeout=30,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  [Anna's Archive] Search error: {e}")
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        cover_links = soup.select('a[href^="/md5/"].custom-a')
        if not cover_links:
            cover_links = [
                a for a in soup.select('a[href^="/md5/"]')
                if "custom-a" in (a.get("class") or [])
            ]

        results = []
        for a_tag in cover_links:
            parent = a_tag.parent
            if parent is None:
                continue
            info_div = parent.select_one("div.max-w-full")
            if info_div is None:
                continue

            title_el = info_div.select_one('a[href^="/md5/"]')
            title = title_el.get_text(strip=True) if title_el else ""
            if not title:
                continue

            authors = ""
            author_icon = info_div.select_one(
                'a[href^="/search"] span.icon-\\[mdi--user-edit\\]'
            )
            if author_icon and author_icon.parent:
                authors = author_icon.parent.get_text(strip=True)

            publisher = ""
            pub_icon = info_div.select_one(
                'a[href^="/search"] span.icon-\\[mdi--company\\]'
            )
            if pub_icon and pub_icon.parent:
                publisher = pub_icon.parent.get_text(strip=True)

            meta_div = info_div.select_one("div.text-gray-800")
            language, fmt, size = "", "", ""
            if meta_div:
                language, fmt, size = _parse_meta(meta_div.get_text())

            href = a_tag.get("href", "")
            md5_hash = href.replace("/md5/", "") if href.startswith("/md5/") else ""
            if not md5_hash:
                continue

            ext = fmt.lower() if fmt else ""
            results.append(BookResult(
                isbn=isbn,
                title=title,
                authors=authors,
                year="",
                language=language.lower(),
                extension=ext,
                size=size,
                download_url=f"https://{self._base}/slow_download/{md5_hash}/0/5",
                source="annas_archive",
                source_metadata={"hash": md5_hash, "publisher": publisher},
            ))

        return results

    # ------------------------------------------------------------------
    # URL extraction (sync Playwright, runs in dedicated thread)
    # ------------------------------------------------------------------

    async def extract_url(self, md5_hash: str, start_server: int) -> str | None:
        """Submit extraction to the dedicated Playwright thread."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor, self._do_extract, md5_hash, start_server,
        )

    def _do_extract(self, md5_hash: str, start_server: int) -> str | None:
        """Try servers starting from start_server. Fully synchronous."""
        self._ensure_browser()
        servers = [start_server] + [s for s in self.SERVERS if s != start_server]
        for sid in servers:
            self._wait_for_backoff()
            url = self._try_server(md5_hash, sid)
            if url:
                self._on_success()
                return url
        return None

    def _try_server(self, md5_hash: str, server_id: int) -> str | None:
        """Visit slow_download page, wait for DDoS-Guard, extract direct URL."""
        url_re = re.compile(
            r'https?://[^\s"<>\']+' + re.escape(md5_hash) + r'[^\s"<>\']*'
        )
        context = self._browser.new_context(user_agent=BROWSER_UA)
        page = context.new_page()
        try:
            url = f"https://{self._base}/slow_download/{md5_hash}/0/{server_id}"
            page.goto(url, timeout=30000)

            # Poll for DDoS-Guard resolution (~6s typical)
            for _ in range(15):
                time.sleep(1)
                try:
                    if page.title() != "DDoS-Guard":
                        time.sleep(2)
                        break
                except Exception:
                    continue

            html = page.content()
            if "Download from partner" not in html and "slow_download" not in html:
                return None

            match = url_re.search(html)
            if match:
                return match.group(0)
        except Exception:
            self._on_failure()
        finally:
            context.close()
        return None

    def _ensure_browser(self):
        if self._browser is None:
            from playwright.sync_api import sync_playwright
            self._pw = sync_playwright().start()
            self._browser = self._pw.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )

    def _wait_for_backoff(self):
        now = time.time()
        if now < self._backoff_until:
            wait = self._backoff_until - now
            time.sleep(wait)

    def _on_failure(self):
        self._backoff_seconds = min(max(self._backoff_seconds * 2, 10), 120)
        self._backoff_until = time.time() + self._backoff_seconds

    def _on_success(self):
        self._backoff_seconds = 0
        self._backoff_until = 0.0

    def close(self):
        def _cleanup():
            if self._browser:
                try:
                    self._browser.close()
                except Exception:
                    pass
            if self._pw:
                try:
                    self._pw.stop()
                except Exception:
                    pass
        try:
            self._executor.submit(_cleanup).result(timeout=30)
        except Exception:
            pass
        self._executor.shutdown(wait=False)


def _parse_meta(meta_text: str) -> tuple[str, str, str]:
    """Parse metadata string like '✅ German [de] · PDF · 9.0MB · 2017'."""
    parts = meta_text.split(" · ")
    if len(parts) < 3:
        return "", "", ""

    lang_part = parts[0].strip()
    bracket_idx = lang_part.find("[")
    if bracket_idx > 0:
        language = lang_part[:bracket_idx].replace("✅", "").strip()
    else:
        language = lang_part.replace("✅", "").strip()

    fmt, size = "", ""
    for part in parts[1:]:
        part = part.strip()
        if not fmt:
            m = FORMAT_RE.search(part)
            if m:
                fmt = m.group(1).upper()
        if not size:
            m = SIZE_RE.search(part)
            if m:
                size = part.strip()
        if fmt and size:
            break

    return language, fmt, size
