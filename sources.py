"""Search and download backends for LibGen, Z-Library, Anna's Archive, and Internet Archive."""

import asyncio
import os
import re
import threading
import time
from dataclasses import dataclass, field
from urllib.parse import quote as url_quote

import requests
from bs4 import BeautifulSoup


@dataclass
class BookResult:
    isbn: str
    title: str
    authors: str
    year: str
    language: str
    extension: str
    size: str
    download_url: str
    source: str  # "libgen", "zlibrary", "annas_archive", "internet_archive"
    source_metadata: dict = field(default_factory=dict, repr=False)

    def to_dict(self) -> dict:
        return {
            "isbn": self.isbn,
            "title": self.title,
            "authors": self.authors,
            "year": self.year,
            "language": self.language,
            "extension": self.extension,
            "size": self.size,
            "download_url": self.download_url,
            "source": self.source,
            "source_metadata": self.source_metadata,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "BookResult":
        return cls(**d)


def _parse_size(size_str: str) -> int:
    """Parse size string like '5 Mb' to bytes."""
    if not size_str:
        return 0
    m = re.match(r"([\d.]+)\s*(kb|mb|gb|bytes?)?", size_str.lower())
    if not m:
        return 0
    val = float(m.group(1))
    unit = m.group(2) or "bytes"
    multipliers = {"bytes": 1, "byte": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3}
    return int(val * multipliers.get(unit, 1))


def select_best(candidates: list[BookResult]) -> BookResult | None:
    """Select the best book match: prefer PDF, then largest file size."""
    if not candidates:
        return None

    def score(book: BookResult) -> tuple:
        is_pdf = book.extension == "pdf"
        size_bytes = _parse_size(book.size)
        return (1 if is_pdf else 0, size_bytes)

    return max(candidates, key=score)


class LibGenSource:
    """Synchronous LibGen backend using libgen-api-enhanced."""

    def __init__(self, mirror: str = "li"):
        from libgen_api_enhanced import LibgenSearch
        self._searcher = LibgenSearch(mirror=mirror)

    def search_isbn(self, isbn: str) -> list[BookResult]:
        try:
            results = self._searcher.search_default(isbn)
        except Exception as e:
            print(f"  [LibGen] Search error: {e}")
            return []

        if not results:
            return []

        books = []
        for book in results:
            # Eagerly resolve download URL so results are serializable
            download_url = ""
            try:
                book.resolve_direct_download_link()
                download_url = book.resolved_download_link or ""
            except Exception:
                pass

            books.append(BookResult(
                isbn=isbn,
                title=getattr(book, "title", "") or "",
                authors=getattr(book, "author", "") or "",
                year=getattr(book, "year", "") or "",
                language=(getattr(book, "language", "") or "").strip().lower(),
                extension=(getattr(book, "extension", "") or "").strip().lower(),
                size=getattr(book, "size", "") or "",
                download_url=download_url,
                source="libgen",
            ))
        return books

    def download(self, result: BookResult, output_path: str) -> bool:
        if not result.download_url:
            print("  [LibGen] No download URL available")
            return False
        return _download_file(result.download_url, output_path, source="LibGen")


class ZLibrarySource:
    """Async Z-Library backend using zlibrary package."""

    def __init__(self, email: str, password: str):
        self._email = email
        self._password = password
        self._lib = None
        self._logged_in = False
        self._lock = asyncio.Lock()

    async def _ensure_login(self):
        if self._logged_in:
            return
        import zlibrary
        self._lib = zlibrary.AsyncZlib()
        try:
            await self._lib.login(self._email, self._password)
            self._logged_in = True
        except Exception as e:
            print(f"  [Z-Library] Login failed: {e}")
            raise

    async def check_limits(self) -> dict | None:
        try:
            await self._ensure_login()
            limits = await self._lib.profile.get_limits()
            return limits
        except Exception:
            return None

    async def search_isbn(self, isbn: str) -> list[BookResult]:
        # Serialize Z-Library calls to avoid concurrent session issues
        async with self._lock:
            try:
                await self._ensure_login()
            except Exception:
                return []

            try:
                paginator = await self._lib.search(q=isbn, count=5)
                page_results = await paginator.next()
            except Exception as e:
                print(f"  [Z-Library] Search error: {e}")
                return []

            if not page_results:
                return []

            results = []
            for item in page_results:
                try:
                    book_detail = await item.fetch()
                except Exception:
                    continue

                download_url = ""
                if hasattr(book_detail, "download_url"):
                    download_url = book_detail.get("download_url", "") if isinstance(book_detail, dict) else getattr(book_detail, "download_url", "")

                def _get(obj, key, default=""):
                    if isinstance(obj, dict):
                        return obj.get(key, default)
                    return getattr(obj, key, default)

                title = _get(book_detail, "name", "") or _get(item, "name", "")
                authors_raw = _get(book_detail, "authors", "") or _get(item, "authors", "")
                if isinstance(authors_raw, list):
                    authors = ", ".join(
                        a.get("author", "") if isinstance(a, dict) else str(a)
                        for a in authors_raw
                    )
                else:
                    authors = str(authors_raw)

                results.append(BookResult(
                    isbn=isbn,
                    title=title or "",
                    authors=authors,
                    year=str(_get(book_detail, "year", "") or _get(item, "year", "")),
                    language=str(_get(book_detail, "language", "") or _get(item, "language", "")).strip().lower(),
                    extension=str(_get(book_detail, "extension", "") or _get(item, "extension", "")).strip().lower(),
                    size=str(_get(book_detail, "size", "") or _get(item, "size", "")),
                    download_url=str(download_url),
                    source="zlibrary",
                ))

            return results

    async def download(self, result: BookResult, output_path: str) -> bool:
        if not result.download_url:
            print("  [Z-Library] No download URL available")
            return False
        return _download_file(result.download_url, output_path, source="Z-Library")

    async def close(self):
        if self._lib and self._logged_in:
            try:
                await self._lib.logout()
            except Exception:
                pass
        self._logged_in = False
        self._lib = None


class AnnasArchiveSource:
    """Anna's Archive backend using HTML scraping (ported from annas-mcp Go project).

    Search uses requests + BeautifulSoup.
    Download uses Playwright to bypass DDoS-Guard on slow_download pages,
    extracts the direct file URL, then downloads with requests.
    """

    BROWSER_UA = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    FORMAT_RE = re.compile(r"(?i)\b(EPUB|PDF|MOBI|AZW3|AZW|DJVU|CBZ|CBR|FB2|DOCX?|TXT)\b")
    SIZE_RE = re.compile(r"\d+\.?\d*\s*(MB|KB|GB|TB)", re.IGNORECASE)

    def __init__(self, base_url: str = "annas-archive.gl"):
        self._base = base_url
        self._pw = None
        self._browser = None
        self._lock = threading.Lock()
        self._backoff_until = 0.0
        self._backoff_seconds = 0

    def _ensure_browser(self):
        """Lazily launch Playwright browser (reused across downloads)."""
        if self._browser is None:
            from playwright.sync_api import sync_playwright
            self._pw = sync_playwright().start()
            self._browser = self._pw.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )

    def _wait_for_backoff(self):
        """Block until backoff period expires."""
        now = time.time()
        if now < self._backoff_until:
            wait = self._backoff_until - now
            print(f"  [Anna's Archive] Rate limited, waiting {wait:.0f}s...")
            time.sleep(wait)

    def _on_failure(self):
        """Increase backoff exponentially (10s → 20s → 40s → ... → 120s cap)."""
        with self._lock:
            self._backoff_seconds = min(max(self._backoff_seconds * 2, 10), 120)
            self._backoff_until = time.time() + self._backoff_seconds

    def _on_success(self):
        """Reset backoff on successful download."""
        with self._lock:
            self._backoff_seconds = 0
            self._backoff_until = 0.0

    def _parse_meta(self, meta_text: str) -> tuple[str, str, str]:
        """Parse metadata string like '✅ German [de] · PDF · 9.0MB · 2017'."""
        parts = meta_text.split(" · ")
        if len(parts) < 3:
            return "", "", ""

        # Language: first part, strip ✅, take before [
        lang_part = parts[0].strip()
        language = ""
        bracket_idx = lang_part.find("[")
        if bracket_idx > 0:
            language = lang_part[:bracket_idx].replace("✅", "").strip()
        else:
            language = lang_part.replace("✅", "").strip()

        # Format and size from remaining parts
        fmt = ""
        size = ""
        for part in parts[1:]:
            part = part.strip()
            if not fmt:
                m = self.FORMAT_RE.search(part)
                if m:
                    fmt = m.group(1).upper()
            if not size:
                m = self.SIZE_RE.search(part)
                if m:
                    size = part.strip()
            if fmt and size:
                break

        return language, fmt, size

    def search_isbn(self, isbn: str) -> list[BookResult]:
        search_url = f"https://{self._base}/search?q={url_quote(isbn)}&content=book_any"
        try:
            resp = requests.get(
                search_url,
                headers={"User-Agent": self.BROWSER_UA},
                timeout=30,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  [Anna's Archive] Search error: {e}")
            return []

        soup = BeautifulSoup(resp.text, "lxml")

        # Find cover image links (same selector as Go code)
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

            # Title
            title_el = info_div.select_one('a[href^="/md5/"]')
            title = title_el.get_text(strip=True) if title_el else ""
            if not title:
                continue

            # Authors
            authors = ""
            author_icon = info_div.select_one('a[href^="/search"] span.icon-\\[mdi--user-edit\\]')
            if author_icon and author_icon.parent:
                authors = author_icon.parent.get_text(strip=True)

            # Publisher (stored in source_metadata)
            publisher = ""
            pub_icon = info_div.select_one('a[href^="/search"] span.icon-\\[mdi--company\\]')
            if pub_icon and pub_icon.parent:
                publisher = pub_icon.parent.get_text(strip=True)

            # Metadata (language, format, size)
            meta_div = info_div.select_one("div.text-gray-800")
            language, fmt, size = "", "", ""
            if meta_div:
                language, fmt, size = self._parse_meta(meta_div.get_text())

            # Hash from href
            href = a_tag.get("href", "")
            md5_hash = href.replace("/md5/", "") if href.startswith("/md5/") else ""
            if not md5_hash:
                continue

            ext = fmt.lower() if fmt else ""
            download_url = f"https://{self._base}/slow_download/{md5_hash}/0/5"

            results.append(BookResult(
                isbn=isbn,
                title=title,
                authors=authors,
                year="",
                language=language.lower(),
                extension=ext,
                size=size,
                download_url=download_url,
                source="annas_archive",
                source_metadata={"hash": md5_hash, "publisher": publisher},
            ))

        return results

    def _extract_download_url(self, md5_hash: str, extension: str) -> str | None:
        """Use Playwright to visit slow_download page, bypass DDoS-Guard, extract direct URL.

        Serialized via self._lock because Playwright's sync API is not thread-safe.
        The actual file download happens outside the lock so multiple files can
        download in parallel.
        """
        # Build regex to match direct download URLs containing the hash
        ext_pattern = re.escape(extension) if extension else r"[a-z]+"
        url_re = re.compile(
            r'https?://[^\s"<>\']+' + re.escape(md5_hash) + r'[^\s"<>\']*\.' + ext_pattern
        )

        with self._lock:
            self._ensure_browser()
            context = self._browser.new_context(user_agent=self.BROWSER_UA)
            page = context.new_page()

            try:
                for server_id in range(5, 9):
                    self._wait_for_backoff()
                    url = f"https://{self._base}/slow_download/{md5_hash}/0/{server_id}"
                    print(f"  [Anna's Archive] Trying server {server_id}...")
                    try:
                        page.goto(url, timeout=30000)
                        page.wait_for_function(
                            "() => !document.body.innerText.includes('Checking your browser')",
                            timeout=15000,
                        )
                        html = page.content()
                        match = url_re.search(html)
                        if match:
                            direct_url = match.group(0)
                            print(f"  [Anna's Archive] Got direct URL from server {server_id}")
                            self._on_success()
                            return direct_url
                    except Exception as e:
                        print(f"  [Anna's Archive] Server {server_id} failed: {e}")
                        self._on_failure()
                        continue
            finally:
                context.close()

        return None

    def download(self, result: BookResult, output_path: str) -> bool:
        md5_hash = result.source_metadata.get("hash", "")
        if not md5_hash:
            if result.download_url:
                return _download_file(result.download_url, output_path, source="Anna's Archive")
            print("  [Anna's Archive] No hash or download URL available")
            return False

        direct_url = self._extract_download_url(md5_hash, result.extension)
        if not direct_url:
            print("  [Anna's Archive] Could not extract download URL from any server")
            return False

        return _download_file(direct_url, output_path, source="Anna's Archive")

    def close(self):
        """Shut down the Playwright browser."""
        if self._browser:
            try:
                self._browser.close()
            except Exception:
                pass
            self._browser = None
        if self._pw:
            try:
                self._pw.stop()
            except Exception:
                pass
            self._pw = None


class InternetArchiveSource:
    """Synchronous Internet Archive backend using internetarchive library (no API key required)."""

    def search_isbn(self, isbn: str) -> list[BookResult]:
        try:
            import internetarchive
        except ImportError:
            print("  [Internet Archive] internetarchive not installed")
            return []

        try:
            search = internetarchive.search_items(
                f"isbn:{isbn}",
                fields=["identifier", "title", "creator", "date", "mediatype", "language"],
            )
            ia_results = list(search)
        except Exception as e:
            print(f"  [Internet Archive] Search error: {e}")
            return []

        books = []
        for result in ia_results[:5]:
            identifier = result.get("identifier", "")
            if not identifier:
                continue

            try:
                item = internetarchive.get_item(identifier)
            except Exception:
                continue

            meta = item.metadata or {}
            title = result.get("title") or meta.get("title", "")
            creator = result.get("creator") or meta.get("creator", "")
            if isinstance(creator, list):
                creator = ", ".join(creator)
            date = str(result.get("date") or meta.get("date", ""))[:4]
            lang = str(meta.get("language", "")).strip().lower()

            try:
                files = list(item.get_files())
            except Exception:
                continue

            for f in files:
                ext = self._format_to_ext(getattr(f, "format", ""), getattr(f, "name", ""))
                if ext not in ("pdf", "epub", "djvu", "mobi"):
                    continue
                url = getattr(f, "url", "")
                if not url:
                    continue
                size_str = self._format_size(getattr(f, "size", None))
                books.append(BookResult(
                    isbn=isbn,
                    title=str(title),
                    authors=str(creator),
                    year=date,
                    language=lang,
                    extension=ext,
                    size=size_str,
                    download_url=url,
                    source="internet_archive",
                    source_metadata={"identifier": identifier, "filename": getattr(f, "name", "")},
                ))

        return books

    @staticmethod
    def _format_to_ext(format_str: str, filename: str) -> str:
        fmt = format_str.lower()
        if "pdf" in fmt:
            return "pdf"
        if "epub" in fmt:
            return "epub"
        if "djvu" in fmt:
            return "djvu"
        if "mobi" in fmt:
            return "mobi"
        if "." in filename:
            return filename.rsplit(".", 1)[-1].lower()
        return ""

    @staticmethod
    def _format_size(size) -> str:
        try:
            b = int(size)
            if b >= 1024**3:
                return f"{b / 1024**3:.1f} Gb"
            if b >= 1024**2:
                return f"{b / 1024**2:.1f} Mb"
            if b >= 1024:
                return f"{b / 1024:.1f} Kb"
            return f"{b} bytes"
        except (ValueError, TypeError):
            return str(size) if size else ""

    def download(self, result: BookResult, output_path: str) -> bool:
        if not result.download_url:
            print("  [Internet Archive] No download URL available")
            return False
        return _download_file(result.download_url, output_path, source="Internet Archive")


def _download_file(url: str, output_path: str, source: str, max_retries: int = 3) -> bool:
    """Download a file with retries and exponential backoff."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, stream=True, timeout=60, allow_redirects=True)

            if resp.status_code in (429, 503):
                wait = min(30 * (attempt + 1), 60)
                print(f"  [{source}] Rate limited ({resp.status_code}), waiting {wait}s...")
                time.sleep(wait)
                continue

            resp.raise_for_status()

            with open(output_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            file_size = os.path.getsize(output_path)
            if file_size == 0:
                print(f"  [{source}] Downloaded file is empty")
                os.remove(output_path)
                return False

            if output_path.lower().endswith(".pdf"):
                with open(output_path, "rb") as f:
                    header = f.read(5)
                if header != b"%PDF-":
                    print(f"  [{source}] File does not appear to be a valid PDF (header: {header!r})")
                    os.remove(output_path)
                    return False

            return True

        except requests.exceptions.RequestException as e:
            wait = 2 ** (attempt + 1)
            print(f"  [{source}] Download error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print(f"  [{source}] Retrying in {wait}s...")
                time.sleep(wait)

    return False
