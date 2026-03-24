"""Search and download backends for LibGen and Z-Library."""

import asyncio
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any

import requests


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
    source: str  # "libgen" or "zlibrary"
    _original: Any = field(default=None, repr=False)


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
            books.append(BookResult(
                isbn=isbn,
                title=getattr(book, "title", "") or "",
                authors=getattr(book, "author", "") or "",
                year=getattr(book, "year", "") or "",
                language=(getattr(book, "language", "") or "").strip().lower(),
                extension=(getattr(book, "extension", "") or "").strip().lower(),
                size=getattr(book, "size", "") or "",
                download_url="",  # resolved at download time
                source="libgen",
                _original=book,
            ))
        return books

    def download(self, result: BookResult, output_path: str) -> bool:
        book = result._original
        try:
            book.resolve_direct_download_link()
            url = book.resolved_download_link
        except Exception as e:
            print(f"  [LibGen] Failed to resolve download link: {e}")
            return False

        if not url:
            print("  [LibGen] No download URL resolved")
            return False

        return _download_file(url, output_path, source="LibGen")


class ZLibrarySource:
    """Async Z-Library backend using zlibrary package."""

    def __init__(self, email: str, password: str):
        self._email = email
        self._password = password
        self._lib = None
        self._logged_in = False

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
        try:
            await self._ensure_login()
        except Exception:
            return []

        results = []

        # Try exact search first
        try:
            paginator = await self._lib.search(q=isbn, count=5)
            page_results = await paginator.next()
        except Exception as e:
            print(f"  [Z-Library] Search error: {e}")
            return []

        if not page_results:
            return []

        for item in page_results:
            try:
                book_detail = await item.fetch()
            except Exception:
                continue

            download_url = ""
            if hasattr(book_detail, "download_url"):
                download_url = book_detail.get("download_url", "") if isinstance(book_detail, dict) else getattr(book_detail, "download_url", "")

            # Extract fields - results may be dict-like or object-like
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
                _original=book_detail,
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

            # Verify file
            file_size = os.path.getsize(output_path)
            if file_size == 0:
                print(f"  [{source}] Downloaded file is empty")
                os.remove(output_path)
                return False

            # Check PDF header if extension is pdf
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
