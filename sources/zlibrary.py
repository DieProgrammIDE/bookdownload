"""Z-Library search backend using zlibrary package."""

import asyncio

from models import BookResult
from tracing import observe, get_client, flush_tracing


class ZLibrarySource:

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
            return await self._lib.profile.get_limits()
        except Exception:
            return None

    @observe(name="search-zlibrary", capture_input=False, capture_output=False)
    async def search_isbn(self, isbn: str) -> list[BookResult]:
        langfuse = get_client()
        langfuse.update_current_span(input={"isbn": isbn})
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
                    download_url = (
                        book_detail.get("download_url", "")
                        if isinstance(book_detail, dict)
                        else getattr(book_detail, "download_url", "")
                    )

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
                    language=str(
                        _get(book_detail, "language", "") or _get(item, "language", "")
                    ).strip().lower(),
                    extension=str(
                        _get(book_detail, "extension", "") or _get(item, "extension", "")
                    ).strip().lower(),
                    size=str(_get(book_detail, "size", "") or _get(item, "size", "")),
                    download_url=str(download_url),
                    source="zlibrary",
                ))

            langfuse.update_current_span(output={
                "result_count": len(results),
                "results": [r.to_dict() for r in results],
            })
            flush_tracing()
            return results

    async def close(self):
        if self._lib and self._logged_in:
            try:
                await self._lib.logout()
            except Exception:
                pass
        self._logged_in = False
        self._lib = None
