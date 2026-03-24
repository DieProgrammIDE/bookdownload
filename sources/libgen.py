"""LibGen search backend using libgen-api-enhanced."""

from models import BookResult
from tracing import observe, get_client, flush_tracing


class LibGenSource:

    def __init__(self, mirror: str = "li"):
        from libgen_api_enhanced import LibgenSearch
        self._searcher = LibgenSearch(mirror=mirror)

    @observe(name="search-libgen", capture_input=False, capture_output=False)
    def search_isbn(self, isbn: str) -> list[BookResult]:
        langfuse = get_client()
        langfuse.update_current_span(input={"isbn": isbn})
        try:
            results = self._searcher.search_default(isbn)
        except Exception as e:
            print(f"  [LibGen] Search error: {e}")
            return []

        if not results:
            return []

        books = []
        for book in results:
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
        langfuse.update_current_span(output={
            "result_count": len(books),
            "results": [b.to_dict() for b in books],
        })
        flush_tracing()
        return books
