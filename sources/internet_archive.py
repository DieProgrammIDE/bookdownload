"""Internet Archive search backend using internetarchive library."""

from models import BookResult
from tracing import observe, get_client, flush_tracing


class InternetArchiveSource:

    @observe(name="search-internet-archive", capture_input=False, capture_output=False)
    def search_isbn(self, isbn: str) -> list[BookResult]:
        langfuse = get_client()
        langfuse.update_current_span(input={"isbn": isbn})
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
                ext = _format_to_ext(getattr(f, "format", ""), getattr(f, "name", ""))
                if ext not in ("pdf", "epub", "djvu", "mobi"):
                    continue
                url = getattr(f, "url", "")
                if not url:
                    continue
                size_str = _format_size(getattr(f, "size", None))
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
                    source_metadata={
                        "identifier": identifier,
                        "filename": getattr(f, "name", ""),
                    },
                ))

        langfuse.update_current_span(output={
            "result_count": len(books),
            "results": [b.to_dict() for b in books],
        })
        flush_tracing()
        return books


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
