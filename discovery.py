"""Phase 1: Discovery — search all sources for each ISBN."""

import asyncio
import contextvars
import os
from collections import defaultdict

import diskcache

from models import BookResult
from sources.zlibrary import ZLibrarySource
from tracing import observe, get_client, flush_tracing


def _format_source_summary(books: list[BookResult]) -> str:
    """Format a summary like '2 PDF (5.2MB, 3.1MB), 1 EPUB (1.2MB)'."""
    if not books:
        return "no results"
    by_ext: dict[str, list[str]] = defaultdict(list)
    for b in books:
        by_ext[b.extension.upper() or "?"].append(b.size or "?")
    parts = []
    for ext in sorted(by_ext, key=lambda e: (e != "PDF", e)):
        sizes = by_ext[ext]
        parts.append(f"{len(sizes)} {ext} ({', '.join(sizes)})")
    return f"{len(books)} result(s): {', '.join(parts)}"


def _file_exists_for_isbn(isbn: str, output_dir: str) -> str | None:
    """Return filename if a file for this ISBN already exists on disk."""
    try:
        for f in os.listdir(output_dir):
            if f.startswith(isbn + "_"):
                return f
    except OSError:
        pass
    return None


@observe(name="discover-isbn", capture_input=False, capture_output=False)
async def discover_isbn(
    isbn: str,
    sources: dict,
    cache: diskcache.Cache,
) -> tuple[list[BookResult], list[str]]:
    """Search all sources for a single ISBN.

    Returns (candidates, log_lines) — caller prints log_lines atomically.
    """
    langfuse = get_client()
    langfuse.update_current_span(input={"isbn": isbn, "sources": list(sources.keys())})

    log: list[str] = []
    cache_key = f"discovery:{isbn}"

    cached = cache.get(cache_key)
    if cached is not None:
        candidates = [BookResult.from_dict(d) for d in cached]
        log.append(f"  [cache] {len(candidates)} candidate(s)")
        langfuse.update_current_span(
            output={"candidate_count": len(candidates), "from_cache": True},
        )
        flush_tracing()
        return candidates, log

    existing = _file_exists_for_isbn(isbn, cache.directory.replace("/.cache", ""))
    if existing:
        log.append(f"  [skip] Already on disk: {existing}")
        langfuse.update_current_span(
            output={"candidate_count": 0, "skipped": True, "reason": "on_disk"},
        )
        flush_tracing()
        return [], log

    candidates: list[BookResult] = []

    async def search_sync(name: str, source):
        try:
            ctx = contextvars.copy_context()
            results = await asyncio.to_thread(ctx.run, source.search_isbn, isbn)
            return name, results, None
        except Exception as e:
            return name, [], str(e)

    async def search_async(name: str, source):
        try:
            results = await source.search_isbn(isbn)
            return name, results, None
        except Exception as e:
            return name, [], str(e)

    tasks = []
    for name, source in sources.items():
        if isinstance(source, ZLibrarySource):
            tasks.append(search_async(name, source))
        else:
            tasks.append(search_sync(name, source))

    task_results = await asyncio.gather(*tasks, return_exceptions=True)

    source_results = {}
    for r in task_results:
        if isinstance(r, Exception):
            log.append(f"  [error] {r}")
            continue
        name, books, error = r
        if error:
            log.append(f"  [{name}] Search error: {error}")
            source_results[name] = {"count": 0, "error": error}
        else:
            log.append(f"  [{name}] {_format_source_summary(books)}")
            source_results[name] = {"count": len(books)}
        if books:
            candidates.extend(books)

    cache.set(cache_key, [c.to_dict() for c in candidates])

    langfuse.update_current_span(
        output={
            "candidate_count": len(candidates),
            "from_cache": False,
            "sources": source_results,
        },
    )
    flush_tracing()
    return candidates, log


@observe(name="discovery-phase", capture_input=False, capture_output=False)
async def run_discovery_phase(
    isbns: list[str],
    sources: dict,
    cache: diskcache.Cache,
    concurrency: int,
) -> dict:
    langfuse = get_client()
    langfuse.update_current_span(
        input={
            "isbn_count": len(isbns),
            "sources": list(sources.keys()),
            "concurrency": concurrency,
        },
    )
    flush_tracing()

    semaphore = asyncio.Semaphore(concurrency)

    async def bounded(isbn: str, idx: int, total: int):
        async with semaphore:
            candidates, log_lines = await discover_isbn(isbn, sources, cache)
            # Print header + results atomically so they don't interleave
            output = f"\n[Discovery {idx}/{total}] {isbn}"
            if log_lines:
                output += "\n" + "\n".join(log_lines)
            print(output)
            return isbn, candidates

    tasks = [bounded(isbn, i + 1, len(isbns)) for i, isbn in enumerate(isbns)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    found = sum(1 for r in results if not isinstance(r, Exception) and r[1])
    not_found = sum(1 for r in results if not isinstance(r, Exception) and not r[1])
    errors = sum(1 for r in results if isinstance(r, Exception))
    print(f"\nDiscovery: {found} found, {not_found} not found, {errors} errors")

    summary = {"found": found, "not_found": not_found, "errors": errors}
    langfuse.update_current_span(output=summary)
    flush_tracing()
    return summary
