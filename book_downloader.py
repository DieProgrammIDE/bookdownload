#!/usr/bin/env python3
"""ISBN Book Downloader - Downloads books from shadow libraries by ISBN."""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from collections import defaultdict

import diskcache
from dotenv import load_dotenv

# Suppress noisy warnings from libgen library ("No results table found on search page")
logging.getLogger("libgen_api_enhanced").setLevel(logging.ERROR)

from sources import (
    AnnasArchiveSource,
    BookResult,
    InternetArchiveSource,
    LibGenSource,
    ZLibrarySource,
    _download_file,
    rank_candidates,
    select_best,
)


def load_isbns(filepath: str) -> list[str]:
    """Load and validate ISBNs from a file (one per line)."""
    isbns = []
    seen = set()
    with open(filepath, "r") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            isbn = re.sub(r"[\s-]", "", line)
            if not re.match(r"^\d{10}(\d{3})?$", isbn):
                print(f"  Warning: Skipping invalid ISBN on line {line_num}: {line}")
                continue
            if isbn not in seen:
                seen.add(isbn)
                isbns.append(isbn)
    return isbns


def sanitize_filename(name: str) -> str:
    """Sanitize a string for use in filenames."""
    name = re.sub(r'[<>:"/\\|?*]', "", name)
    name = re.sub(r"\s+", "_", name)
    name = name.strip("_.")
    return name[:100]


def make_output_filename(isbn: str, book: BookResult) -> str:
    """Create output filename from ISBN and book metadata."""
    title_part = sanitize_filename(book.title) if book.title else "unknown"
    ext = book.extension or "pdf"
    return f"{isbn}_{title_part}.{ext}"


def _file_exists_for_isbn(isbn: str, output_dir: str) -> str | None:
    """Return filename if a file for this ISBN already exists on disk, else None."""
    try:
        for f in os.listdir(output_dir):
            if f.startswith(isbn + "_"):
                return f
    except OSError:
        pass
    return None


def _format_source_summary(books: list) -> str:
    """Format a summary like '2 PDF (5.2MB, 3.1MB), 1 EPUB (1.2MB)'."""
    if not books:
        return "no results"
    by_ext = defaultdict(list)
    for b in books:
        by_ext[b.extension.upper() or "?"].append(b.size or "?")
    parts = []
    for ext in sorted(by_ext, key=lambda e: (e != "PDF", e)):
        sizes = by_ext[ext]
        parts.append(f"{len(sizes)} {ext} ({', '.join(sizes)})")
    return f"{len(books)} result(s): {', '.join(parts)}"


# ---------------------------------------------------------------------------
# Phase 1: Discovery
# ---------------------------------------------------------------------------

async def discover_isbn(
    isbn: str,
    sources: dict,
    cache: diskcache.Cache,
) -> list[BookResult]:
    """Search all sources for a single ISBN. Returns list of BookResult candidates."""
    cache_key = f"discovery:{isbn}"

    cached = cache.get(cache_key)
    if cached is not None:
        candidates = [BookResult.from_dict(d) for d in cached]
        print(f"  [cache] {len(candidates)} candidate(s) for {isbn}")
        return candidates

    existing = _file_exists_for_isbn(isbn, cache.directory.replace("/.cache", ""))
    if existing:
        print(f"  [skip] Already on disk: {existing}")
        return []

    candidates: list[BookResult] = []

    async def search_sync(name: str, source):
        try:
            results = await asyncio.to_thread(source.search_isbn, isbn)
            return name, results
        except Exception as e:
            print(f"  [{name}] Search error: {e}")
            return name, []

    async def search_async(name: str, source):
        try:
            results = await source.search_isbn(isbn)
            return name, results
        except Exception as e:
            print(f"  [{name}] Search error: {e}")
            return name, []

    tasks = []
    for name, source in sources.items():
        if isinstance(source, ZLibrarySource):
            tasks.append(search_async(name, source))
        else:
            tasks.append(search_sync(name, source))

    task_results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in task_results:
        if isinstance(r, Exception):
            print(f"  [error] {r}")
            continue
        name, books = r
        print(f"  [{name}] {_format_source_summary(books)}")
        if books:
            candidates.extend(books)

    cache.set(cache_key, [c.to_dict() for c in candidates])
    return candidates


async def run_discovery_phase(
    isbns: list[str],
    sources: dict,
    cache: diskcache.Cache,
    concurrency: int,
) -> dict:
    semaphore = asyncio.Semaphore(concurrency)

    async def bounded(isbn: str, idx: int, total: int):
        async with semaphore:
            print(f"\n[Discovery {idx}/{total}] {isbn}")
            return isbn, await discover_isbn(isbn, sources, cache)

    tasks = [bounded(isbn, i + 1, len(isbns)) for i, isbn in enumerate(isbns)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    found = sum(1 for r in results if not isinstance(r, Exception) and r[1])
    not_found = sum(1 for r in results if not isinstance(r, Exception) and not r[1])
    errors = sum(1 for r in results if isinstance(r, Exception))
    print(f"\nDiscovery: {found} found, {not_found} not found, {errors} errors")
    return {"found": found, "not_found": not_found, "errors": errors}


# ---------------------------------------------------------------------------
# Phase 2: Download
# ---------------------------------------------------------------------------

async def download_isbn(
    isbn: str,
    sources: dict,
    cache: diskcache.Cache,
    output_dir: str,
) -> tuple[str, BookResult | None]:
    """Download best match for isbn. Returns (status, selected_book).

    Skip condition: file already exists on disk. No cache-based skipping —
    failed downloads are always retried on later runs.
    """
    if _file_exists_for_isbn(isbn, output_dir):
        return "exists", None

    discovery_key = f"discovery:{isbn}"
    cached_discovery = cache.get(discovery_key)
    if cached_discovery is None:
        print(f"  No discovery results (run discovery phase first)")
        return "not_found", None

    candidates = [BookResult.from_dict(d) for d in cached_discovery]
    if not candidates:
        return "not_found", None

    ranked = rank_candidates(candidates)
    if not ranked:
        print(f"  No suitable format found among {len(candidates)} candidate(s)")
        return "not_found", None

    for i, candidate in enumerate(ranked):
        print(f"  → [{i+1}/{len(ranked)}] \"{candidate.title}\" ({candidate.extension}, {candidate.size}) [{candidate.source}]")

        filename = make_output_filename(isbn, candidate)
        output_path = os.path.join(output_dir, filename)

        source = sources.get(candidate.source)
        if source is None:
            if candidate.download_url:
                success = await asyncio.to_thread(_download_file, candidate.download_url, output_path, candidate.source)
            else:
                print(f"  Source '{candidate.source}' unavailable and no URL cached")
                continue
        elif isinstance(source, ZLibrarySource):
            success = await source.download(candidate, output_path)
        else:
            success = await asyncio.to_thread(source.download, candidate, output_path)

        if success:
            size = os.path.getsize(output_path)
            print(f"  Saved: {filename} ({size:,} bytes)")
            return "downloaded", candidate

        if i < len(ranked) - 1:
            print(f"  Trying next candidate...")

    print(f"  All {len(ranked)} candidate(s) failed")
    return "failed", ranked[0]


async def run_download_phase(
    isbns: list[str],
    sources: dict,
    cache: diskcache.Cache,
    output_dir: str,
    concurrency: int,
) -> tuple[dict, dict]:
    semaphore = asyncio.Semaphore(concurrency)
    total = len(isbns)
    progress = {"downloaded": 0, "exists": 0, "not_found": 0, "failed": 0, "done": 0}
    start_time = asyncio.get_event_loop().time()

    async def status_reporter():
        """Print progress every 30 seconds."""
        while progress["done"] < total:
            await asyncio.sleep(30)
            if progress["done"] >= total:
                break
            elapsed = asyncio.get_event_loop().time() - start_time
            print(
                f"\n--- Status [{progress['done']}/{total}] {elapsed:.0f}s elapsed "
                f"| downloaded: {progress['downloaded']} "
                f"| exists: {progress['exists']} "
                f"| failed: {progress['failed']} "
                f"| not_found: {progress['not_found']} ---"
            )

    async def bounded(isbn: str, idx: int, total: int):
        async with semaphore:
            print(f"\n[Download {idx}/{total}] {isbn}")
            status, selected = await download_isbn(isbn, sources, cache, output_dir)
            progress[status] = progress.get(status, 0) + 1
            progress["done"] += 1
            return isbn, status, selected

    reporter = asyncio.create_task(status_reporter())
    tasks = [bounded(isbn, i + 1, total) for i, isbn in enumerate(isbns)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    reporter.cancel()

    stats = {"downloaded": 0, "exists": 0, "not_found": 0, "failed": 0}
    results_log = {}
    for r in results:
        if isinstance(r, Exception):
            print(f"  [error] Download exception: {r}")
            stats["failed"] += 1
        else:
            isbn, status, selected = r
            stats[status] = stats.get(status, 0) + 1
            results_log[isbn] = {
                "status": status,
                "selected": selected.to_dict() if selected else None,
            }

    return stats, results_log


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(
        description="Download books by ISBN from shadow libraries"
    )
    parser.add_argument("isbn_file", help="Path to file with ISBNs (one per line)")
    parser.add_argument("-o", "--output-dir", default="downloads", help="Output directory (default: downloads/)")
    parser.add_argument("--libgen-mirror", default="li", help="LibGen mirror TLD: li, bz, gs (default: li)")
    parser.add_argument("--annas-mirror", default="annas-archive.gl",
                        help="Anna's Archive mirror domain (default: annas-archive.gl)")
    parser.add_argument("--any-format", action="store_true",
                        help="(deprecated, always enabled) Accept any format")

    # Source toggles
    parser.add_argument("--no-libgen", action="store_true", help="Skip LibGen")
    parser.add_argument("--no-zlibrary", action="store_true", help="Skip Z-Library")
    parser.add_argument("--no-annas", action="store_true", help="Skip Anna's Archive")
    parser.add_argument("--no-internet-archive", action="store_true", help="Skip Internet Archive")

    # Cache
    parser.add_argument("--clear-cache", action="store_true", help="Clear cache and start fresh")

    # Phase control
    parser.add_argument("--discovery-only", action="store_true", help="Run discovery phase only")
    parser.add_argument("--download-only", action="store_true", help="Run download phase only (requires cached discovery)")

    # Concurrency
    parser.add_argument("--discovery-concurrency", type=int, default=10,
                        help="Max parallel ISBN searches during discovery (default: 10)")
    parser.add_argument("--download-concurrency", type=int, default=3,
                        help="Max parallel downloads (default: 3)")

    args = parser.parse_args()

    load_dotenv()

    if not os.path.isfile(args.isbn_file):
        print(f"Error: ISBN file not found: {args.isbn_file}")
        sys.exit(1)

    isbns = load_isbns(args.isbn_file)
    if not isbns:
        print("No valid ISBNs found in input file")
        sys.exit(1)

    print(f"Loaded {len(isbns)} ISBN(s)")

    os.makedirs(args.output_dir, exist_ok=True)
    cache_dir = os.path.join(args.output_dir, ".cache")
    cache = diskcache.Cache(cache_dir)

    if args.clear_cache:
        cache.clear()
        print("Cache cleared.")

    # Initialize sources
    sources: dict = {}

    if not args.no_libgen:
        try:
            sources["libgen"] = LibGenSource(mirror=args.libgen_mirror)
            print(f"LibGen initialized (mirror: .{args.libgen_mirror})")
        except Exception as e:
            print(f"Warning: LibGen init failed: {e}")

    if not args.no_annas:
        sources["annas_archive"] = AnnasArchiveSource(base_url=args.annas_mirror)
        print(f"Anna's Archive initialized (mirror: {args.annas_mirror})")

    if not args.no_internet_archive:
        sources["internet_archive"] = InternetArchiveSource()
        print("Internet Archive initialized")

    if not args.no_zlibrary:
        zl_email = os.getenv("ZLIBRARY_EMAIL")
        zl_password = os.getenv("ZLIBRARY_PASSWORD")
        if zl_email and zl_password:
            zlib = ZLibrarySource(zl_email, zl_password)
            limits = await zlib.check_limits()
            if limits:
                print(f"Z-Library initialized (limits: {limits})")
            else:
                print("Z-Library initialized")
            sources["zlibrary"] = zlib
        else:
            print("Z-Library: no credentials found (set ZLIBRARY_EMAIL/ZLIBRARY_PASSWORD in .env)")

    if not sources:
        print("Error: No sources available.")
        sys.exit(1)

    # Phase 1: Discovery
    if not args.download_only:
        print(f"\n{'='*60}")
        print(f"Phase 1: Discovery  [{len(isbns)} ISBNs, concurrency={args.discovery_concurrency}]")
        print(f"{'='*60}")
        await run_discovery_phase(isbns, sources, cache, args.discovery_concurrency)

    # Save discovery metadata JSON
    discovery_data = {}
    for isbn in isbns:
        cached = cache.get(f"discovery:{isbn}")
        discovery_data[isbn] = {
            "candidates": cached if cached is not None else [],
        }
    discovery_path = os.path.join(args.output_dir, "discovery.json")
    with open(discovery_path, "w") as f:
        json.dump(discovery_data, f, indent=2, ensure_ascii=False)
    print(f"\nDiscovery metadata written to {discovery_path}")

    # Phase 2: Download
    stats = {}
    results_log = {}
    if not args.discovery_only:
        print(f"\n{'='*60}")
        print(f"Phase 2: Download  [concurrency={args.download_concurrency}]")
        print(f"{'='*60}")
        stats, results_log = await run_download_phase(
            isbns, sources, cache, args.output_dir, args.download_concurrency
        )

        not_found = [isbn for isbn, info in results_log.items() if info["status"] == "not_found"]
        failed = [isbn for isbn, info in results_log.items() if info["status"] == "failed"]

        print(f"\n{'='*60}")
        print(f"Summary:")
        print(f"  Downloaded:      {stats.get('downloaded', 0)}")
        print(f"  Already existed: {stats.get('exists', 0)}")
        print(f"  Not found:       {stats.get('not_found', 0)}")
        print(f"  Failed:          {stats.get('failed', 0)}")
        print(f"  Total:           {len(isbns)}")

        if not_found:
            print(f"\nNot found ({len(not_found)}):")
            for isbn in not_found:
                print(f"  {isbn}")
        if failed:
            print(f"\nFailed ({len(failed)}):")
            for isbn in failed:
                print(f"  {isbn}")

        results_path = os.path.join(args.output_dir, "results.json")
        with open(results_path, "w") as f:
            json.dump({"stats": stats, "results": results_log,
                       "not_found": not_found, "failed": failed}, f, indent=2, ensure_ascii=False)
        print(f"\nResults written to {results_path}")

    # Cleanup
    if "zlibrary" in sources:
        await sources["zlibrary"].close()
    if "annas_archive" in sources:
        sources["annas_archive"].close()
    cache.close()


if __name__ == "__main__":
    asyncio.run(main())
