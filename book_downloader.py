#!/usr/bin/env python3
"""ISBN Book Downloader - Downloads books from shadow libraries by ISBN."""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
import threading

import diskcache
from dotenv import load_dotenv

logging.getLogger("libgen_api_enhanced").setLevel(logging.ERROR)

from tracing import observe, get_client, flush_tracing  # noqa: E402
from discovery import discover_isbn  # noqa: E402
from downloader import (  # noqa: E402
    download_one, file_exists_for_isbn, log, DEFAULT_CONCURRENCY,
)
from models import BookResult, rank_candidates  # noqa: E402
from sources import AnnasArchiveSource, InternetArchiveSource, LibGenSource, ZLibrarySource  # noqa: E402


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


# ---------------------------------------------------------------------------
# Per-ISBN pipeline (discovery + download under one span)
# ---------------------------------------------------------------------------

@observe(name="isbn", capture_input=False, capture_output=False)
async def process_isbn(
    isbn: str,
    idx: int,
    total: int,
    sources: dict,
    cache: diskcache.Cache,
    output_dir: str,
    source_sems: dict,
    download_metrics: dict,
    discovery_only: bool,
    download_only: bool,
) -> tuple[str, str, BookResult | None]:
    """Process a single ISBN: discover then download. Returns (isbn, status, selected)."""
    langfuse = get_client()
    langfuse.update_current_span(name=f"isbn-{isbn}", input={"isbn": isbn, "index": idx, "total": total})
    flush_tracing()

    # Step 1: Discovery
    candidates = []
    if not download_only:
        candidates, log_lines = await discover_isbn(isbn, sources, cache)
        output = f"\n[{idx}/{total}] {isbn}"
        if log_lines:
            output += "\n" + "\n".join(log_lines)
        print(output)

    # Step 2: Download
    if discovery_only:
        langfuse.update_current_span(
            output={"status": "discovery_only", "candidate_count": len(candidates)},
        )
        flush_tracing()
        return isbn, "discovery_only", None

    # Check if file already exists
    existing = file_exists_for_isbn(isbn, output_dir)
    if existing:
        langfuse.update_current_span(
            output={"status": "exists", "existing_file": existing},
        )
        flush_tracing()
        return isbn, "exists", None

    # Load candidates from cache (covers both fresh discovery and download-only mode)
    cached_discovery = cache.get(f"discovery:{isbn}")
    if cached_discovery is None:
        langfuse.update_current_span(
            output={"status": "not_found", "reason": "no discovery results"},
        )
        flush_tracing()
        return isbn, "not_found", None

    ranked = rank_candidates([BookResult.from_dict(d) for d in cached_discovery])
    if not ranked:
        langfuse.update_current_span(
            output={"status": "not_found", "reason": "no rankable candidates"},
        )
        flush_tracing()
        return isbn, "not_found", None

    log(isbn, f"[{idx}/{total}] {len(ranked)} candidate(s)")
    status, selected = await download_one(
        isbn, ranked, sources, output_dir, source_sems, download_metrics,
    )

    langfuse.update_current_span(
        output={
            "status": status,
            "selected": selected.to_dict() if selected else None,
        },
    )
    flush_tracing()
    return isbn, status, selected


# ---------------------------------------------------------------------------
# Unified pipeline
# ---------------------------------------------------------------------------

@observe(name="pipeline", capture_input=False, capture_output=False)
async def run_pipeline(
    isbns: list[str],
    sources: dict,
    cache: diskcache.Cache,
    output_dir: str,
    concurrency: int,
    host_concurrency: int | None = None,
    discovery_only: bool = False,
    download_only: bool = False,
) -> tuple[dict, dict]:
    langfuse = get_client()
    langfuse.update_current_span(
        input={
            "isbn_count": len(isbns),
            "isbns": isbns,
            "sources": list(sources.keys()),
            "concurrency": concurrency,
            "discovery_only": discovery_only,
            "download_only": download_only,
        },
    )
    flush_tracing()

    # Build per-source semaphores
    source_sems = {}
    for name, default in DEFAULT_CONCURRENCY.items():
        n = host_concurrency if host_concurrency is not None else default
        source_sems[name] = asyncio.Semaphore(n)

    # Anna's Archive per-server semaphores
    if "annas_archive" in sources:
        per_server = host_concurrency if host_concurrency is not None else 1
        sources["annas_archive"].init_semaphores(per_server)

    total = len(isbns)
    stats = {"downloaded": 0, "exists": 0, "not_found": 0, "failed": 0}
    stats_lock = threading.Lock()
    results_log: dict = {}
    download_metrics = {"total_bytes": 0, "completed": 0, "failed": 0}

    semaphore = asyncio.Semaphore(concurrency)

    async def bounded(isbn: str, idx: int):
        async with semaphore:
            return await process_isbn(
                isbn, idx, total, sources, cache, output_dir,
                source_sems, download_metrics,
                discovery_only, download_only,
            )

    phase_start = time.time()

    tasks = [bounded(isbn, i + 1) for i, isbn in enumerate(isbns)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, Exception):
            print(f"  [error] Pipeline exception: {r}")
            with stats_lock:
                stats["failed"] += 1
        else:
            isbn, status, selected = r
            if status != "discovery_only":
                with stats_lock:
                    stats[status] = stats.get(status, 0) + 1
            results_log[isbn] = {
                "status": status,
                "selected": selected.to_dict() if selected else None,
            }

    # Final span update
    wall_elapsed = time.time() - phase_start
    total_mb = download_metrics["total_bytes"] / 1048576
    avg_speed = total_mb / wall_elapsed if wall_elapsed > 0 else 0
    langfuse.update_current_span(
        output={
            "stats": stats,
            "total_mb_downloaded": round(total_mb, 2),
            "avg_speed_mbps": round(avg_speed, 3),
            "duration_s": round(wall_elapsed, 1),
            "results": results_log,
        },
    )
    flush_tracing()

    return stats, results_log


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

@observe(name="book-downloader-session", capture_input=False, capture_output=False)
async def main():
    langfuse = get_client()

    parser = argparse.ArgumentParser(
        description="Download books by ISBN from shadow libraries"
    )
    parser.add_argument("isbn_file", help="Path to file with ISBNs (one per line)")
    parser.add_argument(
        "-o", "--output-dir", default="downloads",
        help="Output directory (default: downloads/)",
    )
    parser.add_argument(
        "--libgen-mirror", default="li",
        help="LibGen mirror TLD: li, bz, gs (default: li)",
    )
    parser.add_argument(
        "--annas-mirror", default="annas-archive.gl",
        help="Anna's Archive mirror domain (default: annas-archive.gl)",
    )

    # Source toggles
    parser.add_argument("--no-libgen", action="store_true", help="Skip LibGen")
    parser.add_argument("--no-zlibrary", action="store_true", help="Skip Z-Library")
    parser.add_argument("--no-annas", action="store_true", help="Skip Anna's Archive")
    parser.add_argument(
        "--no-internet-archive", action="store_true", help="Skip Internet Archive",
    )

    # Cache
    parser.add_argument(
        "--clear-cache", action="store_true", help="Clear cache and start fresh",
    )

    # Phase control
    parser.add_argument(
        "--discovery-only", action="store_true", help="Run discovery phase only",
    )
    parser.add_argument(
        "--download-only", action="store_true",
        help="Run download phase only (requires cached discovery)",
    )

    # Concurrency
    parser.add_argument(
        "--discovery-concurrency", type=int, default=10,
        help="Max parallel ISBN processing (default: 10)",
    )
    parser.add_argument(
        "--host-concurrency", type=int, default=None,
        help="Override per-host download limit for all sources "
             "(default: AA=1/server, ZLib=3, LibGen=3, IA=5)",
    )

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

    # ------------------------------------------------------------------
    # Initialize sources
    # ------------------------------------------------------------------
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
            print(
                "Z-Library: no credentials found "
                "(set ZLIBRARY_EMAIL/ZLIBRARY_PASSWORD in .env)"
            )

    if not sources:
        print("Error: No sources available.")
        sys.exit(1)

    # Update session span
    langfuse.update_current_span(
        input={
            "isbn_count": len(isbns),
            "isbns": isbns,
            "sources": list(sources.keys()),
            "output_dir": args.output_dir,
            "discovery_only": args.discovery_only,
            "download_only": args.download_only,
        },
    )
    flush_tracing()

    try:
        # ------------------------------------------------------------------
        # Run unified pipeline
        # ------------------------------------------------------------------
        print(f"\n{'=' * 60}")
        if args.discovery_only:
            print(f"Pipeline: Discovery only [{len(isbns)} ISBNs]")
        elif args.download_only:
            print(f"Pipeline: Download only [{len(isbns)} ISBNs]")
        else:
            print(f"Pipeline: Discover + Download [{len(isbns)} ISBNs]")
        print(f"{'=' * 60}")

        stats, results_log = await run_pipeline(
            isbns, sources, cache, args.output_dir,
            args.discovery_concurrency, args.host_concurrency,
            args.discovery_only, args.download_only,
        )

        # Save discovery metadata
        discovery_data = {}
        for isbn in isbns:
            cached = cache.get(f"discovery:{isbn}")
            discovery_data[isbn] = {"candidates": cached if cached is not None else []}
        discovery_path = os.path.join(args.output_dir, "discovery.json")
        with open(discovery_path, "w") as f:
            json.dump(discovery_data, f, indent=2, ensure_ascii=False)
        print(f"\nDiscovery metadata written to {discovery_path}")

        # Summary
        if not args.discovery_only:
            not_found = [isbn for isbn, info in results_log.items() if info["status"] == "not_found"]
            failed = [isbn for isbn, info in results_log.items() if info["status"] == "failed"]

            print(f"\n{'=' * 60}")
            print("Summary:")
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
                json.dump(
                    {"stats": stats, "results": results_log, "not_found": not_found, "failed": failed},
                    f, indent=2, ensure_ascii=False,
                )
            print(f"\nResults written to {results_path}")

            langfuse.update_current_span(
                output={
                    "stats": stats,
                    "not_found_count": len(not_found),
                    "failed_count": len(failed),
                    "results": results_log,
                },
            )

    finally:
        if "zlibrary" in sources:
            await sources["zlibrary"].close()
        if "annas_archive" in sources:
            sources["annas_archive"].close()
        cache.close()
        flush_tracing()


if __name__ == "__main__":
    asyncio.run(main())
