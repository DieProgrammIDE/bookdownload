#!/usr/bin/env python3
"""ISBN Book Downloader - Downloads books from shadow libraries by ISBN."""

import argparse
import asyncio
import json
import logging
import os
import re
import sys

import diskcache
from dotenv import load_dotenv

logging.getLogger("libgen_api_enhanced").setLevel(logging.ERROR)

from tracing import observe, get_client, flush_tracing  # noqa: E402
from discovery import run_discovery_phase  # noqa: E402
from downloader import run_download_phase  # noqa: E402
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
        help="Max parallel ISBN searches during discovery (default: 10)",
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

    # Update session span with metadata
    langfuse.update_current_span(
        input={
            "isbn_count": len(isbns),
            "sources": list(sources.keys()),
            "output_dir": args.output_dir,
            "discovery_only": args.discovery_only,
            "download_only": args.download_only,
        },
    )
    flush_tracing()

    try:
        # ------------------------------------------------------------------
        # Phase 1: Discovery
        # ------------------------------------------------------------------
        if not args.download_only:
            print(f"\n{'=' * 60}")
            print(
                f"Phase 1: Discovery  "
                f"[{len(isbns)} ISBNs, concurrency={args.discovery_concurrency}]"
            )
            print(f"{'=' * 60}")
            await run_discovery_phase(isbns, sources, cache, args.discovery_concurrency)
            flush_tracing()

        # Save discovery metadata
        discovery_data = {}
        for isbn in isbns:
            cached = cache.get(f"discovery:{isbn}")
            discovery_data[isbn] = {"candidates": cached if cached is not None else []}
        discovery_path = os.path.join(args.output_dir, "discovery.json")
        with open(discovery_path, "w") as f:
            json.dump(discovery_data, f, indent=2, ensure_ascii=False)
        print(f"\nDiscovery metadata written to {discovery_path}")

        # ------------------------------------------------------------------
        # Phase 2: Download
        # ------------------------------------------------------------------
        stats = {}
        results_log = {}
        if not args.discovery_only:
            print(f"\n{'=' * 60}")
            print(f"Phase 2: Download")
            print(f"{'=' * 60}")
            stats, results_log = await run_download_phase(
                isbns, sources, cache, args.output_dir, args.host_concurrency,
            )
            flush_tracing()

            not_found = [
                isbn for isbn, info in results_log.items()
                if info["status"] == "not_found"
            ]
            failed = [
                isbn for isbn, info in results_log.items()
                if info["status"] == "failed"
            ]

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
                    {
                        "stats": stats,
                        "results": results_log,
                        "not_found": not_found,
                        "failed": failed,
                    },
                    f, indent=2, ensure_ascii=False,
                )
            print(f"\nResults written to {results_path}")

            # Update session span with final stats
            langfuse.update_current_span(
                output={
                    "stats": stats,
                    "not_found_count": len(not_found),
                    "failed_count": len(failed),
                },
            )

    finally:
        # ------------------------------------------------------------------
        # Cleanup
        # ------------------------------------------------------------------
        if "zlibrary" in sources:
            await sources["zlibrary"].close()
        if "annas_archive" in sources:
            sources["annas_archive"].close()
        cache.close()
        flush_tracing()


if __name__ == "__main__":
    asyncio.run(main())
