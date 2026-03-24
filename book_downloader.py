#!/usr/bin/env python3
"""ISBN Book Downloader - Downloads books from shadow libraries by ISBN."""

import argparse
import asyncio
import json
import os
import re
import sys
import time

from dotenv import load_dotenv

from sources import BookResult, LibGenSource, ZLibrarySource


def load_isbns(filepath: str) -> list[str]:
    """Load and validate ISBNs from a file (one per line)."""
    isbns = []
    seen = set()
    with open(filepath, "r") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Strip hyphens and spaces
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
    return name[:100]  # cap length


def select_best(candidates: list[BookResult], any_format: bool = False) -> BookResult | None:
    """Select the best book match from candidates."""
    if not candidates:
        return None

    def score(book: BookResult) -> tuple:
        is_german = book.language in ("german", "deutsch", "de", "ger", "deu")
        is_pdf = book.extension == "pdf"
        allowed_format = is_pdf or (any_format and book.extension in ("epub", "djvu", "mobi"))

        if not allowed_format and not is_pdf:
            return (0, 0, 0, 0)

        # Parse size for comparison (e.g. "5 Mb" -> 5000000)
        size_bytes = _parse_size(book.size)

        if is_german and is_pdf:
            return (4, size_bytes, 1 if is_german else 0, 1 if is_pdf else 0)
        elif is_german and allowed_format:
            return (3, size_bytes, 1, 0)
        elif is_pdf:
            return (2, size_bytes, 0, 1)
        elif allowed_format:
            return (1, size_bytes, 0, 0)
        return (0, 0, 0, 0)

    scored = [(score(c), c) for c in candidates]
    scored = [(s, c) for s, c in scored if s[0] > 0]

    if not scored:
        # If no candidate matched format criteria, fall back to first candidate
        if any_format and candidates:
            return candidates[0]
        # Try returning any PDF
        pdfs = [c for c in candidates if c.extension == "pdf"]
        return pdfs[0] if pdfs else candidates[0]

    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]


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


def make_output_filename(isbn: str, book: BookResult) -> str:
    """Create output filename from ISBN and book metadata."""
    title_part = sanitize_filename(book.title) if book.title else "unknown"
    ext = book.extension or "pdf"
    return f"{isbn}_{title_part}.{ext}"


async def process_isbn(
    isbn: str,
    libgen: LibGenSource | None,
    zlib: ZLibrarySource | None,
    output_dir: str,
    any_format: bool,
    delay: float,
) -> str:
    """Process a single ISBN. Returns status: 'downloaded', 'exists', 'not_found', 'failed'."""

    # Check if already downloaded
    for f in os.listdir(output_dir):
        if f.startswith(isbn + "_"):
            print(f"  Already downloaded: {f}")
            return "exists"

    candidates = []

    # 1. Try LibGen first
    if libgen:
        print(f"  Searching LibGen...")
        try:
            results = libgen.search_isbn(isbn)
            if results:
                print(f"  LibGen: found {len(results)} result(s)")
                candidates.extend(results)
        except Exception as e:
            print(f"  LibGen search error: {e}")

    # 2. Try Z-Library as fallback
    if not candidates and zlib:
        print(f"  Searching Z-Library...")
        try:
            results = await zlib.search_isbn(isbn)
            if results:
                print(f"  Z-Library: found {len(results)} result(s)")
                candidates.extend(results)
        except Exception as e:
            print(f"  Z-Library search error: {e}")

    if not candidates:
        print(f"  Not found in any source")
        return "not_found"

    # Select best match
    best = select_best(candidates, any_format=any_format)
    if not best:
        print(f"  No suitable format found")
        return "not_found"

    print(f"  Selected: \"{best.title}\" by {best.authors} ({best.language}, {best.extension}, {best.size}) [{best.source}]")

    filename = make_output_filename(isbn, best)
    output_path = os.path.join(output_dir, filename)

    # Download
    print(f"  Downloading...")
    if best.source == "libgen":
        success = libgen.download(best, output_path)
    elif best.source == "zlibrary":
        success = await zlib.download(best, output_path)
    else:
        success = False

    if success:
        file_size = os.path.getsize(output_path)
        print(f"  Saved: {filename} ({file_size:,} bytes)")
        return "downloaded"
    else:
        print(f"  Download failed")
        return "failed"


async def main():
    parser = argparse.ArgumentParser(
        description="Download books by ISBN from shadow libraries"
    )
    parser.add_argument("isbn_file", help="Path to file with ISBNs (one per line)")
    parser.add_argument("-o", "--output-dir", default="downloads", help="Output directory (default: downloads/)")
    parser.add_argument("--delay", type=float, default=3.0, help="Seconds between requests (default: 3.0)")
    parser.add_argument("--libgen-mirror", default="li", help="LibGen mirror TLD: li, bz, gs (default: li)")
    parser.add_argument("--no-zlibrary", action="store_true", help="Skip Z-Library entirely")
    parser.add_argument("--any-format", action="store_true", help="Accept EPUB/DJVU if no PDF available")
    args = parser.parse_args()

    # Load .env for Z-Library credentials
    load_dotenv()

    # Load ISBNs
    if not os.path.isfile(args.isbn_file):
        print(f"Error: ISBN file not found: {args.isbn_file}")
        sys.exit(1)

    isbns = load_isbns(args.isbn_file)
    if not isbns:
        print("No valid ISBNs found in input file")
        sys.exit(1)

    print(f"Loaded {len(isbns)} ISBN(s)")

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Initialize sources
    libgen = None
    try:
        libgen = LibGenSource(mirror=args.libgen_mirror)
        print(f"LibGen initialized (mirror: .{args.libgen_mirror})")
    except Exception as e:
        print(f"Warning: LibGen initialization failed: {e}")

    zlib = None
    if not args.no_zlibrary:
        zl_email = os.getenv("ZLIBRARY_EMAIL")
        zl_password = os.getenv("ZLIBRARY_PASSWORD")
        if zl_email and zl_password:
            zlib = ZLibrarySource(zl_email, zl_password)
            # Check limits
            limits = await zlib.check_limits()
            if limits:
                print(f"Z-Library initialized (limits: {limits})")
            else:
                print("Z-Library initialized (could not check limits)")
        else:
            print("Z-Library: no credentials found (set ZLIBRARY_EMAIL and ZLIBRARY_PASSWORD in .env)")
    else:
        print("Z-Library: disabled via --no-zlibrary")

    if not libgen and not zlib:
        print("Error: No sources available. Cannot proceed.")
        sys.exit(1)

    # Process ISBNs
    stats = {"downloaded": 0, "exists": 0, "not_found": 0, "failed": 0}
    results_log = {}

    print(f"\nProcessing {len(isbns)} ISBN(s)...\n")

    for i, isbn in enumerate(isbns, 1):
        print(f"[{i}/{len(isbns)}] ISBN: {isbn}")
        status = await process_isbn(isbn, libgen, zlib, args.output_dir, args.any_format, args.delay)
        stats[status] += 1
        results_log[isbn] = status

        # Delay between requests (skip after last)
        if i < len(isbns) and status not in ("exists",):
            time.sleep(args.delay)

    # Cleanup
    if zlib:
        await zlib.close()

    # Summary
    print(f"\n{'='*50}")
    print(f"Summary:")
    print(f"  Downloaded: {stats['downloaded']}")
    print(f"  Already existed: {stats['exists']}")
    print(f"  Not found: {stats['not_found']}")
    print(f"  Failed: {stats['failed']}")
    print(f"  Total: {len(isbns)}")

    not_found = [isbn for isbn, s in results_log.items() if s == "not_found"]
    failed = [isbn for isbn, s in results_log.items() if s == "failed"]

    if not_found:
        print(f"\nISBNs not found ({len(not_found)}):")
        for isbn in not_found:
            print(f"  {isbn}")

    if failed:
        print(f"\nFailed downloads ({len(failed)}):")
        for isbn in failed:
            print(f"  {isbn}")

    # Write results.json
    results_path = os.path.join(args.output_dir, "results.json")
    with open(results_path, "w") as f:
        json.dump({
            "stats": stats,
            "results": results_log,
            "not_found": not_found,
            "failed": failed,
        }, f, indent=2)
    print(f"\nResults written to {results_path}")


if __name__ == "__main__":
    asyncio.run(main())
