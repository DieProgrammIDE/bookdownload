"""Phase 2: Download — download best candidate for each ISBN."""

import asyncio
import contextvars
import os
import re
import time
import threading

import diskcache
import requests

from models import DOWNLOAD_HEADERS, BookResult, rank_candidates
from tracing import observe, get_client, flush_tracing


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def log(isbn: str, msg: str):
    print(f"  [{isbn}] {msg}")


# ---------------------------------------------------------------------------
# File helpers
# ---------------------------------------------------------------------------

def sanitize_filename(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*]', "", name)
    name = re.sub(r"\s+", "_", name)
    name = name.strip("_.")
    return name[:100]


def make_output_filename(isbn: str, book: BookResult) -> str:
    title_part = sanitize_filename(book.title) if book.title else "unknown"
    ext = book.extension or "pdf"
    return f"{isbn}_{title_part}.{ext}"


def _file_exists_for_isbn(isbn: str, output_dir: str) -> str | None:
    try:
        for f in os.listdir(output_dir):
            if f.startswith(isbn + "_"):
                return f
    except OSError:
        pass
    return None


# ---------------------------------------------------------------------------
# File download
# ---------------------------------------------------------------------------

@observe(name="download-file-attempts", capture_input=False, capture_output=False)
def download_file(
    url: str,
    output_path: str,
    source: str,
    progress_cb=None,
    max_retries: int = 5,
) -> tuple[bool, str | None]:
    """Download a file. Returns (success, error_message).

    progress_cb(downloaded_bytes, total_bytes) is called every ~10s.
    """
    langfuse = get_client()
    langfuse.update_current_span(
        input={"url": url, "source": source, "max_retries": max_retries},
    )
    flush_tracing()

    for attempt in range(max_retries):
        attempt_cm = langfuse.start_as_current_observation(
            as_type="span",
            name=f"download-attempt-{attempt + 1}",
            input={"attempt": attempt + 1, "max_retries": max_retries},
        )
        attempt_obs = attempt_cm.__enter__()
        try:
            resp = requests.get(
                url,
                stream=True,
                timeout=(15, 120),
                headers=DOWNLOAD_HEADERS,
                allow_redirects=True,
            )

            if resp.status_code in (429, 503):
                wait = min(30 * 2**attempt, 300)
                print(
                    f"  [{source}] Rate limited "
                    f"(attempt {attempt + 1}/{max_retries}), waiting {wait}s..."
                )
                attempt_obs.update(
                    output={"status_code": resp.status_code, "action": "rate_limited", "wait_s": wait},
                    level="WARNING",
                )
                attempt_cm.__exit__(None, None, None)
                flush_tracing()
                time.sleep(wait)
                continue

            resp.raise_for_status()

            total_size = int(resp.headers.get("content-length", 0))
            downloaded = 0
            last_report = time.time()
            with open(output_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
                    downloaded += len(chunk)
                    now = time.time()
                    if progress_cb and now - last_report >= 10:
                        progress_cb(downloaded, total_size)
                        last_report = now

            file_size = os.path.getsize(output_path)
            if file_size == 0:
                os.remove(output_path)
                attempt_obs.update(output={"status_code": resp.status_code, "error": "empty file"}, level="ERROR")
                attempt_cm.__exit__(None, None, None)
                flush_tracing()
                langfuse.update_current_span(output={"success": False, "error": "downloaded file is empty", "attempts": attempt + 1})
                flush_tracing()
                return False, "downloaded file is empty"

            if output_path.lower().endswith(".pdf"):
                with open(output_path, "rb") as f:
                    header = f.read(5)
                if header != b"%PDF-":
                    os.remove(output_path)
                    err = f"not a valid PDF (header: {header!r})"
                    attempt_obs.update(output={"status_code": resp.status_code, "error": err}, level="ERROR")
                    attempt_cm.__exit__(None, None, None)
                    flush_tracing()
                    langfuse.update_current_span(output={"success": False, "error": err, "attempts": attempt + 1})
                    flush_tracing()
                    return False, err

            attempt_obs.update(output={"status_code": resp.status_code, "downloaded_bytes": downloaded, "total_bytes": total_size})
            attempt_cm.__exit__(None, None, None)
            flush_tracing()
            langfuse.update_current_span(output={"success": True, "attempts": attempt + 1, "downloaded_bytes": downloaded})
            flush_tracing()
            return True, None

        except requests.exceptions.RequestException as e:
            action = "retry" if attempt < max_retries - 1 else "give_up"
            attempt_obs.update(output={"error": str(e), "action": action}, level="ERROR", status_message=str(e))
            attempt_cm.__exit__(None, None, None)
            flush_tracing()
            if attempt < max_retries - 1:
                wait = min(5 * 2**attempt, 120)
                print(
                    f"  [{source}] Download error "
                    f"(attempt {attempt + 1}/{max_retries}): {e}, "
                    f"retrying in {wait}s..."
                )
                time.sleep(wait)
            else:
                langfuse.update_current_span(output={"success": False, "error": str(e), "attempts": attempt + 1})
                flush_tracing()
                return False, str(e)

    langfuse.update_current_span(output={"success": False, "error": "max retries exceeded", "attempts": max_retries})
    flush_tracing()
    return False, "max retries exceeded"


# ---------------------------------------------------------------------------
# Per-ISBN download with candidate fallback
# ---------------------------------------------------------------------------

async def _do_download(isbn, url, candidate, output_dir, active, download_metrics):
    """Download file, tracking progress in `active` dict. Returns (success, error)."""
    filename = make_output_filename(isbn, candidate)
    output_path = os.path.join(output_dir, filename)

    active[isbn] = {
        "title": candidate.title[:50],
        "source": candidate.source,
        "downloaded": 0,
        "total": 0,
    }

    # Create a Langfuse observation for this download (Pattern B: thread-safe)
    langfuse = get_client()
    observation = langfuse.start_observation(
        as_type="span",
        name="download-file",
        input={
            "isbn": isbn,
            "source": candidate.source,
            "url": url,
            "filename": filename,
        },
        trace_context={
            "trace_id": langfuse.get_current_trace_id(),
            "parent_span_id": langfuse.get_current_observation_id(),
        },
    )
    flush_tracing()

    download_start = time.time()
    last_flush = time.time()

    def on_progress(downloaded, total, _isbn=isbn):
        nonlocal last_flush
        active[_isbn] = {
            **active.get(_isbn, {}),
            "downloaded": downloaded,
            "total": total,
        }
        mb = downloaded / 1048576
        if total:
            log(_isbn, f"{mb:.1f}/{total / 1048576:.1f} MB")
        else:
            log(_isbn, f"{mb:.1f} MB")

        # Update Langfuse observation with live progress (flush every 30s)
        now = time.time()
        elapsed = now - download_start
        speed_mbps = (downloaded / 1048576) / elapsed if elapsed > 0 else 0
        observation.update(
            metadata={
                "downloaded_mb": round(downloaded / 1048576, 2),
                "total_mb": round(total / 1048576, 2) if total else None,
                "speed_mbps": round(speed_mbps, 2),
                "elapsed_s": round(elapsed, 1),
            },
        )
        if now - last_flush >= 30:
            flush_tracing()
            last_flush = now

    # Use max_retries=7 for Anna's Archive, 5 for others
    retries = 7 if candidate.source == "annas_archive" else 5

    ctx = contextvars.copy_context()
    success, error = await asyncio.to_thread(
        ctx.run, download_file, url, output_path, candidate.source, on_progress, retries,
    )

    active.pop(isbn, None)

    # Finalize the observation
    elapsed = time.time() - download_start
    if success:
        file_size = os.path.getsize(output_path)
        speed_mbps = (file_size / 1048576) / elapsed if elapsed > 0 else 0
        log(isbn, f"saved {filename} ({file_size:,} bytes)")
        observation.update(
            output={
                "success": True,
                "file_size_mb": round(file_size / 1048576, 2),
                "duration_s": round(elapsed, 1),
                "avg_speed_mbps": round(speed_mbps, 2),
            },
        )
        # Track aggregate metrics
        download_metrics["total_bytes"] += file_size
        download_metrics["completed"] += 1
    else:
        observation.update(
            output={"success": False, "error": error, "duration_s": round(elapsed, 1)},
            level="ERROR",
            status_message=error,
        )
        download_metrics["failed"] += 1

    observation.end()
    flush_tracing()

    return success, error


@observe(name="download-isbn", capture_input=False, capture_output=False)
async def download_one(
    isbn: str,
    ranked: list[BookResult],
    sources: dict,
    output_dir: str,
    source_sems: dict,
    active: dict,
    download_metrics: dict,
) -> tuple[str, BookResult | None]:
    """Download best match for isbn, trying candidates in ranked order."""
    langfuse = get_client()
    langfuse.update_current_span(
        input={
            "isbn": isbn,
            "candidate_count": len(ranked),
            "candidates": [c.to_dict() for c in ranked],
        },
    )

    for i, candidate in enumerate(ranked):
        tag = f"[{i + 1}/{len(ranked)}]"
        log(isbn, f"{tag} {candidate.source} {candidate.extension} {candidate.size}")

        if candidate.source == "annas_archive" and candidate.source_metadata.get("hash"):
            annas = sources["annas_archive"]
            server = annas.next_server()
            async with annas.server_sem(server):
                log(isbn, f"{tag} extracting URL (server {server})...")
                url = await annas.extract_url(
                    candidate.source_metadata["hash"], server,
                )
                if not url:
                    log(isbn, f"{tag} no download link")
                    continue
                success, error = await _do_download(
                    isbn, url, candidate, output_dir, active, download_metrics,
                )
        else:
            url = candidate.download_url
            if not url:
                log(isbn, f"{tag} no download URL")
                continue
            sem = source_sems.get(candidate.source, asyncio.Semaphore(3))
            async with sem:
                success, error = await _do_download(
                    isbn, url, candidate, output_dir, active, download_metrics,
                )

        if success:
            langfuse.update_current_span(
                output={
                    "status": "downloaded",
                    "source": candidate.source,
                    "candidate_index": i + 1,
                    "selected": candidate.to_dict(),
                },
            )
            flush_tracing()
            return "downloaded", candidate
        log(isbn, f"{tag} failed: {error}")

    langfuse.update_current_span(
        output={
            "status": "failed",
            "candidates_tried": len(ranked),
            "candidates_sources": [c.source for c in ranked],
        },
        level="WARNING",
    )
    flush_tracing()
    return "failed", ranked[0] if ranked else None


# ---------------------------------------------------------------------------
# Download phase orchestrator
# ---------------------------------------------------------------------------

DEFAULT_CONCURRENCY = {
    "libgen": 3,
    "zlibrary": 3,
    "internet_archive": 5,
}


@observe(name="download-phase", capture_input=False, capture_output=False)
async def run_download_phase(
    isbns: list[str],
    sources: dict,
    cache: diskcache.Cache,
    output_dir: str,
    host_concurrency: int | None = None,
) -> tuple[dict, dict]:
    langfuse = get_client()
    langfuse.update_current_span(
        input={"isbn_count": len(isbns), "sources": list(sources.keys())},
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
    active: dict = {}  # isbn -> progress info for status reporter

    # Aggregate download metrics for Langfuse (thread-safe via GIL for simple increments)
    download_metrics = {"total_bytes": 0, "completed": 0, "failed": 0}

    async def process(isbn: str, idx: int):
        existing = _file_exists_for_isbn(isbn, output_dir)
        if existing:
            with stats_lock:
                stats["exists"] += 1
            return isbn, "exists", None

        discovery_key = f"discovery:{isbn}"
        cached_discovery = cache.get(discovery_key)
        if cached_discovery is None:
            with stats_lock:
                stats["not_found"] += 1
            return isbn, "not_found", None

        candidates = [BookResult.from_dict(d) for d in cached_discovery]
        ranked = rank_candidates(candidates)
        if not ranked:
            with stats_lock:
                stats["not_found"] += 1
            return isbn, "not_found", None

        log(isbn, f"[{idx}/{total}] {len(ranked)} candidate(s)")
        status, selected = await download_one(
            isbn, ranked, sources, output_dir, source_sems, active, download_metrics,
        )
        with stats_lock:
            stats[status] = stats.get(status, 0) + 1
        return isbn, status, selected

    # Status reporter
    start_time = asyncio.get_event_loop().time()
    phase_start = time.time()

    async def status_reporter():
        while True:
            await asyncio.sleep(30)
            elapsed = asyncio.get_event_loop().time() - start_time
            done = sum(stats.values())
            snapshot = dict(active)

            # Compute aggregate metrics
            total_mb = download_metrics["total_bytes"] / 1048576
            wall_elapsed = time.time() - phase_start
            avg_speed = total_mb / wall_elapsed if wall_elapsed > 0 else 0

            print(
                f"\n=== Status [{done}/{total}] {elapsed:.0f}s "
                f"| downloaded: {stats['downloaded']} "
                f"| failed: {stats['failed']} "
                f"| not_found: {stats['not_found']} "
                f"| {len(snapshot)} active ==="
            )
            for isbn, info in snapshot.items():
                mb = info["downloaded"] / 1048576
                total_mb_item = info["total"] / 1048576 if info["total"] else 0
                title = info.get("title", "")
                if total_mb_item:
                    print(f"  [{isbn}] {mb:.1f}/{total_mb_item:.1f} MB — {title}")
                elif mb:
                    print(f"  [{isbn}] {mb:.1f} MB — {title}")
                else:
                    print(f"  [{isbn}] starting — {title}")

            # Update Langfuse with live aggregate metrics
            langfuse.update_current_span(
                metadata={
                    "total_mb_downloaded": round(total_mb, 2),
                    "avg_speed_mbps": round(avg_speed, 3),
                    "active_downloads": len(snapshot),
                    "completed": download_metrics["completed"],
                    "failed": download_metrics["failed"],
                    "elapsed_s": round(wall_elapsed, 1),
                },
            )
            flush_tracing()

    reporter = asyncio.create_task(status_reporter())
    tasks = [process(isbn, i + 1) for i, isbn in enumerate(isbns)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    reporter.cancel()

    for r in results:
        if isinstance(r, Exception):
            print(f"  [error] Download exception: {r}")
            stats["failed"] += 1
        else:
            isbn, status, selected = r
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
        },
    )
    flush_tracing()

    return stats, results_log
