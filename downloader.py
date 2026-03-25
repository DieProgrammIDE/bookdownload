"""Download functions — download best candidate for each ISBN."""

import asyncio
import contextvars
import os
import re
import time

import requests

from models import DOWNLOAD_HEADERS, BookResult
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


def file_exists_for_isbn(isbn: str, output_dir: str) -> str | None:
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

def download_file(
    url: str,
    output_path: str,
    source: str,
    progress_cb=None,
    max_retries: int = 5,
    max_download_time: int = 1800,
) -> tuple[bool, str | None]:
    """Download a file. Returns (success, error_message).

    progress_cb(downloaded_bytes, total_bytes) is called every ~10s.
    max_download_time caps total wall-clock time (default 30 min).
    """
    langfuse = get_client()

    # Explicit span management (not @observe) so it's visible immediately
    file_cm = langfuse.start_as_current_observation(
        as_type="span",
        name="download-file",
        input={"url": url, "source": source, "max_retries": max_retries},
    )
    file_obs = file_cm.__enter__()
    flush_tracing()
    download_start = time.time()

    try:
        for attempt in range(max_retries):
            attempt_cm = langfuse.start_as_current_observation(
                as_type="span",
                name=f"download-attempt-{attempt + 1}",
                input={"attempt": attempt + 1, "max_retries": max_retries},
            )
            attempt_obs = attempt_cm.__enter__()
            flush_tracing()
            try:
                resp = requests.get(
                    url,
                    stream=True,
                    timeout=(15, 120),
                    headers=DOWNLOAD_HEADERS,
                    allow_redirects=True,
                )

                # Non-retryable HTTP errors — fail immediately
                if resp.status_code in (401, 403, 404):
                    error = f"{resp.status_code} {resp.reason} for url: {url}"
                    attempt_obs.update(
                        output={"status_code": resp.status_code, "error": error},
                        level="ERROR", status_message=error,
                    )
                    attempt_cm.__exit__(None, None, None)
                    flush_tracing()
                    file_obs.update(output={"success": False, "error": error, "attempts": attempt + 1})
                    flush_tracing()
                    return False, error

                if resp.status_code in (429, 503):
                    wait = min(30 * 2**attempt, 300)
                    msg = f"Rate limited ({resp.status_code}), waiting {wait}s"
                    print(
                        f"  [{source}] {msg} "
                        f"(attempt {attempt + 1}/{max_retries})..."
                    )
                    attempt_obs.update(
                        output={"status_code": resp.status_code, "action": "rate_limited", "wait_s": wait},
                        level="WARNING", status_message=msg,
                    )
                    attempt_cm.__exit__(None, None, None)
                    flush_tracing()
                    time.sleep(wait)
                    continue

                resp.raise_for_status()

                total_size = int(resp.headers.get("content-length", 0))
                downloaded = 0
                last_report = time.time()
                last_span_update = time.time()
                with open(output_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536):
                        f.write(chunk)
                        downloaded += len(chunk)
                        now = time.time()

                        # Total download timeout
                        if now - download_start > max_download_time:
                            if os.path.exists(output_path):
                                os.remove(output_path)
                            error = f"download timeout ({max_download_time}s)"
                            attempt_obs.update(
                                output={"error": error, "downloaded_mb": round(downloaded / 1048576, 2)},
                                level="ERROR", status_message=error,
                            )
                            attempt_cm.__exit__(None, None, None)
                            flush_tracing()
                            file_obs.update(output={"success": False, "error": error})
                            flush_tracing()
                            return False, error

                        if progress_cb and now - last_report >= 10:
                            progress_cb(downloaded, total_size)
                            last_report = now
                        if now - last_span_update >= 30:
                            elapsed = now - download_start
                            speed = (downloaded / 1048576) / elapsed if elapsed > 0 else 0
                            attempt_obs.update(output={
                                "status": "downloading",
                                "downloaded_mb": round(downloaded / 1048576, 2),
                                "total_mb": round(total_size / 1048576, 2) if total_size else None,
                                "speed_mbps": round(speed, 2),
                                "elapsed_s": round(elapsed, 1),
                            })
                            flush_tracing()
                            last_span_update = now

                file_size = os.path.getsize(output_path)
                if file_size == 0:
                    os.remove(output_path)
                    attempt_obs.update(
                        output={"status_code": resp.status_code, "error": "empty file"},
                        level="ERROR", status_message="downloaded file is empty",
                    )
                    attempt_cm.__exit__(None, None, None)
                    flush_tracing()
                    file_obs.update(output={"success": False, "error": "downloaded file is empty", "attempts": attempt + 1})
                    flush_tracing()
                    return False, "downloaded file is empty"

                if output_path.lower().endswith(".pdf"):
                    with open(output_path, "rb") as f:
                        header = f.read(5)
                    if header != b"%PDF-":
                        os.remove(output_path)
                        err = f"not a valid PDF (header: {header!r})"
                        attempt_obs.update(
                            output={"status_code": resp.status_code, "error": err},
                            level="ERROR", status_message=err,
                        )
                        attempt_cm.__exit__(None, None, None)
                        flush_tracing()
                        file_obs.update(output={"success": False, "error": err, "attempts": attempt + 1})
                        flush_tracing()
                        return False, err

                attempt_obs.update(output={
                    "status": "completed",
                    "status_code": resp.status_code,
                    "downloaded_bytes": downloaded,
                    "total_bytes": total_size,
                })
                attempt_cm.__exit__(None, None, None)
                flush_tracing()
                elapsed = time.time() - download_start
                speed = (file_size / 1048576) / elapsed if elapsed > 0 else 0
                file_obs.update(output={
                    "success": True,
                    "attempts": attempt + 1,
                    "file_size_mb": round(file_size / 1048576, 2),
                    "duration_s": round(elapsed, 1),
                    "avg_speed_mbps": round(speed, 2),
                })
                flush_tracing()
                return True, None

            except requests.exceptions.RequestException as e:
                error_str = str(e)

                # IncompleteRead — bail immediately, caller will rotate server
                if "IncompleteRead" in error_str:
                    if os.path.exists(output_path):
                        os.remove(output_path)
                    attempt_obs.update(
                        output={"error": error_str, "action": "incomplete_read_bail"},
                        level="ERROR", status_message=error_str,
                    )
                    attempt_cm.__exit__(None, None, None)
                    flush_tracing()
                    file_obs.update(output={"success": False, "error": error_str, "attempts": attempt + 1})
                    flush_tracing()
                    return False, error_str

                action = "retry" if attempt < max_retries - 1 else "give_up"
                attempt_obs.update(output={"error": error_str, "action": action}, level="ERROR", status_message=error_str)
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
                    file_obs.update(output={"success": False, "error": error_str, "attempts": attempt + 1})
                    flush_tracing()
                    return False, error_str

        file_obs.update(output={
            "success": False, "error": "max retries exceeded", "attempts": max_retries,
        })
        flush_tracing()
        return False, "max retries exceeded"

    finally:
        file_cm.__exit__(None, None, None)
        flush_tracing()


# ---------------------------------------------------------------------------
# Per-ISBN download with candidate fallback
# ---------------------------------------------------------------------------

async def _do_download(isbn, url, candidate, output_dir, download_metrics):
    """Download file. Returns (success, error)."""
    filename = make_output_filename(isbn, candidate)
    output_path = os.path.join(output_dir, filename)

    def on_progress(downloaded, total, _isbn=isbn):
        mb = downloaded / 1048576
        if total:
            log(_isbn, f"{mb:.1f}/{total / 1048576:.1f} MB")
        else:
            log(_isbn, f"{mb:.1f} MB")

    # Anna's Archive URLs expire fast — keep retries low, server rotation handles the rest
    retries = 2 if candidate.source == "annas_archive" else 5

    ctx = contextvars.copy_context()
    success, error = await asyncio.to_thread(
        ctx.run, download_file, url, output_path, candidate.source, on_progress, retries,
    )

    if success:
        file_size = os.path.getsize(output_path)
        log(isbn, f"saved {filename} ({file_size:,} bytes)")
        download_metrics["total_bytes"] += file_size
        download_metrics["completed"] += 1
    else:
        download_metrics["failed"] += 1

    return success, error


@observe(name="download-isbn", capture_input=False, capture_output=False)
async def download_one(
    isbn: str,
    ranked: list[BookResult],
    sources: dict,
    output_dir: str,
    source_sems: dict,
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
    flush_tracing()

    for i, candidate in enumerate(ranked):
        tag = f"[{i + 1}/{len(ranked)}]"
        log(isbn, f"{tag} {candidate.source} {candidate.extension} {candidate.size}")

        if candidate.source == "annas_archive" and candidate.source_metadata.get("hash"):
            annas = sources["annas_archive"]
            md5_hash = candidate.source_metadata["hash"]
            max_server_tries = len(annas.SERVERS)

            success, error = False, "no servers tried"
            for server_try in range(max_server_tries):
                server = annas.next_server()
                async with annas.server_sem(server):
                    log(isbn, f"{tag} extracting URL (server {server})...")
                    url = await annas.extract_url(md5_hash, server)
                    if not url:
                        log(isbn, f"{tag} no download link from server {server}")
                        continue
                    success, error = await _do_download(
                        isbn, url, candidate, output_dir, download_metrics,
                    )
                if success:
                    break
                log(isbn, f"{tag} server {server} failed: {error}, trying next server...")
                continue  # rotate to next server with fresh URL
        else:
            url = candidate.download_url
            if not url:
                log(isbn, f"{tag} no download URL")
                continue
            sem = source_sems.get(candidate.source, asyncio.Semaphore(3))
            async with sem:
                success, error = await _do_download(
                    isbn, url, candidate, output_dir, download_metrics,
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
        status_message=f"all {len(ranked)} candidates failed",
    )
    flush_tracing()
    return "failed", ranked[0] if ranked else None


DEFAULT_CONCURRENCY = {
    "libgen": 3,
    "zlibrary": 3,
    "internet_archive": 5,
}
