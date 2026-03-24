"""Data models, ranking, and shared constants."""

import re
from dataclasses import dataclass, field


BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

DOWNLOAD_HEADERS = {"User-Agent": BROWSER_UA}

SOURCE_RANK = {"annas_archive": 3, "zlibrary": 2, "internet_archive": 1, "libgen": 0}


@dataclass
class BookResult:
    isbn: str
    title: str
    authors: str
    year: str
    language: str
    extension: str
    size: str
    download_url: str
    source: str  # "libgen", "zlibrary", "annas_archive", "internet_archive"
    source_metadata: dict = field(default_factory=dict, repr=False)

    def to_dict(self) -> dict:
        return {
            "isbn": self.isbn,
            "title": self.title,
            "authors": self.authors,
            "year": self.year,
            "language": self.language,
            "extension": self.extension,
            "size": self.size,
            "download_url": self.download_url,
            "source": self.source,
            "source_metadata": self.source_metadata,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "BookResult":
        return cls(**d)


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


def rank_candidates(candidates: list[BookResult]) -> list[BookResult]:
    """Rank candidates: prefer PDF, then Anna's Archive, then largest size."""
    if not candidates:
        return []

    def score(book: BookResult) -> tuple:
        is_pdf = 1 if book.extension == "pdf" else 0
        source_pref = SOURCE_RANK.get(book.source, 0)
        size_bytes = _parse_size(book.size)
        return (is_pdf, source_pref, size_bytes)

    return sorted(candidates, key=score, reverse=True)


def select_best(candidates: list[BookResult]) -> BookResult | None:
    """Select the best book match."""
    ranked = rank_candidates(candidates)
    return ranked[0] if ranked else None
