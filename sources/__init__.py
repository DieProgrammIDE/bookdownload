"""Source backends for book search and download."""

from .annas_archive import AnnasArchiveSource
from .internet_archive import InternetArchiveSource
from .libgen import LibGenSource
from .zlibrary import ZLibrarySource

__all__ = [
    "AnnasArchiveSource",
    "InternetArchiveSource",
    "LibGenSource",
    "ZLibrarySource",
]
