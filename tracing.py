"""Langfuse tracing bootstrap — always enabled."""

import os

from dotenv import load_dotenv

load_dotenv()

# Langfuse SDK reads from os.environ. dotenv already loaded them.
# Set default base URL if not provided.
os.environ.setdefault("LANGFUSE_BASE_URL", "https://cloud.langfuse.com")

from langfuse import observe, get_client  # noqa: E402


def flush_tracing():
    """Flush buffered Langfuse data immediately."""
    try:
        client = get_client()
        if client is not None:
            client.flush()
    except Exception:
        pass
