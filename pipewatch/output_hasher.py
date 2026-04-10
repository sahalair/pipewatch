"""Utilities for hashing pipeline output data for change detection."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

ALGORITHM = "sha256"


def hash_text(text: str) -> str:
    """Return the SHA-256 hex digest of a UTF-8 string."""
    return hashlib.new(ALGORITHM, text.encode("utf-8")).hexdigest()


def hash_file(file_path: Path) -> str:
    """Return the SHA-256 hex digest of a file's contents."""
    h = hashlib.new(ALGORITHM)
    with file_path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def hash_json_serializable(obj: Any) -> str:
    """Return a stable SHA-256 digest of any JSON-serialisable object."""
    canonical = json.dumps(obj, sort_keys=True, ensure_ascii=False)
    return hash_text(canonical)


def hashes_differ(hash_a: str, hash_b: str) -> bool:
    """Return True when two hash strings are not equal."""
    return hash_a != hash_b
