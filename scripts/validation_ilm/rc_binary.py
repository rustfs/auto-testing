"""Resolve and identify the rc executable used by live validation."""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path


def resolve_rc_binary(selection: str) -> tuple[str, str]:
    """Return one canonical executable path and its SHA-256 digest."""

    located = shutil.which(selection)
    if located is None:
        raise FileNotFoundError(selection)
    resolved = Path(located).resolve(strict=True)
    digest = hashlib.sha256()
    with resolved.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return str(resolved), digest.hexdigest()
