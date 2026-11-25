"""Deterministic S3 keys for comparing batch-delete key length and depth."""

from __future__ import annotations

KEY_SHAPES = ("short", "short-shallow", "long-shallow", "long-deep")


def long_key_bytes(shape: str, key_bytes: int | None = None) -> int | None:
    """Resolve the length override before starting a run or constructing its keys."""
    if shape not in KEY_SHAPES:
        raise ValueError(f"unknown key shape: {shape!r}")
    if not shape.startswith("long-"):
        if key_bytes is not None:
            raise ValueError("--key-bytes requires long-shallow or long-deep")
        return None
    if key_bytes is None:
        return 1024
    if type(key_bytes) is not int or not 128 <= key_bytes <= 1024:
        raise ValueError("--key-bytes must be an integer between 128 and 1024")
    return key_bytes


def make_keys(count: int, shape: str, key_bytes: int | None = None) -> list[str]:
    """Return up to one DeleteObjects batch of keys, numbered 0000 through 0999."""
    if type(count) is not int or not 0 <= count <= 1000:
        raise ValueError("count must be an integer between 0 and 1000")
    length = long_key_bytes(shape, key_bytes)

    prefix = "target/"
    if shape == "short-shallow":
        prefix += "k/k/k/"
    elif length is not None:
        depth = 4 if shape == "long-shallow" else 32
        # Reserve the four-digit leaf suffix first: a short, deep layout may
        # have fewer than four bytes per component on average.
        available = length - len(prefix) - (depth - 1)
        leaf = max(4, available // depth)
        width, extra = divmod(available - leaf, depth - 1)
        directories = ["k" * (width + (index < extra)) for index in range(depth - 1)]
        leaf_padding = "k" * (leaf - 4)
        prefix += "/".join([*directories, leaf_padding])

    return [f"{prefix}{index:04d}" for index in range(count)]
