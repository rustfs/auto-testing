"""Deterministic S3 keys for comparing batch-delete key length and depth."""

KEY_SHAPES = ("short", "short-shallow", "long-shallow", "long-deep")


def make_keys(count: int, shape: str) -> list[str]:
    """Return up to one DeleteObjects batch of keys, numbered 0000 through 0999."""
    if type(count) is not int or not 0 <= count <= 1000:
        raise ValueError("count must be an integer between 0 and 1000")
    if shape not in KEY_SHAPES:
        raise ValueError(f"unknown key shape: {shape!r}")

    prefix = "target/"
    if shape == "short-shallow":
        prefix += "k/k/k/"
    elif shape.startswith("long-"):
        depth = 4 if shape == "long-shallow" else 32
        # The ASCII fixture uses one byte per character. Divide the space left
        # after the prefix and separators among the directories and the leaf.
        width, extra = divmod(1024 - len(prefix) - (depth - 1), depth)
        widths = [width + (index < extra) for index in range(depth)]
        directories = ["k" * size for size in widths[:-1]]
        leaf_padding = "k" * (widths[-1] - 4)
        prefix += "/".join([*directories, leaf_padding])

    return [f"{prefix}{index:04d}" for index in range(count)]
