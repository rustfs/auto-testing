"""Share the pinned rc executable resolver with ILM validation."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.validation_ilm.rc_binary import resolve_rc_binary  # noqa: E402
