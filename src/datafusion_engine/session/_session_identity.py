"""Single-authority runtime session identity constants."""

from __future__ import annotations

from typing import Final

from utils.uuid_factory import uuid7_str

RUNTIME_SESSION_ID: Final[str] = uuid7_str()

__all__ = ["RUNTIME_SESSION_ID"]
