"""Call-target classification helpers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.macros.calls_target import infer_target_language

__all__ = ["classify_call_target_language"]


def classify_call_target_language(root: Path, function_name: str) -> str | None:
    """Classify target function language from repository evidence.

    Returns:
        str | None: Inferred language hint for the function target.
    """
    return infer_target_language(root, function_name)
