"""Unit tests for orchestration language-routing helpers."""

from __future__ import annotations

from tools.cq.orchestration.language_router import detect_language, route_by_language


class _Req:
    target = "x.rs"


def test_detect_language_prefers_explicit_hint() -> None:
    """Explicit language hint should take precedence over inferred suffix."""
    assert detect_language(target="x.py", lang="rust") == "rust"


def test_detect_language_uses_target_suffix() -> None:
    """Auto language mode should infer language from the target suffix."""
    assert detect_language(target="x.rs", lang="auto") == "rust"
    assert detect_language(target="x.py", lang="auto") == "python"


def test_route_by_language_uses_request_target() -> None:
    """Routing helper should derive language from request target."""
    assert route_by_language(_Req(), lang="auto") == "rust"
