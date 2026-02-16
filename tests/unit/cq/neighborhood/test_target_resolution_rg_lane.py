"""Tests for neighborhood target resolution via shared rg adapter lane."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.core.target_specs import parse_target_spec
from tools.cq.neighborhood import target_resolution


def test_symbol_fallback_uses_shared_rg_lane(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "a.py").write_text("def alpha():\n    return 1\n", encoding="utf-8")

    monkeypatch.setattr(
        target_resolution,
        "find_symbol_candidates",
        lambda **_kwargs: [("a.py", 1, "def alpha():")],
    )

    resolved = target_resolution.resolve_target(
        parse_target_spec("alpha"),
        root=tmp_path,
        language="python",
    )

    assert resolved.resolution_kind == "symbol_fallback"
    assert resolved.target_file == "a.py"
    assert resolved.target_line == 1


def test_symbol_fallback_prefers_definition_like_lines(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        target_resolution,
        "find_symbol_candidates",
        lambda **_kwargs: [
            ("b.py", 20, "value = alpha"),
            ("a.py", 2, "def alpha():"),
        ],
    )

    resolved = target_resolution.resolve_target(
        parse_target_spec("alpha"),
        root=tmp_path,
        language="python",
    )

    assert resolved.target_file == "a.py"
    assert resolved.target_line == 2
