"""Tests for smart-search section wrapper module."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest
import tools.cq.search.pipeline.smart_search as smart_search_module
from tools.cq.core.schema import Finding, Section
from tools.cq.search._shared.types import QueryMode
from tools.cq.search.pipeline.smart_search_sections import build_finding, build_sections
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch


def test_build_sections_delegates_to_smart_search(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Section wrapper should delegate to smart_search.build_sections."""
    expected = [Section(title="S")]
    monkeypatch.setattr(
        smart_search_module,
        "build_sections",
        lambda *_args, **_kwargs: expected,
    )

    result = build_sections([], tmp_path, "target", QueryMode.IDENTIFIER)
    assert result == expected


def test_build_finding_delegates_to_smart_search(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Finding wrapper should delegate to smart_search.build_finding."""
    expected = Finding(category="definition", message="target")
    monkeypatch.setattr(
        smart_search_module,
        "build_finding",
        lambda *_args, **_kwargs: expected,
    )

    result = build_finding(cast("EnrichedMatch", object()), Path(tmp_path))
    assert result == expected
