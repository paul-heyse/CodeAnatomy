"""Tests for object rendering wrappers."""

from __future__ import annotations

from typing import Any, cast

import pytest
from tools.cq.search.objects import render as render_module


def test_build_resolved_object_sections_wraps_single_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        render_module,
        "build_resolved_objects_section",
        lambda summaries, **kwargs: ("resolved", summaries, kwargs),
    )
    sections = render_module.build_resolved_object_sections([])
    sections_any = cast("Any", sections)
    assert len(sections) == 1
    assert sections_any[0][0] == "resolved"


def test_build_occurrence_sections_wraps_three_sections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        render_module,
        "build_occurrences_section",
        lambda *_args, **_kwargs: "occ",
    )
    monkeypatch.setattr(
        render_module,
        "build_occurrence_kind_counts_section",
        lambda *_args, **_kwargs: "kind",
    )
    monkeypatch.setattr(
        render_module,
        "build_occurrence_hot_files_section",
        lambda *_args, **_kwargs: "hot",
    )
    sections = render_module.build_occurrence_sections([], object_symbols={})
    assert sections == ["occ", "kind", "hot"]
