"""Tests for query registry profile loading helpers."""

from __future__ import annotations

import pytest
from tools.cq.search.tree_sitter.query import registry as registry_module


def test_load_query_pack_sources_for_profile_enforces_required_pack_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = (
        registry_module.QueryPackSourceV1(language="rust", pack_name="00_defs.scm", source=""),
        registry_module.QueryPackSourceV1(language="rust", pack_name="10_refs.scm", source=""),
    )
    monkeypatch.setattr(registry_module, "load_query_pack_sources", lambda *_args, **_kwargs: rows)
    profile = registry_module.QueryPackProfileV1(
        profile_name="required_tags",
        include_distribution=True,
        required_pack_names=("00_defs.scm", "80_tags.scm"),
    )
    loaded = registry_module.load_query_pack_sources_for_profile("rust", profile=profile)
    assert loaded == ()


def test_load_query_pack_sources_for_profile_returns_rows_when_requirements_met(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = (
        registry_module.QueryPackSourceV1(language="rust", pack_name="00_defs.scm", source=""),
        registry_module.QueryPackSourceV1(language="rust", pack_name="80_tags.scm", source=""),
    )
    monkeypatch.setattr(registry_module, "load_query_pack_sources", lambda *_args, **_kwargs: rows)
    profile = registry_module.QueryPackProfileV1(
        profile_name="required_tags",
        include_distribution=True,
        required_pack_names=("00_defs.scm", "80_tags.scm"),
    )
    loaded = registry_module.load_query_pack_sources_for_profile("rust", profile=profile)
    assert loaded == rows
