"""Tests for convention-based extractor discovery (Section 15.3)."""

from __future__ import annotations

import pytest

from datafusion_engine.extract.templates import (
    ExtractorConfigSpec,
    ExtractorTemplate,
    config,
    template,
)

EXPECTED_TEMPLATE_COUNT = 9
DUPLICATE_RANK = 7
DUPLICATE_RANK_COUNT = 2
EXPECTED_CONFIG_COUNT = 11

_EXPECTED_TEMPLATE_KEYS: frozenset[str] = frozenset(
    {
        "ast",
        "bytecode",
        "cst",
        "python_external",
        "python_imports",
        "repo_scan",
        "scip",
        "symtable",
        "tree_sitter",
    }
)

_EXPECTED_CONFIG_KEYS: frozenset[str] = frozenset(
    _EXPECTED_TEMPLATE_KEYS
    | {
        "repo_blobs",
        "file_line_index_v1",
    }
)


class TestTemplateDiscovery:
    """Verify template discovery via public accessors."""

    def test_templates_count(self) -> None:
        """Verify all expected template names resolve."""
        resolved = {key: template(key) for key in _EXPECTED_TEMPLATE_KEYS}
        assert len(resolved) == EXPECTED_TEMPLATE_COUNT

    def test_templates_keys(self) -> None:
        """Verify every expected template key resolves."""
        resolved = {key: template(key) for key in _EXPECTED_TEMPLATE_KEYS}
        assert set(resolved.keys()) == _EXPECTED_TEMPLATE_KEYS

    def test_templates_types(self) -> None:
        """Verify every resolved value is an ExtractorTemplate."""
        for key in _EXPECTED_TEMPLATE_KEYS:
            tmpl = template(key)
            assert isinstance(tmpl, ExtractorTemplate), f"{key} is not ExtractorTemplate"

    def test_template_name_matches_key(self) -> None:
        """Verify each template's extractor_name matches its key."""
        for key in _EXPECTED_TEMPLATE_KEYS:
            tmpl = template(key)
            assert tmpl.extractor_name == key, (
                f"Key {key!r} != extractor_name {tmpl.extractor_name!r}"
            )

    def test_template_accessor(self) -> None:
        """Verify template() returns the correct entry."""
        for key in _EXPECTED_TEMPLATE_KEYS:
            assert template(key).extractor_name == key

    def test_template_accessor_unknown_raises(self) -> None:
        """Verify template() raises KeyError for unknown names."""
        with pytest.raises(KeyError):
            template("nonexistent_extractor")

    def test_evidence_ranks_unique(self) -> None:
        """Verify no two templates share the same evidence rank."""
        ranks = [template(key).evidence_rank for key in _EXPECTED_TEMPLATE_KEYS]
        # python_imports and python_external both have rank 7, so allow that
        rank_counts: dict[int, int] = {}
        for r in ranks:
            rank_counts[r] = rank_counts.get(r, 0) + 1
        # Only rank 7 should have duplicates (python_imports + python_external)
        for rank, count in rank_counts.items():
            if rank == DUPLICATE_RANK:
                assert count == DUPLICATE_RANK_COUNT, "Rank 7 should have exactly 2 extractors"
            else:
                assert count == 1, f"Rank {rank} has {count} extractors (expected 1)"

    def test_metadata_extra_populated(self) -> None:
        """Verify all templates have non-empty metadata_extra."""
        for key in _EXPECTED_TEMPLATE_KEYS:
            tmpl = template(key)
            assert tmpl.metadata_extra, f"{key} has empty metadata_extra"


class TestConfigDiscovery:
    """Verify config discovery via public accessors."""

    def test_configs_count(self) -> None:
        """Verify all expected config names resolve."""
        resolved = {key: config(key) for key in _EXPECTED_CONFIG_KEYS}
        assert len(resolved) == EXPECTED_CONFIG_COUNT

    def test_configs_keys(self) -> None:
        """Verify all expected config keys resolve."""
        resolved = {key: config(key) for key in _EXPECTED_CONFIG_KEYS}
        assert set(resolved.keys()) == _EXPECTED_CONFIG_KEYS

    def test_configs_types(self) -> None:
        """Verify every resolved value is an ExtractorConfigSpec."""
        for key in _EXPECTED_CONFIG_KEYS:
            cfg = config(key)
            assert isinstance(cfg, ExtractorConfigSpec), f"{key} is not ExtractorConfigSpec"

    def test_config_name_matches_key(self) -> None:
        """Verify each config's extractor_name matches its key."""
        for key in _EXPECTED_CONFIG_KEYS:
            cfg = config(key)
            assert cfg.extractor_name == key, (
                f"Key {key!r} != extractor_name {cfg.extractor_name!r}"
            )

    def test_config_accessor(self) -> None:
        """Verify config() returns the correct entry."""
        for key in _EXPECTED_CONFIG_KEYS:
            assert config(key).extractor_name == key

    def test_config_accessor_unknown_raises(self) -> None:
        """Verify config() raises KeyError for unknown names."""
        with pytest.raises(KeyError):
            config("nonexistent_extractor")

    def test_configs_superset_of_templates(self) -> None:
        """Verify every template key has a corresponding config."""
        assert _EXPECTED_TEMPLATE_KEYS.issubset(_EXPECTED_CONFIG_KEYS)

    def test_extra_config_entries(self) -> None:
        """Verify the config-only entries (no template) exist."""
        config_only = _EXPECTED_CONFIG_KEYS - _EXPECTED_TEMPLATE_KEYS
        assert config_only == {"repo_blobs", "file_line_index_v1"}


def test_cst_config_defaults_exposed_via_config_accessor() -> None:
    """Verify known CST boolean defaults remain available via config()."""
    cst = config("cst")
    assert cst.defaults.get("include_refs") is True
    assert cst.defaults.get("compute_type_inference") is False
