"""Tests for convention-based extractor discovery (Section 15.3)."""

from __future__ import annotations

import pytest

from datafusion_engine.extract.templates import (
    CONFIGS,
    TEMPLATES,
    ExtractorConfigSpec,
    ExtractorTemplate,
    config,
    flag_default,
    template,
)

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
    """Verify _discover_templates produces the expected TEMPLATES dict."""

    def test_templates_count(self) -> None:
        """Verify TEMPLATES has exactly 9 entries."""
        assert len(TEMPLATES) == 9

    def test_templates_keys(self) -> None:
        """Verify TEMPLATES contains all expected extractor keys."""
        assert set(TEMPLATES.keys()) == _EXPECTED_TEMPLATE_KEYS

    def test_templates_types(self) -> None:
        """Verify every value is an ExtractorTemplate."""
        for key, tmpl in TEMPLATES.items():
            assert isinstance(tmpl, ExtractorTemplate), f"{key} is not ExtractorTemplate"

    def test_template_name_matches_key(self) -> None:
        """Verify each template's extractor_name matches its dict key."""
        for key, tmpl in TEMPLATES.items():
            assert tmpl.extractor_name == key, (
                f"Key {key!r} != extractor_name {tmpl.extractor_name!r}"
            )

    def test_template_accessor(self) -> None:
        """Verify template() returns the correct entry."""
        for key in _EXPECTED_TEMPLATE_KEYS:
            assert template(key) is TEMPLATES[key]

    def test_template_accessor_unknown_raises(self) -> None:
        """Verify template() raises KeyError for unknown names."""
        with pytest.raises(KeyError):
            template("nonexistent_extractor")

    def test_evidence_ranks_unique(self) -> None:
        """Verify no two templates share the same evidence rank."""
        ranks = [t.evidence_rank for t in TEMPLATES.values()]
        # python_imports and python_external both have rank 7, so allow that
        rank_counts: dict[int, int] = {}
        for r in ranks:
            rank_counts[r] = rank_counts.get(r, 0) + 1
        # Only rank 7 should have duplicates (python_imports + python_external)
        for rank, count in rank_counts.items():
            if rank == 7:
                assert count == 2, "Rank 7 should have exactly 2 extractors"
            else:
                assert count == 1, f"Rank {rank} has {count} extractors (expected 1)"

    def test_metadata_extra_populated(self) -> None:
        """Verify all templates have non-empty metadata_extra."""
        for key, tmpl in TEMPLATES.items():
            assert tmpl.metadata_extra, f"{key} has empty metadata_extra"


class TestConfigDiscovery:
    """Verify _discover_configs produces the expected CONFIGS dict."""

    def test_configs_count(self) -> None:
        """Verify CONFIGS has exactly 11 entries."""
        assert len(CONFIGS) == 11

    def test_configs_keys(self) -> None:
        """Verify CONFIGS contains all expected extractor keys."""
        assert set(CONFIGS.keys()) == _EXPECTED_CONFIG_KEYS

    def test_configs_types(self) -> None:
        """Verify every value is an ExtractorConfigSpec."""
        for key, cfg in CONFIGS.items():
            assert isinstance(cfg, ExtractorConfigSpec), f"{key} is not ExtractorConfigSpec"

    def test_config_name_matches_key(self) -> None:
        """Verify each config's extractor_name matches its dict key."""
        for key, cfg in CONFIGS.items():
            assert cfg.extractor_name == key, (
                f"Key {key!r} != extractor_name {cfg.extractor_name!r}"
            )

    def test_config_accessor(self) -> None:
        """Verify config() returns the correct entry."""
        for key in _EXPECTED_CONFIG_KEYS:
            assert config(key) is CONFIGS[key]

    def test_config_accessor_unknown_raises(self) -> None:
        """Verify config() raises KeyError for unknown names."""
        with pytest.raises(KeyError):
            config("nonexistent_extractor")

    def test_configs_superset_of_templates(self) -> None:
        """Verify every TEMPLATES key also appears in CONFIGS."""
        assert set(TEMPLATES.keys()).issubset(set(CONFIGS.keys()))

    def test_extra_config_entries(self) -> None:
        """Verify the config-only entries (no template) exist."""
        config_only = set(CONFIGS.keys()) - set(TEMPLATES.keys())
        assert config_only == {"repo_blobs", "file_line_index_v1"}


class TestFlagDefaults:
    """Verify flag_default derives values from CONFIG descriptors."""

    def test_known_flag(self) -> None:
        """Verify a known boolean flag returns its configured default."""
        # include_refs is a CST feature flag defaulting to True
        assert flag_default("include_refs") is True

    def test_false_flag(self) -> None:
        """Verify a flag defaulting to False returns False."""
        # compute_type_inference defaults to False in CST config
        assert flag_default("compute_type_inference") is False

    def test_unknown_flag_uses_fallback(self) -> None:
        """Verify unknown flags return the fallback value."""
        assert flag_default("nonexistent_flag") is True
        assert flag_default("nonexistent_flag", fallback=False) is False
