"""Tests for search profiles and limits configuration."""

from __future__ import annotations

from tools.cq.search.profiles import (
    AUDIT,
    DEFAULT,
    INTERACTIVE,
    LITERAL,
    SearchLimits,
)


class TestSearchLimitsDataclass:
    """Test SearchLimits dataclass behavior."""

    def test_defaults(self) -> None:
        """Test SearchLimits creates with expected defaults."""
        limits = SearchLimits()
        assert limits.max_files == 5000
        assert limits.max_matches_per_file == 1000
        assert limits.max_total_matches == 10000
        assert limits.timeout_seconds == 30.0

    def test_custom_values(self) -> None:
        """Test creating SearchLimits with custom values."""
        limits = SearchLimits(
            max_files=100,
            max_matches_per_file=50,
            max_total_matches=500,
            timeout_seconds=10.0,
        )
        assert limits.max_files == 100
        assert limits.max_matches_per_file == 50
        assert limits.max_total_matches == 500
        assert limits.timeout_seconds == 10.0

    def test_partial_override(self) -> None:
        """Test overriding only some fields."""
        limits = SearchLimits(max_files=500)
        assert limits.max_files == 500
        # Other fields should have defaults
        assert limits.max_matches_per_file == 1000
        assert limits.max_total_matches == 10000
        assert limits.timeout_seconds == 30.0


class TestPresetProfiles:
    """Test predefined search limit presets."""

    def test_default_preset(self) -> None:
        """Test DEFAULT has expected values."""
        assert isinstance(DEFAULT, SearchLimits)
        assert DEFAULT.max_files == 5000
        assert DEFAULT.max_matches_per_file == 1000
        assert DEFAULT.max_total_matches == 10000
        assert DEFAULT.timeout_seconds == 30.0

    def test_interactive_preset(self) -> None:
        """Test INTERACTIVE is optimized for quick response."""
        assert isinstance(INTERACTIVE, SearchLimits)
        assert INTERACTIVE.max_files == 1000
        assert INTERACTIVE.timeout_seconds == 10.0
        # Should be faster than default
        assert INTERACTIVE.timeout_seconds <= DEFAULT.timeout_seconds

    def test_audit_preset(self) -> None:
        """Test AUDIT allows comprehensive searches."""
        assert isinstance(AUDIT, SearchLimits)
        assert AUDIT.max_files == 50000
        assert AUDIT.max_total_matches == 100000
        assert AUDIT.timeout_seconds == 300.0
        # Should be more permissive than default
        assert AUDIT.max_files >= DEFAULT.max_files
        assert AUDIT.timeout_seconds >= DEFAULT.timeout_seconds

    def test_literal_preset(self) -> None:
        """Test LITERAL is optimized for literal searches."""
        assert isinstance(LITERAL, SearchLimits)
        assert LITERAL.max_files == 2000
        assert LITERAL.max_matches_per_file == 500

    def test_all_presets_valid(self) -> None:
        """Test all presets have valid positive values."""
        presets = [DEFAULT, INTERACTIVE, AUDIT, LITERAL]
        for preset in presets:
            assert preset.max_files > 0
            assert preset.max_matches_per_file > 0
            assert preset.max_total_matches > 0
            assert preset.timeout_seconds > 0


class TestPresetComparison:
    """Test relationships between different presets."""

    def test_interactive_faster_than_default(self) -> None:
        """Test INTERACTIVE has tighter timeout than DEFAULT."""
        assert INTERACTIVE.timeout_seconds <= DEFAULT.timeout_seconds

    def test_audit_more_permissive_than_default(self) -> None:
        """Test AUDIT allows more results and time than DEFAULT."""
        assert AUDIT.max_total_matches >= DEFAULT.max_total_matches
        assert AUDIT.timeout_seconds >= DEFAULT.timeout_seconds

    def test_presets_are_distinct(self) -> None:
        """Test each preset is a separate instance."""
        assert DEFAULT is not INTERACTIVE
        assert DEFAULT is not AUDIT
        assert DEFAULT is not LITERAL
        assert INTERACTIVE is not AUDIT
