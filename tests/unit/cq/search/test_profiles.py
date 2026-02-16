"""Tests for search profiles and limits configuration."""

from __future__ import annotations

from tools.cq.search.pipeline.profiles import (
    AUDIT,
    DEFAULT,
    INTERACTIVE,
    LITERAL,
    SearchLimits,
)

DEFAULT_MAX_FILES = 5000
DEFAULT_MAX_MATCHES_PER_FILE = 1000
DEFAULT_MAX_TOTAL_MATCHES = 10000
DEFAULT_TIMEOUT_SECONDS = 30.0
INTERACTIVE_MAX_FILES = 1000
INTERACTIVE_TIMEOUT_SECONDS = 10.0
AUDIT_MAX_FILES = 50000
AUDIT_MAX_TOTAL_MATCHES = 100000
AUDIT_TIMEOUT_SECONDS = 300.0
LITERAL_MAX_FILES = 2000
LITERAL_MAX_MATCHES_PER_FILE = 500
POSITIVE_THRESHOLD = 0
CUSTOM_MAX_FILES = 100
CUSTOM_MAX_MATCHES_PER_FILE = 50
CUSTOM_MAX_TOTAL_MATCHES = 500
CUSTOM_TIMEOUT_SECONDS = 10.0
PARTIAL_OVERRIDE_MAX_FILES = 500


class TestSearchLimitsDataclass:
    """Test SearchLimits dataclass behavior."""

    def test_defaults(self) -> None:
        """Test SearchLimits creates with expected defaults."""
        limits = SearchLimits()
        assert limits.max_files == DEFAULT_MAX_FILES
        assert limits.max_matches_per_file == DEFAULT_MAX_MATCHES_PER_FILE
        assert limits.max_total_matches == DEFAULT_MAX_TOTAL_MATCHES
        assert limits.timeout_seconds == DEFAULT_TIMEOUT_SECONDS

    def test_custom_values(self) -> None:
        """Test creating SearchLimits with custom values."""
        limits = SearchLimits(
            max_files=CUSTOM_MAX_FILES,
            max_matches_per_file=CUSTOM_MAX_MATCHES_PER_FILE,
            max_total_matches=CUSTOM_MAX_TOTAL_MATCHES,
            timeout_seconds=CUSTOM_TIMEOUT_SECONDS,
        )
        assert limits.max_files == CUSTOM_MAX_FILES
        assert limits.max_matches_per_file == CUSTOM_MAX_MATCHES_PER_FILE
        assert limits.max_total_matches == CUSTOM_MAX_TOTAL_MATCHES
        assert limits.timeout_seconds == CUSTOM_TIMEOUT_SECONDS

    def test_partial_override(self) -> None:
        """Test overriding only some fields."""
        limits = SearchLimits(max_files=PARTIAL_OVERRIDE_MAX_FILES)
        assert limits.max_files == PARTIAL_OVERRIDE_MAX_FILES
        # Other fields should have defaults
        assert limits.max_matches_per_file == DEFAULT_MAX_MATCHES_PER_FILE
        assert limits.max_total_matches == DEFAULT_MAX_TOTAL_MATCHES
        assert limits.timeout_seconds == DEFAULT_TIMEOUT_SECONDS


class TestPresetProfiles:
    """Test predefined search limit presets."""

    def test_default_preset(self) -> None:
        """Test DEFAULT has expected values."""
        assert isinstance(DEFAULT, SearchLimits)
        assert DEFAULT.max_files == DEFAULT_MAX_FILES
        assert DEFAULT.max_matches_per_file == DEFAULT_MAX_MATCHES_PER_FILE
        assert DEFAULT.max_total_matches == DEFAULT_MAX_TOTAL_MATCHES
        assert DEFAULT.timeout_seconds == DEFAULT_TIMEOUT_SECONDS

    def test_interactive_preset(self) -> None:
        """Test INTERACTIVE is optimized for quick response."""
        assert isinstance(INTERACTIVE, SearchLimits)
        assert INTERACTIVE.max_files == INTERACTIVE_MAX_FILES
        assert INTERACTIVE.timeout_seconds == INTERACTIVE_TIMEOUT_SECONDS
        # Should be faster than default
        assert INTERACTIVE.timeout_seconds <= DEFAULT.timeout_seconds

    def test_audit_preset(self) -> None:
        """Test AUDIT allows comprehensive searches."""
        assert isinstance(AUDIT, SearchLimits)
        assert AUDIT.max_files == AUDIT_MAX_FILES
        assert AUDIT.max_total_matches == AUDIT_MAX_TOTAL_MATCHES
        assert AUDIT.timeout_seconds == AUDIT_TIMEOUT_SECONDS
        # Should be more permissive than default
        assert AUDIT.max_files >= DEFAULT.max_files
        assert AUDIT.timeout_seconds >= DEFAULT.timeout_seconds

    def test_literal_preset(self) -> None:
        """Test LITERAL is optimized for literal searches."""
        assert isinstance(LITERAL, SearchLimits)
        assert LITERAL.max_files == LITERAL_MAX_FILES
        assert LITERAL.max_matches_per_file == LITERAL_MAX_MATCHES_PER_FILE

    def test_all_presets_valid(self) -> None:
        """Test all presets have valid positive values."""
        presets = [DEFAULT, INTERACTIVE, AUDIT, LITERAL]
        for preset in presets:
            assert preset.max_files > POSITIVE_THRESHOLD
            assert preset.max_matches_per_file > POSITIVE_THRESHOLD
            assert preset.max_total_matches > POSITIVE_THRESHOLD
            assert preset.timeout_seconds > POSITIVE_THRESHOLD


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
