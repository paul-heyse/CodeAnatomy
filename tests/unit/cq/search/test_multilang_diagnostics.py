"""Tests for cross-language diagnostics and capability matrix."""

from __future__ import annotations

from tools.cq.search.semantic.diagnostics import (
    CAPABILITY_MATRIX,
    build_capability_diagnostics,
    build_cross_language_diagnostics,
    build_language_capabilities,
    diagnostics_to_summary_payload,
    features_from_macro,
)

MULTI_FEATURE_DIAGNOSTIC_COUNT = 2


class TestCapabilityMatrix:
    """Tests for the capability matrix data structure."""

    @staticmethod
    def test_matrix_has_expected_keys() -> None:
        """Test that capability matrix contains expected feature keys."""
        assert "entity:function" in CAPABILITY_MATRIX
        assert "entity:decorator" in CAPABILITY_MATRIX
        assert "macro:calls" in CAPABILITY_MATRIX

    @staticmethod
    def test_matrix_values_are_valid() -> None:
        """Test that all capability levels are valid."""
        valid_levels = {"full", "partial", "none"}
        for feature, caps in CAPABILITY_MATRIX.items():
            for lang, level in caps.items():
                assert level in valid_levels, f"{feature}[{lang}] = {level!r}"

    @staticmethod
    def test_all_features_have_both_languages() -> None:
        """Test that every feature has python and rust entries."""
        for feature, caps in CAPABILITY_MATRIX.items():
            assert "python" in caps, f"{feature} missing python"
            assert "rust" in caps, f"{feature} missing rust"


class TestBuildCapabilityDiagnostics:
    """Tests for capability-aware diagnostic building."""

    @staticmethod
    def test_no_diagnostics_for_full_support() -> None:
        """Test that no diagnostics are generated for fully supported features."""
        diags = build_capability_diagnostics(
            features=["entity:function"],
            lang_scope="auto",
        )
        assert diags == []

    @staticmethod
    def test_diagnostics_for_rust_none() -> None:
        """Test diagnostic generation for unsupported rust features."""
        diags = build_capability_diagnostics(
            features=["entity:decorator"],
            lang_scope="auto",
        )
        assert len(diags) == 1
        assert diags[0].category == "capability_limitation"
        assert "rust" in diags[0].message
        assert "not supported" in diags[0].message
        code = diags[0].details.data["code"]
        assert isinstance(code, str)
        assert code.startswith("ML_CAP_")

    @staticmethod
    def test_diagnostics_for_partial_support() -> None:
        """Test diagnostic generation for partially supported features."""
        diags = build_capability_diagnostics(
            features=["macro:calls"],
            lang_scope="auto",
        )
        assert len(diags) == 1
        assert "partial" in diags[0].message

    @staticmethod
    def test_no_diagnostics_for_explicit_scope() -> None:
        """Test that explicit python scope suppresses diagnostics."""
        diags = build_capability_diagnostics(
            features=["entity:decorator"],
            lang_scope="python",
        )
        assert diags == []

    @staticmethod
    def test_diagnostics_for_explicit_rust_scope() -> None:
        """Test diagnostic generation for explicit rust scope with unsupported feature."""
        diags = build_capability_diagnostics(
            features=["entity:decorator"],
            lang_scope="rust",
        )
        assert len(diags) == 1
        assert "rust" in diags[0].message

    @staticmethod
    def test_unknown_feature_ignored() -> None:
        """Test that unknown features are ignored."""
        diags = build_capability_diagnostics(
            features=["unknown_feature"],
            lang_scope="auto",
        )
        assert diags == []

    @staticmethod
    def test_multiple_features() -> None:
        """Test diagnostic generation for multiple features."""
        diags = build_capability_diagnostics(
            features=["entity:decorator", "scope_filter"],
            lang_scope="auto",
        )
        assert len(diags) == MULTI_FEATURE_DIAGNOSTIC_COUNT


class TestFeaturesFromMacro:
    """Tests for macro feature extraction."""

    @staticmethod
    def test_calls_macro() -> None:
        """Test feature extraction for calls macro."""
        assert features_from_macro("calls") == ["macro:calls"]

    @staticmethod
    def test_impact_macro() -> None:
        """Test feature extraction for impact macro."""
        assert features_from_macro("impact") == ["macro:impact"]


class TestExistingCrossLanguageDiagnostics:
    """Ensure existing cross_language_hint diagnostics still work."""

    @staticmethod
    def test_cross_lang_hint_fires() -> None:
        """Test that cross-language hint diagnostic fires when appropriate."""
        diags = build_cross_language_diagnostics(
            lang_scope="auto",
            python_matches=0,
            rust_matches=5,
            python_oriented=True,
        )
        assert len(diags) == 1
        assert diags[0].category == "cross_language_hint"
        assert diags[0].details.data["code"] == "ML001"

    @staticmethod
    def test_cross_lang_hint_no_fire_with_python_matches() -> None:
        """Test that hint doesn't fire when python matches exist."""
        diags = build_cross_language_diagnostics(
            lang_scope="auto",
            python_matches=3,
            rust_matches=5,
            python_oriented=True,
        )
        assert diags == []

    @staticmethod
    def test_cross_lang_hint_no_fire_explicit_scope() -> None:
        """Test that hint doesn't fire for explicit python scope."""
        diags = build_cross_language_diagnostics(
            lang_scope="python",
            python_matches=0,
            rust_matches=5,
            python_oriented=True,
        )
        assert diags == []


class TestSummaryPayloadHelpers:
    """Tests for summary payload serialization helpers."""

    @staticmethod
    def test_diagnostics_to_summary_payload() -> None:
        """Test diagnostic list conversion into summary payload rows."""
        diags = build_cross_language_diagnostics(
            lang_scope="auto",
            python_matches=0,
            rust_matches=2,
            python_oriented=True,
        )
        payload = diagnostics_to_summary_payload(diags)
        assert payload
        first = payload[0]
        assert first["code"] == "ML001"
        assert first["severity"] == "warning"
        assert "languages" in first

    @staticmethod
    def test_language_capabilities_shape() -> None:
        """Test language-capabilities payload top-level shape."""
        caps = build_language_capabilities(lang_scope="auto")
        assert "python" in caps
        assert "rust" in caps
        assert "shared" in caps
