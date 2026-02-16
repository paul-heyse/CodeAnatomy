"""Tests for CLI type definitions and converters."""

from __future__ import annotations

import pytest
from tools.cq.cli_app.types import (
    ImpactBucket,
    NeighborhoodLanguageToken,
    OutputFormat,
    ReportPreset,
    SchemaKind,
    SeverityLevel,
    comma_separated_enum,
    comma_separated_list,
)
from tools.cq.core.types import LdmdSliceMode


class TestOutputFormatEnum:
    """Tests for OutputFormat enum."""

    @staticmethod
    def test_enum_values() -> None:
        """Test that enum values match CLI tokens."""
        assert OutputFormat.md.value == "md"
        assert OutputFormat.json.value == "json"
        assert OutputFormat.mermaid_class.value == "mermaid-class"

    @staticmethod
    def test_str_conversion() -> None:
        """Test string conversion returns CLI token."""
        assert str(OutputFormat.md) == "md"
        assert str(OutputFormat.mermaid_class) == "mermaid-class"

    @staticmethod
    def test_value_lookup() -> None:
        """Test creating enum from value string."""
        assert OutputFormat("md") == OutputFormat.md
        assert OutputFormat("mermaid-class") == OutputFormat.mermaid_class


class TestImpactBucketEnum:
    """Tests for ImpactBucket enum."""

    @staticmethod
    def test_all_values() -> None:
        """Test all impact bucket values."""
        assert ImpactBucket.low.value == "low"
        assert ImpactBucket.med.value == "med"
        assert ImpactBucket.high.value == "high"

    @staticmethod
    def test_str_conversion() -> None:
        """Test string conversion."""
        assert str(ImpactBucket.low) == "low"


class TestSeverityLevelEnum:
    """Tests for SeverityLevel enum."""

    @staticmethod
    def test_all_values() -> None:
        """Test all severity values."""
        assert SeverityLevel.info.value == "info"
        assert SeverityLevel.warning.value == "warning"
        assert SeverityLevel.error.value == "error"


class TestReportPresetEnum:
    """Tests for ReportPreset enum."""

    @staticmethod
    def test_all_values() -> None:
        """Test all preset values."""
        assert ReportPreset.refactor_impact.value == "refactor-impact"
        assert ReportPreset.safety_reliability.value == "safety-reliability"
        assert ReportPreset.change_propagation.value == "change-propagation"
        assert ReportPreset.dependency_health.value == "dependency-health"


class TestSchemaKindEnum:
    """Tests for SchemaKind enum."""

    @staticmethod
    def test_all_values() -> None:
        """Test all schema kind values."""
        assert SchemaKind.result.value == "result"
        assert SchemaKind.query.value == "query"
        assert SchemaKind.components.value == "components"


class TestNeighborhoodLanguageTokenEnum:
    """Tests for neighborhood language token enum."""

    @staticmethod
    def test_all_values() -> None:
        """Test supported neighborhood language values."""
        assert NeighborhoodLanguageToken.python.value == "python"
        assert NeighborhoodLanguageToken.rust.value == "rust"


class TestLdmdSliceModeEnum:
    """Tests for LDMD slice mode enum."""

    @staticmethod
    def test_all_values() -> None:
        """Test supported LDMD extraction modes."""
        assert LdmdSliceMode.full.value == "full"
        assert LdmdSliceMode.preview.value == "preview"
        assert LdmdSliceMode.tldr.value == "tldr"


class TestCommaSeparatedList:
    """Tests for comma_separated_list converter."""

    @staticmethod
    def test_single_value() -> None:
        """Test converter with single value."""
        convert = comma_separated_list(str)
        result = convert("foo")
        assert result == ["foo"]

    @staticmethod
    def test_comma_separated() -> None:
        """Test converter with comma-separated values."""
        convert = comma_separated_list(str)
        result = convert("a,b,c")
        assert result == ["a", "b", "c"]

    @staticmethod
    def test_list_input() -> None:
        """Test converter with list input."""
        convert = comma_separated_list(str)
        result = convert(["a", "b", "c"])
        assert result == ["a", "b", "c"]

    @staticmethod
    def test_mixed_list_and_comma() -> None:
        """Test converter with list containing comma-separated items."""
        convert = comma_separated_list(str)
        result = convert(["a,b", "c"])
        assert result == ["a", "b", "c"]

    @staticmethod
    def test_strips_whitespace() -> None:
        """Test that whitespace is stripped."""
        convert = comma_separated_list(str)
        result = convert("a , b , c")
        assert result == ["a", "b", "c"]

    @staticmethod
    def test_empty_parts_ignored() -> None:
        """Test that empty parts are ignored."""
        convert = comma_separated_list(str)
        result = convert("a,,b")
        assert result == ["a", "b"]

    @staticmethod
    def test_type_conversion() -> None:
        """Test type conversion."""
        convert = comma_separated_list(int)
        result = convert("1,2,3")
        assert result == [1, 2, 3]


class TestCommaSeparatedEnum:
    """Tests for comma_separated_enum converter."""

    @staticmethod
    def test_single_value() -> None:
        """Test converter with single enum value."""
        convert = comma_separated_enum(ImpactBucket)
        result = convert("low")
        assert result == [ImpactBucket.low]

    @staticmethod
    def test_comma_separated() -> None:
        """Test converter with comma-separated enum values."""
        convert = comma_separated_enum(ImpactBucket)
        result = convert("low,med,high")
        assert result == [ImpactBucket.low, ImpactBucket.med, ImpactBucket.high]

    @staticmethod
    def test_list_input() -> None:
        """Test converter with list input."""
        convert = comma_separated_enum(SeverityLevel)
        result = convert(["info", "warning"])
        assert result == [SeverityLevel.info, SeverityLevel.warning]

    @staticmethod
    def test_invalid_value_raises() -> None:
        """Test that invalid enum value raises."""
        convert = comma_separated_enum(ImpactBucket)
        with pytest.raises(ValueError, match="invalid"):
            convert("invalid")
