"""Tests for CLI type definitions and converters."""

from __future__ import annotations

import pytest
from tools.cq.cli_app.types import (
    ImpactBucket,
    OutputFormat,
    ReportPreset,
    SeverityLevel,
    comma_separated_enum,
    comma_separated_list,
)


class TestOutputFormatEnum:
    """Tests for OutputFormat enum."""

    def test_enum_values(self) -> None:
        """Test that enum values match CLI tokens."""
        assert OutputFormat.md.value == "md"
        assert OutputFormat.json.value == "json"
        assert OutputFormat.mermaid_class.value == "mermaid-class"

    def test_str_conversion(self) -> None:
        """Test string conversion returns CLI token."""
        assert str(OutputFormat.md) == "md"
        assert str(OutputFormat.mermaid_class) == "mermaid-class"

    def test_value_lookup(self) -> None:
        """Test creating enum from value string."""
        assert OutputFormat("md") == OutputFormat.md
        assert OutputFormat("mermaid-class") == OutputFormat.mermaid_class


class TestImpactBucketEnum:
    """Tests for ImpactBucket enum."""

    def test_all_values(self) -> None:
        """Test all impact bucket values."""
        assert ImpactBucket.low.value == "low"
        assert ImpactBucket.med.value == "med"
        assert ImpactBucket.high.value == "high"

    def test_str_conversion(self) -> None:
        """Test string conversion."""
        assert str(ImpactBucket.low) == "low"


class TestSeverityLevelEnum:
    """Tests for SeverityLevel enum."""

    def test_all_values(self) -> None:
        """Test all severity values."""
        assert SeverityLevel.info.value == "info"
        assert SeverityLevel.warning.value == "warning"
        assert SeverityLevel.error.value == "error"


class TestReportPresetEnum:
    """Tests for ReportPreset enum."""

    def test_all_values(self) -> None:
        """Test all preset values."""
        assert ReportPreset.refactor_impact.value == "refactor-impact"
        assert ReportPreset.safety_reliability.value == "safety-reliability"
        assert ReportPreset.change_propagation.value == "change-propagation"
        assert ReportPreset.dependency_health.value == "dependency-health"


class TestCommaSeparatedList:
    """Tests for comma_separated_list converter."""

    def test_single_value(self) -> None:
        """Test converter with single value."""
        convert = comma_separated_list(str)
        result = convert("foo")
        assert result == ["foo"]

    def test_comma_separated(self) -> None:
        """Test converter with comma-separated values."""
        convert = comma_separated_list(str)
        result = convert("a,b,c")
        assert result == ["a", "b", "c"]

    def test_list_input(self) -> None:
        """Test converter with list input."""
        convert = comma_separated_list(str)
        result = convert(["a", "b", "c"])
        assert result == ["a", "b", "c"]

    def test_mixed_list_and_comma(self) -> None:
        """Test converter with list containing comma-separated items."""
        convert = comma_separated_list(str)
        result = convert(["a,b", "c"])
        assert result == ["a", "b", "c"]

    def test_strips_whitespace(self) -> None:
        """Test that whitespace is stripped."""
        convert = comma_separated_list(str)
        result = convert("a , b , c")
        assert result == ["a", "b", "c"]

    def test_empty_parts_ignored(self) -> None:
        """Test that empty parts are ignored."""
        convert = comma_separated_list(str)
        result = convert("a,,b")
        assert result == ["a", "b"]

    def test_type_conversion(self) -> None:
        """Test type conversion."""
        convert = comma_separated_list(int)
        result = convert("1,2,3")
        assert result == [1, 2, 3]


class TestCommaSeparatedEnum:
    """Tests for comma_separated_enum converter."""

    def test_single_value(self) -> None:
        """Test converter with single enum value."""
        convert = comma_separated_enum(ImpactBucket)
        result = convert("low")
        assert result == [ImpactBucket.low]

    def test_comma_separated(self) -> None:
        """Test converter with comma-separated enum values."""
        convert = comma_separated_enum(ImpactBucket)
        result = convert("low,med,high")
        assert result == [ImpactBucket.low, ImpactBucket.med, ImpactBucket.high]

    def test_list_input(self) -> None:
        """Test converter with list input."""
        convert = comma_separated_enum(SeverityLevel)
        result = convert(["info", "warning"])
        assert result == [SeverityLevel.info, SeverityLevel.warning]

    def test_invalid_value_raises(self) -> None:
        """Test that invalid enum value raises."""
        convert = comma_separated_enum(ImpactBucket)
        with pytest.raises(ValueError, match="invalid"):
            convert("invalid")
