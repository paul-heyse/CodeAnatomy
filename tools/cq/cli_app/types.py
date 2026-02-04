"""Type definitions for cq CLI.

This module defines enums and custom converters for cyclopts.
Enum member names ARE the CLI tokens for correct cyclopts coercion.
"""

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum
from typing import TypeVar

T = TypeVar("T")


class OutputFormat(StrEnum):
    """Output format options.

    Member names are the CLI tokens (e.g., --format md).
    """

    md = "md"
    json = "json"
    both = "both"
    summary = "summary"
    mermaid = "mermaid"
    mermaid_class = "mermaid-class"
    dot = "dot"

    def __str__(self) -> str:
        """Return the CLI token."""
        return self.value


class ImpactBucket(StrEnum):
    """Impact level buckets."""

    low = "low"
    med = "med"
    high = "high"

    def __str__(self) -> str:
        """Return the CLI token."""
        return self.value


class ConfidenceBucket(StrEnum):
    """Confidence level buckets."""

    low = "low"
    med = "med"
    high = "high"

    def __str__(self) -> str:
        """Return the CLI token."""
        return self.value


class SeverityLevel(StrEnum):
    """Severity levels."""

    info = "info"
    warning = "warning"
    error = "error"

    def __str__(self) -> str:
        """Return the CLI token."""
        return self.value


class ReportPreset(StrEnum):
    """Report preset options."""

    refactor_impact = "refactor-impact"
    safety_reliability = "safety-reliability"
    change_propagation = "change-propagation"
    dependency_health = "dependency-health"

    def __str__(self) -> str:
        """Return the CLI token."""
        return self.value


def comma_separated_list(type_: type[T]) -> Callable[[str | list[str]], list[T]]:
    """Create a converter for comma-separated values.

    This handles both:
    - Comma-separated strings: "a,b,c"
    - Repeated flags: --flag a --flag b

    Parameters
    ----------
    type_
        The target type for each element.

    Returns
    -------
    Callable
        A converter function for cyclopts.
    """

    def convert(value: str | list[str]) -> list[T]:
        """Convert input to list of typed values.

        Parameters
        ----------
        value
            Input string or list.

        Returns
        -------
        list[T]
            Converted list.
        """
        if isinstance(value, list):
            # Flatten any comma-separated items in the list
            result: list[T] = []
            for item in value:
                for segment in str(item).split(","):
                    segment = segment.strip()
                    if segment:
                        result.append(type_(segment))  # pyright: ignore[reportCallIssue]
            return result

        # Single comma-separated string
        result = []
        for segment in value.split(","):
            segment = segment.strip()
            if segment:
                result.append(type_(segment))  # pyright: ignore[reportCallIssue]
        return result

    # Mark as cyclopts converter
    convert.__cyclopts_converter__ = True
    return convert


def comma_separated_enum(enum_type: type[T]) -> Callable[[str | list[str]], list[T]]:
    """Create a converter for comma-separated enum values.

    Parameters
    ----------
    enum_type
        The enum type.

    Returns
    -------
    Callable
        A converter function for cyclopts.
    """

    def convert(value: str | list[str]) -> list[T]:
        """Convert input to list of enum values.

        Parameters
        ----------
        value
            Input string or list.

        Returns
        -------
        list[T]
            Converted list of enum values.
        """
        if isinstance(value, list):
            result: list[T] = []
            for item in value:
                for segment in str(item).split(","):
                    segment = segment.strip()
                    if segment:
                        result.append(enum_type(segment))  # pyright: ignore[reportCallIssue]
            return result

        result = []
        for segment in value.split(","):
            segment = segment.strip()
            if segment:
                result.append(enum_type(segment))  # pyright: ignore[reportCallIssue]
        return result

    convert.__cyclopts_converter__ = True
    return convert
