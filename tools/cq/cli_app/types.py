"""Type definitions for cq CLI.

This module defines enums and custom converters for cyclopts.
Enum member names ARE the CLI tokens for correct cyclopts coercion.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from enum import StrEnum


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
    ldmd = "ldmd"  # LLM-friendly progressive disclosure markdown

    def __str__(self) -> str:
        """Return the CLI token.

        Returns:
        -------
        str
            String value used by the CLI.
        """
        return self.value


class ImpactBucket(StrEnum):
    """Impact level buckets."""

    low = "low"
    med = "med"
    high = "high"

    def __str__(self) -> str:
        """Return the CLI token.

        Returns:
        -------
        str
            String value used by the CLI.
        """
        return self.value


class ConfidenceBucket(StrEnum):
    """Confidence level buckets."""

    low = "low"
    med = "med"
    high = "high"

    def __str__(self) -> str:
        """Return the CLI token.

        Returns:
        -------
        str
            String value used by the CLI.
        """
        return self.value


class SeverityLevel(StrEnum):
    """Severity levels."""

    info = "info"
    warning = "warning"
    error = "error"

    def __str__(self) -> str:
        """Return the CLI token.

        Returns:
        -------
        str
            String value used by the CLI.
        """
        return self.value


class QueryLanguageToken(StrEnum):
    """Supported query language tokens."""

    auto = "auto"
    python = "python"
    rust = "rust"

    def __str__(self) -> str:
        """Return the CLI token.

        Returns:
        -------
        str
            String value used by the CLI.
        """
        return self.value


class NeighborhoodLanguageToken(StrEnum):
    """Supported neighborhood language tokens."""

    python = "python"
    rust = "rust"

    def __str__(self) -> str:
        """Return the CLI token.

        Returns:
        -------
        str
            String value used by the CLI.
        """
        return self.value


class ReportPreset(StrEnum):
    """Report preset options."""

    refactor_impact = "refactor-impact"
    safety_reliability = "safety-reliability"
    change_propagation = "change-propagation"
    dependency_health = "dependency-health"

    def __str__(self) -> str:
        """Return the CLI token.

        Returns:
        -------
        str
            String value used by the CLI.
        """
        return self.value


class SchemaKind(StrEnum):
    """Schema export payload kind."""

    result = "result"
    query = "query"
    components = "components"

    def __str__(self) -> str:
        """Return the CLI token.

        Returns:
        -------
        str
            String value used by the CLI.
        """
        return self.value


def _iter_token_values(value: object) -> Iterable[str]:
    items = value if isinstance(value, (list, tuple)) else [value]
    for item in items:
        if item is None:
            continue
        token_value = getattr(item, "value", item)
        text = str(token_value)
        for segment in text.split(","):
            cleaned = segment.strip()
            if cleaned:
                yield cleaned


_CONVERTER_ARGS_WITH_HINT = 2


def _converter_value(args: tuple[object, ...]) -> object:
    if len(args) == 1:
        return args[0]
    if len(args) == _CONVERTER_ARGS_WITH_HINT:
        return args[1]
    msg = "Expected converter args as (value) or (hint, value)."
    raise TypeError(msg)


def comma_separated_list[T](type_: Callable[[str], T]) -> Callable[..., list[T]]:
    """Create a converter for comma-separated values.

    This handles both:
    - Comma-separated strings: "a,b,c"
    - Repeated flags: --flag a --flag b

    Parameters
    ----------
    type_
        Callable that converts a single token to the target type.

    Returns:
    -------
    Callable
        A converter function for cyclopts.
    """

    def convert(*args: object) -> list[T]:
        """Convert input to list of typed values.

                Parameters
                ----------

        Args:
                    Converter inputs as (value) or (hint, value).

                    *args: TODO.
                    convert: TODO.

        Returns:
                -------
                list[T]
                    Converted list.

        """
        value = _converter_value(args)
        return [type_(item) for item in _iter_token_values(value)]

    # Mark as cyclopts converter
    convert.__dict__["__cyclopts_converter__"] = True
    return convert


def comma_separated_enum[T](enum_type: Callable[[str], T]) -> Callable[..., list[T]]:
    """Create a converter for comma-separated enum values.

    Parameters
    ----------
    enum_type
        Callable that converts a single token to the enum type.

    Returns:
    -------
    Callable
        A converter function for cyclopts.
    """

    def convert(*args: object) -> list[T]:
        """Convert input to list of enum values.

                Parameters
                ----------

        Args:
                    Converter inputs as (value) or (hint, value).

                    *args: TODO.
                    convert: TODO.

        Returns:
                -------
                list[T]
                    Converted list of enum values.

        """
        value = _converter_value(args)
        return [enum_type(item) for item in _iter_token_values(value)]

    convert.__dict__["__cyclopts_converter__"] = True
    return convert
