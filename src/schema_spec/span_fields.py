"""Templated span field generators for byte-span columns.

Byte span fields (bstart, bend) are used throughout the codebase with
standard prefixes. This module provides templated helpers to generate
these fields consistently.

Supported prefixes:
    - "" (standard span: bstart, bend)
    - "call_" (callsite span: call_bstart, call_bend)
    - "name_" (name span: name_bstart, name_bend)
    - "def_" (definition span: def_bstart, def_bend)
    - "stmt_" (statement span: stmt_bstart, stmt_bend)
    - "alias_" (alias span: alias_bstart, alias_bend)
    - "callee_" (callee span: callee_bstart, callee_bend)
    - "container_def_" (container definition span)
    - "owner_def_" (owner definition span)
"""

from __future__ import annotations

from typing import Literal, cast, get_args

import pyarrow as pa

from datafusion_engine.arrow import interop
from schema_spec.arrow_type_coercion import coerce_arrow_type
from schema_spec.field_spec import FieldSpec

# Standard span prefixes used throughout the codebase
SpanPrefix = Literal[
    "",
    "call_",
    "name_",
    "def_",
    "stmt_",
    "alias_",
    "callee_",
    "container_def_",
    "owner_def_",
]

# All valid span prefixes for runtime validation
SPAN_PREFIXES: frozenset[SpanPrefix] = frozenset(
    cast("tuple[SpanPrefix, ...]", get_args(SpanPrefix))
)


def _normalize_prefix(prefix: str) -> str:
    """Normalize a prefix to ensure it ends with underscore if non-empty.

    Parameters
    ----------
    prefix
        Raw prefix string (may or may not end with underscore).

    Returns
    -------
    str
        Normalized prefix with trailing underscore if non-empty.
    """
    if not prefix:
        return ""
    return f"{prefix}_" if not prefix.endswith("_") else prefix


def span_field_names(prefix: SpanPrefix = "") -> tuple[str, str]:
    """Return the bstart/bend field names for a span prefix.

    Parameters
    ----------
    prefix
        Span prefix (e.g., "", "call_", "def_").

    Returns
    -------
    tuple[str, str]
        Tuple of (bstart_name, bend_name).

    Examples
    --------
    >>> span_field_names("")
    ('bstart', 'bend')
    >>> span_field_names("call_")
    ('call_bstart', 'call_bend')
    """
    normalized = _normalize_prefix(prefix)
    return (f"{normalized}bstart", f"{normalized}bend")


def make_span_field_specs(prefix: SpanPrefix = "") -> tuple[FieldSpec, FieldSpec]:
    """Return FieldSpec instances for byte-span columns with a given prefix.

    Parameters
    ----------
    prefix
        Span prefix (e.g., "", "call_", "def_").

    Returns
    -------
    tuple[FieldSpec, FieldSpec]
        Tuple of FieldSpec for (bstart, bend) columns.

    Examples
    --------
    >>> bstart, bend = make_span_field_specs("call_")
    >>> bstart.name
    'call_bstart'
    >>> bend.name
    'call_bend'
    """
    bstart_name, bend_name = span_field_names(prefix)
    return (
        FieldSpec(name=bstart_name, dtype=coerce_arrow_type(interop.int64())),
        FieldSpec(name=bend_name, dtype=coerce_arrow_type(interop.int64())),
    )


def make_span_pa_fields(prefix: SpanPrefix = "") -> tuple[pa.Field, pa.Field]:
    """Return pyarrow.Field instances for byte-span columns with a given prefix.

    Parameters
    ----------
    prefix
        Span prefix (e.g., "", "call_", "def_").

    Returns
    -------
    tuple[pa.Field, pa.Field]
        Tuple of pyarrow fields for (bstart, bend) columns.

    Examples
    --------
    >>> bstart, bend = make_span_pa_fields("def_")
    >>> bstart.name
    'def_bstart'
    """
    bstart_name, bend_name = span_field_names(prefix)
    return (
        pa.field(bstart_name, pa.int64()),
        pa.field(bend_name, pa.int64()),
    )


def make_span_pa_tuples(prefix: SpanPrefix = "") -> tuple[tuple[str, pa.DataType], ...]:
    """Return pyarrow field tuples for byte-span columns with a given prefix.

    This format is compatible with pa.struct() field definitions.

    Parameters
    ----------
    prefix
        Span prefix (e.g., "", "call_", "def_").

    Returns
    -------
    tuple[tuple[str, pa.DataType], ...]
        Tuple of (name, dtype) pairs for span columns.

    Examples
    --------
    >>> make_span_pa_tuples("stmt_")
    (('stmt_bstart', DataType(int64)), ('stmt_bend', DataType(int64)))
    """
    bstart_name, bend_name = span_field_names(prefix)
    return (
        (bstart_name, pa.int64()),
        (bend_name, pa.int64()),
    )


# Standard span type mapping for common prefixes
STANDARD_SPAN_TYPES: dict[SpanPrefix, str] = {
    "": "span",
    "call_": "call_span",
    "name_": "name_span",
    "def_": "def_span",
    "stmt_": "stmt_span",
    "alias_": "alias_span",
    "callee_": "callee_span",
    "container_def_": "container_def_span",
    "owner_def_": "owner_def_span",
}


__all__ = [
    "SPAN_PREFIXES",
    "STANDARD_SPAN_TYPES",
    "SpanPrefix",
    "make_span_field_specs",
    "make_span_pa_fields",
    "make_span_pa_tuples",
    "span_field_names",
]
