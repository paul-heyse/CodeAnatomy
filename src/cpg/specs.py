"""Specifications for CPG builders."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Protocol

from ibis.expr.types import Value

from cpg.kind_catalog import EntityKind
from cpg.prop_transforms import (
    expr_context_expr,
    expr_context_value,
    flag_to_bool,
    flag_to_bool_expr,
)

if TYPE_CHECKING:
    from cpg.kind_catalog import EdgeKindId, NodeKindId

type PropValueType = Literal["string", "int", "float", "bool", "json"]
type PropTransformFn = Callable[[object | None], object | None]
type PropTransformExprFn = Callable[[Value], Value]
type PropIncludeFn = Callable[[PropOptions], bool]

TRANSFORM_EXPR_CONTEXT = "expr_context"
TRANSFORM_FLAG_TO_BOOL = "flag_to_bool"
INCLUDE_HEAVY_JSON = "heavy_json"


class PropOptions(Protocol):
    """Minimal options needed for property include filters."""

    @property
    def include_heavy_json_props(self) -> bool:
        """Return whether heavy JSON property fields should be included."""
        ...


@dataclass(frozen=True)
class PropTransformSpec:
    """Bundle of value/expr transforms for property emission."""

    value_fn: PropTransformFn
    expr_fn: PropTransformExprFn


PROP_TRANSFORMS: dict[str, PropTransformSpec] = {
    TRANSFORM_EXPR_CONTEXT: PropTransformSpec(
        value_fn=expr_context_value,
        expr_fn=expr_context_expr,
    ),
    TRANSFORM_FLAG_TO_BOOL: PropTransformSpec(
        value_fn=flag_to_bool,
        expr_fn=flag_to_bool_expr,
    ),
}


def _include_heavy_json(options: PropOptions) -> bool:
    return options.include_heavy_json_props


PROP_INCLUDES: dict[str, PropIncludeFn] = {
    INCLUDE_HEAVY_JSON: _include_heavy_json,
}


def resolve_prop_transform(transform_id: str | None) -> PropTransformSpec | None:
    """Return the PropTransformSpec for a transform id.

    Returns
    -------
    PropTransformSpec | None
        Transform spec for the id, if registered.

    Raises
    ------
    ValueError
        Raised when the transform id is not registered.
    """
    if transform_id is None:
        return None
    spec = PROP_TRANSFORMS.get(transform_id)
    if spec is None:
        msg = f"Unknown prop transform id: {transform_id!r}"
        raise ValueError(msg)
    return spec


def resolve_prop_include(include_id: str | None) -> PropIncludeFn | None:
    """Return the include-if function for a filter id.

    Returns
    -------
    PropIncludeFn | None
        Include predicate for the id, if registered.

    Raises
    ------
    ValueError
        Raised when the include id is not registered.
    """
    if include_id is None:
        return None
    fn = PROP_INCLUDES.get(include_id)
    if fn is None:
        msg = f"Unknown prop include id: {include_id!r}"
        raise ValueError(msg)
    return fn


def filter_fields(
    fields: tuple[PropFieldSpec, ...] | list[PropFieldSpec],
    *,
    options: PropOptions,
) -> list[PropFieldSpec]:
    """Return property fields filtered by include-if rules.

    Returns
    -------
    list[PropFieldSpec]
        Fields that pass include-if filtering.
    """
    selected: list[PropFieldSpec] = []
    for prop_field in fields:
        include_if = resolve_prop_include(prop_field.include_if_id)
        if include_if is not None and not include_if(options):
            continue
        selected.append(prop_field)
    return selected


@dataclass(frozen=True)
class EdgeEmitSpec:
    """Mapping for emitting edges from a relation table."""

    edge_kind: EdgeKindId
    src_cols: tuple[str, ...]
    dst_cols: tuple[str, ...]
    origin: str
    default_resolution_method: str
    path_cols: tuple[str, ...] = ("path",)
    bstart_cols: tuple[str, ...] = ("bstart",)
    bend_cols: tuple[str, ...] = ("bend",)


@dataclass(frozen=True)
class NodeEmitSpec:
    """Spec for emitting anchored nodes."""

    node_kind: NodeKindId
    id_cols: tuple[str, ...]
    path_cols: tuple[str, ...] = ("path",)
    bstart_cols: tuple[str, ...] = ("bstart",)
    bend_cols: tuple[str, ...] = ("bend",)
    file_id_cols: tuple[str, ...] = ("file_id",)


@dataclass(frozen=True)
class NodePlanSpec:
    """Spec for emitting a node family from a table."""

    name: str
    table_ref: str
    emit: NodeEmitSpec
    preprocessor_id: str | None = None


@dataclass(frozen=True)
class PropFieldSpec:
    """Spec for emitting a property from a source column."""

    prop_key: str
    source_col: str | None = None
    literal: object | None = None
    transform_id: str | None = None
    include_if_id: str | None = None
    skip_if_none: bool = False
    value_type: PropValueType | None = None

    def __post_init__(self) -> None:
        """Validate that exactly one of literal or source_col is set.

        Raises
        ------
        ValueError
            Raised when literal and source_col are both set or both missing.
        """
        if (self.literal is None) == (self.source_col is None):
            msg = "PropFieldSpec requires exactly one of literal or source_col."
            raise ValueError(msg)

    def value_from(self, row: Mapping[str, object]) -> object | None:
        """Return the value to emit for this field.

        Parameters
        ----------
        row:
            Row mapping with candidate source columns.

        Returns
        -------
        object | None
            Value to emit for the property.
        """
        if self.literal is not None:
            value = self.literal
        elif self.source_col is not None:
            value = row.get(self.source_col)
        else:
            value = None
        transform = resolve_prop_transform(self.transform_id)
        if transform is None:
            return value
        return transform.value_fn(value)


@dataclass(frozen=True)
class PropTableSpec:
    """Spec for emitting properties from a table."""

    name: str
    table_ref: str
    entity_kind: EntityKind
    id_cols: tuple[str, ...]
    node_kind: NodeKindId | None = None
    fields: tuple[PropFieldSpec, ...] = field(default_factory=tuple)
    include_if_id: str | None = None
