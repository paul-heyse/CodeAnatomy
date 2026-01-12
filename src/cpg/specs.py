"""Pydantic specifications for CPG builders."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from arrowdsl.pyarrow_protocols import TableLike
from cpg.kinds import EdgeKind, EntityKind, NodeKind

type TableGetter = Callable[[Mapping[str, TableLike]], TableLike | None]
type TableFilter = Callable[[TableLike], TableLike]


class PropOptions(Protocol):
    """Minimal options needed for property include filters."""

    @property
    def include_heavy_json_props(self) -> bool:
        """Return whether heavy JSON property fields should be included."""
        ...


class EdgeEmitSpec(BaseModel):
    """Mapping for emitting edges from a relation table."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    edge_kind: EdgeKind
    src_cols: tuple[str, ...]
    dst_cols: tuple[str, ...]
    path_cols: tuple[str, ...] = ("path",)
    bstart_cols: tuple[str, ...] = ("bstart",)
    bend_cols: tuple[str, ...] = ("bend",)
    origin: str
    default_resolution_method: str


class EdgePlanSpec(BaseModel):
    """Spec for emitting a family of edges."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    name: str
    option_flag: str
    relation_getter: TableGetter
    emit: EdgeEmitSpec
    filter_fn: TableFilter | None = None


class NodeEmitSpec(BaseModel):
    """Spec for emitting anchored nodes."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    node_kind: NodeKind
    id_cols: tuple[str, ...]
    path_cols: tuple[str, ...] = ("path",)
    bstart_cols: tuple[str, ...] = ("bstart",)
    bend_cols: tuple[str, ...] = ("bend",)
    file_id_cols: tuple[str, ...] = ("file_id",)


class NodePlanSpec(BaseModel):
    """Spec for emitting a node family from a table."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    name: str
    option_flag: str
    table_getter: TableGetter
    emit: NodeEmitSpec
    preprocessor: Callable[[TableLike], TableLike] | None = None


class PropFieldSpec(BaseModel):
    """Spec for emitting a property from a source column."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    prop_key: str
    source_col: str | None = None
    literal: object | None = None
    transform: Callable[[object | None], object | None] | None = None
    include_if: Callable[[PropOptions], bool] | None = None
    skip_if_none: bool = False

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
        if self.transform is None:
            return value
        return self.transform(value)


class PropTableSpec(BaseModel):
    """Spec for emitting properties from a table."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    name: str
    option_flag: str
    table_getter: TableGetter
    entity_kind: EntityKind
    id_cols: tuple[str, ...]
    node_kind: NodeKind | None = None
    fields: tuple[PropFieldSpec, ...] = Field(default_factory=tuple)
    include_if: Callable[[PropOptions], bool] | None = None
