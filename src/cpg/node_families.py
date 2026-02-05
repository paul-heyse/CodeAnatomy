"""Node family definitions for CPG schema defaults.

Define common field bundles for CPG node families, enabling DRY schema
definitions where node types in the same family share common fields.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from datafusion_engine.arrow import interop
from schema_spec.arrow_type_coercion import coerce_arrow_type
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import FieldBundle, TableSchemaSpec

if TYPE_CHECKING:
    from collections.abc import Iterable


class NodeFamily(StrEnum):
    """Define node family categories for CPG nodes.

    Each family shares common field patterns that are inherited by
    node types in that family.
    """

    FILE = "file"
    CST = "cst"
    SYMTABLE = "symtable"
    SCIP = "scip"
    TREESITTER = "treesitter"
    TYPE = "type"
    DIAGNOSTIC = "diagnostic"
    RUNTIME = "runtime"
    BYTECODE = "bytecode"


def _identity_fields() -> tuple[FieldSpec, ...]:
    """Return common identity fields for anchored nodes.

    Returns
    -------
    tuple[FieldSpec, ...]
        Fields for node_id, node_kind, and file_id.
    """
    return (
        FieldSpec(name="node_id", dtype=coerce_arrow_type(interop.string()), nullable=False),
        FieldSpec(name="node_kind", dtype=coerce_arrow_type(interop.string()), nullable=False),
        FieldSpec(name="file_id", dtype=coerce_arrow_type(interop.string()), nullable=True),
    )


def _path_field() -> FieldSpec:
    """Return the path field spec.

    Returns
    -------
    FieldSpec
        Field for file path.
    """
    return FieldSpec(name="path", dtype=coerce_arrow_type(interop.string()), nullable=True)


def _span_fields() -> tuple[FieldSpec, ...]:
    """Return byte span fields (bstart, bend).

    Returns
    -------
    tuple[FieldSpec, ...]
        Fields for byte span columns.
    """
    return (
        FieldSpec(name="bstart", dtype=coerce_arrow_type(interop.int64()), nullable=True),
        FieldSpec(name="bend", dtype=coerce_arrow_type(interop.int64()), nullable=True),
    )


def _treesitter_span_fields() -> tuple[FieldSpec, ...]:
    """Return tree-sitter byte span fields (start_byte, end_byte).

    Returns
    -------
    tuple[FieldSpec, ...]
        Fields for tree-sitter byte span columns.
    """
    return (
        FieldSpec(name="start_byte", dtype=coerce_arrow_type(interop.int64()), nullable=True),
        FieldSpec(name="end_byte", dtype=coerce_arrow_type(interop.int64()), nullable=True),
    )


def _task_fields() -> tuple[FieldSpec, ...]:
    """Return task identity fields for CPG outputs.

    Returns
    -------
    tuple[FieldSpec, ...]
        Fields for task_name and task_priority.
    """
    return (
        FieldSpec(name="task_name", dtype=coerce_arrow_type(interop.string()), nullable=True),
        FieldSpec(name="task_priority", dtype=coerce_arrow_type(interop.int32()), nullable=True),
    )


def _scip_range_fields() -> tuple[FieldSpec, ...]:
    """Return SCIP line/character range fields.

    Returns
    -------
    tuple[FieldSpec, ...]
        Fields for SCIP range columns.
    """
    return (
        FieldSpec(name="start_line", dtype=coerce_arrow_type(interop.int32()), nullable=True),
        FieldSpec(name="start_char", dtype=coerce_arrow_type(interop.int32()), nullable=True),
        FieldSpec(name="end_line", dtype=coerce_arrow_type(interop.int32()), nullable=True),
        FieldSpec(name="end_char", dtype=coerce_arrow_type(interop.int32()), nullable=True),
    )


@dataclass(frozen=True)
class NodeFamilyDefaults:
    """Default column specs for a node family.

    Captures the common fields, span column names, and other defaults
    that are shared across all node types in a family.
    """

    family: NodeFamily
    fields: tuple[FieldSpec, ...]
    path_cols: tuple[str, ...] = ("path",)
    bstart_cols: tuple[str, ...] = ("bstart",)
    bend_cols: tuple[str, ...] = ("bend",)
    file_id_cols: tuple[str, ...] = ("file_id",)
    has_span: bool = True
    has_file_id: bool = True


@dataclass(frozen=True)
class NodeSpecOptions:
    """Options for creating a node spec with family defaults."""

    version: int | None = None
    required_non_null: tuple[str, ...] = ()
    key_fields: tuple[str, ...] = ()


def _file_family_fields() -> tuple[FieldSpec, ...]:
    """Return fields for FILE family nodes.

    Returns
    -------
    tuple[FieldSpec, ...]
        Complete field set for file nodes.
    """
    return (
        *_identity_fields(),
        _path_field(),
        *_span_fields(),
        *_task_fields(),
    )


def _cst_family_fields() -> tuple[FieldSpec, ...]:
    """Return fields for CST family nodes.

    Returns
    -------
    tuple[FieldSpec, ...]
        Complete field set for CST-derived nodes.
    """
    return (
        *_identity_fields(),
        _path_field(),
        *_span_fields(),
        *_task_fields(),
    )


def _symtable_family_fields() -> tuple[FieldSpec, ...]:
    """Return fields for SYMTABLE family nodes.

    Returns
    -------
    tuple[FieldSpec, ...]
        Complete field set for symtable-derived nodes.
    """
    return (
        *_identity_fields(),
        _path_field(),
        *_span_fields(),
        *_task_fields(),
    )


def _scip_family_fields() -> tuple[FieldSpec, ...]:
    """Return fields for SCIP family nodes.

    SCIP nodes use symbol as identifier and may lack file anchoring.

    Returns
    -------
    tuple[FieldSpec, ...]
        Complete field set for SCIP-derived nodes.
    """
    return (
        FieldSpec(name="node_id", dtype=coerce_arrow_type(interop.string()), nullable=False),
        FieldSpec(name="node_kind", dtype=coerce_arrow_type(interop.string()), nullable=False),
        FieldSpec(name="symbol", dtype=coerce_arrow_type(interop.string()), nullable=False),
        *_scip_range_fields(),
        *_task_fields(),
    )


def _treesitter_family_fields() -> tuple[FieldSpec, ...]:
    """Return fields for TREESITTER family nodes.

    Tree-sitter nodes use start_byte/end_byte instead of bstart/bend.

    Returns
    -------
    tuple[FieldSpec, ...]
        Complete field set for tree-sitter nodes.
    """
    return (
        *_identity_fields(),
        _path_field(),
        *_treesitter_span_fields(),
        *_task_fields(),
    )


def _type_family_fields() -> tuple[FieldSpec, ...]:
    """Return fields for TYPE family nodes.

    Returns
    -------
    tuple[FieldSpec, ...]
        Complete field set for type nodes.
    """
    return (
        *_identity_fields(),
        _path_field(),
        *_span_fields(),
        *_task_fields(),
    )


def _diagnostic_family_fields() -> tuple[FieldSpec, ...]:
    """Return fields for DIAGNOSTIC family nodes.

    Returns
    -------
    tuple[FieldSpec, ...]
        Complete field set for diagnostic nodes.
    """
    return (
        *_identity_fields(),
        _path_field(),
        *_span_fields(),
        *_task_fields(),
    )


def _runtime_family_fields() -> tuple[FieldSpec, ...]:
    """Return fields for RUNTIME family nodes.

    Runtime nodes represent introspection data and may lack file anchoring.

    Returns
    -------
    tuple[FieldSpec, ...]
        Complete field set for runtime introspection nodes.
    """
    return (
        FieldSpec(name="node_id", dtype=coerce_arrow_type(interop.string()), nullable=False),
        FieldSpec(name="node_kind", dtype=coerce_arrow_type(interop.string()), nullable=False),
        *_task_fields(),
    )


def _bytecode_family_fields() -> tuple[FieldSpec, ...]:
    """Return fields for BYTECODE family nodes.

    Returns
    -------
    tuple[FieldSpec, ...]
        Complete field set for bytecode-derived nodes.
    """
    return (
        *_identity_fields(),
        _path_field(),
        *_span_fields(),
        *_task_fields(),
    )


# Family defaults mapping
NODE_FAMILY_DEFAULTS: dict[NodeFamily, NodeFamilyDefaults] = {
    NodeFamily.FILE: NodeFamilyDefaults(
        family=NodeFamily.FILE,
        fields=_file_family_fields(),
        path_cols=("path",),
        bstart_cols=("bstart",),
        bend_cols=("bend",),
        file_id_cols=("file_id",),
    ),
    NodeFamily.CST: NodeFamilyDefaults(
        family=NodeFamily.CST,
        fields=_cst_family_fields(),
        path_cols=("path",),
        bstart_cols=("bstart",),
        bend_cols=("bend",),
        file_id_cols=("file_id",),
    ),
    NodeFamily.SYMTABLE: NodeFamilyDefaults(
        family=NodeFamily.SYMTABLE,
        fields=_symtable_family_fields(),
        path_cols=("path",),
        bstart_cols=("bstart",),
        bend_cols=("bend",),
        file_id_cols=("file_id",),
    ),
    NodeFamily.SCIP: NodeFamilyDefaults(
        family=NodeFamily.SCIP,
        fields=_scip_family_fields(),
        path_cols=(),
        bstart_cols=(),
        bend_cols=(),
        file_id_cols=(),
        has_span=False,
        has_file_id=False,
    ),
    NodeFamily.TREESITTER: NodeFamilyDefaults(
        family=NodeFamily.TREESITTER,
        fields=_treesitter_family_fields(),
        path_cols=("path",),
        bstart_cols=("start_byte",),
        bend_cols=("end_byte",),
        file_id_cols=("file_id",),
    ),
    NodeFamily.TYPE: NodeFamilyDefaults(
        family=NodeFamily.TYPE,
        fields=_type_family_fields(),
        path_cols=("path",),
        bstart_cols=("bstart",),
        bend_cols=("bend",),
        file_id_cols=("file_id",),
    ),
    NodeFamily.DIAGNOSTIC: NodeFamilyDefaults(
        family=NodeFamily.DIAGNOSTIC,
        fields=_diagnostic_family_fields(),
        path_cols=("path",),
        bstart_cols=("bstart",),
        bend_cols=("bend",),
        file_id_cols=("file_id",),
    ),
    NodeFamily.RUNTIME: NodeFamilyDefaults(
        family=NodeFamily.RUNTIME,
        fields=_runtime_family_fields(),
        path_cols=(),
        bstart_cols=(),
        bend_cols=(),
        file_id_cols=(),
        has_span=False,
        has_file_id=False,
    ),
    NodeFamily.BYTECODE: NodeFamilyDefaults(
        family=NodeFamily.BYTECODE,
        fields=_bytecode_family_fields(),
        path_cols=("path",),
        bstart_cols=("bstart",),
        bend_cols=("bend",),
        file_id_cols=("file_id",),
    ),
}


def node_family_bundle(family: NodeFamily) -> FieldBundle:
    """Return a FieldBundle for the given node family.

    Parameters
    ----------
    family
        Node family to get the field bundle for.

    Returns
    -------
    FieldBundle
        Bundle containing the family's default fields.
    """
    defaults = NODE_FAMILY_DEFAULTS[family]
    return FieldBundle(
        name=f"{family.value}_node",
        fields=defaults.fields,
        required_non_null=("node_id", "node_kind"),
    )


def node_family_defaults(family: NodeFamily) -> NodeFamilyDefaults:
    """Return the NodeFamilyDefaults for a given family.

    Parameters
    ----------
    family
        Node family to get defaults for.

    Returns
    -------
    NodeFamilyDefaults
        Default configuration for the family.
    """
    return NODE_FAMILY_DEFAULTS[family]


def node_spec_with_family(
    name: str,
    family: NodeFamily,
    extra_fields: Iterable[FieldSpec] | None = None,
    options: NodeSpecOptions | None = None,
) -> TableSchemaSpec:
    """Create a TableSchemaSpec with family defaults plus extra fields.

    Parameters
    ----------
    name
        Schema name for the table spec.
    family
        Node family providing default fields.
    extra_fields
        Additional fields specific to this node type.
    options
        Optional spec configuration (version, constraints).

    Returns
    -------
    TableSchemaSpec
        Table schema spec combining family defaults and extra fields.
    """
    opts = options or NodeSpecOptions()
    defaults = NODE_FAMILY_DEFAULTS[family]
    all_fields: list[FieldSpec] = list(defaults.fields)

    if extra_fields is not None:
        # Extend with extra fields, but don't duplicate
        existing_names = {f.name for f in all_fields}
        for field in extra_fields:
            if field.name not in existing_names:
                all_fields.append(field)
                existing_names.add(field.name)

    # Combine required non-null from family and explicit
    combined_required = ("node_id", "node_kind", *opts.required_non_null)

    return TableSchemaSpec(
        name=name,
        fields=all_fields,
        version=opts.version,
        required_non_null=combined_required,
        key_fields=opts.key_fields,
    )


def merge_field_bundles(*bundles: FieldBundle) -> tuple[FieldSpec, ...]:
    """Merge multiple field bundles, removing duplicates by name.

    Parameters
    ----------
    bundles
        Field bundles to merge.

    Returns
    -------
    tuple[FieldSpec, ...]
        Merged fields with duplicates removed (first occurrence wins).
    """
    seen: set[str] = set()
    result: list[FieldSpec] = []
    for bundle in bundles:
        for field in bundle.fields:
            if field.name not in seen:
                result.append(field)
                seen.add(field.name)
    return tuple(result)


__all__ = [
    "NODE_FAMILY_DEFAULTS",
    "NodeFamily",
    "NodeFamilyDefaults",
    "NodeSpecOptions",
    "merge_field_bundles",
    "node_family_bundle",
    "node_family_defaults",
    "node_spec_with_family",
]
