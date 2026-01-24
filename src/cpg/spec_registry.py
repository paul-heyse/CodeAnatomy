"""Unified registry for CPG node/prop family specs."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from cpg import kind_catalog
from cpg.constants import ROLE_FLAG_SPECS
from cpg.contract_map import prop_fields_from_catalog
from cpg.kind_catalog import EntityKind
from cpg.specs import (
    INCLUDE_HEAVY_JSON,
    TRANSFORM_EXPR_CONTEXT,
    TRANSFORM_FLAG_TO_BOOL,
    NodeEmitSpec,
    NodePlanSpec,
    PropFieldSpec,
    PropTableSpec,
)

if TYPE_CHECKING:
    from cpg.contract_map import PropFieldInput
    from cpg.kind_catalog import NodeKindId


@dataclass(frozen=True)
class EntityFamilySpec:
    """Spec for a node/prop family with shared identity columns."""

    name: str
    node_kind: NodeKindId
    id_cols: tuple[str, ...]
    node_table: str | None
    prop_source_map: Mapping[str, PropFieldInput] = field(default_factory=dict)
    prop_table: str | None = None
    node_name: str | None = None
    prop_name: str | None = None
    path_cols: tuple[str, ...] = ()
    bstart_cols: tuple[str, ...] = ()
    bend_cols: tuple[str, ...] = ()
    file_id_cols: tuple[str, ...] = ()
    prop_include_if_id: str | None = None

    def to_node_plan(self) -> NodePlanSpec | None:
        """Return a NodePlanSpec for this family when configured.

        Returns
        -------
        NodePlanSpec | None
            Node plan spec or ``None`` when not configured.
        """
        if self.node_table is None:
            return None
        return NodePlanSpec(
            name=self.node_name or self.name,
            table_ref=self.node_table,
            emit=NodeEmitSpec(
                node_kind=self.node_kind,
                id_cols=self.id_cols,
                path_cols=self.path_cols,
                bstart_cols=self.bstart_cols,
                bend_cols=self.bend_cols,
                file_id_cols=self.file_id_cols,
            ),
        )

    def to_prop_table(
        self,
        *,
        source_columns: Sequence[str] | None = None,
    ) -> PropTableSpec | None:
        """Return a PropTableSpec for this family when configured.

        Parameters
        ----------
        source_columns:
            Optional list of available source columns for validation.

        Returns
        -------
        PropTableSpec | None
            Prop table spec or ``None`` when not configured.
        """
        table = self.prop_table or self.node_table
        if table is None:
            return None
        prop_fields = prop_fields_from_catalog(
            source_map=self.prop_source_map,
            source_columns=source_columns,
        )
        return PropTableSpec(
            name=self.prop_name or self.name,
            table_ref=table,
            entity_kind=EntityKind.NODE,
            id_cols=self.id_cols,
            node_kind=self.node_kind,
            fields=prop_fields,
            include_if_id=self.prop_include_if_id,
        )


def _prop_source_map(
    node_kind: NodeKindId,
    source_map: Mapping[str, PropFieldInput],
) -> Mapping[str, PropFieldInput]:
    _ = node_kind
    return dict(source_map)


ENTITY_FAMILY_SPECS: tuple[EntityFamilySpec, ...] = (
    EntityFamilySpec(
        name="file",
        node_kind=kind_catalog.NODE_KIND_PY_FILE,
        id_cols=("file_id",),
        node_table="repo_files_nodes",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_PY_FILE,
            {
                "path": "path",
                "size_bytes": "size_bytes",
                "file_sha256": "file_sha256",
                "encoding": "encoding",
            },
        ),
        prop_table="repo_files",
        path_cols=("path",),
        bstart_cols=("bstart",),
        bend_cols=("bend",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="ref",
        node_kind=kind_catalog.NODE_KIND_CST_REF,
        id_cols=("ref_id",),
        node_table="cst_refs",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_CST_REF,
            {
                "ref_text": "ref_text",
                "ref_kind": "ref_kind",
                "expr_context": PropFieldSpec(
                    prop_key="expr_context",
                    source_col="expr_ctx",
                    transform_id=TRANSFORM_EXPR_CONTEXT,
                ),
                "scope_type": "scope_type",
                "scope_name": "scope_name",
                "scope_role": "scope_role",
                "parent_kind": "parent_kind",
                "inferred_type": "inferred_type",
            },
        ),
        path_cols=("path",),
        bstart_cols=("bstart",),
        bend_cols=("bend",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="import_alias",
        node_kind=kind_catalog.NODE_KIND_CST_IMPORT_ALIAS,
        id_cols=("import_alias_id", "import_id"),
        node_table="cst_imports",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_CST_IMPORT_ALIAS,
            {
                "import_kind": "kind",
                "module": "module",
                "relative_level": "relative_level",
                "imported_name": "name",
                "name": "name",
                "asname": "asname",
                "is_star": "is_star",
            },
        ),
        path_cols=("path",),
        bstart_cols=("bstart", "alias_bstart"),
        bend_cols=("bend", "alias_bend"),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="callsite",
        node_kind=kind_catalog.NODE_KIND_CST_CALLSITE,
        id_cols=("call_id",),
        node_table="cst_callsites",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_CST_CALLSITE,
            {
                "callee_shape": "callee_shape",
                "callee_text": "callee_text",
                "callee_dotted": "callee_dotted",
                "arg_count": "arg_count",
                "inferred_type": "inferred_type",
                "callee_qnames": PropFieldSpec(
                    prop_key="callee_qnames",
                    source_col="callee_qnames",
                    include_if_id=INCLUDE_HEAVY_JSON,
                ),
                "callee_fqns": PropFieldSpec(
                    prop_key="callee_fqns",
                    source_col="callee_fqns",
                    include_if_id=INCLUDE_HEAVY_JSON,
                ),
            },
        ),
        path_cols=("path",),
        bstart_cols=("call_bstart", "bstart"),
        bend_cols=("call_bend", "bend"),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="definition",
        node_kind=kind_catalog.NODE_KIND_CST_DEF,
        id_cols=("def_id",),
        node_table="cst_defs",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_CST_DEF,
            {
                "def_kind": "def_kind_norm",
                "name": "name",
                "container_def_id": "container_def_id",
                "qnames": PropFieldSpec(
                    prop_key="qnames",
                    source_col="qnames",
                    include_if_id=INCLUDE_HEAVY_JSON,
                ),
                "def_fqns": PropFieldSpec(
                    prop_key="def_fqns",
                    source_col="def_fqns",
                    include_if_id=INCLUDE_HEAVY_JSON,
                ),
                "docstring": "docstring",
                "decorator_count": "decorator_count",
            },
        ),
        prop_table="cst_defs_norm",
        path_cols=("path",),
        bstart_cols=("bstart", "name_bstart"),
        bend_cols=("bend", "name_bend"),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="sym_scope",
        node_kind=kind_catalog.NODE_KIND_SYM_SCOPE,
        id_cols=("scope_id",),
        node_table="symtable_scopes",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_SYM_SCOPE,
            {
                "scope_id": "scope_id",
                "scope_type": "scope_type",
                "scope_name": "scope_name",
                "function_partitions": PropFieldSpec(
                    prop_key="function_partitions",
                    source_col="function_partitions",
                    include_if_id=INCLUDE_HEAVY_JSON,
                ),
                "class_methods": PropFieldSpec(
                    prop_key="class_methods",
                    source_col="class_methods",
                    include_if_id=INCLUDE_HEAVY_JSON,
                ),
                "lineno": "lineno",
                "is_meta_scope": "is_meta_scope",
                "path": "path",
            },
        ),
        path_cols=("path",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="sym_symbol",
        node_kind=kind_catalog.NODE_KIND_SYM_SYMBOL,
        id_cols=("sym_symbol_id",),
        node_table="symtable_symbols",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_SYM_SYMBOL,
            {
                "scope_id": "scope_id",
                "name": "symbol_name",
                "namespace_count": "namespace_count",
                "namespace_block_ids": PropFieldSpec(
                    prop_key="namespace_block_ids",
                    source_col="namespace_block_ids",
                    include_if_id=INCLUDE_HEAVY_JSON,
                ),
                "is_local": "is_local",
                "is_global": "is_global",
                "is_nonlocal": "is_nonlocal",
                "is_free": "is_free",
                "is_parameter": "is_parameter",
                "is_imported": "is_imported",
                "is_assigned": "is_assigned",
                "is_referenced": "is_referenced",
                "is_annotated": "is_annotated",
                "is_namespace": "is_namespace",
            },
        ),
        path_cols=("path",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="py_scope",
        node_kind=kind_catalog.NODE_KIND_PY_SCOPE,
        id_cols=("scope_id",),
        node_table="symtable_scopes",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_PY_SCOPE,
            {
                "scope_id": "scope_id",
                "scope_type": "scope_type",
                "path": "path",
            },
        ),
        path_cols=("path",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="py_binding",
        node_kind=kind_catalog.NODE_KIND_PY_BINDING,
        id_cols=("binding_id",),
        node_table="symtable_bindings",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_PY_BINDING,
            {
                "binding_id": "binding_id",
                "scope_id": "scope_id",
                "name": "name",
                "binding_kind": "binding_kind",
                "declared_here": "declared_here",
                "referenced_here": "referenced_here",
                "assigned_here": "assigned_here",
                "annotated_here": "annotated_here",
            },
        ),
        path_cols=("path",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="py_def_site",
        node_kind=kind_catalog.NODE_KIND_PY_DEF_SITE,
        id_cols=("def_site_id",),
        node_table="symtable_def_sites",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_PY_DEF_SITE,
            {
                "binding_id": "binding_id",
                "name": "name",
                "def_site_kind": "def_site_kind",
                "anchor_confidence": "anchor_confidence",
                "anchor_reason": "anchor_reason",
                "ambiguity_group_id": "ambiguity_group_id",
            },
        ),
        path_cols=("path",),
        bstart_cols=("bstart",),
        bend_cols=("bend",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="py_use_site",
        node_kind=kind_catalog.NODE_KIND_PY_USE_SITE,
        id_cols=("use_site_id",),
        node_table="symtable_use_sites",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_PY_USE_SITE,
            {
                "binding_id": "binding_id",
                "name": "name",
                "use_kind": "use_kind",
                "anchor_confidence": "anchor_confidence",
                "anchor_reason": "anchor_reason",
                "ambiguity_group_id": "ambiguity_group_id",
            },
        ),
        path_cols=("path",),
        bstart_cols=("bstart",),
        bend_cols=("bend",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="type_param",
        node_kind=kind_catalog.NODE_KIND_TYPE_PARAM,
        id_cols=("type_param_id",),
        node_table="symtable_type_params",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_TYPE_PARAM,
            {
                "name": "name",
                "variance": "variance",
            },
        ),
        path_cols=("path",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="qualified_name",
        node_kind=kind_catalog.NODE_KIND_PY_QUALIFIED_NAME,
        id_cols=("qname_id",),
        node_table="dim_qualified_names",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_PY_QUALIFIED_NAME,
            {"qname": "qname"},
        ),
        path_cols=("path",),
        bstart_cols=("bstart",),
        bend_cols=("bend",),
    ),
    EntityFamilySpec(
        name="scip_symbol",
        node_kind=kind_catalog.NODE_KIND_SCIP_SYMBOL,
        id_cols=("symbol",),
        node_table="scip_symbols",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_SCIP_SYMBOL,
            {
                "symbol": "symbol",
                "display_name": "display_name",
                "symbol_kind": "kind_name",
                "enclosing_symbol": "enclosing_symbol",
                "documentation": PropFieldSpec(
                    prop_key="documentation",
                    source_col="documentation",
                    include_if_id=INCLUDE_HEAVY_JSON,
                ),
                "signature_text": PropFieldSpec(
                    prop_key="signature_text",
                    source_col="signature_text",
                    include_if_id=INCLUDE_HEAVY_JSON,
                ),
                "signature_language": PropFieldSpec(
                    prop_key="signature_language",
                    source_col="signature_language",
                    include_if_id=INCLUDE_HEAVY_JSON,
                ),
            },
        ),
        prop_table="scip_symbol_information",
    ),
    EntityFamilySpec(
        name="scip_external_symbol",
        node_kind=kind_catalog.NODE_KIND_SCIP_SYMBOL,
        id_cols=("symbol",),
        node_table=None,
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_SCIP_SYMBOL,
            {
                "symbol": "symbol",
                "display_name": "display_name",
                "symbol_kind": "kind_name",
                "enclosing_symbol": "enclosing_symbol",
                "documentation": PropFieldSpec(
                    prop_key="documentation",
                    source_col="documentation",
                    include_if_id=INCLUDE_HEAVY_JSON,
                ),
                "signature_text": PropFieldSpec(
                    prop_key="signature_text",
                    source_col="signature_text",
                    include_if_id=INCLUDE_HEAVY_JSON,
                ),
                "signature_language": PropFieldSpec(
                    prop_key="signature_language",
                    source_col="signature_language",
                    include_if_id=INCLUDE_HEAVY_JSON,
                ),
            },
        ),
        prop_table="scip_external_symbol_information",
        prop_name="scip_external_symbol_props",
    ),
    EntityFamilySpec(
        name="tree_sitter_node",
        node_kind=kind_catalog.NODE_KIND_TS_NODE,
        id_cols=("ts_node_id",),
        node_table="ts_nodes",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_TS_NODE,
            {
                "ts_type": "ts_type",
                "ts_kind_id": "ts_kind_id",
                "ts_grammar_id": "ts_grammar_id",
                "ts_grammar_name": "ts_grammar_name",
                "ts_node_uid": "ts_node_uid",
                "is_named": "is_named",
                "has_error": "has_error",
                "is_error": "is_error",
                "is_missing": "is_missing",
                "is_extra": "is_extra",
                "has_changes": "has_changes",
            },
        ),
        path_cols=("path",),
        bstart_cols=("start_byte",),
        bend_cols=("end_byte",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="tree_sitter_error",
        node_kind=kind_catalog.NODE_KIND_TS_ERROR,
        id_cols=("ts_error_id",),
        node_table="ts_errors",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_TS_ERROR,
            {
                "ts_type": "ts_type",
                "is_error": "is_error",
            },
        ),
        path_cols=("path",),
        bstart_cols=("start_byte",),
        bend_cols=("end_byte",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="tree_sitter_missing",
        node_kind=kind_catalog.NODE_KIND_TS_MISSING,
        id_cols=("ts_missing_id",),
        node_table="ts_missing",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_TS_MISSING,
            {
                "ts_type": "ts_type",
                "is_missing": "is_missing",
            },
        ),
        path_cols=("path",),
        bstart_cols=("start_byte",),
        bend_cols=("end_byte",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="type_expr",
        node_kind=kind_catalog.NODE_KIND_TYPE_EXPR,
        id_cols=("type_expr_id",),
        node_table="type_exprs_norm",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_TYPE_EXPR,
            {
                "expr_text": "expr_text",
                "expr_kind": "expr_kind",
                "expr_role": "expr_role",
                "param_name": "param_name",
            },
        ),
        path_cols=("path",),
        bstart_cols=("bstart",),
        bend_cols=("bend",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="type",
        node_kind=kind_catalog.NODE_KIND_TYPE,
        id_cols=("type_id",),
        node_table="types_norm",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_TYPE,
            {
                "type_repr": "type_repr",
                "type_form": "type_form",
                "origin": "origin",
            },
        ),
        path_cols=(),
        bstart_cols=(),
        bend_cols=(),
        file_id_cols=(),
    ),
    EntityFamilySpec(
        name="diagnostic",
        node_kind=kind_catalog.NODE_KIND_DIAG,
        id_cols=("diag_id",),
        node_table="diagnostics_norm",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_DIAG,
            {
                "severity": "severity",
                "message": "message",
                "diag_source": "diag_source",
                "code": "code",
                "details": "details",
            },
        ),
        path_cols=("path",),
        bstart_cols=("bstart",),
        bend_cols=("bend",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="runtime_object",
        node_kind=kind_catalog.NODE_KIND_RT_OBJECT,
        id_cols=("rt_id",),
        node_table="rt_objects",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_RT_OBJECT,
            {
                "rt_id": "rt_id",
                "module": "module",
                "qualname": "qualname",
                "name": "name",
                "obj_type": "obj_type",
                "source_path": "source_path",
                "source_line": "source_line",
            },
        ),
    ),
    EntityFamilySpec(
        name="runtime_signature",
        node_kind=kind_catalog.NODE_KIND_RT_SIGNATURE,
        id_cols=("sig_id",),
        node_table="rt_signatures",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_RT_SIGNATURE,
            {
                "sig_id": "sig_id",
                "signature": "signature",
                "return_annotation": "return_annotation",
            },
        ),
    ),
    EntityFamilySpec(
        name="runtime_param",
        node_kind=kind_catalog.NODE_KIND_RT_SIGNATURE_PARAM,
        id_cols=("param_id",),
        node_table="rt_signature_params",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_RT_SIGNATURE_PARAM,
            {
                "param_id": "param_id",
                "name": "name",
                "kind": "kind",
                "default_repr": "default_repr",
                "annotation_repr": "annotation_repr",
                "position": "position",
            },
        ),
    ),
    EntityFamilySpec(
        name="runtime_member",
        node_kind=kind_catalog.NODE_KIND_RT_MEMBER,
        id_cols=("member_id",),
        node_table="rt_members",
        prop_source_map=_prop_source_map(
            kind_catalog.NODE_KIND_RT_MEMBER,
            {
                "member_id": "member_id",
                "name": "name",
                "member_kind": "member_kind",
                "value_repr": "value_repr",
                "value_module": "value_module",
                "value_qualname": "value_qualname",
            },
        ),
    ),
)


def node_plan_specs() -> tuple[NodePlanSpec, ...]:
    """Return node plan specs for all node-emitting families.

    Returns
    -------
    tuple[NodePlanSpec, ...]
        Node plan specs for configured families.
    """
    specs: list[NodePlanSpec] = []
    for spec in ENTITY_FAMILY_SPECS:
        plan = spec.to_node_plan()
        if plan is not None:
            specs.append(plan)
    return tuple(specs)


def prop_table_specs(
    *,
    source_columns_lookup: Callable[[str], Sequence[str] | None] | None = None,
) -> tuple[PropTableSpec, ...]:
    """Return prop table specs for all prop-emitting families.

    Parameters
    ----------
    source_columns_lookup:
        Optional function returning source columns for a table name.

    Returns
    -------
    tuple[PropTableSpec, ...]
        Prop table specs for configured families.
    """
    specs: list[PropTableSpec] = []
    for spec in ENTITY_FAMILY_SPECS:
        table_name = spec.prop_table or spec.node_table
        source_columns = None
        if table_name is not None and source_columns_lookup is not None:
            source_columns = source_columns_lookup(table_name)
        table = spec.to_prop_table(source_columns=source_columns)
        if table is not None:
            specs.append(table)
    return tuple(specs)


def scip_role_flag_prop_spec() -> PropTableSpec:
    """Return prop spec for SCIP role flag properties.

    Returns
    -------
    PropTableSpec
        Prop table spec for role flags.
    """
    return PropTableSpec(
        name="scip_role_flags",
        table_ref="scip_role_flags",
        entity_kind=EntityKind.NODE,
        id_cols=("symbol",),
        fields=tuple(
            PropFieldSpec(
                prop_key=prop_key,
                source_col=flag_name,
                transform_id=TRANSFORM_FLAG_TO_BOOL,
                skip_if_none=True,
                value_type="bool",
            )
            for flag_name, _, prop_key in ROLE_FLAG_SPECS
        ),
    )


def edge_prop_spec() -> PropTableSpec:
    """Return prop spec for edge metadata properties.

    Returns
    -------
    PropTableSpec
        Prop table spec for edge metadata.
    """
    return PropTableSpec(
        name="edge_props",
        table_ref="cpg_edges",
        entity_kind=EntityKind.EDGE,
        id_cols=("edge_id",),
        fields=(
            PropFieldSpec(prop_key="edge_kind", source_col="edge_kind", value_type="string"),
            PropFieldSpec(prop_key="origin", source_col="origin", value_type="string"),
            PropFieldSpec(
                prop_key="resolution_method", source_col="resolution_method", value_type="string"
            ),
            PropFieldSpec(prop_key="confidence", source_col="confidence", value_type="float"),
            PropFieldSpec(prop_key="score", source_col="score", value_type="float"),
            PropFieldSpec(prop_key="symbol_roles", source_col="symbol_roles", value_type="int"),
            PropFieldSpec(prop_key="qname_source", source_col="qname_source", value_type="string"),
            PropFieldSpec(
                prop_key="ambiguity_group_id", source_col="ambiguity_group_id", value_type="string"
            ),
            PropFieldSpec(prop_key="task_name", source_col="task_name", value_type="string"),
            PropFieldSpec(prop_key="task_priority", source_col="task_priority", value_type="int"),
        ),
    )


__all__ = [
    "ENTITY_FAMILY_SPECS",
    "ROLE_FLAG_SPECS",
    "EntityFamilySpec",
    "edge_prop_spec",
    "node_plan_specs",
    "prop_table_specs",
    "scip_role_flag_prop_spec",
]
