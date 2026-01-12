"""Unified registry for CPG node/prop family specs."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from cpg.catalog import PlanRef
from cpg.contract_map import PropFieldInput, prop_fields_from_contract
from cpg.kinds import (
    SCIP_ROLE_FORWARD_DEFINITION,
    SCIP_ROLE_GENERATED,
    SCIP_ROLE_TEST,
    EntityKind,
    NodeKind,
)
from cpg.prop_transforms import expr_context_value, flag_to_bool
from cpg.specs import (
    NodeEmitSpec,
    NodePlanSpec,
    PropFieldSpec,
    PropOptions,
    PropTableSpec,
)


@dataclass(frozen=True)
class EntityFamilySpec:
    """Spec for a node/prop family with shared identity columns."""

    name: str
    node_kind: NodeKind
    id_cols: tuple[str, ...]
    node_table: PlanRef | None
    node_option_flag: str | None
    prop_fields: tuple[PropFieldSpec, ...] = ()
    prop_option_flag: str | None = "include_node_props"
    prop_table: PlanRef | None = None
    node_name: str | None = None
    prop_name: str | None = None
    path_cols: tuple[str, ...] = ()
    bstart_cols: tuple[str, ...] = ()
    bend_cols: tuple[str, ...] = ()
    file_id_cols: tuple[str, ...] = ()
    prop_include_if: Callable[[PropOptions], bool] | None = None

    def to_node_plan(self) -> NodePlanSpec | None:
        """Return a NodePlanSpec for this family when configured.

        Returns
        -------
        NodePlanSpec | None
            Node plan spec or ``None`` when not configured.
        """
        if self.node_table is None or self.node_option_flag is None:
            return None
        return NodePlanSpec(
            name=self.node_name or self.name,
            option_flag=self.node_option_flag,
            table_getter=self.node_table.getter(),
            emit=NodeEmitSpec(
                node_kind=self.node_kind,
                id_cols=self.id_cols,
                path_cols=self.path_cols,
                bstart_cols=self.bstart_cols,
                bend_cols=self.bend_cols,
                file_id_cols=self.file_id_cols,
            ),
        )

    def to_prop_table(self) -> PropTableSpec | None:
        """Return a PropTableSpec for this family when configured.

        Returns
        -------
        PropTableSpec | None
            Prop table spec or ``None`` when not configured.
        """
        table = self.prop_table or self.node_table
        if table is None or self.prop_option_flag is None:
            return None
        return PropTableSpec(
            name=self.prop_name or self.name,
            option_flag=self.prop_option_flag,
            table_getter=table.getter(),
            entity_kind=EntityKind.NODE,
            id_cols=self.id_cols,
            node_kind=self.node_kind,
            fields=self.prop_fields,
            include_if=self.prop_include_if,
        )


def _heavy_json(options: PropOptions) -> bool:
    return options.include_heavy_json_props


def _fields(
    node_kind: NodeKind,
    source_map: dict[str, PropFieldInput],
) -> tuple[PropFieldSpec, ...]:
    return prop_fields_from_contract(node_kind=node_kind, source_map=source_map)


ENTITY_FAMILY_SPECS: tuple[EntityFamilySpec, ...] = (
    EntityFamilySpec(
        name="file",
        node_kind=NodeKind.PY_FILE,
        id_cols=("file_id",),
        node_table=PlanRef("repo_files_nodes"),
        node_option_flag="include_file_nodes",
        prop_fields=_fields(
            NodeKind.PY_FILE,
            {
                "path": "path",
                "size_bytes": "size_bytes",
                "file_sha256": "file_sha256",
                "encoding": "encoding",
            },
        ),
        prop_table=PlanRef("repo_files"),
        path_cols=("path",),
        bstart_cols=("bstart",),
        bend_cols=("bend",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="name_ref",
        node_kind=NodeKind.CST_NAME_REF,
        id_cols=("name_ref_id",),
        node_table=PlanRef("cst_name_refs"),
        node_option_flag="include_name_ref_nodes",
        prop_fields=_fields(
            NodeKind.CST_NAME_REF,
            {
                "name": "name",
                "expr_context": PropFieldSpec(
                    prop_key="expr_context",
                    source_col="expr_ctx",
                    transform=expr_context_value,
                ),
            },
        ),
        path_cols=("path",),
        bstart_cols=("bstart",),
        bend_cols=("bend",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="import_alias",
        node_kind=NodeKind.CST_IMPORT_ALIAS,
        id_cols=("import_alias_id", "import_id"),
        node_table=PlanRef("cst_imports"),
        node_option_flag="include_import_alias_nodes",
        prop_fields=_fields(
            NodeKind.CST_IMPORT_ALIAS,
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
        node_kind=NodeKind.CST_CALLSITE,
        id_cols=("call_id",),
        node_table=PlanRef("cst_callsites"),
        node_option_flag="include_callsite_nodes",
        prop_fields=_fields(
            NodeKind.CST_CALLSITE,
            {
                "callee_shape": "callee_shape",
                "callee_text": "callee_text",
                "callee_dotted": "callee_dotted",
                "arg_count": "arg_count",
                "callee_qnames": PropFieldSpec(
                    prop_key="callee_qnames",
                    source_col="callee_qnames",
                    include_if=_heavy_json,
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
        node_kind=NodeKind.CST_DEF,
        id_cols=("def_id",),
        node_table=PlanRef("cst_defs"),
        node_option_flag="include_def_nodes",
        prop_fields=_fields(
            NodeKind.CST_DEF,
            {
                "def_kind": "def_kind_norm",
                "name": "name",
                "container_def_id": "container_def_id",
                "qnames": PropFieldSpec(
                    prop_key="qnames",
                    source_col="qnames",
                    include_if=_heavy_json,
                ),
            },
        ),
        prop_table=PlanRef("cst_defs_norm"),
        path_cols=("path",),
        bstart_cols=("bstart", "name_bstart"),
        bend_cols=("bend", "name_bend"),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="qualified_name",
        node_kind=NodeKind.PY_QUALIFIED_NAME,
        id_cols=("qname_id",),
        node_table=PlanRef("dim_qualified_names"),
        node_option_flag="include_qname_nodes",
        prop_fields=_fields(
            NodeKind.PY_QUALIFIED_NAME,
            {"qname": "qname"},
        ),
        path_cols=("path",),
        bstart_cols=("bstart",),
        bend_cols=("bend",),
    ),
    EntityFamilySpec(
        name="scip_symbol",
        node_kind=NodeKind.SCIP_SYMBOL,
        id_cols=("symbol",),
        node_table=PlanRef("scip_symbols"),
        node_option_flag="include_symbol_nodes",
        prop_fields=_fields(
            NodeKind.SCIP_SYMBOL,
            {
                "symbol": "symbol",
                "display_name": "display_name",
                "symbol_kind": "kind",
                "enclosing_symbol": "enclosing_symbol",
                "documentation": PropFieldSpec(
                    prop_key="documentation",
                    source_col="documentation",
                    include_if=_heavy_json,
                ),
                "signature_documentation": PropFieldSpec(
                    prop_key="signature_documentation",
                    source_col="signature_documentation",
                    include_if=_heavy_json,
                ),
            },
        ),
        prop_table=PlanRef("scip_symbol_information"),
    ),
    EntityFamilySpec(
        name="scip_external_symbol",
        node_kind=NodeKind.SCIP_SYMBOL,
        id_cols=("symbol",),
        node_table=None,
        node_option_flag=None,
        prop_fields=_fields(
            NodeKind.SCIP_SYMBOL,
            {
                "symbol": "symbol",
                "display_name": "display_name",
                "symbol_kind": "kind",
                "enclosing_symbol": "enclosing_symbol",
                "documentation": PropFieldSpec(
                    prop_key="documentation",
                    source_col="documentation",
                    include_if=_heavy_json,
                ),
                "signature_documentation": PropFieldSpec(
                    prop_key="signature_documentation",
                    source_col="signature_documentation",
                    include_if=_heavy_json,
                ),
            },
        ),
        prop_table=PlanRef("scip_external_symbol_information"),
        prop_name="scip_external_symbol_props",
    ),
    EntityFamilySpec(
        name="tree_sitter_node",
        node_kind=NodeKind.TS_NODE,
        id_cols=("ts_node_id",),
        node_table=PlanRef("ts_nodes"),
        node_option_flag="include_tree_sitter_nodes",
        prop_fields=_fields(
            NodeKind.TS_NODE,
            {
                "ts_type": "ts_type",
                "is_named": "is_named",
                "has_error": "has_error",
            },
        ),
        path_cols=("path",),
        bstart_cols=("start_byte",),
        bend_cols=("end_byte",),
        file_id_cols=("file_id",),
    ),
    EntityFamilySpec(
        name="tree_sitter_error",
        node_kind=NodeKind.TS_ERROR,
        id_cols=("ts_error_id",),
        node_table=PlanRef("ts_errors"),
        node_option_flag="include_tree_sitter_nodes",
        prop_fields=_fields(
            NodeKind.TS_ERROR,
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
        node_kind=NodeKind.TS_MISSING,
        id_cols=("ts_missing_id",),
        node_table=PlanRef("ts_missing"),
        node_option_flag="include_tree_sitter_nodes",
        prop_fields=_fields(
            NodeKind.TS_MISSING,
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
        node_kind=NodeKind.TYPE_EXPR,
        id_cols=("type_expr_id",),
        node_table=PlanRef("type_exprs_norm"),
        node_option_flag="include_type_nodes",
        prop_fields=_fields(
            NodeKind.TYPE_EXPR,
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
        node_kind=NodeKind.TYPE,
        id_cols=("type_id",),
        node_table=PlanRef("types_norm"),
        node_option_flag="include_type_nodes",
        prop_fields=_fields(
            NodeKind.TYPE,
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
        node_kind=NodeKind.DIAG,
        id_cols=("diag_id",),
        node_table=PlanRef("diagnostics_norm"),
        node_option_flag="include_diagnostic_nodes",
        prop_fields=_fields(
            NodeKind.DIAG,
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
        node_kind=NodeKind.RT_OBJECT,
        id_cols=("rt_id",),
        node_table=PlanRef("rt_objects"),
        node_option_flag="include_runtime_nodes",
        prop_fields=_fields(
            NodeKind.RT_OBJECT,
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
        node_kind=NodeKind.RT_SIGNATURE,
        id_cols=("sig_id",),
        node_table=PlanRef("rt_signatures"),
        node_option_flag="include_runtime_nodes",
        prop_fields=_fields(
            NodeKind.RT_SIGNATURE,
            {
                "sig_id": "sig_id",
                "signature": "signature",
                "return_annotation": "return_annotation",
            },
        ),
    ),
    EntityFamilySpec(
        name="runtime_param",
        node_kind=NodeKind.RT_SIGNATURE_PARAM,
        id_cols=("param_id",),
        node_table=PlanRef("rt_signature_params"),
        node_option_flag="include_runtime_nodes",
        prop_fields=_fields(
            NodeKind.RT_SIGNATURE_PARAM,
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
        node_kind=NodeKind.RT_MEMBER,
        id_cols=("member_id",),
        node_table=PlanRef("rt_members"),
        node_option_flag="include_runtime_nodes",
        prop_fields=_fields(
            NodeKind.RT_MEMBER,
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


def prop_table_specs() -> tuple[PropTableSpec, ...]:
    """Return prop table specs for all prop-emitting families.

    Returns
    -------
    tuple[PropTableSpec, ...]
        Prop table specs for configured families.
    """
    specs: list[PropTableSpec] = []
    for spec in ENTITY_FAMILY_SPECS:
        table = spec.to_prop_table()
        if table is not None:
            specs.append(table)
    return tuple(specs)


ROLE_FLAG_SPECS: tuple[tuple[str, int, str], ...] = (
    ("generated", SCIP_ROLE_GENERATED, "scip_role_generated"),
    ("test", SCIP_ROLE_TEST, "scip_role_test"),
    ("forward_definition", SCIP_ROLE_FORWARD_DEFINITION, "scip_role_forward_definition"),
)


def scip_role_flag_prop_spec() -> PropTableSpec:
    """Return prop spec for SCIP role flag properties.

    Returns
    -------
    PropTableSpec
        Prop table spec for role flags.
    """
    return PropTableSpec(
        name="scip_role_flags",
        option_flag="include_node_props",
        table_getter=PlanRef("scip_role_flags").getter(),
        entity_kind=EntityKind.NODE,
        id_cols=("symbol",),
        fields=tuple(
            PropFieldSpec(
                prop_key=prop_key,
                source_col=flag_name,
                transform=flag_to_bool,
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
        option_flag="include_edge_props",
        table_getter=PlanRef("cpg_edges").getter(),
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
            PropFieldSpec(prop_key="rule_name", source_col="rule_name", value_type="string"),
            PropFieldSpec(prop_key="rule_priority", source_col="rule_priority", value_type="int"),
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
