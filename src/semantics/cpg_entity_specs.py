"""Semantic-model driven CPG entity specifications."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import msgspec

from cpg import kind_catalog
from cpg.kind_catalog import NodeKindId
from cpg.node_families import NODE_FAMILY_DEFAULTS, NodeFamily
from cpg.prop_catalog import PROP_SPECS, PropSpec
from cpg.specs import (
    INCLUDE_HEAVY_JSON,
    TRANSFORM_EXPR_CONTEXT,
    NodeEmitSpec,
    NodePlanSpec,
    PropFieldSpec,
    PropTableSpec,
    PropValueType,
)
from utils.validation import validate_required_items

if TYPE_CHECKING:
    from collections.abc import Iterable

    from semantics.registry import SemanticModel, SemanticNormalizationSpec

type PropFieldInput = PropFieldSpec | str


def _value_type_for_key(key: str, prop_specs: Mapping[str, PropSpec]) -> PropValueType:
    spec = prop_specs.get(key)
    if spec is None:
        msg = f"Prop key {key!r} is not defined in the catalog."
        raise ValueError(msg)
    return spec.type


def _resolve_prop_field(
    key: str,
    source: PropFieldInput,
    *,
    prop_specs: Mapping[str, PropSpec],
) -> PropFieldSpec:
    if isinstance(source, PropFieldSpec):
        if source.prop_key != key:
            msg = f"PropFieldSpec key mismatch: {source.prop_key!r} != {key!r}"
            raise ValueError(msg)
        value_type = _value_type_for_key(key, prop_specs)
        if source.value_type is None:
            return msgspec.structs.replace(source, value_type=value_type)
        if source.value_type != value_type:
            msg = (
                f"PropFieldSpec value_type mismatch for {key!r}: "
                f"{source.value_type!r} != {value_type!r}"
            )
            raise ValueError(msg)
        return source
    value_type = _value_type_for_key(key, prop_specs)
    return PropFieldSpec(
        prop_key=key,
        source_col=source,
        value_type=value_type,
    )


def _validate_source_column(
    field: PropFieldSpec,
    *,
    source_columns: Sequence[str] | None,
) -> None:
    if source_columns is None or field.source_col is None:
        return
    validate_required_items(
        [field.source_col],
        source_columns,
        item_label=f"source columns for prop {field.prop_key!r}",
        error_type=ValueError,
    )


def prop_fields_from_catalog(
    *,
    source_map: Mapping[str, PropFieldInput],
    source_columns: Sequence[str] | None = None,
    prop_specs: Mapping[str, PropSpec] | None = None,
) -> tuple[PropFieldSpec, ...]:
    """Build PropFieldSpec entries validated against the property catalog.

    Parameters
    ----------
    source_map:
        Mapping of prop keys to PropFieldSpec or source column strings.
    source_columns:
        Optional sequence of available source columns for validation.
    prop_specs:
        Optional property catalog overrides.

    Returns:
    -------
    tuple[PropFieldSpec, ...]
        Prop field specs validated against the catalog.
    """
    resolved_specs = prop_specs or PROP_SPECS
    fields: list[PropFieldSpec] = []
    for key, source in source_map.items():
        field = _resolve_prop_field(key, source, prop_specs=resolved_specs)
        _validate_source_column(field, source_columns=source_columns)
        fields.append(field)
    return tuple(fields)


@dataclass(frozen=True)
class CpgEntitySpec:
    """Spec for a CPG node/prop family sourced from the semantic model."""

    name: str
    node_kind: NodeKindId
    id_cols: tuple[str, ...]
    node_table: str | None
    prop_source_map: Mapping[str, PropFieldInput] = field(default_factory=dict)
    prop_table: str | None = None
    node_name: str | None = None
    prop_name: str | None = None
    node_family: NodeFamily | None = None
    path_cols: tuple[str, ...] | None = None
    bstart_cols: tuple[str, ...] | None = None
    bend_cols: tuple[str, ...] | None = None
    file_id_cols: tuple[str, ...] | None = None
    prop_include_if_id: str | None = None

    def _resolve_cols(
        self,
        explicit: tuple[str, ...] | None,
        family_attr: str,
        default: tuple[str, ...],
    ) -> tuple[str, ...]:
        if explicit is not None:
            return explicit
        if self.node_family is not None:
            defaults = NODE_FAMILY_DEFAULTS.get(self.node_family)
            if defaults is not None:
                return getattr(defaults, family_attr, default)
        return default

    @property
    def resolved_path_cols(self) -> tuple[str, ...]:
        """Return resolved path columns."""
        return self._resolve_cols(self.path_cols, "path_cols", ())

    @property
    def resolved_bstart_cols(self) -> tuple[str, ...]:
        """Return resolved bstart columns."""
        return self._resolve_cols(self.bstart_cols, "bstart_cols", ())

    @property
    def resolved_bend_cols(self) -> tuple[str, ...]:
        """Return resolved bend columns."""
        return self._resolve_cols(self.bend_cols, "bend_cols", ())

    @property
    def resolved_file_id_cols(self) -> tuple[str, ...]:
        """Return resolved file_id columns."""
        return self._resolve_cols(self.file_id_cols, "file_id_cols", ())

    def to_node_plan(self) -> NodePlanSpec | None:
        """Return a NodePlanSpec for this entity when configured.

        Returns:
        -------
        NodePlanSpec | None
            Node plan spec when node emission is configured.
        """
        if self.node_table is None:
            return None
        return NodePlanSpec(
            name=self.node_name or self.name,
            table_ref=self.node_table,
            emit=NodeEmitSpec(
                node_kind=self.node_kind,
                id_cols=self.id_cols,
                path_cols=self.resolved_path_cols,
                bstart_cols=self.resolved_bstart_cols,
                bend_cols=self.resolved_bend_cols,
                file_id_cols=self.resolved_file_id_cols,
            ),
        )

    def to_prop_table(
        self,
        *,
        source_columns: Sequence[str] | None = None,
    ) -> PropTableSpec | None:
        """Return a PropTableSpec for this entity when configured.

        Returns:
        -------
        PropTableSpec | None
            Prop table spec when property emission is configured.
        """
        if not self.prop_source_map:
            return None
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
            entity_kind=kind_catalog.EntityKind.NODE,
            id_cols=self.id_cols,
            node_kind=self.node_kind,
            fields=prop_fields,
            include_if_id=self.prop_include_if_id,
        )


@dataclass(frozen=True)
class SemanticCpgOverrides:
    """Overrides for semantic-normalized CPG entities."""

    name: str
    node_kind: NodeKindId
    prop_source_map: Mapping[str, PropFieldInput] = field(default_factory=dict)
    id_cols: tuple[str, ...] | None = None
    node_family: NodeFamily | None = None
    path_cols: tuple[str, ...] | None = None
    bstart_cols: tuple[str, ...] | None = None
    bend_cols: tuple[str, ...] | None = None
    file_id_cols: tuple[str, ...] | None = None
    prop_table: str | None = None
    prop_name: str | None = None


_SEMANTIC_CPG_OVERRIDES: Mapping[str, SemanticCpgOverrides] = {
    "cst_refs": SemanticCpgOverrides(
        name="ref",
        node_kind=kind_catalog.NODE_KIND_CST_REF,
        node_family=NodeFamily.CST,
        prop_source_map={
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
    "cst_imports": SemanticCpgOverrides(
        name="import_alias",
        node_kind=kind_catalog.NODE_KIND_CST_IMPORT_ALIAS,
        node_family=NodeFamily.CST,
        id_cols=("import_alias_id", "import_id"),
        bstart_cols=("bstart", "alias_bstart"),
        bend_cols=("bend", "alias_bend"),
        prop_source_map={
            "import_kind": "kind",
            "module": "module",
            "relative_level": "relative_level",
            "imported_name": "name",
            "name": "name",
            "asname": "asname",
            "is_star": "is_star",
        },
    ),
    "cst_callsites": SemanticCpgOverrides(
        name="callsite",
        node_kind=kind_catalog.NODE_KIND_CST_CALLSITE,
        node_family=NodeFamily.CST,
        bstart_cols=("call_bstart", "bstart"),
        bend_cols=("call_bend", "bend"),
        prop_source_map={
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
    "cst_defs": SemanticCpgOverrides(
        name="definition",
        node_kind=kind_catalog.NODE_KIND_CST_DEF,
        node_family=NodeFamily.CST,
        bstart_cols=("bstart", "name_bstart"),
        bend_cols=("bend", "name_bend"),
        prop_source_map={
            "def_kind": "kind",
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
    "cst_call_args": SemanticCpgOverrides(
        name="call_arg",
        node_kind=kind_catalog.NODE_KIND_CST_CALL_ARG,
        node_family=NodeFamily.CST,
    ),
    "cst_docstrings": SemanticCpgOverrides(
        name="docstring",
        node_kind=kind_catalog.NODE_KIND_CST_DOCSTRING,
        node_family=NodeFamily.CST,
    ),
    "cst_decorators": SemanticCpgOverrides(
        name="decorator",
        node_kind=kind_catalog.NODE_KIND_CST_DECORATOR,
        node_family=NodeFamily.CST,
    ),
}


def _normalization_specs(
    model: SemanticModel,
) -> Iterable[SemanticNormalizationSpec]:
    return model.normalization_specs


def _spec_from_normalization(spec: SemanticNormalizationSpec) -> CpgEntitySpec:
    override = _SEMANTIC_CPG_OVERRIDES.get(spec.source_table)
    if override is None:
        msg = f"Missing CPG override for normalization source {spec.source_table!r}."
        raise ValueError(msg)
    id_cols = override.id_cols or (spec.spec.entity_id.out_col,)
    node_table = spec.output_name
    return CpgEntitySpec(
        name=override.name,
        node_kind=override.node_kind,
        id_cols=id_cols,
        node_table=node_table,
        prop_source_map=override.prop_source_map,
        prop_table=override.prop_table or node_table,
        prop_name=override.prop_name,
        node_family=override.node_family,
        path_cols=override.path_cols,
        bstart_cols=override.bstart_cols,
        bend_cols=override.bend_cols,
        file_id_cols=override.file_id_cols,
    )


EXPLICIT_CPG_ENTITY_SPECS: tuple[CpgEntitySpec, ...] = (
    CpgEntitySpec(
        name="file",
        node_kind=kind_catalog.NODE_KIND_PY_FILE,
        id_cols=("file_id",),
        node_table="repo_files_v1",
        node_family=NodeFamily.FILE,
        prop_source_map={
            "path": "path",
            "size_bytes": "size_bytes",
            "file_sha256": "file_sha256",
            "encoding": "encoding",
        },
        prop_table="repo_files_v1",
    ),
    CpgEntitySpec(
        name="sym_scope",
        node_kind=kind_catalog.NODE_KIND_SYM_SCOPE,
        id_cols=("scope_id",),
        node_table="symtable_scopes",
        node_family=NodeFamily.SYMTABLE,
        prop_source_map={
            "scope_id": "scope_id",
            "scope_type": "block_type",
            "scope_name": "name",
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
            "lineno": "lineno1",
            "is_meta_scope": "is_meta_scope",
            "path": "path",
        },
        bstart_cols=(),
        bend_cols=(),
    ),
    CpgEntitySpec(
        name="sym_symbol",
        node_kind=kind_catalog.NODE_KIND_SYM_SYMBOL,
        id_cols=("sym_symbol_id",),
        node_table="symtable_symbols",
        node_family=NodeFamily.SYMTABLE,
        prop_source_map={
            "scope_id": "scope_id",
            "name": "name",
            "namespace_count": "namespace_count",
            "namespace_block_ids": PropFieldSpec(
                prop_key="namespace_block_ids",
                source_col="namespace_block_ids",
                include_if_id=INCLUDE_HEAVY_JSON,
            ),
        },
        bstart_cols=(),
        bend_cols=(),
    ),
    CpgEntitySpec(
        name="py_scope",
        node_kind=kind_catalog.NODE_KIND_PY_SCOPE,
        id_cols=("scope_id",),
        node_table="symtable_scopes",
        node_family=NodeFamily.SYMTABLE,
        prop_source_map={
            "scope_id": "scope_id",
            "scope_type": "block_type",
            "path": "path",
        },
        bstart_cols=(),
        bend_cols=(),
    ),
    CpgEntitySpec(
        name="py_binding",
        node_kind=kind_catalog.NODE_KIND_PY_BINDING,
        id_cols=("binding_id",),
        node_table="symtable_bindings",
        node_family=NodeFamily.SYMTABLE,
        prop_source_map={
            "binding_id": "binding_id",
            "scope_id": "scope_id",
            "name": "name",
            "binding_kind": "binding_kind",
            "declared_here": "declared_here",
            "referenced_here": "referenced_here",
            "assigned_here": "assigned_here",
            "annotated_here": "annotated_here",
        },
        bstart_cols=(),
        bend_cols=(),
    ),
    CpgEntitySpec(
        name="py_def_site",
        node_kind=kind_catalog.NODE_KIND_PY_DEF_SITE,
        id_cols=("def_site_id",),
        node_table="symtable_def_sites",
        node_family=NodeFamily.SYMTABLE,
        prop_source_map={
            "binding_id": "binding_id",
            "name": "name",
            "def_site_kind": "def_site_kind",
            "anchor_confidence": "anchor_confidence",
            "anchor_reason": "anchor_reason",
            "ambiguity_group_id": "ambiguity_group_id",
        },
    ),
    CpgEntitySpec(
        name="py_use_site",
        node_kind=kind_catalog.NODE_KIND_PY_USE_SITE,
        id_cols=("use_site_id",),
        node_table="symtable_use_sites",
        node_family=NodeFamily.SYMTABLE,
        prop_source_map={
            "binding_id": "binding_id",
            "name": "name",
            "use_kind": "use_kind",
            "anchor_confidence": "anchor_confidence",
            "anchor_reason": "anchor_reason",
            "ambiguity_group_id": "ambiguity_group_id",
        },
    ),
    CpgEntitySpec(
        name="type_param",
        node_kind=kind_catalog.NODE_KIND_TYPE_PARAM,
        id_cols=("type_param_id",),
        node_table="symtable_type_params",
        node_family=NodeFamily.SYMTABLE,
        prop_source_map={
            "name": "name",
            "variance": "variance",
        },
        bstart_cols=(),
        bend_cols=(),
    ),
    CpgEntitySpec(
        name="qualified_name",
        node_kind=kind_catalog.NODE_KIND_PY_QUALIFIED_NAME,
        id_cols=("qname_id",),
        node_table=None,
        node_family=NodeFamily.SYMTABLE,
        prop_source_map={"qname": "qname"},
        file_id_cols=(),
    ),
    CpgEntitySpec(
        name="scip_symbol",
        node_kind=kind_catalog.NODE_KIND_SCIP_SYMBOL,
        id_cols=("symbol",),
        node_table="scip_symbol_information_v1",
        node_family=NodeFamily.SCIP,
        prop_source_map={
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
        prop_table="scip_symbol_information_v1",
    ),
    CpgEntitySpec(
        name="scip_external_symbol",
        node_kind=kind_catalog.NODE_KIND_SCIP_SYMBOL,
        id_cols=("symbol",),
        node_table=None,
        node_family=NodeFamily.SCIP,
        prop_source_map={
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
        prop_table="scip_external_symbol_information_v1",
        prop_name="scip_external_symbol_props",
    ),
    CpgEntitySpec(
        name="tree_sitter_node",
        node_kind=kind_catalog.NODE_KIND_TS_NODE,
        id_cols=("node_id",),
        node_table="ts_nodes",
        node_family=NodeFamily.TREESITTER,
        prop_source_map={
            "ts_type": "kind",
            "ts_kind_id": "kind_id",
            "ts_grammar_id": "grammar_id",
            "ts_grammar_name": "grammar_name",
            "ts_node_uid": "node_uid",
        },
    ),
    CpgEntitySpec(
        name="tree_sitter_error",
        node_kind=kind_catalog.NODE_KIND_TS_ERROR,
        id_cols=("error_id",),
        node_table="ts_errors",
        node_family=NodeFamily.TREESITTER,
        prop_source_map={
            "is_error": PropFieldSpec(prop_key="is_error", literal=True),
        },
    ),
    CpgEntitySpec(
        name="tree_sitter_missing",
        node_kind=kind_catalog.NODE_KIND_TS_MISSING,
        id_cols=("missing_id",),
        node_table="ts_missing",
        node_family=NodeFamily.TREESITTER,
        prop_source_map={
            "is_missing": PropFieldSpec(prop_key="is_missing", literal=True),
        },
    ),
    CpgEntitySpec(
        name="type_expr",
        node_kind=kind_catalog.NODE_KIND_TYPE_EXPR,
        id_cols=("type_expr_id",),
        node_table="type_exprs_norm",
        node_family=NodeFamily.TYPE,
        prop_source_map={
            "expr_text": "expr_text",
            "expr_kind": "expr_kind",
            "expr_role": "expr_role",
            "param_name": "param_name",
        },
    ),
    CpgEntitySpec(
        name="type",
        node_kind=kind_catalog.NODE_KIND_TYPE,
        id_cols=("type_id",),
        node_table="type_nodes",
        node_family=NodeFamily.TYPE,
        prop_source_map={
            "type_repr": "type_repr",
            "type_form": "type_form",
            "origin": "origin",
        },
        path_cols=(),
        bstart_cols=(),
        bend_cols=(),
        file_id_cols=(),
    ),
    CpgEntitySpec(
        name="diagnostic",
        node_kind=kind_catalog.NODE_KIND_DIAG,
        id_cols=("diag_id",),
        node_table="scip_diagnostics_v1",
        node_family=NodeFamily.DIAGNOSTIC,
        prop_source_map={
            "severity": "severity",
            "message": "message",
            "diag_source": "diag_source",
            "code": "code",
            "details": "details",
        },
    ),
)


def build_cpg_entity_specs(model: SemanticModel) -> tuple[CpgEntitySpec, ...]:
    """Build CPG entity specs from the semantic model.

    Returns:
    -------
    tuple[CpgEntitySpec, ...]
        CPG entity specs for node and property emission.
    """
    semantic_specs = [
        _spec_from_normalization(spec)
        for spec in _normalization_specs(model)
        if spec.include_in_cpg_nodes
    ]
    return (*semantic_specs, *EXPLICIT_CPG_ENTITY_SPECS)


__all__ = [
    "EXPLICIT_CPG_ENTITY_SPECS",
    "CpgEntitySpec",
    "build_cpg_entity_specs",
]
