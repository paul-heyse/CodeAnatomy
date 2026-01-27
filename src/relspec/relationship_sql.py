"""SQLGlot AST builders for relationship plans.

DEPRECATED: This module provides SQLGlot AST builders for legacy compatibility.
New code should use the DataFusion-native builders in relationship_datafusion.py.

The SQLGlot AST builders in this module are maintained for compatibility with
existing code that compiles SQLGlot to Ibis, but will be removed once all
internal authoring surfaces use DataFusion expression/DataFrame builders.

See relationship_datafusion.py for the DataFusion-native implementation.
"""

from __future__ import annotations

from dataclasses import dataclass

from cpg.kind_catalog import (
    EDGE_KIND_PY_CALLS_QNAME,
    EDGE_KIND_PY_CALLS_SYMBOL,
    EDGE_KIND_PY_DEFINES_SYMBOL,
    EDGE_KIND_PY_IMPORTS_SYMBOL,
    EDGE_KIND_PY_REFERENCES_SYMBOL,
)
from sqlglot_tools.compat import Expression, exp


def build_rel_name_symbol_sql(*, task_name: str, task_priority: int) -> Expression:
    """Build SQLGlot AST for name-based symbol relations.

    Parameters
    ----------
    task_name
        Task name for provenance metadata.
    task_priority
        Task priority for ordering metadata.

    Returns
    -------
    Expression
        SQLGlot SELECT expression for name symbol relations.
    """
    source = exp.to_table("cst_refs")
    return _select(
        source,
        _alias(_col("ref_id"), "ref_id"),
        _alias(_col("ref_text"), "symbol"),
        _alias(_col("symbol_roles"), "symbol_roles"),
        _alias(_col("path"), "path"),
        _alias(_coalesce(_col("edge_owner_file_id"), _col("file_id")), "edge_owner_file_id"),
        _alias(_col("bstart"), "bstart"),
        _alias(_col("bend"), "bend"),
        _alias(_lit("cst_ref_text"), "resolution_method"),
        _alias(_lit(0.5), "confidence"),
        _alias(_lit(0.5), "score"),
        _alias(_lit(task_name), "task_name"),
        _alias(_lit(task_priority), "task_priority"),
    )


def build_rel_import_symbol_sql(*, task_name: str, task_priority: int) -> Expression:
    """Build SQLGlot AST for import-based symbol relations.

    Parameters
    ----------
    task_name
        Task name for provenance metadata.
    task_priority
        Task priority for ordering metadata.

    Returns
    -------
    Expression
        SQLGlot SELECT expression for import symbol relations.
    """
    source = exp.to_table("cst_imports")
    symbol = _coalesce(_col("name"), _col("module"))
    return _select(
        source,
        _alias(_coalesce(_col("import_alias_id"), _col("import_id")), "import_alias_id"),
        _alias(symbol, "symbol"),
        _alias(_col("symbol_roles"), "symbol_roles"),
        _alias(_col("path"), "path"),
        _alias(_coalesce(_col("edge_owner_file_id"), _col("file_id")), "edge_owner_file_id"),
        _alias(_coalesce(_col("alias_bstart"), _col("stmt_bstart")), "bstart"),
        _alias(_coalesce(_col("alias_bend"), _col("stmt_bend")), "bend"),
        _alias(_lit("cst_import_name"), "resolution_method"),
        _alias(_lit(0.5), "confidence"),
        _alias(_lit(0.5), "score"),
        _alias(_lit(task_name), "task_name"),
        _alias(_lit(task_priority), "task_priority"),
    )


def build_rel_def_symbol_sql(*, task_name: str, task_priority: int) -> Expression:
    """Build SQLGlot AST for definition-based symbol relations.

    Parameters
    ----------
    task_name
        Task name for provenance metadata.
    task_priority
        Task priority for ordering metadata.

    Returns
    -------
    Expression
        SQLGlot SELECT expression for definition symbol relations.
    """
    source = exp.to_table("cst_defs")
    return _select(
        source,
        _alias(_col("def_id"), "def_id"),
        _alias(_coalesce(_col("name")), "symbol"),
        _alias(_col("symbol_roles"), "symbol_roles"),
        _alias(_col("path"), "path"),
        _alias(_coalesce(_col("edge_owner_file_id"), _col("file_id")), "edge_owner_file_id"),
        _alias(_coalesce(_col("name_bstart"), _col("def_bstart")), "bstart"),
        _alias(_coalesce(_col("name_bend"), _col("def_bend")), "bend"),
        _alias(_lit("cst_def_name"), "resolution_method"),
        _alias(_lit(0.6), "confidence"),
        _alias(_lit(0.6), "score"),
        _alias(_lit(task_name), "task_name"),
        _alias(_lit(task_priority), "task_priority"),
    )


def build_rel_callsite_symbol_sql(*, task_name: str, task_priority: int) -> Expression:
    """Build SQLGlot AST for callsite symbol relations.

    Parameters
    ----------
    task_name
        Task name for provenance metadata.
    task_priority
        Task priority for ordering metadata.

    Returns
    -------
    Expression
        SQLGlot SELECT expression for callsite symbol relations.
    """
    source = exp.to_table("cst_callsites")
    symbol = _coalesce(_col("callee_text"), _col("callee_dotted"))
    return _select(
        source,
        _alias(_col("call_id"), "call_id"),
        _alias(symbol, "symbol"),
        _alias(_col("symbol_roles"), "symbol_roles"),
        _alias(_col("path"), "path"),
        _alias(_coalesce(_col("edge_owner_file_id"), _col("file_id")), "edge_owner_file_id"),
        _alias(_col("call_bstart"), "call_bstart"),
        _alias(_col("call_bend"), "call_bend"),
        _alias(_lit("cst_callsite"), "resolution_method"),
        _alias(_lit(0.6), "confidence"),
        _alias(_lit(0.6), "score"),
        _alias(_lit(task_name), "task_name"),
        _alias(_lit(task_priority), "task_priority"),
    )


def build_rel_callsite_qname_sql(*, task_name: str, task_priority: int) -> Expression:
    """Build SQLGlot AST for callsite qname relations.

    Parameters
    ----------
    task_name
        Task name for provenance metadata.
    task_priority
        Task priority for ordering metadata.

    Returns
    -------
    Expression
        SQLGlot SELECT expression for callsite qname relations.
    """
    source = exp.to_table("callsite_qname_candidates_v1")
    qname = _col("qname")
    qname_id = _call_udf("stable_id", _lit("qname"), qname)
    return _select(
        source,
        _alias(_col("call_id"), "call_id"),
        _alias(qname_id, "qname_id"),
        _alias(_col("qname_source"), "qname_source"),
        _alias(_col("path"), "path"),
        _alias(_col("edge_owner_file_id"), "edge_owner_file_id"),
        _alias(_col("call_bstart"), "call_bstart"),
        _alias(_col("call_bend"), "call_bend"),
        _alias(_lit(0.5), "confidence"),
        _alias(_lit(0.5), "score"),
        _alias(_col("ambiguity_group_id"), "ambiguity_group_id"),
        _alias(_lit(task_name), "task_name"),
        _alias(_lit(task_priority), "task_priority"),
    )


def build_relation_output_sql() -> Expression:
    """Build SQLGlot AST for the relation output union.

    Returns
    -------
    Expression
        SQLGlot UNION expression for relation outputs.
    """
    selects = (
        _relation_output_from_name(),
        _relation_output_from_import(),
        _relation_output_from_def(),
        _relation_output_from_call_symbol(),
        _relation_output_from_call_qname(),
    )
    combined = selects[0]
    for item in selects[1:]:
        combined = exp.union(combined, item, distinct=False)
    return combined


@dataclass(frozen=True)
class RelationOutputSpec:
    src_col: str
    dst_col: str
    bstart_col: str
    bend_col: str
    kind: str
    origin: str
    qname_source: Expression | None = None
    ambiguity_group_id: Expression | None = None


def _relation_output_from_name() -> Expression:
    return _relation_output_base(
        "rel_name_symbol_v1",
        RelationOutputSpec(
            src_col="ref_id",
            dst_col="symbol",
            bstart_col="bstart",
            bend_col="bend",
            kind=str(EDGE_KIND_PY_REFERENCES_SYMBOL),
            origin="cst",
        ),
    )


def _relation_output_from_import() -> Expression:
    return _relation_output_base(
        "rel_import_symbol_v1",
        RelationOutputSpec(
            src_col="import_alias_id",
            dst_col="symbol",
            bstart_col="bstart",
            bend_col="bend",
            kind=str(EDGE_KIND_PY_IMPORTS_SYMBOL),
            origin="cst",
        ),
    )


def _relation_output_from_def() -> Expression:
    return _relation_output_base(
        "rel_def_symbol_v1",
        RelationOutputSpec(
            src_col="def_id",
            dst_col="symbol",
            bstart_col="bstart",
            bend_col="bend",
            kind=str(EDGE_KIND_PY_DEFINES_SYMBOL),
            origin="cst",
        ),
    )


def _relation_output_from_call_symbol() -> Expression:
    return _relation_output_base(
        "rel_callsite_symbol_v1",
        RelationOutputSpec(
            src_col="call_id",
            dst_col="symbol",
            bstart_col="call_bstart",
            bend_col="call_bend",
            kind=str(EDGE_KIND_PY_CALLS_SYMBOL),
            origin="cst",
        ),
    )


def _relation_output_from_call_qname() -> Expression:
    return _relation_output_base(
        "rel_callsite_qname_v1",
        RelationOutputSpec(
            src_col="call_id",
            dst_col="qname_id",
            bstart_col="call_bstart",
            bend_col="call_bend",
            kind=str(EDGE_KIND_PY_CALLS_QNAME),
            origin="cst",
            qname_source=_col("qname_source"),
            ambiguity_group_id=_col("ambiguity_group_id"),
        ),
    )


def _relation_output_base(source: str, spec: RelationOutputSpec) -> Expression:
    table = exp.to_table(source)
    qname_source = spec.qname_source or exp.Null()
    ambiguity_group_id = spec.ambiguity_group_id or exp.Null()
    return _select(
        table,
        _alias(_col(spec.src_col), "src"),
        _alias(_col(spec.dst_col), "dst"),
        _alias(_col("path"), "path"),
        _alias(_coalesce(_col("edge_owner_file_id"), _col("file_id")), "edge_owner_file_id"),
        _alias(_col(spec.bstart_col), "bstart"),
        _alias(_col(spec.bend_col), "bend"),
        _alias(_lit(spec.origin), "origin"),
        _alias(_col("resolution_method"), "resolution_method"),
        _alias(_col("binding_kind"), "binding_kind"),
        _alias(_col("def_site_kind"), "def_site_kind"),
        _alias(_col("use_kind"), "use_kind"),
        _alias(_lit(spec.kind), "kind"),
        _alias(_col("reason"), "reason"),
        _alias(_col("confidence"), "confidence"),
        _alias(_col("score"), "score"),
        _alias(_col("symbol_roles"), "symbol_roles"),
        _alias(qname_source, "qname_source"),
        _alias(ambiguity_group_id, "ambiguity_group_id"),
        _alias(_col("diag_source"), "diag_source"),
        _alias(_col("severity"), "severity"),
        _alias(_col("task_name"), "task_name"),
        _alias(_col("task_priority"), "task_priority"),
    )


def _select(source: Expression, *expressions: Expression) -> Expression:
    return exp.select(*expressions).from_(source)


def _alias(expression: Expression, name: str) -> Expression:
    return exp.alias_(expression, name)


def _col(name: str) -> Expression:
    return exp.column(name)


def _lit(value: object) -> Expression:
    if value is None:
        return exp.Null()
    if isinstance(value, bool):
        return exp.Boolean(this=value)
    if isinstance(value, (int, float)):
        return exp.Literal.number(value)
    return exp.Literal.string(str(value))


def _coalesce(*exprs: Expression) -> Expression:
    items = [expr for expr in exprs if expr is not None]
    if not items:
        return exp.Null()
    if len(items) == 1:
        return items[0]
    if hasattr(exp, "Coalesce"):
        return exp.Coalesce(this=items[0], expressions=items[1:])
    return exp.Anonymous(this="coalesce", expressions=items)


def _call_udf(name: str, *exprs: Expression) -> Expression:
    return exp.Anonymous(this=name, expressions=list(exprs))


__all__ = [
    "build_rel_callsite_qname_sql",
    "build_rel_callsite_symbol_sql",
    "build_rel_def_symbol_sql",
    "build_rel_import_symbol_sql",
    "build_rel_name_symbol_sql",
    "build_relation_output_sql",
]
