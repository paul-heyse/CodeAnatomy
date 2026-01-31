"""Dynamic view registry for DataFusion schema views."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Final

import pyarrow as pa
from datafusion import SessionContext, col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr

from datafusion_engine.arrow.field_builders import (
    bool_field,
    int32_field,
    int64_field,
    string_field,
)
from datafusion_engine.schema.introspection import table_names_snapshot
from datafusion_engine.schema.registry import (
    SCIP_VIEW_NAMES,
    extract_nested_dataset_names,
    nested_base_df,
    nested_path_for,
)
from datafusion_engine.udf.shims import (
    arrow_metadata,
    col_to_byte,
    list_extract,
    map_entries,
    map_extract,
    map_keys,
    map_values,
    span_make,
    union_extract,
    union_tag,
)
from datafusion_engine.udf.shims import prefixed_hash_parts64 as prefixed_hash64
from datafusion_engine.udf.shims import stable_id_parts as stable_id
from datafusion_engine.views.graph import ViewNode
from schema_spec.view_specs import ViewSpec, ViewSpecInputs, view_spec_from_builder
from utils.registry_protocol import ImmutableRegistry, MutableRegistry
from utils.uuid_factory import uuid7_hex

if TYPE_CHECKING:
    from datafusion_engine.lineage.scan import ScanUnit
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.udf.platform import RustUdfPlatformOptions


def _arrow_cast(expr: Expr, data_type: str) -> Expr:
    return f.arrow_cast(expr, lit(data_type))


def _null_expr(data_type: str) -> Expr:
    return _arrow_cast(lit(None), data_type)


@dataclass(frozen=True)
class SpanComponents:
    """Span component expressions for nested span structs."""

    start_line: Expr | None = None
    start_col: Expr | None = None
    end_line: Expr | None = None
    end_col: Expr | None = None
    col_unit: Expr | None = None
    end_exclusive: Expr | None = None
    byte_start: Expr | None = None
    byte_len: Expr | None = None


def _span_struct_from_components(components: SpanComponents) -> Expr:
    null_i32 = _null_expr("Int32")
    null_bool = _null_expr("Boolean")
    null_str = _null_expr("Utf8")
    start_line_expr = (
        _arrow_cast(components.start_line, "Int32")
        if components.start_line is not None
        else null_i32
    )
    start_col_expr = (
        _arrow_cast(components.start_col, "Int32") if components.start_col is not None else null_i32
    )
    end_line_expr = (
        _arrow_cast(components.end_line, "Int32") if components.end_line is not None else null_i32
    )
    end_col_expr = (
        _arrow_cast(components.end_col, "Int32") if components.end_col is not None else null_i32
    )
    col_unit_expr = (
        _arrow_cast(components.col_unit, "Utf8") if components.col_unit is not None else null_str
    )
    end_exclusive_expr = (
        _arrow_cast(components.end_exclusive, "Boolean")
        if components.end_exclusive is not None
        else null_bool
    )
    byte_start_expr = (
        _arrow_cast(components.byte_start, "Int32")
        if components.byte_start is not None
        else null_i32
    )
    byte_len_expr = (
        _arrow_cast(components.byte_len, "Int32") if components.byte_len is not None else null_i32
    )
    return f.named_struct(
        [
            ("start", f.named_struct([("line0", start_line_expr), ("col", start_col_expr)])),
            ("end", f.named_struct([("line0", end_line_expr), ("col", end_col_expr)])),
            ("end_exclusive", end_exclusive_expr),
            ("col_unit", col_unit_expr),
            (
                "byte_span",
                f.named_struct([("byte_start", byte_start_expr), ("byte_len", byte_len_expr)]),
            ),
        ]
    )


def _byte_span_struct(
    bstart: Expr,
    bend: Expr,
    *,
    col_unit: Expr | None = None,
    end_exclusive: Expr | None = None,  # noqa: ARG001
) -> Expr:
    """Build a span struct from byte offsets using span_make UDF.

    Parameters
    ----------
    bstart
        Byte start offset expression.
    bend
        Byte end offset expression.
    col_unit
        Optional column unit expression (defaults to "byte").
    end_exclusive
        Ignored; kept for backward compatibility.

    Returns
    -------
    Expr
        Span struct from span_make UDF.
    """
    unit = col_unit if col_unit is not None else lit("byte")
    return span_make(bstart, bend, unit)


def _span_byte_start(span: Expr) -> Expr:
    return (span["byte_span"])["byte_start"]


def _span_byte_end(span: Expr) -> Expr:
    byte_start = _span_byte_start(span)
    byte_len = (span["byte_span"])["byte_len"]
    null_i32 = _null_expr("Int32")
    ok = byte_start.is_not_null() & byte_len.is_not_null()
    return f.when(ok, byte_start + byte_len).otherwise(null_i32)


def _span_struct(span: Expr) -> Expr:
    return f.named_struct(
        [
            ("start", span["start"]),
            ("end", span["end"]),
            ("end_exclusive", span["end_exclusive"]),
            ("col_unit", span["col_unit"]),
            ("byte_span", span["byte_span"]),
        ]
    )


def _ast_record(span: Expr, attrs: Expr) -> Expr:
    return f.named_struct([("span", _span_struct(span)), ("attrs", attrs)])


def _view_exprs(name: str) -> tuple[Expr, ...]:
    exprs = VIEW_SELECT_REGISTRY.get(name)
    if exprs is None:
        msg = f"Unknown view name: {name!r}."
        raise KeyError(msg)
    return exprs


_VIEW_SELECT_EXPRS: dict[str, tuple[Expr, ...]] = {
    "ast_call_attrs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ast_id").alias("ast_id"),
        col("parent_ast_id").alias("parent_ast_id"),
        col("func_kind").alias("func_kind"),
        col("func_name").alias("func_name"),
        (col("kv"))["key"].alias("attr_key"),
        (col("kv"))["value"].alias("attr_value"),
    ),
    "ast_calls": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ast_id").alias("ast_id"),
        col("parent_ast_id").alias("parent_ast_id"),
        col("func_kind").alias("func_kind"),
        col("func_name").alias("func_name"),
        _arrow_cast(list_extract(map_extract(col("attrs"), "arg_count"), 1), "Int32").alias(
            "arg_count"
        ),
        _arrow_cast(list_extract(map_extract(col("attrs"), "keyword_count"), 1), "Int32").alias(
            "keyword_count"
        ),
        _arrow_cast(list_extract(map_extract(col("attrs"), "starred_count"), 1), "Int32").alias(
            "starred_count"
        ),
        _arrow_cast(list_extract(map_extract(col("attrs"), "kw_star_count"), 1), "Int32").alias(
            "kw_star_count"
        ),
        _arrow_cast((((col("span"))["start"])["line0"] + lit(1)), "Int64").alias("lineno"),
        _arrow_cast(((col("span"))["start"])["col"], "Int64").alias("col_offset"),
        _arrow_cast((((col("span"))["end"])["line0"] + lit(1)), "Int64").alias("end_lineno"),
        _arrow_cast(((col("span"))["end"])["col"], "Int64").alias("end_col_offset"),
        _arrow_cast(lit(1), "Int32").alias("line_base"),
        _arrow_cast((col("span"))["col_unit"], "Utf8").alias("col_unit"),
        _arrow_cast((col("span"))["end_exclusive"], "Boolean").alias("end_exclusive"),
        _span_struct(col("span")).alias("span"),
        col("attrs").alias("attrs"),
        _ast_record(col("span"), col("attrs")).alias("ast_record"),
    ),
    "ast_def_attrs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ast_id").alias("ast_id"),
        col("parent_ast_id").alias("parent_ast_id"),
        col("kind").alias("kind"),
        col("name").alias("name"),
        (col("kv"))["key"].alias("attr_key"),
        (col("kv"))["value"].alias("attr_value"),
    ),
    "ast_defs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ast_id").alias("ast_id"),
        col("parent_ast_id").alias("parent_ast_id"),
        col("kind").alias("kind"),
        col("name").alias("name"),
        _arrow_cast(list_extract(map_extract(col("attrs"), "decorator_count"), 1), "Int32").alias(
            "decorator_count"
        ),
        _arrow_cast(list_extract(map_extract(col("attrs"), "arg_count"), 1), "Int32").alias(
            "arg_count"
        ),
        _arrow_cast(list_extract(map_extract(col("attrs"), "posonly_count"), 1), "Int32").alias(
            "posonly_count"
        ),
        _arrow_cast(list_extract(map_extract(col("attrs"), "kwonly_count"), 1), "Int32").alias(
            "kwonly_count"
        ),
        _arrow_cast(list_extract(map_extract(col("attrs"), "type_params_count"), 1), "Int32").alias(
            "type_params_count"
        ),
        _arrow_cast(list_extract(map_extract(col("attrs"), "base_count"), 1), "Int32").alias(
            "base_count"
        ),
        _arrow_cast(list_extract(map_extract(col("attrs"), "keyword_count"), 1), "Int32").alias(
            "keyword_count"
        ),
        _arrow_cast((((col("span"))["start"])["line0"] + lit(1)), "Int64").alias("lineno"),
        _arrow_cast(((col("span"))["start"])["col"], "Int64").alias("col_offset"),
        _arrow_cast((((col("span"))["end"])["line0"] + lit(1)), "Int64").alias("end_lineno"),
        _arrow_cast(((col("span"))["end"])["col"], "Int64").alias("end_col_offset"),
        _arrow_cast(lit(1), "Int32").alias("line_base"),
        _arrow_cast((col("span"))["col_unit"], "Utf8").alias("col_unit"),
        _arrow_cast((col("span"))["end_exclusive"], "Boolean").alias("end_exclusive"),
        _span_struct(col("span")).alias("span"),
        col("attrs").alias("attrs"),
        _ast_record(col("span"), col("attrs")).alias("ast_record"),
    ),
    "ast_docstrings": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("owner_ast_id").alias("owner_ast_id"),
        col("owner_kind").alias("owner_kind"),
        col("owner_name").alias("owner_name"),
        col("docstring").alias("docstring"),
        col("source").alias("source"),
        _arrow_cast((((col("span"))["start"])["line0"] + lit(1)), "Int64").alias("lineno"),
        _arrow_cast(((col("span"))["start"])["col"], "Int64").alias("col_offset"),
        _arrow_cast((((col("span"))["end"])["line0"] + lit(1)), "Int64").alias("end_lineno"),
        _arrow_cast(((col("span"))["end"])["col"], "Int64").alias("end_col_offset"),
        _arrow_cast(lit(1), "Int32").alias("line_base"),
        _arrow_cast((col("span"))["col_unit"], "Utf8").alias("col_unit"),
        _arrow_cast((col("span"))["end_exclusive"], "Boolean").alias("end_exclusive"),
        _span_struct(col("span")).alias("span"),
        col("attrs").alias("attrs"),
        _ast_record(col("span"), col("attrs")).alias("ast_record"),
    ),
    "ast_edge_attrs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("src").alias("src"),
        col("dst").alias("dst"),
        col("kind").alias("kind"),
        col("slot").alias("slot"),
        col("idx").alias("idx"),
        (col("kv"))["key"].alias("attr_key"),
        (col("kv"))["value"].alias("attr_value"),
    ),
    "ast_edges": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("src").alias("src"),
        col("dst").alias("dst"),
        col("kind").alias("kind"),
        col("slot").alias("slot"),
        col("idx").alias("idx"),
        _arrow_cast(col("attrs"), "Map(Utf8, Utf8)").alias("attrs"),
    ),
    "ast_errors": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("error_type").alias("error_type"),
        col("message").alias("message"),
        _arrow_cast((((col("span"))["start"])["line0"] + lit(1)), "Int64").alias("lineno"),
        _arrow_cast(((col("span"))["start"])["col"], "Int64").alias("col_offset"),
        _arrow_cast((((col("span"))["end"])["line0"] + lit(1)), "Int64").alias("end_lineno"),
        _arrow_cast(((col("span"))["end"])["col"], "Int64").alias("end_col_offset"),
        _arrow_cast(lit(1), "Int32").alias("line_base"),
        _arrow_cast((col("span"))["col_unit"], "Utf8").alias("col_unit"),
        _arrow_cast((col("span"))["end_exclusive"], "Boolean").alias("end_exclusive"),
        _span_struct(col("span")).alias("span"),
        col("attrs").alias("attrs"),
        _ast_record(col("span"), col("attrs")).alias("ast_record"),
    ),
    "ast_imports": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ast_id").alias("ast_id"),
        col("parent_ast_id").alias("parent_ast_id"),
        col("kind").alias("kind"),
        col("module").alias("module"),
        col("name").alias("name"),
        col("asname").alias("asname"),
        col("alias_index").alias("alias_index"),
        col("level").alias("level"),
        _arrow_cast((((col("span"))["start"])["line0"] + lit(1)), "Int64").alias("lineno"),
        _arrow_cast(((col("span"))["start"])["col"], "Int64").alias("col_offset"),
        _arrow_cast((((col("span"))["end"])["line0"] + lit(1)), "Int64").alias("end_lineno"),
        _arrow_cast(((col("span"))["end"])["col"], "Int64").alias("end_col_offset"),
        _arrow_cast(lit(1), "Int32").alias("line_base"),
        _arrow_cast((col("span"))["col_unit"], "Utf8").alias("col_unit"),
        _arrow_cast((col("span"))["end_exclusive"], "Boolean").alias("end_exclusive"),
        _span_struct(col("span")).alias("span"),
        col("attrs").alias("attrs"),
        _ast_record(col("span"), col("attrs")).alias("ast_record"),
    ),
    "ast_node_attrs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ast_id").alias("ast_id"),
        col("parent_ast_id").alias("parent_ast_id"),
        col("kind").alias("kind"),
        col("name").alias("name"),
        (col("kv"))["key"].alias("attr_key"),
        (col("kv"))["value"].alias("attr_value"),
    ),
    "ast_nodes": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ast_id").alias("ast_id"),
        col("parent_ast_id").alias("parent_ast_id"),
        col("kind").alias("kind"),
        col("name").alias("name"),
        col("value").alias("value_repr"),
        _arrow_cast((((col("span"))["start"])["line0"] + lit(1)), "Int64").alias("lineno"),
        _arrow_cast(((col("span"))["start"])["col"], "Int64").alias("col_offset"),
        _arrow_cast((((col("span"))["end"])["line0"] + lit(1)), "Int64").alias("end_lineno"),
        _arrow_cast(((col("span"))["end"])["col"], "Int64").alias("end_col_offset"),
        _arrow_cast(lit(1), "Int32").alias("line_base"),
        _arrow_cast((col("span"))["col_unit"], "Utf8").alias("col_unit"),
        _arrow_cast((col("span"))["end_exclusive"], "Boolean").alias("end_exclusive"),
        _arrow_cast(((col("span"))["byte_span"])["byte_start"], "Int64").alias("bstart"),
        _arrow_cast(
            (((col("span"))["byte_span"])["byte_start"] + ((col("span"))["byte_span"])["byte_len"]),
            "Int64",
        ).alias("bend"),
        _span_struct(col("span")).alias("span"),
        col("attrs").alias("attrs"),
        _ast_record(col("span"), col("attrs")).alias("ast_record"),
    ),
    "ast_span_metadata": (
        arrow_metadata(col("nodes")["span"], "line_base").alias("nodes_line_base"),
        arrow_metadata(col("nodes")["span"], "col_unit").alias("nodes_col_unit"),
        arrow_metadata(col("nodes")["span"], "end_exclusive").alias("nodes_end_exclusive"),
        arrow_metadata(col("errors")["span"], "line_base").alias("errors_line_base"),
        arrow_metadata(col("errors")["span"], "col_unit").alias("errors_col_unit"),
        arrow_metadata(col("errors")["span"], "end_exclusive").alias("errors_end_exclusive"),
        arrow_metadata(col("docstrings")["span"], "line_base").alias("docstrings_line_base"),
        arrow_metadata(col("docstrings")["span"], "col_unit").alias("docstrings_col_unit"),
        arrow_metadata(col("docstrings")["span"], "end_exclusive").alias(
            "docstrings_end_exclusive"
        ),
        arrow_metadata(col("imports")["span"], "line_base").alias("imports_line_base"),
        arrow_metadata(col("imports")["span"], "col_unit").alias("imports_col_unit"),
        arrow_metadata(col("imports")["span"], "end_exclusive").alias("imports_end_exclusive"),
        arrow_metadata(col("defs")["span"], "line_base").alias("defs_line_base"),
        arrow_metadata(col("defs")["span"], "col_unit").alias("defs_col_unit"),
        arrow_metadata(col("defs")["span"], "end_exclusive").alias("defs_end_exclusive"),
        arrow_metadata(col("calls")["span"], "line_base").alias("calls_line_base"),
        arrow_metadata(col("calls")["span"], "col_unit").alias("calls_col_unit"),
        arrow_metadata(col("calls")["span"], "end_exclusive").alias("calls_end_exclusive"),
        arrow_metadata(col("type_ignores")["span"], "line_base").alias("type_ignores_line_base"),
        arrow_metadata(col("type_ignores")["span"], "col_unit").alias("type_ignores_col_unit"),
        arrow_metadata(col("type_ignores")["span"], "end_exclusive").alias(
            "type_ignores_end_exclusive"
        ),
    ),
    "ast_type_ignores": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ast_id").alias("ast_id"),
        col("tag").alias("tag"),
        _arrow_cast((((col("span"))["start"])["line0"] + lit(1)), "Int64").alias("lineno"),
        _arrow_cast(((col("span"))["start"])["col"], "Int64").alias("col_offset"),
        _arrow_cast((((col("span"))["end"])["line0"] + lit(1)), "Int64").alias("end_lineno"),
        _arrow_cast(((col("span"))["end"])["col"], "Int64").alias("end_col_offset"),
        _arrow_cast(lit(1), "Int32").alias("line_base"),
        _arrow_cast((col("span"))["col_unit"], "Utf8").alias("col_unit"),
        _arrow_cast((col("span"))["end_exclusive"], "Boolean").alias("end_exclusive"),
        _span_struct(col("span")).alias("span"),
        col("attrs").alias("attrs"),
        _ast_record(col("span"), col("attrs")).alias("ast_record"),
    ),
    "bytecode_errors": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("error_type").alias("error_type"),
        col("message").alias("message"),
        list_extract(map_extract(col("attrs"), "error_stage"), 1).alias("error_stage"),
        list_extract(map_extract(col("attrs"), "code_id"), 1).alias("code_id"),
    ),
    "bytecode_exception_table": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        stable_id(
            "code",
            f.concat_ws(
                "\u001f",
                lit("code"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("code_unit_id"),
        col("exc_index").alias("exc_index"),
        col("start_offset").alias("start_offset"),
        col("end_offset").alias("end_offset"),
        col("target_offset").alias("target_offset"),
        col("depth").alias("depth"),
        col("lasti").alias("lasti"),
        prefixed_hash64(
            "bc_exc",
            f.concat_ws(
                ":",
                stable_id(
                    "code",
                    f.concat_ws(
                        "\u001f",
                        lit("code"),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                    ),
                ),
                col("exc_index"),
                col("start_offset"),
                col("end_offset"),
                col("target_offset"),
            ),
        ).alias("exc_entry_id"),
    ),
    "cst_call_args": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("file_sha256").alias("file_sha256"),
        stable_id(
            "cst_call",
            f.concat_ws(
                "\u001f",
                lit("cst_call"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("call_id"),
        col("call_bstart").alias("call_bstart"),
        col("call_bend").alias("call_bend"),
        col("arg_index").alias("arg_index"),
        col("keyword").alias("keyword"),
        col("star").alias("star"),
        col("arg_text").alias("arg_text"),
        col("bstart").alias("bstart"),
        col("bend").alias("bend"),
    ),
    "cst_callsite_span_unnest": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("call_id").alias("call_id"),
        _arrow_cast(_span_byte_start(col("span")), "Int64").alias("bstart"),
        _arrow_cast(_span_byte_end(col("span")), "Int64").alias("bend"),
    ),
    "cst_callsite_spans": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("call_id").alias("call_id"),
        col("call_bstart").alias("bstart"),
        col("call_bend").alias("bend"),
        _byte_span_struct(col("call_bstart"), col("call_bend")).alias("span"),
    ),
    "cst_callsites": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("file_sha256").alias("file_sha256"),
        stable_id(
            "cst_call",
            f.concat_ws(
                "\u001f",
                lit("cst_call"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("call_id"),
        col("call_bstart").alias("call_bstart"),
        col("call_bend").alias("call_bend"),
        col("callee_bstart").alias("callee_bstart"),
        col("callee_bend").alias("callee_bend"),
        col("callee_shape").alias("callee_shape"),
        col("callee_text").alias("callee_text"),
        col("arg_count").alias("arg_count"),
        col("callee_dotted").alias("callee_dotted"),
        col("callee_qnames").alias("callee_qnames"),
        col("callee_fqns").alias("callee_fqns"),
        col("inferred_type").alias("inferred_type"),
        col("attrs").alias("attrs"),
        prefixed_hash64(
            "span", f.concat_ws(":", col("file_id"), col("call_bstart"), col("call_bend"))
        ).alias("span_id"),
    ),
    "cst_callsites_attr_origin": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("call_id").alias("call_id"),
        map_extract(col("attrs"), "origin").alias("origin"),
    ),
    "cst_callsites_attrs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("call_id").alias("call_id"),
        (col("kv"))["key"].alias("attr_key"),
        (col("kv"))["value"].alias("attr_value"),
    ),
    "cst_decorators": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("file_sha256").alias("file_sha256"),
        f.when((col("owner_def_bstart").is_null() | col("owner_def_bend").is_null()), lit(None))
        .otherwise(
            stable_id(
                "cst_def",
                f.concat_ws(
                    "\u001f",
                    lit("cst_def"),
                    f.coalesce(lit("None")),
                    f.coalesce(lit("None")),
                    f.coalesce(lit("None")),
                    f.coalesce(lit("None")),
                ),
            )
        )
        .alias("owner_def_id"),
        col("owner_kind").alias("owner_kind"),
        col("owner_def_bstart").alias("owner_def_bstart"),
        col("owner_def_bend").alias("owner_def_bend"),
        col("decorator_text").alias("decorator_text"),
        col("decorator_index").alias("decorator_index"),
        col("bstart").alias("bstart"),
        col("bend").alias("bend"),
    ),
    "cst_def_span_unnest": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("def_id").alias("def_id"),
        _arrow_cast(_span_byte_start(col("span")), "Int64").alias("bstart"),
        _arrow_cast(_span_byte_end(col("span")), "Int64").alias("bend"),
    ),
    "cst_def_spans": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("def_id").alias("def_id"),
        col("def_bstart").alias("bstart"),
        col("def_bend").alias("bend"),
        _byte_span_struct(col("def_bstart"), col("def_bend")).alias("span"),
    ),
    "cst_defs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("file_sha256").alias("file_sha256"),
        stable_id(
            "cst_def",
            f.concat_ws(
                "\u001f",
                lit("cst_def"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("def_id"),
        col("container_def_kind").alias("container_def_kind"),
        col("container_def_bstart").alias("container_def_bstart"),
        col("container_def_bend").alias("container_def_bend"),
        col("kind").alias("kind"),
        col("name").alias("name"),
        col("def_bstart").alias("def_bstart"),
        col("def_bend").alias("def_bend"),
        col("name_bstart").alias("name_bstart"),
        col("name_bend").alias("name_bend"),
        col("qnames").alias("qnames"),
        col("def_fqns").alias("def_fqns"),
        col("docstring").alias("docstring"),
        col("decorator_count").alias("decorator_count"),
        col("attrs").alias("attrs"),
        f.when(col("container_def_kind").is_null(), lit(None))
        .otherwise(
            stable_id(
                "cst_def",
                f.concat_ws(
                    "\u001f",
                    lit("cst_def"),
                    f.coalesce(lit("None")),
                    f.coalesce(lit("None")),
                    f.coalesce(lit("None")),
                    f.coalesce(lit("None")),
                ),
            )
        )
        .alias("container_def_id"),
        prefixed_hash64(
            "span", f.concat_ws(":", col("file_id"), col("def_bstart"), col("def_bend"))
        ).alias("span_id"),
    ),
    "cst_defs_attr_origin": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("def_id").alias("def_id"),
        map_extract(col("attrs"), "origin").alias("origin"),
    ),
    "cst_defs_attrs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("def_id").alias("def_id"),
        (col("kv"))["key"].alias("attr_key"),
        (col("kv"))["value"].alias("attr_value"),
    ),
    "cst_docstrings": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("file_sha256").alias("file_sha256"),
        f.when((col("owner_def_bstart").is_null() | col("owner_def_bend").is_null()), lit(None))
        .otherwise(
            stable_id(
                "cst_def",
                f.concat_ws(
                    "\u001f",
                    lit("cst_def"),
                    f.coalesce(lit("None")),
                    f.coalesce(lit("None")),
                    f.coalesce(lit("None")),
                    f.coalesce(lit("None")),
                ),
            )
        )
        .alias("owner_def_id"),
        col("owner_kind").alias("owner_kind"),
        col("owner_def_bstart").alias("owner_def_bstart"),
        col("owner_def_bend").alias("owner_def_bend"),
        col("docstring").alias("docstring"),
        col("bstart").alias("bstart"),
        col("bend").alias("bend"),
    ),
    "cst_edges_attr_origin": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("src").alias("src"),
        col("dst").alias("dst"),
        col("kind").alias("kind"),
        col("slot").alias("slot"),
        col("idx").alias("idx"),
        map_extract(col("attrs"), "origin").alias("origin"),
    ),
    "cst_edges_attrs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("src").alias("src"),
        col("dst").alias("dst"),
        col("kind").alias("kind"),
        col("slot").alias("slot"),
        col("idx").alias("idx"),
        (col("kv"))["key"].alias("attr_key"),
        (col("kv"))["value"].alias("attr_value"),
    ),
    "cst_imports": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("file_sha256").alias("file_sha256"),
        col("kind").alias("kind"),
        col("module").alias("module"),
        col("relative_level").alias("relative_level"),
        col("name").alias("name"),
        col("asname").alias("asname"),
        col("is_star").alias("is_star"),
        col("stmt_bstart").alias("stmt_bstart"),
        col("stmt_bend").alias("stmt_bend"),
        col("alias_bstart").alias("alias_bstart"),
        col("alias_bend").alias("alias_bend"),
        col("attrs").alias("attrs"),
        prefixed_hash64(
            "cst_import",
            f.concat_ws(":", col("file_id"), col("kind"), col("alias_bstart"), col("alias_bend")),
        ).alias("import_id"),
        prefixed_hash64(
            "span", f.concat_ws(":", col("file_id"), col("alias_bstart"), col("alias_bend"))
        ).alias("span_id"),
    ),
    "cst_imports_attr_origin": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("import_id").alias("import_id"),
        map_extract(col("attrs"), "origin").alias("origin"),
    ),
    "cst_imports_attrs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("import_id").alias("import_id"),
        (col("kv"))["key"].alias("attr_key"),
        (col("kv"))["value"].alias("attr_value"),
    ),
    "cst_nodes_attr_origin": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("cst_id").alias("cst_id"),
        map_extract(col("attrs"), "origin").alias("origin"),
    ),
    "cst_nodes_attrs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("cst_id").alias("cst_id"),
        (col("kv"))["key"].alias("attr_key"),
        (col("kv"))["value"].alias("attr_value"),
    ),
    "cst_parse_errors": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("file_sha256").alias("file_sha256"),
        col("error_type").alias("error_type"),
        col("message").alias("message"),
        col("raw_line").alias("raw_line"),
        col("raw_column").alias("raw_column"),
        col("editor_line").alias("editor_line"),
        col("editor_column").alias("editor_column"),
        col("context").alias("context"),
        col("line_base").alias("line_base"),
        col("col_unit").alias("col_unit"),
        col("end_exclusive").alias("end_exclusive"),
    ),
    "cst_parse_manifest": (
        (col("n0")["n0_item"])["file_id"].alias("file_id"),
        (col("n0")["n0_item"])["path"].alias("path"),
        (col("n0")["n0_item"])["file_sha256"].alias("file_sha256"),
        (col("n0")["n0_item"])["encoding"].alias("encoding"),
        (col("n0")["n0_item"])["default_indent"].alias("default_indent"),
        (col("n0")["n0_item"])["default_newline"].alias("default_newline"),
        (col("n0")["n0_item"])["has_trailing_newline"].alias("has_trailing_newline"),
        (col("n0")["n0_item"])["future_imports"].alias("future_imports"),
        (col("n0")["n0_item"])["module_name"].alias("module_name"),
        (col("n0")["n0_item"])["package_name"].alias("package_name"),
        (col("n0")["n0_item"])["libcst_version"].alias("libcst_version"),
        (col("n0")["n0_item"])["parser_backend"].alias("parser_backend"),
        (col("n0")["n0_item"])["parsed_python_version"].alias("parsed_python_version"),
        (col("n0")["n0_item"])["schema_identity_hash"].alias("schema_identity_hash"),
    ),
    "cst_ref_span_unnest": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ref_id").alias("ref_id"),
        _arrow_cast(_span_byte_start(col("span")), "Int64").alias("bstart"),
        _arrow_cast(_span_byte_end(col("span")), "Int64").alias("bend"),
    ),
    "cst_ref_spans": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ref_id").alias("ref_id"),
        col("bstart").alias("bstart"),
        col("bend").alias("bend"),
        _byte_span_struct(col("bstart"), col("bend")).alias("span"),
    ),
    "cst_refs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("file_sha256").alias("file_sha256"),
        stable_id(
            "cst_ref",
            f.concat_ws(
                "\u001f",
                lit("cst_ref"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("ref_id"),
        col("ref_kind").alias("ref_kind"),
        col("ref_text").alias("ref_text"),
        col("expr_ctx").alias("expr_ctx"),
        col("scope_type").alias("scope_type"),
        col("scope_name").alias("scope_name"),
        col("scope_role").alias("scope_role"),
        col("parent_kind").alias("parent_kind"),
        col("inferred_type").alias("inferred_type"),
        col("bstart").alias("bstart"),
        col("bend").alias("bend"),
        _arrow_cast(col("attrs"), "Map(Utf8, Utf8)").alias("attrs"),
    ),
    "cst_refs_attr_origin": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ref_id").alias("ref_id"),
        map_extract(col("attrs"), "origin").alias("origin"),
    ),
    "cst_refs_attrs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ref_id").alias("ref_id"),
        (col("kv"))["key"].alias("attr_key"),
        (col("kv"))["value"].alias("attr_value"),
    ),
    "cst_schema_diagnostics": (
        f.arrow_typeof(col("nodes")).alias("nodes_type"),
        f.arrow_typeof(
            _arrow_cast(
                col("nodes"),
                "list<item: struct<cst_id: int64\nnot null, kind: string, span:\nextension<codeintel.span<_SemanticExtensionType>>, span_ws:\nextension<codeintel.span<_SemanticExtensionType>>, attrs: map<string,\nstring>>>",
            )
        ).alias("nodes_cast_type"),
        arrow_metadata(col("nodes")).alias("nodes_meta"),
        arrow_metadata(
            _arrow_cast(
                col("nodes"),
                "list<item: struct<cst_id: int64\nnot null, kind: string, span:\nextension<codeintel.span<_SemanticExtensionType>>, span_ws:\nextension<codeintel.span<_SemanticExtensionType>>, attrs: map<string,\nstring>>>",
            )
        ).alias("nodes_cast_meta"),
        f.arrow_typeof(col("edges")).alias("edges_type"),
        f.arrow_typeof(
            _arrow_cast(
                col("edges"),
                "list<item: struct<src: int64 not\nnull, dst: int64 not null, kind: string, slot: string, idx: int32, attrs:\nmap<string, string>>>",
            )
        ).alias("edges_cast_type"),
        arrow_metadata(col("edges")).alias("edges_meta"),
        arrow_metadata(
            _arrow_cast(
                col("edges"),
                "list<item: struct<src: int64\nnot null, dst: int64 not null, kind: string, slot: string, idx: int32,\nattrs: map<string, string>>>",
            )
        ).alias("edges_cast_meta"),
        f.arrow_typeof(col("parse_manifest")).alias("parse_manifest_type"),
        f.arrow_typeof(
            _arrow_cast(
                col("parse_manifest"),
                "list<item:\nstruct<file_id: string not null, path: string not null, file_sha256: string,\nencoding: string, default_indent: string, default_newline: string,\nhas_trailing_newline: bool, future_imports: list<item: string>, module_name:\nstring, package_name: string, libcst_version: string, parser_backend:\nstring, parsed_python_version: string, schema_identity_hash: string>>",
            )
        ).alias("parse_manifest_cast_type"),
        arrow_metadata(col("parse_manifest")).alias("parse_manifest_meta"),
        arrow_metadata(
            _arrow_cast(
                col("parse_manifest"),
                "list<item:\nstruct<file_id: string not null, path: string not null, file_sha256: string,\nencoding: string, default_indent: string, default_newline: string,\nhas_trailing_newline: bool, future_imports: list<item: string>, module_name:\nstring, package_name: string, libcst_version: string, parser_backend:\nstring, parsed_python_version: string, schema_identity_hash: string>>",
            )
        ).alias("parse_manifest_cast_meta"),
        f.arrow_typeof(col("parse_errors")).alias("parse_errors_type"),
        f.arrow_typeof(
            _arrow_cast(
                col("parse_errors"),
                "list<item: struct<file_id:\nstring not null, path: string not null, file_sha256: string, error_type:\nstring, message: string, raw_line: int64, raw_column: int64, editor_line:\nint64, editor_column: int64, context: string, line_base: int32, col_unit:\nstring, end_exclusive: bool, meta: map<string, string>>>",
            )
        ).alias("parse_errors_cast_type"),
        arrow_metadata(col("parse_errors")).alias("parse_errors_meta"),
        arrow_metadata(
            _arrow_cast(
                col("parse_errors"),
                "list<item:\nstruct<file_id: string not null, path: string not null, file_sha256: string,\nerror_type: string, message: string, raw_line: int64, raw_column: int64,\neditor_line: int64, editor_column: int64, context: string, line_base: int32,\ncol_unit: string, end_exclusive: bool, meta: map<string, string>>>",
            )
        ).alias("parse_errors_cast_meta"),
        f.arrow_typeof(col("refs")).alias("refs_type"),
        f.arrow_typeof(
            _arrow_cast(
                col("refs"),
                "list<item: struct<file_id: string\nnot null, path: string not null, file_sha256: string, ref_id: string,\nref_kind: string, ref_text: string, expr_ctx: string, scope_type: string,\nscope_name: string, scope_role: string, parent_kind: string, inferred_type:\nstring, bstart: int64, bend: int64, attrs: map<string, string>>>",
            )
        ).alias("refs_cast_type"),
        arrow_metadata(col("refs")).alias("refs_meta"),
        arrow_metadata(
            _arrow_cast(
                col("refs"),
                "list<item: struct<file_id:\nstring not null, path: string not null, file_sha256: string, ref_id: string,\nref_kind: string, ref_text: string, expr_ctx: string, scope_type: string,\nscope_name: string, scope_role: string, parent_kind: string, inferred_type:\nstring, bstart: int64, bend: int64, attrs: map<string, string>>>",
            )
        ).alias("refs_cast_meta"),
        f.arrow_typeof(col("imports")).alias("imports_type"),
        f.arrow_typeof(
            _arrow_cast(
                col("imports"),
                "list<item: struct<file_id:\nstring not null, path: string not null, file_sha256: string, kind: string,\nmodule: string, relative_level: int32, name: string, asname: string,\nis_star: bool, stmt_bstart: int64, stmt_bend: int64, alias_bstart: int64,\nalias_bend: int64, attrs: map<string, string>>>",
            )
        ).alias("imports_cast_type"),
        arrow_metadata(col("imports")).alias("imports_meta"),
        arrow_metadata(
            _arrow_cast(
                col("imports"),
                "list<item: struct<file_id:\nstring not null, path: string not null, file_sha256: string, kind: string,\nmodule: string, relative_level: int32, name: string, asname: string,\nis_star: bool, stmt_bstart: int64, stmt_bend: int64, alias_bstart: int64,\nalias_bend: int64, attrs: map<string, string>>>",
            )
        ).alias("imports_cast_meta"),
        f.arrow_typeof(col("callsites")).alias("callsites_type"),
        f.arrow_typeof(
            _arrow_cast(
                col("callsites"),
                "list<item: struct<file_id:\nstring not null, path: string not null, file_sha256: string, call_id:\nstring, call_bstart: int64, call_bend: int64, callee_bstart: int64,\ncallee_bend: int64, callee_shape: string, callee_text: string, arg_count:\nint32, callee_dotted: string, callee_qnames: list<item: struct<name: string,\nsource: string>>, callee_fqns: list<item: string>, inferred_type: string,\nattrs: map<string, string>>>",
            )
        ).alias("callsites_cast_type"),
        arrow_metadata(col("callsites")).alias("callsites_meta"),
        arrow_metadata(
            _arrow_cast(
                col("callsites"),
                "list<item: struct<file_id:\nstring not null, path: string not null, file_sha256: string, call_id:\nstring, call_bstart: int64, call_bend: int64, callee_bstart: int64,\ncallee_bend: int64, callee_shape: string, callee_text: string, arg_count:\nint32, callee_dotted: string, callee_qnames: list<item: struct<name: string,\nsource: string>>, callee_fqns: list<item: string>, inferred_type: string,\nattrs: map<string, string>>>",
            )
        ).alias("callsites_cast_meta"),
        f.arrow_typeof(col("defs")).alias("defs_type"),
        f.arrow_typeof(
            _arrow_cast(
                col("defs"),
                "list<item: struct<file_id: string\nnot null, path: string not null, file_sha256: string, def_id: string,\ncontainer_def_kind: string, container_def_bstart: int64, container_def_bend:\nint64, kind: string, name: string, def_bstart: int64, def_bend: int64,\nname_bstart: int64, name_bend: int64, qnames: list<item: struct<name:\nstring, source: string>>, def_fqns: list<item: string>, docstring: string,\ndecorator_count: int32, attrs: map<string, string>>>",
            )
        ).alias("defs_cast_type"),
        arrow_metadata(col("defs")).alias("defs_meta"),
        arrow_metadata(
            _arrow_cast(
                col("defs"),
                "list<item: struct<file_id:\nstring not null, path: string not null, file_sha256: string, def_id: string,\ncontainer_def_kind: string, container_def_bstart: int64, container_def_bend:\nint64, kind: string, name: string, def_bstart: int64, def_bend: int64,\nname_bstart: int64, name_bend: int64, qnames: list<item: struct<name:\nstring, source: string>>, def_fqns: list<item: string>, docstring: string,\ndecorator_count: int32, attrs: map<string, string>>>",
            )
        ).alias("defs_cast_meta"),
        f.arrow_typeof(col("type_exprs")).alias("type_exprs_type"),
        f.arrow_typeof(
            _arrow_cast(
                col("type_exprs"),
                "list<item: struct<file_id:\nstring not null, path: string not null, file_sha256: string, owner_def_kind:\nstring, owner_def_bstart: int64, owner_def_bend: int64, param_name: string,\nexpr_kind: string, expr_role: string, bstart: int64, bend: int64, expr_text:\nstring>>",
            )
        ).alias("type_exprs_cast_type"),
        arrow_metadata(col("type_exprs")).alias("type_exprs_meta"),
        arrow_metadata(
            _arrow_cast(
                col("type_exprs"),
                "list<item: struct<file_id:\nstring not null, path: string not null, file_sha256: string, owner_def_kind:\nstring, owner_def_bstart: int64, owner_def_bend: int64, param_name: string,\nexpr_kind: string, expr_role: string, bstart: int64, bend: int64, expr_text:\nstring>>",
            )
        ).alias("type_exprs_cast_meta"),
        f.arrow_typeof(col("docstrings")).alias("docstrings_type"),
        f.arrow_typeof(
            _arrow_cast(
                col("docstrings"),
                "list<item: struct<file_id:\nstring not null, path: string not null, file_sha256: string, owner_def_id:\nstring, owner_kind: string, owner_def_bstart: int64, owner_def_bend: int64,\ndocstring: string, bstart: int64, bend: int64>>",
            )
        ).alias("docstrings_cast_type"),
        arrow_metadata(col("docstrings")).alias("docstrings_meta"),
        arrow_metadata(
            _arrow_cast(
                col("docstrings"),
                "list<item: struct<file_id:\nstring not null, path: string not null, file_sha256: string, owner_def_id:\nstring, owner_kind: string, owner_def_bstart: int64, owner_def_bend: int64,\ndocstring: string, bstart: int64, bend: int64>>",
            )
        ).alias("docstrings_cast_meta"),
        f.arrow_typeof(col("decorators")).alias("decorators_type"),
        f.arrow_typeof(
            _arrow_cast(
                col("decorators"),
                "list<item: struct<file_id:\nstring not null, path: string not null, file_sha256: string, owner_def_id:\nstring, owner_kind: string, owner_def_bstart: int64, owner_def_bend: int64,\ndecorator_text: string, decorator_index: int32, bstart: int64, bend:\nint64>>",
            )
        ).alias("decorators_cast_type"),
        arrow_metadata(col("decorators")).alias("decorators_meta"),
        arrow_metadata(
            _arrow_cast(
                col("decorators"),
                "list<item: struct<file_id:\nstring not null, path: string not null, file_sha256: string, owner_def_id:\nstring, owner_kind: string, owner_def_bstart: int64, owner_def_bend: int64,\ndecorator_text: string, decorator_index: int32, bstart: int64, bend:\nint64>>",
            )
        ).alias("decorators_cast_meta"),
        f.arrow_typeof(col("call_args")).alias("call_args_type"),
        f.arrow_typeof(
            _arrow_cast(
                col("call_args"),
                "list<item: struct<file_id:\nstring not null, path: string not null, file_sha256: string, call_id:\nstring, call_bstart: int64, call_bend: int64, arg_index: int32, keyword:\nstring, star: string, arg_text: string, bstart: int64, bend: int64>>",
            )
        ).alias("call_args_cast_type"),
        arrow_metadata(col("call_args")).alias("call_args_meta"),
        arrow_metadata(
            _arrow_cast(
                col("call_args"),
                "list<item: struct<file_id:\nstring not null, path: string not null, file_sha256: string, call_id:\nstring, call_bstart: int64, call_bend: int64, arg_index: int32, keyword:\nstring, star: string, arg_text: string, bstart: int64, bend: int64>>",
            )
        ).alias("call_args_cast_meta"),
        f.arrow_typeof(col("attrs")).alias("attrs_type"),
        arrow_metadata(col("attrs")).alias("attrs_meta"),
    ),
    "cst_type_exprs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("file_sha256").alias("file_sha256"),
        col("owner_def_kind").alias("owner_def_kind"),
        col("owner_def_bstart").alias("owner_def_bstart"),
        col("owner_def_bend").alias("owner_def_bend"),
        col("param_name").alias("param_name"),
        col("expr_kind").alias("expr_kind"),
        col("expr_role").alias("expr_role"),
        col("bstart").alias("bstart"),
        col("bend").alias("bend"),
        col("expr_text").alias("expr_text"),
        prefixed_hash64(
            "cst_type_expr", f.concat_ws(":", col("path"), col("bstart"), col("bend"))
        ).alias("type_expr_id"),
        prefixed_hash64(
            "cst_def",
            f.concat_ws(
                ":",
                col("file_id"),
                col("owner_def_kind"),
                col("owner_def_bstart"),
                col("owner_def_bend"),
            ),
        ).alias("owner_def_id"),
    ),
    "py_bc_blocks": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        stable_id(
            "code",
            f.concat_ws(
                "\u001f",
                lit("code"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("code_unit_id"),
        col("start_offset").alias("start_offset"),
        col("end_offset").alias("end_offset"),
        col("kind").alias("kind"),
        prefixed_hash64(
            "bc_block",
            f.concat_ws(
                ":",
                stable_id(
                    "code",
                    f.concat_ws(
                        "\u001f",
                        lit("code"),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                    ),
                ),
                col("start_offset"),
                col("end_offset"),
            ),
        ).alias("block_id"),
    ),
    "py_bc_cache_entries": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        stable_id(
            "code",
            f.concat_ws(
                "\u001f",
                lit("code"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("code_unit_id"),
        col("instr_index").alias("instr_index"),
        col("offset").alias("offset"),
        col("name").alias("name"),
        col("size").alias("size"),
        col("data_hex").alias("data_hex"),
        prefixed_hash64(
            "bc_instr",
            f.concat_ws(
                ":",
                stable_id(
                    "code",
                    f.concat_ws(
                        "\u001f",
                        lit("code"),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                    ),
                ),
                col("instr_index"),
                col("offset"),
            ),
        ).alias("instr_id"),
        prefixed_hash64(
            "bc_cache",
            f.concat_ws(
                ":",
                stable_id(
                    "code",
                    f.concat_ws(
                        "\u001f",
                        lit("code"),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                    ),
                ),
                col("instr_index"),
                col("offset"),
                col("name"),
                col("size"),
                col("data_hex"),
            ),
        ).alias("cache_entry_id"),
    ),
    "py_bc_cfg_edge_attrs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        stable_id(
            "code",
            f.concat_ws(
                "\u001f",
                lit("code"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("code_unit_id"),
        col("edge_key").alias("edge_key"),
        col("kind").alias("kind"),
        (col("kv"))["key"].alias("attr_key"),
        (col("kv"))["value"].alias("attr_value"),
        union_tag((col("kv"))["value"]).alias("attr_kind"),
        union_extract((col("kv"))["value"], "int_value").alias("attr_int"),
        union_extract((col("kv"))["value"], "bool_value").alias("attr_bool"),
        union_extract((col("kv"))["value"], "str_value").alias("attr_str"),
    ),
    "py_bc_cfg_edges": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        stable_id(
            "code",
            f.concat_ws(
                "\u001f",
                lit("code"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("code_unit_id"),
        col("src_block_start").alias("src_block_start"),
        col("src_block_end").alias("src_block_end"),
        col("dst_block_start").alias("dst_block_start"),
        col("dst_block_end").alias("dst_block_end"),
        col("kind").alias("kind"),
        col("edge_key").alias("edge_key"),
        list_extract(map_extract(col("attrs"), "jump_kind"), 1).alias("jump_kind"),
        _arrow_cast(list_extract(map_extract(col("attrs"), "jump_label"), 1), "Int32").alias(
            "jump_label"
        ),
        col("cond_instr_index").alias("cond_instr_index"),
        col("cond_instr_offset").alias("cond_instr_offset"),
        col("exc_index").alias("exc_index"),
        prefixed_hash64(
            "bc_block",
            f.concat_ws(
                ":",
                stable_id(
                    "code",
                    f.concat_ws(
                        "\u001f",
                        lit("code"),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                    ),
                ),
                col("src_block_start"),
                col("src_block_end"),
            ),
        ).alias("src_block_id"),
        prefixed_hash64(
            "bc_block",
            f.concat_ws(
                ":",
                stable_id(
                    "code",
                    f.concat_ws(
                        "\u001f",
                        lit("code"),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                    ),
                ),
                col("dst_block_start"),
                col("dst_block_end"),
            ),
        ).alias("dst_block_id"),
        prefixed_hash64(
            "bc_instr",
            f.concat_ws(
                ":",
                stable_id(
                    "code",
                    f.concat_ws(
                        "\u001f",
                        lit("code"),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                    ),
                ),
                col("cond_instr_index"),
                col("cond_instr_offset"),
            ),
        ).alias("cond_instr_id"),
        prefixed_hash64(
            "bc_edge",
            f.concat_ws(
                ":",
                stable_id(
                    "code",
                    f.concat_ws(
                        "\u001f",
                        lit("code"),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                    ),
                ),
                prefixed_hash64(
                    "bc_block",
                    f.concat_ws(
                        ":",
                        stable_id(
                            "code",
                            f.concat_ws(
                                "\u001f",
                                lit("code"),
                                f.coalesce(lit("None")),
                                f.coalesce(lit("None")),
                                f.coalesce(lit("None")),
                            ),
                        ),
                        col("src_block_start"),
                        col("src_block_end"),
                    ),
                ),
                prefixed_hash64(
                    "bc_block",
                    f.concat_ws(
                        ":",
                        stable_id(
                            "code",
                            f.concat_ws(
                                "\u001f",
                                lit("code"),
                                f.coalesce(lit("None")),
                                f.coalesce(lit("None")),
                                f.coalesce(lit("None")),
                            ),
                        ),
                        col("dst_block_start"),
                        col("dst_block_end"),
                    ),
                ),
                col("kind"),
                col("edge_key"),
                col("exc_index"),
            ),
        ).alias("edge_id"),
    ),
    "py_bc_code_units": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        stable_id(
            "code",
            f.concat_ws(
                "\u001f",
                lit("code"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("code_unit_id"),
        col("qualname").alias("qualpath"),
        col("co_qualname").alias("co_qualname"),
        col("co_filename").alias("co_filename"),
        col("name").alias("co_name"),
        col("firstlineno1").alias("firstlineno"),
        col("argcount").alias("argcount"),
        col("posonlyargcount").alias("posonlyargcount"),
        col("kwonlyargcount").alias("kwonlyargcount"),
        col("nlocals").alias("nlocals"),
        col("flags").alias("flags"),
        col("flags_detail").alias("flags_detail"),
        col("stacksize").alias("stacksize"),
        col("code_len").alias("code_len"),
        col("varnames").alias("varnames"),
        col("freevars").alias("freevars"),
        col("cellvars").alias("cellvars"),
        col("names").alias("names"),
        col("consts").alias("consts"),
        col("consts_json").alias("consts_json"),
    ),
    "py_bc_consts": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        stable_id(
            "code",
            f.concat_ws(
                "\u001f",
                lit("code"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("code_unit_id"),
        col("const_index").alias("const_index"),
        col("const_repr").alias("const_repr"),
        prefixed_hash64(
            "bc_const",
            f.concat_ws(
                ":",
                stable_id(
                    "code",
                    f.concat_ws(
                        "\u001f",
                        lit("code"),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                    ),
                ),
                col("const_index"),
                col("const_repr"),
            ),
        ).alias("const_id"),
    ),
    "py_bc_dfg_edges": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        stable_id(
            "code",
            f.concat_ws(
                "\u001f",
                lit("code"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("code_unit_id"),
        col("src_instr_index").alias("src_instr_index"),
        col("dst_instr_index").alias("dst_instr_index"),
        col("kind").alias("kind"),
        prefixed_hash64(
            "bc_dfg",
            f.concat_ws(
                ":",
                stable_id(
                    "code",
                    f.concat_ws(
                        "\u001f",
                        lit("code"),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                    ),
                ),
                col("src_instr_index"),
                col("dst_instr_index"),
                col("kind"),
            ),
        ).alias("edge_id"),
    ),
    "py_bc_error_attrs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("error_type").alias("error_type"),
        col("message").alias("message"),
        (col("kv"))["key"].alias("attr_key"),
        (col("kv"))["value"].alias("attr_value"),
        union_tag((col("kv"))["value"]).alias("attr_kind"),
        union_extract((col("kv"))["value"], "int_value").alias("attr_int"),
        union_extract((col("kv"))["value"], "bool_value").alias("attr_bool"),
        union_extract((col("kv"))["value"], "str_value").alias("attr_str"),
    ),
    "py_bc_flags_detail": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        stable_id(
            "code",
            f.concat_ws(
                "\u001f",
                lit("code"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("code_unit_id"),
        (col("flags_detail"))["is_optimized"].alias("is_optimized"),
        (col("flags_detail"))["is_newlocals"].alias("is_newlocals"),
        (col("flags_detail"))["has_varargs"].alias("has_varargs"),
        (col("flags_detail"))["has_varkeywords"].alias("has_varkeywords"),
        (col("flags_detail"))["is_nested"].alias("is_nested"),
        (col("flags_detail"))["is_generator"].alias("is_generator"),
        (col("flags_detail"))["is_nofree"].alias("is_nofree"),
        (col("flags_detail"))["is_coroutine"].alias("is_coroutine"),
        (col("flags_detail"))["is_iterable_coroutine"].alias("is_iterable_coroutine"),
        (col("flags_detail"))["is_async_generator"].alias("is_async_generator"),
    ),
    "py_bc_instruction_attr_keys": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        stable_id(
            "code",
            f.concat_ws(
                "\u001f",
                lit("code"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("code_unit_id"),
        col("instr_index").alias("instr_index"),
        col("offset").alias("offset"),
        map_keys(col("attrs")).alias("attr_key"),
    ),
    "py_bc_instruction_attr_values": (
        col("expanded")["file_id"].alias("file_id"),
        col("expanded")["path"].alias("path"),
        col("expanded")["code_unit_id"].alias("code_unit_id"),
        col("expanded")["instr_index"].alias("instr_index"),
        col("expanded")["offset"].alias("offset"),
        col("expanded")["attr_value"].alias("attr_value"),
        union_tag(col("expanded")["attr_value"]).alias("attr_kind"),
        union_extract(col("expanded")["attr_value"], "int_value").alias("attr_int"),
        union_extract(col("expanded")["attr_value"], "bool_value").alias("attr_bool"),
        union_extract(col("expanded")["attr_value"], "str_value").alias("attr_str"),
    ),
    "py_bc_instruction_attrs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        stable_id(
            "code",
            f.concat_ws(
                "\u001f",
                lit("code"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("code_unit_id"),
        col("instr_index").alias("instr_index"),
        col("offset").alias("offset"),
        (col("kv"))["key"].alias("attr_key"),
        (col("kv"))["value"].alias("attr_value"),
        union_tag((col("kv"))["value"]).alias("attr_kind"),
        union_extract((col("kv"))["value"], "int_value").alias("attr_int"),
        union_extract((col("kv"))["value"], "bool_value").alias("attr_bool"),
        union_extract((col("kv"))["value"], "str_value").alias("attr_str"),
    ),
    "py_bc_instruction_span_fields": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("code_unit_id").alias("code_unit_id"),
        col("instr_id").alias("instr_id"),
        col("instr_index").alias("instr_index"),
        col("offset").alias("offset"),
        ((col("span"))["start"])["line0"].alias("pos_start_line"),
        ((col("span"))["start"])["col"].alias("pos_start_col"),
        ((col("span"))["end"])["line0"].alias("pos_end_line"),
        ((col("span"))["end"])["col"].alias("pos_end_col"),
        (col("span"))["col_unit"].alias("col_unit"),
        (col("span"))["end_exclusive"].alias("end_exclusive"),
    ),
    "py_bc_instruction_spans": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("code_unit_id").alias("code_unit_id"),
        col("instr_id").alias("instr_id"),
        col("instr_index").alias("instr_index"),
        col("offset").alias("offset"),
        _span_struct_from_components(
            SpanComponents(
                start_line=col("pos_start_line"),
                start_col=col("pos_start_col"),
                end_line=col("pos_end_line"),
                end_col=col("pos_end_col"),
                col_unit=col("col_unit"),
                end_exclusive=col("end_exclusive"),
            )
        ).alias("span"),
    ),
    "py_bc_instructions": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        stable_id(
            "code",
            f.concat_ws(
                "\u001f",
                lit("code"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("code_unit_id"),
        col("instr_index").alias("instr_index"),
        col("offset").alias("offset"),
        col("start_offset").alias("start_offset"),
        col("end_offset").alias("end_offset"),
        col("opname").alias("opname"),
        col("baseopname").alias("baseopname"),
        col("opcode").alias("opcode"),
        col("baseopcode").alias("baseopcode"),
        col("arg").alias("arg"),
        col("oparg").alias("oparg"),
        col("argval_kind").alias("argval_kind"),
        col("argval_int").alias("argval_int"),
        col("argval_str").alias("argval_str"),
        col("argrepr").alias("argrepr"),
        col("line_number").alias("line_number"),
        col("starts_line").alias("starts_line"),
        col("label").alias("label"),
        col("is_jump_target").alias("is_jump_target"),
        col("jump_target").alias("jump_target_offset"),
        col("cache_info").alias("cache_info"),
        ((col("span"))["start"])["line0"].alias("pos_start_line"),
        ((col("span"))["end"])["line0"].alias("pos_end_line"),
        ((col("span"))["start"])["col"].alias("pos_start_col"),
        ((col("span"))["end"])["col"].alias("pos_end_col"),
        _arrow_cast(arrow_metadata(col("span"), "line_base"), "Int32").alias("line_base"),
        arrow_metadata(col("span"), "col_unit").alias("col_unit"),
        f.when(
            (f.lower(arrow_metadata(col("span"), "end_exclusive")) == lit("true")),
            lit(value=True),
        )
        .when(
            (f.lower(arrow_metadata(col("span"), "end_exclusive")) == lit("false")),
            lit(value=False),
        )
        .otherwise(lit(None))
        .alias("end_exclusive"),
        union_extract(
            list_extract(map_extract(col("attrs"), "stack_depth_before"), 1), "int_value"
        ).alias("stack_depth_before"),
        union_extract(
            list_extract(map_extract(col("attrs"), "stack_depth_after"), 1), "int_value"
        ).alias("stack_depth_after"),
        prefixed_hash64(
            "bc_instr",
            f.concat_ws(
                ":",
                stable_id(
                    "code",
                    f.concat_ws(
                        "\u001f",
                        lit("code"),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                        f.coalesce(lit("None")),
                    ),
                ),
                col("instr_index"),
                col("offset"),
            ),
        ).alias("instr_id"),
    ),
    "py_bc_line_table": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        stable_id(
            "code",
            f.concat_ws(
                "\u001f",
                lit("code"),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
                f.coalesce(lit("None")),
            ),
        ).alias("code_unit_id"),
        col("offset").alias("offset"),
        col("line1").alias("line1"),
        col("line0").alias("line0"),
        _arrow_cast(arrow_metadata(col("line1"), "line_base"), "Int32").alias("line_base"),
    ),
    "py_bc_metadata": (
        arrow_metadata(col("code_objects")["instructions"]).alias("instructions_metadata"),
        arrow_metadata(col("code_objects")["line_table"]).alias("line_table_metadata"),
    ),
    "scip_diagnostics": (
        f.when((col("path").is_null() | (col("path") == lit(""))), lit(None))
        .otherwise(
            stable_id("scip_doc", f.concat_ws("\u001f", lit("scip_doc"), f.coalesce(lit("None"))))
        )
        .alias("document_id"),
        col("path").alias("path"),
        col("severity").alias("severity"),
        col("message").alias("message"),
        col("raw_line").alias("raw_line"),
        col("raw_column").alias("raw_column"),
        col("editor_line").alias("editor_line"),
        col("editor_column").alias("editor_column"),
        col("context").alias("context"),
        col("line_base").alias("line_base"),
        col("col_unit").alias("col_unit"),
        col("end_exclusive").alias("end_exclusive"),
    ),
    "scip_document_symbols": (
        f.when((col("path").is_null() | (col("path") == lit(""))), lit(None))
        .otherwise(
            stable_id("scip_doc", f.concat_ws("\u001f", lit("scip_doc"), f.coalesce(lit("None"))))
        )
        .alias("document_id"),
        col("path").alias("path"),
        col("symbol").alias("symbol"),
        col("display_name").alias("display_name"),
        col("kind").alias("kind"),
        col("kind_name").alias("kind_name"),
        col("enclosing_symbol").alias("enclosing_symbol"),
        col("documentation").alias("documentation"),
        col("signature_text").alias("signature_text"),
        col("signature_language").alias("signature_language"),
    ),
    "scip_document_texts": (
        f.when((col("path").is_null() | (col("path") == lit(""))), lit(None))
        .otherwise(
            stable_id("scip_doc", f.concat_ws("\u001f", lit("scip_doc"), f.coalesce(lit("None"))))
        )
        .alias("document_id"),
        col("path").alias("path"),
        col("text").alias("text"),
    ),
    "scip_documents": (
        f.when((col("index_path").is_null() | (col("index_path") == lit(""))), lit(None))
        .otherwise(
            stable_id(
                "scip_index", f.concat_ws("\u001f", lit("scip_index"), f.coalesce(lit("None")))
            )
        )
        .alias("index_id"),
        f.when((col("path").is_null() | (col("path") == lit(""))), lit(None))
        .otherwise(
            stable_id("scip_doc", f.concat_ws("\u001f", lit("scip_doc"), f.coalesce(lit("None"))))
        )
        .alias("document_id"),
        col("path").alias("path"),
        col("language").alias("language"),
        col("position_encoding").alias("position_encoding"),
    ),
    "scip_external_symbol_information": (),
    "scip_index_stats": (
        f.when((col("index_path").is_null() | (col("index_path") == lit(""))), lit(None))
        .otherwise(
            stable_id(
                "scip_index", f.concat_ws("\u001f", lit("scip_index"), f.coalesce(lit("None")))
            )
        )
        .alias("index_id"),
        col("document_count").alias("document_count"),
        col("occurrence_count").alias("occurrence_count"),
        col("diagnostic_count").alias("diagnostic_count"),
        col("symbol_count").alias("symbol_count"),
        col("external_symbol_count").alias("external_symbol_count"),
        col("missing_position_encoding_count").alias("missing_position_encoding_count"),
        col("document_text_count").alias("document_text_count"),
        col("document_text_bytes").alias("document_text_bytes"),
    ),
    "scip_metadata": (
        f.when((col("index_path").is_null() | (col("index_path") == lit(""))), lit(None))
        .otherwise(
            stable_id(
                "scip_index", f.concat_ws("\u001f", lit("scip_index"), f.coalesce(lit("None")))
            )
        )
        .alias("index_id"),
        col("protocol_version").alias("protocol_version"),
        col("tool_name").alias("tool_name"),
        col("tool_version").alias("tool_version"),
        col("tool_arguments").alias("tool_arguments"),
        col("project_root").alias("project_root"),
        col("text_document_encoding").alias("text_document_encoding"),
        col("project_name").alias("project_name"),
        col("project_version").alias("project_version"),
        col("project_namespace").alias("project_namespace"),
    ),
    "scip_occurrences": (
        f.when((col("path").is_null() | (col("path") == lit(""))), lit(None))
        .otherwise(
            stable_id("scip_doc", f.concat_ws("\u001f", lit("scip_doc"), f.coalesce(lit("None"))))
        )
        .alias("document_id"),
        col("path").alias("path"),
        col("symbol").alias("symbol"),
        col("symbol_roles").alias("symbol_roles"),
        col("syntax_kind").alias("syntax_kind"),
        col("syntax_kind_name").alias("syntax_kind_name"),
        col("override_documentation").alias("override_documentation"),
        col("range_raw").alias("range_raw"),
        col("enclosing_range_raw").alias("enclosing_range_raw"),
        col("start_line").alias("start_line"),
        col("start_char").alias("start_char"),
        col("end_line").alias("end_line"),
        col("end_char").alias("end_char"),
        col("range_len").alias("range_len"),
        col("enc_start_line").alias("enc_start_line"),
        col("enc_start_char").alias("enc_start_char"),
        col("enc_end_line").alias("enc_end_line"),
        col("enc_end_char").alias("enc_end_char"),
        col("enc_range_len").alias("enc_range_len"),
        col("line_base").alias("line_base"),
        col("col_unit").alias("col_unit"),
        col("end_exclusive").alias("end_exclusive"),
        col("is_definition").alias("is_definition"),
        col("is_import").alias("is_import"),
        col("is_write").alias("is_write"),
        col("is_read").alias("is_read"),
        col("is_generated").alias("is_generated"),
        col("is_test").alias("is_test"),
        col("is_forward_definition").alias("is_forward_definition"),
    ),
    "scip_occurrences_norm": (),
    "scip_signature_occurrences": (),
    "scip_symbol_information": (),
    "scip_symbol_relationships": (),
    "symtable_class_methods": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("scope_id").alias("scope_id"),
        col("scope_name").alias("scope_name"),
        col("method_name").alias("method_name"),
    ),
    "symtable_function_partitions": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("scope_id").alias("scope_id"),
        col("scope_name").alias("scope_name"),
        (col("function_partitions"))["parameters"].alias("parameters"),
        (col("function_partitions"))["locals"].alias("locals"),
        (col("function_partitions"))["globals"].alias("globals"),
        (col("function_partitions"))["nonlocals"].alias("nonlocals"),
        (col("function_partitions"))["frees"].alias("frees"),
    ),
    "symtable_namespace_edges": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("scope_id").alias("scope_id"),
        col("name").alias("symbol_name"),
        col("child_block_id").alias("child_table_id"),
        col("child")["scope_id"].alias("child_scope_id"),
        col("namespace_count").alias("namespace_count"),
    ),
    "symtable_scope_edges": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("parent_block_id").alias("parent_table_id"),
        col("block_id").alias("child_table_id"),
        col("parent")["scope_id"].alias("parent_scope_id"),
        col("child")["scope_id"].alias("child_scope_id"),
    ),
    "symtable_scopes": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("block_id").alias("table_id"),
        col("scope_id").alias("scope_id"),
        col("scope_local_id").alias("scope_local_id"),
        col("block_type").alias("scope_type"),
        col("scope_type_value").alias("scope_type_value"),
        col("name").alias("scope_name"),
        col("qualpath").alias("qualpath"),
        col("function_partitions").alias("function_partitions"),
        col("class_methods").alias("class_methods"),
        col("lineno1").alias("lineno"),
        col("is_meta_scope").alias("is_meta_scope"),
        col("parent_block_id").alias("parent_table_id"),
    ),
    "symtable_symbol_attrs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("scope_id").alias("scope_id"),
        col("name").alias("symbol_name"),
        (col("kv"))["key"].alias("attr_key"),
        (col("kv"))["value"].alias("attr_value"),
    ),
    "symtable_symbols": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("block_id").alias("table_id"),
        col("scope_id").alias("scope_id"),
        col("name").alias("symbol_name"),
        f.when((col("scope_id").is_null() | col("name").is_null()), lit(None))
        .otherwise(prefixed_hash64("sym_symbol", f.concat_ws(":", col("scope_id"), col("name"))))
        .alias("sym_symbol_id"),
        (col("flags"))["is_referenced"].alias("is_referenced"),
        (col("flags"))["is_imported"].alias("is_imported"),
        (col("flags"))["is_parameter"].alias("is_parameter"),
        (col("flags"))["is_type_parameter"].alias("is_type_parameter"),
        (col("flags"))["is_global"].alias("is_global"),
        (col("flags"))["is_nonlocal"].alias("is_nonlocal"),
        (col("flags"))["is_declared_global"].alias("is_declared_global"),
        (col("flags"))["is_local"].alias("is_local"),
        (col("flags"))["is_annotated"].alias("is_annotated"),
        (col("flags"))["is_free"].alias("is_free"),
        (col("flags"))["is_assigned"].alias("is_assigned"),
        (col("flags"))["is_namespace"].alias("is_namespace"),
        col("namespace_count").alias("namespace_count"),
        col("namespace_block_ids").alias("namespace_block_ids"),
    ),
    "ts_ast_calls_check": (
        col("file_id"),
        col("path"),
        f.sum(f.when(~(col("ts_start_byte").is_null()), lit(1)).otherwise(lit(0))).alias(
            "ts_calls"
        ),
        f.sum(f.when(~(col("ast_start_byte").is_null()), lit(1)).otherwise(lit(0))).alias(
            "ast_calls"
        ),
        f.sum(
            f.when(
                (~(col("ts_start_byte").is_null()) & col("ast_start_byte").is_null()), lit(1)
            ).otherwise(lit(0))
        ).alias("ts_only"),
        f.sum(
            f.when(
                (col("ts_start_byte").is_null() & ~(col("ast_start_byte").is_null())), lit(1)
            ).otherwise(lit(0))
        ).alias("ast_only"),
        (
            f.sum(
                f.when(
                    (~(col("ts_start_byte").is_null()) & col("ast_start_byte").is_null()), lit(1)
                ).otherwise(lit(0))
            )
            + f.sum(
                f.when(
                    (col("ts_start_byte").is_null() & ~(col("ast_start_byte").is_null())), lit(1)
                ).otherwise(lit(0))
            )
        ).alias("mismatch_count"),
        (
            (
                f.sum(
                    f.when(
                        (~(col("ts_start_byte").is_null()) & col("ast_start_byte").is_null()),
                        lit(1),
                    ).otherwise(lit(0))
                )
                + f.sum(
                    f.when(
                        (col("ts_start_byte").is_null() & ~(col("ast_start_byte").is_null())),
                        lit(1),
                    ).otherwise(lit(0))
                )
            )
            > lit(0)
        ).alias("mismatch"),
    ),
    "ts_ast_defs_check": (
        col("file_id"),
        col("path"),
        f.sum(f.when(~(col("ts_start_byte").is_null()), lit(1)).otherwise(lit(0))).alias("ts_defs"),
        f.sum(f.when(~(col("ast_start_byte").is_null()), lit(1)).otherwise(lit(0))).alias(
            "ast_defs"
        ),
        f.sum(
            f.when(
                (~(col("ts_start_byte").is_null()) & col("ast_start_byte").is_null()), lit(1)
            ).otherwise(lit(0))
        ).alias("ts_only"),
        f.sum(
            f.when(
                (col("ts_start_byte").is_null() & ~(col("ast_start_byte").is_null())), lit(1)
            ).otherwise(lit(0))
        ).alias("ast_only"),
        (
            f.sum(
                f.when(
                    (~(col("ts_start_byte").is_null()) & col("ast_start_byte").is_null()), lit(1)
                ).otherwise(lit(0))
            )
            + f.sum(
                f.when(
                    (col("ts_start_byte").is_null() & ~(col("ast_start_byte").is_null())), lit(1)
                ).otherwise(lit(0))
            )
        ).alias("mismatch_count"),
        (
            (
                f.sum(
                    f.when(
                        (~(col("ts_start_byte").is_null()) & col("ast_start_byte").is_null()),
                        lit(1),
                    ).otherwise(lit(0))
                )
                + f.sum(
                    f.when(
                        (col("ts_start_byte").is_null() & ~(col("ast_start_byte").is_null())),
                        lit(1),
                    ).otherwise(lit(0))
                )
            )
            > lit(0)
        ).alias("mismatch"),
    ),
    "ts_ast_imports_check": (
        col("file_id"),
        col("path"),
        f.sum(f.when(~(col("ts_start_byte").is_null()), lit(1)).otherwise(lit(0))).alias(
            "ts_imports"
        ),
        f.sum(f.when(~(col("ast_start_byte").is_null()), lit(1)).otherwise(lit(0))).alias(
            "ast_imports"
        ),
        f.sum(
            f.when(
                (~(col("ts_start_byte").is_null()) & col("ast_start_byte").is_null()), lit(1)
            ).otherwise(lit(0))
        ).alias("ts_only"),
        f.sum(
            f.when(
                (col("ts_start_byte").is_null() & ~(col("ast_start_byte").is_null())), lit(1)
            ).otherwise(lit(0))
        ).alias("ast_only"),
        (
            f.sum(
                f.when(
                    (~(col("ts_start_byte").is_null()) & col("ast_start_byte").is_null()), lit(1)
                ).otherwise(lit(0))
            )
            + f.sum(
                f.when(
                    (col("ts_start_byte").is_null() & ~(col("ast_start_byte").is_null())), lit(1)
                ).otherwise(lit(0))
            )
        ).alias("mismatch_count"),
        (
            (
                f.sum(
                    f.when(
                        (~(col("ts_start_byte").is_null()) & col("ast_start_byte").is_null()),
                        lit(1),
                    ).otherwise(lit(0))
                )
                + f.sum(
                    f.when(
                        (col("ts_start_byte").is_null() & ~(col("ast_start_byte").is_null())),
                        lit(1),
                    ).otherwise(lit(0))
                )
            )
            > lit(0)
        ).alias("mismatch"),
    ),
    "ts_calls": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("node_id").alias("ts_node_id"),
        col("parent_id").alias("parent_ts_id"),
        col("callee_kind").alias("callee_kind"),
        col("callee_text").alias("callee_text"),
        col("callee_node_id").alias("callee_node_id"),
        ((col("span"))["start"])["line0"].alias("start_line"),
        ((col("span"))["start"])["col"].alias("start_col"),
        ((col("span"))["end"])["line0"].alias("end_line"),
        ((col("span"))["end"])["col"].alias("end_col"),
        _arrow_cast(lit(0), "Int32").alias("line_base"),
        (col("span"))["col_unit"].alias("col_unit"),
        (col("span"))["end_exclusive"].alias("end_exclusive"),
        ((col("span"))["byte_span"])["byte_start"].alias("start_byte"),
        (
            ((col("span"))["byte_span"])["byte_start"] + ((col("span"))["byte_span"])["byte_len"]
        ).alias("end_byte"),
        _arrow_cast(col("attrs"), "Map(Utf8, Utf8)").alias("attrs"),
    ),
    "ts_captures": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("capture_id").alias("ts_capture_id"),
        col("query_name").alias("query_name"),
        col("capture_name").alias("capture_name"),
        col("pattern_index").alias("pattern_index"),
        col("node_id").alias("ts_node_id"),
        col("node_kind").alias("node_kind"),
        ((col("span"))["start"])["line0"].alias("start_line"),
        ((col("span"))["start"])["col"].alias("start_col"),
        ((col("span"))["end"])["line0"].alias("end_line"),
        ((col("span"))["end"])["col"].alias("end_col"),
        _arrow_cast(lit(0), "Int32").alias("line_base"),
        (col("span"))["col_unit"].alias("col_unit"),
        (col("span"))["end_exclusive"].alias("end_exclusive"),
        ((col("span"))["byte_span"])["byte_start"].alias("start_byte"),
        (
            ((col("span"))["byte_span"])["byte_start"] + ((col("span"))["byte_span"])["byte_len"]
        ).alias("end_byte"),
        _arrow_cast(col("attrs"), "Map(Utf8, Utf8)").alias("attrs"),
    ),
    "ts_cst_docstrings_check": (
        col("file_id"),
        col("path"),
        f.sum(f.when(~(col("ts_start_byte").is_null()), lit(1)).otherwise(lit(0))).alias(
            "ts_docstrings"
        ),
        f.sum(f.when(~(col("cst_start_byte").is_null()), lit(1)).otherwise(lit(0))).alias(
            "cst_docstrings"
        ),
        f.sum(
            f.when(
                (~(col("ts_start_byte").is_null()) & col("cst_start_byte").is_null()), lit(1)
            ).otherwise(lit(0))
        ).alias("ts_only"),
        f.sum(
            f.when(
                (col("ts_start_byte").is_null() & ~(col("cst_start_byte").is_null())), lit(1)
            ).otherwise(lit(0))
        ).alias("cst_only"),
        (
            f.sum(
                f.when(
                    (~(col("ts_start_byte").is_null()) & col("cst_start_byte").is_null()), lit(1)
                ).otherwise(lit(0))
            )
            + f.sum(
                f.when(
                    (col("ts_start_byte").is_null() & ~(col("cst_start_byte").is_null())), lit(1)
                ).otherwise(lit(0))
            )
        ).alias("mismatch_count"),
        (
            (
                f.sum(
                    f.when(
                        (~(col("ts_start_byte").is_null()) & col("cst_start_byte").is_null()),
                        lit(1),
                    ).otherwise(lit(0))
                )
                + f.sum(
                    f.when(
                        (col("ts_start_byte").is_null() & ~(col("cst_start_byte").is_null())),
                        lit(1),
                    ).otherwise(lit(0))
                )
            )
            > lit(0)
        ).alias("mismatch"),
    ),
    "ts_defs": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("node_id").alias("ts_node_id"),
        col("parent_id").alias("parent_ts_id"),
        col("kind").alias("kind"),
        col("name").alias("name"),
        ((col("span"))["start"])["line0"].alias("start_line"),
        ((col("span"))["start"])["col"].alias("start_col"),
        ((col("span"))["end"])["line0"].alias("end_line"),
        ((col("span"))["end"])["col"].alias("end_col"),
        _arrow_cast(lit(0), "Int32").alias("line_base"),
        (col("span"))["col_unit"].alias("col_unit"),
        (col("span"))["end_exclusive"].alias("end_exclusive"),
        ((col("span"))["byte_span"])["byte_start"].alias("start_byte"),
        (
            ((col("span"))["byte_span"])["byte_start"] + ((col("span"))["byte_span"])["byte_len"]
        ).alias("end_byte"),
        _arrow_cast(col("attrs"), "Map(Utf8, Utf8)").alias("attrs"),
    ),
    "ts_docstrings": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("owner_node_id").alias("owner_node_id"),
        col("owner_kind").alias("owner_kind"),
        col("owner_name").alias("owner_name"),
        col("doc_node_id").alias("doc_node_id"),
        col("docstring").alias("docstring"),
        col("source").alias("source"),
        ((col("span"))["start"])["line0"].alias("start_line"),
        ((col("span"))["start"])["col"].alias("start_col"),
        ((col("span"))["end"])["line0"].alias("end_line"),
        ((col("span"))["end"])["col"].alias("end_col"),
        _arrow_cast(lit(0), "Int32").alias("line_base"),
        (col("span"))["col_unit"].alias("col_unit"),
        (col("span"))["end_exclusive"].alias("end_exclusive"),
        ((col("span"))["byte_span"])["byte_start"].alias("start_byte"),
        (
            ((col("span"))["byte_span"])["byte_start"] + ((col("span"))["byte_span"])["byte_len"]
        ).alias("end_byte"),
        _arrow_cast(col("attrs"), "Map(Utf8, Utf8)").alias("attrs"),
    ),
    "ts_edges": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("parent_id").alias("parent_ts_id"),
        col("child_id").alias("child_ts_id"),
        col("field_name").alias("field_name"),
        col("child_index").alias("child_index"),
        _arrow_cast(col("attrs"), "Map(Utf8, Utf8)").alias("attrs"),
    ),
    "ts_errors": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("error_id").alias("ts_error_id"),
        col("node_id").alias("ts_node_id"),
        ((col("span"))["start"])["line0"].alias("start_line"),
        ((col("span"))["start"])["col"].alias("start_col"),
        ((col("span"))["end"])["line0"].alias("end_line"),
        ((col("span"))["end"])["col"].alias("end_col"),
        _arrow_cast(lit(0), "Int32").alias("line_base"),
        (col("span"))["col_unit"].alias("col_unit"),
        (col("span"))["end_exclusive"].alias("end_exclusive"),
        ((col("span"))["byte_span"])["byte_start"].alias("start_byte"),
        (
            ((col("span"))["byte_span"])["byte_start"] + ((col("span"))["byte_span"])["byte_len"]
        ).alias("end_byte"),
        _arrow_cast(lit(value=True), "Boolean").alias("is_error"),
        _arrow_cast(col("attrs"), "Map(Utf8, Utf8)").alias("attrs"),
    ),
    "ts_imports": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("node_id").alias("ts_node_id"),
        col("parent_id").alias("parent_ts_id"),
        col("kind").alias("kind"),
        col("module").alias("module"),
        col("name").alias("name"),
        col("asname").alias("asname"),
        col("alias_index").alias("alias_index"),
        col("level").alias("level"),
        ((col("span"))["start"])["line0"].alias("start_line"),
        ((col("span"))["start"])["col"].alias("start_col"),
        ((col("span"))["end"])["line0"].alias("end_line"),
        ((col("span"))["end"])["col"].alias("end_col"),
        _arrow_cast(lit(0), "Int32").alias("line_base"),
        (col("span"))["col_unit"].alias("col_unit"),
        (col("span"))["end_exclusive"].alias("end_exclusive"),
        ((col("span"))["byte_span"])["byte_start"].alias("start_byte"),
        (
            ((col("span"))["byte_span"])["byte_start"] + ((col("span"))["byte_span"])["byte_len"]
        ).alias("end_byte"),
        _arrow_cast(col("attrs"), "Map(Utf8, Utf8)").alias("attrs"),
    ),
    "ts_missing": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("missing_id").alias("ts_missing_id"),
        col("node_id").alias("ts_node_id"),
        ((col("span"))["start"])["line0"].alias("start_line"),
        ((col("span"))["start"])["col"].alias("start_col"),
        ((col("span"))["end"])["line0"].alias("end_line"),
        ((col("span"))["end"])["col"].alias("end_col"),
        _arrow_cast(lit(0), "Int32").alias("line_base"),
        (col("span"))["col_unit"].alias("col_unit"),
        (col("span"))["end_exclusive"].alias("end_exclusive"),
        ((col("span"))["byte_span"])["byte_start"].alias("start_byte"),
        (
            ((col("span"))["byte_span"])["byte_start"] + ((col("span"))["byte_span"])["byte_len"]
        ).alias("end_byte"),
        _arrow_cast(lit(value=True), "Boolean").alias("is_missing"),
        _arrow_cast(col("attrs"), "Map(Utf8, Utf8)").alias("attrs"),
    ),
    "ts_nodes": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("node_id").alias("ts_node_id"),
        col("node_uid").alias("ts_node_uid"),
        col("parent_id").alias("parent_ts_id"),
        col("kind").alias("ts_type"),
        col("kind_id").alias("ts_kind_id"),
        col("grammar_id").alias("ts_grammar_id"),
        col("grammar_name").alias("ts_grammar_name"),
        ((col("span"))["start"])["line0"].alias("start_line"),
        ((col("span"))["start"])["col"].alias("start_col"),
        ((col("span"))["end"])["line0"].alias("end_line"),
        ((col("span"))["end"])["col"].alias("end_col"),
        _arrow_cast(lit(0), "Int32").alias("line_base"),
        (col("span"))["col_unit"].alias("col_unit"),
        (col("span"))["end_exclusive"].alias("end_exclusive"),
        ((col("span"))["byte_span"])["byte_start"].alias("start_byte"),
        (
            ((col("span"))["byte_span"])["byte_start"] + ((col("span"))["byte_span"])["byte_len"]
        ).alias("end_byte"),
        (col("flags"))["is_named"].alias("is_named"),
        (col("flags"))["has_error"].alias("has_error"),
        (col("flags"))["is_error"].alias("is_error"),
        (col("flags"))["is_missing"].alias("is_missing"),
        (col("flags"))["is_extra"].alias("is_extra"),
        (col("flags"))["has_changes"].alias("has_changes"),
        _arrow_cast(col("attrs"), "Map(Utf8, Utf8)").alias("attrs"),
    ),
    "ts_span_metadata": (
        arrow_metadata(col("nodes")["span"], "line_base").alias("nodes_line_base"),
        arrow_metadata(col("nodes")["span"], "col_unit").alias("nodes_col_unit"),
        arrow_metadata(col("nodes")["span"], "end_exclusive").alias("nodes_end_exclusive"),
        arrow_metadata(col("errors")["span"], "line_base").alias("errors_line_base"),
        arrow_metadata(col("errors")["span"], "col_unit").alias("errors_col_unit"),
        arrow_metadata(col("errors")["span"], "end_exclusive").alias("errors_end_exclusive"),
        arrow_metadata(col("missing")["span"], "line_base").alias("missing_line_base"),
        arrow_metadata(col("missing")["span"], "col_unit").alias("missing_col_unit"),
        arrow_metadata(col("missing")["span"], "end_exclusive").alias("missing_end_exclusive"),
        arrow_metadata(col("captures")["span"], "line_base").alias("captures_line_base"),
        arrow_metadata(col("captures")["span"], "col_unit").alias("captures_col_unit"),
        arrow_metadata(col("captures")["span"], "end_exclusive").alias("captures_end_exclusive"),
        arrow_metadata(col("defs")["span"], "line_base").alias("defs_line_base"),
        arrow_metadata(col("defs")["span"], "col_unit").alias("defs_col_unit"),
        arrow_metadata(col("defs")["span"], "end_exclusive").alias("defs_end_exclusive"),
        arrow_metadata(col("calls")["span"], "line_base").alias("calls_line_base"),
        arrow_metadata(col("calls")["span"], "col_unit").alias("calls_col_unit"),
        arrow_metadata(col("calls")["span"], "end_exclusive").alias("calls_end_exclusive"),
        arrow_metadata(col("imports")["span"], "line_base").alias("imports_line_base"),
        arrow_metadata(col("imports")["span"], "col_unit").alias("imports_col_unit"),
        arrow_metadata(col("imports")["span"], "end_exclusive").alias("imports_end_exclusive"),
        arrow_metadata(col("docstrings")["span"], "line_base").alias("docstrings_line_base"),
        arrow_metadata(col("docstrings")["span"], "col_unit").alias("docstrings_col_unit"),
        arrow_metadata(col("docstrings")["span"], "end_exclusive").alias(
            "docstrings_end_exclusive"
        ),
    ),
    "ts_stats": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("node_count").alias("node_count"),
        col("named_count").alias("named_count"),
        col("error_count").alias("error_count"),
        col("missing_count").alias("missing_count"),
        col("parse_ms").alias("parse_ms"),
        col("parse_timed_out").alias("parse_timed_out"),
        col("incremental_used").alias("incremental_used"),
        col("query_match_count").alias("query_match_count"),
        col("query_capture_count").alias("query_capture_count"),
        col("match_limit_exceeded").alias("match_limit_exceeded"),
    ),
    "python_imports": (),
}

VIEW_SELECT_REGISTRY: Final[ImmutableRegistry[str, tuple[Expr, ...]]] = ImmutableRegistry.from_dict(
    _VIEW_SELECT_EXPRS
)

VIEW_BASE_TABLE: Final[ImmutableRegistry[str, str]] = ImmutableRegistry.from_dict(
    {
        "ast_call_attrs": "ast_calls",
        "ast_calls": "ast_files_v1",
        "ast_def_attrs": "ast_defs",
        "ast_defs": "ast_files_v1",
        "ast_docstrings": "ast_files_v1",
        "ast_edge_attrs": "ast_edges",
        "ast_edges": "ast_files_v1",
        "ast_errors": "ast_files_v1",
        "ast_imports": "ast_files_v1",
        "ast_node_attrs": "ast_nodes",
        "ast_nodes": "ast_files_v1",
        "ast_span_metadata": "ast_files_v1",
        "ast_type_ignores": "ast_files_v1",
        "bytecode_errors": "bytecode_files_v1",
        "bytecode_exception_table": "bytecode_files_v1",
        "cst_call_args": "libcst_files_v1",
        "cst_callsite_span_unnest": "cst_callsites",
        "cst_callsite_spans": "cst_callsites",
        "cst_callsites": "libcst_files_v1",
        "cst_callsites_attr_origin": "cst_callsites",
        "cst_callsites_attrs": "cst_callsites",
        "cst_decorators": "libcst_files_v1",
        "cst_def_span_unnest": "cst_defs",
        "cst_def_spans": "cst_defs",
        "cst_defs": "libcst_files_v1",
        "cst_defs_attr_origin": "cst_defs",
        "cst_defs_attrs": "cst_defs",
        "cst_docstrings": "libcst_files_v1",
        "cst_edges_attr_origin": "cst_edges",
        "cst_edges_attrs": "cst_edges",
        "cst_imports": "libcst_files_v1",
        "cst_imports_attr_origin": "cst_imports",
        "cst_imports_attrs": "cst_imports",
        "cst_nodes_attr_origin": "cst_nodes",
        "cst_nodes_attrs": "cst_nodes",
        "cst_parse_errors": "libcst_files_v1",
        "cst_parse_manifest": "libcst_files_v1",
        "cst_ref_span_unnest": "cst_refs",
        "cst_ref_spans": "cst_refs",
        "cst_refs": "libcst_files_v1",
        "cst_refs_attr_origin": "cst_refs",
        "cst_refs_attrs": "cst_refs",
        "cst_schema_diagnostics": "libcst_files_v1",
        "cst_type_exprs": "libcst_files_v1",
        "py_bc_blocks": "bytecode_files_v1",
        "py_bc_cache_entries": "bytecode_files_v1",
        "py_bc_cfg_edge_attrs": "bytecode_files_v1",
        "py_bc_cfg_edges": "bytecode_files_v1",
        "py_bc_code_units": "bytecode_files_v1",
        "py_bc_consts": "bytecode_files_v1",
        "py_bc_dfg_edges": "bytecode_files_v1",
        "py_bc_error_attrs": "bytecode_files_v1",
        "py_bc_flags_detail": "bytecode_files_v1",
        "py_bc_instruction_attr_keys": "bytecode_files_v1",
        "py_bc_instruction_attrs": "bytecode_files_v1",
        "py_bc_instruction_span_fields": "py_bc_instruction_spans",
        "py_bc_instruction_spans": "py_bc_instructions",
        "py_bc_instructions": "bytecode_files_v1",
        "py_bc_line_table": "bytecode_files_v1",
        "py_bc_metadata": "bytecode_files_v1",
        "symtable_class_methods": "symtable_files_v1",
        "symtable_function_partitions": "symtable_files_v1",
        "symtable_scopes": "symtable_files_v1",
        "symtable_symbol_attrs": "symtable_files_v1",
        "symtable_symbols": "symtable_files_v1",
        "ts_calls": "tree_sitter_files_v1",
        "ts_captures": "tree_sitter_files_v1",
        "ts_defs": "tree_sitter_files_v1",
        "ts_docstrings": "tree_sitter_files_v1",
        "ts_edges": "tree_sitter_files_v1",
        "ts_errors": "tree_sitter_files_v1",
        "ts_imports": "tree_sitter_files_v1",
        "ts_missing": "tree_sitter_files_v1",
        "ts_nodes": "tree_sitter_files_v1",
        "ts_span_metadata": "tree_sitter_files_v1",
        "ts_stats": "tree_sitter_files_v1",
    }
)

NESTED_VIEW_NAMES: Final[frozenset[str]] = frozenset(extract_nested_dataset_names())

ATTRS_VIEW_BASE: Final[ImmutableRegistry[str, str]] = ImmutableRegistry.from_dict(
    {
        "ast_call_attrs": "ast_calls",
        "ast_def_attrs": "ast_defs",
        "ast_edge_attrs": "ast_edges",
        "ast_node_attrs": "ast_nodes",
        "cst_callsites_attrs": "cst_callsites",
        "cst_defs_attrs": "cst_defs",
        "cst_edges_attrs": "cst_edges",
        "cst_imports_attrs": "cst_imports",
        "cst_nodes_attrs": "cst_nodes",
        "cst_refs_attrs": "cst_refs",
        "py_bc_cfg_edge_attrs": "py_bc_cfg_edges",
        "py_bc_error_attrs": "bytecode_errors",
        "py_bc_instruction_attrs": "py_bc_instructions",
        "symtable_symbol_attrs": "symtable_symbols",
    }
)

ATTRS_RAW_BASE: Final[frozenset[str]] = frozenset(
    {
        "py_bc_cfg_edge_attrs",
        "py_bc_error_attrs",
        "py_bc_instruction_attrs",
        "symtable_symbol_attrs",
    }
)

MAP_KEYS_VIEW_BASE: Final[ImmutableRegistry[str, str]] = ImmutableRegistry.from_dict(
    {"py_bc_instruction_attr_keys": "py_bc_instructions"}
)

MAP_VALUES_VIEW_BASE: Final[ImmutableRegistry[str, str]] = ImmutableRegistry.from_dict(
    {"py_bc_instruction_attr_values": "py_bc_instructions"}
)

CST_SPAN_UNNEST_BASE: Final[ImmutableRegistry[str, str]] = ImmutableRegistry.from_dict(
    {
        "cst_callsite_span_unnest": "cst_callsites",
        "cst_def_span_unnest": "cst_defs",
        "cst_ref_span_unnest": "cst_refs",
    }
)


def _nested_base_df(ctx: SessionContext, name: str) -> DataFrame:
    return nested_base_df(ctx, name)


def _should_skip_exprs(exprs: Sequence[object]) -> bool:
    return not exprs


def _base_df(ctx: SessionContext, name: str) -> DataFrame:
    if name in NESTED_VIEW_NAMES:
        return _nested_base_df(ctx, name)
    base_table = VIEW_BASE_TABLE.get(name)
    if base_table is None:
        msg = f"Missing base table mapping for view {name!r}."
        raise KeyError(msg)
    return ctx.table(base_table)


def _map_entries_view_df(
    ctx: SessionContext,
    *,
    name: str,
    base_view: str,
    raw_base: bool,
) -> DataFrame:
    base_df = _nested_base_df(ctx, base_view) if raw_base else _view_df(ctx, base_view)
    df = base_df.with_column("kv", map_entries(col("attrs")))
    df = df.unnest_columns("kv")
    exprs = _view_exprs(name)
    return df.select(*exprs)


def _map_keys_view_df(ctx: SessionContext, *, name: str, base_view: str) -> DataFrame:
    base_df = _nested_base_df(ctx, base_view)
    df = base_df.with_column("attr_key", map_keys(col("attrs")))
    df = df.unnest_columns("attr_key")
    exprs = [expr for expr in _view_exprs(name) if expr.schema_name() != "attr_key"]
    exprs.append(col("attr_key").alias("attr_key"))
    return df.select(*exprs)


def _map_values_view_exprs() -> tuple[Expr, ...]:
    return (
        col("file_id"),
        col("path"),
        col("code_unit_id"),
        col("instr_index"),
        col("offset"),
        col("attr_value"),
        union_tag(col("attr_value")).alias("attr_kind"),
        union_extract(col("attr_value"), "int_value").alias("attr_int"),
        union_extract(col("attr_value"), "bool_value").alias("attr_bool"),
        union_extract(col("attr_value"), "str_value").alias("attr_str"),
    )


def _map_values_view_df(ctx: SessionContext, *, _name: str, base_view: str) -> DataFrame:
    base_df = _nested_base_df(ctx, base_view)
    df = base_df.with_column("attr_value", map_values(col("attrs")))
    df = df.unnest_columns("attr_value")
    return df.select(*_map_values_view_exprs())


def _cst_span_unnest_df(ctx: SessionContext, *, name: str, base_view: str) -> DataFrame:
    base_df = _view_df(ctx, base_view)
    return base_df.select(*_cst_span_unnest_exprs(name))


def _cst_span_unnest_exprs(name: str) -> tuple[Expr, ...]:
    if name == "cst_callsite_span_unnest":
        return (
            col("file_id"),
            col("path"),
            col("call_id"),
            col("call_bstart").alias("bstart"),
            col("call_bend").alias("bend"),
        )
    if name == "cst_def_span_unnest":
        return (
            col("file_id"),
            col("path"),
            col("def_id"),
            col("def_bstart").alias("bstart"),
            col("def_bend").alias("bend"),
        )
    return (
        col("file_id"),
        col("path"),
        col("ref_id"),
        col("bstart").alias("bstart"),
        col("bend").alias("bend"),
    )


def _symtable_class_methods_df(ctx: SessionContext) -> DataFrame:
    scopes = _view_df(ctx, "symtable_scopes")
    expanded = scopes.unnest_columns("class_methods")
    return expanded.select(
        col("file_id"),
        col("path"),
        col("scope_id"),
        col("scope_name"),
        col("class_methods").alias("method_name"),
    )


def _symtable_function_partitions_df(ctx: SessionContext) -> DataFrame:
    scopes = _view_df(ctx, "symtable_scopes")
    filtered = scopes.filter(~col("function_partitions").is_null())
    exprs = _view_exprs("symtable_function_partitions")
    return filtered.select(*exprs)


def _symtable_scope_edges_df(ctx: SessionContext) -> DataFrame:
    scopes = _view_df(ctx, "symtable_scopes")
    parent = scopes.select(
        col("table_id").alias("parent_table_id"),
        col("scope_id").alias("parent_scope_id"),
        col("path").alias("parent_path"),
    )
    child = scopes.select(
        col("table_id").alias("child_table_id"),
        col("scope_id").alias("child_scope_id"),
        col("path").alias("child_path"),
    )
    joined = scopes.join(
        parent,
        join_keys=(["parent_table_id", "path"], ["parent_table_id", "parent_path"]),
        how="left",
        coalesce_duplicate_keys=True,
    )
    joined = joined.join(
        child,
        join_keys=(["table_id", "path"], ["child_table_id", "child_path"]),
        how="left",
        coalesce_duplicate_keys=True,
    )
    return joined.select(
        col("file_id"),
        col("path"),
        col("parent_table_id"),
        col("table_id").alias("child_table_id"),
        col("parent_scope_id"),
        col("child_scope_id"),
    )


def _symtable_namespace_edges_df(ctx: SessionContext) -> DataFrame:
    symbols = _view_df(ctx, "symtable_symbols")
    expanded = symbols.with_column("child_table_id", col("namespace_block_ids")).unnest_columns(
        "child_table_id"
    )
    scopes = _view_df(ctx, "symtable_scopes").select(
        col("table_id").alias("child_table_id"),
        col("scope_id").alias("child_scope_id"),
        col("path").alias("child_path"),
    )
    joined = expanded.join(
        scopes,
        join_keys=(["child_table_id", "path"], ["child_table_id", "child_path"]),
        how="left",
        coalesce_duplicate_keys=True,
    )
    return joined.select(
        col("file_id"),
        col("path"),
        col("scope_id"),
        col("name").alias("symbol_name"),
        col("child_table_id"),
        col("child_scope_id"),
        col("namespace_count"),
    )


def _scip_occurrences_norm_df(ctx: SessionContext) -> DataFrame:
    table_names = table_names_snapshot(ctx)
    if "scip_occurrences" not in table_names:
        msg = "Missing SCIP occurrences table 'scip_occurrences' for scip_occurrences_norm."
        raise ValueError(msg)
    if "file_line_index_v1" not in table_names:
        msg = "Missing line index table 'file_line_index_v1' for scip_occurrences_norm."
        raise ValueError(msg)
    from datafusion_engine.udf.runtime import rust_udf_snapshot, validate_required_udfs

    snapshot = rust_udf_snapshot(ctx)
    validate_required_udfs(snapshot, required=("col_to_byte",))

    scip = _view_df(ctx, "scip_occurrences")
    scip = scip.with_column("start_line_no", col("start_line") - col("line_base"))
    scip = scip.with_column("end_line_no", col("end_line") - col("line_base"))

    line_index = ctx.table("file_line_index_v1")
    start_idx = line_index.select(
        col("path").alias("start_path"),
        col("line_no").alias("start_line_no"),
        col("line_start_byte").alias("start_line_start_byte"),
        col("line_text").alias("start_line_text"),
    )
    end_idx = line_index.select(
        col("path").alias("end_path"),
        col("line_no").alias("end_line_no"),
        col("line_start_byte").alias("end_line_start_byte"),
        col("line_text").alias("end_line_text"),
    )

    joined = scip.join(
        start_idx,
        join_keys=(
            ["path", "start_line_no"],
            ["start_path", "start_line_no"],
        ),
        how="left",
        coalesce_duplicate_keys=True,
    )
    joined = joined.join(
        end_idx,
        join_keys=(
            ["path", "end_line_no"],
            ["end_path", "end_line_no"],
        ),
        how="left",
        coalesce_duplicate_keys=True,
    )

    def _byte_offset(line_start: str, line_text: str, col_name: str) -> Expr:
        base = _arrow_cast(col(line_start), "Int64")
        char_col = _arrow_cast(col(col_name), "Int64")
        offset = col_to_byte(col(line_text), char_col, col("col_unit"))
        guard = (
            col(line_start).is_null()
            | col(line_text).is_null()
            | col(col_name).is_null()
            | col("col_unit").is_null()
        )
        return f.when(guard, _arrow_cast(lit(None), "Int64")).otherwise(base + offset)

    df = joined.with_column(
        "bstart",
        _byte_offset("start_line_start_byte", "start_line_text", "start_char"),
    )
    df = df.with_column(
        "bend",
        _byte_offset("end_line_start_byte", "end_line_text", "end_char"),
    )
    return df.drop(
        "start_line_no",
        "end_line_no",
        "start_path",
        "end_path",
        "start_line_start_byte",
        "end_line_start_byte",
        "start_line_text",
        "end_line_text",
    )


def _empty_ts_ast_check_df(ctx: SessionContext) -> DataFrame:
    empty_schema = pa.schema(
        [
            string_field("file_id", nullable=False),
            string_field("path", nullable=False),
            int64_field("ts_start_byte"),
            int64_field("ts_end_byte"),
            int64_field("ast_start_byte"),
            int64_field("ast_end_byte"),
        ]
    )
    empty_table = pa.Table.from_arrays(
        [pa.array([], type=field.type) for field in empty_schema],
        schema=empty_schema,
    )
    empty_name = f"__ts_ast_check_empty_{uuid7_hex()}"
    ctx.from_arrow(empty_table, name=empty_name)
    return ctx.table(empty_name)


def _ts_ast_check_df(ctx: SessionContext, *, ts_view: str, ast_view: str, label: str) -> DataFrame:
    if "file_line_index_v1" not in table_names_snapshot(ctx):
        return _empty_ts_ast_check_df(ctx)
    ts = _view_df(ctx, ts_view).select(
        col("file_id"),
        col("path"),
        col("start_byte").alias("ts_start_byte"),
        col("end_byte").alias("ts_end_byte"),
    )
    ast_base = _view_df(ctx, ast_view).select(
        col("file_id"),
        col("path"),
        col("lineno"),
        col("col_offset"),
        col("end_lineno"),
        col("end_col_offset"),
        col("line_base"),
    )
    ast_base = ast_base.with_column("start_line_no", col("lineno") - col("line_base")).with_column(
        "end_line_no",
        col("end_lineno") - col("line_base"),
    )
    start_idx = ctx.table("file_line_index_v1").select(
        col("file_id").alias("start_file_id"),
        col("path").alias("start_path"),
        col("line_no").alias("start_line_no"),
        col("line_start_byte").alias("start_line_start_byte"),
    )
    end_idx = ctx.table("file_line_index_v1").select(
        col("file_id").alias("end_file_id"),
        col("path").alias("end_path"),
        col("line_no").alias("end_line_no"),
        col("line_start_byte").alias("end_line_start_byte"),
    )
    ast_joined = ast_base.join(
        start_idx,
        join_keys=(
            ["file_id", "path", "start_line_no"],
            ["start_file_id", "start_path", "start_line_no"],
        ),
        how="left",
        coalesce_duplicate_keys=True,
    )
    ast_joined = ast_joined.join(
        end_idx,
        join_keys=(["file_id", "path", "end_line_no"], ["end_file_id", "end_path", "end_line_no"]),
        how="left",
        coalesce_duplicate_keys=True,
    )
    ast = ast_joined.select(
        col("file_id"),
        col("path"),
        (col("start_line_start_byte") + _arrow_cast(col("col_offset"), "Int64")).alias(
            "ast_start_byte"
        ),
        (col("end_line_start_byte") + _arrow_cast(col("end_col_offset"), "Int64")).alias(
            "ast_end_byte"
        ),
    )
    joined = ts.join(
        ast,
        join_keys=(
            ["file_id", "path", "ts_start_byte", "ts_end_byte"],
            ["file_id", "path", "ast_start_byte", "ast_end_byte"],
        ),
        how="full",
        coalesce_duplicate_keys=True,
    )
    ts_present = col("ts_start_byte").is_not_null()
    ast_present = col("ast_start_byte").is_not_null()
    ts_only_expr = f.when(ts_present & col("ast_start_byte").is_null(), lit(1)).otherwise(lit(0))
    ast_only_expr = f.when(col("ts_start_byte").is_null() & ast_present, lit(1)).otherwise(lit(0))
    agg = joined.aggregate(
        [col("file_id"), col("path")],
        [
            f.sum(f.when(ts_present, lit(1)).otherwise(lit(0))).alias(f"ts_{label}"),
            f.sum(f.when(ast_present, lit(1)).otherwise(lit(0))).alias(f"ast_{label}"),
            f.sum(ts_only_expr).alias("ts_only"),
            f.sum(ast_only_expr).alias("ast_only"),
        ],
    )
    agg = agg.with_column("mismatch_count", col("ts_only") + col("ast_only"))
    return agg.with_column("mismatch", col("mismatch_count") > lit(0))


def _ts_cst_docstrings_check_df(ctx: SessionContext) -> DataFrame:
    ts = _view_df(ctx, "ts_docstrings").select(
        col("file_id"),
        col("path"),
        col("start_byte").alias("ts_start_byte"),
        col("end_byte").alias("ts_end_byte"),
    )
    cst = _view_df(ctx, "cst_docstrings").select(
        col("file_id"),
        col("path"),
        _arrow_cast(col("bstart"), "Int64").alias("cst_start_byte"),
        _arrow_cast(col("bend"), "Int64").alias("cst_end_byte"),
    )
    joined = ts.join(
        cst,
        join_keys=(
            ["file_id", "path", "ts_start_byte", "ts_end_byte"],
            ["file_id", "path", "cst_start_byte", "cst_end_byte"],
        ),
        how="full",
        coalesce_duplicate_keys=True,
    )
    ts_present = col("ts_start_byte").is_not_null()
    cst_present = col("cst_start_byte").is_not_null()
    ts_only_expr = f.when(ts_present & col("cst_start_byte").is_null(), lit(1)).otherwise(lit(0))
    cst_only_expr = f.when(col("ts_start_byte").is_null() & cst_present, lit(1)).otherwise(lit(0))
    agg = joined.aggregate(
        [col("file_id"), col("path")],
        [
            f.sum(f.when(ts_present, lit(1)).otherwise(lit(0))).alias("ts_docstrings"),
            f.sum(f.when(cst_present, lit(1)).otherwise(lit(0))).alias("cst_docstrings"),
            f.sum(ts_only_expr).alias("ts_only"),
            f.sum(cst_only_expr).alias("cst_only"),
        ],
    )
    agg = agg.with_column("mismatch_count", col("ts_only") + col("cst_only"))
    return agg.with_column("mismatch", col("mismatch_count") > lit(0))


def _python_imports_df(ctx: SessionContext) -> DataFrame:
    available = table_names_snapshot(ctx)
    if "python_imports_v1" in available:
        return ctx.table("python_imports_v1").select(
            col("file_id"),
            col("path"),
            col("source"),
            col("kind"),
            col("module"),
            col("name"),
            col("asname"),
            _arrow_cast(col("level"), "Int32").alias("level"),
            _arrow_cast(col("is_star"), "Boolean").alias("is_star"),
        )
    frames: list[DataFrame] = []
    if "ast_files_v1" in available:
        ast_df = _view_df(ctx, "ast_imports").select(
            col("file_id"),
            col("path"),
            lit("ast").alias("source"),
            col("kind"),
            col("module"),
            col("name"),
            col("asname"),
            _arrow_cast(col("level"), "Int32").alias("level"),
            _arrow_cast(lit(value=False), "Boolean").alias("is_star"),
        )
        frames.append(ast_df)
    if "libcst_files_v1" in available:
        cst_df = _view_df(ctx, "cst_imports").select(
            col("file_id"),
            col("path"),
            lit("cst").alias("source"),
            col("kind"),
            col("module"),
            col("name"),
            col("asname"),
            _arrow_cast(col("relative_level"), "Int32").alias("level"),
            _arrow_cast(col("is_star"), "Boolean").alias("is_star"),
        )
        frames.append(cst_df)
    if "tree_sitter_files_v1" in available:
        ts_df = _view_df(ctx, "ts_imports").select(
            col("file_id"),
            col("path"),
            lit("tree_sitter").alias("source"),
            col("kind"),
            col("module"),
            col("name"),
            col("asname"),
            _arrow_cast(col("level"), "Int32").alias("level"),
            _arrow_cast(lit(value=False), "Boolean").alias("is_star"),
        )
        frames.append(ts_df)
    if not frames:
        return _empty_python_imports_df(ctx)
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.union(frame)
    return combined


def _empty_python_imports_df(ctx: SessionContext) -> DataFrame:
    empty_schema = pa.schema(
        [
            string_field("file_id", nullable=False),
            string_field("path", nullable=False),
            string_field("source"),
            string_field("kind"),
            string_field("module"),
            string_field("name"),
            string_field("asname"),
            int32_field("level"),
            bool_field("is_star"),
        ]
    )
    empty_table = pa.Table.from_arrays(
        [pa.array([], type=field.type) for field in empty_schema],
        schema=empty_schema,
    )
    empty_name = f"__python_imports_empty_{uuid7_hex()}"
    ctx.from_arrow(empty_table, name=empty_name)
    return ctx.table(empty_name)


def _view_df(ctx: SessionContext, name: str) -> DataFrame:
    builder = _CUSTOM_BUILDERS.get(name)
    if builder is not None:
        return builder(ctx)
    base_df = _base_df(ctx, name)
    if name in NESTED_VIEW_NAMES and name not in SCIP_VIEW_NAMES:
        return base_df
    exprs = VIEW_SELECT_REGISTRY.get(name) or ()
    if _should_skip_exprs(exprs):
        return base_df
    return base_df.select(*exprs)


def _base_table_for_view(name: str) -> str:
    base_table = VIEW_BASE_TABLE.get(name)
    if base_table is not None:
        return base_table
    if name in NESTED_VIEW_NAMES and name not in SCIP_VIEW_NAMES:
        return name
    msg = f"Missing base table mapping for view {name!r}."
    raise KeyError(msg)


def _register_builder_map() -> MutableRegistry[str, Callable[[SessionContext], DataFrame]]:
    builders: MutableRegistry[str, Callable[[SessionContext], DataFrame]] = MutableRegistry()
    for view_name in ATTRS_VIEW_BASE:
        base_view = ATTRS_VIEW_BASE.get(view_name)
        if base_view is None:
            msg = f"Missing attribute view base for {view_name!r}."
            raise KeyError(msg)
        raw_base = view_name in ATTRS_RAW_BASE
        builders.register(
            view_name,
            partial(
                _map_entries_view_df,
                name=view_name,
                base_view=base_view,
                raw_base=raw_base,
            ),
        )
    for view_name in MAP_KEYS_VIEW_BASE:
        base_view = MAP_KEYS_VIEW_BASE.get(view_name)
        if base_view is None:
            msg = f"Missing map keys base for {view_name!r}."
            raise KeyError(msg)
        builders.register(
            view_name,
            partial(
                _map_keys_view_df,
                name=view_name,
                base_view=base_view,
            ),
        )
    for view_name in MAP_VALUES_VIEW_BASE:
        base_view = MAP_VALUES_VIEW_BASE.get(view_name)
        if base_view is None:
            msg = f"Missing map values base for {view_name!r}."
            raise KeyError(msg)
        builders.register(
            view_name,
            partial(
                _map_values_view_df,
                _name=view_name,
                base_view=base_view,
            ),
        )
    for view_name in CST_SPAN_UNNEST_BASE:
        base_view = CST_SPAN_UNNEST_BASE.get(view_name)
        if base_view is None:
            msg = f"Missing CST span base for {view_name!r}."
            raise KeyError(msg)
        builders.register(
            view_name,
            partial(
                _cst_span_unnest_df,
                name=view_name,
                base_view=base_view,
            ),
        )
    builders.register("symtable_class_methods", _symtable_class_methods_df)
    builders.register("symtable_function_partitions", _symtable_function_partitions_df)
    builders.register("symtable_namespace_edges", _symtable_namespace_edges_df)
    builders.register("symtable_scope_edges", _symtable_scope_edges_df)
    builders.register("scip_occurrences_norm", _scip_occurrences_norm_df)
    builders.register(
        "ts_ast_calls_check",
        partial(
            _ts_ast_check_df,
            ts_view="ts_calls",
            ast_view="ast_calls",
            label="calls",
        ),
    )
    builders.register(
        "ts_ast_defs_check",
        partial(
            _ts_ast_check_df,
            ts_view="ts_defs",
            ast_view="ast_defs",
            label="defs",
        ),
    )
    builders.register(
        "ts_ast_imports_check",
        partial(
            _ts_ast_check_df,
            ts_view="ts_imports",
            ast_view="ast_imports",
            label="imports",
        ),
    )
    builders.register("ts_cst_docstrings_check", _ts_cst_docstrings_check_df)
    builders.register("python_imports", _python_imports_df)
    return builders


_CUSTOM_BUILDERS: Final[MutableRegistry[str, Callable[[SessionContext], DataFrame]]] = (
    _register_builder_map()
)

_CUSTOM_VIEW_DEPENDENCIES: Final[ImmutableRegistry[str, tuple[str, ...]]] = (
    ImmutableRegistry.from_dict(
        {
            "python_imports": ("python_imports_v1", "ast_imports", "cst_imports", "ts_imports"),
            "scip_occurrences_norm": ("scip_occurrences", "file_line_index_v1"),
            "symtable_namespace_edges": ("symtable_scopes", "symtable_symbols"),
            "ts_ast_calls_check": ("ts_calls", "ast_calls"),
            "ts_ast_defs_check": ("ts_defs", "ast_defs"),
            "ts_ast_imports_check": ("ts_imports", "ast_imports"),
            "ts_cst_docstrings_check": ("ts_docstrings", "cst_docstrings"),
        }
    )
)


def _resolve_root_table(name: str) -> str:
    visited: set[str] = set()
    current = name
    while True:
        if current in visited:
            msg = f"Cycle detected while resolving base table for view {name!r}."
            raise ValueError(msg)
        visited.add(current)
        if current in NESTED_VIEW_NAMES:
            root, _path = nested_path_for(current)
            return root
        base_table = (
            VIEW_BASE_TABLE.get(current)
            or ATTRS_VIEW_BASE.get(current)
            or MAP_KEYS_VIEW_BASE.get(current)
            or MAP_VALUES_VIEW_BASE.get(current)
            or CST_SPAN_UNNEST_BASE.get(current)
        )
        if base_table is None:
            return current
        current = base_table


def _required_root_tables(name: str) -> tuple[str, ...]:
    dependencies = _CUSTOM_VIEW_DEPENDENCIES.get(name)
    if dependencies is None:
        return (_resolve_root_table(name),)
    roots = {_resolve_root_table(dep) for dep in dependencies}
    return tuple(sorted(roots))


def _required_roots_available(ctx: SessionContext, name: str) -> bool:
    roots = _required_root_tables(name)
    if name == "python_imports":
        return any(ctx.table_exist(root) for root in roots)
    return all(ctx.table_exist(root) for root in roots)


def registry_view_specs(
    ctx: SessionContext,
    *,
    exclude: Sequence[str] | None = None,
) -> tuple[ViewSpec, ...]:
    """Return ViewSpecs for registry views using builder definitions.

    Parameters
    ----------
    ctx:
        DataFusion session context used to infer schemas.
    exclude:
        Optional view names to skip.

    Returns
    -------
    tuple[ViewSpec, ...]
        View specifications for registry views.
    """
    excluded = set(exclude or ())
    specs: list[ViewSpec] = []
    for name in sorted(VIEW_SELECT_REGISTRY):
        if name in excluded:
            continue
        if not _required_roots_available(ctx, name):
            continue
        builder = partial(_view_df, name=name)
        specs.append(
            view_spec_from_builder(
                ViewSpecInputs(
                    ctx=ctx,
                    name=name,
                    builder=builder,
                )
            )
        )
    return tuple(specs)


def register_all_views(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    runtime_profile: DataFusionRuntimeProfile | None = None,
    include_registry_views: bool = True,
) -> None:
    """Register registry + pipeline views in dependency order."""
    if include_registry_views:
        from datafusion_engine.views.graph import (
            ViewGraphRuntimeOptions,
            register_view_graph,
        )

        registry_nodes = registry_view_nodes(ctx, runtime_profile=runtime_profile)
        if registry_nodes:
            register_view_graph(
                ctx,
                nodes=registry_nodes,
                snapshot=snapshot,
                runtime_options=ViewGraphRuntimeOptions(
                    runtime_profile=runtime_profile,
                    require_artifacts=True,
                ),
            )
    from datafusion_engine.views.graph import register_view_graph
    from datafusion_engine.views.registry_specs import view_graph_nodes

    pre_nodes = view_graph_nodes(
        ctx,
        snapshot=snapshot,
        runtime_profile=runtime_profile,
        stage="pre_cpg",
    )
    if pre_nodes:
        from datafusion_engine.views.graph import ViewGraphRuntimeOptions

        register_view_graph(
            ctx,
            nodes=pre_nodes,
            snapshot=snapshot,
            runtime_options=ViewGraphRuntimeOptions(
                runtime_profile=runtime_profile,
                require_artifacts=True,
            ),
        )
    cpg_nodes = view_graph_nodes(
        ctx,
        snapshot=snapshot,
        runtime_profile=runtime_profile,
        stage="cpg",
    )
    if cpg_nodes:
        from datafusion_engine.views.graph import ViewGraphRuntimeOptions

        register_view_graph(
            ctx,
            nodes=cpg_nodes,
            snapshot=snapshot,
            runtime_options=ViewGraphRuntimeOptions(
                runtime_profile=runtime_profile,
                require_artifacts=True,
            ),
        )


def registry_view_nodes(
    ctx: SessionContext,
    *,
    exclude: Sequence[str] | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> tuple[ViewNode, ...]:
    """Return view nodes for registry fragment views.

    Returns
    -------
    tuple[ViewNode, ...]
        Registry view nodes with DataFusion plan bundles.

    Raises
    ------
    ValueError
        Raised when the runtime profile is unavailable.
    """
    excluded = set(exclude or ())
    from datafusion_engine.plan.bundle import PlanBundleOptions, build_plan_bundle

    if runtime_profile is None:
        msg = "Runtime profile is required for view planning."
        raise ValueError(msg)
    session_runtime = runtime_profile.session_runtime()
    nodes: list[ViewNode] = []
    for name in sorted(VIEW_SELECT_REGISTRY):
        if name in excluded:
            continue
        builder = partial(_view_df, name=name)
        df = builder(ctx)
        plan_bundle = build_plan_bundle(
            ctx,
            df,
            options=PlanBundleOptions(
                compute_execution_plan=True,
                session_runtime=session_runtime,
            ),
        )
        nodes.append(
            ViewNode(
                name=name,
                deps=(),
                builder=builder,
                plan_bundle=plan_bundle,
            )
        )
    return tuple(nodes)


def ensure_view_graph(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    include_registry_views: bool = True,
    scan_units: Sequence[ScanUnit] = (),
) -> Mapping[str, object]:
    """Install the UDF platform (if needed) and register all views.

    Parameters
    ----------
    ctx
        Active DataFusion session context.
    runtime_profile
        Runtime profile used for registration policies and diagnostics.
    include_registry_views
        Whether to register registry-fragment views before pipeline views.
    scan_units
        Optional scan units that pin Delta versions and candidate files
        before view registration.

    Returns
    -------
    Mapping[str, object]
        Rust UDF snapshot used for view registration.

    Raises
    ------
    ValueError
        Raised when the runtime profile is unavailable.
    """
    if runtime_profile is None:
        msg = "Runtime profile is required for view registration."
        raise ValueError(msg)
    from datafusion_engine.udf.platform import install_rust_udf_platform
    from datafusion_engine.udf.runtime import rust_udf_snapshot

    options = _platform_options(runtime_profile)
    platform = install_rust_udf_platform(ctx, options=options)
    snapshot = platform.snapshot or rust_udf_snapshot(ctx)
    if scan_units:
        from datafusion_engine.dataset.resolution import apply_scan_unit_overrides

        apply_scan_unit_overrides(
            ctx,
            scan_units=scan_units,
            runtime_profile=runtime_profile,
        )
    try:
        register_all_views(
            ctx,
            snapshot=snapshot,
            runtime_profile=runtime_profile,
            include_registry_views=include_registry_views,
        )
    except Exception as exc:
        from datafusion_engine.views.graph import SchemaContractViolationError

        if isinstance(exc, SchemaContractViolationError) and runtime_profile is not None:
            from datafusion_engine.lineage.diagnostics import record_artifact

            payload = {
                "view": exc.table_name,
                "violations": [
                    {
                        "violation_type": violation.violation_type.value,
                        "column_name": violation.column_name,
                        "expected": violation.expected,
                        "actual": violation.actual,
                    }
                    for violation in exc.violations
                ],
            }
            record_artifact(runtime_profile, "view_contract_violations_v1", payload)
        raise
    if runtime_profile is not None:
        from datafusion_engine.lineage.diagnostics import (
            record_artifact,
            rust_udf_snapshot_payload,
            view_fingerprint_payload,
            view_udf_parity_payload,
        )
        from datafusion_engine.views.registry_specs import view_graph_nodes

        nodes = view_graph_nodes(
            ctx,
            snapshot=snapshot,
            runtime_profile=runtime_profile,
        )
        record_artifact(
            runtime_profile,
            "rust_udf_snapshot_v1",
            rust_udf_snapshot_payload(snapshot),
        )
        record_artifact(
            runtime_profile,
            "view_udf_parity_v1",
            view_udf_parity_payload(snapshot=snapshot, view_nodes=nodes, ctx=ctx),
        )
        record_artifact(
            runtime_profile,
            "view_fingerprints_v1",
            view_fingerprint_payload(view_nodes=nodes),
        )
    return snapshot


def _platform_options(
    runtime_profile: DataFusionRuntimeProfile | None,
) -> RustUdfPlatformOptions:
    from datafusion_engine.udf.platform import RustUdfPlatformOptions

    profile = runtime_profile
    if profile is None:
        return RustUdfPlatformOptions(
            enable_udfs=True,
            enable_function_factory=True,
            enable_expr_planners=True,
            expr_planner_names=("codeanatomy_domain",),
            strict=True,
        )
    return RustUdfPlatformOptions(
        enable_udfs=getattr(profile, "enable_udfs", True),
        enable_async_udfs=getattr(profile, "enable_async_udfs", False),
        async_udf_timeout_ms=getattr(profile, "async_udf_timeout_ms", None),
        async_udf_batch_size=getattr(profile, "async_udf_batch_size", None),
        enable_function_factory=getattr(profile, "enable_function_factory", True),
        enable_expr_planners=getattr(profile, "enable_expr_planners", True),
        function_factory_hook=getattr(profile, "function_factory_hook", None),
        expr_planner_hook=getattr(profile, "expr_planner_hook", None),
        expr_planner_names=getattr(profile, "expr_planner_names", ()),
        strict=True,
    )


__all__ = [
    "ensure_view_graph",
    "register_all_views",
    "registry_view_nodes",
    "registry_view_specs",
]
