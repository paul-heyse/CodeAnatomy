"""Generated view expression definitions (DataFusion Expr builders)."""

from __future__ import annotations

from typing import Final

from datafusion import col, lit
from datafusion import functions as f
from datafusion.expr import Expr

from datafusion_ext import (
    arrow_metadata,
    list_extract,
    map_extract,
    map_keys,
    prefixed_hash64,
    stable_id,
    union_extract,
    union_tag,
)


def _arrow_cast(expr: Expr, data_type: str) -> Expr:
    return f.arrow_cast(expr, lit(data_type))


VIEW_SELECT_EXPRS: Final[dict[str, tuple[Expr, ...]]] = {
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
        f.named_struct(
            [
                ("start", (col("span"))["start"]),
                ("end", (col("span"))["end"]),
                ("byte_span", (col("span"))["byte_span"]),
                ("col_unit", (col("span"))["col_unit"]),
                ("end_exclusive", (col("span"))["end_exclusive"]),
            ]
        ).alias("span"),
        col("attrs").alias("attrs"),
        f.named_struct(
            [
                (
                    "span",
                    f.named_struct(
                        [
                            ("start", (col("span"))["start"]),
                            ("end", (col("span"))["end"]),
                            ("byte_span", (col("span"))["byte_span"]),
                            ("col_unit", (col("span"))["col_unit"]),
                            ("end_exclusive", (col("span"))["end_exclusive"]),
                        ]
                    ),
                ),
                ("attrs", col("attrs")),
            ]
        ).alias("ast_record"),
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
        f.named_struct(
            [
                ("start", (col("span"))["start"]),
                ("end", (col("span"))["end"]),
                ("byte_span", (col("span"))["byte_span"]),
                ("col_unit", (col("span"))["col_unit"]),
                ("end_exclusive", (col("span"))["end_exclusive"]),
            ]
        ).alias("span"),
        col("attrs").alias("attrs"),
        f.named_struct(
            [
                (
                    "span",
                    f.named_struct(
                        [
                            ("start", (col("span"))["start"]),
                            ("end", (col("span"))["end"]),
                            ("byte_span", (col("span"))["byte_span"]),
                            ("col_unit", (col("span"))["col_unit"]),
                            ("end_exclusive", (col("span"))["end_exclusive"]),
                        ]
                    ),
                ),
                ("attrs", col("attrs")),
            ]
        ).alias("ast_record"),
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
        f.named_struct(
            [
                ("start", (col("span"))["start"]),
                ("end", (col("span"))["end"]),
                ("byte_span", (col("span"))["byte_span"]),
                ("col_unit", (col("span"))["col_unit"]),
                ("end_exclusive", (col("span"))["end_exclusive"]),
            ]
        ).alias("span"),
        col("attrs").alias("attrs"),
        f.named_struct(
            [
                (
                    "span",
                    f.named_struct(
                        [
                            ("start", (col("span"))["start"]),
                            ("end", (col("span"))["end"]),
                            ("byte_span", (col("span"))["byte_span"]),
                            ("col_unit", (col("span"))["col_unit"]),
                            ("end_exclusive", (col("span"))["end_exclusive"]),
                        ]
                    ),
                ),
                ("attrs", col("attrs")),
            ]
        ).alias("ast_record"),
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
        f.named_struct(
            [
                ("start", (col("span"))["start"]),
                ("end", (col("span"))["end"]),
                ("byte_span", (col("span"))["byte_span"]),
                ("col_unit", (col("span"))["col_unit"]),
                ("end_exclusive", (col("span"))["end_exclusive"]),
            ]
        ).alias("span"),
        col("attrs").alias("attrs"),
        f.named_struct(
            [
                (
                    "span",
                    f.named_struct(
                        [
                            ("start", (col("span"))["start"]),
                            ("end", (col("span"))["end"]),
                            ("byte_span", (col("span"))["byte_span"]),
                            ("col_unit", (col("span"))["col_unit"]),
                            ("end_exclusive", (col("span"))["end_exclusive"]),
                        ]
                    ),
                ),
                ("attrs", col("attrs")),
            ]
        ).alias("ast_record"),
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
        f.named_struct(
            [
                ("start", (col("span"))["start"]),
                ("end", (col("span"))["end"]),
                ("byte_span", (col("span"))["byte_span"]),
                ("col_unit", (col("span"))["col_unit"]),
                ("end_exclusive", (col("span"))["end_exclusive"]),
            ]
        ).alias("span"),
        col("attrs").alias("attrs"),
        f.named_struct(
            [
                (
                    "span",
                    f.named_struct(
                        [
                            ("start", (col("span"))["start"]),
                            ("end", (col("span"))["end"]),
                            ("byte_span", (col("span"))["byte_span"]),
                            ("col_unit", (col("span"))["col_unit"]),
                            ("end_exclusive", (col("span"))["end_exclusive"]),
                        ]
                    ),
                ),
                ("attrs", col("attrs")),
            ]
        ).alias("ast_record"),
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
        f.named_struct(
            [
                ("start", (col("span"))["start"]),
                ("end", (col("span"))["end"]),
                ("byte_span", (col("span"))["byte_span"]),
                ("col_unit", (col("span"))["col_unit"]),
                ("end_exclusive", (col("span"))["end_exclusive"]),
            ]
        ).alias("span"),
        col("attrs").alias("attrs"),
        f.named_struct(
            [
                (
                    "span",
                    f.named_struct(
                        [
                            ("start", (col("span"))["start"]),
                            ("end", (col("span"))["end"]),
                            ("byte_span", (col("span"))["byte_span"]),
                            ("col_unit", (col("span"))["col_unit"]),
                            ("end_exclusive", (col("span"))["end_exclusive"]),
                        ]
                    ),
                ),
                ("attrs", col("attrs")),
            ]
        ).alias("ast_record"),
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
        f.named_struct(
            [
                ("start", (col("span"))["start"]),
                ("end", (col("span"))["end"]),
                ("byte_span", (col("span"))["byte_span"]),
                ("col_unit", (col("span"))["col_unit"]),
                ("end_exclusive", (col("span"))["end_exclusive"]),
            ]
        ).alias("span"),
        col("attrs").alias("attrs"),
        f.named_struct(
            [
                (
                    "span",
                    f.named_struct(
                        [
                            ("start", (col("span"))["start"]),
                            ("end", (col("span"))["end"]),
                            ("byte_span", (col("span"))["byte_span"]),
                            ("col_unit", (col("span"))["col_unit"]),
                            ("end_exclusive", (col("span"))["end_exclusive"]),
                        ]
                    ),
                ),
                ("attrs", col("attrs")),
            ]
        ).alias("ast_record"),
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
        (col("span"))["bstart"].alias("bstart"),
        (col("span"))["bend"].alias("bend"),
    ),
    "cst_callsite_spans": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("call_id").alias("call_id"),
        col("call_bstart").alias("bstart"),
        col("call_bend").alias("bend"),
        f.named_struct([("bstart", col("call_bstart")), ("bend", col("call_bend"))]).alias("span"),
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
        (col("span"))["bstart"].alias("bstart"),
        (col("span"))["bend"].alias("bend"),
    ),
    "cst_def_spans": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("def_id").alias("def_id"),
        col("def_bstart").alias("bstart"),
        col("def_bend").alias("bend"),
        f.named_struct([("bstart", col("def_bstart")), ("bend", col("def_bend"))]).alias("span"),
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
        (col("n0")["n0_item"])["schema_fingerprint"].alias("schema_fingerprint"),
    ),
    "cst_ref_span_unnest": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ref_id").alias("ref_id"),
        (col("span"))["bstart"].alias("bstart"),
        (col("span"))["bend"].alias("bend"),
    ),
    "cst_ref_spans": (
        col("file_id").alias("file_id"),
        col("path").alias("path"),
        col("ref_id").alias("ref_id"),
        col("bstart").alias("bstart"),
        col("bend").alias("bend"),
        f.named_struct([("bstart", col("bstart")), ("bend", col("bend"))]).alias("span"),
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
                "list<item:\nstruct<file_id: string not null, path: string not null, file_sha256: string,\nencoding: string, default_indent: string, default_newline: string,\nhas_trailing_newline: bool, future_imports: list<item: string>, module_name:\nstring, package_name: string, libcst_version: string, parser_backend:\nstring, parsed_python_version: string, schema_fingerprint: string>>",
            )
        ).alias("parse_manifest_cast_type"),
        arrow_metadata(col("parse_manifest")).alias("parse_manifest_meta"),
        arrow_metadata(
            _arrow_cast(
                col("parse_manifest"),
                "list<item:\nstruct<file_id: string not null, path: string not null, file_sha256: string,\nencoding: string, default_indent: string, default_newline: string,\nhas_trailing_newline: bool, future_imports: list<item: string>, module_name:\nstring, package_name: string, libcst_version: string, parser_backend:\nstring, parsed_python_version: string, schema_fingerprint: string>>",
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
        f.named_struct(
            [
                (
                    "start",
                    f.named_struct(
                        [("line0", col("pos_start_line")), ("col", col("pos_start_col"))]
                    ),
                ),
                (
                    "end",
                    f.named_struct([("line0", col("pos_end_line")), ("col", col("pos_end_col"))]),
                ),
                ("col_unit", col("col_unit")),
                ("end_exclusive", col("end_exclusive")),
            ]
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
}

VIEW_BASE_TABLE: Final[dict[str, str]] = {
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
    "scip_diagnostics": "scip_diagnostics_v1",
    "scip_document_symbols": "scip_document_symbols_v1",
    "scip_document_texts": "scip_document_texts_v1",
    "scip_documents": "scip_documents_v1",
    "scip_external_symbol_information": "scip_external_symbol_information_v1",
    "scip_index_stats": "scip_index_stats_v1",
    "scip_metadata": "scip_metadata_v1",
    "scip_occurrences": "scip_occurrences_v1",
    "scip_signature_occurrences": "scip_signature_occurrences_v1",
    "scip_symbol_information": "scip_symbol_information_v1",
    "scip_symbol_relationships": "scip_symbol_relationships_v1",
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
