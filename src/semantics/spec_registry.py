"""Canonical semantic table specs for ambiguous schemas."""

from __future__ import annotations

from typing import Final

from semantics.specs import ForeignKeyDerivation, IdDerivation, SemanticTableSpec, SpanBinding

SEMANTIC_TABLE_SPECS: Final[dict[str, SemanticTableSpec]] = {
    "cst_defs": SemanticTableSpec(
        table="cst_defs",
        primary_span=SpanBinding("def_bstart", "def_bend"),
        entity_id=IdDerivation(
            out_col="def_id",
            namespace="cst_def",
            start_col="def_bstart",
            end_col="def_bend",
        ),
        foreign_keys=(
            ForeignKeyDerivation(
                out_col="container_def_id",
                target_namespace="cst_def",
                start_col="container_def_bstart",
                end_col="container_def_bend",
                guard_null_if=("container_def_kind",),
            ),
        ),
    ),
    "cst_imports": SemanticTableSpec(
        table="cst_imports",
        primary_span=SpanBinding("alias_bstart", "alias_bend"),
        entity_id=IdDerivation(
            out_col="import_id",
            namespace="cst_import",
            start_col="alias_bstart",
            end_col="alias_bend",
        ),
    ),
    "cst_callsites": SemanticTableSpec(
        table="cst_callsites",
        primary_span=SpanBinding("call_bstart", "call_bend"),
        entity_id=IdDerivation(
            out_col="call_id",
            namespace="cst_call",
            start_col="call_bstart",
            end_col="call_bend",
        ),
    ),
    "cst_docstrings": SemanticTableSpec(
        table="cst_docstrings",
        primary_span=SpanBinding("bstart", "bend"),
        entity_id=IdDerivation(
            out_col="docstring_id",
            namespace="cst_docstring",
            start_col="bstart",
            end_col="bend",
        ),
        foreign_keys=(
            ForeignKeyDerivation(
                out_col="owner_def_id",
                target_namespace="cst_def",
                start_col="owner_def_bstart",
                end_col="owner_def_bend",
            ),
        ),
        text_cols=("docstring",),
    ),
    "cst_decorators": SemanticTableSpec(
        table="cst_decorators",
        primary_span=SpanBinding("bstart", "bend"),
        entity_id=IdDerivation(
            out_col="decorator_id",
            namespace="cst_decorator",
            start_col="bstart",
            end_col="bend",
        ),
        foreign_keys=(
            ForeignKeyDerivation(
                out_col="owner_def_id",
                target_namespace="cst_def",
                start_col="owner_def_bstart",
                end_col="owner_def_bend",
            ),
        ),
        text_cols=("decorator_text",),
    ),
}


def spec_for_table(table: str) -> SemanticTableSpec | None:
    """Return a semantic spec for the table name when registered."""
    return SEMANTIC_TABLE_SPECS.get(table)


__all__ = ["SEMANTIC_TABLE_SPECS", "spec_for_table"]
