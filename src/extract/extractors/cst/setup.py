"""CST extractor option and metadata setup helpers."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from pathlib import Path

from datafusion_engine.extract.registry import dataset_schema
from datafusion_engine.schema.introspection_core import find_struct_field_keys
from extract.infrastructure.options import ParallelOptions, RepoOptions, WorklistQueueOptions


@dataclass(frozen=True)
class CstExtractOptions(RepoOptions, WorklistQueueOptions, ParallelOptions):
    """Define LibCST extraction options."""

    repo_root: Path | None = None
    include_parse_manifest: bool = True
    include_parse_errors: bool = True
    include_refs: bool = True
    include_imports: bool = True
    include_callsites: bool = True
    include_defs: bool = True
    include_type_exprs: bool = True
    include_docstrings: bool = True
    include_decorators: bool = True
    include_call_args: bool = True
    compute_expr_context: bool = True
    compute_qualified_names: bool = True
    compute_fully_qualified_names: bool = True
    compute_scope: bool = True
    compute_type_inference: bool = False
    matcher_templates: tuple[str, ...] = ()


_QNAME_FIELD_NAMES: tuple[str, ...] = ("qnames", "callee_qnames")
_DEFAULT_QNAME_KEYS: tuple[str, str] = ("name", "source")


@cache
def _qname_keys() -> tuple[str, ...]:
    schema = dataset_schema("libcst_files_v1")
    try:
        return find_struct_field_keys(schema, field_names=_QNAME_FIELD_NAMES)
    except KeyError:
        return _DEFAULT_QNAME_KEYS


__all__ = ["CstExtractOptions"]
