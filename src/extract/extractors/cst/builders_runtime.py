"""Runtime extraction/orchestration helpers for CST builders."""

from __future__ import annotations

import contextlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, overload

import libcst as cst
from libcst import helpers
from libcst.metadata import (
    BaseMetadataProvider,
    FullRepoManager,
    FullyQualifiedNameProvider,
    TypeInferenceProvider,
)

from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.extract.registry import normalize_options
from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
from datafusion_engine.schema import default_attrs_value
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from extract.coordination.context import (
    ExtractExecutionContext,
    FileContext,
    attrs_map,
    bytes_from_file_ctx,
    file_identity_row,
)
from extract.coordination.materialization import (
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    extract_plan_from_row_batches,
    extract_plan_from_rows,
    materialize_extract_plan,
)
from extract.coordination.schema_ops import ExtractNormalizeOptions
from extract.extractors.cst.builders import (
    CollectorContext,
    CollectorMetadata,
    DefKey,
    _callee_shape,
    _collect_cst_nodes_edges,
    _iter_params,
    _manifest_row,
    _matcher_counts,
    _module_package_names,
    _parse_or_record_error,
    _repo_relative_path,
    _row_map,
    _should_collect,
    _visit_with_collector,
)
from extract.extractors.cst.setup import CstExtractOptions, _qname_keys
from extract.extractors.cst.visitors import (
    CSTExtractContext,
    CSTFileContext,
    TypeExprOwner,
)
from extract.infrastructure.parallel import parallel_map, resolve_max_workers, supports_fork
from extract.infrastructure.result_types import ExtractResult
from extract.infrastructure.string_utils import normalize_string_items
from extract.infrastructure.worklists import (
    WorklistRequest,
    iter_worklist_contexts,
    worklist_queue_name,
)
from obs.otel import SCOPE_EXTRACT, stage_span

if TYPE_CHECKING:
    from extract.coordination.evidence_plan import EvidencePlan
    from extract.scanning.scope_manifest import ScopeManifest
    from extract.session import ExtractSession


class CSTCollector(cst.CSTVisitor):
    """Collect CST-derived rows for a single file."""

    def __init__(
        self,
        *,
        ctx: CollectorContext,
        meta: CollectorMetadata,
    ) -> None:
        """Initialize the instance.

        Args:
            ctx: Description.
            meta: Description.
        """
        self._module = ctx.module
        self._file_ctx = ctx.file_ctx
        self._options = ctx.extract_ctx.options
        self._identity = file_identity_row(self._file_ctx.file_ctx)
        self._span_map = meta.span_map
        self._ctx_map = meta.ctx_map
        self._scope_map = meta.scope_map
        self._parent_map = meta.parent_map
        self._qn_map = meta.qn_map
        self._fqn_map = meta.fqn_map
        self._type_map = meta.type_map
        self._ref_rows = ctx.extract_ctx.ref_rows
        self._import_rows = ctx.extract_ctx.import_rows
        self._call_rows = ctx.extract_ctx.call_rows
        self._def_rows = ctx.extract_ctx.def_rows
        self._type_expr_rows = ctx.extract_ctx.type_expr_rows
        self._docstring_rows = ctx.extract_ctx.docstring_rows
        self._decorator_rows = ctx.extract_ctx.decorator_rows
        self._call_arg_rows = ctx.extract_ctx.call_arg_rows
        self._skip_name_nodes: set[cst.Name] = set()
        self._def_stack: list[DefKey] = []

    def _span(self, node: cst.CSTNode) -> tuple[int, int]:
        sp = self._span_map.get(node)
        if sp is None:
            return 0, 0
        start = int(getattr(sp, "start", 0))
        length = int(getattr(sp, "length", 0))
        return start, start + length

    def _code_for_node(self, node: cst.CSTNode) -> str | None:
        try:
            return self._module.code_for_node(node)
        except (AttributeError, ValueError):
            return None

    def _qnames(self, node: cst.CSTNode) -> list[dict[str, str]]:
        qset = self._qn_map.get(node)
        if not qset:
            return []
        if callable(qset):
            qset = qset()
        qualified = sorted(qset, key=lambda q: (q.name, str(q.source)))
        keys = _qname_keys()
        return [dict(zip(keys, (q.name, str(q.source)), strict=True)) for q in qualified]

    def _fqns(self, node: cst.CSTNode) -> list[str]:
        qset = self._fqn_map.get(node)
        if not qset:
            return []
        if callable(qset):
            qset = qset()
        return sorted({q.name for q in qset})

    def _scope_details(self, node: cst.CSTNode) -> tuple[str | None, str | None, str | None]:
        scope = self._scope_map.get(node)
        if scope is None:
            return None, None, None
        scope_type = getattr(scope, "scope_type", None) or type(scope).__name__
        scope_name = getattr(scope, "name", None)
        scope_role = getattr(scope, "role", None)
        return (
            str(scope_type) if scope_type is not None else None,
            str(scope_name) if scope_name is not None else None,
            str(scope_role) if scope_role is not None else None,
        )

    def _parent_kind(self, node: cst.CSTNode) -> str | None:
        parent = self._parent_map.get(node)
        if parent is None:
            return None
        return type(parent).__name__

    def _type_for_node(self, node: cst.CSTNode) -> str | None:
        value = self._type_map.get(node)
        if value is None:
            return None
        inferred = getattr(value, "inferred_type", None)
        if inferred is not None:
            return str(inferred)
        return str(value)

    @staticmethod
    def _docstring_node(
        body: Sequence[cst.BaseStatement | cst.BaseSmallStatement],
    ) -> cst.CSTNode | None:
        if not body:
            return None
        first = body[0]
        if isinstance(first, cst.SimpleStatementLine) and len(first.body) == 1:
            expr = first.body[0]
            if isinstance(expr, cst.Expr) and isinstance(expr.value, cst.BaseString):
                return expr.value
        return None

    def _record_docstring(
        self,
        *,
        owner_kind: str,
        owner_def_bstart: int | None,
        owner_def_bend: int | None,
        node: cst.Module | cst.ClassDef | cst.FunctionDef,
    ) -> None:
        if not self._options.include_docstrings:
            return
        docstring = node.get_docstring()
        if not docstring:
            return
        doc_node: cst.CSTNode | None = None
        if isinstance(node, cst.Module):
            doc_node = self._docstring_node(node.body)
        elif isinstance(node, (cst.ClassDef, cst.FunctionDef)):
            doc_node = self._docstring_node(node.body.body)
        bstart: int | None = None
        bend: int | None = None
        if doc_node is not None:
            bstart, bend = self._span(doc_node)
        self._docstring_rows.append(
            {
                **self._identity,
                "owner_def_id": None,
                "owner_kind": owner_kind,
                "owner_def_bstart": owner_def_bstart,
                "owner_def_bend": owner_def_bend,
                "docstring": docstring,
                "bstart": bstart,
                "bend": bend,
            }
        )

    def _record_decorators(
        self,
        *,
        owner_kind: str,
        owner_def_bstart: int | None,
        owner_def_bend: int | None,
        decorators: Sequence[cst.Decorator],
    ) -> None:
        if not self._options.include_decorators:
            return
        for idx, decorator in enumerate(decorators):
            bstart, bend = self._span(decorator)
            decorator_text = self._code_for_node(decorator.decorator)
            self._decorator_rows.append(
                {
                    **self._identity,
                    "owner_def_id": None,
                    "owner_kind": owner_kind,
                    "owner_def_bstart": owner_def_bstart,
                    "owner_def_bend": owner_def_bend,
                    "decorator_text": decorator_text,
                    "decorator_index": idx,
                    "bstart": bstart,
                    "bend": bend,
                }
            )

    def _record_type_expr(
        self,
        annotation: cst.Annotation,
        *,
        owner: TypeExprOwner,
    ) -> None:
        if not self._options.include_type_exprs:
            return
        expr = annotation.annotation
        bstart, bend = self._span(expr)
        expr_text = self._code_for_node(expr)
        if expr_text is None:
            return
        self._type_expr_rows.append(
            {
                **self._identity,
                "owner_def_kind": owner.owner_def_kind,
                "owner_def_bstart": owner.owner_def_bstart,
                "owner_def_bend": owner.owner_def_bend,
                "param_name": owner.param_name,
                "expr_kind": "annotation",
                "expr_role": owner.expr_role,
                "bstart": bstart,
                "bend": bend,
                "expr_text": expr_text,
            }
        )

    def visit_Module(self, node: cst.Module) -> bool | None:
        """Collect module docstrings.

        Returns:
        -------
        bool | None
            True to continue CST traversal.
        """
        self._record_docstring(
            owner_kind="module",
            owner_def_bstart=None,
            owner_def_bend=None,
            node=node,
        )
        return True

    def visit_FunctionDef(self, node: cst.FunctionDef) -> bool | None:
        """Collect function definition rows.

        Returns:
        -------
        bool | None
            True to continue CST traversal.
        """
        if not (
            self._options.include_defs
            or self._options.include_docstrings
            or self._options.include_decorators
            or self._options.include_type_exprs
        ):
            return True
        def_bstart, def_bend = self._span(node)
        name_bstart, name_bend = self._span(node.name)
        self._skip_name_nodes.add(node.name)

        container = self._def_stack[-1] if self._def_stack else None
        container_kind: str | None = None
        container_bstart: int | None = None
        container_bend: int | None = None
        if container is not None:
            container_kind, container_bstart, container_bend = container
        def_kind = "async_function" if node.asynchronous is not None else "function"
        def_key: DefKey = (def_kind, def_bstart, def_bend)
        qnames = self._qnames(node)
        def_fqns = self._fqns(node)
        def_id = None
        docstring = node.get_docstring()
        decorator_count = len(node.decorators)

        if self._options.include_defs:
            self._def_rows.append(
                {
                    **self._identity,
                    "def_id": def_id,
                    "container_def_kind": container_kind,
                    "container_def_bstart": container_bstart,
                    "container_def_bend": container_bend,
                    "kind": def_kind,
                    "name": node.name.value,
                    "def_bstart": def_bstart,
                    "def_bend": def_bend,
                    "name_bstart": name_bstart,
                    "name_bend": name_bend,
                    "qnames": qnames,
                    "def_fqns": def_fqns,
                    "docstring": docstring,
                    "decorator_count": decorator_count,
                    "attrs": attrs_map(default_attrs_value()),
                }
            )
        self._record_docstring(
            owner_kind=def_kind,
            owner_def_bstart=def_bstart,
            owner_def_bend=def_bend,
            node=node,
        )
        self._record_decorators(
            owner_kind=def_kind,
            owner_def_bstart=def_bstart,
            owner_def_bend=def_bend,
            decorators=node.decorators,
        )
        if self._options.include_type_exprs:
            if node.returns is not None:
                self._record_type_expr(
                    node.returns,
                    owner=TypeExprOwner(
                        owner_def_kind=def_kind,
                        owner_def_bstart=def_bstart,
                        owner_def_bend=def_bend,
                        expr_role="return",
                    ),
                )
            for param in _iter_params(node.params):
                if param.annotation is None:
                    continue
                self._record_type_expr(
                    param.annotation,
                    owner=TypeExprOwner(
                        owner_def_kind=def_kind,
                        owner_def_bstart=def_bstart,
                        owner_def_bend=def_bend,
                        expr_role="param",
                        param_name=param.name.value,
                    ),
                )
        if self._options.include_defs:
            self._def_stack.append(def_key)
        return True

    def leave_FunctionDef(self, original_node: cst.FunctionDef) -> None:
        """Finalize function definition collection."""
        _ = original_node
        if self._options.include_defs and self._def_stack:
            self._def_stack.pop()

    def visit_ClassDef(self, node: cst.ClassDef) -> bool | None:
        """Collect class definition rows.

        Returns:
        -------
        bool | None
            True to continue CST traversal.
        """
        if not (
            self._options.include_defs
            or self._options.include_docstrings
            or self._options.include_decorators
        ):
            return True
        def_bstart, def_bend = self._span(node)
        name_bstart, name_bend = self._span(node.name)
        self._skip_name_nodes.add(node.name)

        container = self._def_stack[-1] if self._def_stack else None
        container_kind: str | None = None
        container_bstart: int | None = None
        container_bend: int | None = None
        if container is not None:
            container_kind, container_bstart, container_bend = container
        def_kind = "class"
        def_key: DefKey = (def_kind, def_bstart, def_bend)
        qnames = self._qnames(node)
        def_fqns = self._fqns(node)
        def_id = None
        docstring = node.get_docstring()
        decorator_count = len(node.decorators)

        if self._options.include_defs:
            self._def_rows.append(
                {
                    **self._identity,
                    "def_id": def_id,
                    "container_def_kind": container_kind,
                    "container_def_bstart": container_bstart,
                    "container_def_bend": container_bend,
                    "kind": def_kind,
                    "name": node.name.value,
                    "def_bstart": def_bstart,
                    "def_bend": def_bend,
                    "name_bstart": name_bstart,
                    "name_bend": name_bend,
                    "qnames": qnames,
                    "def_fqns": def_fqns,
                    "docstring": docstring,
                    "decorator_count": decorator_count,
                    "attrs": attrs_map(default_attrs_value()),
                }
            )
        self._record_docstring(
            owner_kind=def_kind,
            owner_def_bstart=def_bstart,
            owner_def_bend=def_bend,
            node=node,
        )
        self._record_decorators(
            owner_kind=def_kind,
            owner_def_bstart=def_bstart,
            owner_def_bend=def_bend,
            decorators=node.decorators,
        )
        if self._options.include_defs:
            self._def_stack.append(def_key)
        return True

    def leave_ClassDef(self, original_node: cst.ClassDef) -> None:
        """Finalize class definition collection."""
        _ = original_node
        if self._options.include_defs and self._def_stack:
            self._def_stack.pop()

    def visit_Name(self, node: cst.Name) -> bool | None:
        """Collect name reference rows.

        Returns:
        -------
        bool | None
            True to continue CST traversal.
        """
        if not self._options.include_refs:
            return True
        if node in self._skip_name_nodes:
            return True
        parent = self._parent_map.get(node)
        if isinstance(parent, cst.Attribute):
            return True
        bstart, bend = self._span(node)
        expr_ctx = None
        if self._options.compute_expr_context:
            ctx = self._ctx_map.get(node)
            expr_ctx = str(ctx) if ctx is not None else None
        scope_type, scope_name, scope_role = self._scope_details(node)
        parent_kind = self._parent_kind(node)
        inferred_type = self._type_for_node(node)
        self._ref_rows.append(
            {
                **self._identity,
                "ref_id": None,
                "ref_kind": "name",
                "ref_text": node.value,
                "expr_ctx": expr_ctx,
                "scope_type": scope_type,
                "scope_name": scope_name,
                "scope_role": scope_role,
                "parent_kind": parent_kind,
                "inferred_type": inferred_type,
                "bstart": bstart,
                "bend": bend,
                "attrs": attrs_map(default_attrs_value()),
            }
        )
        return True

    def visit_Attribute(self, node: cst.Attribute) -> bool | None:
        """Collect attribute reference rows.

        Returns:
        -------
        bool | None
            True to continue CST traversal.
        """
        if not self._options.include_refs:
            return True
        bstart, bend = self._span(node)
        expr_ctx = None
        if self._options.compute_expr_context:
            ctx = self._ctx_map.get(node)
            expr_ctx = str(ctx) if ctx is not None else None
        ref_text = helpers.get_full_name_for_node(node) or self._code_for_node(node)
        if ref_text is None:
            return True
        scope_type, scope_name, scope_role = self._scope_details(node)
        parent_kind = self._parent_kind(node)
        inferred_type = self._type_for_node(node)
        self._ref_rows.append(
            {
                **self._identity,
                "ref_id": None,
                "ref_kind": "attribute",
                "ref_text": ref_text,
                "expr_ctx": expr_ctx,
                "scope_type": scope_type,
                "scope_name": scope_name,
                "scope_role": scope_role,
                "parent_kind": parent_kind,
                "inferred_type": inferred_type,
                "bstart": bstart,
                "bend": bend,
                "attrs": attrs_map(default_attrs_value()),
            }
        )
        return True

    def visit_Import(self, node: cst.Import) -> bool | None:
        """Collect import rows.

        Returns:
        -------
        bool | None
            True to continue CST traversal.
        """
        if not self._options.include_imports:
            return True
        stmt_bstart, stmt_bend = self._span(node)
        for alias in node.names:
            alias_bstart, alias_bend = self._span(alias)
            name = helpers.get_full_name_for_node(alias.name) or getattr(alias.name, "value", None)

            asname: str | None = None
            if isinstance(alias.asname, cst.AsName):
                asname = helpers.get_full_name_for_node(alias.asname.name) or getattr(
                    alias.asname.name, "value", None
                )

            self._import_rows.append(
                {
                    **self._identity,
                    "kind": "import",
                    "module": None,
                    "relative_level": 0,
                    "name": name,
                    "asname": asname,
                    "is_star": False,
                    "stmt_bstart": stmt_bstart,
                    "stmt_bend": stmt_bend,
                    "alias_bstart": alias_bstart,
                    "alias_bend": alias_bend,
                    "attrs": attrs_map(default_attrs_value()),
                }
            )
        return True

    def visit_ImportFrom(self, node: cst.ImportFrom) -> bool | None:
        """Collect from-import rows.

        Returns:
        -------
        bool | None
            True to continue CST traversal.
        """
        if not self._options.include_imports:
            return True
        stmt_bstart, stmt_bend = self._span(node)

        module = None
        if node.module is not None:
            module = helpers.get_full_name_for_node(node.module) or getattr(
                node.module, "value", None
            )

        relative = node.relative
        relative_level = len(relative) if isinstance(relative, Sequence) else 0

        if isinstance(node.names, cst.ImportStar):
            alias_bstart, alias_bend = self._span(node.names)
            self._import_rows.append(
                {
                    **self._identity,
                    "kind": "importfrom",
                    "module": module,
                    "relative_level": relative_level,
                    "name": "*",
                    "asname": None,
                    "is_star": True,
                    "stmt_bstart": stmt_bstart,
                    "stmt_bend": stmt_bend,
                    "alias_bstart": alias_bstart,
                    "alias_bend": alias_bend,
                    "attrs": attrs_map(default_attrs_value()),
                }
            )
            return True

        for alias in node.names:
            alias_bstart, alias_bend = self._span(alias)
            name = helpers.get_full_name_for_node(alias.name) or getattr(alias.name, "value", None)

            asname: str | None = None
            if isinstance(alias.asname, cst.AsName):
                asname = helpers.get_full_name_for_node(alias.asname.name) or getattr(
                    alias.asname.name, "value", None
                )

            self._import_rows.append(
                {
                    **self._identity,
                    "kind": "importfrom",
                    "module": module,
                    "relative_level": relative_level,
                    "name": name,
                    "asname": asname,
                    "is_star": False,
                    "stmt_bstart": stmt_bstart,
                    "stmt_bend": stmt_bend,
                    "alias_bstart": alias_bstart,
                    "alias_bend": alias_bend,
                    "attrs": attrs_map(default_attrs_value()),
                }
            )
        return True

    def visit_Call(self, node: cst.Call) -> bool | None:
        """Collect callsite rows.

        Returns:
        -------
        bool | None
            True to continue CST traversal.
        """
        if not self._options.include_callsites:
            return True
        call_bstart, call_bend, row = self._callsite_row(node)
        self._call_rows.append(row)
        if self._options.include_call_args:
            self._append_call_args(call_bstart, call_bend, node)
        return True

    def _callsite_row(self, node: cst.Call) -> tuple[int | None, int | None, dict[str, object]]:
        call_bstart, call_bend = self._span(node)
        callee_bstart, callee_bend = self._span(node.func)
        callee_dotted = helpers.get_full_name_for_node(node.func)
        callee_shape = _callee_shape(node.func)
        callee_text = self._code_for_node(node.func)
        arg_count = len(node.args)
        qnames = self._qnames(node.func) or self._qnames(node)
        callee_fqns = self._fqns(node.func) or self._fqns(node)
        inferred_type = self._type_for_node(node)
        row: dict[str, object] = {
            **self._identity,
            "call_id": None,
            "call_bstart": call_bstart,
            "call_bend": call_bend,
            "callee_bstart": callee_bstart,
            "callee_bend": callee_bend,
            "callee_shape": callee_shape,
            "callee_text": callee_text,
            "arg_count": int(arg_count),
            "callee_dotted": callee_dotted,
            "callee_qnames": qnames,
            "callee_fqns": callee_fqns,
            "inferred_type": inferred_type,
            "attrs": attrs_map(default_attrs_value()),
        }
        return call_bstart, call_bend, row

    def _append_call_args(
        self,
        call_bstart: int | None,
        call_bend: int | None,
        node: cst.Call,
    ) -> None:
        for idx, arg in enumerate(node.args):
            arg_text = self._code_for_node(arg.value)
            bstart, bend = self._span(arg.value)
            keyword = arg.keyword.value if arg.keyword is not None else None
            star = arg.star
            self._call_arg_rows.append(
                {
                    **self._identity,
                    "call_id": None,
                    "call_bstart": call_bstart,
                    "call_bend": call_bend,
                    "arg_index": idx,
                    "keyword": keyword,
                    "star": star,
                    "arg_text": arg_text,
                    "bstart": bstart,
                    "bend": bend,
                }
            )


def _extract_cst_for_context(
    file_ctx: FileContext,
    ctx: CSTExtractContext,
) -> cst.Module | None:
    if not file_ctx.file_id or not file_ctx.path:
        return None

    data = bytes_from_file_ctx(file_ctx)
    if data is None:
        return None

    module = _parse_or_record_error(
        data,
        file_ctx=file_ctx,
        ctx=ctx,
    )
    if module is None:
        return None

    module_name, package_name = _module_package_names(
        abs_path=file_ctx.abs_path,
        options=ctx.options,
        path=file_ctx.path,
    )
    cst_file_ctx = CSTFileContext(file_ctx=file_ctx, options=ctx.options)
    if ctx.options.include_parse_manifest:
        future_imports = getattr(module, "future_imports", []) or []
        normalized_future_imports = normalize_string_items(future_imports)
        ctx.manifest_rows.append(
            _manifest_row(
                cst_file_ctx,
                module,
                module_name=module_name,
                package_name=package_name,
                future_imports=normalized_future_imports,
            )
        )

    if _should_collect(ctx.options):
        _visit_with_collector(module, file_ctx=cst_file_ctx, ctx=ctx)
    return module


def _build_repo_manager(
    options: CstExtractOptions,
    file_contexts: Sequence[FileContext],
) -> FullRepoManager | None:
    if options.repo_root is None:
        return None
    if not (options.compute_fully_qualified_names or options.compute_type_inference):
        return None
    providers: set[type[BaseMetadataProvider[object]]] = set()
    if options.compute_fully_qualified_names:
        providers.add(FullyQualifiedNameProvider)
    if options.compute_type_inference:
        providers.add(TypeInferenceProvider)
    if not providers:
        return None
    rel_paths: set[Path] = set()
    for ctx in file_contexts:
        rel_path = _repo_relative_path(ctx, options.repo_root)
        if rel_path is not None:
            rel_paths.add(rel_path)
    if not rel_paths:
        return None
    rel_paths_str = sorted(str(path) for path in rel_paths)
    manager = FullRepoManager(
        options.repo_root,
        rel_paths_str,
        providers,
        use_pyproject_toml=True,
    )
    manager.resolve_cache()
    return manager


_CST_WORKER_STATE: dict[str, FullRepoManager | None] = {"repo_manager": None}


def _set_worker_repo_manager(repo_manager: FullRepoManager | None) -> None:
    _CST_WORKER_STATE["repo_manager"] = repo_manager


def _resolve_worker_repo_manager(repo_manager: FullRepoManager | None) -> FullRepoManager | None:
    if repo_manager is not None:
        return repo_manager
    return _CST_WORKER_STATE["repo_manager"]


def _warm_cst_parser() -> None:
    with contextlib.suppress(cst.ParserSyntaxError):
        cst.parse_module(b"pass\n")


def _cst_row_worker(
    file_ctx: FileContext,
    *,
    options: CstExtractOptions,
    evidence_plan: EvidencePlan | None,
) -> dict[str, object] | None:
    repo_manager = _resolve_worker_repo_manager(None)
    return _cst_file_row(
        file_ctx,
        options=options,
        evidence_plan=evidence_plan,
        repo_manager=repo_manager,
    )


def _cst_file_row(
    file_ctx: FileContext,
    *,
    options: CstExtractOptions,
    evidence_plan: EvidencePlan | None,
    repo_manager: FullRepoManager | None,
) -> dict[str, object] | None:
    if not file_ctx.file_id or not file_ctx.path:
        return None
    ctx = CSTExtractContext.build(
        options,
        evidence_plan=evidence_plan,
        repo_manager=repo_manager,
    )
    module = _extract_cst_for_context(file_ctx, ctx)
    if module is None and not ctx.error_rows:
        return None
    nodes: list[dict[str, object]] = []
    edges: list[dict[str, object]] = []
    if module is not None:
        nodes, edges = _collect_cst_nodes_edges(module)
    matcher_counts = _matcher_counts(module, options) if module is not None else {}
    return {
        "repo": options.repo_id,
        "path": file_ctx.path,
        "file_id": file_ctx.file_id,
        "file_sha256": file_ctx.file_sha256,
        "nodes": nodes,
        "edges": edges,
        "parse_manifest": [_row_map(row) for row in ctx.manifest_rows],
        "parse_errors": [_row_map(row) for row in ctx.error_rows],
        "refs": [_row_map(row) for row in ctx.ref_rows],
        "imports": [_row_map(row) for row in ctx.import_rows],
        "callsites": [_row_map(row) for row in ctx.call_rows],
        "defs": [_row_map(row) for row in ctx.def_rows],
        "type_exprs": [_row_map(row) for row in ctx.type_expr_rows],
        "docstrings": [_row_map(row) for row in ctx.docstring_rows],
        "decorators": [_row_map(row) for row in ctx.decorator_rows],
        "call_args": [_row_map(row) for row in ctx.call_arg_rows],
        "attrs": attrs_map(matcher_counts),
    }


@dataclass(frozen=True)
class _CstRowRequest:
    repo_files: TableLike
    file_contexts: Iterable[FileContext] | None
    scope_manifest: ScopeManifest | None
    options: CstExtractOptions
    evidence_plan: EvidencePlan | None
    runtime_profile: DataFusionRuntimeProfile | None


def _collect_cst_file_rows(request: _CstRowRequest) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    contexts = list(
        iter_worklist_contexts(
            WorklistRequest(
                repo_files=request.repo_files,
                output_table="libcst_files_v1",
                runtime_profile=request.runtime_profile,
                file_contexts=request.file_contexts,
                queue_name=(
                    worklist_queue_name(
                        output_table="libcst_files_v1", repo_id=request.options.repo_id
                    )
                    if request.options.use_worklist_queue
                    else None
                ),
                scope_manifest=request.scope_manifest,
            )
        )
    )
    repo_manager = _build_repo_manager(request.options, contexts)
    if not contexts:
        return rows
    use_parallel = request.options.parallel
    if repo_manager is not None and not supports_fork():
        use_parallel = False
    if use_parallel:
        _warm_cst_parser()
        _set_worker_repo_manager(repo_manager)
        runner = partial(
            _cst_row_worker,
            options=request.options,
            evidence_plan=request.evidence_plan,
        )
        try:
            rows.extend(
                row
                for row in parallel_map(
                    contexts,
                    runner,
                    max_workers=resolve_max_workers(request.options.max_workers, kind="cpu"),
                )
                if row is not None
            )
        finally:
            _set_worker_repo_manager(None)
        return rows
    for file_ctx in contexts:
        row = _cst_file_row(
            file_ctx,
            options=request.options,
            evidence_plan=request.evidence_plan,
            repo_manager=repo_manager,
        )
        if row is not None:
            rows.append(row)
    return rows


def _build_cst_file_plan(
    rows: list[dict[str, object]] | None,
    *,
    row_batches: Iterable[Sequence[Mapping[str, object]]] | None,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None,
    session: ExtractSession,
) -> DataFusionPlanArtifact:
    plan_options = ExtractPlanOptions(
        normalize=normalize,
        evidence_plan=evidence_plan,
    )
    if row_batches is not None:
        return extract_plan_from_row_batches(
            "libcst_files_v1",
            row_batches,
            session=session,
            options=plan_options,
        )
    return extract_plan_from_rows(
        "libcst_files_v1",
        rows or [],
        session=session,
        options=plan_options,
    )


@dataclass(frozen=True)
class _CstBatchContext:
    repo_files: TableLike
    file_contexts: Iterable[FileContext] | None
    scope_manifest: ScopeManifest | None
    options: CstExtractOptions
    evidence_plan: EvidencePlan | None
    runtime_profile: DataFusionRuntimeProfile | None
    batch_size: int


def _iter_cst_row_batches(
    context: _CstBatchContext,
) -> Iterable[Sequence[Mapping[str, object]]]:
    contexts = list(
        iter_worklist_contexts(
            WorklistRequest(
                repo_files=context.repo_files,
                output_table="libcst_files_v1",
                runtime_profile=context.runtime_profile,
                file_contexts=context.file_contexts,
                queue_name=(
                    worklist_queue_name(
                        output_table="libcst_files_v1",
                        repo_id=context.options.repo_id,
                    )
                    if context.options.use_worklist_queue
                    else None
                ),
                scope_manifest=context.scope_manifest,
            )
        )
    )
    if not contexts:
        return
    repo_manager = _build_repo_manager(context.options, contexts)
    yield from _iter_cst_batches_for_contexts(
        contexts,
        repo_manager=repo_manager,
        context=context,
    )


def _iter_cst_batches_for_contexts(
    contexts: Sequence[FileContext],
    *,
    repo_manager: FullRepoManager | None,
    context: _CstBatchContext,
) -> Iterable[Sequence[Mapping[str, object]]]:
    batch: list[dict[str, object]] = []
    options = context.options
    evidence_plan = context.evidence_plan
    batch_size = context.batch_size
    use_parallel = options.parallel and (repo_manager is None or supports_fork())
    if use_parallel:
        _warm_cst_parser()
        _set_worker_repo_manager(repo_manager)
        runner = partial(_cst_row_worker, options=options, evidence_plan=evidence_plan)
        try:
            for row in parallel_map(
                contexts,
                runner,
                max_workers=resolve_max_workers(options.max_workers, kind="cpu"),
            ):
                if row is None:
                    continue
                batch.append(row)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
        finally:
            _set_worker_repo_manager(None)
    else:
        for file_ctx in contexts:
            row = _cst_file_row(
                file_ctx,
                options=options,
                evidence_plan=evidence_plan,
                repo_manager=repo_manager,
            )
            if row is None:
                continue
            batch.append(row)
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if batch:
        yield batch


def _resolve_batch_size(options: CstExtractOptions) -> int | None:
    if options.batch_size is None:
        return None
    if options.batch_size <= 0:
        msg = "batch_size must be a positive integer."
        raise ValueError(msg)
    return options.batch_size


def extract_cst(
    repo_files: TableLike,
    options: CstExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> ExtractResult[TableLike]:
    """Extract LibCST-derived structures from repo files.

    Returns:
    -------
    ExtractResult[TableLike]
        Tables derived from LibCST parsing and metadata providers.
    """
    normalized_options = normalize_options("cst", options, CstExtractOptions)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    determinism_tier = exec_context.determinism_tier()
    normalize = ExtractNormalizeOptions(
        options=normalized_options,
        repo_id=normalized_options.repo_id,
    )
    rows: list[dict[str, object]] | None = None
    row_batches: Iterable[Sequence[Mapping[str, object]]] | None = None
    batch_size = _resolve_batch_size(normalized_options)
    if batch_size is None:
        rows = _collect_cst_file_rows(
            _CstRowRequest(
                repo_files=repo_files,
                file_contexts=exec_context.file_contexts,
                scope_manifest=exec_context.scope_manifest,
                options=normalized_options,
                evidence_plan=exec_context.evidence_plan,
                runtime_profile=runtime_profile,
            )
        )
    else:
        row_batches = _iter_cst_row_batches(
            _CstBatchContext(
                repo_files=repo_files,
                file_contexts=exec_context.file_contexts,
                scope_manifest=exec_context.scope_manifest,
                options=normalized_options,
                evidence_plan=exec_context.evidence_plan,
                runtime_profile=runtime_profile,
                batch_size=batch_size,
            )
        )
    plan = _build_cst_file_plan(
        rows,
        row_batches=row_batches,
        normalize=normalize,
        evidence_plan=exec_context.evidence_plan,
        session=session,
    )
    table = materialize_extract_plan(
        "libcst_files_v1",
        plan,
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        options=ExtractMaterializeOptions(
            normalize=normalize,
            apply_post_kernels=True,
        ),
    )
    return ExtractResult(table=table, extractor_name="cst")


def extract_cst_plans(
    repo_files: TableLike,
    options: CstExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> dict[str, DataFusionPlanArtifact]:
    """Extract CST plans for nested file records.

    Returns:
    -------
    dict[str, DataFusionPlanArtifact]
        Plan bundle keyed by ``libcst_files``.
    """
    normalized_options = normalize_options("cst", options, CstExtractOptions)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    normalize = ExtractNormalizeOptions(
        options=normalized_options,
        repo_id=normalized_options.repo_id,
    )
    rows: list[dict[str, object]] | None = None
    row_batches: Iterable[Sequence[Mapping[str, object]]] | None = None
    batch_size = _resolve_batch_size(normalized_options)
    if batch_size is None:
        rows = _collect_cst_file_rows(
            _CstRowRequest(
                repo_files=repo_files,
                file_contexts=exec_context.file_contexts,
                scope_manifest=exec_context.scope_manifest,
                options=normalized_options,
                evidence_plan=exec_context.evidence_plan,
                runtime_profile=runtime_profile,
            )
        )
    else:
        row_batches = _iter_cst_row_batches(
            _CstBatchContext(
                repo_files=repo_files,
                file_contexts=exec_context.file_contexts,
                scope_manifest=exec_context.scope_manifest,
                options=normalized_options,
                evidence_plan=exec_context.evidence_plan,
                runtime_profile=runtime_profile,
                batch_size=batch_size,
            )
        )
    return {
        "libcst_files": _build_cst_file_plan(
            rows,
            row_batches=row_batches,
            normalize=normalize,
            evidence_plan=exec_context.evidence_plan,
            session=session,
        ),
    }


class _CstTablesKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: CstExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    prefer_reader: bool


class _CstTablesKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: CstExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    prefer_reader: Literal[False]


class _CstTablesKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: CstExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    prefer_reader: Required[Literal[True]]


@overload
def extract_cst_tables(
    **kwargs: Unpack[_CstTablesKwargsTable],
) -> Mapping[str, TableLike]: ...


@overload
def extract_cst_tables(
    **kwargs: Unpack[_CstTablesKwargsReader],
) -> Mapping[str, TableLike | RecordBatchReaderLike]: ...


def extract_cst_tables(
    **kwargs: Unpack[_CstTablesKwargs],
) -> Mapping[str, TableLike | RecordBatchReaderLike]:
    """Extract CST tables as a name-keyed bundle.

    Parameters
    ----------
    kwargs:
        Keyword-only arguments for extraction (repo_files, options, file_contexts, ctx, profile,
        prefer_reader).

    Returns:
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Extracted CST outputs keyed by output name.
    """
    with stage_span(
        "extract.cst_tables",
        stage="extract",
        scope_name=SCOPE_EXTRACT,
        attributes={"codeanatomy.extractor": "cst"},
    ):
        repo_files = kwargs["repo_files"]
        normalized_options = normalize_options("cst", kwargs.get("options"), CstExtractOptions)
        file_contexts = kwargs.get("file_contexts")
        evidence_plan = kwargs.get("evidence_plan")
        profile = kwargs.get("profile", "default")
        prefer_reader = kwargs.get("prefer_reader", False)
        exec_context = ExtractExecutionContext(
            file_contexts=file_contexts,
            evidence_plan=evidence_plan,
            scope_manifest=kwargs.get("scope_manifest"),
            session=kwargs.get("session"),
            profile=profile,
        )
        session = exec_context.ensure_session()
        exec_context = replace(exec_context, session=session)
        runtime_profile = exec_context.ensure_runtime_profile()
        determinism_tier = exec_context.determinism_tier()
        normalize = ExtractNormalizeOptions(
            options=normalized_options,
            repo_id=normalized_options.repo_id,
        )
        options = ExtractMaterializeOptions(
            normalize=normalize,
            prefer_reader=prefer_reader,
            apply_post_kernels=True,
        )
        plans = extract_cst_plans(
            repo_files,
            options=normalized_options,
            context=exec_context,
        )
        return {
            "libcst_files": materialize_extract_plan(
                "libcst_files_v1",
                plans["libcst_files"],
                runtime_profile=runtime_profile,
                determinism_tier=determinism_tier,
                options=options,
            )
        }
