"""Extract LibCST-derived structures into Arrow tables using shared helpers."""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, cast, overload

import libcst as cst
import pyarrow as pa
from libcst import helpers
from libcst.metadata import (
    BaseMetadataProvider,
    ByteSpanPositionProvider,
    ExpressionContextProvider,
    MetadataWrapper,
    QualifiedName,
    QualifiedNameProvider,
)

from arrowdsl.core.execution_context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.schema.structs import flatten_struct_field
from extract.helpers import (
    ExtractExecutionContext,
    ExtractMaterializeOptions,
    FileContext,
    apply_query_and_project,
    bytes_from_file_ctx,
    file_identity_row,
    ibis_plan_from_rows,
    iter_contexts,
    materialize_extract_plan,
)
from extract.registry_fields import QNAME_STRUCT
from extract.registry_specs import dataset_enabled, dataset_row_schema, normalize_options
from extract.schema_ops import ExtractNormalizeOptions
from extract.string_utils import normalize_string_items
from ibis_engine.plan import IbisPlan

if TYPE_CHECKING:
    from extract.evidence_plan import EvidencePlan

type QualifiedNameSet = Collection[QualifiedName] | Callable[[], Collection[QualifiedName]]
type Row = dict[str, object]

CST_LINE_BASE = 1
CST_COL_UNIT = "utf32"
CST_END_EXCLUSIVE = True


@dataclass(frozen=True)
class CSTExtractOptions:
    """Define LibCST extraction options."""

    repo_id: str | None = None
    repo_root: Path | None = None
    include_parse_manifest: bool = True
    include_parse_errors: bool = True
    include_name_refs: bool = True
    include_imports: bool = True
    include_callsites: bool = True
    include_defs: bool = True
    include_type_exprs: bool = True
    compute_expr_context: bool = True
    compute_qualified_names: bool = True


@dataclass(frozen=True)
class CSTExtractResult:
    """Hold extracted CST tables for manifests, errors, names, imports, and calls."""

    py_cst_parse_manifest: TableLike
    py_cst_parse_errors: TableLike
    py_cst_name_refs: TableLike
    py_cst_imports: TableLike
    py_cst_callsites: TableLike
    py_cst_defs: TableLike
    py_cst_type_exprs: TableLike


_QNAME_FLAT_FIELDS = flatten_struct_field(pa.field("qname", QNAME_STRUCT))
QNAME_KEYS = tuple(field.name.split(".", 1)[1] for field in _QNAME_FLAT_FIELDS)

PARSE_MANIFEST_ROW_SCHEMA = dataset_row_schema("py_cst_parse_manifest_v1")
PARSE_ERRORS_ROW_SCHEMA = dataset_row_schema("py_cst_parse_errors_v1")
NAME_REFS_ROW_SCHEMA = dataset_row_schema("py_cst_name_refs_v1")
IMPORTS_ROW_SCHEMA = dataset_row_schema("py_cst_imports_v1")
CALLSITES_ROW_SCHEMA = dataset_row_schema("py_cst_callsites_v1")
DEFS_ROW_SCHEMA = dataset_row_schema("py_cst_defs_v1")
TYPE_EXPRS_ROW_SCHEMA = dataset_row_schema("py_cst_type_exprs_v1")


@dataclass(frozen=True)
class CSTFileContext:
    """Per-file context for CST extraction."""

    file_ctx: FileContext
    options: CSTExtractOptions

    @property
    def file_id(self) -> str:
        """Return the file id for this extraction context.

        Returns
        -------
        str
            File id from the file context.
        """
        return self.file_ctx.file_id

    @property
    def path(self) -> str:
        """Return the file path for this extraction context.

        Returns
        -------
        str
            File path from the file context.
        """
        return self.file_ctx.path

    @property
    def file_sha256(self) -> str | None:
        """Return the file sha256 for this extraction context.

        Returns
        -------
        str | None
            File hash from the file context.
        """
        return self.file_ctx.file_sha256


@dataclass
class CSTExtractContext:
    """Shared extraction context and output buffers."""

    options: CSTExtractOptions
    manifest_rows: list[Row]
    error_rows: list[Row]
    name_ref_rows: list[Row]
    import_rows: list[Row]
    call_rows: list[Row]
    def_rows: list[Row]
    type_expr_rows: list[Row]
    evidence_plan: EvidencePlan | None = None

    @classmethod
    def build(
        cls,
        options: CSTExtractOptions,
        *,
        evidence_plan: EvidencePlan | None = None,
    ) -> CSTExtractContext:
        """Create an empty extraction context.

        Returns
        -------
        CSTExtractContext
            New extraction context with empty buffers.
        """
        return cls(
            options=options,
            manifest_rows=[],
            error_rows=[],
            name_ref_rows=[],
            import_rows=[],
            call_rows=[],
            def_rows=[],
            type_expr_rows=[],
            evidence_plan=evidence_plan,
        )


type DefKey = tuple[str, int, int]


@dataclass(frozen=True)
class TypeExprOwner:
    """Context for a collected type expression."""

    owner_def_kind: str
    owner_def_bstart: int
    owner_def_bend: int
    expr_role: str
    param_name: str | None = None


def _iter_params(params: cst.Parameters) -> list[cst.Param]:
    items: list[cst.Param] = []
    items.extend(params.posonly_params)
    items.extend(params.params)
    items.extend(params.kwonly_params)
    if isinstance(params.star_arg, cst.Param):
        items.append(params.star_arg)
    if isinstance(params.star_kwarg, cst.Param):
        items.append(params.star_kwarg)
    return items


def _callee_shape(node: cst.CSTNode) -> str:
    if isinstance(node, cst.Name):
        return "NAME"
    if isinstance(node, cst.Attribute):
        return "ATTRIBUTE"
    if isinstance(node, cst.Subscript):
        return "SUBSCRIPT"
    return "OTHER"


def _calculate_module_and_package(repo_root: Path, abs_path: Path) -> tuple[str | None, str | None]:
    try:
        result = helpers.calculate_module_and_package(str(repo_root), str(abs_path))
    except (OSError, ValueError):
        return None, None
    else:
        return result.name, result.package


def _parse_module(
    data: bytes,
    *,
    file_ctx: FileContext,
) -> tuple[cst.Module | None, Row | None]:
    try:
        return cst.parse_module(data), None
    except cst.ParserSyntaxError as exc:
        raw_line = int(getattr(exc, "raw_line", 0) or 0)
        raw_column = int(getattr(exc, "raw_column", 0) or 0)
        return (
            None,
            {
                **file_identity_row(file_ctx),
                "error_type": type(exc).__name__,
                "message": str(exc),
                "raw_line": raw_line,
                "raw_column": raw_column,
                "line_base": CST_LINE_BASE,
                "col_unit": CST_COL_UNIT,
                "end_exclusive": CST_END_EXCLUSIVE,
                "meta": {
                    "raw_line": str(raw_line),
                    "raw_column": str(raw_column),
                },
            },
        )


def _module_package_names(
    *,
    abs_path: str | Path | None,
    options: CSTExtractOptions,
    path: str,
) -> tuple[str | None, str | None]:
    if options.repo_root is None:
        return None, None
    if isinstance(abs_path, Path):
        abs_path_value = abs_path
    elif isinstance(abs_path, str):
        abs_path_value = Path(abs_path)
    else:
        abs_path_value = Path(path)
    return _calculate_module_and_package(options.repo_root, abs_path_value)


def _parse_or_record_error(
    data: bytes,
    *,
    file_ctx: FileContext,
    ctx: CSTExtractContext,
) -> cst.Module | None:
    module, err = _parse_module(data, file_ctx=file_ctx)
    if module is None:
        if err is not None and dataset_enabled("py_cst_parse_errors_v1", ctx.options):
            ctx.error_rows.append(err)
        return None
    return module


def _manifest_row(
    ctx: CSTFileContext,
    module: cst.Module,
    *,
    module_name: str | None,
    package_name: str | None,
    future_imports: Sequence[str | None],
) -> Row:
    return {
        **file_identity_row(ctx.file_ctx),
        "encoding": getattr(module, "encoding", None),
        "default_indent": getattr(module, "default_indent", None),
        "default_newline": getattr(module, "default_newline", None),
        "has_trailing_newline": bool(getattr(module, "has_trailing_newline", False)),
        "future_imports": list(future_imports),
        "module_name": module_name,
        "package_name": package_name,
    }


def _resolve_metadata_maps(
    wrapper: MetadataWrapper,
    options: CSTExtractOptions,
) -> tuple[
    Mapping[cst.CSTNode, object],
    Mapping[cst.CSTNode, object],
    Mapping[cst.CSTNode, QualifiedNameSet],
]:
    providers: set[type[BaseMetadataProvider[object]]] = {ByteSpanPositionProvider}
    if options.compute_expr_context and dataset_enabled("py_cst_name_refs_v1", options):
        providers.add(ExpressionContextProvider)
    include_callsites = dataset_enabled("py_cst_callsites_v1", options)
    include_defs = dataset_enabled("py_cst_defs_v1", options)
    if options.compute_qualified_names and (include_callsites or include_defs):
        providers.add(QualifiedNameProvider)

    resolved = wrapper.resolve_many(providers)
    return (
        resolved.get(ByteSpanPositionProvider, {}),
        resolved.get(ExpressionContextProvider, {}),
        cast("Mapping[cst.CSTNode, QualifiedNameSet]", resolved.get(QualifiedNameProvider, {})),
    )


@dataclass(frozen=True)
class CollectorContext:
    """Context required to build a CST collector."""

    module: cst.Module
    file_ctx: CSTFileContext
    extract_ctx: CSTExtractContext


@dataclass(frozen=True)
class CollectorMetadata:
    """Metadata maps needed by the CST collector."""

    span_map: Mapping[cst.CSTNode, object]
    ctx_map: Mapping[cst.CSTNode, object]
    qn_map: Mapping[cst.CSTNode, QualifiedNameSet]


def _visit_with_collector(
    module: cst.Module,
    *,
    file_ctx: CSTFileContext,
    ctx: CSTExtractContext,
) -> None:
    wrapper = MetadataWrapper(module, unsafe_skip_copy=True)
    span_map, ctx_map, qn_map = _resolve_metadata_maps(wrapper, ctx.options)
    collector = CSTCollector(
        ctx=CollectorContext(module=module, file_ctx=file_ctx, extract_ctx=ctx),
        meta=CollectorMetadata(span_map=span_map, ctx_map=ctx_map, qn_map=qn_map),
    )
    wrapper.visit(collector)


class CSTCollector(cst.CSTVisitor):
    """Collect CST-derived rows for a single file."""

    def __init__(
        self,
        *,
        ctx: CollectorContext,
        meta: CollectorMetadata,
    ) -> None:
        self._module = ctx.module
        self._file_ctx = ctx.file_ctx
        self._options = ctx.extract_ctx.options
        self._identity = file_identity_row(self._file_ctx.file_ctx)
        self._span_map = meta.span_map
        self._ctx_map = meta.ctx_map
        self._qn_map = meta.qn_map
        self._name_ref_rows = ctx.extract_ctx.name_ref_rows
        self._import_rows = ctx.extract_ctx.import_rows
        self._call_rows = ctx.extract_ctx.call_rows
        self._def_rows = ctx.extract_ctx.def_rows
        self._type_expr_rows = ctx.extract_ctx.type_expr_rows
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
        return [dict(zip(QNAME_KEYS, (q.name, str(q.source)), strict=True)) for q in qualified]

    def _record_type_expr(
        self,
        annotation: cst.Annotation,
        *,
        owner: TypeExprOwner,
    ) -> None:
        if not dataset_enabled("py_cst_type_exprs_v1", self._options):
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

    def visit_FunctionDef(self, node: cst.FunctionDef) -> bool | None:
        """Collect function definition rows.

        Returns
        -------
        bool | None
            True to continue CST traversal.
        """
        if not dataset_enabled("py_cst_defs_v1", self._options):
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

        self._def_rows.append(
            {
                **self._identity,
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
            }
        )
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
        self._def_stack.append(def_key)
        return True

    def leave_FunctionDef(self, original_node: cst.FunctionDef) -> None:
        """Finalize function definition collection."""
        _ = original_node
        if dataset_enabled("py_cst_defs_v1", self._options) and self._def_stack:
            self._def_stack.pop()

    def visit_ClassDef(self, node: cst.ClassDef) -> bool | None:
        """Collect class definition rows.

        Returns
        -------
        bool | None
            True to continue CST traversal.
        """
        if not dataset_enabled("py_cst_defs_v1", self._options):
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

        self._def_rows.append(
            {
                **self._identity,
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
            }
        )
        self._def_stack.append(def_key)
        return True

    def leave_ClassDef(self, original_node: cst.ClassDef) -> None:
        """Finalize class definition collection."""
        _ = original_node
        if dataset_enabled("py_cst_defs_v1", self._options) and self._def_stack:
            self._def_stack.pop()

    def visit_Name(self, node: cst.Name) -> bool | None:
        """Collect name reference rows.

        Returns
        -------
        bool | None
            True to continue CST traversal.
        """
        if not dataset_enabled("py_cst_name_refs_v1", self._options):
            return True
        if node in self._skip_name_nodes:
            return True
        bstart, bend = self._span(node)
        expr_ctx = None
        if self._options.compute_expr_context:
            ctx = self._ctx_map.get(node)
            expr_ctx = str(ctx) if ctx is not None else None

        self._name_ref_rows.append(
            {
                **self._identity,
                "name": node.value,
                "expr_ctx": expr_ctx,
                "bstart": bstart,
                "bend": bend,
            }
        )
        return True

    def visit_Import(self, node: cst.Import) -> bool | None:
        """Collect import rows.

        Returns
        -------
        bool | None
            True to continue CST traversal.
        """
        if not dataset_enabled("py_cst_imports_v1", self._options):
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
                }
            )
        return True

    def visit_ImportFrom(self, node: cst.ImportFrom) -> bool | None:
        """Collect from-import rows.

        Returns
        -------
        bool | None
            True to continue CST traversal.
        """
        if not dataset_enabled("py_cst_imports_v1", self._options):
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
                }
            )
        return True

    def visit_Call(self, node: cst.Call) -> bool | None:
        """Collect callsite rows.

        Returns
        -------
        bool | None
            True to continue CST traversal.
        """
        if not dataset_enabled("py_cst_callsites_v1", self._options):
            return True
        call_bstart, call_bend = self._span(node)
        callee_bstart, callee_bend = self._span(node.func)

        callee_dotted = helpers.get_full_name_for_node(node.func)
        callee_shape = _callee_shape(node.func)
        callee_text = self._code_for_node(node.func)
        arg_count = len(node.args)

        qnames = self._qnames(node.func)
        if not qnames:
            qnames = self._qnames(node)

        self._call_rows.append(
            {
                **self._identity,
                "call_bstart": call_bstart,
                "call_bend": call_bend,
                "callee_bstart": callee_bstart,
                "callee_bend": callee_bend,
                "callee_shape": callee_shape,
                "callee_text": callee_text,
                "arg_count": int(arg_count),
                "callee_dotted": callee_dotted,
                "callee_qnames": qnames,
            }
        )
        return True


def _extract_cst_for_context(file_ctx: FileContext, ctx: CSTExtractContext) -> None:
    if not file_ctx.file_id or not file_ctx.path:
        return

    data = bytes_from_file_ctx(file_ctx)
    if data is None:
        return

    module = _parse_or_record_error(
        data,
        file_ctx=file_ctx,
        ctx=ctx,
    )
    if module is None:
        return

    module_name, package_name = _module_package_names(
        abs_path=file_ctx.abs_path,
        options=ctx.options,
        path=file_ctx.path,
    )
    cst_file_ctx = CSTFileContext(file_ctx=file_ctx, options=ctx.options)
    if dataset_enabled("py_cst_parse_manifest_v1", ctx.options):
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

    _visit_with_collector(module, file_ctx=cst_file_ctx, ctx=ctx)


def _extract_cst_for_row(rf: dict[str, object], ctx: CSTExtractContext) -> None:
    file_ctx = FileContext.from_repo_row(rf)
    _extract_cst_for_context(file_ctx, ctx)


def extract_cst(
    repo_files: TableLike,
    options: CSTExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> CSTExtractResult:
    """Extract LibCST-derived structures from repo files.

    Returns
    -------
    CSTExtractResult
        Tables derived from LibCST parsing and metadata providers.
    """
    normalized_options = normalize_options("cst", options, CSTExtractOptions)
    exec_context = context or ExtractExecutionContext()
    exec_ctx = exec_context.ensure_ctx()
    extract_ctx = CSTExtractContext.build(
        normalized_options,
        evidence_plan=exec_context.evidence_plan,
    )
    normalize = ExtractNormalizeOptions(
        options=normalized_options,
        repo_id=normalized_options.repo_id,
    )

    for file_ctx in iter_contexts(repo_files, exec_context.file_contexts):
        _extract_cst_for_context(file_ctx, extract_ctx)

    plans = _build_cst_plans(extract_ctx)
    return CSTExtractResult(
        py_cst_parse_manifest=materialize_extract_plan(
            "py_cst_parse_manifest_v1",
            plans["cst_parse_manifest"],
            ctx=exec_ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
        py_cst_parse_errors=materialize_extract_plan(
            "py_cst_parse_errors_v1",
            plans["cst_parse_errors"],
            ctx=exec_ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
        py_cst_name_refs=materialize_extract_plan(
            "py_cst_name_refs_v1",
            plans["cst_name_refs"],
            ctx=exec_ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
        py_cst_imports=materialize_extract_plan(
            "py_cst_imports_v1",
            plans["cst_imports"],
            ctx=exec_ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
        py_cst_callsites=materialize_extract_plan(
            "py_cst_callsites_v1",
            plans["cst_callsites"],
            ctx=exec_ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
        py_cst_defs=materialize_extract_plan(
            "py_cst_defs_v1",
            plans["cst_defs"],
            ctx=exec_ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
        py_cst_type_exprs=materialize_extract_plan(
            "py_cst_type_exprs_v1",
            plans["cst_type_exprs"],
            ctx=exec_ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
    )


def _normalize_ctx(ctx: CSTExtractContext) -> ExtractNormalizeOptions:
    return ExtractNormalizeOptions(options=ctx.options, repo_id=ctx.options.repo_id)


def _build_manifest_plan(ctx: CSTExtractContext) -> IbisPlan:
    raw_plan = ibis_plan_from_rows(
        "py_cst_parse_manifest_v1",
        ctx.manifest_rows,
        row_schema=PARSE_MANIFEST_ROW_SCHEMA,
    )
    return apply_query_and_project(
        "py_cst_parse_manifest_v1",
        raw_plan.expr,
        normalize=_normalize_ctx(ctx),
        evidence_plan=ctx.evidence_plan,
        repo_id=ctx.options.repo_id,
    )


def _build_errors_plan(ctx: CSTExtractContext) -> IbisPlan:
    raw_plan = ibis_plan_from_rows(
        "py_cst_parse_errors_v1",
        ctx.error_rows,
        row_schema=PARSE_ERRORS_ROW_SCHEMA,
    )
    return apply_query_and_project(
        "py_cst_parse_errors_v1",
        raw_plan.expr,
        normalize=_normalize_ctx(ctx),
        evidence_plan=ctx.evidence_plan,
        repo_id=ctx.options.repo_id,
    )


def _build_name_refs_plan(ctx: CSTExtractContext) -> IbisPlan:
    raw_plan = ibis_plan_from_rows(
        "py_cst_name_refs_v1",
        ctx.name_ref_rows,
        row_schema=NAME_REFS_ROW_SCHEMA,
    )
    return apply_query_and_project(
        "py_cst_name_refs_v1",
        raw_plan.expr,
        normalize=_normalize_ctx(ctx),
        evidence_plan=ctx.evidence_plan,
        repo_id=ctx.options.repo_id,
    )


def _build_imports_plan(ctx: CSTExtractContext) -> IbisPlan:
    raw_plan = ibis_plan_from_rows(
        "py_cst_imports_v1",
        ctx.import_rows,
        row_schema=IMPORTS_ROW_SCHEMA,
    )
    return apply_query_and_project(
        "py_cst_imports_v1",
        raw_plan.expr,
        normalize=_normalize_ctx(ctx),
        evidence_plan=ctx.evidence_plan,
        repo_id=ctx.options.repo_id,
    )


def _build_callsites_plan(ctx: CSTExtractContext) -> IbisPlan:
    raw_plan = ibis_plan_from_rows(
        "py_cst_callsites_v1",
        ctx.call_rows,
        row_schema=CALLSITES_ROW_SCHEMA,
    )
    return apply_query_and_project(
        "py_cst_callsites_v1",
        raw_plan.expr,
        normalize=_normalize_ctx(ctx),
        evidence_plan=ctx.evidence_plan,
        repo_id=ctx.options.repo_id,
    )


def _build_defs_plan(ctx: CSTExtractContext) -> IbisPlan:
    raw_plan = ibis_plan_from_rows(
        "py_cst_defs_v1",
        ctx.def_rows,
        row_schema=DEFS_ROW_SCHEMA,
    )
    return apply_query_and_project(
        "py_cst_defs_v1",
        raw_plan.expr,
        normalize=_normalize_ctx(ctx),
        evidence_plan=ctx.evidence_plan,
        repo_id=ctx.options.repo_id,
    )


def _build_type_exprs_plan(ctx: CSTExtractContext) -> IbisPlan:
    raw_plan = ibis_plan_from_rows(
        "py_cst_type_exprs_v1",
        ctx.type_expr_rows,
        row_schema=TYPE_EXPRS_ROW_SCHEMA,
    )
    return apply_query_and_project(
        "py_cst_type_exprs_v1",
        raw_plan.expr,
        normalize=_normalize_ctx(ctx),
        evidence_plan=ctx.evidence_plan,
        repo_id=ctx.options.repo_id,
    )


def _build_cst_plans(ctx: CSTExtractContext) -> dict[str, IbisPlan]:
    return {
        "cst_parse_manifest": _build_manifest_plan(ctx),
        "cst_parse_errors": _build_errors_plan(ctx),
        "cst_name_refs": _build_name_refs_plan(ctx),
        "cst_imports": _build_imports_plan(ctx),
        "cst_callsites": _build_callsites_plan(ctx),
        "cst_defs": _build_defs_plan(ctx),
        "cst_type_exprs": _build_type_exprs_plan(ctx),
    }


class _CstTablesKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: CSTExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
    profile: str
    prefer_reader: bool


class _CstTablesKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: CSTExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
    profile: str
    prefer_reader: Literal[False]


class _CstTablesKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: CSTExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
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

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Extracted CST outputs keyed by output name.
    """
    repo_files = kwargs["repo_files"]
    normalized_options = normalize_options("cst", kwargs.get("options"), CSTExtractOptions)
    file_contexts = kwargs.get("file_contexts")
    evidence_plan = kwargs.get("evidence_plan")
    profile = kwargs.get("profile", "default")
    exec_ctx = kwargs.get("ctx") or execution_context_factory(profile)
    prefer_reader = kwargs.get("prefer_reader", False)
    extract_ctx = CSTExtractContext.build(normalized_options, evidence_plan=evidence_plan)
    normalize = ExtractNormalizeOptions(
        options=normalized_options,
        repo_id=normalized_options.repo_id,
    )
    for file_ctx in iter_contexts(repo_files, file_contexts):
        _extract_cst_for_context(file_ctx, extract_ctx)
    plans = _build_cst_plans(extract_ctx)
    options = ExtractMaterializeOptions(
        normalize=normalize,
        prefer_reader=prefer_reader,
        apply_post_kernels=True,
    )
    return {
        "cst_parse_manifest": materialize_extract_plan(
            "py_cst_parse_manifest_v1",
            plans["cst_parse_manifest"],
            ctx=exec_ctx,
            options=options,
        ),
        "cst_parse_errors": materialize_extract_plan(
            "py_cst_parse_errors_v1",
            plans["cst_parse_errors"],
            ctx=exec_ctx,
            options=options,
        ),
        "cst_name_refs": materialize_extract_plan(
            "py_cst_name_refs_v1",
            plans["cst_name_refs"],
            ctx=exec_ctx,
            options=options,
        ),
        "cst_imports": materialize_extract_plan(
            "py_cst_imports_v1",
            plans["cst_imports"],
            ctx=exec_ctx,
            options=options,
        ),
        "cst_callsites": materialize_extract_plan(
            "py_cst_callsites_v1",
            plans["cst_callsites"],
            ctx=exec_ctx,
            options=options,
        ),
        "cst_defs": materialize_extract_plan(
            "py_cst_defs_v1",
            plans["cst_defs"],
            ctx=exec_ctx,
            options=options,
        ),
        "cst_type_exprs": materialize_extract_plan(
            "py_cst_type_exprs_v1",
            plans["cst_type_exprs"],
            ctx=exec_ctx,
            options=options,
        ),
    }
