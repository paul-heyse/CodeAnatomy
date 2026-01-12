"""Extract LibCST-derived structures into Arrow tables using shared helpers."""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Required, TypedDict, Unpack, cast, overload

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

from arrowdsl.compute.expr_specs import MaskedHashExprSpec
from arrowdsl.core.context import ExecutionContext, OrderingLevel, RuntimeProfile
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import ProjectionSpec, QuerySpec
from arrowdsl.plan.runner import materialize_plan, run_plan_bundle
from arrowdsl.schema.schema import SchemaMetadataSpec, empty_table
from extract.common import bytes_from_file_ctx, file_identity_row, iter_contexts
from extract.file_context import FileContext
from extract.hash_specs import (
    CST_CALL_ID_SPEC,
    CST_CONTAINER_DEF_ID_SPEC,
    CST_DEF_ID_SPEC,
    CST_IMPORT_ID_SPEC,
    CST_NAME_REF_ID_SPEC,
    CST_OWNER_DEF_ID_SPEC,
    CST_TYPE_EXPR_ID_SPEC,
)
from extract.spec_helpers import (
    DatasetRegistration,
    infer_ordering_keys,
    merge_metadata_specs,
    options_metadata_spec,
    ordering_metadata_spec,
    register_dataset,
)
from extract.tables import align_plan, flatten_struct_field, plan_from_rows
from schema_spec.specs import (
    ArrowFieldSpec,
    call_span_bundle,
    file_identity_bundle,
)

SCHEMA_VERSION = 1

type Row = dict[str, object]


def _normalize_string_items(items: Sequence[object]) -> list[str | None]:
    out: list[str | None] = []
    for item in items:
        if item is None:
            out.append(None)
        elif isinstance(item, str):
            out.append(item)
        else:
            out.append(str(item))
    return out


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


QNAME_STRUCT = pa.struct([("name", pa.string()), ("source", pa.string())])
QNAME_LIST = pa.large_list_view(QNAME_STRUCT)
_QNAME_FLAT_FIELDS = flatten_struct_field(pa.field("qname", QNAME_STRUCT))
QNAME_KEYS = tuple(field.name.split(".", 1)[1] for field in _QNAME_FLAT_FIELDS)

_CST_METADATA_EXTRA = {
    b"extractor_name": b"cst",
    b"extractor_version": str(SCHEMA_VERSION).encode("utf-8"),
}

_PARSE_MANIFEST_FIELDS = [
    ArrowFieldSpec(name="encoding", dtype=pa.string()),
    ArrowFieldSpec(name="default_indent", dtype=pa.string()),
    ArrowFieldSpec(name="default_newline", dtype=pa.string()),
    ArrowFieldSpec(name="has_trailing_newline", dtype=pa.bool_()),
    ArrowFieldSpec(name="future_imports", dtype=pa.large_list_view(pa.string())),
    ArrowFieldSpec(name="module_name", dtype=pa.string()),
    ArrowFieldSpec(name="package_name", dtype=pa.string()),
]

_PARSE_ERRORS_FIELDS = [
    ArrowFieldSpec(name="error_type", dtype=pa.string()),
    ArrowFieldSpec(name="message", dtype=pa.string()),
    ArrowFieldSpec(name="raw_line", dtype=pa.int32()),
    ArrowFieldSpec(name="raw_column", dtype=pa.int32()),
    ArrowFieldSpec(name="meta", dtype=pa.map_(pa.string(), pa.string())),
]

_NAME_REFS_FIELDS = [
    ArrowFieldSpec(name="name_ref_id", dtype=pa.string()),
    ArrowFieldSpec(name="name", dtype=pa.string()),
    ArrowFieldSpec(name="expr_ctx", dtype=pa.string()),
    ArrowFieldSpec(name="bstart", dtype=pa.int64()),
    ArrowFieldSpec(name="bend", dtype=pa.int64()),
]

_IMPORTS_FIELDS = [
    ArrowFieldSpec(name="import_id", dtype=pa.string()),
    ArrowFieldSpec(name="kind", dtype=pa.string()),
    ArrowFieldSpec(name="module", dtype=pa.string()),
    ArrowFieldSpec(name="relative_level", dtype=pa.int32()),
    ArrowFieldSpec(name="name", dtype=pa.string()),
    ArrowFieldSpec(name="asname", dtype=pa.string()),
    ArrowFieldSpec(name="is_star", dtype=pa.bool_()),
    ArrowFieldSpec(name="stmt_bstart", dtype=pa.int64()),
    ArrowFieldSpec(name="stmt_bend", dtype=pa.int64()),
    ArrowFieldSpec(name="alias_bstart", dtype=pa.int64()),
    ArrowFieldSpec(name="alias_bend", dtype=pa.int64()),
]

_CALLSITES_FIELDS = [
    ArrowFieldSpec(name="call_id", dtype=pa.string()),
    *call_span_bundle().fields,
    ArrowFieldSpec(name="callee_bstart", dtype=pa.int64()),
    ArrowFieldSpec(name="callee_bend", dtype=pa.int64()),
    ArrowFieldSpec(name="callee_shape", dtype=pa.string()),
    ArrowFieldSpec(name="callee_text", dtype=pa.string()),
    ArrowFieldSpec(name="arg_count", dtype=pa.int32()),
    ArrowFieldSpec(name="callee_dotted", dtype=pa.string()),
    ArrowFieldSpec(name="callee_qnames", dtype=QNAME_LIST),
]

_DEFS_FIELDS = [
    ArrowFieldSpec(name="def_id", dtype=pa.string()),
    ArrowFieldSpec(name="container_def_id", dtype=pa.string()),
    ArrowFieldSpec(name="kind", dtype=pa.string()),
    ArrowFieldSpec(name="name", dtype=pa.string()),
    ArrowFieldSpec(name="def_bstart", dtype=pa.int64()),
    ArrowFieldSpec(name="def_bend", dtype=pa.int64()),
    ArrowFieldSpec(name="name_bstart", dtype=pa.int64()),
    ArrowFieldSpec(name="name_bend", dtype=pa.int64()),
    ArrowFieldSpec(name="qnames", dtype=QNAME_LIST),
]

_TYPE_EXPRS_FIELDS = [
    ArrowFieldSpec(name="type_expr_id", dtype=pa.string()),
    ArrowFieldSpec(name="owner_def_id", dtype=pa.string()),
    ArrowFieldSpec(name="param_name", dtype=pa.string()),
    ArrowFieldSpec(name="expr_kind", dtype=pa.string()),
    ArrowFieldSpec(name="expr_role", dtype=pa.string()),
    ArrowFieldSpec(name="bstart", dtype=pa.int64()),
    ArrowFieldSpec(name="bend", dtype=pa.int64()),
    ArrowFieldSpec(name="expr_text", dtype=pa.string()),
]

_PARSE_MANIFEST_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_PARSE_MANIFEST_FIELDS)
)
_PARSE_ERRORS_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_PARSE_ERRORS_FIELDS)
)
_NAME_REFS_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_NAME_REFS_FIELDS)
)
_IMPORTS_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_IMPORTS_FIELDS)
)
_CALLSITES_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_CALLSITES_FIELDS)
)
_DEFS_BASE_COLUMNS = tuple(field.name for field in (*file_identity_bundle().fields, *_DEFS_FIELDS))
_TYPE_EXPRS_BASE_COLUMNS = tuple(
    field.name for field in (*file_identity_bundle().fields, *_TYPE_EXPRS_FIELDS)
)

_PARSE_MANIFEST_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_PARSE_MANIFEST_BASE_COLUMNS),
    extra=_CST_METADATA_EXTRA,
)
_PARSE_ERRORS_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_PARSE_ERRORS_BASE_COLUMNS),
    extra=_CST_METADATA_EXTRA,
)
_NAME_REFS_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_NAME_REFS_BASE_COLUMNS),
    extra=_CST_METADATA_EXTRA,
)
_IMPORTS_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_IMPORTS_BASE_COLUMNS),
    extra=_CST_METADATA_EXTRA,
)
_CALLSITES_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_CALLSITES_BASE_COLUMNS),
    extra=_CST_METADATA_EXTRA,
)
_DEFS_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_DEFS_BASE_COLUMNS),
    extra=_CST_METADATA_EXTRA,
)
_TYPE_EXPRS_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_TYPE_EXPRS_BASE_COLUMNS),
    extra=_CST_METADATA_EXTRA,
)

PARSE_MANIFEST_QUERY = QuerySpec.simple(*_PARSE_MANIFEST_BASE_COLUMNS)
PARSE_ERRORS_QUERY = QuerySpec.simple(*_PARSE_ERRORS_BASE_COLUMNS)
NAME_REFS_QUERY = QuerySpec(
    projection=ProjectionSpec(
        base=_NAME_REFS_BASE_COLUMNS,
        derived={
            "name_ref_id": MaskedHashExprSpec(
                spec=CST_NAME_REF_ID_SPEC,
                required=("file_id", "bstart", "bend"),
            )
        },
    )
)
IMPORTS_QUERY = QuerySpec(
    projection=ProjectionSpec(
        base=_IMPORTS_BASE_COLUMNS,
        derived={
            "import_id": MaskedHashExprSpec(
                spec=CST_IMPORT_ID_SPEC,
                required=("file_id", "kind", "alias_bstart", "alias_bend"),
            )
        },
    )
)
CALLSITES_QUERY = QuerySpec(
    projection=ProjectionSpec(
        base=_CALLSITES_BASE_COLUMNS,
        derived={
            "call_id": MaskedHashExprSpec(
                spec=CST_CALL_ID_SPEC,
                required=("file_id", "call_bstart", "call_bend"),
            )
        },
    )
)
DEFS_QUERY = QuerySpec(
    projection=ProjectionSpec(
        base=_DEFS_BASE_COLUMNS,
        derived={
            "def_id": MaskedHashExprSpec(
                spec=CST_DEF_ID_SPEC,
                required=("file_id", "kind", "def_bstart", "def_bend"),
            ),
            "container_def_id": MaskedHashExprSpec(
                spec=CST_CONTAINER_DEF_ID_SPEC,
                required=(
                    "file_id",
                    "container_def_kind",
                    "container_def_bstart",
                    "container_def_bend",
                ),
            ),
        },
    )
)
TYPE_EXPRS_QUERY = QuerySpec(
    projection=ProjectionSpec(
        base=_TYPE_EXPRS_BASE_COLUMNS,
        derived={
            "type_expr_id": MaskedHashExprSpec(
                spec=CST_TYPE_EXPR_ID_SPEC,
                required=("path", "bstart", "bend"),
            ),
            "owner_def_id": MaskedHashExprSpec(
                spec=CST_OWNER_DEF_ID_SPEC,
                required=("file_id", "owner_def_kind", "owner_def_bstart", "owner_def_bend"),
            ),
        },
    )
)

PARSE_MANIFEST_SPEC = register_dataset(
    name="py_cst_parse_manifest_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_PARSE_MANIFEST_FIELDS,
    registration=DatasetRegistration(
        query_spec=PARSE_MANIFEST_QUERY,
        metadata_spec=_PARSE_MANIFEST_METADATA,
    ),
)

PARSE_ERRORS_SPEC = register_dataset(
    name="py_cst_parse_errors_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_PARSE_ERRORS_FIELDS,
    registration=DatasetRegistration(
        query_spec=PARSE_ERRORS_QUERY,
        metadata_spec=_PARSE_ERRORS_METADATA,
    ),
)

NAME_REFS_SPEC = register_dataset(
    name="py_cst_name_refs_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_NAME_REFS_FIELDS,
    registration=DatasetRegistration(
        query_spec=NAME_REFS_QUERY,
        metadata_spec=_NAME_REFS_METADATA,
    ),
)

IMPORTS_SPEC = register_dataset(
    name="py_cst_imports_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_IMPORTS_FIELDS,
    registration=DatasetRegistration(
        query_spec=IMPORTS_QUERY,
        metadata_spec=_IMPORTS_METADATA,
    ),
)

CALLSITES_SPEC = register_dataset(
    name="py_cst_callsites_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_CALLSITES_FIELDS,
    registration=DatasetRegistration(
        query_spec=CALLSITES_QUERY,
        metadata_spec=_CALLSITES_METADATA,
    ),
)

DEFS_SPEC = register_dataset(
    name="py_cst_defs_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_DEFS_FIELDS,
    registration=DatasetRegistration(
        query_spec=DEFS_QUERY,
        metadata_spec=_DEFS_METADATA,
    ),
)

TYPE_EXPRS_SPEC = register_dataset(
    name="py_cst_type_exprs_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=_TYPE_EXPRS_FIELDS,
    registration=DatasetRegistration(
        query_spec=TYPE_EXPRS_QUERY,
        metadata_spec=_TYPE_EXPRS_METADATA,
    ),
)

PARSE_MANIFEST_SCHEMA = PARSE_MANIFEST_SPEC.schema()
PARSE_ERRORS_SCHEMA = PARSE_ERRORS_SPEC.schema()
NAME_REFS_SCHEMA = NAME_REFS_SPEC.schema()
IMPORTS_SCHEMA = IMPORTS_SPEC.schema()
CALLSITES_SCHEMA = CALLSITES_SPEC.schema()
DEFS_SCHEMA = DEFS_SPEC.schema()
TYPE_EXPRS_SCHEMA = TYPE_EXPRS_SPEC.schema()


def _cst_metadata_specs(options: CSTExtractOptions) -> dict[str, SchemaMetadataSpec]:
    run_meta = options_metadata_spec(options=options, repo_id=options.repo_id)
    return {
        "cst_parse_manifest": merge_metadata_specs(_PARSE_MANIFEST_METADATA, run_meta),
        "cst_parse_errors": merge_metadata_specs(_PARSE_ERRORS_METADATA, run_meta),
        "cst_name_refs": merge_metadata_specs(_NAME_REFS_METADATA, run_meta),
        "cst_imports": merge_metadata_specs(_IMPORTS_METADATA, run_meta),
        "cst_callsites": merge_metadata_specs(_CALLSITES_METADATA, run_meta),
        "cst_defs": merge_metadata_specs(_DEFS_METADATA, run_meta),
        "cst_type_exprs": merge_metadata_specs(_TYPE_EXPRS_METADATA, run_meta),
    }


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

    @classmethod
    def build(cls, options: CSTExtractOptions) -> CSTExtractContext:
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
        if err is not None and ctx.options.include_parse_errors:
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
    Mapping[cst.CSTNode, Collection[QualifiedName]],
]:
    providers: set[type[BaseMetadataProvider[object]]] = {ByteSpanPositionProvider}
    if options.compute_expr_context and options.include_name_refs:
        providers.add(ExpressionContextProvider)
    if options.compute_qualified_names and (options.include_callsites or options.include_defs):
        providers.add(QualifiedNameProvider)

    resolved = wrapper.resolve_many(providers)
    return (
        resolved.get(ByteSpanPositionProvider, {}),
        resolved.get(ExpressionContextProvider, {}),
        cast(
            "Mapping[cst.CSTNode, Collection[QualifiedName]]",
            resolved.get(QualifiedNameProvider, {}),
        ),
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
    qn_map: Mapping[cst.CSTNode, Collection[QualifiedName]]


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
        qualified = sorted(qset, key=lambda q: (q.name, str(q.source)))
        return [dict(zip(QNAME_KEYS, (q.name, str(q.source)), strict=True)) for q in qualified]

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

    def visit_FunctionDef(self, node: cst.FunctionDef) -> bool | None:
        """Collect function definition rows.

        Returns
        -------
        bool | None
            True to continue CST traversal.
        """
        if not self._options.include_defs:
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
        if self._options.include_defs and self._def_stack:
            self._def_stack.pop()

    def visit_ClassDef(self, node: cst.ClassDef) -> bool | None:
        """Collect class definition rows.

        Returns
        -------
        bool | None
            True to continue CST traversal.
        """
        if not self._options.include_defs:
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
        if self._options.include_defs and self._def_stack:
            self._def_stack.pop()

    def visit_Name(self, node: cst.Name) -> bool | None:
        """Collect name reference rows.

        Returns
        -------
        bool | None
            True to continue CST traversal.
        """
        if not self._options.include_name_refs:
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
        if not self._options.include_callsites:
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
    if ctx.options.include_parse_manifest:
        future_imports = getattr(module, "future_imports", []) or []
        normalized_future_imports = _normalize_string_items(future_imports)
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
    file_contexts: Iterable[FileContext] | None = None,
    ctx: ExecutionContext | None = None,
) -> CSTExtractResult:
    """Extract LibCST-derived structures from repo files.

    Returns
    -------
    CSTExtractResult
        Tables derived from LibCST parsing and metadata providers.
    """
    options = options or CSTExtractOptions()
    exec_ctx = ctx or ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))
    extract_ctx = CSTExtractContext.build(options)
    metadata_specs = _cst_metadata_specs(options)

    for file_ctx in iter_contexts(repo_files, file_contexts):
        _extract_cst_for_context(file_ctx, extract_ctx)

    return _build_cst_result(extract_ctx, exec_ctx, metadata_specs)


def _build_manifest_table(
    ctx: CSTExtractContext,
    exec_ctx: ExecutionContext,
) -> TableLike:
    return materialize_plan(
        _build_manifest_plan(ctx, exec_ctx),
        ctx=exec_ctx,
        attach_ordering_metadata=True,
    )


def _build_manifest_plan(
    ctx: CSTExtractContext,
    exec_ctx: ExecutionContext,
) -> Plan:
    if not ctx.manifest_rows:
        return Plan.table_source(empty_table(PARSE_MANIFEST_SCHEMA))
    plan = plan_from_rows(ctx.manifest_rows, schema=PARSE_MANIFEST_SCHEMA, label="cst_manifest")
    plan = PARSE_MANIFEST_QUERY.apply_to_plan(plan, ctx=exec_ctx)
    return align_plan(
        plan,
        schema=PARSE_MANIFEST_SCHEMA,
        available=PARSE_MANIFEST_SCHEMA.names,
        ctx=exec_ctx,
    )


def _build_errors_table(
    ctx: CSTExtractContext,
    exec_ctx: ExecutionContext,
) -> TableLike:
    return materialize_plan(
        _build_errors_plan(ctx, exec_ctx),
        ctx=exec_ctx,
        attach_ordering_metadata=True,
    )


def _build_errors_plan(
    ctx: CSTExtractContext,
    exec_ctx: ExecutionContext,
) -> Plan:
    if not ctx.error_rows:
        return Plan.table_source(empty_table(PARSE_ERRORS_SCHEMA))
    plan = plan_from_rows(ctx.error_rows, schema=PARSE_ERRORS_SCHEMA, label="cst_parse_errors")
    plan = PARSE_ERRORS_QUERY.apply_to_plan(plan, ctx=exec_ctx)
    return align_plan(
        plan,
        schema=PARSE_ERRORS_SCHEMA,
        available=PARSE_ERRORS_SCHEMA.names,
        ctx=exec_ctx,
    )


def _build_name_refs_table(
    ctx: CSTExtractContext,
    exec_ctx: ExecutionContext,
) -> TableLike:
    return materialize_plan(
        _build_name_refs_plan(ctx, exec_ctx),
        ctx=exec_ctx,
        attach_ordering_metadata=True,
    )


def _build_name_refs_plan(
    ctx: CSTExtractContext,
    exec_ctx: ExecutionContext,
) -> Plan:
    if not ctx.name_ref_rows:
        return Plan.table_source(empty_table(NAME_REFS_SCHEMA))
    plan = plan_from_rows(ctx.name_ref_rows, schema=NAME_REFS_SCHEMA, label="cst_name_refs")
    plan = NAME_REFS_QUERY.apply_to_plan(plan, ctx=exec_ctx)
    return align_plan(
        plan,
        schema=NAME_REFS_SCHEMA,
        available=NAME_REFS_SCHEMA.names,
        ctx=exec_ctx,
    )


def _build_imports_table(
    ctx: CSTExtractContext,
    exec_ctx: ExecutionContext,
) -> TableLike:
    return materialize_plan(
        _build_imports_plan(ctx, exec_ctx),
        ctx=exec_ctx,
        attach_ordering_metadata=True,
    )


def _build_imports_plan(
    ctx: CSTExtractContext,
    exec_ctx: ExecutionContext,
) -> Plan:
    if not ctx.import_rows:
        return Plan.table_source(empty_table(IMPORTS_SCHEMA))
    plan = plan_from_rows(ctx.import_rows, schema=IMPORTS_SCHEMA, label="cst_imports")
    plan = IMPORTS_QUERY.apply_to_plan(plan, ctx=exec_ctx)
    return align_plan(
        plan,
        schema=IMPORTS_SCHEMA,
        available=IMPORTS_SCHEMA.names,
        ctx=exec_ctx,
    )


def _build_callsites_table(
    ctx: CSTExtractContext,
    exec_ctx: ExecutionContext,
) -> TableLike:
    return materialize_plan(
        _build_callsites_plan(ctx, exec_ctx),
        ctx=exec_ctx,
        attach_ordering_metadata=True,
    )


def _build_callsites_plan(
    ctx: CSTExtractContext,
    exec_ctx: ExecutionContext,
) -> Plan:
    if not ctx.call_rows:
        return Plan.table_source(empty_table(CALLSITES_SCHEMA))
    plan = plan_from_rows(ctx.call_rows, schema=CALLSITES_SCHEMA, label="cst_callsites")
    plan = CALLSITES_QUERY.apply_to_plan(plan, ctx=exec_ctx)
    return align_plan(
        plan,
        schema=CALLSITES_SCHEMA,
        available=CALLSITES_SCHEMA.names,
        ctx=exec_ctx,
    )


def _build_defs_table(
    ctx: CSTExtractContext,
    exec_ctx: ExecutionContext,
) -> TableLike:
    return materialize_plan(
        _build_defs_plan(ctx, exec_ctx),
        ctx=exec_ctx,
        attach_ordering_metadata=True,
    )


def _build_defs_plan(
    ctx: CSTExtractContext,
    exec_ctx: ExecutionContext,
) -> Plan:
    if not ctx.def_rows:
        return Plan.table_source(empty_table(DEFS_SCHEMA))
    plan = plan_from_rows(ctx.def_rows, schema=DEFS_SCHEMA, label="cst_defs")
    plan = DEFS_QUERY.apply_to_plan(plan, ctx=exec_ctx)
    return align_plan(
        plan,
        schema=DEFS_SCHEMA,
        available=DEFS_SCHEMA.names,
        ctx=exec_ctx,
    )


def _build_type_exprs_table(
    ctx: CSTExtractContext,
    exec_ctx: ExecutionContext,
) -> TableLike:
    return materialize_plan(
        _build_type_exprs_plan(ctx, exec_ctx),
        ctx=exec_ctx,
        attach_ordering_metadata=True,
    )


def _build_type_exprs_plan(
    ctx: CSTExtractContext,
    exec_ctx: ExecutionContext,
) -> Plan:
    if not ctx.type_expr_rows:
        return Plan.table_source(empty_table(TYPE_EXPRS_SCHEMA))
    plan = plan_from_rows(ctx.type_expr_rows, schema=TYPE_EXPRS_SCHEMA, label="cst_type_exprs")
    plan = TYPE_EXPRS_QUERY.apply_to_plan(plan, ctx=exec_ctx)
    return align_plan(
        plan,
        schema=TYPE_EXPRS_SCHEMA,
        available=TYPE_EXPRS_SCHEMA.names,
        ctx=exec_ctx,
    )


def _build_cst_result(
    ctx: CSTExtractContext,
    exec_ctx: ExecutionContext,
    metadata_specs: Mapping[str, SchemaMetadataSpec],
) -> CSTExtractResult:
    plans = _build_cst_plans(ctx, exec_ctx)
    return CSTExtractResult(
        py_cst_parse_manifest=materialize_plan(
            plans["cst_parse_manifest"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["cst_parse_manifest"],
            attach_ordering_metadata=True,
        ),
        py_cst_parse_errors=materialize_plan(
            plans["cst_parse_errors"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["cst_parse_errors"],
            attach_ordering_metadata=True,
        ),
        py_cst_name_refs=materialize_plan(
            plans["cst_name_refs"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["cst_name_refs"],
            attach_ordering_metadata=True,
        ),
        py_cst_imports=materialize_plan(
            plans["cst_imports"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["cst_imports"],
            attach_ordering_metadata=True,
        ),
        py_cst_callsites=materialize_plan(
            plans["cst_callsites"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["cst_callsites"],
            attach_ordering_metadata=True,
        ),
        py_cst_defs=materialize_plan(
            plans["cst_defs"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["cst_defs"],
            attach_ordering_metadata=True,
        ),
        py_cst_type_exprs=materialize_plan(
            plans["cst_type_exprs"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["cst_type_exprs"],
            attach_ordering_metadata=True,
        ),
    )


def _build_cst_plans(
    ctx: CSTExtractContext,
    exec_ctx: ExecutionContext,
) -> dict[str, Plan]:
    return {
        "cst_parse_manifest": _build_manifest_plan(ctx, exec_ctx),
        "cst_parse_errors": _build_errors_plan(ctx, exec_ctx),
        "cst_name_refs": _build_name_refs_plan(ctx, exec_ctx),
        "cst_imports": _build_imports_plan(ctx, exec_ctx),
        "cst_callsites": _build_callsites_plan(ctx, exec_ctx),
        "cst_defs": _build_defs_plan(ctx, exec_ctx),
        "cst_type_exprs": _build_type_exprs_plan(ctx, exec_ctx),
    }


class _CstTablesKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: CSTExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    ctx: ExecutionContext | None
    prefer_reader: bool


class _CstTablesKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: CSTExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    ctx: ExecutionContext | None
    prefer_reader: Literal[False]


class _CstTablesKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: CSTExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    ctx: ExecutionContext | None
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
        Keyword-only arguments for extraction (repo_files, options, file_contexts, ctx,
        prefer_reader).

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Extracted CST outputs keyed by output name.
    """
    repo_files = kwargs["repo_files"]
    options = kwargs.get("options") or CSTExtractOptions()
    file_contexts = kwargs.get("file_contexts")
    exec_ctx = kwargs.get("ctx") or ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))
    prefer_reader = kwargs.get("prefer_reader", False)
    extract_ctx = CSTExtractContext.build(options)
    metadata_specs = _cst_metadata_specs(options)
    for file_ctx in iter_contexts(repo_files, file_contexts):
        _extract_cst_for_context(file_ctx, extract_ctx)
    return run_plan_bundle(
        _build_cst_plans(extract_ctx, exec_ctx),
        ctx=exec_ctx,
        prefer_reader=prefer_reader,
        metadata_specs=metadata_specs,
        attach_ordering_metadata=True,
    )
