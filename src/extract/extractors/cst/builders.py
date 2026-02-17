"""Extract LibCST-derived structures into Arrow tables using shared helpers."""

from __future__ import annotations

import re
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import libcst as cst
import libcst.matchers as m
from libcst import helpers
from libcst.metadata import (
    BaseMetadataProvider,
    ByteSpanPositionProvider,
    ExpressionContextProvider,
    FullRepoManager,
    FullyQualifiedNameProvider,
    MetadataWrapper,
    ParentNodeProvider,
    PositionProvider,
    QualifiedName,
    QualifiedNameProvider,
    ScopeProvider,
    TypeInferenceProvider,
    WhitespaceInclusivePositionProvider,
)

from core_types import RowPermissive as Row
from datafusion_engine.schema import default_attrs_value
from extract.coordination.context import (
    FileContext,
    SpanSpec,
    attrs_map,
    file_identity_row,
    span_dict,
)
from extract.extractors.cst.setup import CstExtractOptions
from extract.extractors.cst.visitors import (
    CSTExtractContext,
    CSTFileContext,
)
from extract.infrastructure.schema_cache import libcst_files_fingerprint

type QualifiedNameSet = Collection[QualifiedName] | Callable[[], Collection[QualifiedName]]
CST_LINE_BASE = 1
CST_COL_UNIT = "utf32"
CST_END_EXCLUSIVE = True


type DefKey = tuple[str, int, int]


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
        editor_line = int(getattr(exc, "editor_line", raw_line) or raw_line)
        editor_column = int(getattr(exc, "editor_column", raw_column) or raw_column)
        context = getattr(exc, "context", None)
        return (
            None,
            {
                **file_identity_row(file_ctx),
                "error_type": type(exc).__name__,
                "message": str(exc),
                "raw_line": raw_line,
                "raw_column": raw_column,
                "editor_line": editor_line,
                "editor_column": editor_column,
                "context": context,
                "line_base": CST_LINE_BASE,
                "col_unit": CST_COL_UNIT,
                "end_exclusive": CST_END_EXCLUSIVE,
                "meta": {
                    "raw_line": str(raw_line),
                    "raw_column": str(raw_column),
                    "editor_line": str(editor_line),
                    "editor_column": str(editor_column),
                },
            },
        )


def _module_package_names(
    *,
    abs_path: str | Path | None,
    options: CstExtractOptions,
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


def _repo_relative_path(file_ctx: FileContext, repo_root: Path | None) -> Path | None:
    if repo_root is None:
        return None
    repo_root = repo_root.resolve()
    abs_path = file_ctx.abs_path
    if abs_path:
        try:
            return Path(abs_path).resolve().relative_to(repo_root)
        except ValueError:
            return None
    rel_path = Path(file_ctx.path)
    if rel_path.is_absolute():
        try:
            return rel_path.resolve().relative_to(repo_root)
        except ValueError:
            return None
    return rel_path


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
    config = getattr(module, "config", None)
    python_version = getattr(config, "python_version", None)
    parsed_python_version = None
    if python_version:
        parsed_python_version = ".".join(str(part) for part in python_version)
    parser_backend = getattr(config, "parser_backend", None) or "libcst"
    return {
        **file_identity_row(ctx.file_ctx),
        "encoding": getattr(module, "encoding", None),
        "default_indent": getattr(module, "default_indent", None),
        "default_newline": getattr(module, "default_newline", None),
        "has_trailing_newline": bool(getattr(module, "has_trailing_newline", False)),
        "future_imports": list(future_imports),
        "module_name": module_name,
        "package_name": package_name,
        "libcst_version": getattr(cst, "__version__", None),
        "parser_backend": parser_backend,
        "parsed_python_version": parsed_python_version,
        "schema_identity_hash": libcst_files_fingerprint(),
    }


def _resolve_metadata_maps(
    wrapper: MetadataWrapper,
    options: CstExtractOptions,
    *,
    repo_manager: FullRepoManager | None,
) -> tuple[
    Mapping[cst.CSTNode, object],
    Mapping[cst.CSTNode, object],
    Mapping[cst.CSTNode, object],
    Mapping[cst.CSTNode, object],
    Mapping[cst.CSTNode, QualifiedNameSet],
    Mapping[cst.CSTNode, QualifiedNameSet],
    Mapping[cst.CSTNode, object],
]:
    providers: set[type[BaseMetadataProvider[object]]] = {ByteSpanPositionProvider}
    include_refs = options.include_refs
    if options.compute_expr_context and include_refs:
        providers.add(ExpressionContextProvider)
    if options.compute_scope and include_refs:
        providers.add(ScopeProvider)
        providers.add(ParentNodeProvider)
    include_callsites = options.include_callsites
    include_defs = options.include_defs
    if options.compute_qualified_names and (include_callsites or include_defs):
        providers.add(QualifiedNameProvider)
    if (
        repo_manager is not None
        and options.compute_fully_qualified_names
        and (include_callsites or include_defs or include_refs)
    ):
        providers.add(FullyQualifiedNameProvider)
    if (
        repo_manager is not None
        and options.compute_type_inference
        and (include_callsites or include_refs)
    ):
        providers.add(TypeInferenceProvider)
    optional = {FullyQualifiedNameProvider, TypeInferenceProvider}
    try:
        resolved = wrapper.resolve_many(providers)
    except (
        cst.CSTLogicError,
        cst.CSTValidationError,
        cst.MetadataException,
        cst.ParserSyntaxError,
    ):
        fallback = providers - optional
        resolved = wrapper.resolve_many(fallback)
    return (
        resolved.get(ByteSpanPositionProvider, {}),
        resolved.get(ExpressionContextProvider, {}),
        resolved.get(ScopeProvider, {}),
        resolved.get(ParentNodeProvider, {}),
        cast("Mapping[cst.CSTNode, QualifiedNameSet]", resolved.get(QualifiedNameProvider, {})),
        cast(
            "Mapping[cst.CSTNode, QualifiedNameSet]", resolved.get(FullyQualifiedNameProvider, {})
        ),
        resolved.get(TypeInferenceProvider, {}),
    )


def _row_map(row: Mapping[str, object]) -> dict[str, object]:
    return dict(row)


def _parse_matcher_template(
    template: str,
    config: cst.PartialParserConfig,
) -> cst.CSTNode | None:
    try:
        return cst.parse_expression(template, config=config)
    except (TypeError, cst.ParserSyntaxError):
        pass
    try:
        return cst.parse_statement(template, config=config)
    except (TypeError, cst.ParserSyntaxError):
        return None


def _matcher_counts(module: cst.Module, options: CstExtractOptions) -> dict[str, str]:
    if not options.matcher_templates:
        return {}
    source = module.code

    def _prefilter(template: str) -> bool:
        if not source:
            return True
        if template in source:
            return True
        match = re.search(r"[A-Za-z_][A-Za-z0-9_]*", template)
        return not (match and match.group(0) not in source)

    config = module.config_for_parsing
    counts: dict[str, str] = {}
    for template in options.matcher_templates:
        if not _prefilter(template):
            continue
        parsed = _parse_matcher_template(template, config)
        if parsed is None:
            continue

        def _match(node: cst.CSTNode, *, tmpl: cst.CSTNode = parsed) -> bool:
            return node.deep_equals(tmpl)

        matcher = m.MatchIfTrue(_match)
        matches = m.findall(module, matcher)
        counts[f"matcher_template:{template}"] = str(len(matches))
    return counts


def _should_collect(options: CstExtractOptions) -> bool:
    return any(
        (
            options.include_refs,
            options.include_imports,
            options.include_callsites,
            options.include_defs,
            options.include_type_exprs,
            options.include_docstrings,
            options.include_decorators,
            options.include_call_args,
            bool(options.matcher_templates),
        )
    )


def _iter_cst_children(
    node: cst.CSTNode,
) -> Iterable[tuple[str, int | None, cst.CSTNode]]:
    for field_name in getattr(node, "__dataclass_fields__", {}):
        if field_name == "__slots__":
            continue
        value = getattr(node, field_name, None)
        if isinstance(value, cst.CSTNode):
            yield field_name, None, value
            continue
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            for idx, item in enumerate(value):
                if isinstance(item, cst.CSTNode):
                    yield field_name, idx, item


def _span_from_positions(
    *,
    pos: object | None,
    byte_span: object | None,
    col_unit: str,
) -> dict[str, object] | None:
    start_line0 = start_col = end_line0 = end_col = None
    if pos is not None:
        start = getattr(pos, "start", None)
        end = getattr(pos, "end", None)
        if start is not None:
            start_line0 = int(getattr(start, "line", 0)) - CST_LINE_BASE
            start_col = int(getattr(start, "column", 0))
        if end is not None:
            end_line0 = int(getattr(end, "line", 0)) - CST_LINE_BASE
            end_col = int(getattr(end, "column", 0))
    byte_start = byte_len = None
    if byte_span is not None:
        byte_start = int(getattr(byte_span, "start", 0))
        byte_len = int(getattr(byte_span, "length", 0))
    return span_dict(
        SpanSpec(
            start_line0=start_line0,
            start_col=start_col,
            end_line0=end_line0,
            end_col=end_col,
            end_exclusive=CST_END_EXCLUSIVE,
            col_unit=col_unit,
            byte_start=byte_start,
            byte_len=byte_len,
        )
    )


def _collect_cst_nodes_edges(
    module: cst.Module,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    wrapper = MetadataWrapper(module, unsafe_skip_copy=True)
    byte_map = wrapper.resolve(ByteSpanPositionProvider)
    pos_map = wrapper.resolve(PositionProvider)
    ws_map = wrapper.resolve(WhitespaceInclusivePositionProvider)
    nodes: list[dict[str, object]] = []
    edges: list[dict[str, object]] = []
    node_ids: dict[int, int] = {}
    next_id = 1

    def _visit(
        node: cst.CSTNode, *, parent_id: int | None, slot: str | None, idx: int | None
    ) -> None:
        nonlocal next_id
        node_key = id(node)
        node_id = node_ids.get(node_key)
        if node_id is None:
            node_id = next_id
            next_id += 1
            node_ids[node_key] = node_id
            span = _span_from_positions(
                pos=pos_map.get(node),
                byte_span=byte_map.get(node),
                col_unit=CST_COL_UNIT,
            )
            span_ws = _span_from_positions(
                pos=ws_map.get(node),
                byte_span=byte_map.get(node),
                col_unit=CST_COL_UNIT,
            )
            nodes.append(
                {
                    "cst_id": node_id,
                    "kind": f"libcst.{type(node).__name__}",
                    "span": span,
                    "span_ws": span_ws,
                    "attrs": attrs_map(default_attrs_value()),
                }
            )
        if parent_id is not None:
            edges.append(
                {
                    "src": parent_id,
                    "dst": node_id,
                    "kind": "CHILD",
                    "slot": slot,
                    "idx": idx,
                    "attrs": attrs_map(default_attrs_value()),
                }
            )
        for child_slot, child_idx, child in _iter_cst_children(node):
            _visit(child, parent_id=node_id, slot=child_slot, idx=child_idx)

    _visit(module, parent_id=None, slot=None, idx=None)
    return nodes, edges


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
    scope_map: Mapping[cst.CSTNode, object]
    parent_map: Mapping[cst.CSTNode, object]
    qn_map: Mapping[cst.CSTNode, QualifiedNameSet]
    fqn_map: Mapping[cst.CSTNode, QualifiedNameSet]
    type_map: Mapping[cst.CSTNode, object]


def _visit_with_collector(
    module: cst.Module,
    *,
    file_ctx: CSTFileContext,
    ctx: CSTExtractContext,
) -> None:
    cache: Mapping[type[BaseMetadataProvider[object]], object] | None = None
    if ctx.repo_manager is not None:
        rel_path = _repo_relative_path(file_ctx.file_ctx, ctx.options.repo_root)
        if rel_path is not None:
            try:
                cache = ctx.repo_manager.get_cache_for_path(str(rel_path))
            except KeyError:
                cache = None
    wrapper = MetadataWrapper(module, unsafe_skip_copy=True, cache=cache or {})
    (
        span_map,
        ctx_map,
        scope_map,
        parent_map,
        qn_map,
        fqn_map,
        type_map,
    ) = _resolve_metadata_maps(wrapper, ctx.options, repo_manager=ctx.repo_manager)
    collector = CSTCollector(
        ctx=CollectorContext(module=module, file_ctx=file_ctx, extract_ctx=ctx),
        meta=CollectorMetadata(
            span_map=span_map,
            ctx_map=ctx_map,
            scope_map=scope_map,
            parent_map=parent_map,
            qn_map=qn_map,
            fqn_map=fqn_map,
            type_map=type_map,
        ),
    )
    wrapper.visit(collector)


from extract.extractors.cst.builders_runtime import (
    CSTCollector,
    extract_cst,
    extract_cst_plans,
    extract_cst_tables,
)

__all__ = [
    "CSTCollector",
    "extract_cst",
    "extract_cst_plans",
    "extract_cst_tables",
]
