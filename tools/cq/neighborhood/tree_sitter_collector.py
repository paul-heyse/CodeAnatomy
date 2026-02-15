"""Tree-sitter-first neighborhood collector."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from tools.cq.core.snb_schema import (
    DegradeEventV1,
    NeighborhoodSliceKind,
    NeighborhoodSliceV1,
    SemanticEdgeV1,
    SemanticNodeRefV1,
)
from tools.cq.neighborhood.contracts import (
    TreeSitterNeighborhoodCollectRequest,
    TreeSitterNeighborhoodCollectResult,
)
from tools.cq.neighborhood.tree_sitter_neighborhood_query_engine import collect_callers_callees
from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryWindowV1,
    TreeSitterDiagnosticV1,
)
from tools.cq.search.tree_sitter.core.infrastructure import (
    ParserControlSettingsV1,
    apply_parser_controls,
    make_parser,
    parse_streaming_source,
)
from tools.cq.search.tree_sitter.core.node_utils import node_text
from tools.cq.search.tree_sitter.structural.export import export_structural_rows
from tools.cq.search.tree_sitter.structural.exports import collect_diagnostic_rows

if TYPE_CHECKING:
    from tree_sitter import Node, Parser

try:
    from tree_sitter import Parser as _TreeSitterParser
except ImportError:  # pragma: no cover - optional dependency
    _TreeSitterParser = None

_SCOPE_KINDS: frozenset[str] = frozenset(
    {
        "function_definition",
        "class_definition",
        "function_item",
        "struct_item",
        "enum_item",
        "trait_item",
        "impl_item",
        "mod_item",
    }
)

_STOP_CONTEXT_KINDS: frozenset[str] = frozenset({"block", "module", "source_file"})

_PY_DEFINITION_KINDS: frozenset[str] = frozenset({"function_definition", "class_definition"})
_RUST_DEFINITION_KINDS: frozenset[str] = frozenset(
    {
        "function_item",
        "struct_item",
        "enum_item",
        "trait_item",
        "impl_item",
        "mod_item",
        "macro_definition",
    }
)

_MAX_WALK_NODES = 10_000
_CONJUNCTION_PEER_MAX_DEPTH = 8


def _parser_controls() -> ParserControlSettingsV1:
    from tools.cq.core.settings_factory import SettingsFactory

    return SettingsFactory.parser_controls()


@dataclass(frozen=True, slots=True)
class _SliceBuildSpec:
    kind: NeighborhoodSliceKind
    title: str
    edge_kind: str
    edge_direction: Literal["inbound", "outbound", "none"]
    metadata: dict[str, object] | None = None


@dataclass(frozen=True, slots=True)
class _SliceBuildContext:
    source_bytes: bytes
    file_path: str
    subject_node_id: str
    max_per_slice: int
    slice_limits: Mapping[str, int] | None


@dataclass(frozen=True, slots=True)
class _AnchorNeighborhood:
    statement: Node
    parent_nodes: list[Node]
    child_nodes: list[Node]
    sibling_nodes: list[Node]
    related_nodes: list[Node]
    callers: list[Node]
    callees: list[Node]


def _parser(language: str) -> Parser:
    if _TreeSitterParser is None:
        msg = "tree_sitter parser bindings are unavailable"
        raise RuntimeError(msg)
    parser = make_parser(language)
    apply_parser_controls(parser, _parser_controls())
    return parser


def _walk_named(root: Node, *, max_nodes: int = _MAX_WALK_NODES) -> Iterable[Node]:
    stack: list[Node] = [root]
    count = 0
    while stack and count < max_nodes:
        node = stack.pop()
        count += 1
        yield node
        children = [child for child in node.children if getattr(child, "is_named", False)]
        stack.extend(reversed(children))


def _display_name(node: Node, source_bytes: bytes) -> str:
    name_node = node.child_by_field_name("name")
    if name_node is not None:
        name_text = node_text(name_node, source_bytes)
        if name_text:
            return name_text
    if node.type in {"identifier", "type_identifier"}:
        ident = node_text(node, source_bytes)
        if ident:
            return ident
    fallback = node_text(node, source_bytes)
    return fallback or node.type


def _node_id(node: Node, file_path: str) -> str:
    return (
        f"{file_path}:{int(getattr(node, 'start_byte', 0))}:"
        f"{int(getattr(node, 'end_byte', 0))}:{node.type}"
    )


def _node_ref(node: Node, source_bytes: bytes, file_path: str) -> SemanticNodeRefV1:
    return SemanticNodeRefV1(
        node_id=_node_id(node, file_path),
        kind=node.type,
        name=_display_name(node, source_bytes),
        display_label=_display_name(node, source_bytes),
        file_path=file_path,
        byte_span=(int(getattr(node, "start_byte", 0)), int(getattr(node, "end_byte", 0))),
    )


def _resolve_file(root: str, target_file: str) -> Path:
    path = Path(target_file)
    if path.is_absolute():
        return path
    return Path(root) / path


def _definition_kinds(language: str) -> frozenset[str]:
    return _RUST_DEFINITION_KINDS if language == "rust" else _PY_DEFINITION_KINDS


def _find_definition_by_name(
    root: Node,
    source_bytes: bytes,
    *,
    target_name: str,
    language: str,
) -> Node | None:
    def_kinds = _definition_kinds(language)
    for node in _walk_named(root):
        if node.type not in def_kinds:
            continue
        name_node = node.child_by_field_name("name")
        if name_node is None:
            continue
        if node_text(name_node, source_bytes) == target_name:
            return node
    for node in _walk_named(root):
        if node.type not in {"identifier", "type_identifier"}:
            continue
        if node_text(node, source_bytes) == target_name:
            return node
    return None


def _lift_anchor(node: Node) -> Node:
    current = node
    while current.parent is not None:
        parent = current.parent
        if parent.type in {
            "call",
            "attribute",
            "assignment",
            "import_statement",
            "import_from_statement",
            "call_expression",
            "macro_invocation",
            "use_declaration",
        }:
            return parent
        if current.type in _SCOPE_KINDS:
            return current
        current = parent
    return node


def _resolve_anchor(
    root: Node,
    source_bytes: bytes,
    *,
    target_name: str,
    target_line: int | None,
    target_col: int | None,
    language: str,
) -> Node | None:
    if target_line is not None:
        row = max(0, target_line - 1)
        col = max(0, target_col or 0)
        anchor = root.named_descendant_for_point_range((row, col), (row, col))
        if anchor is not None:
            lifted = _lift_anchor(anchor)
            if lifted.type not in {"module", "source_file"}:
                return lifted

    if target_name:
        by_name = _find_definition_by_name(
            root,
            source_bytes,
            target_name=target_name,
            language=language,
        )
        if by_name is not None:
            return _lift_anchor(by_name)

    return None


def _statement_node(node: Node) -> Node:
    current = node
    while current.parent is not None and current.parent.type not in _STOP_CONTEXT_KINDS:
        current = current.parent
    return current


def _dedupe_nodes(nodes: Iterable[Node], file_path: str) -> list[Node]:
    deduped: OrderedDict[str, Node] = OrderedDict()
    for node in nodes:
        deduped[_node_id(node, file_path)] = node
    return list(deduped.values())


def _apply_limit(
    nodes: list[Node],
    *,
    kind: NeighborhoodSliceKind,
    max_per_slice: int,
    slice_limits: Mapping[str, int] | None,
) -> list[Node]:
    limit = max_per_slice
    if slice_limits is not None and kind in slice_limits:
        limit = max(1, int(slice_limits[kind]))
    return nodes[:limit]


def _build_slice(
    spec: _SliceBuildSpec,
    nodes: list[Node],
    context: _SliceBuildContext,
) -> NeighborhoodSliceV1 | None:
    if not nodes:
        return None
    total = len(nodes)
    limited = _apply_limit(
        _dedupe_nodes(nodes, context.file_path),
        kind=spec.kind,
        max_per_slice=context.max_per_slice,
        slice_limits=context.slice_limits,
    )
    refs = tuple(_node_ref(node, context.source_bytes, context.file_path) for node in limited)
    edges: list[SemanticEdgeV1] = []
    if spec.edge_direction != "none":
        for ref in refs:
            source_node_id = (
                ref.node_id if spec.edge_direction == "inbound" else context.subject_node_id
            )
            target_node_id = (
                context.subject_node_id if spec.edge_direction == "inbound" else ref.node_id
            )
            edges.append(
                SemanticEdgeV1(
                    edge_id=f"{source_node_id}->{target_node_id}:{spec.edge_kind}",
                    source_node_id=source_node_id,
                    target_node_id=target_node_id,
                    edge_kind=spec.edge_kind,
                    evidence_source="tree_sitter",
                )
            )
    return NeighborhoodSliceV1(
        kind=spec.kind,
        title=spec.title,
        total=total,
        preview=refs,
        edges=tuple(edges),
        collapsed=True,
        metadata=spec.metadata,
    )


def _call_kinds(language: str) -> set[str]:
    return {"call_expression", "macro_invocation"} if language == "rust" else {"call"}


def _collect_callers_callees(
    *,
    anchor: Node,
    tree_root: Node,
    source_bytes: bytes,
    language: str,
) -> tuple[list[Node], list[Node]]:
    anchor_name = _display_name(anchor, source_bytes)
    if not anchor_name:
        return [], []
    callers, callees = collect_callers_callees(
        language=language,
        tree_root=tree_root,
        anchor=anchor,
        source_bytes=source_bytes,
        anchor_name=anchor_name,
    )
    if callers or callees:
        return callers, callees

    call_kinds = _call_kinds(language)
    fallback_callees = [node for node in _walk_named(anchor) if node.type in call_kinds]
    return [], fallback_callees


def _collect_parent_nodes(anchor: Node) -> list[Node]:
    parent_nodes: list[Node] = []
    current = anchor.parent
    while current is not None:
        if current.is_named:
            parent_nodes.append(current)
        current = current.parent
    if not parent_nodes and anchor.type in _SCOPE_KINDS:
        parent_nodes.append(anchor)
    return parent_nodes


def _collect_sibling_nodes(anchor: Node, statement: Node) -> list[Node]:
    siblings: list[Node] = []
    prev_sibling = anchor.prev_named_sibling
    if prev_sibling is not None:
        siblings.append(prev_sibling)
    next_sibling = anchor.next_named_sibling
    if next_sibling is not None:
        siblings.append(next_sibling)

    prev_stmt = statement.prev_named_sibling
    if prev_stmt is not None:
        siblings.append(prev_stmt)
    next_stmt = statement.next_named_sibling
    if next_stmt is not None:
        siblings.append(next_stmt)
    return siblings


def _collect_conjunction_peers(anchor: Node) -> list[Node]:
    conjunction_peers: list[Node] = []
    child_on_path = anchor
    current = anchor.parent
    depth = 0
    while (
        current is not None
        and current.type not in _STOP_CONTEXT_KINDS
        and depth < _CONJUNCTION_PEER_MAX_DEPTH
    ):
        conjunction_peers.extend(peer for peer in current.named_children if peer != child_on_path)
        child_on_path = current
        current = current.parent
        depth += 1
    return conjunction_peers


def _collect_anchor_neighborhood(
    *,
    anchor: Node,
    tree_root: Node,
    source_bytes: bytes,
    language: str,
) -> _AnchorNeighborhood:
    statement = _statement_node(anchor)
    callers, callees = _collect_callers_callees(
        anchor=anchor,
        tree_root=tree_root,
        source_bytes=source_bytes,
        language=language,
    )
    return _AnchorNeighborhood(
        statement=statement,
        parent_nodes=_collect_parent_nodes(anchor),
        child_nodes=list(anchor.named_children),
        sibling_nodes=_collect_sibling_nodes(anchor, statement),
        related_nodes=_collect_conjunction_peers(anchor),
        callers=callers,
        callees=callees,
    )


def _build_slices(
    *,
    request: TreeSitterNeighborhoodCollectRequest,
    subject_node_id: str,
    source_bytes: bytes,
    relationships: _AnchorNeighborhood,
) -> list[NeighborhoodSliceV1]:
    context = _SliceBuildContext(
        source_bytes=source_bytes,
        file_path=request.target_file,
        subject_node_id=subject_node_id,
        max_per_slice=request.max_per_slice,
        slice_limits=request.slice_limits,
    )
    slice_rows: tuple[tuple[_SliceBuildSpec, list[Node]], ...] = (
        (
            _SliceBuildSpec(
                kind="enclosing_context",
                title="Enclosing Context",
                edge_kind="contains",
                edge_direction="inbound",
            ),
            [relationships.statement],
        ),
        (
            _SliceBuildSpec(
                kind="parents",
                title="Parent Scopes",
                edge_kind="contains",
                edge_direction="inbound",
            ),
            relationships.parent_nodes,
        ),
        (
            _SliceBuildSpec(
                kind="children",
                title="Children",
                edge_kind="contains",
                edge_direction="outbound",
            ),
            relationships.child_nodes,
        ),
        (
            _SliceBuildSpec(
                kind="siblings",
                title="Siblings",
                edge_kind="adjacent",
                edge_direction="none",
                metadata={"includes": ["immediate", "statement"]},
            ),
            relationships.sibling_nodes,
        ),
        (
            _SliceBuildSpec(
                kind="related",
                title="Conjunction Peers",
                edge_kind="related",
                edge_direction="none",
            ),
            relationships.related_nodes,
        ),
        (
            _SliceBuildSpec(
                kind="callers",
                title="Callers",
                edge_kind="calls",
                edge_direction="inbound",
            ),
            relationships.callers,
        ),
        (
            _SliceBuildSpec(
                kind="callees",
                title="Callees",
                edge_kind="calls",
                edge_direction="outbound",
            ),
            relationships.callees,
        ),
    )
    slices: list[NeighborhoodSliceV1] = []
    for spec, nodes in slice_rows:
        maybe_slice = _build_slice(spec, nodes, context)
        if maybe_slice is not None:
            slices.append(maybe_slice)
    return slices


def _file_missing_result(source_path: Path) -> TreeSitterNeighborhoodCollectResult:
    return TreeSitterNeighborhoodCollectResult(
        diagnostics=(
            DegradeEventV1(
                stage="tree_sitter.neighborhood",
                severity="error",
                category="file_missing",
                message=f"Target file not found: {source_path}",
            ),
        )
    )


def _parse_error_result(message: str) -> TreeSitterNeighborhoodCollectResult:
    return TreeSitterNeighborhoodCollectResult(
        diagnostics=(
            DegradeEventV1(
                stage="tree_sitter.neighborhood",
                severity="error",
                category="parse_error",
                message=message,
            ),
        )
    )


def _parse_tree_for_request(
    request: TreeSitterNeighborhoodCollectRequest,
    source_path: Path,
) -> tuple[bytes, Node] | TreeSitterNeighborhoodCollectResult:
    try:
        source_bytes = source_path.read_bytes()
        parser = _parser(request.language)
        tree = parse_streaming_source(parser, source_bytes)
    except (OSError, RuntimeError, TypeError, ValueError, AttributeError) as exc:
        return _parse_error_result(type(exc).__name__)

    if tree is None:
        return _parse_error_result("parser returned no tree")
    return source_bytes, tree.root_node


def _anchor_unresolved_result(target_name: str) -> TreeSitterNeighborhoodCollectResult:
    return TreeSitterNeighborhoodCollectResult(
        diagnostics=(
            DegradeEventV1(
                stage="tree_sitter.neighborhood",
                severity="error",
                category="anchor_unresolved",
                message=f"Unable to resolve anchor for '{target_name}'",
            ),
        )
    )


def _tree_parse_diagnostics(
    *,
    tree_root: Node,
    diagnostic_rows: tuple[TreeSitterDiagnosticV1, ...],
) -> tuple[DegradeEventV1, ...]:
    if not tree_root.has_error:
        return ()
    if diagnostic_rows:
        first = diagnostic_rows[0]
        return (
            DegradeEventV1(
                stage="tree_sitter.neighborhood",
                severity="warning",
                category="parse_error_nodes",
                message=(
                    f"{getattr(first, 'kind', 'tree_sitter')} near line "
                    f"{getattr(first, 'start_line', '?')}"
                ),
            ),
        )
    return (
        DegradeEventV1(
            stage="tree_sitter.neighborhood",
            severity="warning",
            category="parse_error_nodes",
            message="Tree contains parse errors; neighborhood may be partial",
        ),
    )


def collect_tree_sitter_neighborhood(
    request: TreeSitterNeighborhoodCollectRequest,
) -> TreeSitterNeighborhoodCollectResult:
    """Collect neighborhood slices using tree-sitter node relationships.

    Returns:
    -------
    TreeSitterNeighborhoodCollectResult
        Collected subject, slices, and parse diagnostics.
    """
    source_path = _resolve_file(request.root, request.target_file)
    if not source_path.exists():
        return _file_missing_result(source_path)

    parsed = _parse_tree_for_request(request, source_path)
    if isinstance(parsed, TreeSitterNeighborhoodCollectResult):
        return parsed
    source_bytes, tree_root = parsed

    anchor = _resolve_anchor(
        tree_root,
        source_bytes,
        target_name=request.target_name,
        target_line=request.target_line,
        target_col=request.target_col,
        language=request.language,
    )
    if anchor is None:
        return _anchor_unresolved_result(request.target_name)

    subject = _node_ref(anchor, source_bytes, request.target_file)
    relationships = _collect_anchor_neighborhood(
        anchor=anchor,
        tree_root=tree_root,
        source_bytes=source_bytes,
        language=request.language,
    )
    slices = _build_slices(
        request=request,
        subject_node_id=subject.node_id,
        source_bytes=source_bytes,
        relationships=relationships,
    )
    structural_export = export_structural_rows(
        file_path=request.target_file,
        root=anchor,
        source_bytes=source_bytes,
    )
    diagnostic_rows = collect_diagnostic_rows(
        language=request.language,
        root=tree_root,
        windows=(
            QueryWindowV1(
                start_byte=int(getattr(tree_root, "start_byte", 0)),
                end_byte=int(getattr(tree_root, "end_byte", 0)),
            ),
        ),
        match_limit=512,
    )
    return TreeSitterNeighborhoodCollectResult(
        subject=subject,
        slices=tuple(slices),
        diagnostics=_tree_parse_diagnostics(tree_root=tree_root, diagnostic_rows=diagnostic_rows),
        structural_export=structural_export,
        cst_tokens=tuple(structural_export.tokens),
        cst_diagnostics=diagnostic_rows,
        cst_query_hits=(),
    )


__all__ = [
    "collect_tree_sitter_neighborhood",
]
