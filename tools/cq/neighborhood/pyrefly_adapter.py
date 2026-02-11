# ruff: noqa: DOC201,PLR0913
"""Pyrefly-to-SNB neighborhood slice adapter."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import cast

from tools.cq.core.snb_schema import (
    DegradeEventV1,
    NeighborhoodSliceKind,
    NeighborhoodSliceV1,
    SemanticEdgeV1,
    SemanticNodeRefV1,
)
from tools.cq.search.pyrefly_lsp import PyreflyLspRequest, enrich_with_pyrefly_lsp


def collect_pyrefly_slices(
    *,
    root: Path,
    target_file: str,
    target_line: int | None,
    target_col: int | None,
    target_name: str,
    feasible_slices: tuple[NeighborhoodSliceKind, ...],
    top_k: int,
    symbol_hint: str | None = None,
) -> tuple[tuple[NeighborhoodSliceV1, ...], tuple[DegradeEventV1, ...], dict[str, object]]:
    """Collect Pyrefly-derived LSP slices for feasible neighborhood kinds."""
    if not target_file or target_line is None:
        return (
            (),
            (
                DegradeEventV1(
                    stage="lsp.pyrefly",
                    severity="warning",
                    category="missing_anchor",
                    message="Pyrefly neighborhood slices require a resolved file:line target",
                ),
            ),
            {},
        )

    file_path = Path(target_file)
    if not file_path.is_absolute():
        file_path = root / file_path

    payload = enrich_with_pyrefly_lsp(
        PyreflyLspRequest(
            root=root,
            file_path=file_path,
            line=target_line,
            col=max(0, target_col or 0),
            symbol_hint=symbol_hint or target_name,
        )
    )
    if not isinstance(payload, Mapping):
        return (
            (),
            (
                DegradeEventV1(
                    stage="lsp.pyrefly",
                    severity="warning",
                    category="unavailable",
                    message="Pyrefly enrichment unavailable for resolved target",
                ),
            ),
            {},
        )

    subject_id = f"target.{target_file}:{target_name}"
    slices: list[NeighborhoodSliceV1] = []
    degrades: list[DegradeEventV1] = []

    for kind in feasible_slices:
        if kind == "references":
            references = _coerce_ref_rows(payload)
            slices.append(
                _build_slice_from_mappings(
                    kind="references",
                    title="References",
                    rows=references,
                    subject_id=subject_id,
                    edge_kind="references",
                    top_k=top_k,
                )
            )
            continue
        if kind == "implementations":
            impl_rows = _nested_rows(payload, ("symbol_grounding", "implementation_targets"))
            slices.append(
                _build_slice_from_mappings(
                    kind="implementations",
                    title="Implementations",
                    rows=impl_rows,
                    subject_id=subject_id,
                    edge_kind="implements",
                    top_k=top_k,
                )
            )
            continue
        if kind == "type_supertypes":
            rows = _string_rows(payload, ("class_method_context", "base_classes"))
            slices.append(
                _build_slice_from_mappings(
                    kind="type_supertypes",
                    title="Supertypes",
                    rows=rows,
                    subject_id=subject_id,
                    edge_kind="extends",
                    top_k=top_k,
                )
            )
            continue
        if kind == "type_subtypes":
            rows = _string_rows(payload, ("class_method_context", "overriding_methods"))
            slices.append(
                _build_slice_from_mappings(
                    kind="type_subtypes",
                    title="Subtypes",
                    rows=rows,
                    subject_id=subject_id,
                    edge_kind="overrides",
                    top_k=top_k,
                )
            )

    lsp_env = {
        "lsp_position_encoding": _nested_scalar(payload, ("coverage", "position_encoding")),
        "lsp_health": "ok",
        "lsp_quiescent": True,
    }
    if not slices:
        degrades.append(
            DegradeEventV1(
                stage="lsp.pyrefly",
                severity="info",
                category="no_signal",
                message="Pyrefly returned no neighborhood slice signals for this target",
            )
        )

    return tuple(slices), tuple(degrades), lsp_env


def _coerce_ref_rows(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
    rows = _nested_rows(payload, ("local_scope_context", "reference_locations"))
    normalized: list[Mapping[str, object]] = []
    for row in rows:
        file_value = row.get("file")
        line_value = row.get("line")
        col_value = row.get("col")
        if not isinstance(file_value, str):
            continue
        normalized.append(
            {
                "file": file_value,
                "line": line_value if isinstance(line_value, int) else 0,
                "col": col_value if isinstance(col_value, int) else 0,
                "kind": "reference",
                "symbol": file_value.split("/")[-1],
            }
        )
    return normalized


def _nested_rows(
    payload: Mapping[str, object],
    path: tuple[str, str],
) -> list[Mapping[str, object]]:
    container = payload.get(path[0])
    if not isinstance(container, Mapping):
        return []
    rows = container.get(path[1])
    if not isinstance(rows, list):
        return []
    return [cast("Mapping[str, object]", row) for row in rows if isinstance(row, Mapping)]


def _string_rows(
    payload: Mapping[str, object],
    path: tuple[str, str],
) -> list[Mapping[str, object]]:
    container = payload.get(path[0])
    if not isinstance(container, Mapping):
        return []
    rows = container.get(path[1])
    if not isinstance(rows, list):
        return []
    result: list[Mapping[str, object]] = []
    for item in rows:
        if not isinstance(item, str):
            continue
        result.append({"symbol": item, "kind": "type", "file": "", "line": 0, "col": 0})
    return result


def _build_slice_from_mappings(
    *,
    kind: NeighborhoodSliceKind,
    title: str,
    rows: list[Mapping[str, object]],
    subject_id: str,
    edge_kind: str,
    top_k: int,
) -> NeighborhoodSliceV1:
    total = len(rows)
    preview_rows = rows[: max(0, top_k)]

    preview: list[SemanticNodeRefV1] = []
    edges: list[SemanticEdgeV1] = []
    for row in preview_rows:
        node = _row_to_node(row)
        preview.append(node)
        edges.append(
            SemanticEdgeV1(
                edge_id=f"{subject_id}→{node.node_id}:{edge_kind}",
                source_node_id=subject_id,
                target_node_id=node.node_id,
                edge_kind=edge_kind,
                evidence_source="lsp.pyrefly",
            )
        )

    return NeighborhoodSliceV1(
        kind=kind,
        title=title,
        total=total,
        preview=tuple(preview),
        edges=tuple(edges),
        collapsed=True,
    )


def _row_to_node(row: Mapping[str, object]) -> SemanticNodeRefV1:
    symbol = row.get("symbol")
    if not isinstance(symbol, str) or not symbol:
        symbol = row.get("name")
    if not isinstance(symbol, str) or not symbol:
        symbol = "<unknown>"

    file_value = row.get("file")
    file_path = file_value if isinstance(file_value, str) else ""
    line = row.get("line")
    col = row.get("col")
    line_value = line if isinstance(line, int) else 0
    col_value = col if isinstance(col, int) else 0
    kind = row.get("kind")
    kind_value = kind if isinstance(kind, str) else "symbol"

    return SemanticNodeRefV1(
        node_id=f"lsp.pyrefly.{kind_value}.{file_path}:{line_value}:{col_value}:{symbol}",
        kind=kind_value,
        name=symbol,
        display_label=symbol,
        file_path=file_path,
    )


def _nested_scalar(payload: Mapping[str, object], path: tuple[str, str]) -> object | None:
    container = payload.get(path[0])
    if not isinstance(container, Mapping):
        return None
    return container.get(path[1])


__all__ = ["collect_pyrefly_slices"]
