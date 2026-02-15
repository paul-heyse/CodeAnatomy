"""Rust grammar bundle helpers (manifest, node schema, query packs)."""

from __future__ import annotations

import json
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter.query.grammar_drift import build_grammar_drift_report
from tools.cq.search.tree_sitter.query.registry import (
    QueryPackSourceV1,
    load_query_pack_sources,
)


class RustGrammarBundleV1(CqStruct, frozen=True):
    """Resolved Rust grammar bundle metadata."""

    distribution: str = "tree-sitter-rust"
    manifest_path: str | None = None
    node_types_path: str | None = None
    query_pack_names: tuple[str, ...] = ()
    drift_compatible: bool = True
    drift_errors: tuple[str, ...] = ()


class RustManifestV1(CqStruct, frozen=True):
    """Subset of `tree-sitter.json` used by CQ."""

    scope: str | None = None
    file_types: tuple[str, ...] = ()
    highlights: tuple[str, ...] = ()
    injections: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()


def _distribution_file_path(suffix: str) -> Path | None:
    """Resolve a distribution asset path by suffix.

    Returns:
    -------
    Path | None
        Located asset path when present in installed distribution.
    """
    try:
        dist = distribution("tree-sitter-rust")
    except PackageNotFoundError:
        return None
    for item in dist.files or ():
        item_text = str(item)
        if item_text.endswith(suffix):
            return Path(str(dist.locate_file(item)))
    return None


def load_rust_manifest() -> RustManifestV1 | None:
    """Load Rust grammar manifest from installed distribution when available.

    Returns:
    -------
    RustManifestV1 | None
        Normalized manifest fields when distribution assets are present.
    """
    manifest_path = _distribution_file_path("tree-sitter.json")
    if manifest_path is None or not manifest_path.exists():
        return None
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, RuntimeError, TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None

    grammar = payload.get("grammars")
    if not isinstance(grammar, list) or not grammar:
        return None
    first = grammar[0] if isinstance(grammar[0], dict) else {}

    def _tuple_values(key: str) -> tuple[str, ...]:
        value = first.get(key)
        if not isinstance(value, list):
            return ()
        return tuple(item for item in value if isinstance(item, str))

    file_types = first.get("file-types")
    normalized_file_types = (
        tuple(item for item in file_types if isinstance(item, str))
        if isinstance(file_types, list)
        else ()
    )
    scope = first.get("scope")
    return RustManifestV1(
        scope=scope if isinstance(scope, str) else None,
        file_types=normalized_file_types,
        highlights=_tuple_values("highlights"),
        injections=_tuple_values("injections"),
        tags=_tuple_values("tags"),
    )


def load_rust_grammar_bundle(
    *,
    include_distribution_queries: bool = True,
) -> RustGrammarBundleV1:
    """Resolve Rust grammar bundle metadata and query pack names.

    Returns:
    -------
    RustGrammarBundleV1
        Bundle metadata for grammar assets and query packs.
    """
    manifest_path = _distribution_file_path("tree-sitter.json")
    node_types_path = _distribution_file_path("node-types.json")
    query_sources = load_query_pack_sources(
        "rust",
        include_distribution=include_distribution_queries,
    )
    drift_report = build_grammar_drift_report(language="rust", query_sources=query_sources)
    return RustGrammarBundleV1(
        manifest_path=str(manifest_path) if manifest_path is not None else None,
        node_types_path=str(node_types_path) if node_types_path is not None else None,
        query_pack_names=tuple(source.pack_name for source in query_sources),
        drift_compatible=drift_report.compatible,
        drift_errors=drift_report.errors,
    )


def load_rust_query_sources(
    *,
    include_distribution_queries: bool = True,
) -> tuple[QueryPackSourceV1, ...]:
    """Load Rust query-pack source rows for extractor lanes.

    Returns:
    -------
    tuple[QueryPackSourceV1, ...]
        Rust query pack sources in load order.
    """
    return load_query_pack_sources("rust", include_distribution=include_distribution_queries)


__all__ = [
    "RustGrammarBundleV1",
    "RustManifestV1",
    "load_rust_grammar_bundle",
    "load_rust_manifest",
    "load_rust_query_sources",
]
