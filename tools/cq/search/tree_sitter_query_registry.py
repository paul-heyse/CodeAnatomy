"""Query-pack discovery helpers for tree-sitter enrichment lanes."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path

from tools.cq.core.structs import CqStruct


class QueryPackSourceV1(CqStruct, frozen=True):
    """One query-pack source unit."""

    language: str
    pack_name: str
    source: str
    source_path: str | None = None


def _local_query_dir(language: str) -> Path:
    return Path(__file__).with_suffix("").parent / "queries" / language


def _load_local_query_sources(language: str) -> list[QueryPackSourceV1]:
    query_dir = _local_query_dir(language)
    if not query_dir.exists():
        return []
    return [
        QueryPackSourceV1(
            language=language,
            pack_name=query_path.name,
            source=query_path.read_text(encoding="utf-8"),
            source_path=str(query_path),
        )
        for query_path in sorted(query_dir.glob("*.scm"))
    ]


def _load_rust_distribution_queries() -> list[QueryPackSourceV1]:
    try:
        dist = distribution("tree-sitter-rust")
    except PackageNotFoundError:
        return []

    out: list[QueryPackSourceV1] = []
    for item in sorted(dist.files or (), key=str):
        item_text = str(item)
        if not item_text.endswith(".scm") or "/queries/" not in item_text:
            continue
        source_path = dist.locate_file(item)
        try:
            source = source_path.read_text(encoding="utf-8")
        except OSError:
            continue
        out.append(
            QueryPackSourceV1(
                language="rust",
                pack_name=Path(item_text).name,
                source=source,
                source_path=str(source_path),
            )
        )
    return out


def load_query_pack_sources(
    language: str,
    *,
    include_distribution: bool = True,
) -> tuple[QueryPackSourceV1, ...]:
    """Load query-pack sources for one language.

    Local repository packs are preferred and returned first. For Rust, upstream
    distribution query packs are appended when not already present locally.

    Returns:
    -------
    tuple[QueryPackSourceV1, ...]
        Ordered local-first query-pack sources for the selected language.
    """
    local_sources = _load_local_query_sources(language)
    if language != "rust" or not include_distribution:
        return tuple(local_sources)

    seen = {source.pack_name for source in local_sources}
    combined = list(local_sources)
    for source in _load_rust_distribution_queries():
        if source.pack_name in seen:
            continue
        seen.add(source.pack_name)
        combined.append(source)
    return tuple(combined)


__all__ = [
    "QueryPackSourceV1",
    "load_query_pack_sources",
]
