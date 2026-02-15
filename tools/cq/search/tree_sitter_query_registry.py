"""Query-pack discovery helpers for tree-sitter enrichment lanes."""

from __future__ import annotations

import hashlib
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend
from tools.cq.core.structs import CqStruct

if TYPE_CHECKING:
    from diskcache import FanoutCache

try:
    from diskcache import memoize_stampede
except ImportError:  # pragma: no cover - optional dependency
    memoize_stampede = None

_STAMP_TTL_SECONDS = 300
_STAMP_TAG = "ns:tree_sitter|kind:query_pack_load"

_STAMPED_LOADERS: dict[str, object] = {}


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


def _local_query_pack_hash(language: str) -> str:
    query_dir = _local_query_dir(language)
    if not query_dir.exists():
        return "none"
    entries: list[str] = []
    for query_path in sorted(query_dir.glob("*.scm")):
        try:
            stat = query_path.stat()
        except OSError:
            continue
        entries.append(f"{query_path.name}:{int(stat.st_size)}:{int(stat.st_mtime_ns)}")
    digest = hashlib.sha256("|".join(entries).encode("utf-8")).hexdigest()
    return digest[:24]


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


def _load_sources_uncached(
    *,
    language: str,
    include_distribution: bool,
) -> tuple[QueryPackSourceV1, ...]:
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


def _fanout_cache() -> FanoutCache | None:
    backend = get_cq_cache_backend(root=Path.cwd())
    cache = getattr(backend, "cache", None)
    if cache is None:
        return None
    try:
        _ = cache.get
        _ = cache.set
    except AttributeError:
        return None
    return cache


def _stamped_loader(language: str) -> object | None:
    cache = _fanout_cache()
    if cache is None or memoize_stampede is None:
        return None
    cached = _STAMPED_LOADERS.get(language)
    if cached is not None:
        return cached

    @memoize_stampede(cache, expire=_STAMP_TTL_SECONDS, tag=_STAMP_TAG)
    def _load(*, include_distribution: bool, local_hash: str) -> tuple[QueryPackSourceV1, ...]:
        _ = local_hash
        return _load_sources_uncached(
            language=language,
            include_distribution=include_distribution,
        )

    _STAMPED_LOADERS[language] = _load
    return _load


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
    loader = _stamped_loader(language)
    local_hash = _local_query_pack_hash(language)
    if callable(loader):
        try:
            loaded = loader(include_distribution=include_distribution, local_hash=local_hash)
        except (RuntimeError, TypeError, ValueError):
            loaded = _load_sources_uncached(
                language=language,
                include_distribution=include_distribution,
            )
        if isinstance(loaded, tuple):
            return loaded
        if isinstance(loaded, list):
            return tuple(row for row in loaded if isinstance(row, QueryPackSourceV1))
    return _load_sources_uncached(
        language=language,
        include_distribution=include_distribution,
    )


__all__ = [
    "QueryPackSourceV1",
    "load_query_pack_sources",
]
