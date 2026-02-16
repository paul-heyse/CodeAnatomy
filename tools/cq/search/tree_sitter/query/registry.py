"""Query-pack discovery helpers for tree-sitter enrichment lanes."""

from __future__ import annotations

import hashlib
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter.contracts.query_models import GrammarDriftReportV1
from tools.cq.search.tree_sitter.query.drift import build_grammar_drift_report
from tools.cq.search.tree_sitter.query.support import query_pack_dir, query_registry_cache

if TYPE_CHECKING:
    from diskcache import FanoutCache

try:
    from diskcache import memoize_stampede
except ImportError:  # pragma: no cover - optional dependency
    memoize_stampede = None

_STAMP_TTL_SECONDS = 300
_STAMP_TAG = "ns:tree_sitter|kind:query_pack_load"
_DISTRIBUTION_PACKAGES: dict[str, str] = {
    "python": "tree-sitter-python",
    "rust": "tree-sitter-rust",
}


class QueryPackSourceV1(CqStruct, frozen=True):
    """One query-pack source unit."""

    language: str
    pack_name: str
    source: str
    source_path: str | None = None


class QueryPackProfileV1(CqStruct, frozen=True):
    """Profile describing query-pack load behavior for one lane."""

    profile_name: str
    include_distribution: bool = False
    required_pack_names: tuple[str, ...] = ()


def _local_query_dir(language: str) -> Path:
    return query_pack_dir(language)


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


def _load_distribution_queries(language: str) -> list[QueryPackSourceV1]:
    package_name = _DISTRIBUTION_PACKAGES.get(language)
    if package_name is None:
        return []
    try:
        dist = distribution(package_name)
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
                language=language,
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
    normalized = language.strip().lower()
    local_sources = _load_local_query_sources(normalized)
    if not include_distribution:
        return tuple(local_sources)

    seen = {source.pack_name for source in local_sources}
    combined = list(local_sources)
    for source in _load_distribution_queries(normalized):
        if source.pack_name in seen:
            continue
        seen.add(source.pack_name)
        combined.append(source)
    return tuple(combined)


def load_distribution_query_source(language: str, pack_name: str) -> str | None:
    """Load one distribution query-pack source for a language by pack name.

    Returns:
        str | None: Function return value.
    """
    normalized = language.strip().lower()
    for row in _load_distribution_queries(normalized):
        if row.pack_name == pack_name:
            return row.source
    return None


def _fanout_cache() -> FanoutCache | None:
    return query_registry_cache(root=Path.cwd())


def _stamped_loader(language: str) -> object | None:
    cache = _fanout_cache()
    if cache is None or memoize_stampede is None:
        return None

    from tools.cq.search.tree_sitter.core.adaptive_runtime import memoized_value

    @memoize_stampede(cache, expire=_STAMP_TTL_SECONDS, tag=_STAMP_TAG)
    def _load(*, include_distribution: bool, local_hash: str) -> tuple[QueryPackSourceV1, ...]:
        cache_key = f"ts_query_registry:{language}:{int(include_distribution)}:{local_hash}"
        return memoized_value(
            key=cache_key,
            compute=lambda: _load_sources_uncached(
                language=language,
                include_distribution=include_distribution,
            ),
            ttl_seconds=_STAMP_TTL_SECONDS,
        )

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
    normalized = language.strip().lower()
    loader = _stamped_loader(normalized)
    local_hash = _local_query_pack_hash(normalized)
    loaded_rows: tuple[QueryPackSourceV1, ...]
    if callable(loader):
        try:
            loaded = loader(include_distribution=include_distribution, local_hash=local_hash)
        except (RuntimeError, TypeError, ValueError):
            loaded = _load_sources_uncached(
                language=normalized,
                include_distribution=include_distribution,
            )
        if isinstance(loaded, tuple):
            loaded_rows = loaded
        elif isinstance(loaded, list):
            loaded_rows = tuple(row for row in loaded if isinstance(row, QueryPackSourceV1))
        else:
            loaded_rows = _load_sources_uncached(
                language=normalized,
                include_distribution=include_distribution,
            )
    else:
        loaded_rows = _load_sources_uncached(
            language=normalized,
            include_distribution=include_distribution,
        )
    from tools.cq.search.tree_sitter.query.runtime_state import get_query_runtime_state

    runtime_state = get_query_runtime_state()
    report = build_grammar_drift_report(language=normalized, query_sources=loaded_rows)
    runtime_state.last_drift_reports[normalized] = report
    if not report.compatible and include_distribution:
        fallback_rows = _load_sources_uncached(language=normalized, include_distribution=False)
        runtime_state.last_drift_reports[normalized] = build_grammar_drift_report(
            language=normalized,
            query_sources=fallback_rows,
        )
        return fallback_rows
    return loaded_rows


def load_query_pack_sources_for_profile(
    language: str,
    *,
    profile: QueryPackProfileV1,
) -> tuple[QueryPackSourceV1, ...]:
    """Load query-pack sources according to a profile contract.

    Returns:
        tuple[QueryPackSourceV1, ...]: Function return value.
    """
    rows = load_query_pack_sources(
        language,
        include_distribution=profile.include_distribution,
    )
    required = set(profile.required_pack_names)
    if not required:
        return rows
    present = {row.pack_name for row in rows}
    if required.issubset(present):
        return rows
    return ()


def get_last_grammar_drift_report(language: str) -> GrammarDriftReportV1 | None:
    """Return latest grammar drift report for a language load lane."""
    from tools.cq.search.tree_sitter.query.runtime_state import get_query_runtime_state

    return get_query_runtime_state().last_drift_reports.get(language.strip().lower())


__all__ = [
    "QueryPackProfileV1",
    "QueryPackSourceV1",
    "get_last_grammar_drift_report",
    "load_distribution_query_source",
    "load_query_pack_sources",
    "load_query_pack_sources_for_profile",
]
