"""Merged query utility helpers: resource paths, pack metadata, cache adapter."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

# -- Resource Paths -----------------------------------------------------------

_QUERY_ROOT = Path(__file__).resolve().parents[2] / "queries"


def query_pack_dir(language: str) -> Path:
    """Return the query pack directory for one language lane."""
    return _QUERY_ROOT / language


def query_pack_path(language: str, pack_name: str) -> Path:
    """Return the absolute path for one query pack source file."""
    return query_pack_dir(language) / pack_name


def query_contracts_path(language: str) -> Path:
    """Return the absolute path for one query contracts YAML file."""
    return query_pack_dir(language) / "contracts.yaml"


def diagnostics_query_path(language: str) -> Path:
    """Return the absolute path for one diagnostics query pack file."""
    return query_pack_dir(language) / "95_diagnostics.scm"


# -- Pack Metadata ------------------------------------------------------------


def pattern_settings(query: object, pattern_idx: int) -> dict[str, str]:
    """Return normalized string-only metadata for one pattern index."""
    pattern_settings_fn = getattr(query, "pattern_settings", None)
    if not callable(pattern_settings_fn):
        return {}
    settings = pattern_settings_fn(pattern_idx)
    if not isinstance(settings, Mapping):
        return {}
    out: dict[str, str] = {}
    for key, value in settings.items():
        if not isinstance(key, str):
            continue
        if value is None:
            continue
        out[key] = str(value)
    return out


def first_capture(capture_map: Mapping[str, Sequence[object]], capture_name: str) -> object | None:
    """Return first node for capture name when available."""
    nodes = capture_map.get(capture_name)
    if not isinstance(nodes, Sequence) or not nodes:
        return None
    return nodes[0]


# -- Cache Adapter ------------------------------------------------------------


def query_registry_cache(*, root: Path | None = None) -> object | None:
    """Return optional query-registry cache capability.

    Query-pack stampede memoization requires backend-native cache primitives.
    The cache protocol intentionally does not expose those internals, so this
    helper degrades to ``None`` and callers operate in uncached mode.
    """
    _ = root
    return None


__all__ = [
    "diagnostics_query_path",
    "first_capture",
    "pattern_settings",
    "query_contracts_path",
    "query_pack_dir",
    "query_pack_path",
    "query_registry_cache",
]
