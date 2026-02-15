"""Artifact saving for cq results."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import msgspec

from tools.cq.core.cache import (
    CacheWriteTagRequestV1,
    CqCacheBackend,
    build_cache_key,
    build_search_artifact_cache_key,
    build_search_artifact_index_key,
    default_cache_policy,
    get_cq_cache_backend,
    record_cache_decode_failure,
    record_cache_get,
    record_cache_set,
    resolve_namespace_ttl_seconds,
    resolve_write_cache_tag,
)
from tools.cq.core.cache.contracts import (
    SearchArtifactBundleV1,
    SearchArtifactIndexEntryV1,
    SearchArtifactIndexV1,
)
from tools.cq.core.codec import dumps_json_value
from tools.cq.core.contracts import contract_to_builtins
from tools.cq.core.diagnostics_contracts import build_diagnostics_artifact_payload
from tools.cq.core.schema import Artifact, CqResult
from tools.cq.core.serialization import to_builtins

DEFAULT_ARTIFACT_DIR = ".cq/artifacts"
_SEARCH_ARTIFACT_NAMESPACE = "search_artifacts"
_SEARCH_ARTIFACT_INDEX_LIMIT = 200


def _resolve_artifact_dir(result: CqResult, artifact_dir: str | Path | None) -> Path:
    if artifact_dir is None:
        return Path(result.run.root) / DEFAULT_ARTIFACT_DIR
    return Path(artifact_dir)


def _timestamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def _default_filename(result: CqResult, suffix: str) -> str:
    run_id = result.run.run_id or "no_run_id"
    return f"{result.run.macro}_{suffix}_{_timestamp()}_{run_id}.json"


def _write_json_artifact(
    *,
    result: CqResult,
    payload: object,
    artifact_dir: str | Path | None,
    filename: str,
) -> Artifact:
    target_dir = _resolve_artifact_dir(result, artifact_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    filepath = target_dir / filename
    with Path(filepath).open("w", encoding="utf-8") as handle:
        handle.write(dumps_json_value(to_builtins(payload), indent=2))
    try:
        rel_path = filepath.relative_to(result.run.root)
    except ValueError:
        rel_path = filepath
    return Artifact(path=str(rel_path), format="json")


def save_artifact_json(
    result: CqResult,
    artifact_dir: str | Path | None = None,
    filename: str | None = None,
) -> Artifact:
    """Save result as JSON artifact.

    Args:
        result: Result to save.
        artifact_dir: Destination directory. Defaults to `.cq/artifacts`.
        filename: Explicit filename. When omitted, a timestamped name is used.

    Returns:
        Reference to the saved artifact.
    """
    artifact_name = filename or _default_filename(result, "result")
    return _write_json_artifact(
        result=result,
        payload=result,
        artifact_dir=artifact_dir,
        filename=artifact_name,
    )


def save_diagnostics_artifact(
    result: CqResult,
    artifact_dir: str | Path | None = None,
    filename: str | None = None,
) -> Artifact | None:
    """Persist offloaded diagnostics summary payload for artifact-first rendering.

    Returns:
        Artifact reference when diagnostics payload exists, otherwise `None`.
    """
    payload = build_diagnostics_artifact_payload(result)
    if payload is None:
        return None
    artifact_name = filename or _default_filename(result, "diagnostics")
    return _write_json_artifact(
        result=result,
        payload=payload,
        artifact_dir=artifact_dir,
        filename=artifact_name,
    )


def save_neighborhood_overflow_artifact(
    result: CqResult,
    artifact_dir: str | Path | None = None,
    filename: str | None = None,
) -> Artifact | None:
    """Persist neighborhood overflow payload when insight preview is truncated.

    Returns:
        Artifact reference when overflow rows are present, otherwise `None`.
    """
    from tools.cq.core.front_door_insight import coerce_front_door_insight

    insight = coerce_front_door_insight(result.summary.get("front_door_insight"))
    if insight is None:
        return None

    slices = (
        ("callers", insight.neighborhood.callers),
        ("callees", insight.neighborhood.callees),
        ("references", insight.neighborhood.references),
        ("hierarchy_or_scope", insight.neighborhood.hierarchy_or_scope),
    )
    overflow_rows: list[dict[str, object]] = []
    for name, slice_payload in slices:
        preview_count = len(slice_payload.preview)
        if slice_payload.total <= preview_count:
            continue
        overflow_rows.append(
            {
                "slice": name,
                "total": slice_payload.total,
                "preview_count": preview_count,
                "source": slice_payload.source,
                "availability": slice_payload.availability,
                "preview": [to_builtins(node) for node in slice_payload.preview],
            }
        )
    if not overflow_rows:
        return None

    payload: Mapping[str, object] = {
        "target": to_builtins(insight.target),
        "budget": to_builtins(insight.budget),
        "overflow_slices": overflow_rows,
    }
    artifact_name = filename or _default_filename(result, "neighborhood_overflow")
    return _write_json_artifact(
        result=result,
        payload=payload,
        artifact_dir=artifact_dir,
        filename=artifact_name,
    )


def save_search_artifact_bundle_cache(
    result: CqResult,
    bundle: SearchArtifactBundleV1,
) -> Artifact | None:
    """Persist search artifact bundle in runtime cache and return artifact reference.

    Returns:
        Artifact | None: Reference when cache write succeeds, otherwise ``None``.
    """
    if result.run.macro != "search":
        return None

    root = Path(result.run.root)
    cache = get_cq_cache_backend(root=root)
    policy = default_cache_policy(root=root)
    ttl_seconds = resolve_namespace_ttl_seconds(policy=policy, namespace=_SEARCH_ARTIFACT_NAMESPACE)
    run_id = bundle.run_id or result.run.run_id or "no_run_id"
    cache_key = build_search_artifact_cache_key(
        workspace=str(root),
        run_id=run_id,
        query=bundle.query,
        macro=bundle.macro,
    )
    tag = resolve_write_cache_tag(
        CacheWriteTagRequestV1(
            policy=policy,
            workspace=str(root),
            language="auto",
            namespace=_SEARCH_ARTIFACT_NAMESPACE,
            run_id=run_id,
        )
    )
    ok = cache.set(
        cache_key,
        contract_to_builtins(bundle),
        expire=ttl_seconds,
        tag=tag,
    )
    record_cache_set(namespace=_SEARCH_ARTIFACT_NAMESPACE, ok=ok, key=cache_key)
    if not ok:
        return None

    created_ms = bundle.created_ms or result.run.started_ms
    entry = SearchArtifactIndexEntryV1(
        run_id=run_id,
        cache_key=cache_key,
        query=bundle.query,
        macro=bundle.macro,
        created_ms=float(created_ms),
    )
    _persist_search_artifact_index_entry(
        cache=cache,
        index_key=build_search_artifact_index_key(workspace=str(root), run_id=run_id),
        ttl_seconds=ttl_seconds,
        tag=tag,
        entry=entry,
    )
    _persist_search_artifact_index_entry(
        cache=cache,
        index_key=_global_search_artifact_index_key(workspace=str(root)),
        ttl_seconds=ttl_seconds,
        tag=tag,
        entry=entry,
    )
    return Artifact(path=f"cache://search_artifacts/{run_id}/{cache_key}", format="cache")


def list_search_artifact_index_entries(
    *,
    root: Path,
    run_id: str | None = None,
    limit: int = 50,
) -> list[SearchArtifactIndexEntryV1]:
    """List cached search artifact index entries for workspace scope.

    Returns:
        list[SearchArtifactIndexEntryV1]: Matching index entries.
    """
    cache = get_cq_cache_backend(root=root)
    index_key = (
        build_search_artifact_index_key(workspace=str(root), run_id=run_id)
        if run_id
        else _global_search_artifact_index_key(workspace=str(root))
    )
    index = _load_search_artifact_index(cache=cache, index_key=index_key)
    if index is None:
        return []
    normalized = sorted(index.entries, key=lambda row: row.created_ms, reverse=True)
    return normalized[: max(1, int(limit))]


def load_search_artifact_bundle(
    *,
    root: Path,
    run_id: str,
) -> tuple[SearchArtifactBundleV1 | None, SearchArtifactIndexEntryV1 | None]:
    """Load latest cached search artifact bundle for run_id.

    Returns:
        tuple[SearchArtifactBundleV1 | None, SearchArtifactIndexEntryV1 | None]:
            The bundle and index entry, or ``None`` entries when missing.
    """
    entries = list_search_artifact_index_entries(root=root, run_id=run_id, limit=1)
    if not entries:
        return None, None
    entry = entries[0]
    cache = get_cq_cache_backend(root=root)
    cached = cache.get(entry.cache_key)
    record_cache_get(
        namespace=_SEARCH_ARTIFACT_NAMESPACE,
        hit=isinstance(cached, dict),
        key=entry.cache_key,
    )
    if not isinstance(cached, dict):
        return None, entry
    try:
        payload = msgspec.convert(cached, type=SearchArtifactBundleV1)
    except (RuntimeError, TypeError, ValueError):
        record_cache_decode_failure(namespace=_SEARCH_ARTIFACT_NAMESPACE)
        return None, entry
    return payload, entry


def _persist_search_artifact_index_entry(
    *,
    cache: CqCacheBackend,
    index_key: str,
    ttl_seconds: int,
    tag: str | None,
    entry: SearchArtifactIndexEntryV1,
) -> None:
    current = _load_search_artifact_index(cache=cache, index_key=index_key)
    entries = [] if current is None else list(current.entries)
    entries = [row for row in entries if row.cache_key != entry.cache_key]
    entries.insert(0, entry)
    payload = SearchArtifactIndexV1(entries=entries[:_SEARCH_ARTIFACT_INDEX_LIMIT])
    ok = cache.set(
        index_key,
        contract_to_builtins(payload),
        expire=ttl_seconds,
        tag=tag,
    )
    record_cache_set(namespace=_SEARCH_ARTIFACT_NAMESPACE, ok=ok, key=index_key)


def _load_search_artifact_index(
    *,
    cache: CqCacheBackend,
    index_key: str,
) -> SearchArtifactIndexV1 | None:
    cached = cache.get(index_key)
    record_cache_get(
        namespace=_SEARCH_ARTIFACT_NAMESPACE,
        hit=isinstance(cached, dict),
        key=index_key,
    )
    if not isinstance(cached, dict):
        return None
    try:
        return msgspec.convert(cached, type=SearchArtifactIndexV1)
    except (RuntimeError, TypeError, ValueError):
        record_cache_decode_failure(namespace=_SEARCH_ARTIFACT_NAMESPACE)
        return None


def _global_search_artifact_index_key(*, workspace: str) -> str:
    return build_cache_key(
        _SEARCH_ARTIFACT_NAMESPACE,
        version="v1",
        workspace=workspace,
        language="auto",
        target="index:all",
        extras={"kind": "global_index"},
    )
