"""Deterministic cache key builders for CQ runtime artifacts."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence

import msgspec

_HASH_LIKE_RE = re.compile(r"^[0-9a-fA-F]{8,128}$")
_MIN_HASH_ATOM_SIZE = 8
_DEFAULT_TAG_HASH_SIZE = 24


def _digest_text(value: str, *, size: int = 16) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return digest[: max(8, int(size))]


def _normalize_hash_atom(value: str | None, *, size: int = _DEFAULT_TAG_HASH_SIZE) -> str | None:
    if value is None:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    width = max(_MIN_HASH_ATOM_SIZE, int(size))
    if _HASH_LIKE_RE.fullmatch(candidate):
        return candidate.lower()[:width]
    return _digest_text(candidate, size=width)


def _compose_tag(
    *,
    workspace: str,
    language: str,
    namespace: str | None = None,
    scope_hash: str | None = None,
    snapshot: str | None = None,
    run_id: str | None = None,
) -> str:
    parts = [
        f"ws:{_digest_text(workspace)}",
        f"lang:{language.strip().lower()}",
    ]
    if namespace:
        parts.append(f"ns:{namespace.strip()}")
    normalized_scope = _normalize_hash_atom(scope_hash)
    if normalized_scope:
        parts.append(f"scope:{normalized_scope}")
    normalized_snapshot = _normalize_hash_atom(snapshot)
    if normalized_snapshot:
        parts.append(f"snap:{normalized_snapshot}")
    if run_id:
        parts.append(f"run:{_digest_text(run_id, size=_DEFAULT_TAG_HASH_SIZE)}")
    return "|".join(parts)


def _canonicalize(value: object) -> object:
    if isinstance(value, Mapping):
        items = sorted((str(key), _canonicalize(val)) for key, val in value.items())
        return dict(items)
    if isinstance(value, (set, frozenset)):
        normalized = [_canonicalize(item) for item in value]
        return sorted(normalized, key=msgspec.json.encode)
    if isinstance(value, tuple):
        return tuple(_canonicalize(item) for item in value)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_canonicalize(item) for item in value]
    return value


def canonicalize_cache_payload(payload: Mapping[str, object]) -> dict[str, object]:
    """Canonicalize payload data before digest generation.

    Returns:
        Deterministically ordered payload converted to built-in containers.
    """
    normalized = _canonicalize(dict(payload))
    if isinstance(normalized, dict):
        return normalized
    return {}


def build_cache_key(
    namespace: str,
    *,
    version: str,
    workspace: str,
    language: str,
    target: str,
    extras: dict[str, object] | None = None,
) -> str:
    """Build a deterministic cache key string for CQ runtime data.

    Returns:
        Namespaced cache key that includes a stable payload digest.
    """
    payload = canonicalize_cache_payload(
        {
            "namespace": namespace,
            "version": version,
            "workspace": workspace,
            "language": language,
            "target": target,
            "extras": extras or {},
        }
    )
    digest = hashlib.sha256(msgspec.json.encode(payload)).hexdigest()
    return f"cq:{namespace}:{version}:{digest}"


def build_cache_tag(*, workspace: str, language: str) -> str:
    """Build tag used for bulk invalidation.

    Returns:
        Workspace-language cache tag.
    """
    return _compose_tag(workspace=workspace, language=language)


def build_run_cache_tag(*, workspace: str, language: str, run_id: str) -> str:
    """Build run-scoped cache invalidation tag.

    Returns:
        Run-scoped cache tag.
    """
    return _compose_tag(
        workspace=workspace,
        language=language,
        run_id=run_id,
    )


def build_scope_hash(scope_payload: Mapping[str, object] | None = None) -> str | None:
    """Build a stable scope hash from scope-identifying payload.

    Returns:
        Truncated digest when scope payload is provided, otherwise ``None``.
    """
    if not scope_payload:
        return None
    payload = canonicalize_cache_payload(scope_payload)
    return hashlib.sha256(msgspec.json.encode(payload)).hexdigest()[:24]


def build_namespace_cache_tag(
    *,
    workspace: str,
    language: str,
    namespace: str,
    scope_hash: str | None = None,
    snapshot: str | None = None,
    run_id: str | None = None,
) -> str:
    """Build namespace-oriented cache invalidation tag.

    Returns:
        Canonically ordered namespace cache tag.
    """
    return _compose_tag(
        workspace=workspace,
        language=language,
        namespace=namespace,
        scope_hash=scope_hash,
        snapshot=snapshot,
        run_id=run_id,
    )


__all__ = [
    "build_cache_key",
    "build_cache_tag",
    "build_namespace_cache_tag",
    "build_run_cache_tag",
    "build_scope_hash",
    "canonicalize_cache_payload",
]
