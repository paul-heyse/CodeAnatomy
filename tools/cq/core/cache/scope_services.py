"""Shared scope/inventory/snapshot resolution services."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path

import msgspec

from tools.cq.core.cache.key_builder import build_scope_hash
from tools.cq.core.cache.snapshot_fingerprint import build_scope_snapshot_fingerprint
from tools.cq.core.structs import CqStruct


class ScopePlanV1(CqStruct, frozen=True):
    """Serializable scope resolution request contract."""

    root: str
    paths: tuple[str, ...]
    globs: tuple[str, ...] = ()
    language: str = "python"
    include_inventory_token: bool = True


class ScopeResolutionV1(CqStruct, frozen=True):
    """Scope resolution result with deterministic snapshot data."""

    root: str
    language: str
    files: tuple[str, ...]
    scope_hash: str | None = None
    snapshot_digest: str = ""
    inventory_token: dict[str, object] = msgspec.field(default_factory=dict)


ListFilesFn = Callable[[list[Path], Path, list[str] | None, str], list[Path]]
InventoryFn = Callable[[Path], Mapping[str, object]]


def resolve_scope(
    plan: ScopePlanV1,
    *,
    list_files: ListFilesFn,
    inventory_token_fn: InventoryFn | None = None,
) -> ScopeResolutionV1:
    """Resolve deterministic scope files and snapshot metadata.

    Returns:
        ScopeResolutionV1: Deterministic scope metadata contract.
    """
    resolved_root = Path(plan.root).resolve()
    resolved_paths = _resolve_scope_paths(root=resolved_root, paths=plan.paths)
    globs = list(plan.globs)
    files = sorted(
        list_files(resolved_paths, resolved_root, globs or None, plan.language),
        key=lambda item: item.as_posix(),
    )
    scope_roots = _scope_roots(root=resolved_root, paths=resolved_paths)
    token = (
        dict(inventory_token_fn(resolved_root))
        if (plan.include_inventory_token and inventory_token_fn is not None)
        else {}
    )
    scope_hash = build_scope_hash(
        {
            "scope_roots": scope_roots,
            "scope_globs": tuple(globs),
            "language": plan.language,
            "inventory_token": token,
        }
    )
    snapshot = build_scope_snapshot_fingerprint(
        root=resolved_root,
        files=files,
        language=plan.language,
        scope_globs=globs,
        scope_roots=[Path(item) for item in scope_roots],
        inventory_token=token,
    )
    return ScopeResolutionV1(
        root=str(resolved_root),
        language=plan.language,
        files=tuple(_to_rel_path(root=resolved_root, path=item) for item in files),
        scope_hash=scope_hash,
        snapshot_digest=snapshot.digest,
        inventory_token=token,
    )


def _resolve_scope_paths(*, root: Path, paths: tuple[str, ...]) -> list[Path]:
    output: list[Path] = []
    for raw in paths:
        path = Path(raw)
        output.append(path if path.is_absolute() else root / path)
    return output or [root]


def _scope_roots(*, root: Path, paths: list[Path]) -> tuple[str, ...]:
    roots = sorted({str(path.resolve()) for path in paths if path.exists()})
    if roots:
        return tuple(roots)
    return (str(root.resolve()),)


def _to_rel_path(*, root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


__all__ = [
    "ScopePlanV1",
    "ScopeResolutionV1",
    "resolve_scope",
]
