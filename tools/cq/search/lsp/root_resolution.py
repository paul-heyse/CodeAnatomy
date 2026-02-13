"""Workspace root resolution for language-specific LSP providers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.query.language import QueryLanguage

_PYTHON_ROOT_MARKERS = ("pyproject.toml", "setup.cfg", "setup.py")
_RUST_ROOT_MARKERS = ("Cargo.toml",)


def _is_within(candidate: Path, root: Path) -> bool:
    try:
        candidate.relative_to(root)
    except ValueError:
        return False
    return True


def _normalize_target_path(command_root: Path, file_path: Path) -> Path:
    candidate = file_path if file_path.is_absolute() else command_root / file_path
    with_resolve_fallback = candidate.resolve()
    return with_resolve_fallback if with_resolve_fallback.exists() else candidate


def _nearest_workspace_root(
    *,
    command_root: Path,
    target_path: Path,
    markers: tuple[str, ...],
) -> Path | None:
    current = target_path if target_path.is_dir() else target_path.parent
    while True:
        if _is_within(current, command_root) and any((current / marker).exists() for marker in markers):
            return current
        if current == command_root:
            return None
        parent = current.parent
        if parent == current:
            return None
        current = parent


def resolve_lsp_provider_root(
    *,
    language: QueryLanguage,
    command_root: Path,
    file_path: Path,
) -> Path:
    """Resolve the effective workspace root for an LSP request.

    Returns:
    -------
    Path
        Nearest language workspace root for ``file_path`` within ``command_root``,
        or ``command_root`` when no language marker is found.
    """
    normalized_command_root = command_root.resolve()
    target_path = _normalize_target_path(normalized_command_root, file_path)
    markers = _PYTHON_ROOT_MARKERS if language == "python" else _RUST_ROOT_MARKERS
    workspace_root = _nearest_workspace_root(
        command_root=normalized_command_root,
        target_path=target_path,
        markers=markers,
    )
    return workspace_root or normalized_command_root


__all__ = [
    "resolve_lsp_provider_root",
]
