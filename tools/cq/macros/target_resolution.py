"""Shared target resolution helpers for macro entry points."""

from __future__ import annotations

from pathlib import Path

from tools.cq.macros.scan_utils import iter_files


def resolve_target_files(
    *,
    root: Path,
    target: str,
    max_files: int,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    extensions: tuple[str, ...] = (".py",),
) -> list[Path]:
    """Resolve explicit path targets or symbol-hint file candidates."""
    target_path = Path(target)
    if target_path.exists() and target_path.is_file():
        return [target_path.resolve()]
    rooted_target = root / target
    if rooted_target.exists() and rooted_target.is_file():
        return [rooted_target]

    files: list[Path] = []
    for candidate in iter_files(
        root=root,
        include=include,
        exclude=exclude,
        extensions=extensions,
        max_files=max_files,
    ):
        try:
            source = candidate.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        if f"def {target}" in source or f"class {target}" in source:
            files.append(candidate)
    return files


__all__ = ["resolve_target_files"]
