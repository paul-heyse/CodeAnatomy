"""Canonical extraction option contracts and normalization helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

import msgspec

from utils.value_coercion import coerce_bool


class RepoScanDiffOptions(msgspec.Struct, frozen=True):
    """Diff-selection controls for repository scanning."""

    diff_base_ref: str | None = None
    diff_head_ref: str | None = None
    changed_only: bool = False


class ExtractionRunOptions(msgspec.Struct, frozen=True):
    """Normalized extraction options consumed by the extraction orchestrator."""

    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    include_untracked: bool = True
    include_submodules: bool = False
    include_worktrees: bool = False
    follow_symlinks: bool = False
    tree_sitter_enabled: bool = True
    max_workers: int = 6
    diff_base_ref: str | None = None
    diff_head_ref: str | None = None
    changed_only: bool = False
    materialization_mode: str = "delta"

    @property
    def repo_scan_diff(self) -> RepoScanDiffOptions:
        """Return repo scan diff controls as a typed sub-struct."""
        return RepoScanDiffOptions(
            diff_base_ref=self.diff_base_ref,
            diff_head_ref=self.diff_head_ref,
            changed_only=self.changed_only,
        )


def normalize_extraction_options(
    options: ExtractionRunOptions | Mapping[str, object] | None,
    *,
    default_tree_sitter_enabled: bool = True,
    default_max_workers: int = 6,
) -> ExtractionRunOptions:
    """Normalize extraction options from typed or mapping payloads.

    Compatibility keys accepted during migration:
    - ``enable_tree_sitter`` -> ``tree_sitter_enabled``
    - top-level ``git_base_ref`` / ``git_head_ref`` / ``git_changed_only``
    - nested ``incremental_config.git_*``

    Returns:
        Normalized extraction options with canonical keys and defaults.

    Raises:
        TypeError: If ``options`` is not ``None``, ``ExtractionRunOptions``, or a mapping.
    """
    if isinstance(options, ExtractionRunOptions):
        _validate_materialization_mode(options.materialization_mode)
        _validate_diff_options(
            changed_only=options.changed_only,
            diff_base_ref=options.diff_base_ref,
            diff_head_ref=options.diff_head_ref,
        )
        return options

    if options is None:
        return ExtractionRunOptions(
            tree_sitter_enabled=default_tree_sitter_enabled,
            max_workers=default_max_workers,
        )

    if not isinstance(options, Mapping):
        msg = "Extraction options must be a mapping, ExtractionRunOptions, or None."
        raise TypeError(msg)

    include_globs = _coerce_globs(options.get("include_globs"))
    exclude_globs = _coerce_globs(options.get("exclude_globs"))

    tree_sitter_enabled = _coerce_bool_with_default(
        options.get("tree_sitter_enabled"),
        default=default_tree_sitter_enabled,
        field_name="tree_sitter_enabled",
    )
    if "tree_sitter_enabled" not in options:
        tree_sitter_enabled = _coerce_bool_with_default(
            options.get("enable_tree_sitter"),
            default=default_tree_sitter_enabled,
            field_name="enable_tree_sitter",
        )

    max_workers = _coerce_positive_int(options.get("max_workers"), default=default_max_workers)

    incremental_diff = _coerce_incremental_diff(options.get("incremental_config"))

    diff_base_ref = (
        _coerce_optional_ref(options.get("diff_base_ref"))
        or _coerce_optional_ref(options.get("git_base_ref"))
        or incremental_diff.diff_base_ref
    )
    diff_head_ref = (
        _coerce_optional_ref(options.get("diff_head_ref"))
        or _coerce_optional_ref(options.get("git_head_ref"))
        or incremental_diff.diff_head_ref
    )
    changed_only = _resolve_changed_only(options, incremental_default=incremental_diff.changed_only)
    materialization_mode = _coerce_materialization_mode(
        options.get("materialization_mode"),
        default="delta",
    )

    _validate_diff_options(
        changed_only=changed_only,
        diff_base_ref=diff_base_ref,
        diff_head_ref=diff_head_ref,
    )
    _validate_materialization_mode(materialization_mode)

    return ExtractionRunOptions(
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        include_untracked=_coerce_bool_with_default(
            options.get("include_untracked"),
            default=True,
            field_name="include_untracked",
        ),
        include_submodules=_coerce_bool_with_default(
            options.get("include_submodules"),
            default=False,
            field_name="include_submodules",
        ),
        include_worktrees=_coerce_bool_with_default(
            options.get("include_worktrees"),
            default=False,
            field_name="include_worktrees",
        ),
        follow_symlinks=_coerce_bool_with_default(
            options.get("follow_symlinks"),
            default=False,
            field_name="follow_symlinks",
        ),
        tree_sitter_enabled=tree_sitter_enabled,
        max_workers=max_workers,
        diff_base_ref=diff_base_ref,
        diff_head_ref=diff_head_ref,
        changed_only=changed_only,
        materialization_mode=materialization_mode,
    )


def _coerce_incremental_diff(value: object) -> RepoScanDiffOptions:
    if value is None:
        return RepoScanDiffOptions()
    if isinstance(value, Mapping):
        return RepoScanDiffOptions(
            diff_base_ref=_coerce_optional_ref(value.get("git_base_ref")),
            diff_head_ref=_coerce_optional_ref(value.get("git_head_ref")),
            changed_only=_coerce_bool_with_default(
                value.get("git_changed_only"),
                default=False,
                field_name="git_changed_only",
            ),
        )
    return RepoScanDiffOptions(
        diff_base_ref=_coerce_optional_ref(getattr(value, "git_base_ref", None)),
        diff_head_ref=_coerce_optional_ref(getattr(value, "git_head_ref", None)),
        changed_only=_coerce_bool_with_default(
            getattr(value, "git_changed_only", None),
            default=False,
            field_name="git_changed_only",
        ),
    )


def _resolve_changed_only(
    options: Mapping[str, object],
    *,
    incremental_default: bool,
) -> bool:
    if isinstance(options.get("changed_only"), bool):
        return bool(options.get("changed_only"))
    if isinstance(options.get("git_changed_only"), bool):
        return bool(options.get("git_changed_only"))
    return incremental_default


def _validate_diff_options(
    *,
    changed_only: bool,
    diff_base_ref: str | None,
    diff_head_ref: str | None,
) -> None:
    if changed_only and (diff_base_ref is None or diff_head_ref is None):
        msg = "changed_only=True requires both diff_base_ref and diff_head_ref."
        raise ValueError(msg)


def _validate_materialization_mode(mode: str) -> None:
    from datafusion_engine.io.delta_write_handler import (
        resolve_extraction_materialization_mode,
    )

    _ = resolve_extraction_materialization_mode(mode)


def _coerce_globs(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        return tuple(str(item) for item in value if isinstance(item, (str, Path)))
    return ()


def _coerce_optional_ref(value: object) -> str | None:
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    return None


def _coerce_positive_int(value: object, *, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, float) and value > 0:
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if raw:
            try:
                parsed = int(raw)
            except ValueError:
                return default
            if parsed > 0:
                return parsed
    return default


def _coerce_materialization_mode(value: object, *, default: str) -> str:
    if isinstance(value, str):
        cleaned = value.strip().lower()
        if cleaned:
            return cleaned
    return default


def _coerce_bool_with_default(value: object, *, default: bool, field_name: str) -> bool:
    coerced = coerce_bool(value, default=default, label=field_name)
    if coerced is None:
        msg = f"{field_name} could not be coerced to bool."
        raise TypeError(msg)
    return coerced


__all__ = [
    "ExtractionRunOptions",
    "RepoScanDiffOptions",
    "normalize_extraction_options",
]
