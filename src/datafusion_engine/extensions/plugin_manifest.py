"""Resolve DataFusion plugin manifest payloads across wheel/repo layouts."""

from __future__ import annotations

import importlib
import os
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_PLUGIN_BASENAME = "df_plugin_codeanatomy"
_PLUGIN_ENV_VAR = "CODEANATOMY_DF_PLUGIN_PATH"


@dataclass(frozen=True)
class PluginManifestResolution:
    """Resolved plugin manifest payload and diagnostics."""

    manifest: Mapping[str, object] | None
    error: str | None


def resolve_plugin_manifest(module_name: str = "datafusion_ext") -> PluginManifestResolution:
    """Resolve plugin manifest payload from the extension module.

    Returns
    -------
    PluginManifestResolution
        Resolved manifest payload with best-effort diagnostics when unavailable.
    """
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        return PluginManifestResolution(manifest=None, error=str(exc))

    manifest_fn = getattr(module, "plugin_manifest", None)
    if not callable(manifest_fn):
        msg = f"{module_name}.plugin_manifest() is unavailable."
        return PluginManifestResolution(manifest=None, error=msg)

    errors: list[str] = []
    manifest, error = _call_plugin_manifest(manifest_fn, path=None)
    if manifest is not None:
        return PluginManifestResolution(manifest=manifest, error=None)
    if error is not None:
        errors.append(error)

    for candidate in _plugin_path_candidates(module):
        manifest, error = _call_plugin_manifest(manifest_fn, path=str(candidate))
        if manifest is not None:
            return PluginManifestResolution(manifest=manifest, error=None)
        if error is not None:
            errors.append(f"{candidate}: {error}")

    detail = "; ".join(_dedupe(errors)) if errors else "plugin manifest is unavailable."
    return PluginManifestResolution(manifest=None, error=detail)


def _call_plugin_manifest(
    manifest_fn: Callable[..., object],
    *,
    path: str | None,
) -> tuple[dict[str, object] | None, str | None]:
    try:
        payload = manifest_fn() if path is None else manifest_fn(path)
    except (RuntimeError, TypeError, ValueError, OSError) as exc:
        return None, str(exc)
    if not isinstance(payload, Mapping):
        return None, "plugin_manifest() returned a non-mapping payload."
    manifest = dict(payload)
    plugin_path = manifest.get("plugin_path")
    if not isinstance(plugin_path, str) or not plugin_path:
        return None, "plugin_manifest() payload is missing a plugin_path string."
    if not Path(plugin_path).expanduser().exists():
        return None, f"plugin_manifest() plugin_path does not exist: {plugin_path!r}"
    return manifest, None


def _plugin_path_candidates(module: Any) -> tuple[Path, ...]:
    candidates: list[Path] = []
    filename = _plugin_library_filename()

    env_path = os.environ.get(_PLUGIN_ENV_VAR)
    if env_path:
        candidates.append(Path(env_path).expanduser())

    plugin_path_fn = getattr(module, "plugin_library_path", None)
    if callable(plugin_path_fn):
        try:
            value = plugin_path_fn()
        except (RuntimeError, TypeError, ValueError, OSError):
            value = None
        if isinstance(value, str) and value:
            candidates.append(Path(value).expanduser())

    module_file = getattr(module, "__file__", None)
    if isinstance(module_file, str) and module_file:
        base = Path(module_file).resolve().parent
        candidates.append(base / "plugin" / filename)
        candidates.append(base / filename)

    root = _repo_root()
    candidates.append(root / "rust" / "datafusion_ext_py" / "plugin" / filename)
    candidates.append(root / "rust" / "target" / "release" / filename)
    candidates.append(root / "rust" / "target" / "debug" / filename)

    existing: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        resolved = candidate.expanduser().resolve(strict=False)
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        if resolved.exists():
            existing.append(resolved)
    return tuple(existing)


def _plugin_library_filename() -> str:
    if os.name == "nt":
        return f"{_PLUGIN_BASENAME}.dll"
    if sys.platform == "darwin":
        return f"lib{_PLUGIN_BASENAME}.dylib"
    return f"lib{_PLUGIN_BASENAME}.so"


def _repo_root() -> Path:
    # src/datafusion_engine/extensions/plugin_manifest.py -> repo root at parents[3]
    return Path(__file__).resolve().parents[3]


def _dedupe(errors: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for entry in errors:
        if entry in seen:
            continue
        seen.add(entry)
        deduped.append(entry)
    return deduped


__all__ = ["PluginManifestResolution", "resolve_plugin_manifest"]
