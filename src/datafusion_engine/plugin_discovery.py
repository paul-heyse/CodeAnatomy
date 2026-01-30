"""Canonical discovery and validation for DataFusion plugins."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

from utils.env_utils import env_bool, env_value
from utils.hashing import hash_json_canonical
from utils.validation import ensure_mapping


class PluginDiscoveryError(RuntimeError):
    """Raised when plugin discovery or validation fails."""


@dataclass(frozen=True)
class PluginDiscoveryReport:
    """Plugin discovery diagnostics payload."""

    plugin_path: Path | None
    source: str
    manifest: dict[str, object] | None
    manifest_hash: str | None
    stub_enabled: bool

    def to_payload(self) -> dict[str, object]:
        """Return a JSON-ready payload for diagnostics.

        Returns
        -------
        dict[str, object]
            JSON-serializable diagnostics payload.
        """
        return {
            "plugin_path": str(self.plugin_path) if self.plugin_path is not None else None,
            "source": self.source,
            "manifest": self.manifest,
            "manifest_hash": self.manifest_hash,
            "stub_enabled": self.stub_enabled,
        }


_PLUGIN_PATH_ENV = "CODEANATOMY_DF_PLUGIN_PATH"
_PLUGIN_MANIFEST_ENV = "CODEANATOMY_DF_PLUGIN_MANIFEST_PATH"
_PLUGIN_STUB_ENV = "CODEANATOMY_PLUGIN_STUB"


def plugin_stub_enabled() -> bool:
    """Return True when the explicit plugin stub is enabled.

    Returns
    -------
    bool
        ``True`` when the plugin stub gate is enabled.
    """
    return env_bool(_PLUGIN_STUB_ENV, default=False)


def _plugin_library_filename(crate_name: str) -> str:
    import sys

    if sys.platform == "win32":
        return f"{crate_name}.dll"
    if sys.platform == "darwin":
        return f"lib{crate_name}.dylib"
    return f"lib{crate_name}.so"


def _require_datafusion_ext() -> ModuleType:
    try:
        import datafusion_ext
    except ImportError as exc:  # pragma: no cover - validated by callers
        msg = (
            "datafusion_ext is required for DataFusion plugin discovery. "
            "Build and install the extension (see scripts/bootstrap.sh)."
        )
        raise PluginDiscoveryError(msg) from exc
    return datafusion_ext


def _manifest_path() -> Path:
    env_path = env_value(_PLUGIN_MANIFEST_ENV)
    if env_path:
        return Path(env_path)
    root = Path(__file__).resolve().parents[2]
    return root / "build" / "datafusion_plugin_manifest.json"


def _manifest_plugin_path() -> Path | None:
    path = _manifest_path()
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        msg = f"Invalid plugin manifest at {path}."
        raise PluginDiscoveryError(msg) from exc
    mapping = ensure_mapping(payload, label="plugin manifest")
    raw = mapping.get("plugin_path") or mapping.get("path")
    if raw is None:
        return None
    candidate = Path(str(raw))
    if candidate.is_absolute():
        return candidate
    return (path.parent / candidate).resolve()


def _extension_plugin_path(module: ModuleType) -> Path | None:
    plugin_path = getattr(module, "plugin_library_path", None)
    if not callable(plugin_path):
        return None
    try:
        result = plugin_path()
    except Exception as exc:  # pragma: no cover - defensive wrapping
        msg = "datafusion_ext.plugin_library_path failed."
        raise PluginDiscoveryError(msg) from exc
    if not isinstance(result, str):
        msg = "datafusion_ext.plugin_library_path returned a non-string value."
        raise PluginDiscoveryError(msg)
    candidate = Path(result)
    if candidate.exists():
        return candidate
    return None


def _workspace_plugin_candidates() -> list[Path]:
    root = Path(__file__).resolve().parents[2]
    lib_name = _plugin_library_filename("df_plugin_codeanatomy")
    candidates: list[Path] = []
    for profile in ("release", "debug"):
        candidates.append(root / "rust" / "target" / profile / lib_name)
        candidates.append(root / "rust" / "df_plugin_codeanatomy" / "target" / profile / lib_name)
    return candidates


def resolve_plugin_path() -> Path:
    """Resolve the plugin library path using the canonical discovery order.

    Returns
    -------
    pathlib.Path
        Resolved plugin library path.

    Raises
    ------
    PluginDiscoveryError
        Raised when discovery fails or stub mode is enabled.
    """
    if plugin_stub_enabled():
        msg = "Plugin stub enabled; plugin path resolution is disabled."
        raise PluginDiscoveryError(msg)
    env_path = env_value(_PLUGIN_PATH_ENV)
    if env_path:
        candidate = Path(env_path)
        if candidate.is_absolute():
            return candidate
        root = Path(__file__).resolve().parents[2]
        return (root / candidate).resolve()
    module = _require_datafusion_ext()
    extension_path = _extension_plugin_path(module)
    if extension_path is not None:
        return extension_path
    manifest_path = _manifest_plugin_path()
    if manifest_path is not None:
        return manifest_path
    for candidate in _workspace_plugin_candidates():
        if candidate.exists():
            return candidate
    msg = (
        "DataFusion plugin library not found. Set CODEANATOMY_DF_PLUGIN_PATH or build "
        "rust/df_plugin_codeanatomy and ensure datafusion_ext.plugin_library_path is available."
    )
    raise PluginDiscoveryError(msg)


def plugin_manifest(path: Path | None = None) -> dict[str, object]:
    """Return the plugin manifest payload.

    Returns
    -------
    dict[str, object]
        JSON-ready manifest payload.

    Raises
    ------
    PluginDiscoveryError
        Raised when manifest retrieval fails.
    """
    if plugin_stub_enabled():
        return {"stub": True}
    module = _require_datafusion_ext()
    manifest_fn = getattr(module, "plugin_manifest", None)
    if not callable(manifest_fn):
        msg = "datafusion_ext.plugin_manifest is unavailable."
        raise PluginDiscoveryError(msg)
    resolved = str(path or resolve_plugin_path())
    try:
        manifest = manifest_fn(resolved)
    except Exception as exc:  # pragma: no cover - defensive wrapping
        msg = f"datafusion_ext.plugin_manifest failed for {resolved!r}."
        raise PluginDiscoveryError(msg) from exc
    payload = ensure_mapping(manifest, label="plugin manifest")
    return {str(key): value for key, value in payload.items()}


def assert_plugin_available(path: Path | None = None) -> None:
    """Fail fast if the plugin is missing or incompatible.

    Raises
    ------
    PluginDiscoveryError
        Raised when the plugin is missing or incompatible.
    """
    if plugin_stub_enabled():
        return
    resolved = path or resolve_plugin_path()
    if not resolved.exists():
        msg = f"DataFusion plugin library not found at {resolved}."
        raise PluginDiscoveryError(msg)
    _ = plugin_manifest(resolved)


def plugin_diagnostics() -> PluginDiscoveryReport:
    """Return a discovery report suitable for diagnostics sinks.

    Returns
    -------
    PluginDiscoveryReport
        Diagnostics report for plugin discovery and manifest validation.
    """
    if plugin_stub_enabled():
        return PluginDiscoveryReport(
            plugin_path=None,
            source="stub",
            manifest={"stub": True},
            manifest_hash=None,
            stub_enabled=True,
        )
    plugin_path, source = _resolve_discovery_path()
    manifest, manifest_hash = _manifest_payload(plugin_path)
    return PluginDiscoveryReport(
        plugin_path=plugin_path,
        source=source,
        manifest=manifest,
        manifest_hash=manifest_hash,
        stub_enabled=False,
    )


def _resolve_discovery_path() -> tuple[Path | None, str]:
    source = "unresolved"
    env_path = env_value(_PLUGIN_PATH_ENV)
    if env_path:
        candidate = Path(env_path)
        if candidate.is_absolute():
            return candidate, "env"
        root = Path(__file__).resolve().parents[2]
        return (root / candidate).resolve(), "env"
    module = _require_datafusion_ext()
    extension_path = _extension_plugin_path(module)
    if extension_path is not None:
        return extension_path, "extension"
    manifest_path = _manifest_plugin_path()
    if manifest_path is not None:
        return manifest_path, "manifest"
    for candidate in _workspace_plugin_candidates():
        if candidate.exists():
            return candidate, "workspace"
    return None, source


def _manifest_payload(
    plugin_path: Path | None,
) -> tuple[dict[str, object] | None, str | None]:
    if plugin_path is None or not plugin_path.exists():
        return None, None
    manifest = plugin_manifest(plugin_path)
    manifest_hash = hash_json_canonical(manifest, str_keys=True)
    return manifest, manifest_hash


__all__ = [
    "PluginDiscoveryError",
    "PluginDiscoveryReport",
    "assert_plugin_available",
    "plugin_diagnostics",
    "plugin_manifest",
    "plugin_stub_enabled",
    "resolve_plugin_path",
]
