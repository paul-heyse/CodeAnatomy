"""Global pygit2 settings helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import cache

import pygit2


@dataclass(frozen=True)
class GitSettingsSpec:
    """Optional pygit2 Settings overrides."""

    owner_validation: bool | None = None
    server_timeout_ms: int | None = None
    ssl_cert_file: str | None = None


def apply_git_settings(spec: GitSettingsSpec) -> None:
    """Apply pygit2 settings overrides when supported.

    Disabling owner validation allows reading repositories owned by other users
    (common in CI or shared volumes) but removes a safety guard. Only disable it
    for trusted workspaces.
    """
    settings = pygit2.Settings()
    if spec.owner_validation is not None and hasattr(settings, "owner_validation"):
        settings.owner_validation = spec.owner_validation
    if spec.server_timeout_ms is not None and hasattr(settings, "server_timeout"):
        settings.server_timeout = spec.server_timeout_ms
    if spec.ssl_cert_file and hasattr(settings, "ssl_cert_file"):
        settings.ssl_cert_file = spec.ssl_cert_file


@cache
def apply_git_settings_once() -> None:
    """Apply settings once based on environment overrides."""
    spec = git_settings_from_env()
    if spec is not None:
        apply_git_settings(spec)


def git_settings_from_env() -> GitSettingsSpec | None:
    """Build GitSettingsSpec from environment variables when present.

    Returns
    -------
    GitSettingsSpec | None
        Settings derived from environment variables.
    """
    owner_validation = _env_bool("CODEANATOMY_GIT_OWNER_VALIDATION")
    server_timeout_ms = _env_int("CODEANATOMY_GIT_SERVER_TIMEOUT_MS")
    ssl_cert_file = os.getenv("CODEANATOMY_GIT_SSL_CERT_FILE")
    if owner_validation is None and server_timeout_ms is None and not ssl_cert_file:
        return None
    return GitSettingsSpec(
        owner_validation=owner_validation,
        server_timeout_ms=server_timeout_ms,
        ssl_cert_file=ssl_cert_file or None,
    )


def _env_bool(name: str) -> bool | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    return None


def _env_int(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


__all__ = [
    "GitSettingsSpec",
    "apply_git_settings",
    "apply_git_settings_once",
    "git_settings_from_env",
]
