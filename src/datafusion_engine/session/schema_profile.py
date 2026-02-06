"""Schema hardening profile for deterministic DataFusion SessionContext configuration.

Schema inference and catalog introspection depend on SessionContext configuration.
Without a hardened baseline, the same logical plan can yield different schema surfaces
based on configuration defaults. This module provides a canonical profile that ensures
deterministic schema behavior across sessions.

The SCHEMA_PROFILE constant captures the minimal set of DataFusion settings required
for stable, reproducible schema introspection:
- Catalog namespace configuration (default catalog/schema, information_schema)
- Type representation settings (view types, string mapping)
- Explain/format output controls (schema visibility, type info)
- Execution determinism (timezone, parquet metadata handling)
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext

# Canonical schema hardening profile for deterministic schema behavior.
# This tuple of (key, value) pairs configures DataFusion for stable schema inference
# and catalog introspection across sessions.
#
# Keys are organized by category:
# 1. Catalog namespace - ensure consistent catalog/schema resolution
# 2. Information schema - enable metadata introspection surfaces
# 3. Explain/format - control schema visibility in plan output
# 4. Execution - timezone and parquet handling for determinism
# 5. SQL parser - string type mapping for schema stability
SCHEMA_PROFILE: Final[tuple[tuple[str, str], ...]] = (
    # Catalog namespace settings
    ("datafusion.catalog.create_default_catalog_and_schema", "true"),
    ("datafusion.catalog.default_catalog", "cpg"),
    ("datafusion.catalog.default_schema", "public"),
    # Information schema enablement (required for introspection)
    ("datafusion.catalog.information_schema", "true"),
    # Explain/format output controls
    ("datafusion.explain.show_schema", "true"),
    ("datafusion.format.types_info", "true"),
    # Execution determinism settings
    ("datafusion.execution.time_zone", "UTC"),
    ("datafusion.execution.parquet.skip_metadata", "true"),
    # Type representation controls - disable view types for schema stability
    ("datafusion.execution.parquet.schema_force_view_types", "false"),
    ("datafusion.sql_parser.map_string_types_to_utf8view", "false"),
)

# Keys that are applied via builder methods rather than config.set()
_BUILDER_METHOD_KEYS: Final[frozenset[str]] = frozenset(
    {
        "datafusion.catalog.create_default_catalog_and_schema",
        "datafusion.catalog.default_catalog",
        "datafusion.catalog.default_schema",
        "datafusion.catalog.information_schema",
    }
)


def schema_profile_settings() -> Mapping[str, str]:
    """Return the schema profile as a mapping.

    Returns:
    -------
    Mapping[str, str]
        Dictionary of DataFusion config keys to string values.
    """
    return dict(SCHEMA_PROFILE)


def build_session_config(
    *,
    profile: tuple[tuple[str, str], ...] | None = None,
    overrides: Mapping[str, str] | None = None,
) -> SessionConfig:
    """Build a SessionConfig with schema hardening profile applied.

    Create a SessionConfig instance with the schema hardening profile settings.
    The profile ensures deterministic schema behavior for introspection and
    plan compilation.

    Parameters
    ----------
    profile : tuple[tuple[str, str], ...] | None
        Optional schema profile to apply. Defaults to SCHEMA_PROFILE.
    overrides : Mapping[str, str] | None
        Optional settings to override after profile application.

    Returns:
    -------
    SessionConfig
        Configured SessionConfig with schema hardening applied.

    Examples:
    --------
    >>> config = build_session_config()
    >>> ctx = SessionContext(config)

    >>> config = build_session_config(
    ...     overrides={"datafusion.execution.time_zone": "America/New_York"}
    ... )
    """
    effective_profile = profile if profile is not None else SCHEMA_PROFILE
    config = SessionConfig()

    # Apply profile settings - use builder methods where available for type safety,
    # fall back to set() for settings without dedicated methods
    default_catalog: str = "cpg"
    default_schema: str = "public"
    enable_information_schema: bool = True

    # First pass: extract values from profile for builder methods
    for key, value in effective_profile:
        if key == "datafusion.catalog.default_catalog":
            default_catalog = value
        elif key == "datafusion.catalog.default_schema":
            default_schema = value
        elif key == "datafusion.catalog.information_schema":
            enable_information_schema = value.lower() == "true"

    # Apply settings using builder methods where available
    config = config.with_default_catalog_and_schema(default_catalog, default_schema)
    config = config.with_create_default_catalog_and_schema(enabled=True)
    config = config.with_information_schema(enable_information_schema)

    # Apply remaining settings via set()
    for key, value in effective_profile:
        # Skip settings already applied via builder methods
        if key in _BUILDER_METHOD_KEYS:
            continue
        config = config.set(key, value)

    # Apply overrides last
    if overrides:
        for key, value in overrides.items():
            config = config.set(key, str(value))

    return config


def create_session_context(
    *,
    profile: tuple[tuple[str, str], ...] | None = None,
    overrides: Mapping[str, str] | None = None,
    runtime_env: RuntimeEnvBuilder | None = None,
) -> SessionContext:
    """Create a hardened SessionContext for schema introspection.

    Build a SessionContext with deterministic schema behavior. The context
    is configured with the schema hardening profile to ensure consistent
    schema inference and catalog introspection across sessions.

    Parameters
    ----------
    profile : tuple[tuple[str, str], ...] | None
        Optional schema profile to apply. Defaults to SCHEMA_PROFILE.
    overrides : Mapping[str, str] | None
        Optional settings to override after profile application.
    runtime_env : RuntimeEnvBuilder | None
        Optional RuntimeEnvBuilder for custom runtime configuration.
        If not provided, uses default runtime environment.

    Returns:
    -------
    SessionContext
        Hardened SessionContext ready for schema introspection.

    Examples:
    --------
    >>> ctx = create_session_context()
    >>> ctx.sql("SELECT * FROM information_schema.tables").show()

    >>> ctx = create_session_context(overrides={"datafusion.execution.target_partitions": "1"})
    """
    config = build_session_config(profile=profile, overrides=overrides)

    if runtime_env is not None:
        return SessionContext(config, runtime_env)
    return SessionContext(config)


def apply_schema_profile(
    config: SessionConfig,
    *,
    profile: tuple[tuple[str, str], ...] | None = None,
) -> SessionConfig:
    """Apply schema hardening profile to an existing SessionConfig.

    Mutate an existing SessionConfig by applying the schema hardening
    profile settings. This is useful when integrating with other config
    construction pipelines.

    Parameters
    ----------
    config : SessionConfig
        Existing SessionConfig to modify.
    profile : tuple[tuple[str, str], ...] | None
        Optional schema profile to apply. Defaults to SCHEMA_PROFILE.

    Returns:
    -------
    SessionConfig
        Modified SessionConfig with schema hardening applied.
    """
    effective_profile = profile if profile is not None else SCHEMA_PROFILE

    for key, value in effective_profile:
        config = config.set(key, value)

    return config


__all__ = [
    "SCHEMA_PROFILE",
    "apply_schema_profile",
    "build_session_config",
    "create_session_context",
    "schema_profile_settings",
]
