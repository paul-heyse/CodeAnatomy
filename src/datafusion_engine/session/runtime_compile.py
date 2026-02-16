"""Compile invariant functions, coercion helpers, and compile option resolution for DataFusion runtime.

This module contains artifact recording helpers, compile/resolver invariant enforcement,
compile option resolution types, and public facade functions for runtime feature queries.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING

from datafusion_engine.compile.options import (
    DataFusionCacheEvent,
    DataFusionCompileOptions,
    DataFusionSqlPolicy,
    DataFusionSubstraitFallbackEvent,
)
from datafusion_engine.lineage.diagnostics import record_artifact as _lineage_record_artifact
from datafusion_engine.session.contracts import IdentifierNormalizationMode
from datafusion_engine.session.runtime_config_policies import (
    DATAFUSION_MAJOR_VERSION,
    DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION,
    _effective_catalog_autoload_for_profile,
)
from utils.coercion import coerce_bool, coerce_int
from utils.env_utils import env_bool

if TYPE_CHECKING:
    from collections.abc import Callable

    from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from serde_schema_registry import ArtifactSpec

    ExplainRows = TableLike | RecordBatchReaderLike
else:
    ExplainRows = object

_COMPILE_RESOLVER_STRICT_ENV = "CODEANATOMY_COMPILE_RESOLVER_INVARIANTS_STRICT"
_CI_ENV = "CI"


@lru_cache(maxsize=1)
def _load_runtime_artifact_specs() -> None:
    """Load artifact specs exactly once to avoid module import cycles."""
    import serde_artifact_specs  # noqa: F401


def _ensure_runtime_artifact_specs_registered() -> None:
    """Ensure typed artifact specs are registered before artifact writes."""
    _load_runtime_artifact_specs()


def record_artifact(
    profile: DataFusionRuntimeProfile,
    name: ArtifactSpec,
    payload: Mapping[str, object],
) -> None:
    """Record an artifact after ensuring artifact-spec side effects are loaded.

    Raises:
        TypeError: If ``name`` is not an ``ArtifactSpec`` instance.
    """
    from serde_schema_registry import ArtifactSpec as RuntimeArtifactSpec

    if not isinstance(name, RuntimeArtifactSpec):
        msg = (
            "record_artifact() requires an ArtifactSpec instance for `name`; "
            f"got {type(name).__name__}."
        )
        raise TypeError(msg)
    _ensure_runtime_artifact_specs_registered()
    _lineage_record_artifact(profile, name, payload)


def compile_resolver_invariants_strict_mode() -> bool:
    """Resolve strict-mode behavior for compile/resolver invariant enforcement.

    Returns:
        bool: True when strict invariant enforcement is enabled for this runtime.
    """
    strict_override = env_bool(
        _COMPILE_RESOLVER_STRICT_ENV,
        default=None,
        on_invalid="none",
    )
    if strict_override is not None:
        return strict_override
    return bool(env_bool(_CI_ENV, default=False, on_invalid="false"))


def compile_resolver_invariant_artifact_payload(
    *,
    invariants: _CompileResolverInvariantInputs,
) -> dict[str, object]:
    """Return payload for ``compile_resolver_invariants_v1`` artifacts.

    Returns:
    -------
    dict[str, object]
        Normalized artifact payload for compile/resolver invariants.
    """
    normalized_violations = tuple(item for item in invariants.violations if item)
    return {
        "label": invariants.label,
        "compile_count": invariants.compile_count,
        "max_compiles": invariants.max_compiles,
        "distinct_resolver_count": invariants.distinct_resolver_count,
        "strict": invariants.strict,
        "violations": normalized_violations,
    }


@dataclass(frozen=True)
class _CompileResolverInvariantInputs:
    label: str
    compile_count: int
    max_compiles: int
    distinct_resolver_count: int
    strict: bool
    violations: Sequence[str]


def _parse_compile_resolver_invariant_inputs(
    invariants: _CompileResolverInvariantInputs | None,
    kwargs: Mapping[str, object],
) -> _CompileResolverInvariantInputs:
    if invariants is not None:
        if kwargs:
            message = "Cannot pass legacy keyword arguments when `invariants` is provided."
            raise TypeError(message)
        return invariants
    label = _coerce_str(kwargs.get("label"), "label")
    compile_count = coerce_int(kwargs.get("compile_count"), label="compile_count")
    max_compiles = coerce_int(kwargs.get("max_compiles"), label="max_compiles")
    distinct_resolver_count = coerce_int(
        kwargs.get("distinct_resolver_count"),
        label="distinct_resolver_count",
    )
    strict = coerce_bool(kwargs.get("strict"), default=False, label="strict")
    violations = _coerce_str_sequence(kwargs.get("violations"), "violations")
    return _CompileResolverInvariantInputs(
        label=label,
        compile_count=compile_count,
        max_compiles=max_compiles,
        distinct_resolver_count=distinct_resolver_count,
        strict=strict,
        violations=violations,
    )


def _coerce_str(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        message = f"Field {field_name!r} must be a string."
        raise TypeError(message)
    return value


def _coerce_str_sequence(value: object, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str) or not isinstance(value, Sequence):
        message = f"Field {field_name!r} must be a sequence of strings."
        raise TypeError(message)
    return tuple(item for item in value if isinstance(item, str))


def record_compile_resolver_invariants(
    profile: DataFusionRuntimeProfile,
    *,
    invariants: _CompileResolverInvariantInputs | None = None,
    **kwargs: object,
) -> tuple[str, ...]:
    """Record compile/resolver invariant artifact and optionally enforce strict mode.

    Returns:
        tuple[str, ...]: Normalized violations written into the artifact payload.

    Raises:
        RuntimeError: If strict mode is enabled and at least one violation is present.
    """
    from serde_artifact_specs import COMPILE_RESOLVER_INVARIANTS_SPEC

    invariants_payload = _parse_compile_resolver_invariant_inputs(invariants, kwargs)
    normalized_violations = tuple(item for item in invariants_payload.violations if item)
    normalized_payload = _CompileResolverInvariantInputs(
        label=invariants_payload.label,
        compile_count=invariants_payload.compile_count,
        max_compiles=invariants_payload.max_compiles,
        distinct_resolver_count=invariants_payload.distinct_resolver_count,
        strict=invariants_payload.strict,
        violations=normalized_violations,
    )
    profile.record_artifact(
        COMPILE_RESOLVER_INVARIANTS_SPEC,
        compile_resolver_invariant_artifact_payload(invariants=normalized_payload),
    )
    if normalized_payload.strict and normalized_violations:
        message = "\n".join(normalized_violations)
        raise RuntimeError(message)
    return normalized_violations


@dataclass(frozen=True)
class _ResolvedCompileHooks:
    explain_hook: Callable[[str, ExplainRows], None] | None
    plan_artifacts_hook: Callable[[Mapping[str, object]], None] | None
    semantic_diff_hook: Callable[[Mapping[str, object]], None] | None
    sql_ingest_hook: Callable[[Mapping[str, object]], None] | None
    cache_event_hook: Callable[[DataFusionCacheEvent], None] | None
    substrait_fallback_hook: Callable[[DataFusionSubstraitFallbackEvent], None] | None


@dataclass(frozen=True)
class _CompileOptionResolution:
    cache: bool | None
    cache_max_columns: int | None
    params: Mapping[str, object] | None
    param_allowlist: tuple[str, ...] | None
    prepared_param_types: Mapping[str, str] | None
    prepared_statements: bool
    dynamic_projection: bool | None
    capture_explain: bool
    explain_analyze: bool
    substrait_validation: bool
    capture_plan_artifacts: bool
    capture_semantic_diff: bool
    sql_policy: DataFusionSqlPolicy | None
    sql_policy_name: str | None


def _resolve_prepared_statement_options(
    resolved: DataFusionCompileOptions,
) -> tuple[Mapping[str, str] | None, bool, bool | None]:
    prepared_param_types = resolved.prepared_param_types
    prepared_statements = resolved.prepared_statements
    dynamic_projection = resolved.dynamic_projection
    return prepared_param_types, prepared_statements, dynamic_projection


def _supports_explain_analyze_level() -> bool:
    if DATAFUSION_MAJOR_VERSION is None:
        return False
    return DATAFUSION_MAJOR_VERSION >= DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION


def effective_catalog_autoload(
    profile: DataFusionRuntimeProfile,
) -> tuple[str | None, str | None]:
    """Return effective catalog autoload settings for a profile.

    Returns:
    -------
    tuple[str | None, str | None]
        Catalog location and file format.
    """
    return _effective_catalog_autoload_for_profile(profile)


def _identifier_normalization_mode(
    profile: DataFusionRuntimeProfile,
) -> IdentifierNormalizationMode:
    mode = profile.features.identifier_normalization_mode
    if (
        profile.features.enable_delta_session_defaults
        and mode != IdentifierNormalizationMode.STRICT
    ):
        return IdentifierNormalizationMode.RAW
    return mode


def _effective_ident_normalization(profile: DataFusionRuntimeProfile) -> bool:
    mode = _identifier_normalization_mode(profile)
    return mode in {
        IdentifierNormalizationMode.SQL_SAFE,
        IdentifierNormalizationMode.STRICT,
    }


def effective_ident_normalization(profile: DataFusionRuntimeProfile) -> bool:
    """Return whether identifier normalization is enabled for a profile.

    Returns:
    -------
    bool
        True when identifier normalization is enabled.
    """
    return _effective_ident_normalization(profile)


def supports_explain_analyze_level() -> bool:
    """Return whether explain analyze level is supported.

    Returns:
    -------
    bool
        True when explain analyze level is supported.
    """
    return _supports_explain_analyze_level()


__all__ = [
    "_CompileOptionResolution",
    "_CompileResolverInvariantInputs",
    "_ResolvedCompileHooks",
    "_coerce_str",
    "_coerce_str_sequence",
    "_effective_ident_normalization",
    "_ensure_runtime_artifact_specs_registered",
    "_identifier_normalization_mode",
    "_load_runtime_artifact_specs",
    "_parse_compile_resolver_invariant_inputs",
    "_resolve_prepared_statement_options",
    "_supports_explain_analyze_level",
    "compile_resolver_invariant_artifact_payload",
    "compile_resolver_invariants_strict_mode",
    "effective_catalog_autoload",
    "effective_ident_normalization",
    "record_artifact",
    "record_compile_resolver_invariants",
    "supports_explain_analyze_level",
]
