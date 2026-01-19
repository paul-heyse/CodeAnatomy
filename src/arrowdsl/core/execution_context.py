"""Execution context helpers."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Literal

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.runtime_profiles import RuntimeProfile, runtime_profile_factory


@dataclass(frozen=True)
class SchemaValidationPolicy:
    """Schema validation settings for contract boundaries."""

    enabled: bool = False
    strict: bool | Literal["filter"] = "filter"
    coerce: bool = False
    lazy: bool = True


@dataclass(frozen=True)
class ExecutionContext:
    """Execution-time knobs passed through the DSL."""

    runtime: RuntimeProfile
    mode: Literal["strict", "tolerant"] = "tolerant"
    provenance: bool = False
    safe_cast: bool = True
    debug: bool = False
    schema_validation: SchemaValidationPolicy = field(default_factory=SchemaValidationPolicy)

    @property
    def determinism(self) -> DeterminismTier:
        """Return the active determinism tier.

        Returns
        -------
        DeterminismTier
            Determinism tier for this context.
        """
        return self.runtime.determinism

    @property
    def use_threads(self) -> bool:
        """Return whether to enable plan execution threads.

        Returns
        -------
        bool
            ``True`` when plan execution should use threads.
        """
        return self.runtime.plan_use_threads

    @property
    def scan_use_threads(self) -> bool:
        """Return whether dataset scanning should use threads.

        Returns
        -------
        bool
            ``True`` when dataset scanning should use threads.
        """
        return self.runtime.scan.use_threads

    def with_mode(self, mode: Literal["strict", "tolerant"]) -> ExecutionContext:
        """Return a copy with a different finalize mode.

        Parameters
        ----------
        mode:
            New finalize mode.

        Returns
        -------
        ExecutionContext
            Updated execution context.
        """
        return replace(self, mode=mode)

    def with_provenance(self, *, provenance: bool) -> ExecutionContext:
        """Return a copy with provenance toggled.

        Parameters
        ----------
        provenance:
            When ``True``, include provenance columns in scans.

        Returns
        -------
        ExecutionContext
            Updated execution context.
        """
        return replace(self, provenance=provenance)

    def with_determinism(self, tier: DeterminismTier) -> ExecutionContext:
        """Return a copy with a determinism tier override applied.

        Parameters
        ----------
        tier:
            Determinism tier override.

        Returns
        -------
        ExecutionContext
            Updated execution context.
        """
        runtime = self.runtime.with_determinism(tier)
        return replace(self, runtime=runtime)


@dataclass(frozen=True)
class ExecutionContextOptions:
    """Execution context option bundle for factory construction."""

    mode: Literal["strict", "tolerant"] = "tolerant"
    provenance: bool = False
    safe_cast: bool = True
    debug: bool = False
    schema_validation: SchemaValidationPolicy = field(default_factory=SchemaValidationPolicy)


def execution_context_factory(
    profile: str,
    *,
    options: ExecutionContextOptions | None = None,
) -> ExecutionContext:
    """Return an ExecutionContext for the named profile.

    Returns
    -------
    ExecutionContext
        Execution context with profile defaults applied.
    """
    runtime = runtime_profile_factory(profile)
    runtime.apply_global_thread_pools()
    options = options or ExecutionContextOptions()
    if options.debug and runtime.datafusion is not None:
        datafusion_profile = replace(runtime.datafusion, capture_explain=True)
        runtime = runtime.with_datafusion(datafusion_profile)
    return ExecutionContext(
        runtime=runtime,
        mode=options.mode,
        provenance=options.provenance,
        safe_cast=options.safe_cast,
        debug=options.debug,
        schema_validation=options.schema_validation,
    )


__all__ = [
    "ExecutionContext",
    "ExecutionContextOptions",
    "SchemaValidationPolicy",
    "execution_context_factory",
]
