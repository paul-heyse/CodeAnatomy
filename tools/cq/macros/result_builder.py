"""Shared macro result-construction helpers."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.run_context import RunContext
from tools.cq.core.schema import (
    CqResult,
    Finding,
    Section,
    assign_result_finding_ids,
    mk_result,
    ms,
)
from tools.cq.core.summary_contract import apply_summary_mapping
from tools.cq.macros.contracts import ScoringDetailsV1
from tools.cq.macros.rust_fallback_policy import RustFallbackPolicyV1, apply_rust_fallback_policy

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain


class MacroResultBuilder:
    """Common helper for macro run context, summary, and result assembly."""

    def __init__(
        self,
        macro_name: str,
        *,
        root: Path,
        argv: list[str],
        tc: Toolchain | None,
        run_id: str | None = None,
        started_ms: float | None = None,
    ) -> None:
        """Initialize a macro result builder with run context and empty result state."""
        started = ms() if started_ms is None else started_ms
        run_ctx = RunContext.from_parts(
            root=root,
            argv=argv,
            tc=tc,
            started_ms=started,
            run_id=run_id,
        )
        self.root = root
        self.result = mk_result(run_ctx.to_runmeta(macro_name))
        self._scoring: ScoringDetailsV1 | None = None

    def set_summary(self, **kwargs: object) -> MacroResultBuilder:
        """Update result summary with keyword fields and return builder.

        Returns:
        -------
        MacroResultBuilder
            Current builder for fluent chaining.
        """
        self.result.summary = apply_summary_mapping(self.result.summary, kwargs)
        return self

    def set_scoring(self, details: ScoringDetailsV1 | None) -> MacroResultBuilder:
        """Set scoring details used by macro callers and return builder.

        Returns:
        -------
        MacroResultBuilder
            Current builder for fluent chaining.
        """
        self._scoring = details
        return self

    def add_findings(self, findings: list[Finding]) -> MacroResultBuilder:
        """Append key findings and return builder.

        Returns:
        -------
        MacroResultBuilder
            Current builder for fluent chaining.
        """
        self.result.key_findings.extend(findings)
        return self

    def add_section(self, section: Section) -> MacroResultBuilder:
        """Append one section and return builder.

        Returns:
        -------
        MacroResultBuilder
            Current builder for fluent chaining.
        """
        self.result.sections.append(section)
        return self

    def apply_rust_fallback(self, *, policy: RustFallbackPolicyV1) -> MacroResultBuilder:
        """Apply Rust fallback policy to current result and return builder.

        Returns:
        -------
        MacroResultBuilder
            Current builder for fluent chaining.
        """
        self.result = apply_rust_fallback_policy(self.result, root=self.root, policy=policy)
        return self

    def build(self) -> CqResult:
        """Finalize finding identifiers and return the assembled macro result.

        Returns:
        -------
        CqResult
            Final macro result with stable finding identifiers.
        """
        self.result = assign_result_finding_ids(self.result)
        return self.result


__all__ = ["MacroResultBuilder"]
