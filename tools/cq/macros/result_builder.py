"""Shared macro result-construction helpers."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

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
        seed = mk_result(run_ctx.to_runmeta(macro_name))
        self._run_meta = seed.run
        self._summary = seed.summary
        self._key_findings: list[Finding] = []
        self._evidence: list[Finding] = []
        self._sections: list[Section] = []
        self._artifacts = list(seed.artifacts)
        self._scoring: ScoringDetailsV1 | None = None

    @property
    def result(self) -> CqResult:
        """Expose a mutable draft view backed by builder-local state."""
        return CqResult(
            run=self._run_meta,
            summary=self._summary,
            key_findings=tuple(self._key_findings),
            evidence=tuple(self._evidence),
            sections=tuple(self._sections),
            artifacts=tuple(self._artifacts),
        )

    def add_finding(self, finding: Finding) -> MacroResultBuilder:
        """Append one key finding and return builder.

        Returns:
            MacroResultBuilder: Current builder for fluent chaining.
        """
        self._key_findings.append(finding)
        return self

    def set_summary(self, **kwargs: object) -> MacroResultBuilder:
        """Update result summary with keyword fields and return builder.

        Returns:
        -------
        MacroResultBuilder
            Current builder for fluent chaining.
        """
        self._summary = apply_summary_mapping(self._summary, kwargs)
        return self

    def set_summary_field(self, key: str, value: object) -> MacroResultBuilder:
        """Set one summary field and return builder.

        Returns:
            MacroResultBuilder: Current builder for fluent chaining.
        """
        self._summary = apply_summary_mapping(self._summary, ((key, value),))
        return self

    def with_summary(self, summary: object) -> MacroResultBuilder:
        """Replace entire summary payload and return builder.

        Returns:
            MacroResultBuilder: Current builder for fluent chaining.
        """
        self._summary = msgspec.convert(summary, type=type(self._summary), strict=False)
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

    def add_findings(self, findings: Iterable[Finding]) -> MacroResultBuilder:
        """Append key findings and return builder.

        Returns:
        -------
        MacroResultBuilder
            Current builder for fluent chaining.
        """
        self._key_findings.extend(findings)
        return self

    def add_classified_findings(
        self,
        buckets: dict[str, list[Finding]],
    ) -> MacroResultBuilder:
        """Append all findings from classified buckets and return builder.

        Returns:
            MacroResultBuilder: Current builder for fluent chaining.
        """
        for findings in buckets.values():
            self._key_findings.extend(findings)
        return self

    def add_evidence(self, finding: Finding) -> MacroResultBuilder:
        """Append one evidence finding and return builder.

        Returns:
            MacroResultBuilder: Current builder for fluent chaining.
        """
        self._evidence.append(finding)
        return self

    def add_evidences(self, findings: Iterable[Finding]) -> MacroResultBuilder:
        """Append multiple evidence findings and return builder.

        Returns:
            MacroResultBuilder: Current builder for fluent chaining.
        """
        self._evidence.extend(findings)
        return self

    def add_taint_findings(self, sites: Iterable[Finding]) -> MacroResultBuilder:
        """Append taint-site findings to evidence and return builder.

        Returns:
            MacroResultBuilder: Current builder for fluent chaining.
        """
        self._evidence.extend(sites)
        return self

    def add_section(self, section: Section) -> MacroResultBuilder:
        """Append one section and return builder.

        Returns:
        -------
        MacroResultBuilder
            Current builder for fluent chaining.
        """
        self._sections.append(section)
        return self

    def apply_rust_fallback(self, *, policy: RustFallbackPolicyV1) -> MacroResultBuilder:
        """Apply Rust fallback policy to current result and return builder.

        Returns:
        -------
        MacroResultBuilder
            Current builder for fluent chaining.
        """
        updated = apply_rust_fallback_policy(self.result, root=self.root, policy=policy)
        self._summary = updated.summary
        self._key_findings = list(updated.key_findings)
        self._evidence = list(updated.evidence)
        self._sections = list(updated.sections)
        self._artifacts = list(updated.artifacts)
        return self

    def build(self) -> CqResult:
        """Finalize finding identifiers and return the assembled macro result.

        Returns:
        -------
        CqResult
            Final macro result with stable finding identifiers.
        """
        return assign_result_finding_ids(
            CqResult(
                run=self._run_meta,
                summary=self._summary,
                key_findings=tuple(self._key_findings),
                evidence=tuple(self._evidence),
                sections=tuple(self._sections),
                artifacts=tuple(self._artifacts),
            )
        )


__all__ = ["MacroResultBuilder"]
