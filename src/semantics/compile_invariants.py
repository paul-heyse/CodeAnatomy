"""Compile-time invariant enforcement for semantic pipeline runs.

Provide a ``CompileTracker`` that records compile invocations and
verifies single-compile and resolver-identity invariants at pipeline end.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from semantics.compile_context import SemanticExecutionContext


@dataclass
class CompileTracker:
    """Track compile invocations and verify invariants at pipeline end.

    Record each compile call and validate at pipeline completion that
    exactly one compile occurred and all resolver instances share identity.

    Parameters
    ----------
    _compile_count
        Number of compile invocations recorded.
    _resolver_ids
        Object identity hashes of dataset resolvers observed.
    _manifest_fingerprints
        Manifest fingerprints recorded across compile calls.
    """

    _compile_count: int = 0
    _resolver_ids: list[int] = field(default_factory=list)
    _manifest_fingerprints: list[str] = field(default_factory=list)

    @property
    def compile_count(self) -> int:
        """Return the number of compile invocations recorded."""
        return self._compile_count

    @property
    def resolver_identity_count(self) -> int:
        """Return the number of distinct resolver identities recorded."""
        return len(set(self._resolver_ids))

    def record_compile(self, ctx: SemanticExecutionContext) -> None:
        """Record a semantic compile invocation.

        Parameters
        ----------
        ctx
            Semantic execution context from the compile call.
        """
        self._compile_count += 1
        self._resolver_ids.append(id(ctx.dataset_resolver))
        if ctx.manifest.fingerprint is not None:
            self._manifest_fingerprints.append(ctx.manifest.fingerprint)

    def verify_invariants(self) -> list[str]:
        """Verify all compile-time invariants and return violations.

        Returns:
        -------
        list[str]
            List of invariant violation messages. Empty when all pass.
        """
        violations: list[str] = []
        if self._compile_count != 1:
            violations.append(f"Expected 1 compile call, got {self._compile_count}")
        distinct_resolvers = len(set(self._resolver_ids))
        if distinct_resolvers > 1:
            violations.append(
                f"Resolver identity violation: {distinct_resolvers} "
                f"distinct resolvers across {len(self._resolver_ids)} calls"
            )
        distinct_fingerprints = len(set(self._manifest_fingerprints))
        if distinct_fingerprints > 1:
            violations.append(
                f"Manifest fingerprint inconsistency: {distinct_fingerprints} distinct fingerprints"
            )
        return violations

    def assert_invariants(self) -> None:
        """Assert all compile-time invariants hold.

        Raises:
        ------
        AssertionError
            When any invariant is violated.
        """
        violations = self.verify_invariants()
        if violations:
            msg = "Compile invariant violations:\n" + "\n".join(f"  - {v}" for v in violations)
            raise AssertionError(msg)

    def reset(self) -> None:
        """Reset the tracker for a new pipeline run."""
        self._compile_count = 0
        self._resolver_ids.clear()
        self._manifest_fingerprints.clear()


__all__ = ["CompileTracker"]
