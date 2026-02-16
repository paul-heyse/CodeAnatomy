"""Declarative Rust fallback policy for macro commands.

Replaces 8 per-macro ``_apply_rust_fallback`` wrappers with a single
policy-driven helper.  Each macro defines a ``RustFallbackPolicyV1``
constant (or builds one at runtime for dynamic patterns) and calls
``apply_rust_fallback_policy`` instead.
"""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.schema import CqResult
from tools.cq.core.structs import CqStruct


class RustFallbackPolicyV1(CqStruct, frozen=True):
    """Declarative Rust fallback parameters for a macro."""

    macro_name: str
    pattern: str
    query: str | None = None
    fallback_matches_summary_key: str | None = None


def apply_rust_fallback_policy(
    result: CqResult,
    *,
    root: Path,
    policy: RustFallbackPolicyV1,
) -> CqResult:
    """Apply Rust fallback using policy parameters.

    Parameters
    ----------
    result
        Existing Python-only CqResult to enrich.
    root
        Repository root path.
    policy
        Declarative policy capturing macro name, pattern, query,
        and optional summary key for fallback_matches.

    Returns:
    -------
    CqResult
        The mutated result with Rust fallback data merged in.
    """
    from tools.cq.macros.multilang_fallback import apply_rust_macro_fallback

    fallback_matches = 0
    if policy.fallback_matches_summary_key:
        raw = getattr(result.summary, policy.fallback_matches_summary_key, 0)
        fallback_matches = int(raw) if isinstance(raw, int) else 0

    apply_rust_macro_fallback(
        result=result,
        root=root,
        pattern=policy.pattern,
        macro_name=policy.macro_name,
        fallback_matches=fallback_matches,
        query=policy.query,
    )
    return result


__all__ = ["RustFallbackPolicyV1", "apply_rust_fallback_policy"]
