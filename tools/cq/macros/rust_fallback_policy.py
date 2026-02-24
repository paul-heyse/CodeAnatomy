"""Declarative Rust fallback policy for macro commands.

Replaces 8 per-macro ``_apply_rust_fallback`` wrappers with a single
policy-driven helper.  Each macro defines a ``RustFallbackPolicyV1``
constant (or builds one at runtime for dynamic patterns) and calls
``apply_rust_fallback_policy`` instead.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from tools.cq.core.schema import CqResult
from tools.cq.core.structs import CqStruct
from tools.cq.search.semantic.diagnostics import CAPABILITY_MATRIX


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
    from tools.cq.macros.multilang_fallback import (
        RustMacroFallbackRequestV1,
        apply_rust_macro_fallback,
        apply_unsupported_macro_contract,
    )

    capability = CAPABILITY_MATRIX.get(f"macro:{policy.macro_name}")
    rust_level = capability.get("rust", "none") if capability is not None else "none"
    if rust_level == "none":
        return apply_unsupported_macro_contract(
            result=result,
            root=root,
            macro_name=policy.macro_name,
            rust_only=_is_rust_only_request(result.run.argv),
            query=policy.query,
        )

    fallback_matches = 0
    if policy.fallback_matches_summary_key:
        raw = getattr(result.summary, policy.fallback_matches_summary_key, 0)
        fallback_matches = int(raw) if isinstance(raw, int) else 0

    return apply_rust_macro_fallback(
        RustMacroFallbackRequestV1(
            result=result,
            root=root,
            pattern=policy.pattern,
            macro_name=policy.macro_name,
            fallback_matches=fallback_matches,
            query=policy.query,
        )
    )


def _is_rust_only_request(argv: Sequence[str]) -> bool:
    """Heuristically detect Rust-only command intent from argv tokens.

    Returns:
        bool: True when argv tokens indicate Rust scope without Python scope.
    """
    has_rust_scope = False
    has_python_scope = False
    previous: str | None = None
    for raw in argv:
        token = str(raw).strip().lower()
        if token in {"--lang", "--in", "--include", "--exclude"}:
            previous = token
            continue

        if previous in {"--lang", "--in"}:
            if token == "rust":
                has_rust_scope = True
            elif token == "python":
                has_python_scope = True
        previous = None

        if "lang=rust" in token or "in=rust" in token:
            has_rust_scope = True
        if token.endswith(".rs") or ".rs" in token or "rust/" in token or "/rust/" in token:
            has_rust_scope = True

        if "lang=python" in token or "in=python" in token:
            has_python_scope = True
        if token.endswith(".py") or ".py" in token or "/python/" in token:
            has_python_scope = True

    return has_rust_scope and not has_python_scope


__all__ = ["RustFallbackPolicyV1", "apply_rust_fallback_policy"]
