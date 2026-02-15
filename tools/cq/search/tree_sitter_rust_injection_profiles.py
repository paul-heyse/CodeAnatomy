"""Rust macro injection profile selection for tree-sitter enrichment."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class RustInjectionProfileV1(CqStruct, frozen=True):
    """Profile row describing macro injection parse behavior."""

    profile_name: str
    language: str
    combined: bool = False
    macro_names: tuple[str, ...] = ()


_PROFILES: tuple[RustInjectionProfileV1, ...] = (
    RustInjectionProfileV1(
        profile_name="sql",
        language="sql",
        combined=True,
        macro_names=("sql", "query"),
    ),
    RustInjectionProfileV1(
        profile_name="regex",
        language="regex",
        combined=True,
        macro_names=("regex",),
    ),
    RustInjectionProfileV1(
        profile_name="rust_default",
        language="rust",
        combined=False,
        macro_names=(),
    ),
)


def resolve_rust_injection_profile(macro_name: str | None) -> RustInjectionProfileV1:
    """Resolve best-fit injection profile for a macro name.

    Returns:
        RustInjectionProfileV1: Selected injection profile.
    """
    normalized = (macro_name or "").strip()
    for profile in _PROFILES:
        if normalized and normalized in profile.macro_names:
            return profile
    return _PROFILES[-1]


__all__ = ["RustInjectionProfileV1", "resolve_rust_injection_profile"]
