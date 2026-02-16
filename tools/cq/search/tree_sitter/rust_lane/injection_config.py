"""Merged injection config: settings and profile selection for Rust injection planning."""

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.core.structs import CqStruct

# -- Injection Settings -------------------------------------------------------


class InjectionSettingsV1(CqStruct, frozen=True):
    """Pattern-level injection settings derived from ``#set!`` metadata."""

    language: str | None = None
    combined: bool = False
    include_children: bool = False
    use_self_language: bool = False
    use_parent_language: bool = False


def settings_for_pattern(query: object, pattern_idx: int) -> InjectionSettingsV1:
    """Load normalized injection settings for one query pattern index.

    Returns:
        InjectionSettingsV1: Function return value.
    """
    pattern_settings = getattr(query, "pattern_settings", None)
    if not callable(pattern_settings):
        return InjectionSettingsV1()
    raw = pattern_settings(pattern_idx)
    if not isinstance(raw, Mapping):
        return InjectionSettingsV1()
    language_value = raw.get("injection.language")
    language = str(language_value) if isinstance(language_value, str) and language_value else None
    return InjectionSettingsV1(
        language=language,
        combined="injection.combined" in raw,
        include_children="injection.include-children" in raw,
        use_self_language="injection.self" in raw,
        use_parent_language="injection.parent" in raw,
    )


# -- Injection Profiles -------------------------------------------------------


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


__all__ = [
    "InjectionSettingsV1",
    "RustInjectionProfileV1",
    "resolve_rust_injection_profile",
    "settings_for_pattern",
]
