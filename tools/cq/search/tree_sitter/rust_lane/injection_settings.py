"""Query-setting helpers for Rust injection planning."""

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.core.structs import CqStruct


class InjectionSettingsV1(CqStruct, frozen=True):
    """Pattern-level injection settings derived from ``#set!`` metadata."""

    language: str | None = None
    combined: bool = False
    include_children: bool = False
    use_self_language: bool = False
    use_parent_language: bool = False


def settings_for_pattern(query: object, pattern_idx: int) -> InjectionSettingsV1:
    """Load normalized injection settings for one query pattern index."""
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


__all__ = ["InjectionSettingsV1", "settings_for_pattern"]
