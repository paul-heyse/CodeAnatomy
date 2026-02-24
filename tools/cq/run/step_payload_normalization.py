"""Compatibility normalization for run-step payload aliases."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

_SEARCH_ALIASES: dict[str, str] = {
    "lang": "lang_scope",
    "in": "in_dir",
}


def normalize_step_payload(payload: Mapping[str, object]) -> dict[str, object]:
    """Normalize one run-step payload.

    Returns:
        dict[str, object]: Step payload with legacy aliases normalized.
    """
    out = dict(payload)
    if out.get("type") != "search":
        return out
    for src, dst in _SEARCH_ALIASES.items():
        if src in out and dst not in out:
            out[dst] = out.pop(src)
    return out


def normalize_plan_payload(payload: Mapping[str, object]) -> dict[str, object]:
    """Normalize a run-plan mapping payload.

    Returns:
        dict[str, object]: Plan payload with normalized step mappings.
    """
    out = dict(payload)
    raw_steps = out.get("steps")
    if isinstance(raw_steps, Sequence) and not isinstance(raw_steps, (str, bytes, bytearray)):
        normalized_steps: list[object] = []
        for item in raw_steps:
            if isinstance(item, Mapping):
                normalized_steps.append(normalize_step_payload(item))
            else:
                normalized_steps.append(item)
        out["steps"] = normalized_steps
    return out


__all__ = ["normalize_plan_payload", "normalize_step_payload"]
