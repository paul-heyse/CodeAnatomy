"""Bridge tree-sitter Rust macro captures into explicit expansion evidence."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec

from tools.cq.search.rust_macro_expansion_contracts import RustMacroExpansionEvidenceV1


def build_macro_expansion_evidence(
    payload: Mapping[str, object],
) -> tuple[RustMacroExpansionEvidenceV1, ...]:
    """Build macro expansion evidence rows from rust tree-sitter payload fields.

    Returns:
        tuple[RustMacroExpansionEvidenceV1, ...]: Unique macro evidence rows.
    """
    facts = payload.get("rust_tree_sitter_facts")
    if not isinstance(facts, dict):
        return ()
    calls = facts.get("calls")
    if not isinstance(calls, list):
        return ()
    rows: list[RustMacroExpansionEvidenceV1] = []
    seen: set[str] = set()
    for entry in calls:
        if not isinstance(entry, str):
            continue
        normalized = entry.strip()
        if not normalized.endswith("!"):
            continue
        macro_name = normalized[:-1]
        if not macro_name or macro_name in seen:
            continue
        seen.add(macro_name)
        rows.append(
            RustMacroExpansionEvidenceV1(
                macro_name=macro_name,
                expansion_hint=f"{macro_name}! expansion candidate",
                confidence="medium",
            )
        )
    return tuple(rows)


def attach_macro_expansion_evidence(payload: dict[str, object]) -> dict[str, object]:
    """Attach serialized macro expansion evidence to a payload in-place.

    Returns:
        dict[str, object]: Updated payload.
    """
    evidence = build_macro_expansion_evidence(payload)
    payload["macro_expansions"] = [msgspec.to_builtins(row) for row in evidence]
    return payload


__all__ = ["attach_macro_expansion_evidence", "build_macro_expansion_evidence"]
