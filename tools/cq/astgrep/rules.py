"""Language-aware ast-grep rule dispatch."""

from __future__ import annotations

from tools.cq.astgrep.sgpy_scanner import RecordType, RuleSpec
from tools.cq.query.language import QueryLanguage


def get_rules_for_types(
    record_types: set[RecordType] | None,
    *,
    lang: QueryLanguage,
) -> tuple[RuleSpec, ...]:
    """Get ast-grep rules for record types in a specific language.

    Returns
    -------
    tuple[RuleSpec, ...]
        Rules matching the requested language and record types.
    """
    if lang == "rust":
        from tools.cq.astgrep.rules_rust import get_rules_for_types as get_rust_rules_for_types

        return get_rust_rules_for_types(record_types)

    from tools.cq.astgrep.rules_py import get_rules_for_types as get_python_rules_for_types

    return get_python_rules_for_types(record_types)


__all__ = ["get_rules_for_types"]
