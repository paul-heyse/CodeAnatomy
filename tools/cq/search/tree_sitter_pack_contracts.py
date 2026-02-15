"""Contracts loader for tree-sitter query-pack governance."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.structs import CqStruct


class QueryPackRulesV1(CqStruct, frozen=True):
    """Validation rules enforced for one query-pack language lane."""

    require_rooted: bool = True
    forbid_non_local: bool = True


def _contracts_path(language: str) -> Path:
    return Path(__file__).with_suffix("").parent / "queries" / language / "contracts.yaml"


def load_pack_rules(language: str) -> QueryPackRulesV1:
    """Load query-pack rules from `contracts.yaml`, falling back to defaults.

    Returns:
        QueryPackRulesV1: Query-pack loading and enforcement rules.
    """
    path = _contracts_path(language)
    if not path.exists():
        return QueryPackRulesV1()
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return QueryPackRulesV1()
    require_rooted = True
    forbid_non_local = True
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith("require_rooted:"):
            require_rooted = stripped.split(":", maxsplit=1)[1].strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
        if stripped.startswith("forbid_non_local:"):
            forbid_non_local = stripped.split(":", maxsplit=1)[1].strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
    return QueryPackRulesV1(
        require_rooted=require_rooted,
        forbid_non_local=forbid_non_local,
    )


__all__ = [
    "QueryPackRulesV1",
    "load_pack_rules",
]
