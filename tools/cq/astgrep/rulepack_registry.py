"""Injectable rule-pack registry for ast-grep rule loading."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from tools.cq.astgrep.rulepack_loader import load_rules_from_directory, load_utils
from tools.cq.astgrep.sgpy_scanner import RuleSpec


@dataclass(slots=True)
class RulePackRegistry:
    """Caches built-in YAML rule packs for one process context."""

    _cache: dict[str, tuple[RuleSpec, ...]] = field(default_factory=dict)

    def load_default(self, *, base: Path | None = None) -> dict[str, tuple[RuleSpec, ...]]:
        """Load default built-in rule packs from disk once per registry instance.

        Returns:
            dict[str, tuple[RuleSpec, ...]]: Rule packs keyed by language.
        """
        if self._cache:
            return dict(self._cache)
        resolved_base = (base or Path(__file__).parent).resolve()
        self._cache = _load_rulepacks_uncached(resolved_base)
        return dict(self._cache)

    def reset(self) -> None:
        """Reset in-memory cache for deterministic test isolation."""
        self._cache.clear()


def _load_rulepacks_uncached(base: Path) -> dict[str, tuple[RuleSpec, ...]]:
    rules_base = base / "rules"
    utils = load_utils(base / "utils")

    packs: dict[str, tuple[RuleSpec, ...]] = {}
    for facts_dir in sorted(rules_base.glob("*_facts")):
        lang = facts_dir.name.removesuffix("_facts")
        rules = load_rules_from_directory(facts_dir, utils_by_language=utils)
        if rules:
            packs[lang] = rules
    return packs


__all__ = ["RulePackRegistry"]
