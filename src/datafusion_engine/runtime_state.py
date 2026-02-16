"""Shared runtime state containers for DataFusion engine modules."""

from __future__ import annotations

from collections.abc import MutableMapping
from dataclasses import dataclass, field
from weakref import WeakKeyDictionary

from datafusion import SessionContext


@dataclass
class RuntimeStateRegistry:
    """Track mutable per-context state in weakly-referenced maps."""

    _state: WeakKeyDictionary[SessionContext, MutableMapping[str, object]] = field(
        default_factory=WeakKeyDictionary
    )

    def state_for(self, ctx: SessionContext) -> MutableMapping[str, object]:
        """Return mutable state for a session context."""
        state = self._state.get(ctx)
        if state is None:
            state = {}
            self._state[ctx] = state
        return state

    def clear(self) -> None:
        """Clear all tracked state."""
        self._state = WeakKeyDictionary()


__all__ = ["RuntimeStateRegistry"]
