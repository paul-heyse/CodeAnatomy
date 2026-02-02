"""Tag index helpers for CQ diskcache usage."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache

_TAG_INDEX_PREFIX = "__cq_tag_index__:"
_TAG_INDEX_TAG = "cq_tag_index"

_MISSING = object()


@dataclass
class TagIndex:
    """Maintain a tag -> keys mapping inside a diskcache instance."""

    cache: "Cache | FanoutCache"
    prefix: str = _TAG_INDEX_PREFIX

    def add(self, key: str, tags: Sequence[str], *, use_transaction: bool = True) -> None:
        """Add key to each tag mapping."""
        tag_list = _normalize_tags(tags)
        if not tag_list:
            return
        if use_transaction and hasattr(self.cache, "transact"):
            with self.cache.transact():
                self._add_no_tx(key, tag_list)
        else:
            self._add_no_tx(key, tag_list)

    def remove(self, key: str, tags: Sequence[str], *, use_transaction: bool = True) -> None:
        """Remove key from each tag mapping."""
        tag_list = _normalize_tags(tags)
        if not tag_list:
            return
        if use_transaction and hasattr(self.cache, "transact"):
            with self.cache.transact():
                self._remove_no_tx(key, tag_list)
        else:
            self._remove_no_tx(key, tag_list)

    def keys_for(self, tag: str, *, prune: bool = False) -> set[str]:
        """Return keys associated with a tag.

        If prune is True, stale keys are removed from the index.
        """
        tag_key = self._tag_key(tag)
        keys = self.cache.get(tag_key, default=None)
        if not isinstance(keys, set):
            return set()
        if not prune:
            return set(keys)
        valid: set[str] = set()
        stale: set[str] = set()
        for key in keys:
            if self.cache.get(key, default=_MISSING) is _MISSING:
                stale.add(key)
            else:
                valid.add(key)
        if stale:
            updated = set(keys)
            updated.difference_update(stale)
            if updated:
                self.cache.set(tag_key, updated, tag=_TAG_INDEX_TAG, retry=True)
            else:
                self.cache.delete(tag_key, retry=True)
        return valid

    def is_tag_index_key(self, key: str) -> bool:
        """Return True if the key is a tag index entry."""
        return key.startswith(self.prefix)

    def _tag_key(self, tag: str) -> str:
        return f"{self.prefix}{tag}"

    def _add_no_tx(self, key: str, tags: Sequence[str]) -> None:
        for tag in tags:
            tag_key = self._tag_key(tag)
            current = self.cache.get(tag_key, default=None)
            if isinstance(current, set):
                updated = set(current)
            else:
                updated = set()
            updated.add(key)
            self.cache.set(tag_key, updated, tag=_TAG_INDEX_TAG, retry=True)

    def _remove_no_tx(self, key: str, tags: Sequence[str]) -> None:
        for tag in tags:
            tag_key = self._tag_key(tag)
            current = self.cache.get(tag_key, default=None)
            if not isinstance(current, set):
                continue
            updated = set(current)
            updated.discard(key)
            if updated:
                self.cache.set(tag_key, updated, tag=_TAG_INDEX_TAG, retry=True)
            else:
                self.cache.delete(tag_key, retry=True)


def _normalize_tags(tags: Iterable[Any]) -> list[str]:
    normalized: list[str] = []
    for tag in tags:
        if not isinstance(tag, str):
            continue
        if not tag:
            continue
        normalized.append(tag)
    return normalized


__all__ = [
    "TagIndex",
]
