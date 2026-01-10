from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal, Dict, Optional, Sequence, Tuple


import pyarrow as pa


InvariantFn = Callable[["pa.Table"], Tuple["pa.Array", str]]  # (bad_mask, error_code)


@dataclass(frozen=True)
class SortKey:
    column: str
    order: Literal["ascending", "descending"] = "ascending"


DedupeStrategy = Literal[
    "KEEP_FIRST_AFTER_SORT",
    "KEEP_BEST_BY_SCORE",
    "COLLAPSE_LIST",
    "KEEP_ARBITRARY",
]


@dataclass(frozen=True)
class DedupeSpec:
    """Dedupe semantics are explicit, centralized policy."""

    keys: Tuple[str, ...]
    tie_breakers: Tuple[SortKey, ...] = ()
    strategy: DedupeStrategy = "KEEP_FIRST_AFTER_SORT"


@dataclass(frozen=True)
class Contract:
    """Output contract (schema + invariants + determinism policy)."""

    name: str
    schema: "pa.Schema"

    key_fields: Tuple[str, ...] = ()
    required_non_null: Tuple[str, ...] = ()
    invariants: Tuple[InvariantFn, ...] = ()

    dedupe: Optional[DedupeSpec] = None
    canonical_sort: Tuple[SortKey, ...] = ()

    version: Optional[int] = None

    # NEW: properties required by downstream consumers but not stored as columns
    # Example: origin="scip" injected by the edge emission stage.
    virtual_fields: Tuple[str, ...] = ()

    # Optional docs for virtual fields (helpful for debugging)
    virtual_field_docs: Optional[Dict[str, str]] = None


    def with_versioned_schema(self) -> "pa.Schema":
        """Attach contract metadata to schema (best effort)."""
        import pyarrow as pa

        meta = dict(self.schema.metadata or {})
        meta[b"contract_name"] = str(self.name).encode("utf-8")
        if self.version is not None:
            meta[b"contract_version"] = str(self.version).encode("utf-8")
        return self.schema.with_metadata(meta)


    def available_fields(self) -> Tuple[str, ...]:
        """
        Columns + declared virtual fields. Used by validators.
        """
        cols = tuple(self.schema.names) if self.schema is not None else ()
        return tuple(cols) + tuple(self.virtual_fields or ())
