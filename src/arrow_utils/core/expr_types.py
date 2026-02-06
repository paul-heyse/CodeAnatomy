"""Shared expression type helpers."""

from __future__ import annotations

from dataclasses import dataclass

from datafusion_engine.expr.spec import ScalarLiteralInput

type ScalarValue = ScalarLiteralInput


@dataclass(frozen=True)
class ExplodeSpec:
    """Configuration for exploding list columns with stable offsets."""

    parent_keys: tuple[str, ...]
    list_col: str
    value_col: str
    idx_col: str | None = "idx"
    keep_empty: bool = True

    def rename_parent(self, *, parent: str, value: str) -> tuple[str, str] | None:
        """Return rename columns when defaults were used.

        Returns:
        -------
        tuple[str, str] | None
            Rename pair for parent/value when defaults were used.
        """
        if self.parent_keys != (parent,) or self.value_col != value:
            return None
        return parent, value


__all__ = ["ExplodeSpec", "ScalarValue"]
