"""Spec tables for extract dataset registry rows."""

from __future__ import annotations

from relspec.rules.cache import rule_table_cached

EXTRACT_RULE_TABLE = rule_table_cached("extract")

__all__ = ["EXTRACT_RULE_TABLE"]
