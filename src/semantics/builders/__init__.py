"""Semantic view builder protocol and registration utilities.

This module provides the protocol for semantic view builders and
registration utilities for the semantic catalog.

Example
-------
>>> from semantics.builders import SemanticViewBuilder
>>> from semantics.catalog import SEMANTIC_CATALOG
>>>
>>> class MyViewBuilder:
...     @property
...     def name(self) -> str:
...         return "my_view"
...
...     @property
...     def evidence_tier(self) -> int:
...         return 2
...
...     @property
...     def upstream_deps(self) -> tuple[str, ...]:
...         return ("cst_defs", "scip_occurrences")
...
...     def build(self, ctx: SessionContext) -> DataFrame:
...         return ctx.sql("SELECT * FROM cst_defs")
>>> builder = MyViewBuilder()
>>> isinstance(builder, SemanticViewBuilder)
True
"""

from __future__ import annotations

from semantics.builders.protocol import SemanticViewBuilder
from semantics.builders.registry import register_builder, register_builders

__all__ = [
    "SemanticViewBuilder",
    "register_builder",
    "register_builders",
]
