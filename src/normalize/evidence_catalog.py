"""Evidence availability catalog for normalize rules."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike
from arrowdsl.plan.catalog import PlanCatalog
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.scan_io import DatasetSource
from normalize.rule_model import EvidenceSpec


@dataclass
class EvidenceCatalog:
    """Track available evidence sources and columns."""

    sources: set[str] = field(default_factory=set)
    columns_by_dataset: dict[str, set[str]] = field(default_factory=dict)

    @classmethod
    def from_plan_catalog(cls, catalog: PlanCatalog, *, ctx: ExecutionContext) -> EvidenceCatalog:
        """Build an evidence catalog from a plan catalog.

        Returns
        -------
        EvidenceCatalog
            Evidence catalog populated from catalog sources.
        """
        evidence = cls(sources=set(catalog.tables))
        for name, source in catalog.tables.items():
            schema = _schema_from_source(source, ctx=ctx)
            if schema is not None:
                evidence.columns_by_dataset[name] = set(schema.names)
        return evidence

    def register(self, name: str, schema: SchemaLike) -> None:
        """Register an evidence dataset and its schema."""
        self.sources.add(name)
        self.columns_by_dataset[name] = set(schema.names)

    def satisfies(self, spec: EvidenceSpec | None, *, inputs: Sequence[str]) -> bool:
        """Return whether the evidence spec requirements are met.

        Returns
        -------
        bool
            ``True`` when the spec requirements are satisfied.
        """
        if spec is None:
            return True
        sources = spec.sources or tuple(inputs)
        if not set(sources).issubset(self.sources):
            return False
        if not spec.required_columns:
            return True
        for source in sources:
            columns = self.columns_by_dataset.get(source)
            if columns is None:
                continue
            if not set(spec.required_columns).issubset(columns):
                return False
        return True


def _schema_from_source(source: object, *, ctx: ExecutionContext) -> SchemaLike | None:
    if isinstance(source, Plan):
        return source.schema(ctx=ctx)
    if isinstance(source, DatasetSource):
        return source.dataset.schema
    schema = getattr(source, "schema", None)
    if schema is not None and hasattr(schema, "names"):
        return schema
    return None


__all__ = ["EvidenceCatalog"]
