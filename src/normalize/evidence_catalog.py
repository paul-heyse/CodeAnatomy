"""Evidence availability catalog for normalize rules."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
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
    types_by_dataset: dict[str, dict[str, str]] = field(default_factory=dict)
    metadata_by_dataset: dict[str, dict[bytes, bytes]] = field(default_factory=dict)

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
                evidence.types_by_dataset[name] = _schema_types(schema)
                evidence.metadata_by_dataset[name] = _schema_metadata(schema)
        return evidence

    def register(self, name: str, schema: SchemaLike) -> None:
        """Register an evidence dataset and its schema."""
        self.sources.add(name)
        self.columns_by_dataset[name] = set(schema.names)
        self.types_by_dataset[name] = _schema_types(schema)
        self.metadata_by_dataset[name] = _schema_metadata(schema)

    def clone(self) -> EvidenceCatalog:
        """Return a shallow copy for staged updates.

        Returns
        -------
        EvidenceCatalog
            Copy of the catalog for staged updates.
        """
        return EvidenceCatalog(
            sources=set(self.sources),
            columns_by_dataset={key: set(cols) for key, cols in self.columns_by_dataset.items()},
            types_by_dataset={key: dict(types) for key, types in self.types_by_dataset.items()},
            metadata_by_dataset={key: dict(meta) for key, meta in self.metadata_by_dataset.items()},
        )

    def satisfies(self, spec: EvidenceSpec | None, *, inputs: Sequence[str]) -> bool:
        """Return whether the evidence spec requirements are met.

        Returns
        -------
        bool
            ``True`` when the spec requirements are satisfied.
        """
        resolved = spec or EvidenceSpec(sources=tuple(inputs))
        sources = resolved.sources or tuple(inputs)
        has_sources = self._sources_available(sources)
        has_columns = not resolved.required_columns or self._columns_available(
            sources, resolved.required_columns
        )
        has_types = not resolved.required_types or self._types_available(
            sources, resolved.required_types
        )
        has_metadata = not resolved.required_metadata or self._metadata_available(
            sources, resolved.required_metadata
        )
        return has_sources and has_columns and has_types and has_metadata

    def _sources_available(self, sources: Sequence[str]) -> bool:
        return set(sources).issubset(self.sources)

    def _columns_available(self, sources: Sequence[str], required_columns: Sequence[str]) -> bool:
        required = set(required_columns)
        for source in sources:
            columns = self.columns_by_dataset.get(source)
            if columns is None or not required.issubset(columns):
                return False
        return True

    def _types_available(self, sources: Sequence[str], required_types: Mapping[str, str]) -> bool:
        for source in sources:
            types = self.types_by_dataset.get(source)
            if types is None:
                return False
            for col, dtype in required_types.items():
                if types.get(col) != dtype:
                    return False
        return True

    def _metadata_available(
        self, sources: Sequence[str], required_metadata: Mapping[bytes, bytes]
    ) -> bool:
        for source in sources:
            metadata = self.metadata_by_dataset.get(source)
            if metadata is None:
                return False
            for key, value in required_metadata.items():
                if metadata.get(key) != value:
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


def _schema_types(schema: SchemaLike) -> dict[str, str]:
    return {field.name: str(field.type) for field in schema}


def _schema_metadata(schema: SchemaLike) -> dict[bytes, bytes]:
    return dict(schema.metadata or {})


__all__ = ["EvidenceCatalog"]
