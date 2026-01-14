"""Shared evidence planning and catalog helpers."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike
from arrowdsl.plan.catalog import PlanCatalog
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import ScanTelemetry
from arrowdsl.plan.scan_io import DatasetSource
from relspec.rules.definitions import EvidenceSpec, RuleDefinition


@dataclass(frozen=True)
class EvidenceDatasetSpec:
    """Evidence dataset metadata used for extract planning."""

    name: str
    alias: str
    template: str | None
    required_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class EvidenceRequirement:
    """Required evidence dataset and columns."""

    name: str
    alias: str
    template: str | None
    required_columns: tuple[str, ...]


@dataclass(frozen=True)
class EvidencePlan:
    """Compiled evidence plan describing required datasets and ops."""

    sources: tuple[str, ...]
    normalize_ops: tuple[str, ...]
    requirements: tuple[EvidenceRequirement, ...]
    telemetry: Mapping[str, ScanTelemetry] = field(default_factory=dict)

    def templates(self) -> tuple[str, ...]:
        """Return required extractor templates.

        Returns
        -------
        tuple[str, ...]
            Template names required by the plan.
        """
        return tuple(sorted({req.template for req in self.requirements if req.template}))

    def requires_template(self, template: str) -> bool:
        """Return whether a template is required.

        Returns
        -------
        bool
            ``True`` when the template is required.
        """
        return template in self.templates()

    def requires_dataset(self, name: str) -> bool:
        """Return whether a dataset alias/name is required.

        Returns
        -------
        bool
            ``True`` when the dataset is required.
        """
        if name in self.sources:
            return True
        return any(name in {req.name, req.alias} for req in self.requirements)

    def required_columns_for(self, name: str) -> tuple[str, ...]:
        """Return required columns for a dataset alias/name.

        Returns
        -------
        tuple[str, ...]
            Required column names for the dataset.
        """
        for req in self.requirements:
            if name in {req.name, req.alias}:
                return req.required_columns
        return ()

    def datasets_for_template(self, template: str) -> tuple[str, ...]:
        """Return dataset aliases required for a template.

        Returns
        -------
        tuple[str, ...]
            Dataset aliases required for the template.
        """
        return tuple(sorted(req.alias for req in self.requirements if req.template == template))


class NormalizeOpLike(Protocol):
    """Minimal normalize op interface for dependency expansion."""

    @property
    def name(self) -> str: ...

    @property
    def inputs(self) -> tuple[str, ...]: ...


NormalizeOpsProvider = Callable[[str], Sequence[NormalizeOpLike]]
EvidenceSpecProvider = Callable[[str], EvidenceDatasetSpec | None]


def compile_evidence_plan(
    rules: Sequence[RuleDefinition],
    *,
    extra_sources: Sequence[str] = (),
    evidence_spec: EvidenceSpecProvider,
    normalize_ops_for_output: NormalizeOpsProvider,
) -> EvidencePlan:
    """Compile an evidence plan from centralized rule definitions.

    Returns
    -------
    EvidencePlan
        Evidence plan describing required datasets and normalize ops.
    """
    sources: set[str] = set(extra_sources)
    required_columns: dict[str, set[str]] = {}
    for rule in rules:
        rule_sources = _rule_sources(rule)
        sources.update(rule_sources)
        _merge_required_columns(required_columns, rule_sources, rule.evidence)
    sources, normalize_ops = _expand_normalize_ops(sources, normalize_ops_for_output)
    requirements = _requirements_from_sources(sources, required_columns, evidence_spec)
    return EvidencePlan(
        sources=tuple(sorted(sources)),
        normalize_ops=tuple(sorted(normalize_ops)),
        requirements=requirements,
    )


def _rule_sources(rule: RuleDefinition) -> tuple[str, ...]:
    """Return evidence source names for a rule.

    Parameters
    ----------
    rule
        Rule definition to inspect.

    Returns
    -------
    tuple[str, ...]
        Evidence source names for the rule.
    """
    spec = rule.evidence
    if spec is None:
        return rule.inputs
    return spec.sources or rule.inputs


def _merge_required_columns(
    required_columns: dict[str, set[str]],
    sources: Iterable[str],
    spec: EvidenceSpec | None,
) -> None:
    """Merge required column names into a per-source mapping.

    Parameters
    ----------
    required_columns
        Accumulator of required columns per source.
    sources
        Evidence source names to update.
    spec
        Evidence spec carrying required columns.
    """
    if spec is None or not spec.required_columns:
        return
    for name in sources:
        required_columns.setdefault(name, set()).update(spec.required_columns)


def _expand_normalize_ops(
    sources: set[str],
    normalize_ops_for_output: NormalizeOpsProvider,
) -> tuple[set[str], set[str]]:
    """Expand evidence sources with normalize op dependencies.

    Parameters
    ----------
    sources
        Initial evidence source names.
    normalize_ops_for_output
        Provider for normalize ops by output name.

    Returns
    -------
    tuple[set[str], set[str]]
        Expanded sources and normalize op names.
    """
    normalize_ops: set[str] = set()
    pending = set(sources)
    while pending:
        name = pending.pop()
        for op in normalize_ops_for_output(name):
            normalize_ops.add(op.name)
            for input_name in op.inputs:
                if input_name not in sources:
                    sources.add(input_name)
                    pending.add(input_name)
    return sources, normalize_ops


def _requirements_from_sources(
    sources: Iterable[str],
    required_columns: Mapping[str, set[str]],
    evidence_spec: EvidenceSpecProvider,
) -> tuple[EvidenceRequirement, ...]:
    """Build evidence requirements from sources and specs.

    Parameters
    ----------
    sources
        Evidence source names to resolve.
    required_columns
        Required columns by source.
    evidence_spec
        Provider for evidence dataset specs.

    Returns
    -------
    tuple[EvidenceRequirement, ...]
        Requirements for evidence datasets.
    """
    requirements: list[EvidenceRequirement] = []
    seen: set[str] = set()
    for source in sorted(set(sources)):
        spec = evidence_spec(source)
        if spec is None or spec.name in seen:
            continue
        seen.add(spec.name)
        extra_columns = _required_columns_for_source(source, spec, required_columns)
        cols = tuple(sorted(set(spec.required_columns).union(extra_columns)))
        requirements.append(
            EvidenceRequirement(
                name=spec.name,
                alias=spec.alias,
                template=spec.template,
                required_columns=cols,
            )
        )
    return tuple(requirements)


def _required_columns_for_source(
    source: str,
    spec: EvidenceDatasetSpec,
    required_columns: Mapping[str, set[str]],
) -> set[str]:
    """Return extra required columns for a source alias or name.

    Parameters
    ----------
    source
        Evidence source name.
    spec
        Evidence dataset specification.
    required_columns
        Required columns by source.

    Returns
    -------
    set[str]
        Extra required columns for the source.
    """
    extra: set[str] = set()
    for key in (source, spec.alias, spec.name):
        extra.update(required_columns.get(key, set()))
    return extra


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
            Evidence catalog populated from the catalog.
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
            Copy of the evidence catalog.
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
            ``True`` when requirements are satisfied.
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
        """Return whether all sources are available.

        Parameters
        ----------
        sources
            Sources required by a rule.

        Returns
        -------
        bool
            ``True`` when all sources are available.
        """
        return set(sources).issubset(self.sources)

    def _columns_available(self, sources: Sequence[str], required_columns: Sequence[str]) -> bool:
        """Return whether required columns are available for sources.

        Parameters
        ----------
        sources
            Sources to validate.
        required_columns
            Column names required for each source.

        Returns
        -------
        bool
            ``True`` when all required columns are available.
        """
        required = set(required_columns)
        for source in sources:
            columns = self.columns_by_dataset.get(source)
            if columns is None or not required.issubset(columns):
                return False
        return True

    def _types_available(self, sources: Sequence[str], required_types: Mapping[str, str]) -> bool:
        """Return whether required column types are available for sources.

        Parameters
        ----------
        sources
            Sources to validate.
        required_types
            Column type requirements keyed by column name.

        Returns
        -------
        bool
            ``True`` when all required types are available.
        """
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
        """Return whether required metadata is available for sources.

        Parameters
        ----------
        sources
            Sources to validate.
        required_metadata
            Metadata key/value requirements.

        Returns
        -------
        bool
            ``True`` when all required metadata is available.
        """
        for source in sources:
            metadata = self.metadata_by_dataset.get(source)
            if metadata is None:
                return False
            for key, value in required_metadata.items():
                if metadata.get(key) != value:
                    return False
        return True


def _schema_from_source(source: object, *, ctx: ExecutionContext) -> SchemaLike | None:
    """Extract a schema from a plan or dataset source.

    Parameters
    ----------
    source
        Plan, dataset source, or schema-bearing object.
    ctx
        Execution context for plan schema evaluation.

    Returns
    -------
    SchemaLike | None
        Schema when available.
    """
    if isinstance(source, Plan):
        return source.schema(ctx=ctx)
    if isinstance(source, DatasetSource):
        return source.dataset.schema
    schema = getattr(source, "schema", None)
    if schema is not None and hasattr(schema, "names"):
        return schema
    return None


def _schema_types(schema: SchemaLike) -> dict[str, str]:
    """Return a mapping of column names to type strings.

    Parameters
    ----------
    schema
        Schema to inspect.

    Returns
    -------
    dict[str, str]
        Column type mapping.
    """
    return {field.name: str(field.type) for field in schema}


def _schema_metadata(schema: SchemaLike) -> dict[bytes, bytes]:
    """Return schema metadata as a concrete mapping.

    Parameters
    ----------
    schema
        Schema to inspect.

    Returns
    -------
    dict[bytes, bytes]
        Metadata mapping.
    """
    return dict(schema.metadata or {})


__all__ = [
    "EvidenceCatalog",
    "EvidenceDatasetSpec",
    "EvidencePlan",
    "EvidenceRequirement",
    "compile_evidence_plan",
]
